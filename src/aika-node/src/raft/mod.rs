pub mod election;
pub mod http_transport;
pub mod log;
pub mod replication;
pub mod rpc;
pub mod state;
pub mod storage;

#[cfg(test)]
mod tests;

use storage::RaftStorage;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use tokio::sync::{Mutex, mpsc};

use crate::common::Command;
use http_transport::HttpTransport;
use log::RaftLog;
use state::{LogIndex, NodeId, RaftState, Term};

/// A committed log entry delivered to the application state machine.
///
/// Produced by the Raft core once an entry reaches commit quorum; consumed
/// by the task that drives `StateMachine::apply`.
pub enum ApplyMsg<C> {
    Command {
        index: LogIndex,
        term: Term,
        command: C,
    },
}

/// Errors that can occur during Raft operations (e.g. proposing a command).
#[derive(Debug, thiserror::Error)]
pub enum RaftError {
    #[error("not leader")]
    NotLeader,

    #[error("transport error: {0}")]
    Transport(#[from] rpc::TransportError),

    #[error("internal error: {0}")]
    Internal(String),
}

/// A handle to a running Raft node.
///
/// This is the main interface for cluster controller nodes to interact with Raft: proposing
/// commands, checking leadership, and registering commit callbacks.
#[derive(Clone)]
pub struct RaftNode {
    /// This node's id
    node_id: u64,

    /// Raft State, shared across all tasks
    state: Arc<Mutex<RaftState>>,

    /// Replicated log, shared across all tasks.
    log: Arc<Mutex<RaftLog<Command>>>,

    /// HTTP transport for sending `RequestVote` / `AppendEntries` RPCs.
    transport: Arc<HttpTransport<Command>>,

    /// Maps each peer's `NodeId` (its address string) to a stable `u64`
    /// so that `leader_info` can return a numeric ID for any leader.
    ///
    /// Peers are assigned IDs equal to their 0-based index in the `peers`
    /// slice passed to `new`.  The local node always uses its own `node_id`.
    peer_id_map: HashMap<NodeId, u64>,

    /// Submits propose requests to the main Raft event loop.
    ///
    /// Each message carries the command and a one-shot reply channel that is
    /// fulfilled once the entry is committed (or an error occurs).
    propose_tx: mpsc::Sender<(Command, tokio::sync::oneshot::Sender<anyhow::Result<()>>)>,

    /// Callbacks invoked (in order, inside the apply task) whenever a log
    /// entry is committed.  Uses `std::sync::Mutex` so `on_commit` can be
    /// called from synchronous context during node setup.
    commit_callbacks: Arc<std::sync::Mutex<Vec<Box<dyn Fn(Command) + Send + 'static>>>>,

    /// Persistent storage handle — used to durably save term/vote and log.
    storage: Arc<RaftStorage>,
}

impl RaftNode {
    /// Create a new Raft node with the given ID, peer addresses, and local data directory.
    ///
    /// `data_dir` must be a node-local path (e.g. `/tmp/inf3203_raft_<node_id>/`).
    /// It must **not** be on a shared/distributed filesystem.
    pub fn new(node_id: u64, peers: Vec<String>, data_dir: std::path::PathBuf) -> Self {
        let own_node_id: NodeId = node_id.to_string();

        // Assign each peer a stable u64 equal to its index in the input slice.
        let peer_id_map: HashMap<NodeId, u64> = peers
            .iter()
            .enumerate()
            .map(|(i, addr)| (addr.clone(), i as u64))
            .collect();

        // Peer NodeIds are the raw address strings; base URLs add the http:// scheme
        // so reqwest can form valid endpoints like "http://host:port/raft/request_vote".
        let peer_urls: HashMap<NodeId, String> = peers
            .iter()
            .map(|addr| (addr.clone(), format!("http://{addr}")))
            .collect();

        let blocked_peers = Arc::new(Mutex::new(HashSet::new()));
        let transport = Arc::new(HttpTransport::new(
            own_node_id.clone(),
            peer_urls,
            blocked_peers,
            std::time::Duration::from_millis(200),
        ));

        let peer_node_ids: Vec<NodeId> = peers.clone();
        let raft_state = Arc::new(Mutex::new(RaftState::new(own_node_id, peer_node_ids)));
        let raft_log: Arc<Mutex<RaftLog<Command>>> = Arc::new(Mutex::new(RaftLog::new()));

        let storage = Arc::new(RaftStorage::new(data_dir).expect("failed to open raft storage"));

        // Restore persisted state (term, votedFor) if this node has run before.
        if let Some(ps) = storage
            .load_persistent_state()
            .expect("failed to load persistent state")
        {
            raft_state.blocking_lock().persistent = ps;
        }

        // Restore the replicated log from disk.
        let entries = storage.load_log::<Command>().expect("failed to load log");
        if !entries.is_empty() {
            *raft_log.blocking_lock() = RaftLog::from_entries(entries);
        }

        let (propose_tx, _propose_rx) = mpsc::channel(64);

        // TODO: Spawn the main Raft event loop.  It should:
        //   1. Start an ElectionTimer (election::ElectionTimer::start).
        //   2. Pass Arc<RaftStorage> into the loop so mutation helpers can call
        //      storage.save_persistent_state() and storage.save_log() as needed.
        //   3. Loop selecting on:
        //      - Election timeout  → election::start_election(...)
        //      - Vote result       → election::handle_vote_response(...)
        //      - Majority won      → election::become_leader(...)
        //      - Propose request   → append to log, replicate, await commit,
        //                           reply on the oneshot channel.
        //   4. On commit, invoke each callback in commit_callbacks.
        //   5. Keep _propose_rx alive inside the spawned task.

        RaftNode {
            node_id,
            state: raft_state,
            log: raft_log,
            transport,
            peer_id_map,
            propose_tx,
            commit_callbacks: Arc::new(std::sync::Mutex::new(Vec::new())),
            storage,
        }
    }

    /// Propose a new command to be appended to the log.
    /// Returns once the entry is committed (or fails).
    pub async fn propose(&self, command: Command) -> anyhow::Result<()> {
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        self.propose_tx
            .send((command, reply_tx))
            .await
            .map_err(|_| anyhow::anyhow!("raft event loop has shut down"))?;
        reply_rx
            .await
            .map_err(|_| anyhow::anyhow!("raft event loop dropped the reply sender"))?
    }

    /// Check if this node is currently the Raft leader.
    pub fn is_leader(&self) -> bool {
        self.state
            .try_lock()
            .map(|s| s.is_leader())
            .unwrap_or(false)
    }

    /// Get the current leader's ID and address, if known.
    pub fn leader_info(&self) -> Option<(u64, String)> {
        let state = self.state.try_lock().ok()?;
        let leader_node_id: &NodeId = state.volatile.current_leader.as_ref()?;

        // If this node is the leader, return its own numeric ID and NodeId string.
        if leader_node_id == &state.id {
            return Some((self.node_id, leader_node_id.clone()));
        }

        // For a peer leader, look up the numeric ID assigned in `new`.
        let numeric_id = self.peer_id_map.get(leader_node_id).copied().unwrap_or(0);
        Some((numeric_id, leader_node_id.clone()))
    }

    /// Handle an incoming `RequestVote` RPC from a candidate.
    ///
    /// Raft paper: Figure 2, "RequestVote RPC", "Receiver implementation".
    ///
    /// Called by the CC HTTP server on `POST /raft/request_vote`.
    /// Rules (Raft §5.2 + §5.4):
    ///   1. If `args.term < current_term` → deny.
    ///   2. If `args.term > current_term` → step down to follower in new term.
    ///   3. Grant vote if `voted_for` is `None` or equals `args.candidate_id`,
    ///      AND the candidate's log is at least as up-to-date as ours.
    ///   4. Persist `voted_for` before replying (call `storage.save_persistent_state`).
    ///   5. If vote is granted, reset the election timer.
    pub async fn handle_request_vote(&self, args: rpc::RequestVoteArgs) -> rpc::RequestVoteReply {
        // Lock state then log — consistent ordering with every other call site.
        let mut state_guard = self.state.lock().await;
        let log_guard = self.log.lock().await;

        let current_term = state_guard.persistent.current_term;

        // 1. Reject if the candidate's term is stale.
        if args.term < current_term {
            return rpc::RequestVoteReply {
                term: current_term,
                vote_granted: false,
            };
        }

        // 2. Step down if the candidate has a higher term.
        if args.term > current_term {
            state_guard.step_down_to_follower(&self.storage, args.term);
        }

        let current_term = state_guard.persistent.current_term;

        // 3. Grant vote if we haven't voted (or already voted for this candidate)
        //    AND the candidate's log is at least as up-to-date as ours.
        let can_vote = match &state_guard.persistent.voted_for {
            None => true,
            Some(id) => id == &args.candidate_id,
        };

        let log_ok = log_guard.is_at_least_as_up_to_date(args.last_log_index, args.last_log_term);

        if can_vote && log_ok {
            // 4. Record vote and persist before replying.
            state_guard.persistent.voted_for = Some(args.candidate_id.clone());
            self.storage
                .save_persistent_state(&state_guard.persistent)
                .expect("failed to persist voted_for");

            return rpc::RequestVoteReply {
                term: current_term,
                vote_granted: true,
            };
        }

        rpc::RequestVoteReply {
            term: current_term,
            vote_granted: false,
        }
    }

    /// Handle an incoming `AppendEntries` RPC from the leader.
    ///
    /// Called by the CC HTTP server on `POST /raft/append_entries`.
    /// Delegates to `replication::handle_append_entries`.
    /// The caller must supply an `apply_tx` sender so committed entries reach
    /// the state machine.  In the full event-loop design, this sender is
    /// created once and stored alongside the `RaftNode`.
    pub async fn handle_append_entries(
        &self,
        args: rpc::AppendEntriesArgs<Command>,
        apply_tx: tokio::sync::mpsc::Sender<ApplyMsg<Command>>,
        reset_election_timer: impl Fn() + Send,
    ) -> rpc::AppendEntriesReply {
        replication::handle_append_entries(
            args,
            Arc::clone(&self.state),
            Arc::clone(&self.log),
            Arc::clone(&self.storage),
            apply_tx,
            reset_election_timer,
        )
        .await
    }

    /// Register a callback that is invoked when a log entry is committed.
    /// This is how the StateMachine receives commands to apply.
    pub fn on_commit(&self, callback: impl Fn(Command) + Send + 'static) {
        self.commit_callbacks
            .lock()
            .expect("commit_callbacks lock poisoned")
            .push(Box::new(callback));
    }
}
