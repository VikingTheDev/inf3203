/// Leader election logic.
///
/// Raft elects leaders using a randomised timeout mechanism:
///
///   1. Followers start an **election timer** set to a random duration in a
///      configured range (typically 150–300 ms).
///   2. On timeout, the follower transitions to **Candidate**, increments its
///      term, votes for itself, and broadcasts `RequestVote` RPCs.
///   3. A candidate that receives votes from a **majority** (including itself)
///      transitions to **Leader** and immediately sends heartbeats.
///   4. Any node that receives a message with a higher term reverts to
///      **Follower** (`RaftState::step_down_to_follower`).
///
/// Functions here are called from the main Raft loop (`mod.rs`) and must
/// never block — they schedule async tasks via the provided Tokio handle.
use std::sync::Arc;
use std::time::Duration;

use rand::Rng;
use tokio::sync::{Mutex, mpsc};
use tokio::time::{Instant, sleep};
use tracing::{debug, info};

use super::{
    RaftError,
    log::RaftLog,
    rpc::{RaftTransport, RequestVoteArgs, RequestVoteReply},
    state::{LeaderState, NodeId, RaftState, Role, Term},
    storage::RaftStorage,
};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Tunable parameters for the election subsystem.
#[derive(Debug, Clone)]
pub struct ElectionConfig {
    /// Minimum election timeout.
    pub timeout_min: Duration,

    /// Maximum election timeout (random value chosen in `[min, max)`).
    pub timeout_max: Duration,
}

impl Default for ElectionConfig {
    fn default() -> Self {
        ElectionConfig {
            timeout_min: Duration::from_millis(150),
            timeout_max: Duration::from_millis(300),
        }
    }
}

// ---------------------------------------------------------------------------
// Election timer
// ---------------------------------------------------------------------------

/// Handle to the background election timer task.
///
/// The timer is reset every time the node receives a heartbeat or grants a
/// vote.  When it fires, the node starts an election.
///
/// Raft paper: §5.2, paragraph 1.
///
/// Internally uses a `tokio::sync::watch` channel: the timer task reads the
/// latest `Instant` at which the deadline was last reset; any change to that
/// value restarts the countdown.
pub struct ElectionTimer {
    /// Sends a "reset" signal to the background timer task.
    reset_tx: tokio::sync::watch::Sender<Instant>,
}

impl ElectionTimer {
    /// Spawn the background timer task.
    ///
    /// `on_timeout` is an async closure (or channel sender) invoked when the
    /// timer expires without being reset. Typically, it sends a message to
    /// the main Raft loop to start an election.
    ///
    /// Returns the `ElectionTimer` handle (keep it alive for as long as the
    /// timer should run).
    pub fn start(config: ElectionConfig, on_timeout: mpsc::Sender<()>) -> Self {
        let (reset_tx, mut reset_rx) = tokio::sync::watch::channel(Instant::now());

        tokio::spawn(async move {
            loop {
                let timeout_duration = random_timeout(&config);
                let deadline = *reset_rx.borrow_and_update() + timeout_duration;

                tokio::select! {
                    // Timer expired — check if the deadline is still valid
                    // (i.e. no reset happened while we slept).
                    _ = sleep(deadline.saturating_duration_since(Instant::now())) => {
                        // Re-check: if reset_tx was updated after we read the
                        // borrow, the deadline moved and this is a stale wake.
                        if Instant::now() >= deadline {
                            if on_timeout.send(()).await.is_err() {
                                // Receiver dropped — node is shutting down.
                                break;
                            }
                        }
                        // Otherwise loop again with a fresh timeout.
                    }
                    // A reset arrived — loop to recalculate the deadline.
                    result = reset_rx.changed() => {
                        if result.is_err() {
                            // Sender dropped — node is shutting down.
                            break;
                        }
                    }
                }
            }
        });

        ElectionTimer { reset_tx }
    }

    /// Reset the election timer (call on every valid heartbeat or vote grant).
    ///
    /// Sends the current `Instant::now()` through the watch channel so the
    /// background task recalculates its deadline.
    pub fn reset(&self) {
        self.reset_tx.send(Instant::now()).ok();
    }
}

// ---------------------------------------------------------------------------
// Starting an election
// ---------------------------------------------------------------------------

/// Transition this node from Follower → Candidate and broadcast
/// `RequestVote` RPCs to all peers.
///
/// Raft paper: §5.2, paragraph 3; Figure 2, "Rules for Servers", "Candidates" bullet 1.
///
/// Steps (per Raft §5.2):
///   1. Lock state, increment `current_term`, set role to Candidate,
///      vote for self (`voted_for = Some(self.id)`).
///   2. Persist the updated term and vote.
///   3. Record the term of this election attempt (to detect stale replies).
///   4. Release the lock, then fan-out `RequestVote` RPCs concurrently to all
///      peers (one `tokio::task::spawn` per peer).
///   5. Collect replies via a channel and pass each to
///      `handle_vote_response`.
///
/// Must **not** hold the state lock while awaiting RPCs.
pub async fn start_election<C, T>(
    state: Arc<Mutex<RaftState>>,
    log: Arc<Mutex<RaftLog<C>>>,
    transport: Arc<T>,
    storage: Arc<RaftStorage>,
    timer: &ElectionTimer,
    vote_result_tx: mpsc::Sender<(NodeId, Term, RequestVoteReply)>,
) -> Result<(), RaftError>
where
    C: Clone
        + Send
        + Sync
        + std::fmt::Debug
        + serde::Serialize
        + for<'de> serde::Deserialize<'de>
        + 'static,
    T: RaftTransport<C>,
{
    // 1–3. Lock state + log, transition to Candidate, snapshot, release.
    let (election_term, candidate_id, peers, last_log_index, last_log_term): (
        Term,
        NodeId,
        Vec<NodeId>,
        _,
        _,
    ) = {
        let mut state_guard = state.lock().await;
        let log_guard = log.lock().await;

        state_guard.persistent.current_term += 1;
        state_guard.role = Role::Candidate;
        state_guard.persistent.voted_for = Some(state_guard.id.clone());
        state_guard.leader_state = None;
        state_guard.volatile.current_leader = None;

        // Persist term + vote before releasing the lock.
        storage
            .save_persistent_state(&state_guard.persistent)
            .expect("failed to persist state during election start");

        let snapshot = (
            state_guard.persistent.current_term,
            state_guard.id.clone(),
            state_guard.peers.clone(),
            log_guard.last_index(),
            log_guard.last_term(),
        );

        // Reset election timer so we don't immediately time out again.
        timer.reset();

        snapshot
        // Both locks released here.
    };

    info!("starting election for term {election_term}");

    // 4. Fan-out RequestVote RPCs — one spawned task per peer.
    for peer in peers {
        let transport = Arc::clone(&transport);
        let tx = vote_result_tx.clone();
        let candidate_id = candidate_id.clone();

        tokio::spawn(async move {
            let args = RequestVoteArgs {
                term: election_term,
                candidate_id,
                last_log_index,
                last_log_term,
            };
            match transport.send_request_vote(&peer, args).await {
                Ok(reply) => {
                    let _ = tx.send((peer.clone(), election_term, reply)).await;
                }
                Err(e) => {
                    debug!("RequestVote to {peer} failed: {e}");
                }
            }
        });
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Handling vote responses
// ---------------------------------------------------------------------------

/// Process a single `RequestVoteReply` received from `peer`.
///
/// Raft paper: §5.2, paragraph 3; Figure 2, "Rules for Servers", "Candidates" bullet 2.
///
/// Returns `true` when the node has just won the election (gathered a
/// majority of votes for `election_term`), indicating the caller should call
/// `become_leader`.
///
/// Steps:
///   1. Lock state.
///   2. If `reply.term > current_term`, call `step_down_to_follower` and
///      return `false` (stale election).
///   3. If our role is no longer Candidate or `election_term != current_term`,
///      ignore (stale reply from a previous election) and return `false`.
///   4. If `reply.vote_granted`, increment vote tally.
///   5. If tally > (cluster_size / 2), return `true`.
///   6. Otherwise return `false`.
///
/// The `votes_received` counter lives outside the lock — it is a local
/// variable in the task that collects replies — so no lock contention occurs
/// on the hot path.
pub async fn handle_vote_response(
    state: Arc<Mutex<RaftState>>,
    storage: Arc<RaftStorage>,
    _peer: NodeId,
    election_term: Term,
    reply: RequestVoteReply,
    votes_received: &mut usize,
    cluster_size: usize,
) -> bool {
    let mut state_guard = state.lock().await;

    // Step down if the responder has a higher term.
    if reply.term > state_guard.persistent.current_term {
        state_guard.step_down_to_follower(&storage, reply.term);
        return false;
    }

    // Stale reply — we moved on from this election.
    if state_guard.role != Role::Candidate || state_guard.persistent.current_term != election_term {
        return false;
    }

    if reply.vote_granted {
        *votes_received += 1;
    }

    // Lock is dropped at end of scope — no lock held across any await.
    *votes_received > cluster_size / 2
}

// ---------------------------------------------------------------------------
// Becoming leader
// ---------------------------------------------------------------------------

/// Transition the node from Candidate → Leader after winning the election.
///
/// Raft paper: §5.2, paragraph 4; §5.4.2 (no-op commit); Figure 2, "Rules for Servers", "Leaders" bullet 1.
///
/// Steps (per Raft §5.2 and the Leader Completeness rule):
///   1. Lock state.
///   2. Double-check we are still Candidate in the expected term (guards
///      against a race where another leader sent a higher-term message).
///   3. Set `role = Leader`, record `volatile.current_leader = Some(self.id)`.
///   4. Initialise `leader_state` via `LeaderState::initialise` with the
///      current last log index.
///   5. Append a no-op log entry in the current term so the leader can
///      commit entries from previous terms (Leader Completeness §5.4).
///   6. Release the lock, then immediately broadcast a heartbeat
///      (`AppendEntries` with empty entries) so followers reset their timers.
pub async fn become_leader<C, T>(
    state: Arc<Mutex<RaftState>>,
    log: Arc<Mutex<RaftLog<C>>>,
    transport: Arc<T>,
    storage: Arc<RaftStorage>,
) -> Result<(), RaftError>
where
    C: Clone
        + Send
        + Sync
        + Default
        + serde::Serialize
        + for<'de> serde::Deserialize<'de>
        + std::fmt::Debug
        + 'static,
    T: RaftTransport<C>,
{
    // Lock state then log (consistent ordering with all other call sites).
    {
        let mut state_guard = state.lock().await;
        let mut log_guard = log.lock().await;

        if state_guard.role != Role::Candidate {
            return Err(RaftError::Internal(
                "become_leader called but no longer Candidate".into(),
            ));
        }

        let term = state_guard.persistent.current_term;

        // Transition to Leader.
        state_guard.role = Role::Leader;
        state_guard.volatile.current_leader = Some(state_guard.id.clone());

        // Initialise per-peer replication tracking.
        let last_log_index = log_guard.last_index();
        state_guard.leader_state =
            Some(LeaderState::initialise(&state_guard.peers, last_log_index));

        // Append a no-op entry so previous-term entries can be committed.
        log_guard.append_command(term, C::default());

        info!("became leader for term {term}");
        // Both locks released here.
    }

    // Broadcast heartbeat — no locks held.
    super::replication::broadcast_heartbeat(state, log, transport, storage).await;

    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Pick a random election timeout duration within the configured range.
pub fn random_timeout(config: &ElectionConfig) -> Duration {
    let min_ms = config.timeout_min.as_millis() as u64;
    let max_ms = config.timeout_max.as_millis() as u64;
    let ms = rand::rng().random_range(min_ms..max_ms);
    Duration::from_millis(ms)
}
