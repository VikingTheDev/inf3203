use crate::raft::storage::RaftStorage;
/// Core state types shared across the Raft module.
///
/// Raft separates state into three categories:
///   - **Persistent** – must be written to stable storage before any RPC reply so it survives
///     crashes (currentTerm, votedFor).
///   - **Volatile (all servers)** – re-derived after a restart (commitIndex, lastApplied).
///   - **Volatile (leader only)** – re-initialized after every election (nextIndex, matchIndex).
use std::collections::HashMap;

/// Unique identifier for a node in the cluster.
pub type NodeId = String;

/// Raft term number. Increases monotonically, acts as a logical clock.
pub type Term = u64;

/// 1-based position of an entry in the replicated log.
/// Index 0 is reserved as a sentinel "no entry" value.
pub type LogIndex = u64;

// region Role

/// The role a Raft node currently occupies in the protocol.
#[derive(Debug, Clone, PartialEq, Eq)]
#[derive(Default)]
pub enum Role {
    /// Default starting role.
    #[default]
    Follower,

    /// Temporarily held while gathering votes.
    Candidate,

    /// Current leader of the quorum.
    Leader,
}


// endregion

// region State

/// Fields that **must** be persisted to stable storage before responding to any RPC.
/// On recovery the node reloads these from disk.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[derive(Default)]
pub struct PersistentState {
    /// Latest term the server has seen.  Initialised to 0 and only ever increases.
    pub current_term: Term,

    /// `NodeId` of the candidate this server voted for in `current_term`,
    /// or `None` if it has not voted yet.
    pub voted_for: Option<NodeId>,
}


/// Fields kept in memory on every node. Safe to re-derive after a crash.
#[derive(Debug, Clone, Default)]
pub struct VolatileState {
    /// Index of the highest log entry known to be committed (replicated on a majority).
    /// Initialised to 0 and only increases.
    pub commit_index: LogIndex,

    /// Index of the highest log entry applied to the local state machine.
    /// Always `<= commit_index`.
    pub last_applied: LogIndex,

    /// `NodeId` of the current leader, if known.  Used to redirect clients.
    pub current_leader: Option<NodeId>,
}

/// Per-peer replication tracking, maintained only while this node is leader.
/// Re-initialized to defaults immediately after winning an election.
#[derive(Debug, Clone)]
pub struct PeerReplicationState {
    /// Index of the next log entry to send to this peer.
    /// Initialised to `leader_last_log_index + 1` after election.
    pub next_index: LogIndex,

    /// Index of the highest log entry known to be replicated on this peer.
    /// Initialised to 0 after election.
    pub match_index: LogIndex,
}

impl PeerReplicationState {
    /// Create a fresh entry for a newly elected leader whose log ends at
    /// `last_log_index`.
    pub fn new_after_election(last_log_index: LogIndex) -> Self {
        PeerReplicationState {
            next_index: last_log_index + 1,
            match_index: 0,
        }
    }
}

/// Aggregated leader-only state across all peers.
#[derive(Debug, Clone, Default)]
pub struct LeaderState {
    /// Keyed by peer `NodeId`.
    pub peers: HashMap<NodeId, PeerReplicationState>,
}

impl LeaderState {
    /// Initialize per-peer entries immediately after winning an election.
    ///
    /// `peers` — the set of all other node IDs in the cluster.
    /// `last_log_index` — the index of the last entry in the leader's own log.
    pub fn initialise(peers: &[NodeId], last_log_index: LogIndex) -> Self {
        let map = peers
            .iter()
            .map(|id| {
                (
                    id.clone(),
                    PeerReplicationState::new_after_election(last_log_index),
                )
            })
            .collect();
        LeaderState { peers: map }
    }
}

// endregion

// region RaftState

/// All Raft state for one server.
///
/// Callers hold a `Mutex` wrapped in an `Arc` so the state can be shared safely across threads.
#[derive(Debug)]
pub struct RaftState {
    /// The node's identifier in the cluster.
    pub id: NodeId,

    /// All peer node IDs (excluding self).
    pub peers: Vec<NodeId>,

    pub persistent: PersistentState,

    pub volatile: VolatileState,

    /// `Some` only when role is `Leader`
    pub leader_state: Option<LeaderState>,

    /// Current role in the Raft protocol.
    pub role: Role,
}

impl RaftState {
    /// Construct initial state for a fresh node.
    pub fn new(id: NodeId, peers: Vec<NodeId>) -> Self {
        RaftState {
            id,
            peers,
            persistent: PersistentState::default(),
            volatile: VolatileState::default(),
            leader_state: None,
            role: Role::Follower,
        }
    }

    /// Transition to a higher term, resetting vote and reverting to Follower.
    ///
    /// Must be called (and state persisted) whenever an incoming message
    /// carries a term greater than `current_term`.
    pub fn step_down_to_follower(&mut self, storage: &RaftStorage, new_term: Term) {
        tracing::info!(
            "stepping down to follower: term {} → {} (was {:?})",
            self.persistent.current_term,
            new_term,
            self.role,
        );
        // Set new terms
        self.persistent.current_term = new_term;
        // Clear vote
        self.persistent.voted_for = None;
        // Revert to follower role
        self.role = Role::Follower;
        // Clear leader state
        self.leader_state = None;
        // Persist state to disk
        storage
            .save_persistent_state(&self.persistent)
            .expect("Failed to persist state on step down to follower");
    }

    /// Convenience: true when this node believes it is the cluster leader.
    pub fn is_leader(&self) -> bool {
        self.role == Role::Leader
    }
}

// endregion
