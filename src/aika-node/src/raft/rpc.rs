/// RPC message types and the network transport abstraction.
///
/// Raft requires two RPCs:
///   - **RequestVote** — sent by candidates during elections.
///   - **AppendEntries** — sent by the leader both for log replication and as
///     a periodic heartbeat (empty `entries` list).
///
/// The `RaftTransport` trait decouples the protocol logic from whatever
/// network mechanism is used (HTTP, gRPC, raw TCP, …).  The implementation
/// of this trait lives outside the `raft` module.
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use super::{
    log::LogEntry,
    state::{LogIndex, NodeId, Term},
};

// region RequestVote

/// Arguments sent by a candidate to every other node to solicit a vote.
///
/// Raft paper: Figure 2, "RequestVote RPC", "Arguments".
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteArgs {
    /// Candidate's current term.
    pub term: Term,

    /// Candidate's own node ID.
    pub candidate_id: NodeId,

    /// Index of the candidate's last log entry.
    pub last_log_index: LogIndex,

    /// Term of the candidate's last log entry.
    pub last_log_term: Term,
}

/// Reply from a voter to a `RequestVote` RPC.
///
/// Raft paper: Figure 2, "RequestVote RPC", "Results".
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteReply {
    /// The voter's current term (so the candidate can step down if stale).
    pub term: Term,

    /// `true` if the voter granted its vote to the candidate.
    pub vote_granted: bool,
}

// endregion

// region AppendEntries

/// Arguments sent by the leader for both log replication and heartbeats.
///
/// When `entries` is empty this is a heartbeat; followers reset their
/// election timer without needing to append anything.
///
/// Raft paper: Figure 2, "AppendEntries RPC", "Arguments".
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesArgs<C> {
    /// Leader's current term.
    pub term: Term,

    /// Leader's own node ID (so followers can redirect clients).
    pub leader_id: NodeId,

    /// Index of the log entry immediately preceding the new entries.
    pub prev_log_index: LogIndex,

    /// Term of the entry at `prev_log_index` (0 if `prev_log_index == 0`).
    pub prev_log_term: Term,

    /// New entries to append (empty for a heartbeat).
    pub entries: Vec<LogEntry<C>>,

    /// Leader's `commit_index`; followers advance their own commit index to
    /// `min(leader_commit, last new entry index)`.
    pub leader_commit: LogIndex,
}

/// Reply from a follower to an `AppendEntries` RPC.
///
/// Raft paper: Figure 2, "AppendEntries RPC", "Results".
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesReply {
    /// The follower's current term (so the leader can step down if stale).
    pub term: Term,

    /// `true` if the follower successfully appended the entries.
    /// `false` if the log consistency check failed (leader should decrement
    /// `nextIndex` for this peer and retry).
    pub success: bool,

    /// Optional optimisation: if `success == false`, the follower reports the
    /// first index of the conflicting term so the leader can skip back an
    /// entire term at once rather than decrementing one by one.
    pub conflict_index: Option<LogIndex>,

    /// The term of the conflicting entry (paired with `conflict_index`).
    pub conflict_term: Option<Term>,
}

// endregion

// region RaftTransport

/// Errors that can occur when sending an RPC.
#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    #[error("peer {0} is unreachable")]
    Unreachable(NodeId),

    #[error("RPC timed out")]
    Timeout,

    #[error("transport error: {0}")]
    Other(String),
}

/// Abstraction over the network layer used to send Raft RPCs to peers.
///
/// The generic parameter `C` is the application command type, which must be
/// `Send + Sync` so it can cross async task boundaries.
///
/// Implementors handle connection management, retries, and serialization.
/// The Raft core calls these methods and awaits the reply; if no reply
/// arrives within a reasonable deadline the implementation should return
/// `Err(TransportError::Timeout)`.
#[async_trait]
pub trait RaftTransport<C>: Send + Sync + 'static
where
    C: Clone + Send + Sync + Serialize + for<'de> Deserialize<'de> + 'static,
{
    /// Send a `RequestVote` RPC to `peer` and await the reply.
    async fn send_request_vote(
        &self,
        peer: &NodeId,
        args: RequestVoteArgs,
    ) -> Result<RequestVoteReply, TransportError>;

    /// Send an `AppendEntries` RPC to `peer` and await the reply.
    async fn send_append_entries(
        &self,
        peer: &NodeId,
        args: AppendEntriesArgs<C>,
    ) -> Result<AppendEntriesReply, TransportError>;
}

// endregion
