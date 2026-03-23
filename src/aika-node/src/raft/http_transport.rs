/// HTTP implementation of `RaftTransport`.
///
/// Sends `RequestVote` and `AppendEntries` RPCs to peer nodes over HTTP,
/// serializing arguments as JSON and deserializing replies.
///
/// ### Partition simulation (for testing)
/// The transport checks `blocked_peers` before every send.  The test harness
/// configures this set via the `POST /debug/block_peer` endpoint, which writes
/// to the same `Arc<Mutex<HashSet<String>>>` injected here at startup.  This
/// gives the Python integration tests full partition control without a proxy.
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tracing::warn;

use super::{
    rpc::{
        AppendEntriesArgs, AppendEntriesReply, RaftTransport, RequestVoteArgs, RequestVoteReply,
        TransportError,
    },
    state::NodeId,
};

// region HttpTransport

/// Sends Raft RPCs to peers over HTTP using `reqwest`.
///
/// `peer_urls` maps each peer's `NodeId` to its base HTTP URL
/// (e.g. `"http://localhost:8001"`).
pub struct HttpTransport<C> {
    /// This node's own ID (used for logging).
    own_id: NodeId,

    /// Map of peer_id → base URL.
    peer_urls: HashMap<NodeId, String>,

    /// Peers whose outgoing RPCs are currently blocked (partition simulation).
    blocked_peers: Arc<Mutex<HashSet<NodeId>>>,

    /// How long to wait for a response before returning `TransportError::Timeout`.
    rpc_timeout: Duration,

    /// Underlying HTTP client (reused across calls for connection pooling).
    client: reqwest::Client,

    /// PhantomData so the struct is generic over the command type without
    /// storing a value of type C directly.
    _phantom: std::marker::PhantomData<C>,
}

impl<C> HttpTransport<C>
where
    C: Clone + Send + Sync + Serialize + for<'de> Deserialize<'de> + 'static,
{
    /// Create a new transport.
    ///
    /// `own_id`        — this node's identifier (for logs).
    /// `peer_urls`     — map of every other node's ID → base HTTP URL.
    /// `blocked_peers` — shared set controlled by the debug API.
    /// `rpc_timeout`   — per-RPC deadline.
    pub fn new(
        own_id: NodeId,
        peer_urls: HashMap<NodeId, String>,
        blocked_peers: Arc<Mutex<HashSet<NodeId>>>,
        rpc_timeout: Duration,
    ) -> Self {
        let client = reqwest::Client::builder()
            .timeout(rpc_timeout)
            .build()
            .expect("failed to build reqwest client");

        HttpTransport {
            own_id,
            peer_urls,
            blocked_peers,
            rpc_timeout,
            client,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Resolve `peer` to its base URL, returning `TransportError::Unreachable`
    /// if the peer is unknown or currently blocked.
    async fn resolve_url(&self, peer: &NodeId) -> Result<String, TransportError> {
        // Check the partition / block table first.
        if self.blocked_peers.lock().await.contains(peer) {
            return Err(TransportError::Unreachable(peer.clone()));
        }

        self.peer_urls
            .get(peer)
            .cloned()
            .ok_or_else(|| TransportError::Unreachable(peer.clone()))
    }

    /// Map a `reqwest::Error` to a `TransportError`.
    fn map_err(peer: &NodeId, e: reqwest::Error) -> TransportError {
        if e.is_timeout() {
            TransportError::Timeout
        } else {
            TransportError::Other(format!("HTTP error to {peer}: {e}"))
        }
    }
}

// endregion

// region RaftTransport

#[async_trait]
impl<C> RaftTransport<C> for HttpTransport<C>
where
    C: Clone + Send + Sync + Serialize + for<'de> Deserialize<'de> + 'static,
{
    /// POST `args` as JSON to `{peer_url}/raft/request_vote` and return the reply.
    async fn send_request_vote(
        &self,
        peer: &NodeId,
        args: RequestVoteArgs,
    ) -> Result<RequestVoteReply, TransportError> {
        let url = self.resolve_url(peer).await?;
        let endpoint = format!("{url}/raft/request_vote");

        self.client
            .post(&endpoint)
            .json(&args)
            .send()
            .await
            .map_err(|e| Self::map_err(peer, e))?
            .json::<RequestVoteReply>()
            .await
            .map_err(|e| {
                warn!(peer = %peer, err = %e, "failed to parse RequestVoteReply");
                TransportError::Other(e.to_string())
            })
    }

    /// POST `args` as JSON to `{peer_url}/raft/append_entries` and return the reply.
    async fn send_append_entries(
        &self,
        peer: &NodeId,
        args: AppendEntriesArgs<C>,
    ) -> Result<AppendEntriesReply, TransportError> {
        let url = self.resolve_url(peer).await?;
        let endpoint = format!("{url}/raft/append_entries");

        self.client
            .post(&endpoint)
            .json(&args)
            .send()
            .await
            .map_err(|e| Self::map_err(peer, e))?
            .json::<AppendEntriesReply>()
            .await
            .map_err(|e| {
                warn!(peer = %peer, err = %e, "failed to parse AppendEntriesReply");
                TransportError::Other(e.to_string())
            })
    }
}

// endregion
