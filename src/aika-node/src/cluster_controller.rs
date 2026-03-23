use crate::common::*;
use std::collections::HashMap;
use std::net::SocketAddr;

pub struct ClusterControllerConfig {
    pub node_id: u64,
    pub bind: SocketAddr,
    pub peers: Vec<String>,
    pub task_ttl_secs: u64,
}

/// Holds the replicated state derived from applying Raft log entries.
/// This is your Raft state machine.
struct StateMachine {
    /// All task batches keyed by batch_id.
    tasks: HashMap<u64, TaskBatch>,
    /// Registered nodes (local controllers) keyed by node_id.
    nodes: HashMap<String, NodeInfo>,
    /// Monotonically increasing counter for assigning new batch IDs.
    next_batch_id: u64,
}

impl StateMachine {
    fn new() -> Self {
        StateMachine {
            tasks: HashMap::new(),
            nodes: HashMap::new(),
            next_batch_id: 0,
        }
    }

    /// Apply a committed Command to the state machine.
    /// Called by the Raft layer after a log entry is committed.
    fn apply(&mut self, command: Command) {
        match command {
            Command::NoOp => {} // leader no-op entry — nothing to apply

            Command::AddTaskBatch {
                batch_id,
                image_paths,
            } => {
                todo!("Insert new TaskBatch with status Pending")
            }
            Command::AssignTask {
                batch_id,
                agent_id,
                assigned_at,
            } => {
                todo!("Change batch status from Pending to Assigned")
            }
            Command::CompleteTask { batch_id, labels } => {
                todo!("Change batch status to Completed, store labels")
            }
            Command::ExpireTask { batch_id } => {
                todo!("If batch is Assigned, reset to Pending")
            }
            Command::RegisterNode { node_id, address } => {
                todo!("Add or update node in registry")
            }
            Command::DeregisterNode { node_id } => {
                todo!("Remove node from registry")
            }
        }
    }

    /// Find the next pending batch and return it, or None if all work is done.
    fn next_pending_batch(&self) -> Option<&TaskBatch> {
        todo!("Return first batch with status == Pending")
    }

    /// Collect all batches whose TTL has expired.
    fn expired_batches(&self, now: u64, ttl_secs: u64) -> Vec<u64> {
        todo!("Return batch_ids where status is Assigned and now - assigned_at > ttl_secs")
    }

    /// Build a ClusterStatus summary.
    fn status(&self) -> ClusterStatus {
        todo!()
    }
}

/// Main entry point for the cluster controller.
pub async fn run(config: ClusterControllerConfig) -> anyhow::Result<()> {
    tracing::info!(
        "Starting cluster controller {} on {}",
        config.node_id,
        config.bind
    );

    // TODO: Initialize your Raft node with config.node_id and config.peers
    // TODO: Initialize the StateMachine
    // TODO: Start the TTL reaper background task
    // TODO: Start the HTTP server with the routes below
    // TODO: If elected leader, ingest tasks from the image directory

    start_http_server(config).await
}

// ---------------------------------------------------------------------------
// HTTP server and route handlers
// ---------------------------------------------------------------------------

async fn start_http_server(config: ClusterControllerConfig) -> anyhow::Result<()> {
    // TODO: Set up axum/actix/warp router with the routes below, bind to config.bind
    // Routes:
    //   POST /task/request   -> handle_task_request
    //   POST /task/complete   -> handle_task_complete
    //   POST /heartbeat       -> handle_heartbeat
    //   GET  /leader          -> handle_leader_query
    //   GET  /status          -> handle_status
    //
    // The router needs shared access to the Raft node and StateMachine,
    // typically via Arc<Mutex<...>> or Arc<RwLock<...>>

    todo!()
}

/// POST /task/request
///
/// Called by agents (via local controller) to get a batch of work.
/// Only the Raft leader can assign tasks. If this node is not the leader,
/// return a redirect to the leader's address.
async fn handle_task_request(
    // state: extract shared Raft + StateMachine
    request: TaskRequest,
) -> Result<TaskAssignment, ApiError> {
    // 1. Check if we are the Raft leader. If not, return leader redirect.
    // 2. Find next pending batch from StateMachine.
    // 3. Propose an AssignTask command through Raft.
    // 4. Wait for commit.
    // 5. Return the TaskAssignment with batch_id and image_paths.
    todo!()
}

/// POST /task/complete
///
/// Called by agents to report completed work. Idempotent — completing an
/// already-completed batch is a no-op success.
async fn handle_task_complete(
    // state: extract shared Raft + StateMachine
    request: TaskCompletion,
) -> Result<TaskCompletionResponse, ApiError> {
    // 1. Check if we are the Raft leader. If not, redirect.
    // 2. Check batch status:
    //    - Already Completed -> return accepted (idempotent)
    //    - Assigned to this agent -> propose CompleteTask via Raft
    //    - Pending (TTL expired and reassigned) -> still accept if valid
    // 3. Wait for commit.
    // 4. Return response.
    todo!()
}

/// POST /heartbeat
///
/// Called periodically by local controllers to report liveness.
async fn handle_heartbeat(
    // state: extract shared Raft + StateMachine
    request: HeartbeatRequest,
) -> Result<HeartbeatResponse, ApiError> {
    // 1. Update last_heartbeat and agent list for this node in state.
    //    (This can be done locally without Raft if you treat heartbeats
    //     as ephemeral state, or propose a RegisterNode command if you
    //     want it replicated.)
    // 2. Return acknowledgment.
    todo!()
}

/// GET /leader
///
/// Returns the current Raft leader's address so clients can find it.
async fn handle_leader_query(// state: extract shared Raft
) -> LeaderResponse {
    // 1. Ask Raft layer who the current leader is.
    // 2. Return leader_id and leader_address.
    todo!()
}

/// GET /status
///
/// Returns a summary of task progress and node health.
async fn handle_status(// state: extract shared StateMachine
) -> ClusterStatus {
    // 1. Call state_machine.status()
    todo!()
}

// ---------------------------------------------------------------------------
// Background tasks
// ---------------------------------------------------------------------------

/// Periodically scans for assigned tasks whose TTL has expired and
/// proposes ExpireTask commands to return them to Pending.
async fn ttl_reaper_loop(
    // raft: shared Raft handle,
    // state_machine: shared StateMachine,
    ttl_secs: u64,
) {
    // loop {
    //     sleep(Duration::from_secs(ttl_secs / 2)).await;
    //     if !raft.is_leader() { continue; }
    //     let now = current_unix_timestamp();
    //     let expired = state_machine.expired_batches(now, ttl_secs);
    //     for batch_id in expired {
    //         raft.propose(Command::ExpireTask { batch_id }).await;
    //     }
    // }
    todo!()
}

/// Scans the image directory and ingests tasks in batches.
/// Only runs on the leader, and only once (or checks for already-ingested).
async fn ingest_image_tasks(
    // raft: shared Raft handle,
    image_dir: &str,
    batch_size: usize,
) {
    // 1. Read all filenames from image_dir
    // 2. Chunk into batches of batch_size
    // 3. For each chunk, propose AddTaskBatch command via Raft
    // 4. Track progress so we don't re-ingest on leader re-election
    //    (check state machine for existing batches)
    todo!()
}

// ---------------------------------------------------------------------------
// Error type for HTTP handlers
// ---------------------------------------------------------------------------

#[derive(Debug)]
enum ApiError {
    NotLeader { leader_address: Option<String> },
    NoWorkAvailable,
    Internal(String),
}

// TODO: Implement Into<HttpResponse> or axum's IntoResponse for ApiError
