use crate::common::*;
use std::collections::HashMap;
use std::net::SocketAddr;

pub struct LocalControllerConfig {
    pub node_id: String,
    pub bind: SocketAddr,
    pub cc_addrs: Vec<String>,
    pub agent_count: usize,
    pub health_check_interval: u64,
}

/// Tracks the state of a locally managed agent process.
struct ManagedAgent {
    agent_id: String,
    /// Handle to the spawned child process; used to check liveness and kill on shutdown.
    process: std::process::Child,
    /// Unix timestamp of the last heartbeat received from this agent.
    last_heartbeat: u64,
    /// The batch this agent is currently processing, if any.
    current_batch_id: Option<u64>,
}

/// The local controller's runtime state.
struct LocalControllerState {
    config: LocalControllerConfig,
    agents: HashMap<String, ManagedAgent>,
    /// Cached address of the current Raft leader (updated on redirects)
    current_leader: Option<String>,
}

impl LocalControllerState {
    fn new(config: LocalControllerConfig) -> Self {
        todo!()
    }

    /// Resolve the current cluster controller leader address.
    /// Tries cached leader first, then queries each CC address.
    async fn find_leader(&mut self) -> anyhow::Result<String> {
        // 1. If current_leader is Some, return it.
        // 2. Otherwise, iterate cc_addrs and GET /leader on each.
        // 3. Cache and return the leader address.
        // 4. If no leader found, return error.
        todo!()
    }

    /// Forward a task request to the cluster controller on behalf of an agent.
    async fn request_task_for_agent(&mut self, agent_id: &str) -> anyhow::Result<TaskAssignment> {
        // 1. Find leader address.
        // 2. POST /task/request with TaskRequest { agent_id }.
        // 3. If response is a redirect (NotLeader), update cached leader and retry.
        // 4. Return the TaskAssignment.
        todo!()
    }

    /// Forward a task completion to the cluster controller.
    async fn complete_task(
        &mut self,
        completion: TaskCompletion,
    ) -> anyhow::Result<TaskCompletionResponse> {
        // 1. Find leader.
        // 2. POST /task/complete with the completion payload.
        // 3. Handle redirects.
        // 4. Return response.
        todo!()
    }

    /// Send a heartbeat to the cluster controller.
    async fn send_heartbeat(&mut self) -> anyhow::Result<()> {
        // 1. Build HeartbeatRequest with node_id, agent_ids, load info.
        // 2. POST /heartbeat to the leader.
        // 3. Handle errors gracefully (leader might be unavailable temporarily).
        todo!()
    }
}

/// Main entry point for the local controller.
pub async fn run(config: LocalControllerConfig) -> anyhow::Result<()> {
    tracing::info!(
        "Starting local controller {} on {} with {} agents",
        config.node_id,
        config.bind,
        config.agent_count
    );

    let agent_count = config.agent_count;
    let health_interval = config.health_check_interval;

    // TODO: Initialize LocalControllerState
    // TODO: Spawn initial agent processes (if agent_count > 0)
    // TODO: Start background loops:
    //       - Agent health checker
    //       - Heartbeat sender to cluster controller
    // TODO: Start HTTP server for agent-facing endpoints

    if agent_count == 0 {
        tracing::info!("Running in replica mode (no agents) — standing by for failover");
    }

    todo!()
}

// ---------------------------------------------------------------------------
// Agent process lifecycle
// ---------------------------------------------------------------------------

/// Spawn a new agent as a child process on this node.
fn spawn_agent(
    agent_id: &str,
    lc_addr: &str,
    cc_addrs: &[String],
    extractor_script: &str,
    image_base_path: &str,
) -> anyhow::Result<ManagedAgent> {
    // 1. Build the command:
    //    inf3203_aika agent --agent-id <id> --lc-addr <addr> --cc-addrs <addrs>
    // 2. Spawn as a child process.
    // 3. Return ManagedAgent with the child handle.
    //
    // Use std::process::Command or tokio::process::Command
    // Make sure to use the same binary path (std::env::current_exe())
    todo!()
}

/// Check if an agent process is still alive and restart it if not.
async fn check_and_recover_agent(
    // state: &mut LocalControllerState,
    agent_id: &str,
) {
    // 1. Check if the child process is still running (try_wait).
    // 2. If it exited, log the crash.
    // 3. Respawn the agent with the same agent_id.
    // 4. The agent will request new work on startup — any in-flight task
    //    will expire via TTL and be reassigned.
    todo!()
}

// ---------------------------------------------------------------------------
// Background loops
// ---------------------------------------------------------------------------

/// Periodically checks that all managed agents are alive.
async fn agent_health_loop(
    // state: shared LocalControllerState,
    interval_secs: u64,
) {
    // loop {
    //     sleep(Duration::from_secs(interval_secs)).await;
    //     for each agent in state.agents:
    //         check_and_recover_agent(agent_id).await;
    // }
    todo!()
}

/// Periodically sends heartbeats to the cluster controller.
async fn heartbeat_loop(// state: shared LocalControllerState,
) {
    // loop {
    //     sleep(Duration::from_secs(10)).await;
    //     state.send_heartbeat().await;
    // }
    todo!()
}

// ---------------------------------------------------------------------------
// HTTP server — endpoints for agents to communicate through
// ---------------------------------------------------------------------------

async fn start_http_server(
    // state: shared LocalControllerState,
    bind: SocketAddr,
) -> anyhow::Result<()> {
    // Routes:
    //   POST /agent/heartbeat    -> handle_agent_heartbeat
    //   POST /agent/request_task -> handle_agent_task_request
    //   POST /agent/complete     -> handle_agent_task_complete
    todo!()
}

/// POST /agent/heartbeat
///
/// Agents call this periodically to report they're alive.
async fn handle_agent_heartbeat(
    // state: extract shared state,
    heartbeat: AgentHeartbeat,
) {
    // 1. Update the agent's last_heartbeat timestamp in state.
    // 2. Update current_batch_id if provided.
    todo!()
}

/// POST /agent/request_task
///
/// Agent asks for work. Local controller proxies to cluster controller.
async fn handle_agent_task_request(
    // state: extract shared state,
    request: TaskRequest,
) -> Result<TaskAssignment, ()> {
    // 1. Proxy to state.request_task_for_agent(request.agent_id).
    // 2. Return the assignment or an error.
    todo!()
}

/// POST /agent/complete
///
/// Agent reports completion. Local controller proxies to cluster controller.
async fn handle_agent_task_complete(
    // state: extract shared state,
    completion: TaskCompletion,
) -> Result<TaskCompletionResponse, ()> {
    // 1. Proxy to state.complete_task(completion).
    // 2. Return the response.
    todo!()
}
