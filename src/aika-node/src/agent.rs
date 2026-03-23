use crate::common::*;

pub struct AgentConfig {
    pub agent_id: String,
    pub lc_addr: String,
    pub cc_addrs: Vec<String>,
    pub extractor_script: String,
    pub image_base_path: String,
}

/// Main entry point for the agent worker.
pub async fn run(config: AgentConfig) -> anyhow::Result<()> {
    tracing::info!(
        "Starting agent {} (local controller: {})",
        config.agent_id,
        config.lc_addr
    );

    // TODO: Start heartbeat background task to local controller
    // TODO: Enter the main work loop

    work_loop(&config).await
}

// ---------------------------------------------------------------------------
// Main work loop
// ---------------------------------------------------------------------------

/// Core loop: request work, process it, report results, repeat.
async fn work_loop(config: &AgentConfig) -> anyhow::Result<()> {
    loop {
        // 1. Request a task batch
        let assignment = match request_task(config).await {
            Ok(assignment) => assignment,
            Err(e) => {
                tracing::warn!("No work available or error requesting task: {}", e);
                // Back off before retrying
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                continue;
            }
        };

        tracing::info!(
            "Agent {} received batch {} with {} images",
            config.agent_id,
            assignment.batch_id,
            assignment.image_paths.len()
        );

        // 2. Process each image in the batch
        let labels = process_batch(config, &assignment).await?;

        // 3. Report completion
        let completion = TaskCompletion {
            batch_id: assignment.batch_id,
            agent_id: config.agent_id.clone(),
            labels,
        };

        match report_completion(config, completion).await {
            Ok(response) => {
                tracing::info!(
                    "Batch {} completion {}: {}",
                    assignment.batch_id,
                    if response.accepted {
                        "accepted"
                    } else {
                        "rejected"
                    },
                    response.message
                );
            }
            Err(e) => {
                // Not fatal — the batch will either be accepted on retry
                // or expire and be reassigned.
                tracing::error!(
                    "Failed to report completion for batch {}: {}",
                    assignment.batch_id,
                    e
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Communication with local controller / cluster controller
// ---------------------------------------------------------------------------

/// Request a task batch from the local controller (which proxies to CC).
async fn request_task(config: &AgentConfig) -> anyhow::Result<TaskAssignment> {
    // POST to http://{lc_addr}/agent/request_task
    // Body: TaskRequest { agent_id }
    // Parse response as TaskAssignment
    //
    // If local controller is unreachable, optionally fall back to
    // contacting cluster controller directly via cc_addrs.
    todo!()
}

/// Report task completion to the local controller (which proxies to CC).
async fn report_completion(
    config: &AgentConfig,
    completion: TaskCompletion,
) -> anyhow::Result<TaskCompletionResponse> {
    // POST to http://{lc_addr}/agent/complete
    // Body: TaskCompletion
    // Parse response as TaskCompletionResponse
    //
    // Retry with backoff on transient failures.
    todo!()
}

/// Send a heartbeat to the local controller.
async fn send_heartbeat(config: &AgentConfig, current_batch_id: Option<u64>) -> anyhow::Result<()> {
    // POST to http://{lc_addr}/agent/heartbeat
    // Body: AgentHeartbeat { agent_id, current_batch_id }
    todo!()
}

// ---------------------------------------------------------------------------
// Feature extraction
// ---------------------------------------------------------------------------

/// Process all images in a batch by running the feature extractor on each.
/// Returns a list of (image_path, label) pairs.
async fn process_batch(
    config: &AgentConfig,
    assignment: &TaskAssignment,
) -> anyhow::Result<Vec<(String, String)>> {
    let mut results = Vec::with_capacity(assignment.image_paths.len());

    for image_path in &assignment.image_paths {
        let full_path = format!("{}/{}", config.image_base_path, image_path);
        let label = run_feature_extractor(&config.extractor_script, &full_path).await?;
        results.push((image_path.clone(), label));
    }

    Ok(results)
}

/// Run the provided Python feature extraction script on a single image.
/// Returns the predicted label.
async fn run_feature_extractor(script_path: &str, image_path: &str) -> anyhow::Result<String> {
    // 1. Spawn: python3 <script_path> <image_path>
    // 2. Capture stdout.
    // 3. Parse the label from the output.
    //    (Exact parsing depends on the script's output format —
    //     adjust once you see what the provided script produces.)
    // 4. Return the label string.
    //
    // Example:
    // let output = tokio::process::Command::new("python3")
    //     .arg(script_path)
    //     .arg(image_path)
    //     .output()
    //     .await?;
    // let label = String::from_utf8(output.stdout)?.trim().to_string();
    // Ok(label)
    todo!()
}

// ---------------------------------------------------------------------------
// Background heartbeat
// ---------------------------------------------------------------------------

/// Periodically sends heartbeats to the local controller.
async fn heartbeat_loop(config: &AgentConfig) {
    // loop {
    //     sleep(Duration::from_secs(5)).await;
    //     let _ = send_heartbeat(config, current_batch_id).await;
    // }
    todo!()
}
