use clap::{Parser, Subcommand};
use std::net::SocketAddr;

mod agent;
mod cluster_controller;
mod common;
mod local_controller;
mod raft;

#[derive(Parser)]
#[command(name = "inf3203_aika")]
#[command(about = "Distributed edge system for AI inference")]
struct Cli {
    #[command(subcommand)]
    command: Role,
}

#[derive(Subcommand)]
enum Role {
    /// Run as a cluster controller (participates in Raft consensus)
    ClusterController {
        /// This node's unique ID within the Raft cluster
        #[arg(long)]
        node_id: u64,

        /// Address to bind the HTTP + Raft server to
        #[arg(long, default_value = "0.0.0.0:8000")]
        bind: SocketAddr,

        /// Comma-separated list of peer cluster controller addresses (e.g. node1:8000,node2:8000)
        #[arg(long, value_delimiter = ',')]
        peers: Vec<String>,

        /// TTL in seconds for assigned tasks before they are reclaimed
        #[arg(long, default_value = "120")]
        task_ttl_secs: u64,
    },

    /// Run as a local controller (monitors agents on this physical node)
    LocalController {
        /// Unique identifier for this node
        #[arg(long)]
        node_id: String,

        /// Address to bind the local controller HTTP server to
        #[arg(long, default_value = "0.0.0.0:9000")]
        bind: SocketAddr,

        /// Comma-separated cluster controller addresses for task requests
        #[arg(long, value_delimiter = ',')]
        cc_addrs: Vec<String>,

        /// Number of agent workers to spawn (0 = replica mode)
        #[arg(long, default_value = "4")]
        agents: usize,

        /// Seconds between health checks on local agents
        #[arg(long, default_value = "5")]
        health_check_interval: u64,
    },

    /// Run as a worker agent (usually spawned by a local controller)
    Agent {
        /// Unique identifier for this agent
        #[arg(long)]
        agent_id: String,

        /// Address of the local controller managing this agent
        #[arg(long)]
        lc_addr: String,

        /// Comma-separated cluster controller addresses for task requests
        #[arg(long, value_delimiter = ',')]
        cc_addrs: Vec<String>,

        /// Path to the feature extraction Python script
        #[arg(long, default_value = "./feature_extractor.py")]
        extractor_script: String,

        /// Base path where the unlabeled images reside
        #[arg(long, default_value = "/share/inf3203/unlabeled_images")]
        image_base_path: String,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    // Parse command-line arguments to determine what role this node has
    let cli = Cli::parse();

    // Match role to build the needed config struct and start the node
    match cli.command {
        Role::ClusterController {
            node_id,
            bind,
            peers,
            task_ttl_secs,
        } => {
            let config = cluster_controller::ClusterControllerConfig {
                node_id,
                bind,
                peers,
                task_ttl_secs,
            };
            cluster_controller::run(config).await
        }

        Role::LocalController {
            node_id,
            bind,
            cc_addrs,
            agents,
            health_check_interval,
        } => {
            let config = local_controller::LocalControllerConfig {
                node_id,
                bind,
                cc_addrs,
                agent_count: agents,
                health_check_interval,
            };
            local_controller::run(config).await
        }

        Role::Agent {
            agent_id,
            lc_addr,
            cc_addrs,
            extractor_script,
            image_base_path,
        } => {
            let config = agent::AgentConfig {
                agent_id,
                lc_addr,
                cc_addrs,
                extractor_script,
                image_base_path,
            };
            agent::run(config).await
        }
    }
}
