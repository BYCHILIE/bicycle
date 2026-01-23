//! JobManager - Control plane for Bicycle distributed streaming.
//!
//! The JobManager is responsible for:
//! - Accepting job submissions
//! - Building execution graphs from job graphs
//! - Scheduling tasks to workers
//! - Coordinating checkpoints
//! - Monitoring job and task health
//! - Handling failures and recovery

use anyhow::Result;
use bicycle_protocol::control::control_plane_server::ControlPlaneServer;
use clap::Parser;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::time;
use tonic::transport::Server;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

mod scheduler;
mod service;
mod state;

use service::ControlPlaneService;
use state::JobManagerState;

/// Bicycle JobManager - Control plane server.
#[derive(Debug, Parser)]
#[command(name = "jobmanager")]
struct Args {
    /// Address to bind the control-plane gRPC server.
    #[arg(long, default_value = "0.0.0.0:9000")]
    bind: String,

    /// Directory for checkpoint storage.
    #[arg(long, default_value = "/var/bicycle/checkpoints")]
    checkpoint_dir: String,

    /// Worker heartbeat timeout in seconds.
    #[arg(long, default_value = "30")]
    worker_timeout: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse()?))
        .init();

    let args = Args::parse();

    info!(bind = %args.bind, "Starting Bicycle JobManager");

    // Create checkpoint directory
    let checkpoint_dir = PathBuf::from(&args.checkpoint_dir);
    std::fs::create_dir_all(&checkpoint_dir)?;

    // Initialize state
    let state = Arc::new(JobManagerState::new());

    // Create gRPC service
    let service = ControlPlaneService::new(state.clone(), checkpoint_dir);

    // Parse bind address
    let addr: SocketAddr = args.bind.parse()?;

    // Spawn worker health monitor
    let monitor_state = state.clone();
    let worker_timeout = Duration::from_secs(args.worker_timeout);
    tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(10));
        loop {
            interval.tick().await;

            // Check for dead workers
            let dead_workers = monitor_state.get_dead_workers();
            for worker_id in dead_workers {
                warn!(worker_id = %worker_id, "Worker timeout - marking as dead");
                let failed_tasks = monitor_state.unregister_worker(&worker_id);
                if !failed_tasks.is_empty() {
                    warn!(
                        worker_id = %worker_id,
                        tasks = failed_tasks.len(),
                        "Tasks failed due to worker death"
                    );
                }
            }
        }
    });

    // Start gRPC server
    info!(addr = %addr, "JobManager gRPC server starting");

    Server::builder()
        .add_service(ControlPlaneServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}
