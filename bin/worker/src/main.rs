//! Worker - Task executor for Bicycle distributed streaming.
//!
//! The Worker is responsible for:
//! - Registering with the JobManager
//! - Executing tasks assigned by the JobManager
//! - Sending heartbeats with task status
//! - Managing local state for checkpoints
//! - Network communication with other workers

use anyhow::Result;
use bicycle_protocol::control::{
    control_plane_client::ControlPlaneClient, HeartbeatRequest, RegisterWorkerRequest,
    TaskCommandType, TaskState, TaskStatus, WorkerMetrics, WorkerResources,
};
use clap::Parser;
use std::sync::Arc;
use std::time::Duration;
use tonic::transport::Channel;
use tracing::{debug, error, info, warn};
use tracing_subscriber::EnvFilter;

mod task_executor;

use task_executor::TaskExecutor;

/// Bicycle Worker - Task executor and data plane node.
#[derive(Debug, Parser)]
#[command(name = "worker")]
struct Args {
    /// JobManager address to connect to.
    #[arg(long, default_value = "127.0.0.1:9000")]
    jobmanager: String,

    /// Local bind address for data plane.
    #[arg(long, default_value = "0.0.0.0:0")]
    bind: String,

    /// Number of task slots.
    #[arg(long, default_value = "4")]
    slots: i32,

    /// Memory limit in MB.
    #[arg(long, default_value = "2048")]
    memory_mb: i64,

    /// State directory.
    #[arg(long, default_value = "/var/bicycle/state")]
    state_dir: String,
}

/// Worker state and client.
struct Worker {
    worker_id: String,
    hostname: String,
    data_port: i32,
    slots: i32,
    memory_mb: i64,
    client: ControlPlaneClient<Channel>,
    executor: Arc<TaskExecutor>,
    heartbeat_interval: Duration,
}

impl Worker {
    /// Connect to the JobManager and register.
    async fn connect(args: &Args) -> Result<Self> {
        let addr = format!("http://{}", args.jobmanager);
        info!(jobmanager = %addr, "Connecting to JobManager");

        let client = ControlPlaneClient::connect(addr).await?;

        // Generate worker ID
        let worker_id = uuid::Uuid::new_v4().to_string();

        // Get hostname
        let hostname = hostname::get()
            .map(|h| h.to_string_lossy().to_string())
            .unwrap_or_else(|_| "unknown".to_string());

        // Parse bind address to get port (0 means random)
        let data_port: i32 = args
            .bind
            .split(':')
            .last()
            .and_then(|p| p.parse().ok())
            .unwrap_or(0);

        // Create executor
        let executor = Arc::new(TaskExecutor::new(
            worker_id.clone(),
            args.state_dir.clone(),
            args.slots,
        ));

        Ok(Self {
            worker_id,
            hostname,
            data_port,
            slots: args.slots,
            memory_mb: args.memory_mb,
            client,
            executor,
            heartbeat_interval: Duration::from_secs(5), // Will be updated from registration response
        })
    }

    /// Register with the JobManager.
    async fn register(&mut self) -> Result<()> {
        let request = RegisterWorkerRequest {
            worker_id: self.worker_id.clone(),
            hostname: self.hostname.clone(),
            data_port: self.data_port,
            available_slots: self.slots,
            resources: Some(WorkerResources {
                memory_mb: self.memory_mb,
                cpu_cores: num_cpus(),
            }),
        };

        info!(
            worker_id = %self.worker_id,
            hostname = %self.hostname,
            slots = self.slots,
            "Registering with JobManager"
        );

        let response = self.client.register_worker(request).await?;
        let resp = response.into_inner();

        if resp.success {
            self.heartbeat_interval = Duration::from_millis(resp.heartbeat_interval_ms as u64);
            info!(
                worker_id = %self.worker_id,
                heartbeat_interval_ms = resp.heartbeat_interval_ms,
                "Registration successful"
            );
            Ok(())
        } else {
            anyhow::bail!("Registration failed: {}", resp.message)
        }
    }

    /// Run the heartbeat loop.
    async fn run_heartbeat_loop(&mut self) -> Result<()> {
        let mut interval = tokio::time::interval(self.heartbeat_interval);

        loop {
            interval.tick().await;

            // Collect task statuses
            let task_statuses: Vec<TaskStatus> = self
                .executor
                .get_all_task_statuses()
                .into_iter()
                .map(|(task_id, state, records, bytes, error)| TaskStatus {
                    task_id,
                    state: state as i32,
                    error_message: error.unwrap_or_default(),
                    records_processed: records,
                    bytes_processed: bytes,
                })
                .collect();

            // Get system metrics
            let metrics = Some(WorkerMetrics {
                cpu_usage: get_cpu_usage(),
                memory_used_mb: get_memory_usage_mb(),
                network_bytes_in: 0,
                network_bytes_out: 0,
            });

            let request = HeartbeatRequest {
                worker_id: self.worker_id.clone(),
                task_statuses,
                metrics,
            };

            match self.client.heartbeat(request).await {
                Ok(response) => {
                    let resp = response.into_inner();

                    if !resp.success {
                        warn!("Heartbeat not acknowledged");
                    }

                    // Process any commands from JobManager
                    for command in resp.commands {
                        self.handle_command(&command.task_id, command.command).await;
                    }
                }
                Err(e) => {
                    error!(error = %e, "Heartbeat failed - connection to JobManager lost");
                    // Try to reconnect
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }

            // Cleanup finished tasks
            self.executor.cleanup_finished_tasks();
        }
    }

    /// Handle a command from the JobManager.
    async fn handle_command(&self, task_id: &str, command: i32) {
        let cmd = TaskCommandType::try_from(command).unwrap_or(TaskCommandType::TaskCommandNone);

        match cmd {
            TaskCommandType::TaskCommandCancel => {
                info!(task_id = %task_id, "Received cancel command");
                if let Err(e) = self.executor.cancel_task(task_id).await {
                    error!(task_id = %task_id, error = %e, "Failed to cancel task");
                }
            }
            TaskCommandType::TaskCommandCheckpoint => {
                info!(task_id = %task_id, "Received checkpoint command");
                // Trigger checkpoint for task
                // In real impl, would coordinate with task executor
            }
            TaskCommandType::TaskCommandNone => {}
        }
    }
}

/// Get number of CPU cores.
fn num_cpus() -> i32 {
    std::thread::available_parallelism()
        .map(|p| p.get() as i32)
        .unwrap_or(1)
}

/// Get current CPU usage (simplified).
fn get_cpu_usage() -> f64 {
    // In real impl, would read from /proc/stat or use sysinfo crate
    0.0
}

/// Get current memory usage in MB (simplified).
fn get_memory_usage_mb() -> i64 {
    // In real impl, would read from /proc/meminfo or use sysinfo crate
    0
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse()?))
        .init();

    let args = Args::parse();

    info!(
        jobmanager = %args.jobmanager,
        bind = %args.bind,
        slots = args.slots,
        "Starting Bicycle Worker"
    );

    // Create state directory
    std::fs::create_dir_all(&args.state_dir)?;

    // Connect and register
    let mut worker = Worker::connect(&args).await?;

    // Retry registration with backoff
    let mut retry_count = 0;
    loop {
        match worker.register().await {
            Ok(()) => break,
            Err(e) => {
                retry_count += 1;
                if retry_count > 10 {
                    return Err(e);
                }
                warn!(
                    error = %e,
                    retry = retry_count,
                    "Registration failed, retrying..."
                );
                tokio::time::sleep(Duration::from_secs(2_u64.pow(retry_count.min(5)))).await;
                worker = Worker::connect(&args).await?;
            }
        }
    }

    // Run heartbeat loop
    worker.run_heartbeat_loop().await
}
