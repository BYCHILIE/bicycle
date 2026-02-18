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
    control_plane_client::ControlPlaneClient, worker_control_server::WorkerControlServer,
    HeartbeatRequest, RegisterWorkerRequest, TaskCommandType, TaskState, TaskStatus,
    WorkerMetrics, WorkerResources,
};
use clap::Parser;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tonic::transport::{Channel, Server};
use tracing::{debug, error, info, warn};
use tracing_subscriber::EnvFilter;

mod plugin_loader;
mod service;
mod task_executor;

use service::WorkerControlService;
use task_executor::TaskExecutor;

/// Bicycle Worker - Task executor and data plane node.
#[derive(Debug, Parser)]
#[command(name = "worker")]
struct Args {
    /// JobManager address to connect to.
    #[arg(long, default_value = "127.0.0.1:9000")]
    jobmanager: String,

    /// Local bind address for control plane gRPC.
    #[arg(long, default_value = "0.0.0.0:0")]
    bind: String,

    /// Local bind address for data plane network.
    #[arg(long, default_value = "0.0.0.0:0")]
    data_bind: String,

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
    control_port: i32,
    slots: i32,
    memory_mb: i64,
    client: ControlPlaneClient<Channel>,
    executor: Arc<TaskExecutor>,
    heartbeat_interval: Duration,
    bind_addr: SocketAddr,
}

impl Worker {
    /// Connect to the JobManager and create worker instance.
    async fn connect(args: &Args, bind_addr: SocketAddr) -> Result<Self> {
        let addr = format!("http://{}", args.jobmanager);
        info!(jobmanager = %addr, "Connecting to JobManager");

        let client = ControlPlaneClient::connect(addr).await?;

        // Generate worker ID
        let worker_id = uuid::Uuid::new_v4().to_string();

        // Get hostname
        let hostname = hostname::get()
            .map(|h| h.to_string_lossy().to_string())
            .unwrap_or_else(|_| "unknown".to_string());

        // Use the actual bound port
        let control_port = bind_addr.port() as i32;

        // Create executor
        let executor = TaskExecutor::new(
            worker_id.clone(),
            args.state_dir.clone(),
            args.slots,
        );

        // Set JobManager address for checkpoint ack forwarding
        executor.set_jobmanager_addr(args.jobmanager.clone());

        Ok(Self {
            worker_id,
            hostname,
            control_port,
            slots: args.slots,
            memory_mb: args.memory_mb,
            client,
            executor,
            heartbeat_interval: Duration::from_secs(5), // Will be updated from registration response
            bind_addr,
        })
    }

    /// Register with the JobManager.
    async fn register(&mut self) -> Result<()> {
        let request = RegisterWorkerRequest {
            worker_id: self.worker_id.clone(),
            hostname: self.hostname.clone(),
            data_port: self.control_port,
            available_slots: self.slots,
            resources: Some(WorkerResources {
                memory_mb: self.memory_mb,
                cpu_cores: num_cpus(),
            }),
        };

        info!(
            worker_id = %self.worker_id,
            hostname = %self.hostname,
            port = self.control_port,
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
                    operator_name: String::new(),
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
        data_bind = %args.data_bind,
        slots = args.slots,
        "Starting Bicycle Worker"
    );

    // Create state directory
    std::fs::create_dir_all(&args.state_dir)?;

    // Parse and bind the control plane address
    let bind_addr: SocketAddr = args.bind.parse()?;
    let listener = tokio::net::TcpListener::bind(bind_addr).await?;
    let local_addr = listener.local_addr()?;
    info!(bind_addr = %local_addr, "Worker control service bound");

    // Connect and create worker
    let mut worker = Worker::connect(&args, local_addr).await?;

    // Start the data plane network
    let data_bind_addr: SocketAddr = args.data_bind.parse()?;
    let data_addr = worker.executor.start_network(data_bind_addr).await?;
    info!(data_addr = %data_addr, "Worker data plane started");

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
                worker = Worker::connect(&args, local_addr).await?;
            }
        }
    }

    // Create the gRPC service
    let control_service = WorkerControlService::new(worker.executor.clone());

    // Run the gRPC server and heartbeat loop concurrently
    let server_handle = tokio::spawn(async move {
        info!(addr = %local_addr, "Starting Worker control gRPC server");
        Server::builder()
            .add_service(WorkerControlServer::new(control_service))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
    });

    // Run heartbeat loop
    let heartbeat_handle = tokio::spawn(async move {
        worker.run_heartbeat_loop().await
    });

    // Wait for either to complete (or fail)
    tokio::select! {
        result = server_handle => {
            match result {
                Ok(Ok(())) => info!("gRPC server stopped"),
                Ok(Err(e)) => error!(error = %e, "gRPC server error"),
                Err(e) => error!(error = %e, "gRPC server task panicked"),
            }
        }
        result = heartbeat_handle => {
            match result {
                Ok(Ok(())) => info!("Heartbeat loop stopped"),
                Ok(Err(e)) => error!(error = %e, "Heartbeat loop error"),
                Err(e) => error!(error = %e, "Heartbeat loop task panicked"),
            }
        }
    }

    Ok(())
}
