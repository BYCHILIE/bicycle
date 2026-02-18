//! Task execution engine for running streaming tasks.
//!
//! This module implements the execution of streaming operators on workers.
//! It supports:
//! - Source operators with pluggable connectors (Socket, Kafka, etc.)
//! - Processing operators (Map, Filter, FlatMap, KeyBy, Window, etc.)
//! - Sink operators with pluggable connectors
//! - Data flow between operators via channels

mod config;
mod operators;
mod plugin_op;
mod schema;
mod sinks;
mod sources;

use anyhow::Result;
use bicycle_core::{CheckpointBarrier, CheckpointOptions, StateHandle, StreamMessage};
use bicycle_network::{ChannelId, NetworkConfig, NetworkEnvironment};
use bicycle_protocol::control::{
    control_plane_client::ControlPlaneClient, AckCheckpointRequest, CheckpointMetadata,
    OperatorType, TaskDescriptor, TaskState,
};
use dashmap::DashMap;
use futures::FutureExt;
use parking_lot::RwLock;
use std::net::SocketAddr;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use crate::plugin_loader::PluginCache;

/// Status information for a running task.
#[derive(Debug)]
pub struct TaskStatus {
    pub state: RwLock<TaskState>,
    pub records_processed: AtomicI64,
    pub bytes_processed: AtomicI64,
    pub error_message: RwLock<Option<String>>,
    pub started_at: Instant,
    pub cancel_tx: Option<mpsc::Sender<()>>,
}

impl TaskStatus {
    pub fn new() -> Self {
        Self {
            state: RwLock::new(TaskState::Created),
            records_processed: AtomicI64::new(0),
            bytes_processed: AtomicI64::new(0),
            error_message: RwLock::new(None),
            started_at: Instant::now(),
            cancel_tx: None,
        }
    }

    pub fn get_state(&self) -> TaskState {
        *self.state.read()
    }

    pub fn set_state(&self, state: TaskState) {
        *self.state.write() = state;
    }

    pub fn set_error(&self, msg: String) {
        *self.error_message.write() = Some(msg);
        self.set_state(TaskState::Failed);
    }

    pub fn increment_records(&self, count: i64) {
        self.records_processed.fetch_add(count, Ordering::Relaxed);
    }

    pub fn increment_bytes(&self, count: i64) {
        self.bytes_processed.fetch_add(count, Ordering::Relaxed);
    }

    pub fn is_cancelled(&self) -> bool {
        let state = self.get_state();
        matches!(state, TaskState::Canceling | TaskState::Canceled)
    }
}

impl Default for TaskStatus {
    fn default() -> Self {
        Self::new()
    }
}

/// Executor for running streaming tasks.
pub struct TaskExecutor {
    /// Running tasks by task ID
    tasks: DashMap<String, Arc<TaskStatus>>,

    /// Worker ID
    worker_id: String,

    /// State directory
    state_dir: String,

    /// Available task slots
    total_slots: i32,
    used_slots: AtomicU64,

    /// Network environment for data plane communication
    network: Arc<NetworkEnvironment>,

    /// Local data plane address
    data_addr: RwLock<Option<SocketAddr>>,

    /// Internal channels for operator data flow (task_id -> sender)
    /// Upstream operators send to downstream task's channel
    /// Wrapped in Arc so all tasks share the same map
    internal_channels: Arc<DashMap<String, mpsc::Sender<StreamMessage<Vec<u8>>>>>,

    /// Internal channel receivers (task_id -> receiver)
    /// Each task receives from its own channel
    /// Wrapped in Arc so all tasks share the same map
    internal_receivers: Arc<DashMap<String, Arc<tokio::sync::Mutex<mpsc::Receiver<StreamMessage<Vec<u8>>>>>>>,

    /// Plugin cache for native plugin loading
    plugin_cache: Arc<PluginCache>,

    /// Channel for sink tasks to send checkpoint acknowledgments (job_id, task_id, checkpoint_id)
    checkpoint_ack_tx: mpsc::Sender<(String, String, i64)>,

    /// JobManager address for sending checkpoint acks
    jobmanager_addr: RwLock<Option<String>>,
}

impl TaskExecutor {
    pub fn new(worker_id: String, state_dir: String, slots: i32) -> Arc<Self> {
        let network = Arc::new(NetworkEnvironment::new(NetworkConfig::default()));
        let plugin_cache = Arc::new(
            PluginCache::new(&state_dir).expect("Failed to create plugin cache"),
        );

        let (checkpoint_ack_tx, checkpoint_ack_rx) = mpsc::channel::<(String, String, i64)>(256);

        let executor = Arc::new(Self {
            tasks: DashMap::new(),
            worker_id,
            state_dir,
            total_slots: slots,
            used_slots: AtomicU64::new(0),
            network,
            data_addr: RwLock::new(None),
            internal_channels: Arc::new(DashMap::new()),
            internal_receivers: Arc::new(DashMap::new()),
            plugin_cache,
            checkpoint_ack_tx,
            jobmanager_addr: RwLock::new(None),
        });

        // Spawn background task to drain checkpoint acks and forward to JobManager
        let exec_clone = executor.clone();
        tokio::spawn(Self::run_checkpoint_ack_sender(exec_clone, checkpoint_ack_rx));

        executor
    }

    /// Set the JobManager address for checkpoint ack forwarding.
    pub fn set_jobmanager_addr(&self, addr: String) {
        *self.jobmanager_addr.write() = Some(addr);
    }

    /// Background task that forwards checkpoint acks to the JobManager via gRPC.
    async fn run_checkpoint_ack_sender(
        executor: Arc<Self>,
        mut rx: mpsc::Receiver<(String, String, i64)>,
    ) {
        while let Some((job_id, task_id, checkpoint_id)) = rx.recv().await {
            let addr = {
                let guard = executor.jobmanager_addr.read();
                guard.clone()
            };
            if let Some(addr) = addr {
                let endpoint = if addr.starts_with("http") {
                    addr.clone()
                } else {
                    format!("http://{}", addr)
                };
                match ControlPlaneClient::connect(endpoint).await {
                    Ok(mut client) => {
                        let req = AckCheckpointRequest {
                            job_id: job_id.clone(),
                            task_id: task_id.clone(),
                            checkpoint_id,
                            metadata: Some(CheckpointMetadata {
                                state_size_bytes: 0,
                                duration_ms: 0,
                                state_handle: String::new(),
                            }),
                        };
                        if let Err(e) = client.acknowledge_checkpoint(req).await {
                            warn!(
                                job_id = %job_id,
                                task_id = %task_id,
                                checkpoint_id = checkpoint_id,
                                error = %e,
                                "Failed to send checkpoint ack to JobManager"
                            );
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, "Failed to connect to JobManager for checkpoint ack");
                    }
                }
            }
        }
    }

    /// Start the network environment for data plane communication.
    pub async fn start_network(&self, bind_addr: SocketAddr) -> Result<SocketAddr> {
        let actual_addr = self.network.start(bind_addr).await?;
        *self.data_addr.write() = Some(actual_addr);
        info!(addr = %actual_addr, "Task executor network started");
        Ok(actual_addr)
    }

    /// Get the data plane address.
    pub fn data_addr(&self) -> Option<SocketAddr> {
        *self.data_addr.read()
    }

    /// Get the network environment.
    pub fn network(&self) -> Arc<NetworkEnvironment> {
        self.network.clone()
    }

    /// Get available slots.
    pub fn available_slots(&self) -> i32 {
        self.total_slots - self.used_slots.load(Ordering::Relaxed) as i32
    }

    /// Deploy and start a task.
    pub async fn deploy_task(
        &self,
        job_id: &str,
        task_id: &str,
        descriptor: TaskDescriptor,
    ) -> Result<()> {
        // Check if we have slots
        if self.available_slots() <= 0 {
            anyhow::bail!("No available slots");
        }

        // Check if task already exists
        if self.tasks.contains_key(task_id) {
            anyhow::bail!("Task {} already exists", task_id);
        }

        let operator_type =
            OperatorType::try_from(descriptor.operator_type).unwrap_or(OperatorType::Unknown);

        info!(
            task_id = %task_id,
            job_id = %job_id,
            operator = %descriptor.operator_name,
            operator_type = ?operator_type,
            input_gates = descriptor.input_gates.len(),
            output_gates = descriptor.output_gates.len(),
            "Deploying task"
        );

        // Create task status
        let status = Arc::new(TaskStatus::new());
        status.set_state(TaskState::Deploying);

        // Reserve slot
        self.used_slots.fetch_add(1, Ordering::Relaxed);
        self.tasks.insert(task_id.to_string(), status.clone());

        // Create internal channel for this task's input
        // Upstream operators will send to this task's channel
        let (input_tx, input_rx) = mpsc::channel::<StreamMessage<Vec<u8>>>(1024);
        self.internal_channels
            .insert(task_id.to_string(), input_tx);
        self.internal_receivers
            .insert(task_id.to_string(), Arc::new(tokio::sync::Mutex::new(input_rx)));

        // Set up network channels for input gates
        let network = self.network.clone();
        for (idx, input_gate) in descriptor.input_gates.iter().enumerate() {
            let channel_id = ChannelId::new(
                &input_gate.upstream_task_id,
                task_id,
                idx as u32,
            );
            let (tx, _rx) = mpsc::channel(1024);
            network.register_incoming(channel_id, tx);
            debug!(
                task_id = %task_id,
                upstream = %input_gate.upstream_task_id,
                "Registered input gate"
            );
        }

        // Start task execution
        let task_id_clone = task_id.to_string();
        let job_id_clone = job_id.to_string();
        let status_clone = status.clone();
        let network_clone = network.clone();
        let internal_channels = self.internal_channels.clone();
        let internal_receivers = self.internal_receivers.clone();
        let plugin_cache = self.plugin_cache.clone();
        let checkpoint_ack_tx = self.checkpoint_ack_tx.clone();

        tokio::spawn(async move {
            status_clone.set_state(TaskState::Running);

            // Set up output connections
            for (idx, output_gate) in descriptor.output_gates.iter().enumerate() {
                let addr: SocketAddr = format!(
                    "{}:{}",
                    output_gate.downstream_worker_host,
                    output_gate.downstream_worker_port
                )
                .parse()
                .unwrap_or_else(|_| "127.0.0.1:0".parse().unwrap());

                let channel_id = ChannelId::new(
                    &task_id_clone,
                    &output_gate.downstream_task_id,
                    idx as u32,
                );

                match network_clone.create_outgoing(addr, channel_id.clone()).await {
                    Ok(_sender) => {
                        debug!(
                            task_id = %task_id_clone,
                            downstream = %output_gate.downstream_task_id,
                            "Created output connection"
                        );
                    }
                    Err(e) => {
                        debug!(
                            task_id = %task_id_clone,
                            error = %e,
                            "Failed to create output connection (will retry)"
                        );
                    }
                }
            }

            // Execute the task based on operator type, catching panics
            let task_future = Self::run_task(
                &task_id_clone,
                &job_id_clone,
                descriptor,
                status_clone.clone(),
                internal_channels,
                internal_receivers,
                plugin_cache,
                checkpoint_ack_tx,
            );

            // Wrap in AssertUnwindSafe to catch panics
            let result = AssertUnwindSafe(task_future).catch_unwind().await;

            match result {
                Ok(Ok(())) => {
                    let current_state = status_clone.get_state();
                    if !matches!(current_state, TaskState::Canceled | TaskState::Canceling) {
                        status_clone.set_state(TaskState::Finished);
                        info!(task_id = %task_id_clone, "Task finished successfully");
                    }
                }
                Ok(Err(e)) => {
                    status_clone.set_error(e.to_string());
                    error!(task_id = %task_id_clone, error = %e, "Task failed");
                }
                Err(panic_info) => {
                    // Extract panic message
                    let panic_msg = if let Some(s) = panic_info.downcast_ref::<&str>() {
                        s.to_string()
                    } else if let Some(s) = panic_info.downcast_ref::<String>() {
                        s.clone()
                    } else {
                        "Unknown panic".to_string()
                    };
                    status_clone.set_error(format!("Task panicked: {}", panic_msg));
                    error!(task_id = %task_id_clone, panic = %panic_msg, "Task panicked");
                }
            }
        });

        Ok(())
    }

    /// Run a task based on its operator type.
    async fn run_task(
        task_id: &str,
        job_id: &str,
        descriptor: TaskDescriptor,
        status: Arc<TaskStatus>,
        internal_channels: Arc<DashMap<String, mpsc::Sender<StreamMessage<Vec<u8>>>>>,
        internal_receivers: Arc<DashMap<String, Arc<tokio::sync::Mutex<mpsc::Receiver<StreamMessage<Vec<u8>>>>>>>,
        plugin_cache: Arc<PluginCache>,
        checkpoint_ack_tx: mpsc::Sender<(String, String, i64)>,
    ) -> Result<()> {
        let operator_type =
            OperatorType::try_from(descriptor.operator_type).unwrap_or(OperatorType::Unknown);

        info!(
            task_id = %task_id,
            operator_type = ?operator_type,
            "Task execution started"
        );

        match operator_type {
            OperatorType::Source => {
                Self::run_source_operator(task_id, job_id, &descriptor, status, internal_channels, internal_receivers, plugin_cache).await
            }
            OperatorType::Sink => {
                Self::run_sink_operator(task_id, job_id, &descriptor, status, internal_channels, internal_receivers, checkpoint_ack_tx, plugin_cache).await
            }
            OperatorType::Map => {
                Self::run_map_operator(task_id, &descriptor, status, internal_channels, internal_receivers).await
            }
            OperatorType::FlatMap => {
                Self::run_flatmap_operator(task_id, &descriptor, status, internal_channels, internal_receivers).await
            }
            OperatorType::Filter => {
                Self::run_filter_operator(task_id, &descriptor, status, internal_channels, internal_receivers).await
            }
            OperatorType::KeyBy => {
                Self::run_keyby_operator(task_id, &descriptor, status, internal_channels, internal_receivers).await
            }
            OperatorType::Window => {
                Self::run_window_operator(task_id, &descriptor, status, internal_channels, internal_receivers).await
            }
            OperatorType::Count => {
                Self::run_count_operator(task_id, &descriptor, status, internal_channels, internal_receivers).await
            }
            OperatorType::Reduce => {
                Self::run_reduce_operator(task_id, &descriptor, status, internal_channels, internal_receivers).await
            }
            OperatorType::Process => {
                // Check if this task has a plugin module
                if !descriptor.plugin_module.is_empty() && descriptor.plugin_type == "native" {
                    Self::run_plugin_operator(
                        task_id,
                        job_id,
                        &descriptor,
                        status,
                        internal_channels,
                        internal_receivers,
                        plugin_cache,
                    )
                    .await
                } else {
                    // Fallback to passthrough for non-plugin process operators
                    Self::run_passthrough_operator(task_id, &descriptor, status, internal_channels, internal_receivers).await
                }
            }
            _ => {
                Self::run_passthrough_operator(task_id, &descriptor, status, internal_channels, internal_receivers)
                    .await
            }
        }
    }

    // =========================================================================
    // Helper: Send to downstream with retry
    // =========================================================================

    /// Send data to downstream tasks, retrying if channels aren't ready yet
    async fn send_to_downstream(
        data: Vec<u8>,
        downstream_tasks: &[String],
        internal_channels: &Arc<DashMap<String, mpsc::Sender<StreamMessage<Vec<u8>>>>>,
    ) {
        for downstream_id in downstream_tasks {
            let mut retries = 0;
            loop {
                if let Some(tx) = internal_channels.get(downstream_id) {
                    let _ = tx.send(StreamMessage::Data(data.clone())).await;
                    break;
                } else if retries < 50 {
                    retries += 1;
                    if retries == 1 {
                        let keys: Vec<String> = internal_channels.iter().map(|e| e.key().clone()).collect();
                        warn!(
                            downstream = %downstream_id,
                            available_keys = ?keys,
                            "Looking for downstream channel (will retry)"
                        );
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                } else {
                    let keys: Vec<String> = internal_channels.iter().map(|e| e.key().clone()).collect();
                    warn!(
                        downstream = %downstream_id,
                        available_keys = ?keys,
                        "Downstream channel not found after retries"
                    );
                    break;
                }
            }
        }
    }

    /// Send a control message (barrier, watermark) to downstream tasks, retrying if channels aren't ready yet
    async fn send_control_downstream(
        msg: StreamMessage<Vec<u8>>,
        downstream_tasks: &[String],
        internal_channels: &Arc<DashMap<String, mpsc::Sender<StreamMessage<Vec<u8>>>>>,
    ) {
        for downstream_id in downstream_tasks {
            let mut retries = 0;
            loop {
                if let Some(tx) = internal_channels.get(downstream_id) {
                    let _ = tx.send(msg.clone()).await;
                    break;
                } else if retries < 50 {
                    retries += 1;
                    if retries == 1 {
                        warn!(
                            downstream = %downstream_id,
                            "Looking for downstream channel for control message (will retry)"
                        );
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                } else {
                    warn!(
                        downstream = %downstream_id,
                        "Downstream channel not found for control message after retries"
                    );
                    break;
                }
            }
        }
    }

    // =========================================================================
    // Task Management
    // =========================================================================

    /// Cancel a task.
    pub async fn cancel_task(&self, task_id: &str) -> Result<()> {
        if let Some(status) = self.tasks.get(task_id) {
            status.set_state(TaskState::Canceling);
            status.set_state(TaskState::Canceled);
            info!(task_id = %task_id, "Task cancelled");
        } else {
            anyhow::bail!("Task {} not found", task_id);
        }
        Ok(())
    }

    /// Get status of all tasks.
    pub fn get_all_task_statuses(&self) -> Vec<(String, TaskState, i64, i64, Option<String>)> {
        self.tasks
            .iter()
            .map(|entry| {
                let status = entry.value();
                (
                    entry.key().clone(),
                    status.get_state(),
                    status.records_processed.load(Ordering::Relaxed),
                    status.bytes_processed.load(Ordering::Relaxed),
                    status.error_message.read().clone(),
                )
            })
            .collect()
    }

    /// Get status of a specific task.
    pub fn get_task_status(&self, task_id: &str) -> Option<Arc<TaskStatus>> {
        self.tasks.get(task_id).map(|e| e.value().clone())
    }

    /// Remove completed/failed tasks.
    pub fn cleanup_finished_tasks(&self) {
        let to_remove: Vec<String> = self
            .tasks
            .iter()
            .filter(|entry| {
                let state = entry.value().get_state();
                matches!(
                    state,
                    TaskState::Finished | TaskState::Failed | TaskState::Canceled
                )
            })
            .map(|entry| entry.key().clone())
            .collect();

        for task_id in to_remove {
            if self.tasks.remove(&task_id).is_some() {
                self.used_slots.fetch_sub(1, Ordering::Relaxed);
                self.internal_channels.remove(&task_id);
                self.internal_receivers.remove(&task_id);
                debug!(task_id = %task_id, "Cleaned up finished task");
            }
        }
    }

    /// Trigger a checkpoint for a task by injecting a barrier into its internal channel.
    pub async fn trigger_checkpoint(&self, task_id: &str, checkpoint_id: i64) -> Result<()> {
        if let Some(_status) = self.tasks.get(task_id) {
            info!(
                task_id = %task_id,
                checkpoint_id = checkpoint_id,
                "Injecting checkpoint barrier"
            );

            let barrier = StreamMessage::Barrier(CheckpointBarrier {
                checkpoint_id: checkpoint_id as u64,
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64,
                options: CheckpointOptions::default(),
            });

            // Send barrier to the task's internal channel
            if let Some(tx) = self.internal_channels.get(task_id) {
                let _ = tx.send(barrier).await;
            } else {
                warn!(task_id = %task_id, "No internal channel found for task");
            }

            Ok(())
        } else {
            anyhow::bail!("Task {} not found", task_id)
        }
    }
}
