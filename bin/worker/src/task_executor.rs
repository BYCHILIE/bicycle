//! Task execution engine for running streaming tasks.

use anyhow::Result;
use bicycle_core::{CheckpointBarrier, StreamMessage};
use bicycle_network::{ChannelId, NetworkConfig, NetworkEnvironment};
use bicycle_protocol::control::{InputGate, OperatorType, OutputGate, TaskDescriptor, TaskState};
use bicycle_runtime::{stream_channel, Emitter, Receiver, Sender};
use dashmap::DashMap;
use parking_lot::RwLock;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{debug, error, info};

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
}

impl TaskExecutor {
    pub fn new(worker_id: String, state_dir: String, slots: i32) -> Self {
        let network = Arc::new(NetworkEnvironment::new(NetworkConfig::default()));

        Self {
            tasks: DashMap::new(),
            worker_id,
            state_dir,
            total_slots: slots,
            used_slots: AtomicU64::new(0),
            network,
            data_addr: RwLock::new(None),
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

        info!(
            task_id = %task_id,
            operator = %descriptor.operator_name,
            operator_type = ?OperatorType::try_from(descriptor.operator_type),
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
        let status_clone = status.clone();
        let network_clone = network.clone();

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
                        // In production, we'd implement retry logic here
                    }
                }
            }

            // Execute the task
            match Self::run_task(&task_id_clone, descriptor, status_clone.clone()).await {
                Ok(()) => {
                    // Check if it was cancelled vs finished normally
                    let current_state = status_clone.get_state();
                    if !matches!(current_state, TaskState::Canceled | TaskState::Canceling) {
                        status_clone.set_state(TaskState::Finished);
                        info!(task_id = %task_id_clone, "Task finished successfully");
                    }
                }
                Err(e) => {
                    status_clone.set_error(e.to_string());
                    error!(task_id = %task_id_clone, error = %e, "Task failed");
                }
            }
        });

        Ok(())
    }

    /// Run a task based on its operator type.
    async fn run_task(
        task_id: &str,
        descriptor: TaskDescriptor,
        status: Arc<TaskStatus>,
    ) -> Result<()> {
        info!(task_id = %task_id, "Task execution started");

        let operator_type =
            OperatorType::try_from(descriptor.operator_type).unwrap_or(OperatorType::Unknown);

        match operator_type {
            OperatorType::Source => {
                Self::run_source_task(task_id, &descriptor, status).await
            }
            OperatorType::Sink => {
                Self::run_sink_task(task_id, &descriptor, status).await
            }
            OperatorType::Map => {
                Self::run_map_task(task_id, &descriptor, status).await
            }
            OperatorType::Window => {
                Self::run_window_task(task_id, &descriptor, status).await
            }
            _ => {
                // For other operators, run a placeholder
                Self::run_passthrough_task(task_id, &descriptor, status).await
            }
        }
    }

    /// Run a source task that generates demo word count data.
    async fn run_source_task(
        task_id: &str,
        descriptor: &TaskDescriptor,
        status: Arc<TaskStatus>,
    ) -> Result<()> {
        info!(task_id = %task_id, "Starting source operator");

        let words = vec!["hello", "world", "bicycle", "streaming", "data", "rust", "flink"];
        let mut record_count: i64 = 0;

        // Generate records continuously
        loop {
            for word in &words {
                // Check if we should stop
                let state = status.get_state();
                if matches!(state, TaskState::Canceling | TaskState::Canceled) {
                    info!(task_id = %task_id, "Source task cancelled");
                    return Ok(());
                }

                // Simulate generating a record
                let record = format!("{},{}", word, 1);
                let bytes = record.len() as i64;

                status.increment_records(1);
                status.increment_bytes(bytes);
                record_count += 1;

                if record_count % 100 == 0 {
                    debug!(
                        task_id = %task_id,
                        records = record_count,
                        "Source generated records"
                    );
                }

                // Rate limit
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }

    /// Run a map task that transforms data.
    async fn run_map_task(
        task_id: &str,
        descriptor: &TaskDescriptor,
        status: Arc<TaskStatus>,
    ) -> Result<()> {
        info!(task_id = %task_id, "Starting map operator");

        // In a real implementation, this would receive data from upstream
        // and transform it. For demo, we simulate processing.
        loop {
            let state = status.get_state();
            if matches!(state, TaskState::Canceling | TaskState::Canceled) {
                info!(task_id = %task_id, "Map task cancelled");
                return Ok(());
            }

            // Simulate processing records
            tokio::time::sleep(Duration::from_millis(500)).await;

            // Update metrics (simulated)
            status.increment_records(10);
            status.increment_bytes(100);
        }
    }

    /// Run a window task that aggregates data.
    async fn run_window_task(
        task_id: &str,
        descriptor: &TaskDescriptor,
        status: Arc<TaskStatus>,
    ) -> Result<()> {
        info!(task_id = %task_id, "Starting window operator");

        let window_duration = Duration::from_secs(5);
        let mut window_start = Instant::now();
        let mut window_count: i64 = 0;

        loop {
            let state = status.get_state();
            if matches!(state, TaskState::Canceling | TaskState::Canceled) {
                info!(task_id = %task_id, "Window task cancelled");
                return Ok(());
            }

            // Simulate receiving and aggregating records
            tokio::time::sleep(Duration::from_millis(100)).await;
            window_count += 1;
            status.increment_records(1);
            status.increment_bytes(10);

            // Check if window should close
            if window_start.elapsed() >= window_duration {
                debug!(
                    task_id = %task_id,
                    records = window_count,
                    "Window closed, emitting aggregate"
                );
                window_start = Instant::now();
                window_count = 0;
            }
        }
    }

    /// Run a sink task that consumes and outputs data.
    async fn run_sink_task(
        task_id: &str,
        descriptor: &TaskDescriptor,
        status: Arc<TaskStatus>,
    ) -> Result<()> {
        info!(task_id = %task_id, "Starting sink operator");

        let mut total_records: i64 = 0;

        loop {
            let state = status.get_state();
            if matches!(state, TaskState::Canceling | TaskState::Canceled) {
                info!(task_id = %task_id, "Sink task cancelled");
                return Ok(());
            }

            // Simulate receiving and writing records
            tokio::time::sleep(Duration::from_millis(200)).await;

            let records_batch = 5;
            let bytes_batch = 50;
            total_records += records_batch;

            status.increment_records(records_batch);
            status.increment_bytes(bytes_batch);

            if total_records % 50 == 0 {
                debug!(
                    task_id = %task_id,
                    total_records = total_records,
                    "Sink output progress"
                );
            }
        }
    }

    /// Run a passthrough task for other operator types.
    async fn run_passthrough_task(
        task_id: &str,
        descriptor: &TaskDescriptor,
        status: Arc<TaskStatus>,
    ) -> Result<()> {
        info!(
            task_id = %task_id,
            operator_type = descriptor.operator_type,
            "Starting passthrough operator"
        );

        loop {
            let state = status.get_state();
            if matches!(state, TaskState::Canceling | TaskState::Canceled) {
                info!(task_id = %task_id, "Passthrough task cancelled");
                return Ok(());
            }

            // Simulate processing
            tokio::time::sleep(Duration::from_millis(500)).await;
            status.increment_records(1);
            status.increment_bytes(10);
        }
    }

    /// Cancel a task.
    pub async fn cancel_task(&self, task_id: &str) -> Result<()> {
        if let Some(status) = self.tasks.get(task_id) {
            status.set_state(TaskState::Canceling);

            // In real impl, would signal task to stop
            // For now, just mark as cancelled
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
                debug!(task_id = %task_id, "Cleaned up finished task");
            }
        }
    }

    /// Trigger a checkpoint for a task.
    pub async fn trigger_checkpoint(&self, task_id: &str, checkpoint_id: i64) -> Result<()> {
        if let Some(status) = self.tasks.get(task_id) {
            // In a full implementation, we would:
            // 1. Send a checkpoint barrier to the task
            // 2. Wait for the task to snapshot its state
            // 3. Report completion to the JobManager
            info!(
                task_id = %task_id,
                checkpoint_id = checkpoint_id,
                "Triggering checkpoint for task"
            );
            Ok(())
        } else {
            anyhow::bail!("Task {} not found", task_id)
        }
    }
}
