//! Task execution engine for running streaming tasks.
//!
//! This module implements the execution of streaming operators on workers.
//! It supports:
//! - Source operators with pluggable connectors (Socket, Kafka, etc.)
//! - Processing operators (Map, Filter, FlatMap, KeyBy, Window, etc.)
//! - Sink operators with pluggable connectors
//! - Data flow between operators via channels

use anyhow::Result;
use bicycle_connectors::kafka::{KafkaConfig, KafkaSink, KafkaSource};
use bicycle_connectors::socket::{SocketConfig, SocketTextSink, SocketTextSource};
use bicycle_core::StreamMessage;
use bicycle_network::{ChannelId, NetworkConfig, NetworkEnvironment};
use bicycle_protocol::control::{ConnectorType, OperatorType, TaskDescriptor, TaskState};
use bicycle_runtime::{stream_channel, Emitter};
use dashmap::DashMap;
use futures::FutureExt;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use crate::plugin_loader::{PluginCache, PluginContext, PluginFunction};

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
}

impl TaskExecutor {
    pub fn new(worker_id: String, state_dir: String, slots: i32) -> Self {
        let network = Arc::new(NetworkEnvironment::new(NetworkConfig::default()));
        let plugin_cache = Arc::new(
            PluginCache::new(&state_dir).expect("Failed to create plugin cache"),
        );

        Self {
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
                Self::run_source_operator(task_id, &descriptor, status, internal_channels, internal_receivers).await
            }
            OperatorType::Sink => {
                Self::run_sink_operator(task_id, &descriptor, status, internal_channels, internal_receivers).await
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

    // =========================================================================
    // Source Operator - Reads from connectors (Socket, Kafka, etc.)
    // =========================================================================

    async fn run_source_operator(
        task_id: &str,
        descriptor: &TaskDescriptor,
        status: Arc<TaskStatus>,
        internal_channels: Arc<DashMap<String, mpsc::Sender<StreamMessage<Vec<u8>>>>>,
        internal_receivers: Arc<DashMap<String, Arc<tokio::sync::Mutex<mpsc::Receiver<StreamMessage<Vec<u8>>>>>>>,
    ) -> Result<()> {
        let connector_config = descriptor.connector_config.as_ref();
        let connector_type = connector_config
            .map(|c| ConnectorType::try_from(c.connector_type).unwrap_or(ConnectorType::None))
            .unwrap_or(ConnectorType::Generator);

        info!(
            task_id = %task_id,
            connector = ?connector_type,
            "Starting source operator"
        );

        match connector_type {
            ConnectorType::Socket => {
                Self::run_socket_source(task_id, descriptor, status, internal_channels, internal_receivers).await
            }
            ConnectorType::Kafka => {
                Self::run_kafka_source(task_id, descriptor, status, internal_channels, internal_receivers).await
            }
            ConnectorType::Generator => {
                Self::run_generator_source(task_id, descriptor, status, internal_channels, internal_receivers).await
            }
            _ => {
                // Default to generator for unsupported connectors
                Self::run_generator_source(task_id, descriptor, status, internal_channels, internal_receivers).await
            }
        }
    }

    /// Socket source - reads lines from TCP connections
    async fn run_socket_source(
        task_id: &str,
        descriptor: &TaskDescriptor,
        status: Arc<TaskStatus>,
        internal_channels: Arc<DashMap<String, mpsc::Sender<StreamMessage<Vec<u8>>>>>,
        _internal_receivers: Arc<DashMap<String, Arc<tokio::sync::Mutex<mpsc::Receiver<StreamMessage<Vec<u8>>>>>>>,
    ) -> Result<()> {
        let props = descriptor
            .connector_config
            .as_ref()
            .map(|c| &c.properties)
            .cloned()
            .unwrap_or_default();

        let port: u16 = props
            .get("port")
            .and_then(|p| p.parse().ok())
            .unwrap_or(9999);
        let host = props.get("host").cloned().unwrap_or_else(|| "0.0.0.0".to_string());

        info!(
            task_id = %task_id,
            host = %host,
            port = port,
            "Starting socket source"
        );

        let source_config = SocketConfig::new(&host, port);
        let mut source = SocketTextSource::new(source_config);

        // Create channel for receiving from socket
        let (tx, mut rx) = stream_channel::<String>(1024);
        let emitter = Emitter::new(tx);

        // Get downstream channels
        let downstream_tasks: Vec<String> = descriptor
            .output_gates
            .iter()
            .map(|g| g.downstream_task_id.clone())
            .collect();

        // Spawn socket reader
        let source_status = status.clone();
        let source_task_id = task_id.to_string();
        tokio::spawn(async move {
            loop {
                if source_status.is_cancelled() {
                    break;
                }
                if let Err(e) = source.run(emitter.clone()).await {
                    warn!(
                        task_id = %source_task_id,
                        error = %e,
                        "Source connection ended, waiting for reconnection"
                    );
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        });

        // Forward data to downstream operators
        loop {
            if status.is_cancelled() {
                return Ok(());
            }

            match tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
                Ok(Some(msg)) => {
                    if let StreamMessage::Data(line) = msg {
                        // JSON-encode the string for downstream operators (plugins expect JSON)
                        let bytes = serde_json::to_vec(&line).unwrap_or_else(|_| line.as_bytes().to_vec());
                        status.increment_records(1);
                        status.increment_bytes(bytes.len() as i64);

                        // Send to all downstream tasks (with retry for race condition)
                        for downstream_id in &downstream_tasks {
                            // Retry a few times if channel not found (race condition during startup)
                            let mut retries = 0;
                            loop {
                                if let Some(tx) = internal_channels.get(downstream_id) {
                                    let _ = tx.send(StreamMessage::Data(bytes.clone())).await;
                                    break;
                                } else if retries < 50 {
                                    retries += 1;
                                    tokio::time::sleep(Duration::from_millis(10)).await;
                                } else {
                                    warn!(task_id = %task_id, downstream = %downstream_id, "Downstream channel not found after retries");
                                    break;
                                }
                            }
                        }

                        debug!(task_id = %task_id, line = %line, "Source emitted record");
                    }
                }
                Ok(None) => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(_) => {
                    // Timeout - continue loop
                }
            }
        }
    }

    /// Kafka source - consumes messages from a Kafka topic
    async fn run_kafka_source(
        task_id: &str,
        descriptor: &TaskDescriptor,
        status: Arc<TaskStatus>,
        internal_channels: Arc<DashMap<String, mpsc::Sender<StreamMessage<Vec<u8>>>>>,
        _internal_receivers: Arc<DashMap<String, Arc<tokio::sync::Mutex<mpsc::Receiver<StreamMessage<Vec<u8>>>>>>>,
    ) -> Result<()> {
        let props = descriptor
            .connector_config
            .as_ref()
            .map(|c| &c.properties)
            .cloned()
            .unwrap_or_default();

        let brokers = props
            .get("bootstrap.servers")
            .or_else(|| props.get("brokers"))
            .cloned()
            .unwrap_or_else(|| "localhost:9092".to_string());
        let topic = props
            .get("topic")
            .cloned()
            .unwrap_or_else(|| "default".to_string());
        let group_id = props
            .get("group.id")
            .or_else(|| props.get("group_id"))
            .cloned()
            .unwrap_or_else(|| "bicycle-consumer".to_string());

        info!(
            task_id = %task_id,
            brokers = %brokers,
            topic = %topic,
            group_id = %group_id,
            "Starting Kafka source"
        );

        let mut kafka_config = KafkaConfig::new(&brokers)
            .with_group_id(&group_id)
            .with_topics(vec![topic.clone()]);

        // Pass through additional Kafka properties (skip our meta-properties)
        let skip_keys = ["bootstrap.servers", "brokers", "topic", "group.id", "group_id", "value.deserializer"];
        for (key, value) in &props {
            if !skip_keys.contains(&key.as_str()) {
                kafka_config.properties.insert(key.clone(), value.clone());
            }
        }

        let mut source = KafkaSource::new(kafka_config);
        source.connect()?;

        let downstream_tasks: Vec<String> = descriptor
            .output_gates
            .iter()
            .map(|g| g.downstream_task_id.clone())
            .collect();

        // Pass raw Kafka message bytes directly to downstream.
        // Data in Kafka is already JSON-encoded by the upstream sink, so no
        // re-encoding is needed — just forward the payload bytes as-is.
        let (tx, mut rx) = stream_channel::<Vec<u8>>(1024);
        let mut emitter = Emitter::new(tx);

        let source_status = status.clone();
        let source_task_id = task_id.to_string();
        tokio::spawn(async move {
            use rdkafka::consumer::Consumer;
            use rdkafka::message::Message;

            let consumer = source.take_consumer().expect("Consumer not connected");

            loop {
                if source_status.is_cancelled() {
                    break;
                }

                match consumer.recv().await {
                    Ok(message) => {
                        if let Some(payload) = message.payload() {
                            let bytes = payload.to_vec();
                            if let Err(e) = emitter.data(bytes).await {
                                warn!(task_id = %source_task_id, error = %e, "Failed to emit Kafka record");
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        debug!(task_id = %source_task_id, error = %e, "Kafka consumer error");
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            }
        });

        // Forward raw bytes to downstream operators
        loop {
            if status.is_cancelled() {
                return Ok(());
            }

            match tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
                Ok(Some(msg)) => {
                    if let StreamMessage::Data(bytes) = msg {
                        status.increment_records(1);
                        status.increment_bytes(bytes.len() as i64);

                        for downstream_id in &downstream_tasks {
                            let mut retries = 0;
                            loop {
                                if let Some(tx) = internal_channels.get(downstream_id) {
                                    let _ = tx.send(StreamMessage::Data(bytes.clone())).await;
                                    break;
                                } else if retries < 50 {
                                    retries += 1;
                                    tokio::time::sleep(Duration::from_millis(10)).await;
                                } else {
                                    warn!(task_id = %task_id, downstream = %downstream_id, "Downstream channel not found after retries");
                                    break;
                                }
                            }
                        }

                        debug!(task_id = %task_id, bytes = bytes.len(), "Kafka source emitted record");
                    }
                }
                Ok(None) => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(_) => {}
            }
        }
    }

    /// Generator source - generates test data
    async fn run_generator_source(
        task_id: &str,
        descriptor: &TaskDescriptor,
        status: Arc<TaskStatus>,
        internal_channels: Arc<DashMap<String, mpsc::Sender<StreamMessage<Vec<u8>>>>>,
        _internal_receivers: Arc<DashMap<String, Arc<tokio::sync::Mutex<mpsc::Receiver<StreamMessage<Vec<u8>>>>>>>,
    ) -> Result<()> {
        let props = descriptor
            .connector_config
            .as_ref()
            .map(|c| &c.properties)
            .cloned()
            .unwrap_or_default();

        let rate_ms: u64 = props
            .get("rate_ms")
            .and_then(|p| p.parse().ok())
            .unwrap_or(100);

        let words = vec!["hello", "world", "bicycle", "streaming", "data", "rust", "flink"];
        let downstream_tasks: Vec<String> = descriptor
            .output_gates
            .iter()
            .map(|g| g.downstream_task_id.clone())
            .collect();

        info!(
            task_id = %task_id,
            rate_ms = rate_ms,
            "Starting generator source"
        );

        let mut counter = 0u64;
        loop {
            if status.is_cancelled() {
                return Ok(());
            }

            let word = words[counter as usize % words.len()];
            let record = format!("{}", word);
            let bytes = record.as_bytes().to_vec();

            status.increment_records(1);
            status.increment_bytes(bytes.len() as i64);

            // Send to downstream
            for downstream_id in &downstream_tasks {
                if let Some(tx) = internal_channels.get(downstream_id) {
                    let _ = tx.send(StreamMessage::Data(bytes.clone())).await;
                }
            }

            counter += 1;
            if counter % 100 == 0 {
                debug!(task_id = %task_id, records = counter, "Generator progress");
            }

            tokio::time::sleep(Duration::from_millis(rate_ms)).await;
        }
    }

    // =========================================================================
    // Sink Operator - Writes to connectors (Socket, Kafka, etc.)
    // =========================================================================

    async fn run_sink_operator(
        task_id: &str,
        descriptor: &TaskDescriptor,
        status: Arc<TaskStatus>,
        internal_channels: Arc<DashMap<String, mpsc::Sender<StreamMessage<Vec<u8>>>>>,
        internal_receivers: Arc<DashMap<String, Arc<tokio::sync::Mutex<mpsc::Receiver<StreamMessage<Vec<u8>>>>>>>,
    ) -> Result<()> {
        let connector_config = descriptor.connector_config.as_ref();
        let connector_type = connector_config
            .map(|c| ConnectorType::try_from(c.connector_type).unwrap_or(ConnectorType::None))
            .unwrap_or(ConnectorType::None);

        info!(
            task_id = %task_id,
            connector = ?connector_type,
            "Starting sink operator"
        );

        match connector_type {
            ConnectorType::Socket => {
                Self::run_socket_sink(task_id, descriptor, status, internal_channels, internal_receivers).await
            }
            ConnectorType::Kafka => {
                Self::run_kafka_sink(task_id, descriptor, status, internal_channels, internal_receivers).await
            }
            _ => {
                // Console sink (default)
                Self::run_console_sink(task_id, descriptor, status, internal_channels, internal_receivers).await
            }
        }
    }

    /// Socket sink - writes data to TCP connections
    async fn run_socket_sink(
        task_id: &str,
        descriptor: &TaskDescriptor,
        status: Arc<TaskStatus>,
        _internal_channels: Arc<DashMap<String, mpsc::Sender<StreamMessage<Vec<u8>>>>>,
        internal_receivers: Arc<DashMap<String, Arc<tokio::sync::Mutex<mpsc::Receiver<StreamMessage<Vec<u8>>>>>>>,
    ) -> Result<()> {
        let props = descriptor
            .connector_config
            .as_ref()
            .map(|c| &c.properties)
            .cloned()
            .unwrap_or_default();

        let port: u16 = props
            .get("port")
            .and_then(|p| p.parse().ok())
            .unwrap_or(9998);
        let host = props.get("host").cloned().unwrap_or_else(|| "0.0.0.0".to_string());

        info!(
            task_id = %task_id,
            host = %host,
            port = port,
            "Starting socket sink"
        );

        let sink_config = SocketConfig::new(&host, port);
        let sink = Arc::new(SocketTextSink::new(sink_config));
        sink.start().await?;

        // Get our input receiver (created in deploy_task)
        let rx_arc = internal_receivers
            .get(task_id)
            .ok_or_else(|| anyhow::anyhow!("No receiver for task {}", task_id))?
            .clone();

        // Process incoming data
        loop {
            if status.is_cancelled() {
                return Ok(());
            }

            let mut rx = rx_arc.lock().await;
            match tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
                Ok(Some(StreamMessage::Data(bytes))) => {
                    drop(rx); // Release lock before writing
                    // Try to deserialize as JSON string, fallback to raw bytes
                    let line: String = serde_json::from_slice(&bytes)
                        .unwrap_or_else(|_| String::from_utf8_lossy(&bytes).to_string());
                    status.increment_records(1);
                    status.increment_bytes(bytes.len() as i64);

                    if let Err(e) = sink.write(&line).await {
                        debug!(task_id = %task_id, error = %e, "Sink write error");
                    }

                    debug!(task_id = %task_id, output = %line, "Sink output");
                }
                Ok(Some(_)) => {
                    // Other message types
                }
                Ok(None) => {
                    drop(rx);
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(_) => {
                    // Timeout
                }
            }
        }
    }

    /// Kafka sink - produces messages to a Kafka topic
    async fn run_kafka_sink(
        task_id: &str,
        descriptor: &TaskDescriptor,
        status: Arc<TaskStatus>,
        _internal_channels: Arc<DashMap<String, mpsc::Sender<StreamMessage<Vec<u8>>>>>,
        internal_receivers: Arc<DashMap<String, Arc<tokio::sync::Mutex<mpsc::Receiver<StreamMessage<Vec<u8>>>>>>>,
    ) -> Result<()> {
        let props = descriptor
            .connector_config
            .as_ref()
            .map(|c| &c.properties)
            .cloned()
            .unwrap_or_default();

        let brokers = props
            .get("bootstrap.servers")
            .or_else(|| props.get("brokers"))
            .cloned()
            .unwrap_or_else(|| "localhost:9092".to_string());
        let topic = props
            .get("topic")
            .cloned()
            .unwrap_or_else(|| "default".to_string());

        info!(
            task_id = %task_id,
            brokers = %brokers,
            topic = %topic,
            "Starting Kafka sink"
        );

        let mut kafka_config = KafkaConfig::new(&brokers).with_topic(&topic);

        // Pass through additional Kafka properties (skip our meta-properties)
        let skip_keys = ["bootstrap.servers", "brokers", "topic", "value.serializer"];
        for (key, value) in &props {
            if !skip_keys.contains(&key.as_str()) {
                kafka_config.properties.insert(key.clone(), value.clone());
            }
        }

        let mut sink = KafkaSink::new(kafka_config, task_id);
        sink.connect()?;
        let sink = Arc::new(sink);

        let rx_arc = internal_receivers
            .get(task_id)
            .ok_or_else(|| anyhow::anyhow!("No receiver for task {}", task_id))?
            .clone();

        loop {
            if status.is_cancelled() {
                return Ok(());
            }

            let mut rx = rx_arc.lock().await;
            match tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
                Ok(Some(StreamMessage::Data(bytes))) => {
                    drop(rx);
                    status.increment_records(1);
                    status.increment_bytes(bytes.len() as i64);

                    // Send bytes directly to Kafka (already JSON-encoded from upstream)
                    if let Err(e) = sink.send(None, &bytes).await {
                        warn!(task_id = %task_id, error = %e, "Kafka sink write error");
                    }

                    debug!(task_id = %task_id, "Kafka sink sent record");
                }
                Ok(Some(_)) => {}
                Ok(None) => {
                    drop(rx);
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(_) => {}
            }
        }
    }

    /// Console sink - prints data to stdout
    async fn run_console_sink(
        task_id: &str,
        _descriptor: &TaskDescriptor,
        status: Arc<TaskStatus>,
        _internal_channels: Arc<DashMap<String, mpsc::Sender<StreamMessage<Vec<u8>>>>>,
        internal_receivers: Arc<DashMap<String, Arc<tokio::sync::Mutex<mpsc::Receiver<StreamMessage<Vec<u8>>>>>>>,
    ) -> Result<()> {
        info!(task_id = %task_id, "Starting console sink");

        // Get our input receiver
        let rx_arc = internal_receivers
            .get(task_id)
            .ok_or_else(|| anyhow::anyhow!("No receiver for task {}", task_id))?
            .clone();

        loop {
            if status.is_cancelled() {
                return Ok(());
            }

            let mut rx = rx_arc.lock().await;
            match tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
                Ok(Some(StreamMessage::Data(bytes))) => {
                    drop(rx);
                    let line = String::from_utf8_lossy(&bytes);
                    status.increment_records(1);
                    status.increment_bytes(bytes.len() as i64);

                    println!("[SINK] {}", line);
                }
                Ok(Some(_)) => {}
                Ok(None) => {
                    drop(rx);
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(_) => {}
            }
        }
    }

    // =========================================================================
    // Processing Operators
    // =========================================================================

    /// Map operator - transforms each element
    async fn run_map_operator(
        task_id: &str,
        descriptor: &TaskDescriptor,
        status: Arc<TaskStatus>,
        internal_channels: Arc<DashMap<String, mpsc::Sender<StreamMessage<Vec<u8>>>>>,
        internal_receivers: Arc<DashMap<String, Arc<tokio::sync::Mutex<mpsc::Receiver<StreamMessage<Vec<u8>>>>>>>,
    ) -> Result<()> {
        // Parse map configuration (what transformation to apply)
        let config: MapConfig = if descriptor.operator_config.is_empty() {
            MapConfig::default()
        } else {
            serde_json::from_slice(&descriptor.operator_config).unwrap_or_default()
        };

        info!(
            task_id = %task_id,
            function = %config.function,
            "Starting map operator"
        );

        let downstream_tasks: Vec<String> = descriptor
            .output_gates
            .iter()
            .map(|g| g.downstream_task_id.clone())
            .collect();

        // Get input receiver
        let rx_arc = internal_receivers
            .get(task_id)
            .ok_or_else(|| anyhow::anyhow!("No receiver for task {}", task_id))?
            .clone();

        loop {
            if status.is_cancelled() {
                return Ok(());
            }

            let mut rx = rx_arc.lock().await;
            match tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
                Ok(Some(StreamMessage::Data(bytes))) => {
                    drop(rx);
                    let input = String::from_utf8_lossy(&bytes);
                    status.increment_records(1);
                    status.increment_bytes(bytes.len() as i64);

                    // Apply transformation based on function
                    let output = match config.function.as_str() {
                        "uppercase" => input.to_uppercase(),
                        "lowercase" => input.to_lowercase(),
                        "word_count" => {
                            let count = input.split_whitespace().count();
                            format!("{} -> {} words", input, count)
                        }
                        "reverse" => input.chars().rev().collect(),
                        _ => input.to_string(), // identity
                    };

                    let output_bytes = output.as_bytes().to_vec();

                    // Forward to downstream
                    Self::send_to_downstream(output_bytes, &downstream_tasks, &internal_channels).await;
                }
                Ok(Some(_)) => {}
                Ok(None) => {
                    drop(rx);
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(_) => {}
            }
        }
    }

    /// FlatMap operator - transforms each element into zero or more elements
    async fn run_flatmap_operator(
        task_id: &str,
        descriptor: &TaskDescriptor,
        status: Arc<TaskStatus>,
        internal_channels: Arc<DashMap<String, mpsc::Sender<StreamMessage<Vec<u8>>>>>,
        internal_receivers: Arc<DashMap<String, Arc<tokio::sync::Mutex<mpsc::Receiver<StreamMessage<Vec<u8>>>>>>>,
    ) -> Result<()> {
        let config: FlatMapConfig = if descriptor.operator_config.is_empty() {
            FlatMapConfig::default()
        } else {
            serde_json::from_slice(&descriptor.operator_config).unwrap_or_default()
        };

        info!(
            task_id = %task_id,
            function = %config.function,
            "Starting flat_map operator"
        );

        let downstream_tasks: Vec<String> = descriptor
            .output_gates
            .iter()
            .map(|g| g.downstream_task_id.clone())
            .collect();

        let rx_arc = internal_receivers
            .get(task_id)
            .ok_or_else(|| anyhow::anyhow!("No receiver for task {}", task_id))?
            .clone();

        loop {
            if status.is_cancelled() {
                return Ok(());
            }

            let mut rx = rx_arc.lock().await;
            match tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
                Ok(Some(StreamMessage::Data(bytes))) => {
                    drop(rx);
                    let input = String::from_utf8_lossy(&bytes);
                    status.increment_records(1);
                    status.increment_bytes(bytes.len() as i64);

                    // Apply flat map function
                    let outputs: Vec<String> = match config.function.as_str() {
                        "split_words" => input.split_whitespace().map(|s| s.to_string()).collect(),
                        "split_lines" => input.lines().map(|s| s.to_string()).collect(),
                        "split_csv" => input.split(',').map(|s| s.trim().to_string()).collect(),
                        _ => vec![input.to_string()],
                    };

                    // Forward each output to downstream
                    for output in outputs {
                        let output_bytes = output.as_bytes().to_vec();
                        Self::send_to_downstream(output_bytes, &downstream_tasks, &internal_channels).await;
                    }
                }
                Ok(Some(_)) => {}
                Ok(None) => {
                    drop(rx);
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(_) => {}
            }
        }
    }

    /// Filter operator - keeps elements matching a predicate
    async fn run_filter_operator(
        task_id: &str,
        descriptor: &TaskDescriptor,
        status: Arc<TaskStatus>,
        internal_channels: Arc<DashMap<String, mpsc::Sender<StreamMessage<Vec<u8>>>>>,
        internal_receivers: Arc<DashMap<String, Arc<tokio::sync::Mutex<mpsc::Receiver<StreamMessage<Vec<u8>>>>>>>,
    ) -> Result<()> {
        let config: FilterConfig = if descriptor.operator_config.is_empty() {
            FilterConfig::default()
        } else {
            serde_json::from_slice(&descriptor.operator_config).unwrap_or_default()
        };

        info!(
            task_id = %task_id,
            predicate = %config.predicate,
            "Starting filter operator"
        );

        let downstream_tasks: Vec<String> = descriptor
            .output_gates
            .iter()
            .map(|g| g.downstream_task_id.clone())
            .collect();

        let rx_arc = internal_receivers
            .get(task_id)
            .ok_or_else(|| anyhow::anyhow!("No receiver for task {}", task_id))?
            .clone();

        loop {
            if status.is_cancelled() {
                return Ok(());
            }

            let mut rx = rx_arc.lock().await;
            match tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
                Ok(Some(StreamMessage::Data(bytes))) => {
                    drop(rx);
                    let input = String::from_utf8_lossy(&bytes);
                    status.increment_records(1);
                    status.increment_bytes(bytes.len() as i64);

                    // Apply filter predicate
                    let keep = match config.predicate.as_str() {
                        "non_empty" => !input.trim().is_empty(),
                        "contains" => input.contains(&config.value),
                        "starts_with" => input.starts_with(&config.value),
                        "ends_with" => input.ends_with(&config.value),
                        "min_length" => {
                            input.len() >= config.value.parse::<usize>().unwrap_or(0)
                        }
                        _ => true, // keep all by default
                    };

                    if keep {
                        Self::send_to_downstream(bytes.clone(), &downstream_tasks, &internal_channels).await;
                    }
                }
                Ok(Some(_)) => {}
                Ok(None) => {
                    drop(rx);
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(_) => {}
            }
        }
    }

    /// KeyBy operator - partitions data by key
    async fn run_keyby_operator(
        task_id: &str,
        descriptor: &TaskDescriptor,
        status: Arc<TaskStatus>,
        internal_channels: Arc<DashMap<String, mpsc::Sender<StreamMessage<Vec<u8>>>>>,
        internal_receivers: Arc<DashMap<String, Arc<tokio::sync::Mutex<mpsc::Receiver<StreamMessage<Vec<u8>>>>>>>,
    ) -> Result<()> {
        info!(task_id = %task_id, "Starting key_by operator");

        let downstream_tasks: Vec<String> = descriptor
            .output_gates
            .iter()
            .map(|g| g.downstream_task_id.clone())
            .collect();

        let rx_arc = internal_receivers
            .get(task_id)
            .ok_or_else(|| anyhow::anyhow!("No receiver for task {}", task_id))?
            .clone();

        loop {
            if status.is_cancelled() {
                return Ok(());
            }

            let mut rx = rx_arc.lock().await;
            match tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
                Ok(Some(StreamMessage::Data(bytes))) => {
                    drop(rx);
                    status.increment_records(1);
                    status.increment_bytes(bytes.len() as i64);

                    // For now, just forward (in real impl, would partition by key hash)
                    Self::send_to_downstream(bytes.clone(), &downstream_tasks, &internal_channels).await;
                }
                Ok(Some(_)) => {}
                Ok(None) => {
                    drop(rx);
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(_) => {}
            }
        }
    }

    /// Window operator - groups elements into time windows
    async fn run_window_operator(
        task_id: &str,
        descriptor: &TaskDescriptor,
        status: Arc<TaskStatus>,
        internal_channels: Arc<DashMap<String, mpsc::Sender<StreamMessage<Vec<u8>>>>>,
        internal_receivers: Arc<DashMap<String, Arc<tokio::sync::Mutex<mpsc::Receiver<StreamMessage<Vec<u8>>>>>>>,
    ) -> Result<()> {
        let config: WindowConfig = if descriptor.operator_config.is_empty() {
            WindowConfig::default()
        } else {
            serde_json::from_slice(&descriptor.operator_config).unwrap_or_default()
        };

        info!(
            task_id = %task_id,
            window_ms = config.window_ms,
            "Starting window operator"
        );

        let downstream_tasks: Vec<String> = descriptor
            .output_gates
            .iter()
            .map(|g| g.downstream_task_id.clone())
            .collect();

        let rx_arc = internal_receivers
            .get(task_id)
            .ok_or_else(|| anyhow::anyhow!("No receiver for task {}", task_id))?
            .clone();

        let mut window_buffer: Vec<Vec<u8>> = Vec::new();
        let mut window_start = Instant::now();
        let window_duration = Duration::from_millis(config.window_ms);

        loop {
            if status.is_cancelled() {
                return Ok(());
            }

            // Check if window should close
            if window_start.elapsed() >= window_duration && !window_buffer.is_empty() {
                // Emit window result
                let result = format!("Window: {} elements", window_buffer.len());
                let result_bytes = result.as_bytes().to_vec();

                Self::send_to_downstream(result_bytes.clone(), &downstream_tasks, &internal_channels).await;

                window_buffer.clear();
                window_start = Instant::now();
            }

            let mut rx = rx_arc.lock().await;
            match tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
                Ok(Some(StreamMessage::Data(bytes))) => {
                    drop(rx);
                    status.increment_records(1);
                    status.increment_bytes(bytes.len() as i64);
                    window_buffer.push(bytes);
                }
                Ok(Some(_)) => {}
                Ok(None) => {
                    drop(rx);
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(_) => {}
            }
        }
    }

    /// Count operator - counts elements by key
    async fn run_count_operator(
        task_id: &str,
        descriptor: &TaskDescriptor,
        status: Arc<TaskStatus>,
        internal_channels: Arc<DashMap<String, mpsc::Sender<StreamMessage<Vec<u8>>>>>,
        internal_receivers: Arc<DashMap<String, Arc<tokio::sync::Mutex<mpsc::Receiver<StreamMessage<Vec<u8>>>>>>>,
    ) -> Result<()> {
        info!(task_id = %task_id, "Starting count operator");

        let downstream_tasks: Vec<String> = descriptor
            .output_gates
            .iter()
            .map(|g| g.downstream_task_id.clone())
            .collect();

        let rx_arc = internal_receivers
            .get(task_id)
            .ok_or_else(|| anyhow::anyhow!("No receiver for task {}", task_id))?
            .clone();

        let mut counts: HashMap<String, u64> = HashMap::new();

        loop {
            if status.is_cancelled() {
                return Ok(());
            }

            let mut rx = rx_arc.lock().await;
            match tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
                Ok(Some(StreamMessage::Data(bytes))) => {
                    drop(rx);
                    let key = String::from_utf8_lossy(&bytes).to_lowercase();
                    status.increment_records(1);
                    status.increment_bytes(bytes.len() as i64);

                    let count = counts.entry(key.clone()).or_insert(0);
                    *count += 1;

                    // Emit count update
                    let result = format!("{}:{}", key, count);
                    let result_bytes = result.as_bytes().to_vec();

                    for downstream_id in &downstream_tasks {
                        if let Some(tx) = internal_channels.get(downstream_id) {
                            let _ = tx.send(StreamMessage::Data(result_bytes.clone())).await;
                        }
                    }
                }
                Ok(Some(_)) => {}
                Ok(None) => {
                    drop(rx);
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(_) => {}
            }
        }
    }

    /// Reduce operator - reduces elements using an aggregation function
    async fn run_reduce_operator(
        task_id: &str,
        descriptor: &TaskDescriptor,
        status: Arc<TaskStatus>,
        internal_channels: Arc<DashMap<String, mpsc::Sender<StreamMessage<Vec<u8>>>>>,
        internal_receivers: Arc<DashMap<String, Arc<tokio::sync::Mutex<mpsc::Receiver<StreamMessage<Vec<u8>>>>>>>,
    ) -> Result<()> {
        info!(task_id = %task_id, "Starting reduce operator");

        let downstream_tasks: Vec<String> = descriptor
            .output_gates
            .iter()
            .map(|g| g.downstream_task_id.clone())
            .collect();

        let rx_arc = internal_receivers
            .get(task_id)
            .ok_or_else(|| anyhow::anyhow!("No receiver for task {}", task_id))?
            .clone();

        loop {
            if status.is_cancelled() {
                return Ok(());
            }

            let mut rx = rx_arc.lock().await;
            match tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
                Ok(Some(StreamMessage::Data(bytes))) => {
                    drop(rx);
                    status.increment_records(1);
                    status.increment_bytes(bytes.len() as i64);

                    // Pass through for now
                    Self::send_to_downstream(bytes.clone(), &downstream_tasks, &internal_channels).await;
                }
                Ok(Some(_)) => {}
                Ok(None) => {
                    drop(rx);
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(_) => {}
            }
        }
    }

    /// Plugin operator - executes native (.so) plugin functions
    async fn run_plugin_operator(
        task_id: &str,
        job_id: &str,
        descriptor: &TaskDescriptor,
        status: Arc<TaskStatus>,
        internal_channels: Arc<DashMap<String, mpsc::Sender<StreamMessage<Vec<u8>>>>>,
        internal_receivers: Arc<DashMap<String, Arc<tokio::sync::Mutex<mpsc::Receiver<StreamMessage<Vec<u8>>>>>>>,
        plugin_cache: Arc<PluginCache>,
    ) -> Result<()> {
        info!(
            task_id = %task_id,
            job_id = %job_id,
            function = %descriptor.plugin_function,
            is_rich = descriptor.is_rich_function,
            "Starting plugin operator"
        );

        // Load the plugin
        let plugin = plugin_cache.get_or_load(job_id, &descriptor.plugin_module)?;

        // Create function instance
        let mut plugin_function = PluginFunction::new(
            plugin.clone(),
            &descriptor.plugin_function,
            &descriptor.operator_config,
        )?;

        // Create context
        let ctx = PluginContext {
            job_id: job_id.to_string(),
            task_id: task_id.to_string(),
            subtask_index: descriptor.subtask_index,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0),
            current_key: None,
        };

        // Open the function
        plugin_function.open(&ctx)?;

        let downstream_tasks: Vec<String> = descriptor
            .output_gates
            .iter()
            .map(|g| g.downstream_task_id.clone())
            .collect();

        let rx_arc = internal_receivers
            .get(task_id)
            .ok_or_else(|| anyhow::anyhow!("No receiver for task {}", task_id))?
            .clone();

        loop {
            if status.is_cancelled() {
                plugin_function.close()?;
                return Ok(());
            }

            let mut rx = rx_arc.lock().await;
            match tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
                Ok(Some(StreamMessage::Data(input_bytes))) => {
                    drop(rx);
                    status.increment_records(1);
                    status.increment_bytes(input_bytes.len() as i64);

                    // Update context timestamp
                    let ctx = PluginContext {
                        job_id: job_id.to_string(),
                        task_id: task_id.to_string(),
                        subtask_index: descriptor.subtask_index,
                        timestamp: std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_millis() as i64)
                            .unwrap_or(0),
                        current_key: None,
                    };

                    // Process through plugin
                    match plugin_function.process(&input_bytes, &ctx) {
                        Ok(output_bytes) => {
                            // Output is a JSON array of outputs, deserialize and send each
                            if !output_bytes.is_empty() {
                                // Try to deserialize as array of outputs
                                if let Ok(outputs) = serde_json::from_slice::<Vec<serde_json::Value>>(&output_bytes) {
                                    for output in outputs {
                                        let output_data = serde_json::to_vec(&output).unwrap_or_default();
                                        Self::send_to_downstream(
                                            output_data,
                                            &downstream_tasks,
                                            &internal_channels,
                                        )
                                        .await;
                                    }
                                } else {
                                    // If not an array, send raw bytes
                                    Self::send_to_downstream(
                                        output_bytes,
                                        &downstream_tasks,
                                        &internal_channels,
                                    )
                                    .await;
                                }
                            }
                        }
                        Err(e) => {
                            error!(
                                task_id = %task_id,
                                error = %e,
                                "Plugin process error"
                            );
                        }
                    }
                }
                Ok(Some(_)) => {}
                Ok(None) => {
                    drop(rx);
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(_) => {}
            }
        }
    }

    /// Passthrough operator for unknown types
    async fn run_passthrough_operator(
        task_id: &str,
        descriptor: &TaskDescriptor,
        status: Arc<TaskStatus>,
        internal_channels: Arc<DashMap<String, mpsc::Sender<StreamMessage<Vec<u8>>>>>,
        internal_receivers: Arc<DashMap<String, Arc<tokio::sync::Mutex<mpsc::Receiver<StreamMessage<Vec<u8>>>>>>>,
    ) -> Result<()> {
        info!(
            task_id = %task_id,
            operator_type = descriptor.operator_type,
            "Starting passthrough operator"
        );

        let downstream_tasks: Vec<String> = descriptor
            .output_gates
            .iter()
            .map(|g| g.downstream_task_id.clone())
            .collect();

        let rx_arc = internal_receivers
            .get(task_id)
            .ok_or_else(|| anyhow::anyhow!("No receiver for task {}", task_id))?
            .clone();

        loop {
            if status.is_cancelled() {
                return Ok(());
            }

            let mut rx = rx_arc.lock().await;
            match tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
                Ok(Some(StreamMessage::Data(bytes))) => {
                    drop(rx);
                    status.increment_records(1);
                    status.increment_bytes(bytes.len() as i64);

                    Self::send_to_downstream(bytes.clone(), &downstream_tasks, &internal_channels).await;
                }
                Ok(Some(_)) => {}
                Ok(None) => {
                    drop(rx);
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(_) => {}
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

    /// Trigger a checkpoint for a task.
    pub async fn trigger_checkpoint(&self, task_id: &str, checkpoint_id: i64) -> Result<()> {
        if let Some(_status) = self.tasks.get(task_id) {
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

// ============================================================================
// Operator Configuration Structs
// ============================================================================

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct MapConfig {
    /// Function to apply: "uppercase", "lowercase", "word_count", "reverse", or "identity"
    pub function: String,
}

impl Default for MapConfig {
    fn default() -> Self {
        Self {
            function: "identity".to_string(),
        }
    }
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct FlatMapConfig {
    /// Function to apply: "split_words", "split_lines", "split_csv"
    pub function: String,
}

impl Default for FlatMapConfig {
    fn default() -> Self {
        Self {
            function: "split_words".to_string(),
        }
    }
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct FilterConfig {
    /// Predicate: "non_empty", "contains", "starts_with", "ends_with", "min_length"
    pub predicate: String,
    /// Value for predicates that need it
    pub value: String,
}

impl Default for FilterConfig {
    fn default() -> Self {
        Self {
            predicate: "non_empty".to_string(),
            value: String::new(),
        }
    }
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct WindowConfig {
    /// Window size in milliseconds
    pub window_ms: u64,
}

impl Default for WindowConfig {
    fn default() -> Self {
        Self { window_ms: 5000 }
    }
}
