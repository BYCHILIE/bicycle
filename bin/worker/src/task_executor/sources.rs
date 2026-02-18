// =========================================================================
// Source Operator - Reads from connectors (Socket, Kafka, etc.)
// =========================================================================

use anyhow::Result;
use bicycle_connectors::kafka::{KafkaConfig, KafkaSource};
use bicycle_connectors::socket::{SocketConfig, SocketTextSource};
use bicycle_core::StreamMessage;
use bicycle_protocol::control::{ConnectorType, TaskDescriptor};
use bicycle_runtime::{stream_channel, Emitter};
use dashmap::DashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use super::schema::apply_source_schema;
use crate::plugin_loader::PluginCache;

use super::{TaskExecutor, TaskStatus};

impl TaskExecutor {
    pub(super) async fn run_source_operator(
        task_id: &str,
        job_id: &str,
        descriptor: &TaskDescriptor,
        status: Arc<TaskStatus>,
        internal_channels: Arc<DashMap<String, mpsc::Sender<StreamMessage<Vec<u8>>>>>,
        internal_receivers: Arc<DashMap<String, Arc<tokio::sync::Mutex<mpsc::Receiver<StreamMessage<Vec<u8>>>>>>>,
        plugin_cache: Arc<PluginCache>,
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
                Self::run_kafka_source(task_id, job_id, descriptor, status, internal_channels, internal_receivers, plugin_cache).await
            }
            ConnectorType::Generator => {
                Self::run_generator_source(task_id, descriptor, status, internal_channels, internal_receivers).await
            }
            ConnectorType::Pulsar => {
                warn!(task_id = %task_id, "Pulsar connector not yet available at runtime");
                Err(anyhow::anyhow!("Pulsar source connector not yet available at runtime"))
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

        // Get internal receiver for barriers injected by trigger_checkpoint
        let ctrl_rx_arc = internal_receivers
            .get(task_id)
            .map(|e| e.clone());

        // Forward data to downstream operators
        let mut record_count: u64 = 0;
        loop {
            if status.is_cancelled() {
                return Ok(());
            }

            // Check for barriers from internal channel (non-blocking)
            if let Some(ref ctrl_rx) = ctrl_rx_arc {
                let mut ctrl = ctrl_rx.lock().await;
                while let Ok(msg) = ctrl.try_recv() {
                    match msg {
                        msg @ StreamMessage::Barrier(_) | msg @ StreamMessage::Watermark(_) => {
                            Self::send_control_downstream(msg, &downstream_tasks, &internal_channels).await;
                        }
                        _ => {}
                    }
                }
            }

            match tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
                Ok(Some(msg)) => {
                    if let StreamMessage::Data(line) = msg {
                        // JSON-encode the string for downstream operators (plugins expect JSON)
                        let bytes = serde_json::to_vec(&line).unwrap_or_else(|_| line.as_bytes().to_vec());
                        status.increment_records(1);
                        status.increment_bytes(bytes.len() as i64);
                        record_count += 1;

                        Self::send_to_downstream(bytes, &downstream_tasks, &internal_channels).await;

                        // Emit periodic watermark (wall-clock based)
                        if record_count % 100 == 0 {
                            let now_ms = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_millis() as u64;
                            let watermark = StreamMessage::Watermark(now_ms.saturating_sub(5000));
                            Self::send_control_downstream(watermark, &downstream_tasks, &internal_channels).await;
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
        job_id: &str,
        descriptor: &TaskDescriptor,
        status: Arc<TaskStatus>,
        internal_channels: Arc<DashMap<String, mpsc::Sender<StreamMessage<Vec<u8>>>>>,
        internal_receivers: Arc<DashMap<String, Arc<tokio::sync::Mutex<mpsc::Receiver<StreamMessage<Vec<u8>>>>>>>,
        plugin_cache: Arc<PluginCache>,
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

        // Pass through additional Kafka properties (skip our meta-properties and schema keys)
        let skip_keys = [
            "bootstrap.servers", "brokers", "topic", "group.id", "group_id",
            "value.deserializer", "schema.decode.fn",
        ];
        for (key, value) in &props {
            if !skip_keys.contains(&key.as_str()) {
                kafka_config.properties.insert(key.clone(), value.clone());
            }
        }

        // Load plugin for schema dispatch (only when plugin module is present)
        let plugin = if !descriptor.plugin_module.is_empty() && descriptor.plugin_type == "native" {
            plugin_cache.get_or_load(job_id, &descriptor.plugin_module).ok()
        } else {
            None
        };

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
                            debug!(
                                task_id = %source_task_id,
                                payload_len = bytes.len(),
                                payload_preview = %String::from_utf8_lossy(&bytes[..bytes.len().min(100)]),
                                "Kafka source received message"
                            );
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

        // Get internal receiver for barriers injected by trigger_checkpoint
        let ctrl_rx_arc = internal_receivers
            .get(task_id)
            .map(|e| e.clone());

        // Forward bytes to downstream operators, applying schema conversion at boundary
        let mut record_count: u64 = 0;
        loop {
            if status.is_cancelled() {
                return Ok(());
            }

            // Check for barriers from internal channel (non-blocking)
            if let Some(ref ctrl_rx) = ctrl_rx_arc {
                let mut ctrl = ctrl_rx.lock().await;
                while let Ok(msg) = ctrl.try_recv() {
                    match msg {
                        msg @ StreamMessage::Barrier(_) | msg @ StreamMessage::Watermark(_) => {
                            Self::send_control_downstream(msg, &downstream_tasks, &internal_channels).await;
                        }
                        _ => {}
                    }
                }
            }

            match tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
                Ok(Some(msg)) => {
                    if let StreamMessage::Data(bytes) = msg {
                        status.increment_records(1);
                        status.increment_bytes(bytes.len() as i64);
                        record_count += 1;

                        // Apply schema: convert raw Kafka bytes → internal JSON bytes
                        let bytes = apply_source_schema(bytes, &props, plugin.as_deref());

                        Self::send_to_downstream(bytes, &downstream_tasks, &internal_channels).await;

                        // Emit periodic watermark based on message timestamps (subtract 5s lateness)
                        if record_count % 100 == 0 {
                            let now_ms = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_millis() as u64;
                            let watermark = StreamMessage::Watermark(now_ms.saturating_sub(5000));
                            Self::send_control_downstream(watermark, &downstream_tasks, &internal_channels).await;
                        }

                        debug!(task_id = %task_id, "Kafka source emitted record");
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
        internal_receivers: Arc<DashMap<String, Arc<tokio::sync::Mutex<mpsc::Receiver<StreamMessage<Vec<u8>>>>>>>,
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

        // Get internal receiver for barriers injected by trigger_checkpoint
        let ctrl_rx_arc = internal_receivers
            .get(task_id)
            .map(|e| e.clone());

        let mut counter = 0u64;
        loop {
            if status.is_cancelled() {
                return Ok(());
            }

            // Check for barriers from internal channel (non-blocking)
            if let Some(ref ctrl_rx) = ctrl_rx_arc {
                let mut ctrl = ctrl_rx.lock().await;
                while let Ok(msg) = ctrl.try_recv() {
                    match msg {
                        msg @ StreamMessage::Barrier(_) | msg @ StreamMessage::Watermark(_) => {
                            Self::send_control_downstream(msg, &downstream_tasks, &internal_channels).await;
                        }
                        _ => {}
                    }
                }
            }

            let word = words[counter as usize % words.len()];
            let record = format!("{}", word);
            let bytes = record.as_bytes().to_vec();

            status.increment_records(1);
            status.increment_bytes(bytes.len() as i64);

            Self::send_to_downstream(bytes, &downstream_tasks, &internal_channels).await;

            counter += 1;
            if counter % 100 == 0 {
                debug!(task_id = %task_id, records = counter, "Generator progress");

                // Emit periodic watermark (wall-clock based)
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;
                let watermark = StreamMessage::Watermark(now_ms.saturating_sub(5000));
                Self::send_control_downstream(watermark, &downstream_tasks, &internal_channels).await;
            }

            tokio::time::sleep(Duration::from_millis(rate_ms)).await;
        }
    }
}
