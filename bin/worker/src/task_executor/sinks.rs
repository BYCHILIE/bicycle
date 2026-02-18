// =========================================================================
// Sink Operator - Writes to connectors (Socket, Kafka, etc.)
// =========================================================================

use anyhow::Result;
use bicycle_connectors::kafka::{KafkaConfig, KafkaSink, TransactionalKafkaSink};
use bicycle_connectors::socket::{SocketConfig, SocketTextSink};
use bicycle_core::StreamMessage;
use bicycle_protocol::control::{ConnectorType, TaskDescriptor};
use bicycle_runtime::sink::{ExactlyOnceSinkWriter, TransactionId, TransactionalSink};
use dashmap::DashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use super::schema::apply_sink_schema;
use crate::plugin_loader::PluginCache;

use super::{TaskExecutor, TaskStatus};

impl TaskExecutor {
    pub(super) async fn run_sink_operator(
        task_id: &str,
        job_id: &str,
        descriptor: &TaskDescriptor,
        status: Arc<TaskStatus>,
        internal_channels: Arc<DashMap<String, mpsc::Sender<StreamMessage<Vec<u8>>>>>,
        internal_receivers: Arc<DashMap<String, Arc<tokio::sync::Mutex<mpsc::Receiver<StreamMessage<Vec<u8>>>>>>>,
        checkpoint_ack_tx: mpsc::Sender<(String, String, i64)>,
        plugin_cache: Arc<PluginCache>,
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
                Self::run_socket_sink(task_id, job_id, descriptor, status, internal_channels, internal_receivers, checkpoint_ack_tx).await
            }
            ConnectorType::Kafka => {
                Self::run_kafka_sink(task_id, job_id, descriptor, status, internal_channels, internal_receivers, checkpoint_ack_tx, plugin_cache).await
            }
            ConnectorType::Pulsar => {
                warn!(task_id = %task_id, "Pulsar connector not yet available at runtime");
                Err(anyhow::anyhow!("Pulsar sink connector not yet available at runtime"))
            }
            _ => {
                // Console sink (default)
                Self::run_console_sink(task_id, job_id, descriptor, status, internal_channels, internal_receivers, checkpoint_ack_tx).await
            }
        }
    }

    /// Socket sink - writes data to TCP connections
    async fn run_socket_sink(
        task_id: &str,
        job_id: &str,
        descriptor: &TaskDescriptor,
        status: Arc<TaskStatus>,
        _internal_channels: Arc<DashMap<String, mpsc::Sender<StreamMessage<Vec<u8>>>>>,
        internal_receivers: Arc<DashMap<String, Arc<tokio::sync::Mutex<mpsc::Receiver<StreamMessage<Vec<u8>>>>>>>,
        checkpoint_ack_tx: mpsc::Sender<(String, String, i64)>,
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
        let mut last_acked_checkpoint: u64 = 0;
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
                Ok(Some(StreamMessage::Barrier(barrier))) => {
                    drop(rx);
                    // Deduplicate: only ack each checkpoint once
                    if barrier.checkpoint_id > last_acked_checkpoint {
                        last_acked_checkpoint = barrier.checkpoint_id;
                        info!(task_id = %task_id, checkpoint_id = barrier.checkpoint_id, "Sink received barrier");
                        let _ = checkpoint_ack_tx.send((
                            job_id.to_string(),
                            task_id.to_string(),
                            barrier.checkpoint_id as i64,
                        )).await;
                    }
                }
                Ok(Some(_)) => {
                    // Other message types (watermarks, latency markers)
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
    /// Supports both at-least-once (default) and exactly-once delivery.
    async fn run_kafka_sink(
        task_id: &str,
        job_id: &str,
        descriptor: &TaskDescriptor,
        status: Arc<TaskStatus>,
        _internal_channels: Arc<DashMap<String, mpsc::Sender<StreamMessage<Vec<u8>>>>>,
        internal_receivers: Arc<DashMap<String, Arc<tokio::sync::Mutex<mpsc::Receiver<StreamMessage<Vec<u8>>>>>>>,
        checkpoint_ack_tx: mpsc::Sender<(String, String, i64)>,
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

        let exactly_once = props
            .get("delivery.guarantee")
            .map(|v| v == "exactly-once")
            .unwrap_or(false);

        info!(
            task_id = %task_id,
            brokers = %brokers,
            topic = %topic,
            exactly_once = exactly_once,
            "Starting Kafka sink"
        );

        let mut kafka_config = KafkaConfig::new(&brokers).with_topic(&topic);

        // For exactly-once, set the transactional ID prefix.
        // Uses a custom prefix from properties if provided, otherwise auto-generates
        // from job_id + task_id (like Flink's transactional.id.prefix).
        if exactly_once {
            let txn_prefix = props
                .get("transactional.id.prefix")
                .cloned()
                .unwrap_or_else(|| format!("bicycle-{}-{}", job_id, task_id));
            kafka_config = kafka_config.with_exactly_once(txn_prefix);
        }

        // Pass through additional Kafka properties (skip our meta-properties and schema keys)
        let skip_keys = [
            "bootstrap.servers", "brokers", "topic",
            "value.serializer", "schema.encode.fn",
            "delivery.guarantee", "transactional.id.prefix",
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

        let rx_arc = internal_receivers
            .get(task_id)
            .ok_or_else(|| anyhow::anyhow!("No receiver for task {}", task_id))?
            .clone();

        if exactly_once {
            // Exactly-once mode using TransactionalKafkaSink + ExactlyOnceSinkWriter
            use bicycle_connectors::kafka::KafkaRecord;

            let txn_sink = TransactionalKafkaSink::new(kafka_config, task_id)?;
            let txn_sink = Arc::new(txn_sink);

            // _commit_rx is intentionally unused: commit notifications are handled by
            // notify_checkpoint_complete() called directly after each barrier, not via a channel.
            let (commit_tx, _commit_rx) = mpsc::channel::<TransactionId>(64);
            let writer = ExactlyOnceSinkWriter::new(txn_sink, commit_tx);

            let mut last_acked_checkpoint: u64 = 0;
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

                        // Apply schema: convert internal JSON bytes → Kafka payload bytes
                        let bytes = apply_sink_schema(bytes, &props, plugin.as_deref());

                        let record = KafkaRecord::new(bytes);
                        if let Err(e) = writer.process(StreamMessage::Data(record)).await {
                            warn!(task_id = %task_id, error = %e, "Exactly-once sink write error");
                        }
                    }
                    Ok(Some(StreamMessage::Barrier(barrier))) => {
                        drop(rx);
                        // Deduplicate: only process each checkpoint once
                        if barrier.checkpoint_id <= last_acked_checkpoint {
                            continue;
                        }
                        last_acked_checkpoint = barrier.checkpoint_id;
                        info!(task_id = %task_id, checkpoint_id = barrier.checkpoint_id, "Exactly-once Kafka sink received barrier");

                        // Process barrier through exactly-once writer (pre-commit + new txn)
                        if let Err(e) = writer.process(StreamMessage::Barrier(barrier.clone())).await {
                            warn!(task_id = %task_id, error = %e, "Exactly-once barrier processing error");
                        }

                        // Notify checkpoint complete (commit the pre-committed txn)
                        if let Err(e) = writer.notify_checkpoint_complete(barrier.checkpoint_id).await {
                            warn!(task_id = %task_id, error = %e, "Exactly-once commit error");
                        }

                        // Send ack
                        let _ = checkpoint_ack_tx.send((
                            job_id.to_string(),
                            task_id.to_string(),
                            barrier.checkpoint_id as i64,
                        )).await;
                    }
                    Ok(Some(_)) => {
                        drop(rx);
                        // Sinks don't forward watermarks — skip non-data, non-barrier messages.
                    }
                    Ok(None) => {
                        drop(rx);
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                    Err(_) => {}
                }
            }
        } else {
            // At-least-once mode (default)
            let mut sink = KafkaSink::new(kafka_config, task_id);
            sink.connect()?;
            let sink = Arc::new(sink);

            let mut last_acked_checkpoint: u64 = 0;
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

                        // Apply schema: convert internal JSON bytes → Kafka payload bytes
                        let bytes = apply_sink_schema(bytes, &props, plugin.as_deref());

                        debug!(
                            task_id = %task_id,
                            bytes_len = bytes.len(),
                            preview = %String::from_utf8_lossy(&bytes[..bytes.len().min(100)]),
                            "Kafka sink sent record"
                        );

                        if let Err(e) = sink.send(None, &bytes).await {
                            warn!(task_id = %task_id, error = %e, "Kafka sink write error");
                        }
                    }
                    Ok(Some(StreamMessage::Barrier(barrier))) => {
                        drop(rx);
                        // Deduplicate: only ack each checkpoint once
                        if barrier.checkpoint_id <= last_acked_checkpoint {
                            continue;
                        }
                        last_acked_checkpoint = barrier.checkpoint_id;
                        info!(task_id = %task_id, checkpoint_id = barrier.checkpoint_id, "Kafka sink received barrier");
                        let _ = sink.flush(Duration::from_secs(5));
                        let _ = checkpoint_ack_tx.send((
                            job_id.to_string(),
                            task_id.to_string(),
                            barrier.checkpoint_id as i64,
                        )).await;
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
    }

    /// Console sink - prints data to stdout
    async fn run_console_sink(
        task_id: &str,
        job_id: &str,
        _descriptor: &TaskDescriptor,
        status: Arc<TaskStatus>,
        _internal_channels: Arc<DashMap<String, mpsc::Sender<StreamMessage<Vec<u8>>>>>,
        internal_receivers: Arc<DashMap<String, Arc<tokio::sync::Mutex<mpsc::Receiver<StreamMessage<Vec<u8>>>>>>>,
        checkpoint_ack_tx: mpsc::Sender<(String, String, i64)>,
    ) -> Result<()> {
        info!(task_id = %task_id, "Starting console sink");

        // Get our input receiver
        let rx_arc = internal_receivers
            .get(task_id)
            .ok_or_else(|| anyhow::anyhow!("No receiver for task {}", task_id))?
            .clone();

        let mut last_acked_checkpoint: u64 = 0;
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
                Ok(Some(StreamMessage::Barrier(barrier))) => {
                    drop(rx);
                    // Deduplicate: only ack each checkpoint once
                    if barrier.checkpoint_id <= last_acked_checkpoint {
                        continue;
                    }
                    last_acked_checkpoint = barrier.checkpoint_id;
                    info!(task_id = %task_id, checkpoint_id = barrier.checkpoint_id, "Console sink received barrier");
                    let _ = checkpoint_ack_tx.send((
                        job_id.to_string(),
                        task_id.to_string(),
                        barrier.checkpoint_id as i64,
                    )).await;
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
}
