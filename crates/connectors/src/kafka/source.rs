//! Kafka source connector for consuming messages from Kafka topics.

use crate::kafka::config::KafkaConfig;
use anyhow::Result;
use bicycle_core::{Event, StreamMessage, Timestamp};
use bicycle_runtime::Emitter;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::{BorrowedMessage, Message};
use rdkafka::TopicPartitionList;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

/// A Kafka source that consumes messages and emits them as events.
pub struct KafkaSource {
    config: KafkaConfig,
    consumer: Option<StreamConsumer>,
    /// Partition watermarks for event-time tracking.
    partition_watermarks: HashMap<i32, Timestamp>,
    /// Allowed lateness for watermark generation.
    allowed_lateness_ms: u64,
}

impl KafkaSource {
    /// Create a new Kafka source.
    pub fn new(config: KafkaConfig) -> Self {
        Self {
            config,
            consumer: None,
            partition_watermarks: HashMap::new(),
            allowed_lateness_ms: 5000, // 5 second default
        }
    }

    /// Set allowed lateness for watermark generation.
    pub fn with_allowed_lateness(mut self, lateness_ms: u64) -> Self {
        self.allowed_lateness_ms = lateness_ms;
        self
    }

    /// Connect to Kafka and subscribe to topics.
    pub fn connect(&mut self) -> Result<()> {
        let mut client_config = ClientConfig::new();

        // Basic configuration
        client_config
            .set("bootstrap.servers", &self.config.bootstrap_servers)
            .set(
                "group.id",
                self.config.group_id.as_deref().unwrap_or("bicycle-consumer"),
            )
            .set(
                "auto.offset.reset",
                self.config.auto_offset_reset.as_deref().unwrap_or("earliest"),
            )
            .set(
                "enable.auto.commit",
                if self.config.enable_auto_commit {
                    "true"
                } else {
                    "false"
                },
            );

        // Optional configurations
        if let Some(client_id) = &self.config.client_id {
            client_config.set("client.id", client_id);
        }

        if let Some(protocol) = &self.config.security_protocol {
            client_config.set("security.protocol", protocol);
        }

        if let Some(mechanism) = &self.config.sasl_mechanism {
            client_config.set("sasl.mechanism", mechanism);
        }

        if let Some(username) = &self.config.sasl_username {
            client_config.set("sasl.username", username);
        }

        if let Some(password) = &self.config.sasl_password {
            client_config.set("sasl.password", password);
        }

        if let Some(max_poll) = self.config.max_poll_interval_ms {
            client_config.set("max.poll.interval.ms", max_poll.to_string());
        }

        if let Some(session_timeout) = self.config.session_timeout_ms {
            client_config.set("session.timeout.ms", session_timeout.to_string());
        }

        if let Some(heartbeat) = self.config.heartbeat_interval_ms {
            client_config.set("heartbeat.interval.ms", heartbeat.to_string());
        }

        // Additional properties
        for (key, value) in &self.config.properties {
            client_config.set(key, value);
        }

        let consumer: StreamConsumer = client_config.create()?;

        // Subscribe to topics
        let topics: Vec<&str> = self.config.topics.iter().map(|s| s.as_str()).collect();
        consumer.subscribe(&topics)?;

        info!(
            topics = ?self.config.topics,
            group_id = ?self.config.group_id,
            "Kafka source connected"
        );

        self.consumer = Some(consumer);
        Ok(())
    }

    /// Take ownership of the underlying consumer for direct use.
    pub fn take_consumer(&mut self) -> Option<StreamConsumer> {
        self.consumer.take()
    }

    /// Run the source, emitting events to the downstream.
    pub async fn run<K, V>(
        &mut self,
        mut emitter: Emitter<Event<K, V>>,
        key_deserializer: impl Fn(&[u8]) -> Option<K> + Send + 'static,
        value_deserializer: impl Fn(&[u8]) -> Option<V> + Send + 'static,
    ) -> Result<()>
    where
        K: Send + 'static,
        V: Send + 'static,
    {
        let consumer = self
            .consumer
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Consumer not connected"))?;

        info!("Kafka source starting message loop");

        loop {
            match consumer.recv().await {
                Ok(message) => {
                    // Extract timestamp (prefer message timestamp, fallback to current time)
                    let timestamp = message
                        .timestamp()
                        .to_millis()
                        .map(|t| t as u64)
                        .unwrap_or_else(|| {
                            std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap()
                                .as_millis() as u64
                        });

                    // Deserialize key and value; messages without keys use empty bytes
                    let key_bytes = message.key().unwrap_or(b"");
                    let key = key_deserializer(key_bytes);
                    let value = message.payload().and_then(&value_deserializer);

                    if let (Some(key), Some(value)) = (key, value) {
                        let event = Event::new(timestamp, key, value);

                        if let Err(e) = emitter.data(event).await {
                            error!(error = %e, "Failed to emit event");
                            break;
                        }

                        // Update partition watermark
                        let partition = message.partition();
                        let watermark = self
                            .partition_watermarks
                            .entry(partition)
                            .or_insert(timestamp);
                        *watermark = (*watermark).max(timestamp);

                        // Emit watermark periodically (every 100 messages for simplicity)
                        // In production, this would be time-based
                        let min_watermark = self
                            .partition_watermarks
                            .values()
                            .copied()
                            .min()
                            .unwrap_or(0)
                            .saturating_sub(self.allowed_lateness_ms);

                        if let Err(e) = emitter.watermark(min_watermark).await {
                            error!(error = %e, "Failed to emit watermark");
                            break;
                        }
                    }

                    // Commit offset if auto-commit is disabled
                    if !self.config.enable_auto_commit {
                        if let Err(e) = consumer.commit_message(&message, CommitMode::Async) {
                            warn!(error = %e, "Failed to commit offset");
                        }
                    }
                }
                Err(e) => {
                    error!(error = %e, "Error receiving Kafka message");
                    // Continue on transient errors
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }

        Ok(())
    }

    /// Get current offsets for checkpointing.
    pub fn get_offsets(&self) -> Result<HashMap<(String, i32), i64>> {
        let consumer = self
            .consumer
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Consumer not connected"))?;

        let position = consumer.position()?;
        let mut offsets = HashMap::new();

        for elem in position.elements() {
            offsets.insert(
                (elem.topic().to_string(), elem.partition()),
                elem.offset().to_raw().unwrap_or(-1),
            );
        }

        Ok(offsets)
    }

    /// Restore offsets from checkpoint.
    pub fn restore_offsets(&self, offsets: HashMap<(String, i32), i64>) -> Result<()> {
        let consumer = self
            .consumer
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Consumer not connected"))?;

        let mut tpl = TopicPartitionList::new();
        for ((topic, partition), offset) in offsets {
            tpl.add_partition_offset(
                &topic,
                partition,
                rdkafka::Offset::Offset(offset),
            )?;
        }

        consumer.assign(&tpl)?;
        info!("Restored Kafka offsets from checkpoint");
        Ok(())
    }
}

/// Helper to create a Kafka source with string key/value.
pub fn create_string_source(config: KafkaConfig) -> KafkaSource {
    KafkaSource::new(config)
}
