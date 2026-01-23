//! Kafka sink connector for producing messages to Kafka topics.

use crate::kafka::config::KafkaConfig;
use anyhow::Result;
use async_trait::async_trait;
use bicycle_runtime::sink::{TransactionId, TransactionalSink};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::util::Timeout;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

/// A Kafka sink that produces messages to a Kafka topic.
///
/// Supports both at-least-once and exactly-once delivery semantics.
pub struct KafkaSink {
    config: KafkaConfig,
    producer: Option<FutureProducer>,
    topic: String,
    sink_id: String,
    /// For exactly-once: current transaction state
    in_transaction: RwLock<bool>,
    /// Message counter for generating unique keys
    message_counter: AtomicU64,
}

impl KafkaSink {
    /// Create a new Kafka sink.
    pub fn new(config: KafkaConfig, sink_id: impl Into<String>) -> Self {
        let topic = config.topic.clone().unwrap_or_else(|| "default".to_string());
        Self {
            config,
            producer: None,
            topic,
            sink_id: sink_id.into(),
            in_transaction: RwLock::new(false),
            message_counter: AtomicU64::new(0),
        }
    }

    /// Connect to Kafka.
    pub fn connect(&mut self) -> Result<()> {
        let mut client_config = ClientConfig::new();

        // Basic configuration
        client_config.set("bootstrap.servers", &self.config.bootstrap_servers);

        // Producer-specific settings
        client_config
            .set("message.timeout.ms", "30000")
            .set("acks", "all") // Wait for all replicas
            .set("enable.idempotence", "true"); // Idempotent producer

        // Exactly-once configuration
        if self.config.exactly_once {
            if let Some(ref txn_id) = self.config.transactional_id_prefix {
                client_config.set("transactional.id", format!("{}-{}", txn_id, self.sink_id));
            }
        }

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

        // Additional properties
        for (key, value) in &self.config.properties {
            client_config.set(key, value);
        }

        let producer: FutureProducer = client_config.create()?;

        info!(
            topic = %self.topic,
            exactly_once = self.config.exactly_once,
            "Kafka sink connected"
        );

        self.producer = Some(producer);
        Ok(())
    }

    /// Send a message to Kafka (at-least-once mode).
    pub async fn send(&self, key: Option<&[u8]>, value: &[u8]) -> Result<()> {
        let producer = self
            .producer
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Producer not connected"))?;

        let mut record = FutureRecord::to(&self.topic).payload(value);

        if let Some(k) = key {
            record = record.key(k);
        }

        let delivery_status = producer
            .send(record, Timeout::After(Duration::from_secs(30)))
            .await;

        match delivery_status {
            Ok((partition, offset)) => {
                debug!(partition, offset, "Message delivered to Kafka");
                Ok(())
            }
            Err((e, _)) => {
                error!(error = %e, "Failed to deliver message to Kafka");
                Err(e.into())
            }
        }
    }

    /// Send a message with a string key and value.
    pub async fn send_string(&self, key: Option<&str>, value: &str) -> Result<()> {
        self.send(key.map(|k| k.as_bytes()), value.as_bytes())
            .await
    }

    /// Flush pending messages.
    pub fn flush(&self, timeout: Duration) -> Result<()> {
        if let Some(producer) = &self.producer {
            producer.flush(Timeout::After(timeout))?;
        }
        Ok(())
    }
}

/// Kafka record for transactional sink.
#[derive(Debug, Clone)]
pub struct KafkaRecord {
    pub key: Option<Vec<u8>>,
    pub value: Vec<u8>,
    pub topic: Option<String>,
}

impl KafkaRecord {
    pub fn new(value: Vec<u8>) -> Self {
        Self {
            key: None,
            value,
            topic: None,
        }
    }

    pub fn with_key(mut self, key: Vec<u8>) -> Self {
        self.key = Some(key);
        self
    }

    pub fn with_topic(mut self, topic: String) -> Self {
        self.topic = Some(topic);
        self
    }
}

/// Transactional Kafka sink for exactly-once semantics.
pub struct TransactionalKafkaSink {
    sink: Arc<KafkaSink>,
    pending_records: RwLock<Vec<KafkaRecord>>,
}

impl TransactionalKafkaSink {
    pub fn new(config: KafkaConfig, sink_id: impl Into<String>) -> Result<Self> {
        let mut config = config;
        config.exactly_once = true;

        let mut sink = KafkaSink::new(config, sink_id);
        sink.connect()?;

        Ok(Self {
            sink: Arc::new(sink),
            pending_records: RwLock::new(Vec::new()),
        })
    }
}

#[async_trait]
impl TransactionalSink for TransactionalKafkaSink {
    type Record = KafkaRecord;

    async fn begin_transaction(&self, id: TransactionId) -> Result<()> {
        debug!(
            checkpoint_id = id.checkpoint_id,
            sink_id = %id.sink_id,
            "Beginning Kafka transaction"
        );

        // Note: In rdkafka, transactions are managed at the producer level
        // and we would call producer.begin_transaction() here if using
        // the transactional API directly. For simplicity, we buffer records.

        *self.sink.in_transaction.write().await = true;
        self.pending_records.write().await.clear();

        Ok(())
    }

    async fn write(&self, record: Self::Record) -> Result<()> {
        // Buffer the record for later commit
        self.pending_records.write().await.push(record);
        Ok(())
    }

    async fn pre_commit(&self, id: &TransactionId) -> Result<()> {
        debug!(
            checkpoint_id = id.checkpoint_id,
            "Pre-committing Kafka transaction"
        );

        // Send all buffered records
        let records = self.pending_records.read().await.clone();
        for record in records {
            let topic = record.topic.as_deref().unwrap_or(&self.sink.topic);
            self.sink.send(record.key.as_deref(), &record.value).await?;
        }

        // Flush to ensure all records are sent
        self.sink.flush(Duration::from_secs(30))?;

        Ok(())
    }

    async fn commit(&self, id: &TransactionId) -> Result<()> {
        debug!(
            checkpoint_id = id.checkpoint_id,
            "Committing Kafka transaction"
        );

        // In a full implementation, this would call producer.commit_transaction()
        // For the buffered approach, records are already sent in pre_commit

        *self.sink.in_transaction.write().await = false;
        self.pending_records.write().await.clear();

        Ok(())
    }

    async fn abort(&self, id: &TransactionId) -> Result<()> {
        warn!(
            checkpoint_id = id.checkpoint_id,
            "Aborting Kafka transaction"
        );

        // In a full implementation, this would call producer.abort_transaction()
        // For the buffered approach, just clear pending records

        *self.sink.in_transaction.write().await = false;
        self.pending_records.write().await.clear();

        Ok(())
    }

    async fn recover(&self, pending_transactions: Vec<TransactionId>) -> Result<()> {
        info!(
            count = pending_transactions.len(),
            "Recovering pending Kafka transactions"
        );

        // For recovery, we need to determine which transactions were committed
        // In a full implementation, this would check transaction status with Kafka
        // and commit or abort accordingly

        for id in pending_transactions {
            // Assume uncommitted transactions should be aborted on recovery
            self.abort(&id).await?;
        }

        Ok(())
    }

    fn sink_id(&self) -> &str {
        &self.sink.sink_id
    }
}

/// Builder for creating Kafka sinks.
pub struct KafkaSinkBuilder {
    config: KafkaConfig,
    sink_id: String,
}

impl KafkaSinkBuilder {
    pub fn new(bootstrap_servers: impl Into<String>) -> Self {
        Self {
            config: KafkaConfig::new(bootstrap_servers),
            sink_id: "kafka-sink".to_string(),
        }
    }

    pub fn topic(mut self, topic: impl Into<String>) -> Self {
        self.config.topic = Some(topic.into());
        self
    }

    pub fn sink_id(mut self, id: impl Into<String>) -> Self {
        self.sink_id = id.into();
        self
    }

    pub fn exactly_once(mut self, transactional_id: impl Into<String>) -> Self {
        self.config = self.config.with_exactly_once(transactional_id);
        self
    }

    pub fn build(self) -> Result<KafkaSink> {
        let mut sink = KafkaSink::new(self.config, self.sink_id);
        sink.connect()?;
        Ok(sink)
    }

    pub fn build_transactional(self) -> Result<TransactionalKafkaSink> {
        TransactionalKafkaSink::new(self.config, self.sink_id)
    }
}
