//! Kafka configuration types.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

/// Configuration for Kafka connections.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaConfig {
    /// Kafka bootstrap servers (comma-separated).
    pub bootstrap_servers: String,

    /// Consumer group ID (for source).
    pub group_id: Option<String>,

    /// Topics to consume from (for source).
    pub topics: Vec<String>,

    /// Topic to produce to (for sink).
    pub topic: Option<String>,

    /// Client ID.
    pub client_id: Option<String>,

    /// Security protocol (PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL).
    pub security_protocol: Option<String>,

    /// SASL mechanism (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512).
    pub sasl_mechanism: Option<String>,

    /// SASL username.
    pub sasl_username: Option<String>,

    /// SASL password.
    pub sasl_password: Option<String>,

    /// Enable exactly-once semantics (for sink).
    pub exactly_once: bool,

    /// Transactional ID prefix (for exactly-once sink).
    pub transactional_id_prefix: Option<String>,

    /// Auto offset reset strategy (earliest, latest).
    pub auto_offset_reset: Option<String>,

    /// Enable auto commit (for source).
    pub enable_auto_commit: bool,

    /// Maximum poll interval.
    pub max_poll_interval_ms: Option<u64>,

    /// Session timeout.
    pub session_timeout_ms: Option<u64>,

    /// Heartbeat interval.
    pub heartbeat_interval_ms: Option<u64>,

    /// Additional properties.
    pub properties: HashMap<String, String>,
}

impl Default for KafkaConfig {
    fn default() -> Self {
        Self {
            bootstrap_servers: "localhost:9092".to_string(),
            group_id: None,
            topics: Vec::new(),
            topic: None,
            client_id: None,
            security_protocol: None,
            sasl_mechanism: None,
            sasl_username: None,
            sasl_password: None,
            exactly_once: false,
            transactional_id_prefix: None,
            auto_offset_reset: Some("earliest".to_string()),
            enable_auto_commit: false,
            max_poll_interval_ms: Some(300000),
            session_timeout_ms: Some(10000),
            heartbeat_interval_ms: Some(3000),
            properties: HashMap::new(),
        }
    }
}

impl KafkaConfig {
    pub fn new(bootstrap_servers: impl Into<String>) -> Self {
        Self {
            bootstrap_servers: bootstrap_servers.into(),
            ..Default::default()
        }
    }

    pub fn with_group_id(mut self, group_id: impl Into<String>) -> Self {
        self.group_id = Some(group_id.into());
        self
    }

    pub fn with_topics(mut self, topics: Vec<String>) -> Self {
        self.topics = topics;
        self
    }

    pub fn with_topic(mut self, topic: impl Into<String>) -> Self {
        self.topic = Some(topic.into());
        self
    }

    pub fn with_exactly_once(mut self, transactional_id: impl Into<String>) -> Self {
        self.exactly_once = true;
        self.transactional_id_prefix = Some(transactional_id.into());
        self
    }

    pub fn with_security(
        mut self,
        protocol: impl Into<String>,
        mechanism: Option<String>,
        username: Option<String>,
        password: Option<String>,
    ) -> Self {
        self.security_protocol = Some(protocol.into());
        self.sasl_mechanism = mechanism;
        self.sasl_username = username;
        self.sasl_password = password;
        self
    }
}
