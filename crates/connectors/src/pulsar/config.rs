//! Pulsar configuration types.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

/// Subscription type for Pulsar consumers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SubscriptionType {
    /// Exclusive: Only one consumer can attach to the subscription.
    Exclusive,
    /// Shared: Multiple consumers can attach and messages are distributed.
    Shared,
    /// Failover: One active consumer, others on standby.
    Failover,
    /// Key_Shared: Messages with the same key go to the same consumer.
    KeyShared,
}

impl Default for SubscriptionType {
    fn default() -> Self {
        Self::Shared
    }
}

/// Initial position for new subscriptions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum InitialPosition {
    /// Start from the earliest available message.
    Earliest,
    /// Start from the latest message.
    Latest,
}

impl Default for InitialPosition {
    fn default() -> Self {
        Self::Earliest
    }
}

/// Configuration for Pulsar connections.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PulsarConfig {
    /// Pulsar service URL.
    pub service_url: String,

    /// Topics to consume from (for source).
    pub topics: Vec<String>,

    /// Topic to produce to (for sink).
    pub topic: Option<String>,

    /// Subscription name (for source).
    pub subscription: Option<String>,

    /// Subscription type.
    pub subscription_type: SubscriptionType,

    /// Consumer name.
    pub consumer_name: Option<String>,

    /// Producer name.
    pub producer_name: Option<String>,

    /// Initial position for new subscriptions.
    pub initial_position: InitialPosition,

    /// Enable exactly-once semantics (for sink).
    pub exactly_once: bool,

    /// Batch receive policy timeout.
    pub batch_receive_timeout: Option<Duration>,

    /// Maximum number of messages in batch.
    pub batch_size: Option<u32>,

    /// Negative acknowledgment redelivery delay.
    pub negative_ack_redelivery_delay: Option<Duration>,

    /// Authentication token.
    pub auth_token: Option<String>,

    /// TLS certificate path.
    pub tls_cert_path: Option<String>,

    /// TLS key path.
    pub tls_key_path: Option<String>,

    /// Additional properties.
    pub properties: HashMap<String, String>,
}

impl Default for PulsarConfig {
    fn default() -> Self {
        Self {
            service_url: "pulsar://localhost:6650".to_string(),
            topics: Vec::new(),
            topic: None,
            subscription: None,
            subscription_type: SubscriptionType::default(),
            consumer_name: None,
            producer_name: None,
            initial_position: InitialPosition::default(),
            exactly_once: false,
            batch_receive_timeout: Some(Duration::from_millis(100)),
            batch_size: Some(100),
            negative_ack_redelivery_delay: Some(Duration::from_secs(60)),
            auth_token: None,
            tls_cert_path: None,
            tls_key_path: None,
            properties: HashMap::new(),
        }
    }
}

impl PulsarConfig {
    pub fn new(service_url: impl Into<String>) -> Self {
        Self {
            service_url: service_url.into(),
            ..Default::default()
        }
    }

    pub fn with_topic(mut self, topic: impl Into<String>) -> Self {
        let topic = topic.into();
        self.topics = vec![topic.clone()];
        self.topic = Some(topic);
        self
    }

    pub fn with_topics(mut self, topics: Vec<String>) -> Self {
        self.topics = topics;
        self
    }

    pub fn with_subscription(mut self, subscription: impl Into<String>) -> Self {
        self.subscription = Some(subscription.into());
        self
    }

    pub fn with_subscription_type(mut self, sub_type: SubscriptionType) -> Self {
        self.subscription_type = sub_type;
        self
    }

    pub fn with_exactly_once(mut self) -> Self {
        self.exactly_once = true;
        self
    }

    pub fn with_auth_token(mut self, token: impl Into<String>) -> Self {
        self.auth_token = Some(token.into());
        self
    }

    pub fn with_tls(mut self, cert_path: impl Into<String>, key_path: impl Into<String>) -> Self {
        self.tls_cert_path = Some(cert_path.into());
        self.tls_key_path = Some(key_path.into());
        self
    }
}
