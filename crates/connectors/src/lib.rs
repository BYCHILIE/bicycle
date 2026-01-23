//! Connectors for external data sources and sinks.
//!
//! This crate provides source and sink connectors for popular message
//! queues and data systems.
//!
//! ## Available Connectors
//!
//! - **Kafka** (enabled by default): Apache Kafka source and sink with exactly-once support
//! - **Pulsar** (optional): Apache Pulsar source and sink
//!
//! ## Feature Flags
//!
//! - `kafka`: Enable Kafka connector (default)
//! - `pulsar`: Enable Pulsar connector
//!
//! ## Example
//!
//! ```ignore
//! use bicycle_connectors::kafka::{KafkaSource, KafkaConfig};
//!
//! let config = KafkaConfig::new("localhost:9092")
//!     .with_group_id("my-consumer-group")
//!     .with_topics(vec!["my-topic".to_string()]);
//!
//! let mut source = KafkaSource::new(config);
//! source.connect()?;
//! ```

#[cfg(feature = "kafka")]
pub mod kafka;

#[cfg(feature = "pulsar")]
pub mod pulsar;
