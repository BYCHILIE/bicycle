//! Apache Pulsar connector for Bicycle.
//!
//! This module provides source and sink connectors for Apache Pulsar,
//! enabling Bicycle to consume from and produce to Pulsar topics.
//!
//! ## Features
//!
//! - **Source**: Consumes messages from Pulsar topics with automatic acknowledgment
//! - **Sink**: Produces messages to Pulsar with exactly-once semantics via deduplication
//! - **Schema**: Supports Pulsar schema registry
//! - **Subscriptions**: Supports all Pulsar subscription types
//!
//! ## Example
//!
//! ```ignore
//! use bicycle_connectors::pulsar::{PulsarSource, PulsarConfig};
//!
//! let config = PulsarConfig::new("pulsar://localhost:6650")
//!     .with_topic("persistent://public/default/my-topic")
//!     .with_subscription("my-subscription");
//!
//! let mut source = PulsarSource::new(config);
//! source.connect().await?;
//! ```

mod config;
mod source;
mod sink;

pub use config::PulsarConfig;
pub use source::PulsarSource;
pub use sink::PulsarSink;
