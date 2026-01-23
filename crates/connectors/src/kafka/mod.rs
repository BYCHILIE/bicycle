//! Apache Kafka connector for Bicycle.
//!
//! This module provides source and sink connectors for Apache Kafka,
//! enabling Bicycle to consume from and produce to Kafka topics.
//!
//! ## Features
//!
//! - **Source**: Consumes messages from Kafka topics with automatic offset management
//! - **Sink**: Produces messages to Kafka with exactly-once semantics via transactions
//! - **Event time**: Extracts timestamps from Kafka message timestamps
//! - **Watermarks**: Generates watermarks based on partition progress

mod source;
mod sink;
mod config;

pub use source::KafkaSource;
pub use sink::KafkaSink;
pub use config::KafkaConfig;
