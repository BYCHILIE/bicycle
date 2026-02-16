//! Typed Kafka source/sink builders (like Flink's KafkaSource.builder() / KafkaSink.builder()).
//!
//! These builders provide a fluent API for configuring Kafka connectors with
//! proper typing and serialization/deserialization control.

use crate::graph::{ConnectorConfig, ConnectorType};
use serde::{de::DeserializeOwned, Serialize};
use std::collections::HashMap;
use std::marker::PhantomData;

/// Builder for a typed Kafka source.
///
/// # Example
///
/// ```ignore
/// use bicycle_api::prelude::*;
///
/// let source = KafkaSourceBuilder::<String>::new("localhost:9092", "my-topic")
///     .group_id("my-group")
///     .property("auto.offset.reset", "earliest")
///     .build();
/// ```
pub struct KafkaSourceBuilder<T> {
    brokers: String,
    topic: String,
    group_id: String,
    properties: HashMap<String, String>,
    _marker: PhantomData<T>,
}

impl<T: Serialize + DeserializeOwned + Send + Sync + 'static> KafkaSourceBuilder<T> {
    /// Create a new Kafka source builder.
    pub fn new(brokers: impl Into<String>, topic: impl Into<String>) -> Self {
        Self {
            brokers: brokers.into(),
            topic: topic.into(),
            group_id: "bicycle-default-group".to_string(),
            properties: HashMap::new(),
            _marker: PhantomData,
        }
    }

    /// Set the consumer group ID.
    pub fn group_id(mut self, group_id: impl Into<String>) -> Self {
        self.group_id = group_id.into();
        self
    }

    /// Set a custom Kafka consumer property.
    pub fn property(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.properties.insert(key.into(), value.into());
        self
    }

    /// Set the starting offsets (e.g., "earliest", "latest").
    pub fn starting_offsets(mut self, offsets: impl Into<String>) -> Self {
        self.properties
            .insert("auto.offset.reset".to_string(), offsets.into());
        self
    }

    /// Build the connector config.
    pub fn build_connector(&self) -> ConnectorConfig {
        let mut props = self.properties.clone();
        props.insert("bootstrap.servers".to_string(), self.brokers.clone());
        props.insert("topic".to_string(), self.topic.clone());
        props.insert("group.id".to_string(), self.group_id.clone());

        // Store the value format based on type
        let type_name = std::any::type_name::<T>();
        if type_name.contains("String") || type_name == "alloc::string::String" {
            props.insert("value.deserializer".to_string(), "string".to_string());
        } else {
            props.insert("value.deserializer".to_string(), "json".to_string());
        }

        ConnectorConfig {
            connector_type: ConnectorType::Kafka,
            properties: props,
        }
    }
}

/// Delivery guarantee for Kafka sinks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryGuarantee {
    /// At-least-once delivery (default).
    AtLeastOnce,
    /// Exactly-once delivery (requires Kafka transactions).
    ExactlyOnce,
}

impl std::fmt::Display for DeliveryGuarantee {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DeliveryGuarantee::AtLeastOnce => write!(f, "at-least-once"),
            DeliveryGuarantee::ExactlyOnce => write!(f, "exactly-once"),
        }
    }
}

/// Builder for a typed Kafka sink.
///
/// # Example
///
/// ```ignore
/// use bicycle_api::prelude::*;
///
/// stream.sink_to(
///     KafkaSinkBuilder::<WordCount>::new("localhost:9092", "word-counts")
///         .delivery_guarantee(DeliveryGuarantee::ExactlyOnce)
///         .property("acks", "all")
/// );
/// ```
pub struct KafkaSinkBuilder<T> {
    brokers: String,
    topic: String,
    properties: HashMap<String, String>,
    _marker: PhantomData<T>,
}

impl<T: Serialize + DeserializeOwned + Send + Sync + 'static> KafkaSinkBuilder<T> {
    /// Create a new Kafka sink builder.
    pub fn new(brokers: impl Into<String>, topic: impl Into<String>) -> Self {
        Self {
            brokers: brokers.into(),
            topic: topic.into(),
            properties: HashMap::new(),
            _marker: PhantomData,
        }
    }

    /// Set a custom Kafka producer property.
    pub fn property(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.properties.insert(key.into(), value.into());
        self
    }

    /// Set the delivery guarantee.
    pub fn delivery_guarantee(mut self, guarantee: DeliveryGuarantee) -> Self {
        self.properties
            .insert("delivery.guarantee".to_string(), guarantee.to_string());
        self
    }

    /// Build the connector config.
    pub fn build_connector(&self) -> ConnectorConfig {
        let mut props = self.properties.clone();
        props.insert("bootstrap.servers".to_string(), self.brokers.clone());
        props.insert("topic".to_string(), self.topic.clone());

        // Store the value format based on type
        let type_name = std::any::type_name::<T>();
        if type_name.contains("String") || type_name == "alloc::string::String" {
            props.insert("value.serializer".to_string(), "string".to_string());
        } else {
            props.insert("value.serializer".to_string(), "json".to_string());
        }

        ConnectorConfig {
            connector_type: ConnectorType::Kafka,
            properties: props,
        }
    }
}
