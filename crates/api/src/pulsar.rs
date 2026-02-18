//! Typed Pulsar source/sink builders (like Flink's PulsarSource.builder() / PulsarSink.builder()).
//!
//! These builders provide a fluent API for configuring Pulsar connectors with
//! proper typing and serialization/deserialization control.

use crate::graph::{ConnectorConfig, ConnectorType};
use serde::{de::DeserializeOwned, Serialize};
use std::collections::HashMap;
use std::marker::PhantomData;

/// Builder for a typed Pulsar source.
///
/// # Example
///
/// ```ignore
/// use bicycle_api::prelude::*;
///
/// let source = PulsarSourceBuilder::<String>::new("pulsar://localhost:6650", "my-topic")
///     .subscription("my-sub")
///     .subscription_type("Shared")
///     .deserializer("json")
///     .build_connector();
/// ```
pub struct PulsarSourceBuilder<T> {
    service_url: String,
    topic: String,
    subscription: String,
    subscription_type: String,
    properties: HashMap<String, String>,
    value_deserializer: Option<String>,
    _marker: PhantomData<T>,
}

impl<T: Serialize + DeserializeOwned + Send + Sync + 'static> PulsarSourceBuilder<T> {
    /// Create a new Pulsar source builder.
    pub fn new(service_url: impl Into<String>, topic: impl Into<String>) -> Self {
        Self {
            service_url: service_url.into(),
            topic: topic.into(),
            subscription: "bicycle-default-subscription".to_string(),
            subscription_type: "Shared".to_string(),
            properties: HashMap::new(),
            value_deserializer: None,
            _marker: PhantomData,
        }
    }

    /// Set the subscription name.
    pub fn subscription(mut self, subscription: impl Into<String>) -> Self {
        self.subscription = subscription.into();
        self
    }

    /// Set the subscription type (e.g., "Exclusive", "Shared", "Failover", "Key_Shared").
    pub fn subscription_type(mut self, sub_type: impl Into<String>) -> Self {
        self.subscription_type = sub_type.into();
        self
    }

    /// Set a custom Pulsar consumer property.
    pub fn property(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.properties.insert(key.into(), value.into());
        self
    }

    /// Set the initial position (e.g., "earliest", "latest").
    pub fn initial_position(mut self, position: impl Into<String>) -> Self {
        self.properties
            .insert("initial.position".to_string(), position.into());
        self
    }

    /// Set the deserialization format explicitly (e.g., "json", "string", "bincode", "avro").
    pub fn deserializer(mut self, format: &str) -> Self {
        self.value_deserializer = Some(format.to_string());
        self
    }

    /// Build the connector config.
    pub fn build_connector(&self) -> ConnectorConfig {
        let mut props = self.properties.clone();
        props.insert("service.url".to_string(), self.service_url.clone());
        props.insert("topic".to_string(), self.topic.clone());
        props.insert("subscription".to_string(), self.subscription.clone());
        props.insert("subscription.type".to_string(), self.subscription_type.clone());

        // Use explicit deserializer if set, otherwise auto-detect from type
        let format = if let Some(ref fmt) = self.value_deserializer {
            fmt.clone()
        } else {
            let type_name = std::any::type_name::<T>();
            if type_name.contains("String") || type_name == "alloc::string::String" {
                "string".to_string()
            } else {
                "json".to_string()
            }
        };
        props.insert("value.deserializer".to_string(), format);

        ConnectorConfig {
            connector_type: ConnectorType::Pulsar,
            properties: props,
        }
    }
}

/// Builder for a typed Pulsar sink.
///
/// # Example
///
/// ```ignore
/// use bicycle_api::prelude::*;
///
/// stream.pulsar_sink_to(
///     PulsarSinkBuilder::<WordCount>::new("pulsar://localhost:6650", "word-counts")
///         .serializer("json")
///         .property("batchingEnabled", "true")
/// );
/// ```
pub struct PulsarSinkBuilder<T> {
    service_url: String,
    topic: String,
    properties: HashMap<String, String>,
    value_serializer: Option<String>,
    _marker: PhantomData<T>,
}

impl<T: Serialize + DeserializeOwned + Send + Sync + 'static> PulsarSinkBuilder<T> {
    /// Create a new Pulsar sink builder.
    pub fn new(service_url: impl Into<String>, topic: impl Into<String>) -> Self {
        Self {
            service_url: service_url.into(),
            topic: topic.into(),
            properties: HashMap::new(),
            value_serializer: None,
            _marker: PhantomData,
        }
    }

    /// Set a custom Pulsar producer property.
    pub fn property(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.properties.insert(key.into(), value.into());
        self
    }

    /// Set the delivery guarantee (e.g., "at-least-once", "exactly-once").
    pub fn delivery_guarantee(mut self, guarantee: crate::kafka::DeliveryGuarantee) -> Self {
        self.properties
            .insert("delivery.guarantee".to_string(), guarantee.to_string());
        self
    }

    /// Set the serialization format explicitly (e.g., "json", "string", "bincode", "avro").
    pub fn serializer(mut self, format: &str) -> Self {
        self.value_serializer = Some(format.to_string());
        self
    }

    /// Build the connector config.
    pub fn build_connector(&self) -> ConnectorConfig {
        let mut props = self.properties.clone();
        props.insert("service.url".to_string(), self.service_url.clone());
        props.insert("topic".to_string(), self.topic.clone());

        // Use explicit serializer if set, otherwise auto-detect from type
        let format = if let Some(ref fmt) = self.value_serializer {
            fmt.clone()
        } else {
            let type_name = std::any::type_name::<T>();
            if type_name.contains("String") || type_name == "alloc::string::String" {
                "string".to_string()
            } else {
                "json".to_string()
            }
        };
        props.insert("value.serializer".to_string(), format);

        ConnectorConfig {
            connector_type: ConnectorType::Pulsar,
            properties: props,
        }
    }
}
