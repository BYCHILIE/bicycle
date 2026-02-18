//! Prelude module - common imports for Bicycle jobs.
//!
//! Import this module to get all commonly used types:
//!
//! ```ignore
//! use bicycle_api::prelude::*;
//! ```

// Core traits
pub use crate::function::{
    AggregateFunction, AsyncFunction, AsyncIOFunction, CoMapFunction, CoProcessFunction,
    FilterFunction, FlatMapFunction, JoinFunction, KeySelector, KeyedProcessFunction,
    MapFunction, ProcessFunction, ReduceFunction, RichAsyncFunction,
};

// Context types for process functions
pub use crate::function::{
    KeyedOnTimerContext, KeyedProcessContext, OnTimerContext, ProcessContext, TimerService,
};

// Serialization schemas
pub use crate::function::{
    ConnectorSchema, DeserializationSchema, JsonSchema, SerializationSchema, StringSchema,
};
#[cfg(feature = "bincode-serde")]
pub use crate::function::BincodeSchema;
#[cfg(feature = "avro")]
pub use crate::function::AvroSchema;

// Kafka builders
pub use crate::kafka::{DeliveryGuarantee, KafkaSinkBuilder, KafkaSourceBuilder};

// Pulsar builders
pub use crate::pulsar::{PulsarSinkBuilder, PulsarSourceBuilder};

// Context types
pub use crate::context::{Context, MemoryStateBackend, RuntimeContext, StateBackend, StateBackendExt, TaskInfo};

// State types
pub use crate::state::{ListState, MapState, ValueState};

// Stream types
pub use crate::datastream::{ConnectedStreams, DataStream, JoinedStreams, KeyedStream, SinkStream, WindowedStream};

// Environment
pub use crate::environment::{StreamEnvBuilder, StreamEnvironment};

// Graph types (for advanced use)
pub use crate::graph::{
    AsyncProcessConfig, ConnectorConfig, ConnectorType, Edge, JobConfig, JobGraph, OperatorType,
    PartitionStrategy, Vertex, WindowType,
};

// Optimizer types
pub use crate::optimizer::{
    ChainEdge, JobGraphOptimizer, OperatorChain, OptimizedJobGraph, OptimizerConfig,
    OptimizationStats,
};

// Error and Result
pub use crate::{Error, Result};

// Re-exports
pub use async_trait::async_trait;
pub use serde::{de::DeserializeOwned, Deserialize, Serialize};

// Common std types used in streaming jobs
pub use std::collections::HashMap;
pub use std::sync::Arc;
