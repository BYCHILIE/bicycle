//! Bicycle Streaming API
//!
//! This crate provides the user-facing API for writing Bicycle streaming jobs,
//! similar to Apache Flink's streaming API.
//!
//! # Example
//!
//! ```ignore
//! use bicycle_api::prelude::*;
//!
//! struct WordSplitter;
//!
//! #[async_trait]
//! impl AsyncFunction for WordSplitter {
//!     type In = String;
//!     type Out = String;
//!
//!     async fn process(&mut self, input: String, ctx: &Context) -> Vec<String> {
//!         input.split_whitespace().map(String::from).collect()
//!     }
//! }
//!
//! #[bicycle_main]
//! async fn main(env: StreamEnvironment) -> Result<()> {
//!     env.socket_source("0.0.0.0", 9999)
//!         .process(WordSplitter)
//!         .socket_sink("0.0.0.0", 9998);
//!
//!     env.execute("wordcount")
//! }
//! ```

pub mod context;
pub mod datastream;
pub mod environment;
pub mod function;
pub mod graph;
pub mod kafka;
pub mod pulsar;
pub mod optimizer;
pub mod prelude;
pub mod state;

pub use context::{Context, RuntimeContext, TaskInfo};
pub use datastream::{ConnectedStreams, DataStream, JoinedStreams};
pub use environment::StreamEnvironment;
pub use function::{
    AsyncFunction, AsyncIOFunction, CoMapFunction, CoProcessFunction, JoinFunction,
    KeyedProcessFunction, ProcessFunction, RichAsyncFunction,
};
pub use function::{
    DeserializationSchema, JsonSchema, SerializationSchema, StringSchema,
};
pub use function::{
    KeyedOnTimerContext, KeyedProcessContext, OnTimerContext, ProcessContext, TimerService,
};
pub use kafka::{DeliveryGuarantee, KafkaSinkBuilder, KafkaSourceBuilder};
pub use pulsar::{PulsarSinkBuilder, PulsarSourceBuilder};
pub use optimizer::{JobGraphOptimizer, OptimizedJobGraph, OperatorChain, OptimizerConfig};
pub use state::{ListState, MapState, ValueState};

// Re-export common dependencies
pub use async_trait::async_trait;
pub use serde::{de::DeserializeOwned, Deserialize, Serialize};

/// Result type for Bicycle operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors that can occur in Bicycle jobs.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("State error: {0}")]
    State(String),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Runtime error: {0}")]
    Runtime(String),
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::Serialization(e.to_string())
    }
}
