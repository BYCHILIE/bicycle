//! DataStream API for building streaming pipelines.
//!
//! This module provides the fluent API for building streaming pipelines,
//! similar to Flink's DataStream API.

mod connected_streams;
mod data_stream;
mod env_inner;
mod joined_streams;
mod keyed_stream;
mod sink_stream;
mod windowed_stream;

pub use connected_streams::ConnectedStreams;
pub use data_stream::DataStream;
pub(crate) use env_inner::StreamEnvInner;
pub use joined_streams::JoinedStreams;
pub use keyed_stream::KeyedStream;
pub use sink_stream::SinkStream;
pub use windowed_stream::WindowedStream;
