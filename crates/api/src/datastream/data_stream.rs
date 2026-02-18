//! The primary DataStream type.

use crate::function::{
    AsyncFunction, AsyncIOFunction, ProcessFunction, RichAsyncFunction,
};
use crate::graph::{AsyncProcessConfig, ConnectorConfig, Edge, OperatorType, PartitionStrategy, Vertex};
use crate::kafka::KafkaSinkBuilder;
use crate::pulsar::PulsarSinkBuilder;
use serde::{de::DeserializeOwned, Serialize};
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use super::{ConnectedStreams, KeyedStream, SinkStream, StreamEnvInner};

/// A stream of elements of type T.
///
/// DataStream provides a fluent API for building streaming pipelines.
/// Operations are lazy - they build up a job graph that is executed
/// when `execute()` is called on the StreamEnvironment.
///
/// # Example
///
/// ```ignore
/// env.socket_source("0.0.0.0", 9999)
///     .map(|s: String| s.to_uppercase())
///     .filter(|s| !s.is_empty())
///     .flat_map(|s: String| s.split_whitespace().map(String::from).collect::<Vec<_>>())
///     .key_by(|word| word.clone())
///     .process(WordCounter::new())
///     .socket_sink("0.0.0.0", 9998);
/// ```
pub struct DataStream<T> {
    pub(crate) env: Arc<StreamEnvInner>,
    pub(crate) vertex_id: String,
    _marker: PhantomData<T>,
}

impl<T: Serialize + DeserializeOwned + Send + Sync + 'static> DataStream<T> {
    /// Create a new DataStream.
    pub(crate) fn new(env: Arc<StreamEnvInner>, vertex_id: String) -> Self {
        Self {
            env,
            vertex_id,
            _marker: PhantomData,
        }
    }

    // ---- Transformations ----

    /// Apply a map transformation.
    pub fn map<U, F>(self, _f: F) -> DataStream<U>
    where
        U: Serialize + DeserializeOwned + Send + Sync + 'static,
        F: Fn(T) -> U + Send + Sync + 'static,
    {
        let new_id = self.env.next_vertex_id();
        let vertex = Vertex::new(&new_id, "Map", OperatorType::Map);
        self.env.add_vertex(vertex);
        self.env.add_edge(Edge::new(&self.vertex_id, &new_id));
        DataStream::new(self.env, new_id)
    }

    /// Apply a flat map transformation.
    pub fn flat_map<U, F>(self, _f: F) -> DataStream<U>
    where
        U: Serialize + DeserializeOwned + Send + Sync + 'static,
        F: Fn(T) -> Vec<U> + Send + Sync + 'static,
    {
        let new_id = self.env.next_vertex_id();
        let vertex = Vertex::new(&new_id, "FlatMap", OperatorType::FlatMap);
        self.env.add_vertex(vertex);
        self.env.add_edge(Edge::new(&self.vertex_id, &new_id));
        DataStream::new(self.env, new_id)
    }

    /// Apply a filter transformation.
    pub fn filter<F>(self, _predicate: F) -> DataStream<T>
    where
        F: Fn(&T) -> bool + Send + Sync + 'static,
    {
        let new_id = self.env.next_vertex_id();
        let vertex = Vertex::new(&new_id, "Filter", OperatorType::Filter);
        self.env.add_vertex(vertex);
        self.env.add_edge(Edge::new(&self.vertex_id, &new_id));
        DataStream::new(self.env, new_id)
    }

    // ---- Process functions ----

    /// Apply an async function.
    pub fn process<F>(self, function: F) -> DataStream<F::Out>
    where
        F: AsyncFunction<In = T>,
    {
        let new_id = self.env.next_vertex_id();
        let name = function.name().split("::").last().unwrap_or("Process");
        let vertex = Vertex::new(&new_id, name, OperatorType::Process { is_rich: false })
            .with_plugin_function(function.name().to_string());
        self.env.add_vertex(vertex);
        self.env.add_edge(Edge::new(&self.vertex_id, &new_id));
        DataStream::new(self.env, new_id)
    }

    /// Apply a rich async function (stateful).
    pub fn process_rich<F>(self, function: F) -> DataStream<F::Out>
    where
        F: RichAsyncFunction<In = T>,
    {
        let new_id = self.env.next_vertex_id();
        let name = function.name().split("::").last().unwrap_or("Process");
        let vertex = Vertex::new(&new_id, name, OperatorType::Process { is_rich: true })
            .with_plugin_function(function.name().to_string());
        self.env.add_vertex(vertex);
        self.env.add_edge(Edge::new(&self.vertex_id, &new_id));
        DataStream::new(self.env, new_id)
    }

    /// Apply a low-level ProcessFunction with access to timers and side outputs.
    pub fn process_fn<F>(self, function: F) -> DataStream<F::Out>
    where
        F: ProcessFunction<In = T>,
    {
        let new_id = self.env.next_vertex_id();
        let name = function.name().split("::").last().unwrap_or("Process");
        let vertex = Vertex::new(&new_id, name, OperatorType::Process { is_rich: false })
            .with_plugin_function(function.name().to_string());
        self.env.add_vertex(vertex);
        self.env.add_edge(Edge::new(&self.vertex_id, &new_id));
        DataStream::new(self.env, new_id)
    }

    /// Apply a plugin function by name.
    ///
    /// # Example
    ///
    /// ```ignore
    /// env.socket_source("0.0.0.0", 9999)
    ///     .process_plugin::<String>("WordSplitter")
    ///     .socket_sink("0.0.0.0", 9998);
    /// ```
    pub fn process_plugin<U>(self, function_name: &str) -> DataStream<U>
    where
        U: Serialize + DeserializeOwned + Send + Sync + 'static,
    {
        let new_id = self.env.next_vertex_id();
        let name = function_name.split("::").last().unwrap_or(function_name);
        let vertex = Vertex::new(&new_id, name, OperatorType::Process { is_rich: false })
            .with_plugin_function(function_name.to_string());
        self.env.add_vertex(vertex);
        self.env.add_edge(Edge::new(&self.vertex_id, &new_id));
        DataStream::new(self.env, new_id)
    }

    /// Apply a rich (stateful) plugin function by name.
    pub fn process_plugin_rich<U>(self, function_name: &str) -> DataStream<U>
    where
        U: Serialize + DeserializeOwned + Send + Sync + 'static,
    {
        let new_id = self.env.next_vertex_id();
        let name = function_name.split("::").last().unwrap_or(function_name);
        let vertex = Vertex::new(&new_id, name, OperatorType::Process { is_rich: true })
            .with_plugin_function(function_name.to_string());
        self.env.add_vertex(vertex);
        self.env.add_edge(Edge::new(&self.vertex_id, &new_id));
        DataStream::new(self.env, new_id)
    }

    // ---- Ordered / Unordered async processing ----

    /// Apply an async function with ordered output (preserves input order).
    ///
    /// Like Flink's `AsyncDataStream.orderedWait(stream, function, timeout, TimeUnit, capacity)`.
    pub fn process_ordered<F>(self, function: F, timeout: Duration, capacity: usize) -> DataStream<F::Out>
    where
        F: AsyncFunction<In = T>,
    {
        let new_id = self.env.next_vertex_id();
        let name = function.name().split("::").last().unwrap_or("Process");
        let config = AsyncProcessConfig { ordered: true, timeout_ms: timeout.as_millis() as u64, capacity };
        let vertex = Vertex::new(&new_id, name, OperatorType::Process { is_rich: false })
            .with_plugin_function(function.name().to_string())
            .with_config(serde_json::to_vec(&config).unwrap_or_default());
        self.env.add_vertex(vertex);
        self.env.add_edge(Edge::new(&self.vertex_id, &new_id));
        DataStream::new(self.env, new_id)
    }

    /// Apply an async function with unordered output (results emitted as they complete).
    ///
    /// Like Flink's `AsyncDataStream.unorderedWait(stream, function, timeout, TimeUnit, capacity)`.
    pub fn process_unordered<F>(self, function: F, timeout: Duration, capacity: usize) -> DataStream<F::Out>
    where
        F: AsyncFunction<In = T>,
    {
        let new_id = self.env.next_vertex_id();
        let name = function.name().split("::").last().unwrap_or("Process");
        let config = AsyncProcessConfig { ordered: false, timeout_ms: timeout.as_millis() as u64, capacity };
        let vertex = Vertex::new(&new_id, name, OperatorType::Process { is_rich: false })
            .with_plugin_function(function.name().to_string())
            .with_config(serde_json::to_vec(&config).unwrap_or_default());
        self.env.add_vertex(vertex);
        self.env.add_edge(Edge::new(&self.vertex_id, &new_id));
        DataStream::new(self.env, new_id)
    }

    /// Apply a rich async function with ordered output.
    pub fn process_rich_ordered<F>(self, function: F, timeout: Duration, capacity: usize) -> DataStream<F::Out>
    where
        F: RichAsyncFunction<In = T>,
    {
        let new_id = self.env.next_vertex_id();
        let name = function.name().split("::").last().unwrap_or("Process");
        let config = AsyncProcessConfig { ordered: true, timeout_ms: timeout.as_millis() as u64, capacity };
        let vertex = Vertex::new(&new_id, name, OperatorType::Process { is_rich: true })
            .with_plugin_function(function.name().to_string())
            .with_config(serde_json::to_vec(&config).unwrap_or_default());
        self.env.add_vertex(vertex);
        self.env.add_edge(Edge::new(&self.vertex_id, &new_id));
        DataStream::new(self.env, new_id)
    }

    /// Apply a rich async function with unordered output.
    pub fn process_rich_unordered<F>(self, function: F, timeout: Duration, capacity: usize) -> DataStream<F::Out>
    where
        F: RichAsyncFunction<In = T>,
    {
        let new_id = self.env.next_vertex_id();
        let name = function.name().split("::").last().unwrap_or("Process");
        let config = AsyncProcessConfig { ordered: false, timeout_ms: timeout.as_millis() as u64, capacity };
        let vertex = Vertex::new(&new_id, name, OperatorType::Process { is_rich: true })
            .with_plugin_function(function.name().to_string())
            .with_config(serde_json::to_vec(&config).unwrap_or_default());
        self.env.add_vertex(vertex);
        self.env.add_edge(Edge::new(&self.vertex_id, &new_id));
        DataStream::new(self.env, new_id)
    }

    // ---- Async I/O ----

    /// Apply an unordered async I/O function for external calls.
    pub fn async_io_unordered<F>(self, function: F, timeout_ms: u64, capacity: usize) -> DataStream<F::Out>
    where
        F: AsyncIOFunction<In = T>,
    {
        let new_id = self.env.next_vertex_id();
        let name = function.name().split("::").last().unwrap_or("AsyncIO");
        let vertex = Vertex::new(&new_id, name, OperatorType::AsyncIO { ordered: false, timeout_ms, capacity })
            .with_plugin_function(function.name().to_string());
        self.env.add_vertex(vertex);
        self.env.add_edge(Edge::new(&self.vertex_id, &new_id));
        DataStream::new(self.env, new_id)
    }

    /// Apply an ordered async I/O function for external calls.
    pub fn async_io_ordered<F>(self, function: F, timeout_ms: u64, capacity: usize) -> DataStream<F::Out>
    where
        F: AsyncIOFunction<In = T>,
    {
        let new_id = self.env.next_vertex_id();
        let name = function.name().split("::").last().unwrap_or("AsyncIO");
        let vertex = Vertex::new(&new_id, name, OperatorType::AsyncIO { ordered: true, timeout_ms, capacity })
            .with_plugin_function(function.name().to_string());
        self.env.add_vertex(vertex);
        self.env.add_edge(Edge::new(&self.vertex_id, &new_id));
        DataStream::new(self.env, new_id)
    }

    // ---- Partitioning ----

    /// Partition the stream by key.
    pub fn key_by<K, F>(self, _key_selector: F) -> KeyedStream<T, K>
    where
        T: Clone,
        K: Serialize + DeserializeOwned + Clone + std::hash::Hash + Eq + Send + Sync + 'static,
        F: Fn(&T) -> K + Send + Sync + 'static,
    {
        let new_id = self.env.next_vertex_id();
        let vertex = Vertex::new(
            &new_id, "KeyBy",
            OperatorType::KeyBy { key_selector: std::any::type_name::<F>().to_string() },
        );
        self.env.add_vertex(vertex);
        self.env.add_edge(Edge::new(&self.vertex_id, &new_id).with_strategy(PartitionStrategy::Hash));
        KeyedStream::new(self.env, new_id)
    }

    /// Merge this stream with another same-typed stream.
    pub fn union(self, other: DataStream<T>) -> DataStream<T> {
        let new_id = self.env.next_vertex_id();
        let vertex = Vertex::new(&new_id, "Union", OperatorType::Union);
        self.env.add_vertex(vertex);
        self.env.add_edge(Edge::new(&self.vertex_id, &new_id));
        self.env.add_edge(Edge::new(&other.vertex_id, &new_id));
        DataStream::new(self.env, new_id)
    }

    /// Connect this stream with another (possibly differently-typed) stream.
    pub fn connect<U>(self, other: DataStream<U>) -> ConnectedStreams<T, U>
    where
        U: Serialize + DeserializeOwned + Send + Sync + 'static,
    {
        ConnectedStreams::new(self.env, self.vertex_id, other.vertex_id)
    }

    /// Random partitioning (round-robin distribution).
    pub fn shuffle(self) -> DataStream<T> {
        self.env.set_next_partition_strategy(&self.vertex_id, PartitionStrategy::Rebalance);
        self
    }

    /// Explicit forward partitioning.
    pub fn forward(self) -> DataStream<T> {
        self.env.set_next_partition_strategy(&self.vertex_id, PartitionStrategy::Forward);
        self
    }

    /// Send all elements to a single parallel instance (parallelism=1).
    pub fn global(self) -> DataStream<T> {
        self.env.set_next_partition_strategy(&self.vertex_id, PartitionStrategy::Forward);
        self
    }

    /// Rebalance the stream (round-robin distribution).
    pub fn rebalance(self) -> DataStream<T> {
        self.env.set_next_partition_strategy(&self.vertex_id, PartitionStrategy::Rebalance);
        self
    }

    /// Broadcast the stream (send to all parallel instances).
    pub fn broadcast(self) -> DataStream<T> {
        self.env.set_next_partition_strategy(&self.vertex_id, PartitionStrategy::Broadcast);
        self
    }

    // ---- Side output ----

    /// Mark this stream for side output with a given tag.
    pub fn side_output(self, tag: &str) -> DataStream<T> {
        let new_id = self.env.next_vertex_id();
        let vertex = Vertex::new(
            &new_id,
            &format!("SideOutput({})", tag),
            OperatorType::SideOutput { tag: tag.to_string() },
        );
        self.env.add_vertex(vertex);
        self.env.add_edge(Edge::new(&self.vertex_id, &new_id));
        DataStream::new(self.env, new_id)
    }

    // ---- Aggregations ----

    /// Count elements in the stream.
    pub fn count(self) -> DataStream<String> {
        let new_id = self.env.next_vertex_id();
        let vertex = Vertex::new(&new_id, "Count", OperatorType::Count);
        self.env.add_vertex(vertex);
        self.env.add_edge(Edge::new(&self.vertex_id, &new_id));
        DataStream::new(self.env, new_id)
    }

    // ---- Sinks ----

    /// Write to a typed Kafka sink using a builder.
    ///
    /// # Example
    ///
    /// ```ignore
    /// stream.sink_to(
    ///     KafkaSinkBuilder::<WordCount>::new("localhost:9092", "word-counts")
    ///         .property("acks", "all")
    /// );
    /// ```
    pub fn sink_to(self, builder: KafkaSinkBuilder<T>) -> SinkStream {
        let new_id = self.env.next_vertex_id();
        let connector = builder.build_connector();
        let vertex = Vertex::new(&new_id, "KafkaSink", OperatorType::Sink { connector });
        self.env.add_vertex(vertex);
        self.env.add_edge(Edge::new(&self.vertex_id, &new_id));
        SinkStream::new(self.env, new_id)
    }

    /// Write to a typed Pulsar sink using a builder.
    ///
    /// # Example
    ///
    /// ```ignore
    /// stream.pulsar_sink_to(
    ///     PulsarSinkBuilder::<WordCount>::new("pulsar://localhost:6650", "word-counts")
    ///         .serializer("json")
    /// );
    /// ```
    pub fn pulsar_sink_to(self, builder: PulsarSinkBuilder<T>) -> SinkStream {
        let new_id = self.env.next_vertex_id();
        let connector = builder.build_connector();
        let vertex = Vertex::new(&new_id, "PulsarSink", OperatorType::Sink { connector });
        self.env.add_vertex(vertex);
        self.env.add_edge(Edge::new(&self.vertex_id, &new_id));
        SinkStream::new(self.env, new_id)
    }

    /// Write to a socket sink.
    pub fn socket_sink(self, host: &str, port: u16) -> SinkStream {
        let new_id = self.env.next_vertex_id();
        let vertex = Vertex::new(
            &new_id, "SocketSink",
            OperatorType::Sink { connector: ConnectorConfig::socket(host, port) },
        );
        self.env.add_vertex(vertex);
        self.env.add_edge(Edge::new(&self.vertex_id, &new_id));
        SinkStream::new(self.env, new_id)
    }

    /// Write to a Kafka topic.
    pub fn kafka_sink(self, brokers: &str, topic: &str) -> SinkStream {
        let new_id = self.env.next_vertex_id();
        let vertex = Vertex::new(
            &new_id, "KafkaSink",
            OperatorType::Sink { connector: ConnectorConfig::kafka(brokers, topic) },
        );
        self.env.add_vertex(vertex);
        self.env.add_edge(Edge::new(&self.vertex_id, &new_id));
        SinkStream::new(self.env, new_id)
    }

    /// Write to stdout (for debugging).
    pub fn print(self) -> SinkStream {
        let new_id = self.env.next_vertex_id();
        let vertex = Vertex::new(
            &new_id, "Print",
            OperatorType::Sink {
                connector: ConnectorConfig {
                    connector_type: crate::graph::ConnectorType::None,
                    properties: Default::default(),
                },
            },
        );
        self.env.add_vertex(vertex);
        self.env.add_edge(Edge::new(&self.vertex_id, &new_id));
        SinkStream::new(self.env, new_id)
    }

    // ---- Configuration ----

    /// Set parallelism for this operator.
    pub fn set_parallelism(self, parallelism: u32) -> Self {
        self.env.set_parallelism(&self.vertex_id, parallelism);
        self
    }

    /// Set maximum parallelism for this operator (for rescaling).
    pub fn set_max_parallelism(self, max_parallelism: u32) -> Self {
        self.env.set_max_parallelism(&self.vertex_id, max_parallelism);
        self
    }

    /// Set a user-defined unique identifier for this operator.
    ///
    /// # Example
    /// ```ignore
    /// stream.process(MyFunction)
    ///     .uid("my-processor-v1")
    ///     .name("My Processor");
    /// ```
    pub fn uid(self, uid: impl Into<String>) -> Self {
        self.env.set_uid(&self.vertex_id, &uid.into());
        self
    }

    /// Set a human-readable name for this operator (shown in UI).
    pub fn name(self, name: impl Into<String>) -> Self {
        self.env.set_name(&self.vertex_id, &name.into());
        self
    }

    /// Set the slot sharing group for this operator.
    pub fn slot_sharing_group(self, group: impl Into<String>) -> Self {
        self.env.set_slot_sharing_group(&self.vertex_id, &group.into());
        self
    }

    /// Disable operator chaining for this operator.
    pub fn disable_chaining(self) -> Self {
        self.env.disable_chaining(&self.vertex_id);
        self
    }

    /// Start a new operator chain from this operator.
    pub fn start_new_chain(self) -> Self {
        self.env.disable_chaining(&self.vertex_id);
        self
    }

    /// Get the vertex ID.
    pub fn vertex_id(&self) -> &str {
        &self.vertex_id
    }
}
