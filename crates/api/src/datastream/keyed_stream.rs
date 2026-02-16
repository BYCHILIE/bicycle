//! Keyed stream partitioned by key K.

use crate::function::{
    AggregateFunction, KeyedProcessFunction, RichAsyncFunction,
};
use crate::graph::{AsyncProcessConfig, Edge, OperatorType, Vertex, WindowType};
use serde::{de::DeserializeOwned, Serialize};
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use super::{DataStream, JoinedStreams, StreamEnvInner, WindowedStream};

/// A keyed stream partitioned by key K.
///
/// Enables keyed operations like reduce, windowing, and stateful processing.
pub struct KeyedStream<T, K> {
    pub(crate) env: Arc<StreamEnvInner>,
    pub(crate) vertex_id: String,
    _marker: PhantomData<(T, K)>,
}

impl<T, K> KeyedStream<T, K>
where
    T: Serialize + DeserializeOwned + Send + Sync + Clone + 'static,
    K: Serialize + DeserializeOwned + Clone + std::hash::Hash + Eq + Send + Sync + 'static,
{
    pub(crate) fn new(env: Arc<StreamEnvInner>, vertex_id: String) -> Self {
        Self { env, vertex_id, _marker: PhantomData }
    }

    // ---- Process functions ----

    /// Apply a rich async function to keyed elements.
    pub fn process<F>(self, function: F) -> DataStream<F::Out>
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

    /// Apply a plugin function by name to keyed elements.
    pub fn process_plugin<U>(self, function_name: &str) -> DataStream<U>
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

    /// Apply a rich async function with ordered output on a keyed stream.
    ///
    /// Like Flink's `AsyncDataStream.orderedWait()` on a keyed stream.
    pub fn process_ordered<F>(self, function: F, timeout: Duration, capacity: usize) -> DataStream<F::Out>
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

    /// Apply a rich async function with unordered output on a keyed stream.
    ///
    /// Like Flink's `AsyncDataStream.unorderedWait()` on a keyed stream.
    pub fn process_unordered<F>(self, function: F, timeout: Duration, capacity: usize) -> DataStream<F::Out>
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

    /// Apply a keyed process function with access to keyed state, timers, and side outputs.
    pub fn process_fn<F>(self, function: F) -> DataStream<F::Out>
    where
        F: KeyedProcessFunction<In = T, Key = K>,
    {
        let new_id = self.env.next_vertex_id();
        let name = function.name().split("::").last().unwrap_or("KeyedProcess");
        let vertex = Vertex::new(&new_id, name, OperatorType::Process { is_rich: true })
            .with_plugin_function(function.name().to_string());
        self.env.add_vertex(vertex);
        self.env.add_edge(Edge::new(&self.vertex_id, &new_id));
        DataStream::new(self.env, new_id)
    }

    // ---- Aggregations ----

    /// Apply a reduce function.
    pub fn reduce<F>(self, _reduce_fn: F) -> DataStream<T>
    where
        F: Fn(T, T) -> T + Send + Sync + 'static,
    {
        let new_id = self.env.next_vertex_id();
        let vertex = Vertex::new(&new_id, "Reduce", OperatorType::Reduce);
        self.env.add_vertex(vertex);
        self.env.add_edge(Edge::new(&self.vertex_id, &new_id));
        DataStream::new(self.env, new_id)
    }

    /// Running sum aggregation by key.
    pub fn sum<F>(self, _selector: F) -> DataStream<T>
    where
        F: Fn(&T) -> f64 + Send + Sync + 'static,
    {
        let new_id = self.env.next_vertex_id();
        let vertex = Vertex::new(&new_id, "Sum", OperatorType::Reduce);
        self.env.add_vertex(vertex);
        self.env.add_edge(Edge::new(&self.vertex_id, &new_id));
        DataStream::new(self.env, new_id)
    }

    /// Running min aggregation by key.
    pub fn min<F>(self, _selector: F) -> DataStream<T>
    where
        F: Fn(&T) -> f64 + Send + Sync + 'static,
    {
        let new_id = self.env.next_vertex_id();
        let vertex = Vertex::new(&new_id, "Min", OperatorType::Reduce);
        self.env.add_vertex(vertex);
        self.env.add_edge(Edge::new(&self.vertex_id, &new_id));
        DataStream::new(self.env, new_id)
    }

    /// Running max aggregation by key.
    pub fn max<F>(self, _selector: F) -> DataStream<T>
    where
        F: Fn(&T) -> f64 + Send + Sync + 'static,
    {
        let new_id = self.env.next_vertex_id();
        let vertex = Vertex::new(&new_id, "Max", OperatorType::Reduce);
        self.env.add_vertex(vertex);
        self.env.add_edge(Edge::new(&self.vertex_id, &new_id));
        DataStream::new(self.env, new_id)
    }

    /// Element with minimum value by key.
    pub fn min_by<F>(self, _selector: F) -> DataStream<T>
    where
        F: Fn(&T) -> f64 + Send + Sync + 'static,
    {
        let new_id = self.env.next_vertex_id();
        let vertex = Vertex::new(&new_id, "MinBy", OperatorType::Reduce);
        self.env.add_vertex(vertex);
        self.env.add_edge(Edge::new(&self.vertex_id, &new_id));
        DataStream::new(self.env, new_id)
    }

    /// Element with maximum value by key.
    pub fn max_by<F>(self, _selector: F) -> DataStream<T>
    where
        F: Fn(&T) -> f64 + Send + Sync + 'static,
    {
        let new_id = self.env.next_vertex_id();
        let vertex = Vertex::new(&new_id, "MaxBy", OperatorType::Reduce);
        self.env.add_vertex(vertex);
        self.env.add_edge(Edge::new(&self.vertex_id, &new_id));
        DataStream::new(self.env, new_id)
    }

    /// Count elements by key.
    pub fn count(self) -> DataStream<String> {
        let new_id = self.env.next_vertex_id();
        let vertex = Vertex::new(&new_id, "Count", OperatorType::Count);
        self.env.add_vertex(vertex);
        self.env.add_edge(Edge::new(&self.vertex_id, &new_id));
        DataStream::new(self.env, new_id)
    }

    // ---- Joins ----

    /// Prepare a join with another keyed stream.
    pub fn join<U>(self, other: KeyedStream<U, K>) -> JoinedStreams<T, U, K>
    where
        U: Serialize + DeserializeOwned + Send + Sync + Clone + 'static,
    {
        JoinedStreams::new(self.env, self.vertex_id, other.vertex_id)
    }

    // ---- Windows ----

    /// Apply a tumbling window.
    pub fn tumbling_window(self, size_ms: u64) -> WindowedStream<T, K> {
        let new_id = self.env.next_vertex_id();
        let vertex = Vertex::new(&new_id, "TumblingWindow", OperatorType::Window { window_type: WindowType::Tumbling { size_ms } });
        self.env.add_vertex(vertex);
        self.env.add_edge(Edge::new(&self.vertex_id, &new_id));
        WindowedStream::new(self.env, new_id)
    }

    /// Apply a sliding window.
    pub fn sliding_window(self, size_ms: u64, slide_ms: u64) -> WindowedStream<T, K> {
        let new_id = self.env.next_vertex_id();
        let vertex = Vertex::new(&new_id, "SlidingWindow", OperatorType::Window { window_type: WindowType::Sliding { size_ms, slide_ms } });
        self.env.add_vertex(vertex);
        self.env.add_edge(Edge::new(&self.vertex_id, &new_id));
        WindowedStream::new(self.env, new_id)
    }

    /// Apply a session window.
    pub fn session_window(self, gap_ms: u64) -> WindowedStream<T, K> {
        let new_id = self.env.next_vertex_id();
        let vertex = Vertex::new(&new_id, "SessionWindow", OperatorType::Window { window_type: WindowType::Session { gap_ms } });
        self.env.add_vertex(vertex);
        self.env.add_edge(Edge::new(&self.vertex_id, &new_id));
        WindowedStream::new(self.env, new_id)
    }

    /// Convert back to a DataStream.
    pub fn into_stream(self) -> DataStream<T> {
        DataStream::new(self.env, self.vertex_id)
    }
}
