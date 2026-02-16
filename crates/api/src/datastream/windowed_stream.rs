//! Windowed stream.

use crate::function::{AggregateFunction, RichAsyncFunction};
use crate::graph::{Edge, OperatorType, Vertex};
use serde::{de::DeserializeOwned, Serialize};
use std::marker::PhantomData;
use std::sync::Arc;

use super::{DataStream, StreamEnvInner};

/// A windowed stream.
pub struct WindowedStream<T, K> {
    env: Arc<StreamEnvInner>,
    vertex_id: String,
    _marker: PhantomData<(T, K)>,
}

impl<T, K> WindowedStream<T, K>
where
    T: Serialize + DeserializeOwned + Send + Sync + Clone + 'static,
    K: Serialize + DeserializeOwned + Clone + std::hash::Hash + Eq + Send + Sync + 'static,
{
    pub(crate) fn new(env: Arc<StreamEnvInner>, vertex_id: String) -> Self {
        Self { env, vertex_id, _marker: PhantomData }
    }

    /// Reduce elements in the window.
    pub fn reduce<F>(self, _reduce_fn: F) -> DataStream<T>
    where
        F: Fn(T, T) -> T + Send + Sync + 'static,
    {
        DataStream::new(self.env, self.vertex_id)
    }

    /// Count elements in the window.
    pub fn count(self) -> DataStream<String> {
        let new_id = self.env.next_vertex_id();
        let vertex = Vertex::new(&new_id, "WindowCount", OperatorType::Count);
        self.env.add_vertex(vertex);
        self.env.add_edge(Edge::new(&self.vertex_id, &new_id));
        DataStream::new(self.env, new_id)
    }

    /// Apply a custom aggregate function to window contents.
    pub fn aggregate<F>(self, _function: F) -> DataStream<F::Out>
    where
        F: AggregateFunction<In = T>,
    {
        let new_id = self.env.next_vertex_id();
        let vertex = Vertex::new(&new_id, "WindowAggregate", OperatorType::Reduce);
        self.env.add_vertex(vertex);
        self.env.add_edge(Edge::new(&self.vertex_id, &new_id));
        DataStream::new(self.env, new_id)
    }

    /// Apply a custom process function to window contents.
    pub fn process<F, O>(self, function: F) -> DataStream<O>
    where
        O: Serialize + DeserializeOwned + Send + Sync + 'static,
        F: RichAsyncFunction<In = T, Out = O>,
    {
        let new_id = self.env.next_vertex_id();
        let name = function.name().split("::").last().unwrap_or("WindowProcess");
        let vertex = Vertex::new(&new_id, name, OperatorType::Process { is_rich: true })
            .with_plugin_function(function.name().to_string());
        self.env.add_vertex(vertex);
        self.env.add_edge(Edge::new(&self.vertex_id, &new_id));
        DataStream::new(self.env, new_id)
    }
}
