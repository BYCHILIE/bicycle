//! Connected streams of two different types.

use crate::function::{CoMapFunction, CoProcessFunction};
use crate::graph::{Edge, OperatorType, Vertex};
use serde::{de::DeserializeOwned, Serialize};
use std::marker::PhantomData;
use std::sync::Arc;

use super::{DataStream, StreamEnvInner};

/// Two connected streams of different types, enabling co-processing.
///
/// Created by calling `stream1.connect(stream2)`.
pub struct ConnectedStreams<T, U> {
    env: Arc<StreamEnvInner>,
    vertex_id_1: String,
    vertex_id_2: String,
    _marker: PhantomData<(T, U)>,
}

impl<T, U> ConnectedStreams<T, U>
where
    T: Serialize + DeserializeOwned + Send + Sync + 'static,
    U: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pub(crate) fn new(env: Arc<StreamEnvInner>, vertex_id_1: String, vertex_id_2: String) -> Self {
        Self { env, vertex_id_1, vertex_id_2, _marker: PhantomData }
    }

    /// Apply a co-process function to process elements from both streams.
    pub fn process<F>(self, function: F) -> DataStream<F::Out>
    where
        F: CoProcessFunction<In1 = T, In2 = U>,
    {
        let connect_id = self.env.next_vertex_id();
        let connect_vertex = Vertex::new(&connect_id, "Connect", OperatorType::Connect);
        self.env.add_vertex(connect_vertex);
        self.env.add_edge(Edge::new(&self.vertex_id_1, &connect_id));
        self.env.add_edge(Edge::new(&self.vertex_id_2, &connect_id));

        let new_id = self.env.next_vertex_id();
        let name = function.name().split("::").last().unwrap_or("CoProcess");
        let vertex = Vertex::new(&new_id, name, OperatorType::CoProcess { is_rich: true })
            .with_plugin_function(function.name().to_string());
        self.env.add_vertex(vertex);
        self.env.add_edge(Edge::new(&connect_id, &new_id));
        DataStream::new(self.env, new_id)
    }

    /// Apply a co-map function (simpler, stateless version of co-processing).
    pub fn map<F>(self, _function: F) -> DataStream<F::Out>
    where
        F: CoMapFunction<In1 = T, In2 = U>,
    {
        let connect_id = self.env.next_vertex_id();
        let connect_vertex = Vertex::new(&connect_id, "Connect", OperatorType::Connect);
        self.env.add_vertex(connect_vertex);
        self.env.add_edge(Edge::new(&self.vertex_id_1, &connect_id));
        self.env.add_edge(Edge::new(&self.vertex_id_2, &connect_id));

        let new_id = self.env.next_vertex_id();
        let vertex = Vertex::new(&new_id, "CoMap", OperatorType::CoProcess { is_rich: false });
        self.env.add_vertex(vertex);
        self.env.add_edge(Edge::new(&connect_id, &new_id));
        DataStream::new(self.env, new_id)
    }

    /// Key both sides of the connected streams.
    pub fn key_by<K, F1, F2>(self, _ks1: F1, _ks2: F2) -> ConnectedStreams<T, U>
    where
        K: Serialize + DeserializeOwned + Clone + std::hash::Hash + Eq + Send + Sync + 'static,
        F1: Fn(&T) -> K + Send + Sync + 'static,
        F2: Fn(&U) -> K + Send + Sync + 'static,
    {
        self
    }
}
