//! Joined streams for join operations on keyed streams.

use crate::function::JoinFunction;
use crate::graph::{Edge, OperatorType, Vertex, WindowType};
use serde::{de::DeserializeOwned, Serialize};
use std::marker::PhantomData;
use std::sync::Arc;

use super::{DataStream, StreamEnvInner};

/// Two keyed streams prepared for a join operation.
///
/// Created by calling `keyed_stream1.join(keyed_stream2)`.
pub struct JoinedStreams<T, U, K> {
    env: Arc<StreamEnvInner>,
    vertex_id_1: String,
    vertex_id_2: String,
    window_size_ms: Option<u64>,
    _marker: PhantomData<(T, U, K)>,
}

impl<T, U, K> JoinedStreams<T, U, K>
where
    T: Serialize + DeserializeOwned + Send + Sync + Clone + 'static,
    U: Serialize + DeserializeOwned + Send + Sync + Clone + 'static,
    K: Serialize + DeserializeOwned + Clone + std::hash::Hash + Eq + Send + Sync + 'static,
{
    pub(crate) fn new(env: Arc<StreamEnvInner>, vertex_id_1: String, vertex_id_2: String) -> Self {
        Self { env, vertex_id_1, vertex_id_2, window_size_ms: None, _marker: PhantomData }
    }

    /// Set the window size for the join.
    pub fn window(mut self, size_ms: u64) -> Self {
        self.window_size_ms = Some(size_ms);
        self
    }

    /// Apply a join function to produce output from matched elements.
    pub fn apply<F>(self, _function: F) -> DataStream<F::Out>
    where
        F: JoinFunction<In1 = T, In2 = U>,
    {
        let join_input_id = if let Some(size_ms) = self.window_size_ms {
            let window_id = self.env.next_vertex_id();
            let vertex = Vertex::new(
                &window_id, "JoinWindow",
                OperatorType::Window { window_type: WindowType::Tumbling { size_ms } },
            );
            self.env.add_vertex(vertex);
            self.env.add_edge(Edge::new(&self.vertex_id_1, &window_id));
            self.env.add_edge(Edge::new(&self.vertex_id_2, &window_id));
            window_id
        } else {
            let connect_id = self.env.next_vertex_id();
            let connect_vertex = Vertex::new(&connect_id, "JoinConnect", OperatorType::Connect);
            self.env.add_vertex(connect_vertex);
            self.env.add_edge(Edge::new(&self.vertex_id_1, &connect_id));
            self.env.add_edge(Edge::new(&self.vertex_id_2, &connect_id));
            connect_id
        };

        let new_id = self.env.next_vertex_id();
        let vertex = Vertex::new(&new_id, "Join", OperatorType::Process { is_rich: true });
        self.env.add_vertex(vertex);
        self.env.add_edge(Edge::new(&join_input_id, &new_id));
        DataStream::new(self.env, new_id)
    }
}
