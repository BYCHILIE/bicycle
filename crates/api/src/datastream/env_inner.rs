//! Internal environment state shared between streams.

use crate::graph::{Edge, PartitionStrategy, Vertex};
use std::sync::Mutex;

/// Internal environment state shared between streams.
pub(crate) struct StreamEnvInner {
    vertices: Mutex<Vec<Vertex>>,
    edges: Mutex<Vec<Edge>>,
    vertex_counter: Mutex<u32>,
}

impl StreamEnvInner {
    pub fn new() -> Self {
        Self {
            vertices: Mutex::new(Vec::new()),
            edges: Mutex::new(Vec::new()),
            vertex_counter: Mutex::new(0),
        }
    }

    pub fn next_vertex_id(&self) -> String {
        let mut counter = self.vertex_counter.lock().unwrap();
        *counter += 1;
        format!("vertex_{}", counter)
    }

    pub fn add_vertex(&self, vertex: Vertex) {
        self.vertices.lock().unwrap().push(vertex);
    }

    pub fn add_edge(&self, edge: Edge) {
        self.edges.lock().unwrap().push(edge);
    }

    pub fn set_parallelism(&self, vertex_id: &str, parallelism: u32) {
        let mut vertices = self.vertices.lock().unwrap();
        if let Some(v) = vertices.iter_mut().find(|v| v.id == vertex_id) {
            v.parallelism = parallelism;
            v.parallelism_set = true;
        }
    }

    pub fn set_max_parallelism(&self, vertex_id: &str, max_parallelism: u32) {
        let mut vertices = self.vertices.lock().unwrap();
        if let Some(v) = vertices.iter_mut().find(|v| v.id == vertex_id) {
            v.max_parallelism = Some(max_parallelism);
        }
    }

    pub fn set_uid(&self, vertex_id: &str, uid: &str) {
        let mut vertices = self.vertices.lock().unwrap();
        if let Some(v) = vertices.iter_mut().find(|v| v.id == vertex_id) {
            v.uid = Some(uid.to_string());
        }
    }

    pub fn set_name(&self, vertex_id: &str, name: &str) {
        let mut vertices = self.vertices.lock().unwrap();
        if let Some(v) = vertices.iter_mut().find(|v| v.id == vertex_id) {
            v.name = name.to_string();
        }
    }

    pub fn set_slot_sharing_group(&self, vertex_id: &str, group: &str) {
        let mut vertices = self.vertices.lock().unwrap();
        if let Some(v) = vertices.iter_mut().find(|v| v.id == vertex_id) {
            v.slot_sharing_group = group.to_string();
        }
    }

    pub fn disable_chaining(&self, vertex_id: &str) {
        let mut vertices = self.vertices.lock().unwrap();
        if let Some(v) = vertices.iter_mut().find(|v| v.id == vertex_id) {
            v.chaining_enabled = false;
        }
    }

    pub fn set_next_partition_strategy(&self, _vertex_id: &str, _strategy: PartitionStrategy) {
        // Placeholder — edges carry the strategy directly.
    }

    pub fn get_vertices(&self) -> Vec<Vertex> {
        self.vertices.lock().unwrap().clone()
    }

    pub fn get_edges(&self) -> Vec<Edge> {
        self.edges.lock().unwrap().clone()
    }
}
