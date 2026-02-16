//! Sink stream (terminal operator).

use std::sync::Arc;

use super::StreamEnvInner;

/// A sink stream (terminal operator).
///
/// This type allows setting properties on sink operators (uid, name, etc.)
/// but does not allow further transformations.
pub struct SinkStream {
    env: Arc<StreamEnvInner>,
    vertex_id: String,
}

impl SinkStream {
    pub(crate) fn new(env: Arc<StreamEnvInner>, vertex_id: String) -> Self {
        Self { env, vertex_id }
    }

    /// Set a user-defined unique identifier for this sink.
    pub fn uid(self, uid: impl Into<String>) -> Self {
        self.env.set_uid(&self.vertex_id, &uid.into());
        self
    }

    /// Set a human-readable name for this sink (shown in UI).
    pub fn name(self, name: impl Into<String>) -> Self {
        self.env.set_name(&self.vertex_id, &name.into());
        self
    }

    /// Set parallelism for this sink.
    pub fn set_parallelism(self, parallelism: u32) -> Self {
        self.env.set_parallelism(&self.vertex_id, parallelism);
        self
    }

    /// Set maximum parallelism for this sink.
    pub fn set_max_parallelism(self, max_parallelism: u32) -> Self {
        self.env.set_max_parallelism(&self.vertex_id, max_parallelism);
        self
    }

    /// Set the slot sharing group for this sink.
    pub fn slot_sharing_group(self, group: impl Into<String>) -> Self {
        self.env.set_slot_sharing_group(&self.vertex_id, &group.into());
        self
    }

    /// Disable operator chaining for this sink.
    pub fn disable_chaining(self) -> Self {
        self.env.disable_chaining(&self.vertex_id);
        self
    }
}
