//! Memory state backend implementation.

use anyhow::{Context, Result};
use async_trait::async_trait;
use bicycle_core::StateHandle;
use bytes::Bytes;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use tracing::info;

use crate::handle::KeyedStateStoreHandle;
use crate::traits::StateBackend;

use super::MemoryKeyedStateStore;

/// In-memory state backend for development and testing.
///
/// This backend stores all state in memory, making it fast but not durable.
/// Use for development, testing, or when state size fits in memory.
pub struct MemoryStateBackend {
    stores: RwLock<HashMap<String, Arc<MemoryKeyedStateStore>>>,
}

impl MemoryStateBackend {
    /// Create a new in-memory state backend.
    pub fn new() -> Self {
        Self {
            stores: RwLock::new(HashMap::new()),
        }
    }
}

impl Default for MemoryStateBackend {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl StateBackend for MemoryStateBackend {
    fn create_keyed_state_store(&self, operator_id: &str) -> Result<Box<KeyedStateStoreHandle>> {
        let mut stores = self.stores.write();
        let store = stores
            .entry(operator_id.to_string())
            .or_insert_with(|| Arc::new(MemoryKeyedStateStore::new()))
            .clone();

        Ok(Box::new(KeyedStateStoreHandle::Memory(store)))
    }

    async fn snapshot(&self, checkpoint_id: u64, path: &Path) -> Result<StateHandle> {
        // Build snapshot data while holding the lock, then drop it before any await.
        let all_state: HashMap<String, Bytes> = {
            let stores = self.stores.read();
            let mut all_state = HashMap::new();

            for (op_id, store) in stores.iter() {
                let snapshot = store.snapshot()?;
                all_state.insert(op_id.clone(), snapshot);
            }

            all_state
        }; // <- stores guard dropped here

        let serialized = bincode::serialize(&all_state)?;
        let snapshot_path = path.join(format!("checkpoint-{}.bin", checkpoint_id));

        tokio::fs::create_dir_all(path).await?;
        tokio::fs::write(&snapshot_path, &serialized)
            .await
            .context("Failed to write checkpoint")?;

        info!(checkpoint_id, path = %snapshot_path.display(), "Checkpoint saved");

        Ok(StateHandle {
            path: snapshot_path.to_string_lossy().to_string(),
            size: serialized.len() as u64,
            checksum: None,
        })
    }

    async fn restore(&self, handle: &StateHandle) -> Result<()> {
        let data = tokio::fs::read(&handle.path)
            .await
            .context("Failed to read checkpoint")?;

        let all_state: HashMap<String, Bytes> = bincode::deserialize(&data)?;

        let mut stores = self.stores.write();
        for (op_id, state_data) in all_state {
            let store = stores
                .entry(op_id.clone())
                .or_insert_with(|| Arc::new(MemoryKeyedStateStore::new()));
            store.restore(&state_data)?;
        }

        info!(path = %handle.path, "State restored from checkpoint");
        Ok(())
    }

    fn name(&self) -> &'static str {
        "memory"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::{KeyedStateStore, ReducingState, ValueState};

    #[test]
    fn test_memory_value_state() {
        let backend = MemoryStateBackend::new();
        let store = backend.create_keyed_state_store("test-op").unwrap();
        store.set_current_key(b"key1");
        let state: Box<dyn ValueState<i64>> = store.get_value_state("counter").unwrap();
        assert!(state.get().unwrap().is_none());
        state.set(42).unwrap();
        assert_eq!(state.get().unwrap(), Some(42));
    }

    #[test]
    fn test_memory_reducing_state() {
        let backend = MemoryStateBackend::new();
        let store = backend.create_keyed_state_store("test-op").unwrap();
        store.set_current_key(b"key1");
        let reduce_fn = Arc::new(|a: &i64, b: &i64| a + b);
        let state: Box<dyn ReducingState<i64>> =
            store.get_reducing_state("sum", reduce_fn).unwrap();
        state.add(10).unwrap();
        state.add(5).unwrap();
        assert_eq!(state.get().unwrap(), Some(15));
    }
}
