//! In-memory keyed state store implementation.

use anyhow::Result;
use bytes::Bytes;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

use crate::traits::{
    KeyedStateStore, ListState, MapState, ReducingState, StateKey, StateValue, ValueState,
};

use super::states::{MemoryListState, MemoryMapState, MemoryReducingState, MemoryValueState};

/// In-memory keyed state store.
///
/// Organizes state by the current key, allowing operators to maintain
/// separate state for each key in a keyed stream.
pub struct MemoryKeyedStateStore {
    pub(crate) current_key: RwLock<Option<Vec<u8>>>,
    pub(crate) value_states: RwLock<HashMap<String, HashMap<Vec<u8>, Bytes>>>,
    pub(crate) list_states: RwLock<HashMap<String, HashMap<Vec<u8>, Vec<Bytes>>>>,
    pub(crate) map_states: RwLock<HashMap<String, HashMap<Vec<u8>, HashMap<Vec<u8>, Bytes>>>>,
}

impl MemoryKeyedStateStore {
    /// Create a new in-memory keyed state store.
    pub fn new() -> Self {
        Self {
            current_key: RwLock::new(None),
            value_states: RwLock::new(HashMap::new()),
            list_states: RwLock::new(HashMap::new()),
            map_states: RwLock::new(HashMap::new()),
        }
    }

    /// Snapshot all state in this store.
    pub fn snapshot(&self) -> Result<Bytes> {
        let vs = self.value_states.read();
        let ls = self.list_states.read();
        let ms = self.map_states.read();
        let data = (vs.clone(), ls.clone(), ms.clone());
        let serialized = bincode::serialize(&data)?;
        Ok(Bytes::from(serialized))
    }

    /// Restore state from snapshot.
    pub fn restore(&self, data: &[u8]) -> Result<()> {
        let (vs, ls, ms): (
            HashMap<String, HashMap<Vec<u8>, Bytes>>,
            HashMap<String, HashMap<Vec<u8>, Vec<Bytes>>>,
            HashMap<String, HashMap<Vec<u8>, HashMap<Vec<u8>, Bytes>>>,
        ) = bincode::deserialize(data)?;
        *self.value_states.write() = vs;
        *self.list_states.write() = ls;
        *self.map_states.write() = ms;
        Ok(())
    }
}

impl Default for MemoryKeyedStateStore {
    fn default() -> Self {
        Self::new()
    }
}

impl KeyedStateStore for Arc<MemoryKeyedStateStore> {
    fn get_value_state<V: StateValue>(&self, name: &str) -> Result<Box<dyn ValueState<V>>> {
        Ok(Box::new(MemoryValueState {
            store: self.clone(),
            name: name.to_string(),
            _phantom: std::marker::PhantomData,
        }))
    }

    fn get_list_state<V: StateValue>(&self, name: &str) -> Result<Box<dyn ListState<V>>> {
        Ok(Box::new(MemoryListState {
            store: self.clone(),
            name: name.to_string(),
            _phantom: std::marker::PhantomData,
        }))
    }

    fn get_map_state<K: StateKey, V: StateValue>(
        &self,
        name: &str,
    ) -> Result<Box<dyn MapState<K, V>>> {
        Ok(Box::new(MemoryMapState {
            store: self.clone(),
            name: name.to_string(),
            _phantom: std::marker::PhantomData,
        }))
    }

    fn get_reducing_state<V: StateValue>(
        &self,
        name: &str,
        reduce_fn: Arc<dyn Fn(&V, &V) -> V + Send + Sync>,
    ) -> Result<Box<dyn ReducingState<V>>> {
        Ok(Box::new(MemoryReducingState {
            store: self.clone(),
            name: name.to_string(),
            reduce_fn,
            _phantom: std::marker::PhantomData,
        }))
    }

    fn set_current_key(&self, key: &[u8]) {
        *self.current_key.write() = Some(key.to_vec());
    }

    fn get_current_key(&self) -> Option<Vec<u8>> {
        self.current_key.read().clone()
    }

    fn clear_current_key(&self) -> Result<()> {
        let key = self.current_key.read().clone();
        if let Some(key) = key {
            for state in self.value_states.write().values_mut() {
                state.remove(&key);
            }
            for state in self.list_states.write().values_mut() {
                state.remove(&key);
            }
            for state in self.map_states.write().values_mut() {
                state.remove(&key);
            }
        }
        Ok(())
    }

    fn snapshot(&self) -> Result<Bytes> {
        MemoryKeyedStateStore::snapshot(self)
    }

    fn restore(&self, data: &[u8]) -> Result<()> {
        MemoryKeyedStateStore::restore(self, data)
    }
}
