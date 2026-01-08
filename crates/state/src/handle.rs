//! Dynamic dispatch handle for keyed state stores.

use anyhow::Result;
use bytes::Bytes;
use std::sync::Arc;

use crate::memory::MemoryKeyedStateStore;
use crate::rocksdb::RocksDBKeyedStateStore;
use crate::traits::{
    KeyedStateStore, ListState, MapState, ReducingState, StateKey, StateValue, ValueState,
};

/// Concrete dynamic dispatch for KeyedStateStore.
///
/// Since `KeyedStateStore` is not object-safe (it has generic methods),
/// we use this enum to enable runtime polymorphism between backends.
pub enum KeyedStateStoreHandle {
    /// In-memory state store.
    Memory(Arc<MemoryKeyedStateStore>),
    /// RocksDB-backed state store.
    Rocks(RocksDBKeyedStateStore),
}

impl KeyedStateStore for KeyedStateStoreHandle {
    fn get_value_state<V: StateValue>(&self, name: &str) -> Result<Box<dyn ValueState<V>>> {
        match self {
            KeyedStateStoreHandle::Memory(s) => s.get_value_state(name),
            KeyedStateStoreHandle::Rocks(s) => s.get_value_state(name),
        }
    }

    fn get_list_state<V: StateValue>(&self, name: &str) -> Result<Box<dyn ListState<V>>> {
        match self {
            KeyedStateStoreHandle::Memory(s) => s.get_list_state(name),
            KeyedStateStoreHandle::Rocks(s) => s.get_list_state(name),
        }
    }

    fn get_map_state<K: StateKey, V: StateValue>(
        &self,
        name: &str,
    ) -> Result<Box<dyn MapState<K, V>>> {
        match self {
            KeyedStateStoreHandle::Memory(s) => s.get_map_state(name),
            KeyedStateStoreHandle::Rocks(s) => s.get_map_state(name),
        }
    }

    fn get_reducing_state<V: StateValue>(
        &self,
        name: &str,
        reduce_fn: Arc<dyn Fn(&V, &V) -> V + Send + Sync>,
    ) -> Result<Box<dyn ReducingState<V>>> {
        match self {
            KeyedStateStoreHandle::Memory(s) => s.get_reducing_state(name, reduce_fn),
            KeyedStateStoreHandle::Rocks(s) => s.get_reducing_state(name, reduce_fn),
        }
    }

    fn set_current_key(&self, key: &[u8]) {
        match self {
            KeyedStateStoreHandle::Memory(s) => s.set_current_key(key),
            KeyedStateStoreHandle::Rocks(s) => s.set_current_key(key),
        }
    }

    fn get_current_key(&self) -> Option<Vec<u8>> {
        match self {
            KeyedStateStoreHandle::Memory(s) => s.get_current_key(),
            KeyedStateStoreHandle::Rocks(s) => s.get_current_key(),
        }
    }

    fn clear_current_key(&self) -> Result<()> {
        match self {
            KeyedStateStoreHandle::Memory(s) => s.clear_current_key(),
            KeyedStateStoreHandle::Rocks(s) => s.clear_current_key(),
        }
    }

    fn snapshot(&self) -> Result<Bytes> {
        match self {
            KeyedStateStoreHandle::Memory(s) => s.snapshot(),
            KeyedStateStoreHandle::Rocks(s) => s.snapshot(),
        }
    }

    fn restore(&self, data: &[u8]) -> Result<()> {
        match self {
            KeyedStateStoreHandle::Memory(s) => s.restore(data),
            KeyedStateStoreHandle::Rocks(s) => s.restore(data),
        }
    }
}
