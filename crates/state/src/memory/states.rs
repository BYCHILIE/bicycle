//! In-memory state implementations (ValueState, ListState, MapState, ReducingState).

use anyhow::Result;
use bytes::Bytes;
use std::sync::Arc;

use crate::traits::{ListState, MapState, ReducingState, StateKey, StateValue, ValueState};

use super::MemoryKeyedStateStore;

// ============================================================================
// MemoryValueState
// ============================================================================

pub(crate) struct MemoryValueState<V> {
    pub(crate) store: Arc<MemoryKeyedStateStore>,
    pub(crate) name: String,
    pub(crate) _phantom: std::marker::PhantomData<V>,
}

impl<V: StateValue> ValueState<V> for MemoryValueState<V> {
    fn get(&self) -> Result<Option<V>> {
        let key = self
            .store
            .current_key
            .read()
            .clone()
            .ok_or_else(|| anyhow::anyhow!("No current key set"))?;
        let states = self.store.value_states.read();
        if let Some(state) = states.get(&self.name) {
            if let Some(data) = state.get(&key) {
                return Ok(Some(bincode::deserialize(data)?));
            }
        }
        Ok(None)
    }

    fn set(&self, value: V) -> Result<()> {
        let key = self
            .store
            .current_key
            .read()
            .clone()
            .ok_or_else(|| anyhow::anyhow!("No current key set"))?;
        let serialized = Bytes::from(bincode::serialize(&value)?);
        self.store
            .value_states
            .write()
            .entry(self.name.clone())
            .or_default()
            .insert(key, serialized);
        Ok(())
    }

    fn clear(&self) -> Result<()> {
        let key = self
            .store
            .current_key
            .read()
            .clone()
            .ok_or_else(|| anyhow::anyhow!("No current key set"))?;
        if let Some(state) = self.store.value_states.write().get_mut(&self.name) {
            state.remove(&key);
        }
        Ok(())
    }
}

// ============================================================================
// MemoryListState
// ============================================================================

pub(crate) struct MemoryListState<V> {
    pub(crate) store: Arc<MemoryKeyedStateStore>,
    pub(crate) name: String,
    pub(crate) _phantom: std::marker::PhantomData<V>,
}

impl<V: StateValue> ListState<V> for MemoryListState<V> {
    fn get(&self) -> Result<Vec<V>> {
        let key = self
            .store
            .current_key
            .read()
            .clone()
            .ok_or_else(|| anyhow::anyhow!("No current key set"))?;
        let states = self.store.list_states.read();
        if let Some(state) = states.get(&self.name) {
            if let Some(items) = state.get(&key) {
                return items
                    .iter()
                    .map(|data| bincode::deserialize(data).map_err(Into::into))
                    .collect();
            }
        }
        Ok(vec![])
    }

    fn add(&self, value: V) -> Result<()> {
        let key = self
            .store
            .current_key
            .read()
            .clone()
            .ok_or_else(|| anyhow::anyhow!("No current key set"))?;
        let serialized = Bytes::from(bincode::serialize(&value)?);
        self.store
            .list_states
            .write()
            .entry(self.name.clone())
            .or_default()
            .entry(key)
            .or_default()
            .push(serialized);
        Ok(())
    }

    fn add_all(&self, values: Vec<V>) -> Result<()> {
        for value in values {
            self.add(value)?;
        }
        Ok(())
    }

    fn clear(&self) -> Result<()> {
        let key = self
            .store
            .current_key
            .read()
            .clone()
            .ok_or_else(|| anyhow::anyhow!("No current key set"))?;
        if let Some(state) = self.store.list_states.write().get_mut(&self.name) {
            state.remove(&key);
        }
        Ok(())
    }
}

// ============================================================================
// MemoryMapState
// ============================================================================

pub(crate) struct MemoryMapState<K, V> {
    pub(crate) store: Arc<MemoryKeyedStateStore>,
    pub(crate) name: String,
    pub(crate) _phantom: std::marker::PhantomData<(K, V)>,
}

impl<K: StateKey, V: StateValue> MapState<K, V> for MemoryMapState<K, V> {
    fn get(&self, map_key: &K) -> Result<Option<V>> {
        let key = self
            .store
            .current_key
            .read()
            .clone()
            .ok_or_else(|| anyhow::anyhow!("No current key set"))?;
        let map_key_bytes = bincode::serialize(map_key)?;
        let states = self.store.map_states.read();
        if let Some(state) = states.get(&self.name) {
            if let Some(map) = state.get(&key) {
                if let Some(data) = map.get(&map_key_bytes) {
                    return Ok(Some(bincode::deserialize(data)?));
                }
            }
        }
        Ok(None)
    }

    fn put(&self, map_key: K, value: V) -> Result<()> {
        let key = self
            .store
            .current_key
            .read()
            .clone()
            .ok_or_else(|| anyhow::anyhow!("No current key set"))?;
        let map_key_bytes = bincode::serialize(&map_key)?;
        let value_bytes = Bytes::from(bincode::serialize(&value)?);
        self.store
            .map_states
            .write()
            .entry(self.name.clone())
            .or_default()
            .entry(key)
            .or_default()
            .insert(map_key_bytes, value_bytes);
        Ok(())
    }

    fn remove(&self, map_key: &K) -> Result<Option<V>> {
        let key = self
            .store
            .current_key
            .read()
            .clone()
            .ok_or_else(|| anyhow::anyhow!("No current key set"))?;
        let map_key_bytes = bincode::serialize(map_key)?;
        let mut states = self.store.map_states.write();
        if let Some(state) = states.get_mut(&self.name) {
            if let Some(map) = state.get_mut(&key) {
                if let Some(data) = map.remove(&map_key_bytes) {
                    return Ok(Some(bincode::deserialize(&data)?));
                }
            }
        }
        Ok(None)
    }

    fn contains(&self, map_key: &K) -> Result<bool> {
        Ok(self.get(map_key)?.is_some())
    }

    fn keys(&self) -> Result<Vec<K>> {
        let key = self
            .store
            .current_key
            .read()
            .clone()
            .ok_or_else(|| anyhow::anyhow!("No current key set"))?;
        let states = self.store.map_states.read();
        if let Some(state) = states.get(&self.name) {
            if let Some(map) = state.get(&key) {
                return map
                    .keys()
                    .map(|k| bincode::deserialize(k).map_err(Into::into))
                    .collect();
            }
        }
        Ok(vec![])
    }

    fn values(&self) -> Result<Vec<V>> {
        let key = self
            .store
            .current_key
            .read()
            .clone()
            .ok_or_else(|| anyhow::anyhow!("No current key set"))?;
        let states = self.store.map_states.read();
        if let Some(state) = states.get(&self.name) {
            if let Some(map) = state.get(&key) {
                return map
                    .values()
                    .map(|v| bincode::deserialize(v).map_err(Into::into))
                    .collect();
            }
        }
        Ok(vec![])
    }

    fn entries(&self) -> Result<Vec<(K, V)>> {
        let key = self
            .store
            .current_key
            .read()
            .clone()
            .ok_or_else(|| anyhow::anyhow!("No current key set"))?;
        let states = self.store.map_states.read();
        if let Some(state) = states.get(&self.name) {
            if let Some(map) = state.get(&key) {
                return map
                    .iter()
                    .map(|(k, v)| Ok((bincode::deserialize(k)?, bincode::deserialize(v)?)))
                    .collect();
            }
        }
        Ok(vec![])
    }

    fn clear(&self) -> Result<()> {
        let key = self
            .store
            .current_key
            .read()
            .clone()
            .ok_or_else(|| anyhow::anyhow!("No current key set"))?;
        if let Some(state) = self.store.map_states.write().get_mut(&self.name) {
            state.remove(&key);
        }
        Ok(())
    }

    fn is_empty(&self) -> Result<bool> {
        Ok(self.keys()?.is_empty())
    }
}

// ============================================================================
// MemoryReducingState
// ============================================================================

pub(crate) struct MemoryReducingState<V> {
    pub(crate) store: Arc<MemoryKeyedStateStore>,
    pub(crate) name: String,
    pub(crate) reduce_fn: Arc<dyn Fn(&V, &V) -> V + Send + Sync>,
    pub(crate) _phantom: std::marker::PhantomData<V>,
}

impl<V: StateValue> ReducingState<V> for MemoryReducingState<V> {
    fn get(&self) -> Result<Option<V>> {
        let key = self
            .store
            .current_key
            .read()
            .clone()
            .ok_or_else(|| anyhow::anyhow!("No current key set"))?;
        let states = self.store.value_states.read();
        if let Some(state) = states.get(&self.name) {
            if let Some(data) = state.get(&key) {
                return Ok(Some(bincode::deserialize(data)?));
            }
        }
        Ok(None)
    }

    fn add(&self, value: V) -> Result<()> {
        let key = self
            .store
            .current_key
            .read()
            .clone()
            .ok_or_else(|| anyhow::anyhow!("No current key set"))?;
        let new_value = {
            let states = self.store.value_states.read();
            if let Some(state) = states.get(&self.name) {
                if let Some(data) = state.get(&key) {
                    let current: V = bincode::deserialize(data)?;
                    (self.reduce_fn)(&current, &value)
                } else {
                    value
                }
            } else {
                value
            }
        };
        let serialized = Bytes::from(bincode::serialize(&new_value)?);
        self.store
            .value_states
            .write()
            .entry(self.name.clone())
            .or_default()
            .insert(key, serialized);
        Ok(())
    }

    fn clear(&self) -> Result<()> {
        let key = self
            .store
            .current_key
            .read()
            .clone()
            .ok_or_else(|| anyhow::anyhow!("No current key set"))?;
        if let Some(state) = self.store.value_states.write().get_mut(&self.name) {
            state.remove(&key);
        }
        Ok(())
    }
}
