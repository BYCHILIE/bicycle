//! RocksDB keyed state store implementation.

use anyhow::Result;
use bytes::Bytes;
use parking_lot::RwLock;
use std::sync::Arc;

use crate::traits::{
    KeyedStateStore, ListState, MapState, ReducingState, StateKey, StateValue, ValueState,
};

use super::states::{RocksDBListState, RocksDBMapState, RocksDBReducingState, RocksDBValueState};

/// RocksDB keyed state store.
///
/// Uses a prefix-based key scheme to partition state by operator and key.
pub struct RocksDBKeyedStateStore {
    pub(crate) db: Arc<rocksdb::DB>,
    pub(crate) prefix: String,
    pub(crate) current_key: RwLock<Option<Vec<u8>>>,
}

impl RocksDBKeyedStateStore {
    /// Create a new RocksDB keyed state store.
    pub fn new(db: Arc<rocksdb::DB>, operator_id: String) -> Self {
        Self {
            db,
            prefix: operator_id,
            current_key: RwLock::new(None),
        }
    }

    /// Construct a key for the given state name, user key, and optional sub-key.
    pub fn make_key(&self, state_name: &str, user_key: &[u8], sub_key: Option<&[u8]>) -> Vec<u8> {
        let mut key =
            Vec::with_capacity(self.prefix.len() + state_name.len() + user_key.len() + 32);
        key.extend_from_slice(self.prefix.as_bytes());
        key.push(0xff);
        key.extend_from_slice(state_name.as_bytes());
        key.push(0xff);
        key.extend_from_slice(user_key);
        if let Some(sk) = sub_key {
            key.push(0xff);
            key.extend_from_slice(sk);
        }
        key
    }
}

impl KeyedStateStore for RocksDBKeyedStateStore {
    fn get_value_state<V: StateValue>(&self, name: &str) -> Result<Box<dyn ValueState<V>>> {
        Ok(Box::new(RocksDBValueState {
            db: self.db.clone(),
            prefix: self.prefix.clone(),
            name: name.to_string(),
            current_key: self.current_key.read().clone(),
            _phantom: std::marker::PhantomData,
        }))
    }

    fn get_list_state<V: StateValue>(&self, name: &str) -> Result<Box<dyn ListState<V>>> {
        Ok(Box::new(RocksDBListState {
            db: self.db.clone(),
            prefix: self.prefix.clone(),
            name: name.to_string(),
            current_key: self.current_key.read().clone(),
            _phantom: std::marker::PhantomData,
        }))
    }

    fn get_map_state<K: StateKey, V: StateValue>(
        &self,
        name: &str,
    ) -> Result<Box<dyn MapState<K, V>>> {
        Ok(Box::new(RocksDBMapState {
            db: self.db.clone(),
            prefix: self.prefix.clone(),
            name: name.to_string(),
            current_key: self.current_key.read().clone(),
            _phantom: std::marker::PhantomData,
        }))
    }

    fn get_reducing_state<V: StateValue>(
        &self,
        name: &str,
        reduce_fn: Arc<dyn Fn(&V, &V) -> V + Send + Sync>,
    ) -> Result<Box<dyn ReducingState<V>>> {
        Ok(Box::new(RocksDBReducingState {
            db: self.db.clone(),
            prefix: self.prefix.clone(),
            name: name.to_string(),
            current_key: self.current_key.read().clone(),
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
        // Would iterate and delete all keys with this prefix
        Ok(())
    }

    fn snapshot(&self) -> Result<Bytes> {
        // RocksDB handles snapshots at backend level
        Ok(Bytes::new())
    }

    fn restore(&self, _data: &[u8]) -> Result<()> {
        Ok(())
    }
}
