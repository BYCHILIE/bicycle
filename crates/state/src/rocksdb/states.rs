//! RocksDB state implementations (ValueState, ListState, MapState, ReducingState).

use anyhow::Result;
use std::sync::Arc;

use crate::traits::{ListState, MapState, ReducingState, StateKey, StateValue, ValueState};

use super::make_rocksdb_key;

// ============================================================================
// RocksDBValueState
// ============================================================================

pub(crate) struct RocksDBValueState<V> {
    pub(crate) db: Arc<rocksdb::DB>,
    pub(crate) prefix: String,
    pub(crate) name: String,
    pub(crate) current_key: Option<Vec<u8>>,
    pub(crate) _phantom: std::marker::PhantomData<V>,
}

impl<V: StateValue> ValueState<V> for RocksDBValueState<V> {
    fn get(&self) -> Result<Option<V>> {
        let user_key = self
            .current_key
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("No current key set"))?;
        let db_key = make_rocksdb_key(&self.prefix, &self.name, user_key, None);
        if let Some(data) = self.db.get(&db_key)? {
            return Ok(Some(bincode::deserialize(&data)?));
        }
        Ok(None)
    }

    fn set(&self, value: V) -> Result<()> {
        let user_key = self
            .current_key
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("No current key set"))?;
        let db_key = make_rocksdb_key(&self.prefix, &self.name, user_key, None);
        self.db.put(&db_key, bincode::serialize(&value)?)?;
        Ok(())
    }

    fn clear(&self) -> Result<()> {
        let user_key = self
            .current_key
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("No current key set"))?;
        let db_key = make_rocksdb_key(&self.prefix, &self.name, user_key, None);
        self.db.delete(&db_key)?;
        Ok(())
    }
}

// ============================================================================
// RocksDBListState
// ============================================================================

pub(crate) struct RocksDBListState<V> {
    pub(crate) db: Arc<rocksdb::DB>,
    pub(crate) prefix: String,
    pub(crate) name: String,
    pub(crate) current_key: Option<Vec<u8>>,
    pub(crate) _phantom: std::marker::PhantomData<V>,
}

impl<V: StateValue> ListState<V> for RocksDBListState<V> {
    fn get(&self) -> Result<Vec<V>> {
        let user_key = self
            .current_key
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("No current key set"))?;
        let db_key = make_rocksdb_key(&self.prefix, &self.name, user_key, None);
        if let Some(data) = self.db.get(&db_key)? {
            return Ok(bincode::deserialize(&data)?);
        }
        Ok(vec![])
    }

    fn add(&self, value: V) -> Result<()> {
        let mut list = self.get()?;
        list.push(value);
        let user_key = self.current_key.as_ref().unwrap();
        let db_key = make_rocksdb_key(&self.prefix, &self.name, user_key, None);
        self.db.put(&db_key, bincode::serialize(&list)?)?;
        Ok(())
    }

    fn add_all(&self, values: Vec<V>) -> Result<()> {
        let mut list = self.get()?;
        list.extend(values);
        let user_key = self.current_key.as_ref().unwrap();
        let db_key = make_rocksdb_key(&self.prefix, &self.name, user_key, None);
        self.db.put(&db_key, bincode::serialize(&list)?)?;
        Ok(())
    }

    fn clear(&self) -> Result<()> {
        let user_key = self
            .current_key
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("No current key set"))?;
        let db_key = make_rocksdb_key(&self.prefix, &self.name, user_key, None);
        self.db.delete(&db_key)?;
        Ok(())
    }
}

// ============================================================================
// RocksDBMapState
// ============================================================================

pub(crate) struct RocksDBMapState<K, V> {
    pub(crate) db: Arc<rocksdb::DB>,
    pub(crate) prefix: String,
    pub(crate) name: String,
    pub(crate) current_key: Option<Vec<u8>>,
    pub(crate) _phantom: std::marker::PhantomData<(K, V)>,
}

impl<K: StateKey, V: StateValue> MapState<K, V> for RocksDBMapState<K, V> {
    fn get(&self, map_key: &K) -> Result<Option<V>> {
        let user_key = self
            .current_key
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("No current key set"))?;
        let map_key_bytes = bincode::serialize(map_key)?;
        let db_key = make_rocksdb_key(&self.prefix, &self.name, user_key, Some(&map_key_bytes));
        if let Some(data) = self.db.get(&db_key)? {
            return Ok(Some(bincode::deserialize(&data)?));
        }
        Ok(None)
    }

    fn put(&self, map_key: K, value: V) -> Result<()> {
        let user_key = self
            .current_key
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("No current key set"))?;
        let map_key_bytes = bincode::serialize(&map_key)?;
        let db_key = make_rocksdb_key(&self.prefix, &self.name, user_key, Some(&map_key_bytes));
        self.db.put(&db_key, bincode::serialize(&value)?)?;
        Ok(())
    }

    fn remove(&self, map_key: &K) -> Result<Option<V>> {
        let result = self.get(map_key)?;
        if result.is_some() {
            let user_key = self.current_key.as_ref().unwrap();
            let map_key_bytes = bincode::serialize(map_key)?;
            let db_key =
                make_rocksdb_key(&self.prefix, &self.name, user_key, Some(&map_key_bytes));
            self.db.delete(&db_key)?;
        }
        Ok(result)
    }

    fn contains(&self, map_key: &K) -> Result<bool> {
        Ok(self.get(map_key)?.is_some())
    }

    fn keys(&self) -> Result<Vec<K>> {
        let user_key = self
            .current_key
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("No current key set"))?;
        let prefix = make_rocksdb_key(&self.prefix, &self.name, user_key, None);
        let mut keys = vec![];
        let iter = self.db.prefix_iterator(&prefix);
        for item in iter {
            let (k, _) = item?;
            if !k.starts_with(&prefix) {
                break;
            }
            // Extract map key from the end
            if let Some(pos) = k.iter().rposition(|&b| b == 0xff) {
                if let Ok(map_key) = bincode::deserialize(&k[pos + 1..]) {
                    keys.push(map_key);
                }
            }
        }
        Ok(keys)
    }

    fn values(&self) -> Result<Vec<V>> {
        self.entries()
            .map(|e| e.into_iter().map(|(_, v)| v).collect())
    }

    fn entries(&self) -> Result<Vec<(K, V)>> {
        let user_key = self
            .current_key
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("No current key set"))?;
        let prefix = make_rocksdb_key(&self.prefix, &self.name, user_key, None);
        let mut entries = vec![];
        let iter = self.db.prefix_iterator(&prefix);
        for item in iter {
            let (k, v) = item?;
            if !k.starts_with(&prefix) {
                break;
            }
            if let Some(pos) = k.iter().rposition(|&b| b == 0xff) {
                if let (Ok(key), Ok(val)) = (
                    bincode::deserialize(&k[pos + 1..]),
                    bincode::deserialize(&v),
                ) {
                    entries.push((key, val));
                }
            }
        }
        Ok(entries)
    }

    fn clear(&self) -> Result<()> {
        let user_key = self
            .current_key
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("No current key set"))?;
        let prefix = make_rocksdb_key(&self.prefix, &self.name, user_key, None);
        let iter = self.db.prefix_iterator(&prefix);
        for item in iter {
            let (k, _) = item?;
            if !k.starts_with(&prefix) {
                break;
            }
            self.db.delete(&k)?;
        }
        Ok(())
    }

    fn is_empty(&self) -> Result<bool> {
        Ok(self.keys()?.is_empty())
    }
}

// ============================================================================
// RocksDBReducingState
// ============================================================================

pub(crate) struct RocksDBReducingState<V> {
    pub(crate) db: Arc<rocksdb::DB>,
    pub(crate) prefix: String,
    pub(crate) name: String,
    pub(crate) current_key: Option<Vec<u8>>,
    pub(crate) reduce_fn: Arc<dyn Fn(&V, &V) -> V + Send + Sync>,
    pub(crate) _phantom: std::marker::PhantomData<V>,
}

impl<V: StateValue> ReducingState<V> for RocksDBReducingState<V> {
    fn get(&self) -> Result<Option<V>> {
        let user_key = self
            .current_key
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("No current key set"))?;
        let db_key = make_rocksdb_key(&self.prefix, &self.name, user_key, None);
        if let Some(data) = self.db.get(&db_key)? {
            return Ok(Some(bincode::deserialize(&data)?));
        }
        Ok(None)
    }

    fn add(&self, value: V) -> Result<()> {
        let user_key = self
            .current_key
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("No current key set"))?;
        let db_key = make_rocksdb_key(&self.prefix, &self.name, user_key, None);
        let new_value = if let Some(data) = self.db.get(&db_key)? {
            let current: V = bincode::deserialize(&data)?;
            (self.reduce_fn)(&current, &value)
        } else {
            value
        };
        self.db.put(&db_key, bincode::serialize(&new_value)?)?;
        Ok(())
    }

    fn clear(&self) -> Result<()> {
        let user_key = self
            .current_key
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("No current key set"))?;
        let db_key = make_rocksdb_key(&self.prefix, &self.name, user_key, None);
        self.db.delete(&db_key)?;
        Ok(())
    }
}
