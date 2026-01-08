//! State backends for stateful operators.
//!
//! This module provides pluggable state backends following Flink's state management model:
//! - MemoryStateBackend: Fast, in-memory state (for development/testing)
//! - RocksDBStateBackend: Persistent, scalable state (for production)
//!
//! State is organized into:
//! - ValueState: Single value per key
//! - ListState: List of values per key
//! - MapState: Map of values per key
//! - ReducingState: Aggregated value with reduce function

use anyhow::{Context, Result};
use async_trait::async_trait;
use bicycle_core::StateHandle;
use bytes::Bytes;
use parking_lot::RwLock;
use serde::{de::DeserializeOwned, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::info;

// ============================================================================
// State Backend Trait
// ============================================================================

/// Trait for state backend implementations.
#[async_trait]
pub trait StateBackend: Send + Sync + 'static {
    /// Create a new keyed state store for an operator.
    fn create_keyed_state_store(&self, operator_id: &str) -> Result<Box<KeyedStateStoreHandle>>;

    /// Snapshot state for a checkpoint.
    async fn snapshot(&self, checkpoint_id: u64, path: &Path) -> Result<StateHandle>;

    /// Restore state from a checkpoint.
    async fn restore(&self, handle: &StateHandle) -> Result<()>;

    /// Get the name of this backend.
    fn name(&self) -> &'static str;
}

/// Store for keyed state, partitioned by key.
///
/// NOTE: This trait is NOT object-safe because it has generic methods.
/// We avoid `Box<dyn KeyedStateStore>` and use `KeyedStateStoreHandle` instead.
pub trait KeyedStateStore: Send + Sync {
    /// Get or create a ValueState.
    fn get_value_state<V: StateValue>(&self, name: &str) -> Result<Box<dyn ValueState<V>>>;

    /// Get or create a ListState.
    fn get_list_state<V: StateValue>(&self, name: &str) -> Result<Box<dyn ListState<V>>>;

    /// Get or create a MapState.
    fn get_map_state<K: StateKey, V: StateValue>(
        &self,
        name: &str,
    ) -> Result<Box<dyn MapState<K, V>>>;

    /// Get or create a ReducingState.
    fn get_reducing_state<V: StateValue>(
        &self,
        name: &str,
        reduce_fn: Arc<dyn Fn(&V, &V) -> V + Send + Sync>,
    ) -> Result<Box<dyn ReducingState<V>>>;

    /// Set the current key for subsequent state operations.
    fn set_current_key(&self, key: &[u8]);

    /// Get the current key.
    fn get_current_key(&self) -> Option<Vec<u8>>;

    /// Clear all state for the current key.
    fn clear_current_key(&self) -> Result<()>;

    /// Snapshot all state in this store.
    fn snapshot(&self) -> Result<Bytes>;

    /// Restore state from snapshot.
    fn restore(&self, data: &[u8]) -> Result<()>;
}

/// Concrete dynamic dispatch for KeyedStateStore (because KeyedStateStore is not object-safe).
pub enum KeyedStateStoreHandle {
    Memory(Arc<MemoryKeyedStateStore>),
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

// Trait bounds for state keys and values
pub trait StateKey:
Serialize + DeserializeOwned + Clone + Eq + std::hash::Hash + Send + Sync + 'static
{
}
pub trait StateValue: Serialize + DeserializeOwned + Clone + Send + Sync + 'static {}

impl<T> StateKey for T where
    T: Serialize + DeserializeOwned + Clone + Eq + std::hash::Hash + Send + Sync + 'static
{
}
impl<T> StateValue for T where T: Serialize + DeserializeOwned + Clone + Send + Sync + 'static {}

// ============================================================================
// State Interfaces
// ============================================================================

/// Single value state.
pub trait ValueState<V>: Send + Sync {
    fn get(&self) -> Result<Option<V>>;
    fn set(&self, value: V) -> Result<()>;
    fn clear(&self) -> Result<()>;
}

/// List state - appends values to a list.
pub trait ListState<V>: Send + Sync {
    fn get(&self) -> Result<Vec<V>>;
    fn add(&self, value: V) -> Result<()>;
    fn add_all(&self, values: Vec<V>) -> Result<()>;
    fn clear(&self) -> Result<()>;
}

/// Map state - nested key-value store.
pub trait MapState<K, V>: Send + Sync {
    fn get(&self, key: &K) -> Result<Option<V>>;
    fn put(&self, key: K, value: V) -> Result<()>;
    fn remove(&self, key: &K) -> Result<Option<V>>;
    fn contains(&self, key: &K) -> Result<bool>;
    fn keys(&self) -> Result<Vec<K>>;
    fn values(&self) -> Result<Vec<V>>;
    fn entries(&self) -> Result<Vec<(K, V)>>;
    fn clear(&self) -> Result<()>;
    fn is_empty(&self) -> Result<bool>;
}

/// Reducing state - aggregates values with a reduce function.
pub trait ReducingState<V>: Send + Sync {
    fn get(&self) -> Result<Option<V>>;
    fn add(&self, value: V) -> Result<()>;
    fn clear(&self) -> Result<()>;
}

// ============================================================================
// Memory State Backend
// ============================================================================

/// In-memory state backend for development and testing.
pub struct MemoryStateBackend {
    stores: RwLock<HashMap<String, Arc<MemoryKeyedStateStore>>>,
}

impl MemoryStateBackend {
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

/// In-memory keyed state store.
pub struct MemoryKeyedStateStore {
    current_key: RwLock<Option<Vec<u8>>>,
    value_states: RwLock<HashMap<String, HashMap<Vec<u8>, Bytes>>>,
    list_states: RwLock<HashMap<String, HashMap<Vec<u8>, Vec<Bytes>>>>,
    map_states: RwLock<HashMap<String, HashMap<Vec<u8>, HashMap<Vec<u8>, Bytes>>>>,
}

impl MemoryKeyedStateStore {
    pub fn new() -> Self {
        Self {
            current_key: RwLock::new(None),
            value_states: RwLock::new(HashMap::new()),
            list_states: RwLock::new(HashMap::new()),
            map_states: RwLock::new(HashMap::new()),
        }
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
        let vs = self.value_states.read();
        let ls = self.list_states.read();
        let ms = self.map_states.read();
        let data = (vs.clone(), ls.clone(), ms.clone());
        let serialized = bincode::serialize(&data)?;
        Ok(Bytes::from(serialized))
    }

    fn restore(&self, data: &[u8]) -> Result<()> {
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

// Memory state implementations
struct MemoryValueState<V> {
    store: Arc<MemoryKeyedStateStore>,
    name: String,
    _phantom: std::marker::PhantomData<V>,
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

struct MemoryListState<V> {
    store: Arc<MemoryKeyedStateStore>,
    name: String,
    _phantom: std::marker::PhantomData<V>,
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

struct MemoryMapState<K, V> {
    store: Arc<MemoryKeyedStateStore>,
    name: String,
    _phantom: std::marker::PhantomData<(K, V)>,
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

struct MemoryReducingState<V> {
    store: Arc<MemoryKeyedStateStore>,
    name: String,
    reduce_fn: Arc<dyn Fn(&V, &V) -> V + Send + Sync>,
    _phantom: std::marker::PhantomData<V>,
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

// ============================================================================
// RocksDB State Backend
// ============================================================================

/// RocksDB-based state backend for production use.
/// Supports large state that exceeds memory and incremental checkpoints.
pub struct RocksDBStateBackend {
    db_path: PathBuf,
    db: Arc<rocksdb::DB>,
}

impl RocksDBStateBackend {
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        let db_path = path.as_ref().to_path_buf();
        std::fs::create_dir_all(&db_path)?;

        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
        opts.set_write_buffer_size(64 * 1024 * 1024); // 64MB
        opts.set_max_write_buffer_number(3);
        opts.set_target_file_size_base(64 * 1024 * 1024);
        opts.set_level_compaction_dynamic_level_bytes(true);

        // Enable bloom filters for faster lookups
        let mut block_opts = rocksdb::BlockBasedOptions::default();
        block_opts.set_bloom_filter(10.0, false);
        block_opts.set_cache_index_and_filter_blocks(true);
        opts.set_block_based_table_factory(&block_opts);

        let db = rocksdb::DB::open(&opts, &db_path)?;

        info!(path = %db_path.display(), "RocksDB state backend initialized");

        Ok(Self {
            db_path,
            db: Arc::new(db),
        })
    }
}

#[async_trait]
impl StateBackend for RocksDBStateBackend {
    fn create_keyed_state_store(&self, operator_id: &str) -> Result<Box<KeyedStateStoreHandle>> {
        Ok(Box::new(KeyedStateStoreHandle::Rocks(
            RocksDBKeyedStateStore::new(self.db.clone(), operator_id.to_string()),
        )))
    }

    async fn snapshot(&self, checkpoint_id: u64, path: &Path) -> Result<StateHandle> {
        let checkpoint_path = path.join(format!("rocksdb-chk-{}", checkpoint_id));
        std::fs::create_dir_all(&checkpoint_path)?;

        // Create RocksDB checkpoint (very efficient - hard links SST files)
        let checkpoint = rocksdb::checkpoint::Checkpoint::new(&self.db)?;
        checkpoint.create_checkpoint(&checkpoint_path)?;

        let size = dir_size(&checkpoint_path)?;

        info!(
            checkpoint_id,
            path = %checkpoint_path.display(),
            size_mb = size / (1024 * 1024),
            "RocksDB checkpoint created"
        );

        Ok(StateHandle {
            path: checkpoint_path.to_string_lossy().to_string(),
            size,
            checksum: None,
        })
    }

    async fn restore(&self, handle: &StateHandle) -> Result<()> {
        info!(path = %handle.path, "Restoring from RocksDB checkpoint");
        // Note: In production, you'd close the DB, copy files, and reopen
        // For now, we just log - full implementation requires DB lifecycle management
        Ok(())
    }

    fn name(&self) -> &'static str {
        "rocksdb"
    }
}

/// RocksDB keyed state store.
pub struct RocksDBKeyedStateStore {
    db: Arc<rocksdb::DB>,
    prefix: String,
    current_key: RwLock<Option<Vec<u8>>>,
}

impl RocksDBKeyedStateStore {
    pub fn new(db: Arc<rocksdb::DB>, operator_id: String) -> Self {
        Self {
            db,
            prefix: operator_id,
            current_key: RwLock::new(None),
        }
    }

    fn make_key(&self, state_name: &str, user_key: &[u8], sub_key: Option<&[u8]>) -> Vec<u8> {
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

// RocksDB state implementations
struct RocksDBValueState<V> {
    db: Arc<rocksdb::DB>,
    prefix: String,
    name: String,
    current_key: Option<Vec<u8>>,
    _phantom: std::marker::PhantomData<V>,
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

struct RocksDBListState<V> {
    db: Arc<rocksdb::DB>,
    prefix: String,
    name: String,
    current_key: Option<Vec<u8>>,
    _phantom: std::marker::PhantomData<V>,
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

struct RocksDBMapState<K, V> {
    db: Arc<rocksdb::DB>,
    prefix: String,
    name: String,
    current_key: Option<Vec<u8>>,
    _phantom: std::marker::PhantomData<(K, V)>,
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
        self.entries().map(|e| e.into_iter().map(|(_, v)| v).collect())
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

struct RocksDBReducingState<V> {
    db: Arc<rocksdb::DB>,
    prefix: String,
    name: String,
    current_key: Option<Vec<u8>>,
    reduce_fn: Arc<dyn Fn(&V, &V) -> V + Send + Sync>,
    _phantom: std::marker::PhantomData<V>,
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

// Helper functions
fn make_rocksdb_key(prefix: &str, name: &str, user_key: &[u8], sub_key: Option<&[u8]>) -> Vec<u8> {
    let mut key = Vec::new();
    key.extend_from_slice(prefix.as_bytes());
    key.push(0xff);
    key.extend_from_slice(name.as_bytes());
    key.push(0xff);
    key.extend_from_slice(user_key);
    if let Some(sk) = sub_key {
        key.push(0xff);
        key.extend_from_slice(sk);
    }
    key
}

fn dir_size(path: &Path) -> Result<u64> {
    let mut size = 0;
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        if metadata.is_file() {
            size += metadata.len();
        } else if metadata.is_dir() {
            size += dir_size(&entry.path())?;
        }
    }
    Ok(size)
}

#[cfg(test)]
mod tests {
    use super::*;

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
