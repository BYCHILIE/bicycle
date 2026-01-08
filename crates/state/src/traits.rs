//! Core traits and interfaces for state management.

use anyhow::Result;
use async_trait::async_trait;
use bicycle_core::StateHandle;
use bytes::Bytes;
use serde::{de::DeserializeOwned, Serialize};
use std::path::Path;
use std::sync::Arc;

use crate::handle::KeyedStateStoreHandle;

// ============================================================================
// Trait Bounds
// ============================================================================

/// Trait bound for types that can be used as state keys.
pub trait StateKey:
    Serialize + DeserializeOwned + Clone + Eq + std::hash::Hash + Send + Sync + 'static
{
}

/// Trait bound for types that can be stored as state values.
pub trait StateValue: Serialize + DeserializeOwned + Clone + Send + Sync + 'static {}

impl<T> StateKey for T where
    T: Serialize + DeserializeOwned + Clone + Eq + std::hash::Hash + Send + Sync + 'static
{
}

impl<T> StateValue for T where T: Serialize + DeserializeOwned + Clone + Send + Sync + 'static {}

// ============================================================================
// State Backend Trait
// ============================================================================

/// Trait for state backend implementations.
///
/// A state backend is responsible for:
/// - Creating keyed state stores for operators
/// - Snapshotting state for checkpoints
/// - Restoring state from checkpoints
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

// ============================================================================
// Keyed State Store Trait
// ============================================================================

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

// ============================================================================
// State Interfaces
// ============================================================================

/// Single value state.
///
/// Stores a single value per key. Typical use cases include counters,
/// running totals, or any single-valued state.
pub trait ValueState<V>: Send + Sync {
    /// Get the current value, if any.
    fn get(&self) -> Result<Option<V>>;

    /// Set the value.
    fn set(&self, value: V) -> Result<()>;

    /// Clear the value.
    fn clear(&self) -> Result<()>;
}

/// List state - appends values to a list.
///
/// Stores a list of values per key. Useful for collecting events
/// or maintaining ordered sequences.
pub trait ListState<V>: Send + Sync {
    /// Get all values in the list.
    fn get(&self) -> Result<Vec<V>>;

    /// Add a single value to the list.
    fn add(&self, value: V) -> Result<()>;

    /// Add multiple values to the list.
    fn add_all(&self, values: Vec<V>) -> Result<()>;

    /// Clear the list.
    fn clear(&self) -> Result<()>;
}

/// Map state - nested key-value store.
///
/// Stores a map of values per key. Useful for maintaining
/// lookups or associations within a keyed context.
pub trait MapState<K, V>: Send + Sync {
    /// Get a value by map key.
    fn get(&self, key: &K) -> Result<Option<V>>;

    /// Put a key-value pair.
    fn put(&self, key: K, value: V) -> Result<()>;

    /// Remove a key-value pair, returning the old value if present.
    fn remove(&self, key: &K) -> Result<Option<V>>;

    /// Check if the map contains a key.
    fn contains(&self, key: &K) -> Result<bool>;

    /// Get all keys in the map.
    fn keys(&self) -> Result<Vec<K>>;

    /// Get all values in the map.
    fn values(&self) -> Result<Vec<V>>;

    /// Get all key-value pairs.
    fn entries(&self) -> Result<Vec<(K, V)>>;

    /// Clear the map.
    fn clear(&self) -> Result<()>;

    /// Check if the map is empty.
    fn is_empty(&self) -> Result<bool>;
}

/// Reducing state - aggregates values with a reduce function.
///
/// Automatically combines incoming values with the stored value
/// using a reduce function. Useful for running aggregations.
pub trait ReducingState<V>: Send + Sync {
    /// Get the current aggregated value, if any.
    fn get(&self) -> Result<Option<V>>;

    /// Add a value, combining it with the existing value using the reduce function.
    fn add(&self, value: V) -> Result<()>;

    /// Clear the aggregated value.
    fn clear(&self) -> Result<()>;
}
