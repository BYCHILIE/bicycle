//! State backends for stateful operators.
//!
//! This module provides pluggable state backends following Flink's state management model:
//! - [`MemoryStateBackend`]: Fast, in-memory state (for development/testing)
//! - [`RocksDBStateBackend`]: Persistent, scalable state (for production)
//!
//! State is organized into:
//! - [`ValueState`]: Single value per key
//! - [`ListState`]: List of values per key
//! - [`MapState`]: Map of values per key
//! - [`ReducingState`]: Aggregated value with reduce function
//!
//! # Example
//!
//! ```ignore
//! use bicycle_state::{MemoryStateBackend, StateBackend, KeyedStateStore, ValueState};
//!
//! let backend = MemoryStateBackend::new();
//! let store = backend.create_keyed_state_store("my-operator")?;
//! store.set_current_key(b"user-123");
//!
//! let counter: Box<dyn ValueState<i64>> = store.get_value_state("counter")?;
//! counter.set(42)?;
//! assert_eq!(counter.get()?, Some(42));
//! ```

mod handle;
pub mod memory;
pub mod rocksdb;
mod traits;

// Re-export main types at crate root for convenience
pub use handle::KeyedStateStoreHandle;
pub use memory::{MemoryKeyedStateStore, MemoryStateBackend};
pub use rocksdb::{RocksDBKeyedStateStore, RocksDBStateBackend};
pub use traits::{
    KeyedStateStore, ListState, MapState, ReducingState, StateBackend, StateKey, StateValue,
    ValueState,
};
