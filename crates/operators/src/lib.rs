//! Built-in operators for the streaming engine.
//!
//! This module provides Flink-like operators organized into categories:
//!
//! ## Stateless Operators
//! - [`MapOperator`] - Transform each element
//! - [`FilterOperator`] - Keep elements matching a predicate
//! - [`FlatMapOperator`] - Transform each element into zero or more elements
//!
//! ## Keyed Operators
//! - [`KeyByOperator`] - Extract keys for partitioning
//! - [`ReduceOperator`] - Continuously reduce values per key
//!
//! ## Window Operators
//! - [`TumblingWindowSum`] - Fixed-size, non-overlapping windows
//! - [`SlidingWindowSum`] - Fixed-size, overlapping windows
//! - [`SessionWindowSum`] - Activity-based windows with gap timeout
//! - [`GenericWindowOperator`] - Customizable window aggregations
//!
//! ## Process Functions
//! - [`ProcessFunctionOperator`] - Custom stateful processing

pub mod keyed;
pub mod process;
pub mod stateless;
pub mod window;

// Re-export all operators at crate root for convenience
pub use keyed::{KeyByOperator, KeyedRecord, ReduceOperator};
pub use process::{ProcessContext, ProcessFunctionOperator};
pub use stateless::{FilterOperator, FlatMapOperator, MapOperator};
pub use window::{
    GenericWindowOperator, KeyedEvent, SessionWindowSum, SlidingWindowSum, TumblingWindowSum,
};
