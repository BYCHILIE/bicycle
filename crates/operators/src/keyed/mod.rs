//! Keyed operators: KeyBy, Reduce.

mod keyby;
mod reduce;

pub use keyby::{KeyByOperator, KeyedRecord};
pub use reduce::ReduceOperator;
