//! Stateless operators: Map, Filter, FlatMap.

mod filter;
mod flatmap;
mod map;

pub use filter::FilterOperator;
pub use flatmap::FlatMapOperator;
pub use map::MapOperator;
