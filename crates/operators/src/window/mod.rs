//! Window operators: Tumbling, Sliding, Session, Generic.

mod generic;
mod session;
mod sliding;
mod tumbling;

pub use generic::{GenericWindowOperator, KeyedEvent};
pub use session::SessionWindowSum;
pub use sliding::SlidingWindowSum;
pub use tumbling::TumblingWindowSum;
