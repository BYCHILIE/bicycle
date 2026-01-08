//! Filter operator implementation.

use anyhow::Result;
use async_trait::async_trait;
use bicycle_core::StreamMessage;
use bicycle_runtime::{Emitter, Operator};
use std::marker::PhantomData;

/// Filter operator: keeps only elements that satisfy a predicate.
///
/// # Example
///
/// ```ignore
/// let positive = FilterOperator::new(|x: &i32| *x > 0);
/// ```
pub struct FilterOperator<F, T> {
    predicate: F,
    _phantom: PhantomData<T>,
}

impl<F, T> FilterOperator<F, T>
where
    F: FnMut(&T) -> bool + Send + 'static,
    T: Send + 'static,
{
    /// Create a new filter operator with the given predicate.
    pub fn new(predicate: F) -> Self {
        Self {
            predicate,
            _phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<F, T> Operator for FilterOperator<F, T>
where
    F: FnMut(&T) -> bool + Send + 'static,
    T: Send + 'static,
{
    type In = T;
    type Out = T;

    async fn on_message(
        &mut self,
        msg: StreamMessage<Self::In>,
        out: &mut Emitter<Self::Out>,
    ) -> Result<()> {
        match msg {
            StreamMessage::Data(v) => {
                if (self.predicate)(&v) {
                    out.data(v).await?;
                }
                Ok(())
            }
            StreamMessage::Watermark(ts) => out.watermark(ts).await,
            StreamMessage::Barrier(b) => out.barrier(b).await,
            StreamMessage::LatencyMarker(m) => out.latency_marker(m).await,
            StreamMessage::End => out.end().await,
        }
    }
}
