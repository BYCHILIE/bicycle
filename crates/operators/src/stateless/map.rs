//! Map operator implementation.

use anyhow::Result;
use async_trait::async_trait;
use bicycle_core::StreamMessage;
use bicycle_runtime::{Emitter, Operator};
use std::marker::PhantomData;

/// A simple map operator: transforms each data element.
///
/// Control messages (watermarks, barriers, etc.) are forwarded unchanged.
///
/// # Example
///
/// ```ignore
/// let double = MapOperator::new(|x: i32| x * 2);
/// ```
pub struct MapOperator<F, In, Out> {
    f: F,
    _phantom: PhantomData<(In, Out)>,
}

impl<F, In, Out> MapOperator<F, In, Out>
where
    F: FnMut(In) -> Out + Send + 'static,
    In: Send + 'static,
    Out: Send + 'static,
{
    /// Create a new map operator with the given transformation function.
    pub fn new(f: F) -> Self {
        Self {
            f,
            _phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<F, In, Out> Operator for MapOperator<F, In, Out>
where
    F: FnMut(In) -> Out + Send + 'static,
    In: Send + 'static,
    Out: Send + 'static,
{
    type In = In;
    type Out = Out;

    async fn on_message(
        &mut self,
        msg: StreamMessage<Self::In>,
        out: &mut Emitter<Self::Out>,
    ) -> Result<()> {
        match msg {
            StreamMessage::Data(v) => out.data((self.f)(v)).await,
            StreamMessage::Watermark(ts) => out.watermark(ts).await,
            StreamMessage::Barrier(b) => out.barrier(b).await,
            StreamMessage::LatencyMarker(m) => out.latency_marker(m).await,
            StreamMessage::End => out.end().await,
        }
    }
}
