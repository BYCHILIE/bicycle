//! FlatMap operator implementation.

use anyhow::Result;
use async_trait::async_trait;
use bicycle_core::StreamMessage;
use bicycle_runtime::{Emitter, Operator};
use std::marker::PhantomData;

/// FlatMap operator: transforms each element into zero or more elements.
///
/// # Example
///
/// ```ignore
/// let words = FlatMapOperator::new(|line: String| line.split_whitespace().map(String::from).collect::<Vec<_>>());
/// ```
pub struct FlatMapOperator<F, In, Out, I>
where
    I: IntoIterator<Item = Out>,
{
    f: F,
    _phantom: PhantomData<(In, Out, I)>,
}

impl<F, In, Out, I> FlatMapOperator<F, In, Out, I>
where
    F: FnMut(In) -> I + Send + 'static,
    I: IntoIterator<Item = Out> + Send,
    In: Send + 'static,
    Out: Send + 'static,
{
    /// Create a new flatmap operator with the given transformation function.
    pub fn new(f: F) -> Self {
        Self {
            f,
            _phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<F, In, Out, I> Operator for FlatMapOperator<F, In, Out, I>
where
    F: FnMut(In) -> I + Send + 'static,
    In: Send + 'static,
    Out: Send + 'static,
    I: IntoIterator<Item = Out> + Send + 'static,
{
    type In = In;
    type Out = Out;

    async fn on_message(
        &mut self,
        msg: StreamMessage<Self::In>,
        out: &mut Emitter<Self::Out>,
    ) -> Result<()> {
        match msg {
            StreamMessage::Data(v) => {
                // IMPORTANT: don't keep the iterator alive across `.await`
                let items: Vec<Out> = (self.f)(v).into_iter().collect();

                for item in items {
                    out.data(item).await?;
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
