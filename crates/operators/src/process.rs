//! Process function operator for custom stateful processing.

use anyhow::Result;
use async_trait::async_trait;
use bicycle_core::{StreamMessage, Timestamp};
use bicycle_runtime::{Emitter, Operator};
use std::marker::PhantomData;

/// Context available to process functions.
pub struct ProcessContext {
    /// Current watermark (event time progress).
    pub current_watermark: Timestamp,
    /// Current processing time (wall clock).
    pub current_processing_time: Timestamp,
}

/// A process function operator for custom stateful processing.
///
/// This operator provides maximum flexibility for implementing custom
/// processing logic with access to state and timing information.
///
/// # Type Parameters
/// * `F` - Process function type
/// * `In` - Input type
/// * `Out` - Output type
/// * `S` - State type
///
/// # Example
///
/// ```ignore
/// let process = ProcessFunctionOperator::new(|state: &mut MyState, input, ctx| {
///     // Custom processing logic
///     vec![output]
/// });
/// ```
pub struct ProcessFunctionOperator<F, In, Out, S>
where
    F: FnMut(&mut S, In, &ProcessContext) -> Vec<Out> + Send + 'static,
    S: Default + Send + 'static,
{
    process_fn: F,
    state: S,
    current_wm: Timestamp,
    _phantom: PhantomData<(In, Out)>,
}

impl<F, In, Out, S> ProcessFunctionOperator<F, In, Out, S>
where
    F: FnMut(&mut S, In, &ProcessContext) -> Vec<Out> + Send + 'static,
    In: Send + 'static,
    Out: Send + 'static,
    S: Default + Send + 'static,
{
    /// Create a new process function operator with default state.
    pub fn new(process_fn: F) -> Self {
        Self {
            process_fn,
            state: S::default(),
            current_wm: 0,
            _phantom: PhantomData,
        }
    }

    /// Create a new process function operator with initial state.
    pub fn with_state(process_fn: F, initial_state: S) -> Self {
        Self {
            process_fn,
            state: initial_state,
            current_wm: 0,
            _phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<F, In, Out, S> Operator for ProcessFunctionOperator<F, In, Out, S>
where
    F: FnMut(&mut S, In, &ProcessContext) -> Vec<Out> + Send + 'static,
    In: Send + 'static,
    Out: Send + 'static,
    S: Default + Send + 'static,
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
                let ctx = ProcessContext {
                    current_watermark: self.current_wm,
                    current_processing_time: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as Timestamp,
                };
                let outputs = (self.process_fn)(&mut self.state, v, &ctx);
                for output in outputs {
                    out.data(output).await?;
                }
                Ok(())
            }
            StreamMessage::Watermark(wm) => {
                self.current_wm = wm;
                out.watermark(wm).await
            }
            StreamMessage::Barrier(b) => out.barrier(b).await,
            StreamMessage::LatencyMarker(m) => out.latency_marker(m).await,
            StreamMessage::End => out.end().await,
        }
    }
}
