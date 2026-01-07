//! Built-in operators for the MVP.

use anyhow::Context;
use async_trait::async_trait;
use bicycle_core::{Event, StreamMessage, Timestamp, WindowResult};
use bicycle_runtime::{Emitter, Operator};
use std::collections::HashMap;
use tracing::debug;

/// A simple map operator: transforms each data element. Control messages are forwarded.
pub struct MapOperator<F, In, Out> {
    f: F,
    _phantom: std::marker::PhantomData<(In, Out)>,
}

impl<F, In, Out> MapOperator<F, In, Out>
where
    F: FnMut(In) -> Out + Send + 'static,
    In: Send + 'static,
    Out: Send + 'static,
{
    pub fn new(f: F) -> Self {
        Self {
            f,
            _phantom: std::marker::PhantomData,
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
    ) -> anyhow::Result<()> {
        match msg {
            StreamMessage::Data(v) => out.data((self.f)(v)).await,
            StreamMessage::Watermark(ts) => out.watermark(ts).await,
            StreamMessage::Barrier(id) => out.barrier(id).await,
            StreamMessage::End => out.end().await,
        }
    }
}

/// Keyed tumbling-window sum over event-time.
///
/// Input: Event<String, i64>
/// Output: WindowResult<String, i64>
///
/// Watermarks trigger window emission. Late events (ts <= current watermark) are currently dropped.
pub struct TumblingWindowSum {
    size_ms: Timestamp,
    current_wm: Timestamp,
    // (key, window_end) -> sum
    sums: HashMap<(String, Timestamp), i64>,
}

impl TumblingWindowSum {
    pub fn new(size_ms: Timestamp) -> Self {
        Self {
            size_ms,
            current_wm: 0,
            sums: HashMap::new(),
        }
    }

    fn window_end(&self, ts: Timestamp) -> Timestamp {
        // ceil(ts / size) * size
        ((ts / self.size_ms) + 1) * self.size_ms
    }
}

#[async_trait]
impl Operator for TumblingWindowSum {
    type In = Event<String, i64>;
    type Out = WindowResult<String, i64>;

    async fn on_message(
        &mut self,
        msg: StreamMessage<Self::In>,
        out: &mut Emitter<Self::Out>,
    ) -> anyhow::Result<()> {
        match msg {
            StreamMessage::Data(ev) => {
                if ev.ts <= self.current_wm {
                    // MVP behavior: drop late events.
                    return Ok(());
                }
                let w_end = self.window_end(ev.ts);
                let key = (ev.key, w_end);
                *self.sums.entry(key).or_insert(0) += ev.value;
                Ok(())
            }
            StreamMessage::Watermark(wm) => {
                if wm <= self.current_wm {
                    return Ok(());
                }
                self.current_wm = wm;

                // Emit all windows whose end <= watermark.
                // This is O(n) over all active windows; good enough for MVP.
                let mut to_emit: Vec<((String, Timestamp), i64)> = Vec::new();
                for (k, v) in self.sums.iter() {
                    if k.1 <= wm {
                        to_emit.push((k.clone(), *v));
                    }
                }
                for ((key, w_end), sum) in to_emit {
                    self.sums.remove(&(key.clone(), w_end));
                    let res = WindowResult {
                        window_start: w_end - self.size_ms,
                        window_end: w_end,
                        key,
                        value: sum,
                    };
                    out.data(res).await.context("emit window result")?;
                }

                // Forward watermark downstream.
                out.watermark(wm).await?;
                Ok(())
            }
            StreamMessage::Barrier(id) => {
                // Placeholder: state snapshot would happen here.
                debug!(checkpoint_id = id, "checkpoint barrier received (MVP: no-op)");
                out.barrier(id).await
            }
            StreamMessage::End => out.end().await,
        }
    }
}
