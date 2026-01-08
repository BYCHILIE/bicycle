//! Tumbling window operator implementation.

use anyhow::{Context, Result};
use async_trait::async_trait;
use bicycle_core::{Event, StreamMessage, Timestamp, WindowResult};
use bicycle_runtime::{Emitter, Operator};
use std::collections::HashMap;
use tracing::debug;

/// Keyed tumbling-window sum over event-time.
///
/// Input: Event<String, i64>
/// Output: WindowResult<String, i64>
///
/// Watermarks trigger window emission. Late events (ts <= current watermark) are dropped.
///
/// # Example
///
/// ```ignore
/// // 5-second tumbling windows
/// let window = TumblingWindowSum::new(5000);
/// ```
pub struct TumblingWindowSum {
    size_ms: Timestamp,
    current_wm: Timestamp,
    // (key, window_end) -> sum
    sums: HashMap<(String, Timestamp), i64>,
}

impl TumblingWindowSum {
    /// Create a new tumbling window sum operator.
    ///
    /// # Arguments
    /// * `size_ms` - Window size in milliseconds
    pub fn new(size_ms: Timestamp) -> Self {
        Self {
            size_ms,
            current_wm: 0,
            sums: HashMap::new(),
        }
    }

    fn window_end(&self, ts: Timestamp) -> Timestamp {
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
    ) -> Result<()> {
        match msg {
            StreamMessage::Data(ev) => {
                if ev.ts <= self.current_wm {
                    // Late event - drop
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

                // Emit all windows whose end <= watermark
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

                out.watermark(wm).await
            }
            StreamMessage::Barrier(b) => {
                debug!(checkpoint_id = b.checkpoint_id, "TumblingWindowSum checkpoint");
                out.barrier(b).await
            }
            StreamMessage::LatencyMarker(m) => out.latency_marker(m).await,
            StreamMessage::End => out.end().await,
        }
    }
}
