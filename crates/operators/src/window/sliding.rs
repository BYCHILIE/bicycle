//! Sliding window operator implementation.

use anyhow::Result;
use async_trait::async_trait;
use bicycle_core::{Event, StreamMessage, Timestamp, WindowResult};
use bicycle_runtime::{Emitter, Operator};
use std::collections::HashMap;

/// Sliding window sum operator.
///
/// Creates overlapping windows of a fixed size that slide by a specified interval.
///
/// # Example
///
/// ```ignore
/// // 10-second windows sliding every 5 seconds
/// let window = SlidingWindowSum::new(10000, 5000);
/// ```
pub struct SlidingWindowSum {
    size_ms: Timestamp,
    slide_ms: Timestamp,
    current_wm: Timestamp,
    // (key, window_end) -> sum
    sums: HashMap<(String, Timestamp), i64>,
}

impl SlidingWindowSum {
    /// Create a new sliding window sum operator.
    ///
    /// # Arguments
    /// * `size_ms` - Window size in milliseconds
    /// * `slide_ms` - Slide interval in milliseconds
    pub fn new(size_ms: Timestamp, slide_ms: Timestamp) -> Self {
        Self {
            size_ms,
            slide_ms,
            current_wm: 0,
            sums: HashMap::new(),
        }
    }

    fn assign_windows(&self, ts: Timestamp) -> Vec<Timestamp> {
        let mut windows = Vec::new();
        let last_start = (ts / self.slide_ms) * self.slide_ms;
        let mut start = last_start;

        loop {
            let end = start + self.size_ms;
            if ts >= start && ts < end {
                windows.push(end);
            }
            if start < self.slide_ms {
                break;
            }
            start -= self.slide_ms;
            if end <= ts {
                break;
            }
        }
        windows
    }
}

#[async_trait]
impl Operator for SlidingWindowSum {
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
                    return Ok(());
                }
                for w_end in self.assign_windows(ev.ts) {
                    let key = (ev.key.clone(), w_end);
                    *self.sums.entry(key).or_insert(0) += ev.value;
                }
                Ok(())
            }
            StreamMessage::Watermark(wm) => {
                if wm <= self.current_wm {
                    return Ok(());
                }
                self.current_wm = wm;

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
                    out.data(res).await?;
                }

                out.watermark(wm).await
            }
            StreamMessage::Barrier(b) => out.barrier(b).await,
            StreamMessage::LatencyMarker(m) => out.latency_marker(m).await,
            StreamMessage::End => out.end().await,
        }
    }
}
