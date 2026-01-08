//! Session window operator implementation.

use anyhow::Result;
use async_trait::async_trait;
use bicycle_core::{Event, StreamMessage, Timestamp, WindowResult};
use bicycle_runtime::{Emitter, Operator};
use std::collections::HashMap;

/// Session window operator that groups events by activity with a gap timeout.
///
/// Events are grouped into sessions based on activity. A session ends when
/// no events arrive within the gap timeout.
///
/// # Example
///
/// ```ignore
/// // 30-second session gap
/// let window = SessionWindowSum::new(30000);
/// ```
pub struct SessionWindowSum {
    gap_ms: Timestamp,
    current_wm: Timestamp,
    // key -> list of (timestamp, value) pairs
    sessions: HashMap<String, Vec<(Timestamp, i64)>>,
}

impl SessionWindowSum {
    /// Create a new session window sum operator.
    ///
    /// # Arguments
    /// * `gap_ms` - Session gap timeout in milliseconds
    pub fn new(gap_ms: Timestamp) -> Self {
        Self {
            gap_ms,
            current_wm: 0,
            sessions: HashMap::new(),
        }
    }

    fn merge_and_emit(&mut self, key: &str, wm: Timestamp) -> Vec<WindowResult<String, i64>> {
        let mut results = Vec::new();

        if let Some(events) = self.sessions.get_mut(key) {
            if events.is_empty() {
                return results;
            }

            events.sort_by_key(|(ts, _)| *ts);

            let mut session_start = events[0].0;
            let mut session_end = events[0].0;
            let mut session_sum = 0i64;
            let mut remaining = Vec::new();

            for (ts, value) in events.drain(..) {
                if ts > session_end + self.gap_ms {
                    // Gap detected - emit previous session if closed
                    if session_end + self.gap_ms <= wm {
                        results.push(WindowResult {
                            window_start: session_start,
                            window_end: session_end + 1,
                            key: key.to_string(),
                            value: session_sum,
                        });
                    } else {
                        // Session not closed yet, keep events
                        remaining.push((session_start, session_sum));
                    }
                    session_start = ts;
                    session_end = ts;
                    session_sum = value;
                } else {
                    session_end = ts.max(session_end);
                    session_sum += value;
                }
            }

            // Handle last session
            if session_end + self.gap_ms <= wm {
                results.push(WindowResult {
                    window_start: session_start,
                    window_end: session_end + 1,
                    key: key.to_string(),
                    value: session_sum,
                });
            } else {
                // Keep for later
                *events = vec![(session_start, session_sum)];
                for (ts, val) in remaining {
                    events.push((ts, val));
                }
            }
        }

        results
    }
}

#[async_trait]
impl Operator for SessionWindowSum {
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
                self.sessions
                    .entry(ev.key)
                    .or_default()
                    .push((ev.ts, ev.value));
                Ok(())
            }
            StreamMessage::Watermark(wm) => {
                if wm <= self.current_wm {
                    return Ok(());
                }
                self.current_wm = wm;

                let keys: Vec<String> = self.sessions.keys().cloned().collect();
                for key in keys {
                    let results = self.merge_and_emit(&key, wm);
                    for result in results {
                        out.data(result).await?;
                    }
                }

                out.watermark(wm).await
            }
            StreamMessage::Barrier(b) => out.barrier(b).await,
            StreamMessage::LatencyMarker(m) => out.latency_marker(m).await,
            StreamMessage::End => out.end().await,
        }
    }
}
