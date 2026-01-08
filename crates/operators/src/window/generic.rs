//! Generic window operator implementation.

use anyhow::Result;
use async_trait::async_trait;
use bicycle_core::{StreamMessage, TimeWindow, Timestamp, WindowResult, WindowSpec};
use bicycle_runtime::{Emitter, Operator};
use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;
use std::marker::PhantomData;

/// Keyed event for generic window operator.
#[derive(Debug, Clone)]
pub struct KeyedEvent<K, V> {
    /// Event timestamp.
    pub ts: Timestamp,
    /// Event key.
    pub key: K,
    /// Event value.
    pub value: V,
}

/// Generic window operator supporting different window types and aggregation functions.
///
/// This operator provides a flexible way to define custom window aggregations
/// with user-defined accumulator and result functions.
///
/// # Type Parameters
/// * `K` - Key type
/// * `V` - Input value type
/// * `A` - Accumulator type
/// * `R` - Result type
/// * `AF` - Accumulate function type
/// * `RF` - Result function type
///
/// # Example
///
/// ```ignore
/// let window = GenericWindowOperator::new(
///     WindowSpec::tumbling(5000),
///     |acc: &mut i64, val: &i64| *acc += val,
///     |acc: &i64| *acc,
/// );
/// ```
pub struct GenericWindowOperator<K, V, A, R, AF, RF>
where
    K: Eq + Hash + Clone + Send + 'static,
    V: Clone + Send + 'static,
    A: Clone + Send + Default + 'static,
    R: Send + 'static,
    AF: Fn(&mut A, &V) + Send + 'static,
    RF: Fn(&A) -> R + Send + 'static,
{
    window_spec: WindowSpec,
    current_wm: Timestamp,
    windows: HashMap<K, BTreeMap<TimeWindow, A>>,
    accumulate_fn: AF,
    result_fn: RF,
    _phantom: PhantomData<(K, V, R)>,
}

impl<K, V, A, R, AF, RF> GenericWindowOperator<K, V, A, R, AF, RF>
where
    K: Eq + Hash + Clone + Send + 'static,
    V: Clone + Send + 'static,
    A: Clone + Send + Default + 'static,
    R: Send + 'static,
    AF: Fn(&mut A, &V) + Send + 'static,
    RF: Fn(&A) -> R + Send + 'static,
{
    /// Create a new generic window operator.
    ///
    /// # Arguments
    /// * `window_spec` - Window specification (tumbling, sliding, session, or global)
    /// * `accumulate_fn` - Function to accumulate values into the accumulator
    /// * `result_fn` - Function to convert the accumulator to a result
    pub fn new(window_spec: WindowSpec, accumulate_fn: AF, result_fn: RF) -> Self {
        Self {
            window_spec,
            current_wm: 0,
            windows: HashMap::new(),
            accumulate_fn,
            result_fn,
            _phantom: PhantomData,
        }
    }

    fn assign_windows(&self, ts: Timestamp) -> Vec<TimeWindow> {
        match &self.window_spec {
            WindowSpec::Tumbling { size_ms } => {
                let start = (ts / size_ms) * size_ms;
                vec![TimeWindow::new(start, start + size_ms)]
            }
            WindowSpec::Sliding { size_ms, slide_ms } => {
                let mut windows = Vec::new();
                let last_start = (ts / slide_ms) * slide_ms;
                let mut start = last_start;
                loop {
                    let end = start + size_ms;
                    if ts >= start && ts < end {
                        windows.push(TimeWindow::new(start, end));
                    }
                    if start < *slide_ms || end <= ts {
                        break;
                    }
                    start -= slide_ms;
                }
                windows
            }
            WindowSpec::Session { .. } => vec![],
            WindowSpec::Global => vec![TimeWindow::new(0, Timestamp::MAX)],
        }
    }
}

#[async_trait]
impl<K, V, A, R, AF, RF> Operator for GenericWindowOperator<K, V, A, R, AF, RF>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
    A: Clone + Send + Sync + Default + 'static,
    R: Send + 'static,
    AF: Fn(&mut A, &V) + Send + Sync + 'static,
    RF: Fn(&A) -> R + Send + Sync + 'static,
{
    type In = KeyedEvent<K, V>;
    type Out = WindowResult<K, R>;

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
                let windows = self.assign_windows(ev.ts);
                for window in windows {
                    let key_windows = self.windows.entry(ev.key.clone()).or_default();
                    let acc = key_windows.entry(window).or_default();
                    (self.accumulate_fn)(acc, &ev.value);
                }
                Ok(())
            }
            StreamMessage::Watermark(wm) => {
                if wm <= self.current_wm {
                    return Ok(());
                }
                self.current_wm = wm;

                let keys: Vec<K> = self.windows.keys().cloned().collect();
                for key in keys {
                    if let Some(key_windows) = self.windows.get_mut(&key) {
                        let completed: Vec<TimeWindow> = key_windows
                            .keys()
                            .filter(|w| w.end <= wm)
                            .cloned()
                            .collect();

                        for window in completed {
                            if let Some(acc) = key_windows.remove(&window) {
                                let result = (self.result_fn)(&acc);
                                out.data(WindowResult::new(
                                    window.start,
                                    window.end,
                                    key.clone(),
                                    result,
                                ))
                                .await?;
                            }
                        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_window_assignment_tumbling() {
        let op: GenericWindowOperator<String, i64, i64, i64, _, _> = GenericWindowOperator::new(
            WindowSpec::tumbling(5000),
            |acc, val| *acc += val,
            |acc| *acc,
        );

        let windows = op.assign_windows(3000);
        assert_eq!(windows.len(), 1);
        assert_eq!(windows[0], TimeWindow::new(0, 5000));

        let windows = op.assign_windows(7000);
        assert_eq!(windows.len(), 1);
        assert_eq!(windows[0], TimeWindow::new(5000, 10000));
    }

    #[test]
    fn test_window_assignment_sliding() {
        let op: GenericWindowOperator<String, i64, i64, i64, _, _> = GenericWindowOperator::new(
            WindowSpec::sliding(10000, 5000),
            |acc, val| *acc += val,
            |acc| *acc,
        );

        let windows = op.assign_windows(7000);
        assert!(windows.len() >= 1);
    }
}
