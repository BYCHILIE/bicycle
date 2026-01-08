//! Built-in operators for the streaming engine.
//!
//! This module provides Flink-like operators:
//! - Stateless: Map, Filter, FlatMap
//! - Keyed: KeyBy, Reduce
//! - Windows: Tumbling, Sliding, Session
//! - Process functions for custom logic

use anyhow::{Context, Result};
use async_trait::async_trait;
use bicycle_core::{
    CheckpointBarrier, Event, StreamMessage, TimeWindow, Timestamp, WindowResult, WindowSpec,
};
use bicycle_runtime::{Emitter, Operator};
use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;
use std::marker::PhantomData;
use tracing::debug;

// ============================================================================
// Map Operator
// ============================================================================

/// A simple map operator: transforms each data element. Control messages are forwarded.
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

// ============================================================================
// Filter Operator
// ============================================================================

/// Filter operator: keeps only elements that satisfy a predicate.
pub struct FilterOperator<F, T> {
    predicate: F,
    _phantom: PhantomData<T>,
}

impl<F, T> FilterOperator<F, T>
where
    F: FnMut(&T) -> bool + Send + 'static,
    T: Send + 'static,
{
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

// ============================================================================
// FlatMap Operator
// ============================================================================

/// FlatMap operator: transforms each element into zero or more elements.
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
    I: IntoIterator<Item = Out> + Send + 'static, // <- add 'static
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

// ============================================================================
// KeyBy Operator
// ============================================================================

/// KeyBy extracts a key from each element for downstream keyed operations.
pub struct KeyByOperator<F, T, K> {
    key_selector: F,
    _phantom: PhantomData<(T, K)>,
}

impl<F, T, K> KeyByOperator<F, T, K>
where
    F: Fn(&T) -> K + Send + 'static,
    T: Send + 'static,
    K: Send + 'static,
{
    pub fn new(key_selector: F) -> Self {
        Self {
            key_selector,
            _phantom: PhantomData,
        }
    }
}

/// A keyed record with extracted key.
#[derive(Debug, Clone)]
pub struct KeyedRecord<K, T> {
    pub key: K,
    pub value: T,
}

#[async_trait]
impl<F, T, K> Operator for KeyByOperator<F, T, K>
where
    F: Fn(&T) -> K + Send + 'static,
    T: Send + 'static,
    K: Send + 'static,
{
    type In = T;
    type Out = KeyedRecord<K, T>;

    async fn on_message(
        &mut self,
        msg: StreamMessage<Self::In>,
        out: &mut Emitter<Self::Out>,
    ) -> Result<()> {
        match msg {
            StreamMessage::Data(v) => {
                let key = (self.key_selector)(&v);
                out.data(KeyedRecord { key, value: v }).await
            }
            StreamMessage::Watermark(ts) => out.watermark(ts).await,
            StreamMessage::Barrier(b) => out.barrier(b).await,
            StreamMessage::LatencyMarker(m) => out.latency_marker(m).await,
            StreamMessage::End => out.end().await,
        }
    }
}

// ============================================================================
// Reduce Operator
// ============================================================================

/// Reduce operator: continuously reduces values per key.
pub struct ReduceOperator<F, K, V> {
    reduce_fn: F,
    state: HashMap<K, V>,
    _phantom: PhantomData<(K, V)>,
}

impl<F, K, V> ReduceOperator<F, K, V>
where
    F: Fn(&V, &V) -> V + Send + 'static,
    K: Eq + Hash + Clone + Send + 'static,
    V: Clone + Send + 'static,
{
    pub fn new(reduce_fn: F) -> Self {
        Self {
            reduce_fn,
            state: HashMap::new(),
            _phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<F, K, V> Operator for ReduceOperator<F, K, V>
where
    F: Fn(&V, &V) -> V + Send + 'static,
    K: Eq + Hash + Clone + Send + 'static,
    V: Clone + Send + 'static,
{
    type In = KeyedRecord<K, V>;
    type Out = KeyedRecord<K, V>;

    async fn on_message(
        &mut self,
        msg: StreamMessage<Self::In>,
        out: &mut Emitter<Self::Out>,
    ) -> Result<()> {
        match msg {
            StreamMessage::Data(record) => {
                let new_value = if let Some(current) = self.state.get(&record.key) {
                    (self.reduce_fn)(current, &record.value)
                } else {
                    record.value.clone()
                };
                self.state.insert(record.key.clone(), new_value.clone());
                out.data(KeyedRecord {
                    key: record.key,
                    value: new_value,
                })
                .await
            }
            StreamMessage::Watermark(ts) => out.watermark(ts).await,
            StreamMessage::Barrier(b) => {
                debug!(checkpoint_id = b.checkpoint_id, "ReduceOperator checkpoint");
                out.barrier(b).await
            }
            StreamMessage::LatencyMarker(m) => out.latency_marker(m).await,
            StreamMessage::End => out.end().await,
        }
    }
}

// ============================================================================
// Tumbling Window Sum Operator
// ============================================================================

/// Keyed tumbling-window sum over event-time.
///
/// Input: Event<String, i64>
/// Output: WindowResult<String, i64>
///
/// Watermarks trigger window emission. Late events (ts <= current watermark) are dropped.
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

// ============================================================================
// Sliding Window Operator
// ============================================================================

/// Sliding window sum operator.
pub struct SlidingWindowSum {
    size_ms: Timestamp,
    slide_ms: Timestamp,
    current_wm: Timestamp,
    // (key, window_end) -> sum
    sums: HashMap<(String, Timestamp), i64>,
}

impl SlidingWindowSum {
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

// ============================================================================
// Session Window Operator
// ============================================================================

/// Session window operator that groups events by activity with a gap timeout.
pub struct SessionWindowSum {
    gap_ms: Timestamp,
    current_wm: Timestamp,
    // key -> list of (timestamp, value) pairs
    sessions: HashMap<String, Vec<(Timestamp, i64)>>,
}

impl SessionWindowSum {
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

// ============================================================================
// Generic Window Operator
// ============================================================================

/// Generic window operator supporting different window types and aggregation functions.
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

/// Keyed event for generic window operator.
#[derive(Debug, Clone)]
pub struct KeyedEvent<K, V> {
    pub ts: Timestamp,
    pub key: K,
    pub value: V,
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

// ============================================================================
// Process Function Operator
// ============================================================================

/// Context available to process functions.
pub struct ProcessContext {
    pub current_watermark: Timestamp,
    pub current_processing_time: Timestamp,
}

/// A process function operator for custom stateful processing.
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
    pub fn new(process_fn: F) -> Self {
        Self {
            process_fn,
            state: S::default(),
            current_wm: 0,
            _phantom: PhantomData,
        }
    }

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

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_window_assignment_tumbling() {
        let op: GenericWindowOperator<String, i64, i64, i64, _, _> =
            GenericWindowOperator::new(
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
        let op: GenericWindowOperator<String, i64, i64, i64, _, _> =
            GenericWindowOperator::new(
                WindowSpec::sliding(10000, 5000),
                |acc, val| *acc += val,
                |acc| *acc,
            );

        let windows = op.assign_windows(7000);
        assert!(windows.len() >= 1);
    }
}
