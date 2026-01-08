//! Reduce operator implementation.

use anyhow::Result;
use async_trait::async_trait;
use bicycle_core::StreamMessage;
use bicycle_runtime::{Emitter, Operator};
use std::collections::HashMap;
use std::hash::Hash;
use std::marker::PhantomData;
use tracing::debug;

use super::KeyedRecord;

/// Reduce operator: continuously reduces values per key.
///
/// Maintains state per key and emits updated aggregation after each input.
///
/// # Example
///
/// ```ignore
/// let sum = ReduceOperator::new(|a: &i64, b: &i64| a + b);
/// ```
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
    /// Create a new reduce operator with the given reduce function.
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
