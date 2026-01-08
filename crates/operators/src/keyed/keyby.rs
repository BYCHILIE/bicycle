//! KeyBy operator implementation.

use anyhow::Result;
use async_trait::async_trait;
use bicycle_core::StreamMessage;
use bicycle_runtime::{Emitter, Operator};
use std::marker::PhantomData;

/// A keyed record with extracted key.
#[derive(Debug, Clone)]
pub struct KeyedRecord<K, T> {
    /// The extracted key.
    pub key: K,
    /// The original value.
    pub value: T,
}

/// KeyBy extracts a key from each element for downstream keyed operations.
///
/// This is the entry point for keyed stream processing, allowing operators
/// like `Reduce` and window functions to operate per-key.
///
/// # Example
///
/// ```ignore
/// let by_user = KeyByOperator::new(|event: &Event| event.user_id.clone());
/// ```
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
    /// Create a new keyby operator with the given key selector function.
    pub fn new(key_selector: F) -> Self {
        Self {
            key_selector,
            _phantom: PhantomData,
        }
    }
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
