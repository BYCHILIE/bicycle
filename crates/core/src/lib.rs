//! Core types shared across the engine.

use serde::{Deserialize, Serialize};

/// Milliseconds since Unix epoch (or any monotonic-ish reference; up to the source).
pub type Timestamp = u64;

/// Messages flowing through operator edges.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StreamMessage<T> {
    Data(T),
    /// Event-time watermark.
    Watermark(Timestamp),
    /// Checkpoint barrier (id).
    Barrier(u64),
    /// End of stream (for bounded inputs / controlled shutdown).
    End,
}

/// A basic event used by examples and common operators.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event<K, V> {
    pub ts: Timestamp,
    pub key: K,
    pub value: V,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowResult<K, V> {
    pub window_start: Timestamp,
    pub window_end: Timestamp,
    pub key: K,
    pub value: V,
}
