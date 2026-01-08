//! Core types shared across the engine.
//!
//! This module defines the fundamental data structures used throughout Bicycle,
//! following Apache Flink's streaming model.

use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::hash::Hash;

/// Milliseconds since Unix epoch (or any monotonic reference).
pub type Timestamp = u64;

/// Unique identifier for jobs, tasks, operators, etc.
pub type Id = String;

// ============================================================================
// Stream Messages
// ============================================================================

/// Messages flowing through operator edges.
/// 
/// This enum represents all types of records that flow through the stream:
/// - Data records carrying user payloads
/// - Control records for coordination (watermarks, barriers, etc.)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StreamMessage<T> {
    /// User data record.
    Data(T),
    /// Event-time watermark indicating progress.
    Watermark(Timestamp),
    /// Checkpoint barrier for exactly-once semantics.
    Barrier(CheckpointBarrier),
    /// Latency marker for measuring end-to-end latency.
    LatencyMarker(LatencyMarker),
    /// End of stream (for bounded inputs / controlled shutdown).
    End,
}

impl<T> StreamMessage<T> {
    pub fn is_data(&self) -> bool {
        matches!(self, StreamMessage::Data(_))
    }

    pub fn is_watermark(&self) -> bool {
        matches!(self, StreamMessage::Watermark(_))
    }

    pub fn is_barrier(&self) -> bool {
        matches!(self, StreamMessage::Barrier(_))
    }

    pub fn is_end(&self) -> bool {
        matches!(self, StreamMessage::End)
    }

    pub fn map<U, F: FnOnce(T) -> U>(self, f: F) -> StreamMessage<U> {
        match self {
            StreamMessage::Data(v) => StreamMessage::Data(f(v)),
            StreamMessage::Watermark(ts) => StreamMessage::Watermark(ts),
            StreamMessage::Barrier(b) => StreamMessage::Barrier(b),
            StreamMessage::LatencyMarker(m) => StreamMessage::LatencyMarker(m),
            StreamMessage::End => StreamMessage::End,
        }
    }
}

/// Checkpoint barrier with metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointBarrier {
    pub checkpoint_id: u64,
    pub timestamp: Timestamp,
    pub options: CheckpointOptions,
}

/// Options for checkpoint behavior.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CheckpointOptions {
    /// Whether this is a savepoint (user-triggered, portable).
    pub is_savepoint: bool,
    /// Whether to use unaligned checkpoints.
    pub is_unaligned: bool,
    /// Timeout for aligned checkpoints before switching to unaligned.
    pub alignment_timeout_ms: Option<u64>,
}

/// Latency marker for measuring processing latency.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatencyMarker {
    pub operator_id: Id,
    pub subtask_index: u32,
    pub marked_time: Timestamp,
}

// ============================================================================
// Events and Records
// ============================================================================

/// A keyed event with timestamp - the primary data carrier.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event<K, V> {
    pub ts: Timestamp,
    pub key: K,
    pub value: V,
}

impl<K, V> Event<K, V> {
    pub fn new(ts: Timestamp, key: K, value: V) -> Self {
        Self { ts, key, value }
    }

    pub fn map_value<U, F: FnOnce(V) -> U>(self, f: F) -> Event<K, U> {
        Event {
            ts: self.ts,
            key: self.key,
            value: f(self.value),
        }
    }
}

/// A record with optional key for partitioning.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record<T> {
    pub timestamp: Timestamp,
    pub key: Option<Vec<u8>>,
    pub value: T,
}

impl<T> Record<T> {
    pub fn new(timestamp: Timestamp, value: T) -> Self {
        Self {
            timestamp,
            key: None,
            value,
        }
    }

    pub fn with_key(timestamp: Timestamp, key: Vec<u8>, value: T) -> Self {
        Self {
            timestamp,
            key: Some(key),
            value,
        }
    }
}

// ============================================================================
// Window Types
// ============================================================================

/// Result of a window aggregation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowResult<K, V> {
    pub window_start: Timestamp,
    pub window_end: Timestamp,
    pub key: K,
    pub value: V,
}

impl<K, V> WindowResult<K, V> {
    pub fn new(window_start: Timestamp, window_end: Timestamp, key: K, value: V) -> Self {
        Self {
            window_start,
            window_end,
            key,
            value,
        }
    }
}

/// Window specification.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WindowSpec {
    /// Fixed-size, non-overlapping windows.
    Tumbling { size_ms: u64 },
    /// Fixed-size, overlapping windows.
    Sliding { size_ms: u64, slide_ms: u64 },
    /// Session windows based on activity gaps.
    Session { gap_ms: u64 },
    /// Global window (single window for all data).
    Global,
}

impl WindowSpec {
    pub fn tumbling(size_ms: u64) -> Self {
        Self::Tumbling { size_ms }
    }

    pub fn sliding(size_ms: u64, slide_ms: u64) -> Self {
        Self::Sliding { size_ms, slide_ms }
    }

    pub fn session(gap_ms: u64) -> Self {
        Self::Session { gap_ms }
    }
}

/// A window instance with start and end times.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TimeWindow {
    pub start: Timestamp,
    pub end: Timestamp,
}

impl TimeWindow {
    pub fn new(start: Timestamp, end: Timestamp) -> Self {
        Self { start, end }
    }

    pub fn contains(&self, ts: Timestamp) -> bool {
        ts >= self.start && ts < self.end
    }

    pub fn duration(&self) -> u64 {
        self.end - self.start
    }

    /// Get the maximum timestamp for this window.
    pub fn max_timestamp(&self) -> Timestamp {
        self.end.saturating_sub(1)
    }

    /// Merge two windows (for session windows).
    pub fn merge(&self, other: &TimeWindow) -> TimeWindow {
        TimeWindow {
            start: self.start.min(other.start),
            end: self.end.max(other.end),
        }
    }

    pub fn intersects(&self, other: &TimeWindow) -> bool {
        self.start < other.end && other.start < self.end
    }
}

impl PartialOrd for TimeWindow {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TimeWindow {
    fn cmp(&self, other: &Self) -> Ordering {
        self.start.cmp(&other.start).then(self.end.cmp(&other.end))
    }
}

// ============================================================================
// Partitioning
// ============================================================================

/// Strategy for partitioning data across parallel instances.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
pub enum PartitionStrategy {
    /// One-to-one forwarding (same parallelism required).
    #[default]
    Forward,
    /// Round-robin distribution.
    Rebalance,
    /// Hash-based partitioning by key.
    Hash,
    /// Broadcast to all downstream instances.
    Broadcast,
    /// Custom partitioner function.
    Custom,
}

// ============================================================================
// Job and Task Identifiers
// ============================================================================

/// Identifies a specific execution of a job.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct JobId(pub String);

impl JobId {
    pub fn new() -> Self {
        Self(uuid::Uuid::new_v4().to_string())
    }

    pub fn from_string(s: impl Into<String>) -> Self {
        Self(s.into())
    }
}

impl Default for JobId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for JobId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Identifies a task (operator instance) within a job.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TaskId {
    pub job_id: JobId,
    pub vertex_id: String,
    pub subtask_index: u32,
}

impl TaskId {
    pub fn new(job_id: JobId, vertex_id: impl Into<String>, subtask_index: u32) -> Self {
        Self {
            job_id,
            vertex_id: vertex_id.into(),
            subtask_index,
        }
    }
}

impl std::fmt::Display for TaskId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}-{}", self.job_id, self.vertex_id, self.subtask_index)
    }
}

// ============================================================================
// State and Checkpoint Types
// ============================================================================

/// Handle to persisted state in a checkpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateHandle {
    /// Path or identifier for the state location.
    pub path: String,
    /// Size in bytes.
    pub size: u64,
    /// Checksum for verification.
    pub checksum: Option<String>,
}

/// Metadata about a completed checkpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointMetadata {
    pub checkpoint_id: u64,
    pub timestamp: Timestamp,
    pub duration_ms: u64,
    pub state_handles: Vec<(TaskId, StateHandle)>,
    pub total_size: u64,
}

// ============================================================================
// Errors
// ============================================================================

/// Errors that can occur in stream processing.
#[derive(Debug, thiserror::Error)]
pub enum StreamError {
    #[error("Channel closed")]
    ChannelClosed,

    #[error("Operator error: {0}")]
    OperatorError(String),

    #[error("Serialization error: {0}")]
    SerializationError(String),

    #[error("State error: {0}")]
    StateError(String),

    #[error("Checkpoint error: {0}")]
    CheckpointError(String),

    #[error("Network error: {0}")]
    NetworkError(String),

    #[error("Configuration error: {0}")]
    ConfigError(String),
}

impl From<bincode::Error> for StreamError {
    fn from(e: bincode::Error) -> Self {
        StreamError::SerializationError(e.to_string())
    }
}

