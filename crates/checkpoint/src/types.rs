//! Common types for checkpoint coordination.

use bicycle_core::{CheckpointOptions, StateHandle, TaskId, Timestamp};
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

/// A checkpoint that is in progress.
#[derive(Debug)]
pub struct PendingCheckpoint {
    pub checkpoint_id: u64,
    pub timestamp: Timestamp,
    pub start_time: Instant,
    pub options: CheckpointOptions,
    /// Tasks that have acknowledged
    pub acknowledged_tasks: HashSet<TaskId>,
    /// State handles from acknowledged tasks
    pub state_handles: HashMap<TaskId, StateHandle>,
}

impl PendingCheckpoint {
    pub fn new(checkpoint_id: u64, options: CheckpointOptions) -> Self {
        Self {
            checkpoint_id,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            start_time: Instant::now(),
            options,
            acknowledged_tasks: HashSet::new(),
            state_handles: HashMap::new(),
        }
    }
}

/// A successfully completed checkpoint.
#[derive(Debug, Clone)]
pub struct CompletedCheckpoint {
    pub checkpoint_id: u64,
    pub timestamp: Timestamp,
    pub duration: Duration,
    pub state_handles: HashMap<TaskId, StateHandle>,
    pub total_size: u64,
}

/// Acknowledgment from a task that it has completed its checkpoint.
#[derive(Debug, Clone)]
pub struct CheckpointAck {
    pub task_id: TaskId,
    pub checkpoint_id: u64,
    pub state_handle: StateHandle,
}

/// Statistics about checkpointing.
#[derive(Debug, Default, Clone)]
pub struct CheckpointStats {
    pub total_checkpoints: u64,
    pub completed_checkpoints: u64,
    pub failed_checkpoints: u64,
    pub total_duration_ms: u64,
    pub total_size_bytes: u64,
    pub last_checkpoint_time: Option<Timestamp>,
    pub last_checkpoint_duration: Option<Duration>,
}
