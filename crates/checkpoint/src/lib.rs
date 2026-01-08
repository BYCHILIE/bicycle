//! Checkpoint coordination following Chandy-Lamport distributed snapshots.
//!
//! This module implements Flink-style checkpointing:
//! - Periodic checkpoint triggering
//! - Barrier propagation through the DAG
//! - Barrier alignment for exactly-once semantics
//! - State snapshot coordination
//! - Checkpoint completion and acknowledgment

use anyhow::{Context, Result};
use bicycle_core::{CheckpointBarrier, CheckpointOptions, JobId, StateHandle, TaskId, Timestamp};
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, error, info, warn};

// ============================================================================
// Checkpoint Coordinator (runs on JobManager)
// ============================================================================

/// Coordinates checkpoints across all tasks in a job.
pub struct CheckpointCoordinator {
    job_id: JobId,
    config: CheckpointConfig,
    
    /// Current checkpoint state
    current_checkpoint: RwLock<Option<PendingCheckpoint>>,
    
    /// Completed checkpoints (keep last N)
    completed_checkpoints: RwLock<Vec<CompletedCheckpoint>>,
    
    /// All tasks that need to acknowledge checkpoints
    tasks: RwLock<HashSet<TaskId>>,
    
    /// Channel to receive checkpoint acknowledgments
    ack_rx: RwLock<Option<mpsc::Receiver<CheckpointAck>>>,
    ack_tx: mpsc::Sender<CheckpointAck>,
    
    /// Channel to trigger checkpoints
    trigger_tx: broadcast::Sender<CheckpointBarrier>,
    
    /// Checkpoint storage path
    checkpoint_dir: PathBuf,
    
    /// Next checkpoint ID
    next_checkpoint_id: RwLock<u64>,
    
    /// Statistics
    stats: RwLock<CheckpointStats>,
}

/// Configuration for checkpoint behavior.
#[derive(Debug, Clone)]
pub struct CheckpointConfig {
    /// Interval between checkpoints.
    pub interval: Duration,
    /// Timeout for checkpoint completion.
    pub timeout: Duration,
    /// Minimum pause between checkpoints.
    pub min_pause: Duration,
    /// Maximum concurrent checkpoints.
    pub max_concurrent: usize,
    /// Whether to use unaligned checkpoints.
    pub unaligned: bool,
    /// Number of checkpoints to retain.
    pub num_retained: usize,
    /// Alignment timeout before switching to unaligned.
    pub alignment_timeout: Option<Duration>,
}

impl Default for CheckpointConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(10),
            timeout: Duration::from_secs(600),
            min_pause: Duration::from_millis(500),
            max_concurrent: 1,
            unaligned: false,
            num_retained: 3,
            alignment_timeout: None,
        }
    }
}

/// A checkpoint that is in progress.
#[derive(Debug)]
struct PendingCheckpoint {
    checkpoint_id: u64,
    timestamp: Timestamp,
    start_time: Instant,
    options: CheckpointOptions,
    /// Tasks that have acknowledged
    acknowledged_tasks: HashSet<TaskId>,
    /// State handles from acknowledged tasks
    state_handles: HashMap<TaskId, StateHandle>,
}

impl PendingCheckpoint {
    fn new(checkpoint_id: u64, options: CheckpointOptions) -> Self {
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

impl CheckpointCoordinator {
    pub fn new(job_id: JobId, config: CheckpointConfig, checkpoint_dir: PathBuf) -> Self {
        let (ack_tx, ack_rx) = mpsc::channel(1024);
        let (trigger_tx, _) = broadcast::channel(16);
        
        Self {
            job_id,
            config,
            current_checkpoint: RwLock::new(None),
            completed_checkpoints: RwLock::new(Vec::new()),
            tasks: RwLock::new(HashSet::new()),
            ack_rx: RwLock::new(Some(ack_rx)),
            ack_tx,
            trigger_tx,
            checkpoint_dir,
            next_checkpoint_id: RwLock::new(1),
            stats: RwLock::new(CheckpointStats::default()),
        }
    }

    /// Register a task that participates in checkpointing.
    pub fn register_task(&self, task_id: TaskId) {
        self.tasks.write().insert(task_id);
    }

    /// Unregister a task.
    pub fn unregister_task(&self, task_id: &TaskId) {
        self.tasks.write().remove(task_id);
    }

    /// Get a sender for checkpoint acknowledgments.
    pub fn ack_sender(&self) -> mpsc::Sender<CheckpointAck> {
        self.ack_tx.clone()
    }

    /// Subscribe to checkpoint triggers.
    pub fn subscribe_triggers(&self) -> broadcast::Receiver<CheckpointBarrier> {
        self.trigger_tx.subscribe()
    }

    /// Trigger a new checkpoint.
    pub fn trigger_checkpoint(&self, is_savepoint: bool) -> Result<u64> {
        // Check if we can start a new checkpoint
        {
            let current = self.current_checkpoint.read();
            if current.is_some() && self.config.max_concurrent == 1 {
                anyhow::bail!("Another checkpoint is already in progress");
            }
        }

        let checkpoint_id = {
            let mut id = self.next_checkpoint_id.write();
            let current = *id;
            *id += 1;
            current
        };

        let options = CheckpointOptions {
            is_savepoint,
            is_unaligned: self.config.unaligned,
            alignment_timeout_ms: self.config.alignment_timeout.map(|d| d.as_millis() as u64),
        };

        let pending = PendingCheckpoint::new(checkpoint_id, options.clone());
        *self.current_checkpoint.write() = Some(pending);

        // Create checkpoint directory
        let checkpoint_path = self.checkpoint_dir.join(format!("chk-{}", checkpoint_id));
        std::fs::create_dir_all(&checkpoint_path)?;

        let barrier = CheckpointBarrier {
            checkpoint_id,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            options,
        };

        // Broadcast to all source tasks
        let _ = self.trigger_tx.send(barrier);

        info!(
            checkpoint_id,
            is_savepoint,
            "Triggered checkpoint"
        );

        self.stats.write().total_checkpoints += 1;

        Ok(checkpoint_id)
    }

    /// Process a checkpoint acknowledgment.
    pub fn process_ack(&self, ack: CheckpointAck) -> Result<Option<CompletedCheckpoint>> {
        let mut current = self.current_checkpoint.write();
        
        let pending = current.as_mut()
            .ok_or_else(|| anyhow::anyhow!("No checkpoint in progress"))?;

        if ack.checkpoint_id != pending.checkpoint_id {
            debug!(
                expected = pending.checkpoint_id,
                received = ack.checkpoint_id,
                "Received ack for wrong checkpoint"
            );
            return Ok(None);
        }

        pending.acknowledged_tasks.insert(ack.task_id.clone());
        pending.state_handles.insert(ack.task_id, ack.state_handle);

        let all_tasks = self.tasks.read();
        
        if pending.acknowledged_tasks.len() == all_tasks.len() {
            // Checkpoint complete!
            let duration = pending.start_time.elapsed();
            let total_size: u64 = pending.state_handles.values().map(|h| h.size).sum();

            let completed = CompletedCheckpoint {
                checkpoint_id: pending.checkpoint_id,
                timestamp: pending.timestamp,
                duration,
                state_handles: pending.state_handles.clone(),
                total_size,
            };

            // Update stats
            {
                let mut stats = self.stats.write();
                stats.completed_checkpoints += 1;
                stats.total_duration_ms += duration.as_millis() as u64;
                stats.total_size_bytes += total_size;
                stats.last_checkpoint_time = Some(pending.timestamp);
                stats.last_checkpoint_duration = Some(duration);
            }

            // Store completed checkpoint
            {
                let mut checkpoints = self.completed_checkpoints.write();
                checkpoints.push(completed.clone());
                
                // Retain only last N checkpoints
                while checkpoints.len() > self.config.num_retained {
                    let removed = checkpoints.remove(0);
                    // Clean up old checkpoint files
                    let old_path = self.checkpoint_dir.join(format!("chk-{}", removed.checkpoint_id));
                    let _ = std::fs::remove_dir_all(old_path);
                }
            }

            info!(
                checkpoint_id = completed.checkpoint_id,
                duration_ms = duration.as_millis(),
                size_bytes = total_size,
                "Checkpoint completed"
            );

            *current = None;
            return Ok(Some(completed));
        }

        Ok(None)
    }

    /// Check for checkpoint timeout.
    pub fn check_timeout(&self) -> Option<u64> {
        let mut current = self.current_checkpoint.write();
        
        if let Some(pending) = current.as_ref() {
            if pending.start_time.elapsed() > self.config.timeout {
                let checkpoint_id = pending.checkpoint_id;
                warn!(checkpoint_id, "Checkpoint timed out");
                self.stats.write().failed_checkpoints += 1;
                *current = None;
                return Some(checkpoint_id);
            }
        }
        None
    }

    /// Get the latest completed checkpoint.
    pub fn latest_checkpoint(&self) -> Option<CompletedCheckpoint> {
        self.completed_checkpoints.read().last().cloned()
    }

    /// Get checkpoint statistics.
    pub fn stats(&self) -> CheckpointStats {
        self.stats.read().clone()
    }

    /// Get checkpoint path for a checkpoint ID.
    pub fn checkpoint_path(&self, checkpoint_id: u64) -> PathBuf {
        self.checkpoint_dir.join(format!("chk-{}", checkpoint_id))
    }

    /// Run the checkpoint coordinator loop.
    pub async fn run(&self) -> Result<()> {
        let mut interval = tokio::time::interval(self.config.interval);
        let mut ack_rx = self.ack_rx.write().take()
            .ok_or_else(|| anyhow::anyhow!("Coordinator already running"))?;
        
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    // Trigger periodic checkpoint
                    if let Err(e) = self.trigger_checkpoint(false) {
                        debug!(error = %e, "Failed to trigger checkpoint");
                    }
                }
                
                Some(ack) = ack_rx.recv() => {
                    if let Err(e) = self.process_ack(ack) {
                        error!(error = %e, "Failed to process checkpoint ack");
                    }
                }
            }
            
            // Check for timeouts
            self.check_timeout();
        }
    }
}

// ============================================================================
// Barrier Tracker (runs on each task)
// ============================================================================

/// Tracks checkpoint barriers for a task with multiple input channels.
/// Implements barrier alignment for exactly-once semantics.
pub struct BarrierTracker {
    task_id: TaskId,
    num_channels: usize,
    
    /// Current checkpoint being aligned
    pending_barrier: Option<PendingBarrier>,
    
    /// Buffered records during alignment (channel_index -> records)
    blocked_channels: HashMap<usize, Vec<BufferedRecord>>,
    
    /// Configuration
    alignment_timeout: Option<Duration>,
    #[allow(dead_code)]
    unaligned: bool,
}

struct PendingBarrier {
    checkpoint_id: u64,
    options: CheckpointOptions,
    received_from: HashSet<usize>,
    start_time: Instant,
}

/// A record buffered during barrier alignment.
pub struct BufferedRecord {
    pub channel: usize,
    pub data: Vec<u8>,
}

/// Result of processing a barrier.
pub enum BarrierResult {
    /// Barrier received but still waiting for other channels.
    Pending,
    /// All barriers received, checkpoint can proceed.
    Aligned(CheckpointBarrier),
    /// Switched to unaligned checkpoint due to timeout.
    Unaligned(CheckpointBarrier, Vec<BufferedRecord>),
}

impl BarrierTracker {
    pub fn new(task_id: TaskId, num_channels: usize, unaligned: bool, alignment_timeout: Option<Duration>) -> Self {
        Self {
            task_id,
            num_channels,
            pending_barrier: None,
            blocked_channels: HashMap::new(),
            alignment_timeout,
            unaligned,
        }
    }

    /// Process a barrier received on a channel.
    pub fn process_barrier(&mut self, channel: usize, barrier: CheckpointBarrier) -> BarrierResult {
        if self.num_channels == 1 {
            // Single input, no alignment needed
            return BarrierResult::Aligned(barrier);
        }

        let pending = self.pending_barrier.get_or_insert_with(|| PendingBarrier {
            checkpoint_id: barrier.checkpoint_id,
            options: barrier.options.clone(),
            received_from: HashSet::new(),
            start_time: Instant::now(),
        });

        if barrier.checkpoint_id != pending.checkpoint_id {
            // Barrier from different checkpoint, ignore (shouldn't happen in correct impl)
            warn!(
                expected = pending.checkpoint_id,
                received = barrier.checkpoint_id,
                "Received barrier for wrong checkpoint"
            );
            return BarrierResult::Pending;
        }

        pending.received_from.insert(channel);
        self.blocked_channels.entry(channel).or_default();

        if pending.received_from.len() == self.num_channels {
            // All barriers received
            let result_barrier = CheckpointBarrier {
                checkpoint_id: pending.checkpoint_id,
                timestamp: barrier.timestamp,
                options: pending.options.clone(),
            };
            
            self.pending_barrier = None;
            let buffered: Vec<_> = self.blocked_channels
                .drain()
                .flat_map(|(_, records)| records)
                .collect();
            
            if buffered.is_empty() {
                BarrierResult::Aligned(result_barrier)
            } else {
                BarrierResult::Unaligned(result_barrier, buffered)
            }
        } else {
            BarrierResult::Pending
        }
    }

    /// Buffer a record during barrier alignment.
    pub fn buffer_record(&mut self, channel: usize, data: Vec<u8>) -> bool {
        if let Some(pending) = &self.pending_barrier {
            if pending.received_from.contains(&channel) {
                // This channel is blocked, buffer the record
                self.blocked_channels
                    .entry(channel)
                    .or_default()
                    .push(BufferedRecord { channel, data });
                return true;
            }
        }
        false
    }

    /// Check if a channel is blocked for alignment.
    pub fn is_blocked(&self, channel: usize) -> bool {
        self.pending_barrier
            .as_ref()
            .map(|p| p.received_from.contains(&channel))
            .unwrap_or(false)
    }

    /// Check for alignment timeout.
    pub fn check_timeout(&mut self) -> Option<(CheckpointBarrier, Vec<BufferedRecord>)> {
        if let Some(timeout) = self.alignment_timeout {
            if let Some(pending) = &self.pending_barrier {
                if pending.start_time.elapsed() > timeout {
                    let barrier = CheckpointBarrier {
                        checkpoint_id: pending.checkpoint_id,
                        timestamp: std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_millis() as u64,
                        options: CheckpointOptions {
                            is_unaligned: true,
                            ..pending.options.clone()
                        },
                    };
                    
                    self.pending_barrier = None;
                    let buffered: Vec<_> = self.blocked_channels
                        .drain()
                        .flat_map(|(_, records)| records)
                        .collect();
                    
                    return Some((barrier, buffered));
                }
            }
        }
        None
    }
}

// ============================================================================
// Checkpoint Storage
// ============================================================================

/// Abstraction for checkpoint storage locations.
#[async_trait::async_trait]
pub trait CheckpointStorage: Send + Sync {
    /// Write state to storage.
    async fn write(&self, checkpoint_id: u64, task_id: &TaskId, data: &[u8]) -> Result<StateHandle>;
    
    /// Read state from storage.
    async fn read(&self, handle: &StateHandle) -> Result<Vec<u8>>;
    
    /// Delete a checkpoint.
    async fn delete(&self, checkpoint_id: u64) -> Result<()>;
    
    /// List all checkpoints.
    async fn list_checkpoints(&self) -> Result<Vec<u64>>;
}

/// File-system based checkpoint storage.
pub struct FsCheckpointStorage {
    base_path: PathBuf,
}

impl FsCheckpointStorage {
    pub fn new(base_path: impl AsRef<Path>) -> Result<Self> {
        let base_path = base_path.as_ref().to_path_buf();
        std::fs::create_dir_all(&base_path)?;
        Ok(Self { base_path })
    }
}

#[async_trait::async_trait]
impl CheckpointStorage for FsCheckpointStorage {
    async fn write(&self, checkpoint_id: u64, task_id: &TaskId, data: &[u8]) -> Result<StateHandle> {
        let checkpoint_dir = self.base_path.join(format!("chk-{}", checkpoint_id));
        tokio::fs::create_dir_all(&checkpoint_dir).await?;
        
        let file_name = format!("{}.state", task_id);
        let file_path = checkpoint_dir.join(&file_name);
        
        tokio::fs::write(&file_path, data).await?;
        
        Ok(StateHandle {
            path: file_path.to_string_lossy().to_string(),
            size: data.len() as u64,
            checksum: None,
        })
    }

    async fn read(&self, handle: &StateHandle) -> Result<Vec<u8>> {
        tokio::fs::read(&handle.path)
            .await
            .context("Failed to read state")
    }

    async fn delete(&self, checkpoint_id: u64) -> Result<()> {
        let checkpoint_dir = self.base_path.join(format!("chk-{}", checkpoint_id));
        if checkpoint_dir.exists() {
            tokio::fs::remove_dir_all(&checkpoint_dir).await?;
        }
        Ok(())
    }

    async fn list_checkpoints(&self) -> Result<Vec<u64>> {
        let mut checkpoints = Vec::new();
        let mut entries = tokio::fs::read_dir(&self.base_path).await?;
        
        while let Some(entry) = entries.next_entry().await? {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if name_str.starts_with("chk-") {
                if let Ok(id) = name_str[4..].parse::<u64>() {
                    checkpoints.push(id);
                }
            }
        }
        
        checkpoints.sort();
        Ok(checkpoints)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_barrier_tracker_single_channel() {
        let task_id = TaskId::new(JobId::new(), "test", 0);
        let mut tracker = BarrierTracker::new(task_id, 1, false, None);
        
        let barrier = CheckpointBarrier {
            checkpoint_id: 1,
            timestamp: 1000,
            options: CheckpointOptions::default(),
        };
        
        match tracker.process_barrier(0, barrier) {
            BarrierResult::Aligned(_) => {},
            _ => panic!("Expected aligned result"),
        }
    }

    #[test]
    fn test_barrier_tracker_multi_channel() {
        let task_id = TaskId::new(JobId::new(), "test", 0);
        let mut tracker = BarrierTracker::new(task_id, 3, false, None);
        
        let barrier = CheckpointBarrier {
            checkpoint_id: 1,
            timestamp: 1000,
            options: CheckpointOptions::default(),
        };
        
        // First barrier
        match tracker.process_barrier(0, barrier.clone()) {
            BarrierResult::Pending => {},
            _ => panic!("Expected pending"),
        }
        
        // Second barrier
        match tracker.process_barrier(1, barrier.clone()) {
            BarrierResult::Pending => {},
            _ => panic!("Expected pending"),
        }
        
        // Third barrier - should align
        match tracker.process_barrier(2, barrier) {
            BarrierResult::Aligned(_) => {},
            _ => panic!("Expected aligned"),
        }
    }
}
