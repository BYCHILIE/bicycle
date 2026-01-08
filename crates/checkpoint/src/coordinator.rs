//! Checkpoint coordinator implementation.

use anyhow::Result;
use bicycle_core::{CheckpointBarrier, CheckpointOptions, JobId, TaskId};
use parking_lot::RwLock;
use std::collections::HashSet;
use std::path::PathBuf;
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, error, info, warn};

use crate::config::CheckpointConfig;
use crate::types::{CheckpointAck, CheckpointStats, CompletedCheckpoint, PendingCheckpoint};

/// Coordinates checkpoints across all tasks in a job.
///
/// The coordinator runs on the JobManager and is responsible for:
/// - Triggering periodic checkpoints
/// - Collecting acknowledgments from tasks
/// - Managing checkpoint lifecycle (completion, timeout, cleanup)
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

impl CheckpointCoordinator {
    /// Create a new checkpoint coordinator.
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

        info!(checkpoint_id, is_savepoint, "Triggered checkpoint");

        self.stats.write().total_checkpoints += 1;

        Ok(checkpoint_id)
    }

    /// Process a checkpoint acknowledgment.
    pub fn process_ack(&self, ack: CheckpointAck) -> Result<Option<CompletedCheckpoint>> {
        let mut current = self.current_checkpoint.write();

        let pending = current
            .as_mut()
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
                    let old_path = self
                        .checkpoint_dir
                        .join(format!("chk-{}", removed.checkpoint_id));
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
        let mut ack_rx = self
            .ack_rx
            .write()
            .take()
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
