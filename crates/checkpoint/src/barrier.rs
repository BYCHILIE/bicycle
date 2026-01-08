//! Barrier tracking for checkpoint alignment.

use bicycle_core::{CheckpointBarrier, CheckpointOptions, TaskId};
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};
use tracing::warn;

/// Tracks checkpoint barriers for a task with multiple input channels.
///
/// Implements barrier alignment for exactly-once semantics. When a barrier
/// arrives on one channel, records from that channel are buffered until
/// barriers arrive on all channels.
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
    /// Channel the record came from.
    pub channel: usize,
    /// Serialized record data.
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
    /// Create a new barrier tracker.
    ///
    /// # Arguments
    /// * `task_id` - ID of the task this tracker belongs to
    /// * `num_channels` - Number of input channels to align
    /// * `unaligned` - Whether to use unaligned checkpoints
    /// * `alignment_timeout` - Optional timeout before switching to unaligned
    pub fn new(
        task_id: TaskId,
        num_channels: usize,
        unaligned: bool,
        alignment_timeout: Option<Duration>,
    ) -> Self {
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
            let buffered: Vec<_> = self
                .blocked_channels
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
    ///
    /// Returns true if the record was buffered (channel is blocked),
    /// false if the record should be processed normally.
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
    ///
    /// Returns the barrier and buffered records if timeout occurred.
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
                    let buffered: Vec<_> = self
                        .blocked_channels
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

#[cfg(test)]
mod tests {
    use super::*;
    use bicycle_core::JobId;

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
            BarrierResult::Aligned(_) => {}
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
            BarrierResult::Pending => {}
            _ => panic!("Expected pending"),
        }

        // Second barrier
        match tracker.process_barrier(1, barrier.clone()) {
            BarrierResult::Pending => {}
            _ => panic!("Expected pending"),
        }

        // Third barrier - should align
        match tracker.process_barrier(2, barrier) {
            BarrierResult::Aligned(_) => {}
            _ => panic!("Expected aligned"),
        }
    }
}
