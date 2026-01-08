//! Checkpoint configuration.

use std::time::Duration;

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
