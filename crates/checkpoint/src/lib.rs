//! Checkpoint coordination following Chandy-Lamport distributed snapshots.
//!
//! This module implements Flink-style checkpointing:
//! - Periodic checkpoint triggering
//! - Barrier propagation through the DAG
//! - Barrier alignment for exactly-once semantics
//! - State snapshot coordination
//! - Checkpoint completion and acknowledgment
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────┐
//! │         CheckpointCoordinator           │
//! │  (runs on JobManager)                   │
//! │                                         │
//! │  - Triggers checkpoints periodically    │
//! │  - Collects acknowledgments             │
//! │  - Manages lifecycle                    │
//! └─────────────────────────────────────────┘
//!                    │
//!                    │ Barriers
//!                    ▼
//! ┌─────────────────────────────────────────┐
//! │            BarrierTracker               │
//! │  (runs on each task)                    │
//! │                                         │
//! │  - Aligns barriers across channels      │
//! │  - Buffers records during alignment     │
//! │  - Handles timeout to unaligned mode    │
//! └─────────────────────────────────────────┘
//!                    │
//!                    │ State
//!                    ▼
//! ┌─────────────────────────────────────────┐
//! │          CheckpointStorage              │
//! │  (pluggable backend)                    │
//! │                                         │
//! │  - FsCheckpointStorage (local/NFS)      │
//! │  - (future: S3, HDFS, etc.)             │
//! └─────────────────────────────────────────┘
//! ```

mod barrier;
mod config;
mod coordinator;
pub mod storage;
mod types;

pub use barrier::{BarrierResult, BarrierTracker, BufferedRecord};
pub use config::CheckpointConfig;
pub use coordinator::CheckpointCoordinator;
pub use storage::{CheckpointStorage, FsCheckpointStorage};
pub use types::{CheckpointAck, CheckpointStats, CompletedCheckpoint};
