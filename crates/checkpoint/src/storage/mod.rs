//! Checkpoint storage abstraction and implementations.

mod fs;

pub use fs::FsCheckpointStorage;

use anyhow::Result;
use bicycle_core::{StateHandle, TaskId};

/// Abstraction for checkpoint storage locations.
///
/// Implementations handle the actual persistence of checkpoint data,
/// whether to local filesystem, distributed storage, or cloud storage.
#[async_trait::async_trait]
pub trait CheckpointStorage: Send + Sync {
    /// Write state to storage.
    async fn write(
        &self,
        checkpoint_id: u64,
        task_id: &TaskId,
        data: &[u8],
    ) -> Result<StateHandle>;

    /// Read state from storage.
    async fn read(&self, handle: &StateHandle) -> Result<Vec<u8>>;

    /// Delete a checkpoint.
    async fn delete(&self, checkpoint_id: u64) -> Result<()>;

    /// List all checkpoints.
    async fn list_checkpoints(&self) -> Result<Vec<u64>>;
}
