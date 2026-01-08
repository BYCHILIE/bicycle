//! File-system based checkpoint storage.

use anyhow::{Context, Result};
use bicycle_core::{StateHandle, TaskId};
use std::path::{Path, PathBuf};

use super::CheckpointStorage;

/// File-system based checkpoint storage.
///
/// Stores checkpoints in a directory structure:
/// ```text
/// base_path/
///   chk-1/
///     task-0.state
///     task-1.state
///   chk-2/
///     ...
/// ```
pub struct FsCheckpointStorage {
    base_path: PathBuf,
}

impl FsCheckpointStorage {
    /// Create a new filesystem checkpoint storage.
    pub fn new(base_path: impl AsRef<Path>) -> Result<Self> {
        let base_path = base_path.as_ref().to_path_buf();
        std::fs::create_dir_all(&base_path)?;
        Ok(Self { base_path })
    }

    /// Get the base path.
    pub fn base_path(&self) -> &Path {
        &self.base_path
    }
}

#[async_trait::async_trait]
impl CheckpointStorage for FsCheckpointStorage {
    async fn write(
        &self,
        checkpoint_id: u64,
        task_id: &TaskId,
        data: &[u8],
    ) -> Result<StateHandle> {
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
