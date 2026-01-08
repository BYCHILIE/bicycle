//! RocksDB state backend implementation.

use anyhow::Result;
use async_trait::async_trait;
use bicycle_core::StateHandle;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::info;

use crate::handle::KeyedStateStoreHandle;
use crate::traits::StateBackend;

use super::RocksDBKeyedStateStore;

/// RocksDB-based state backend for production use.
///
/// Supports large state that exceeds memory and incremental checkpoints.
/// Uses RocksDB's native checkpoint mechanism for efficient snapshots.
pub struct RocksDBStateBackend {
    db_path: PathBuf,
    db: Arc<rocksdb::DB>,
}

impl RocksDBStateBackend {
    /// Create a new RocksDB state backend at the given path.
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        let db_path = path.as_ref().to_path_buf();
        std::fs::create_dir_all(&db_path)?;

        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
        opts.set_write_buffer_size(64 * 1024 * 1024); // 64MB
        opts.set_max_write_buffer_number(3);
        opts.set_target_file_size_base(64 * 1024 * 1024);
        opts.set_level_compaction_dynamic_level_bytes(true);

        // Enable bloom filters for faster lookups
        let mut block_opts = rocksdb::BlockBasedOptions::default();
        block_opts.set_bloom_filter(10.0, false);
        block_opts.set_cache_index_and_filter_blocks(true);
        opts.set_block_based_table_factory(&block_opts);

        let db = rocksdb::DB::open(&opts, &db_path)?;

        info!(path = %db_path.display(), "RocksDB state backend initialized");

        Ok(Self {
            db_path,
            db: Arc::new(db),
        })
    }

    /// Get the database path.
    pub fn db_path(&self) -> &Path {
        &self.db_path
    }
}

#[async_trait]
impl StateBackend for RocksDBStateBackend {
    fn create_keyed_state_store(&self, operator_id: &str) -> Result<Box<KeyedStateStoreHandle>> {
        Ok(Box::new(KeyedStateStoreHandle::Rocks(
            RocksDBKeyedStateStore::new(self.db.clone(), operator_id.to_string()),
        )))
    }

    async fn snapshot(&self, checkpoint_id: u64, path: &Path) -> Result<StateHandle> {
        let checkpoint_path = path.join(format!("rocksdb-chk-{}", checkpoint_id));
        std::fs::create_dir_all(&checkpoint_path)?;

        // Create RocksDB checkpoint (very efficient - hard links SST files)
        let checkpoint = rocksdb::checkpoint::Checkpoint::new(&self.db)?;
        checkpoint.create_checkpoint(&checkpoint_path)?;

        let size = dir_size(&checkpoint_path)?;

        info!(
            checkpoint_id,
            path = %checkpoint_path.display(),
            size_mb = size / (1024 * 1024),
            "RocksDB checkpoint created"
        );

        Ok(StateHandle {
            path: checkpoint_path.to_string_lossy().to_string(),
            size,
            checksum: None,
        })
    }

    async fn restore(&self, handle: &StateHandle) -> Result<()> {
        info!(path = %handle.path, "Restoring from RocksDB checkpoint");
        // Note: In production, you'd close the DB, copy files, and reopen
        // For now, we just log - full implementation requires DB lifecycle management
        Ok(())
    }

    fn name(&self) -> &'static str {
        "rocksdb"
    }
}

/// Calculate the total size of a directory.
fn dir_size(path: &Path) -> Result<u64> {
    let mut size = 0;
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        if metadata.is_file() {
            size += metadata.len();
        } else if metadata.is_dir() {
            size += dir_size(&entry.path())?;
        }
    }
    Ok(size)
}
