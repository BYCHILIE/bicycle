//! Exactly-once sink guarantees through two-phase commit.
//!
//! This module implements transactional sinks that provide exactly-once
//! delivery guarantees by coordinating with Bicycle's checkpoint mechanism.
//!
//! The approach follows Flink's two-phase commit protocol:
//! 1. Pre-commit: Buffer records during a checkpoint interval
//! 2. Commit: On checkpoint completion, commit the transaction
//! 3. Abort: On failure, roll back uncommitted transactions

use anyhow::Result;
use async_trait::async_trait;
use bicycle_core::{CheckpointBarrier, StreamMessage, Timestamp};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

/// Transaction identifier tied to a checkpoint.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TransactionId {
    pub checkpoint_id: u64,
    pub sink_id: String,
}

impl TransactionId {
    pub fn new(checkpoint_id: u64, sink_id: impl Into<String>) -> Self {
        Self {
            checkpoint_id,
            sink_id: sink_id.into(),
        }
    }
}

/// State of a transaction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransactionState {
    /// Transaction is open and accepting writes.
    Open,
    /// Transaction is pre-committed (checkpoint barrier received).
    PreCommitted,
    /// Transaction is fully committed.
    Committed,
    /// Transaction was aborted.
    Aborted,
}

/// A transaction for exactly-once writes.
#[derive(Debug)]
pub struct Transaction<T> {
    pub id: TransactionId,
    pub state: TransactionState,
    pub records: Vec<T>,
    pub created_at: Timestamp,
}

impl<T> Transaction<T> {
    pub fn new(id: TransactionId) -> Self {
        Self {
            id,
            state: TransactionState::Open,
            records: Vec::new(),
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
        }
    }

    pub fn add_record(&mut self, record: T) {
        self.records.push(record);
    }

    pub fn pre_commit(&mut self) {
        self.state = TransactionState::PreCommitted;
    }

    pub fn commit(&mut self) {
        self.state = TransactionState::Committed;
    }

    pub fn abort(&mut self) {
        self.state = TransactionState::Aborted;
    }
}

/// Trait for sinks that support exactly-once semantics.
#[async_trait]
pub trait TransactionalSink: Send + Sync {
    /// The record type this sink handles.
    type Record: Send + 'static;

    /// Begin a new transaction.
    async fn begin_transaction(&self, id: TransactionId) -> Result<()>;

    /// Write a record within the current transaction.
    async fn write(&self, record: Self::Record) -> Result<()>;

    /// Pre-commit the current transaction (flush to external system).
    async fn pre_commit(&self, id: &TransactionId) -> Result<()>;

    /// Commit a pre-committed transaction.
    async fn commit(&self, id: &TransactionId) -> Result<()>;

    /// Abort/rollback a transaction.
    async fn abort(&self, id: &TransactionId) -> Result<()>;

    /// Recover and commit any pending transactions from a previous run.
    async fn recover(&self, pending_transactions: Vec<TransactionId>) -> Result<()>;

    /// Get the sink identifier.
    fn sink_id(&self) -> &str;
}

/// Wrapper that manages transactions for a sink.
pub struct ExactlyOnceSinkWriter<S: TransactionalSink> {
    sink: Arc<S>,
    current_transaction: RwLock<Option<TransactionId>>,
    pending_commits: RwLock<Vec<TransactionId>>,
    commit_notifier: mpsc::Sender<TransactionId>,
}

impl<S: TransactionalSink> ExactlyOnceSinkWriter<S> {
    pub fn new(sink: Arc<S>, commit_notifier: mpsc::Sender<TransactionId>) -> Self {
        Self {
            sink,
            current_transaction: RwLock::new(None),
            pending_commits: RwLock::new(Vec::new()),
            commit_notifier,
        }
    }

    /// Process a stream message.
    pub async fn process(&self, msg: StreamMessage<S::Record>) -> Result<()> {
        match msg {
            StreamMessage::Data(record) => {
                // Ensure we have an active transaction
                if self.current_transaction.read().is_none() {
                    // Start a new transaction with checkpoint 0 (will be updated on barrier)
                    let id = TransactionId::new(0, self.sink.sink_id());
                    self.sink.begin_transaction(id.clone()).await?;
                    *self.current_transaction.write() = Some(id);
                }

                // Write record
                self.sink.write(record).await?;
            }

            StreamMessage::Barrier(barrier) => {
                self.handle_barrier(barrier).await?;
            }

            StreamMessage::Watermark(_) => {
                // Watermarks don't affect transactions
            }

            StreamMessage::LatencyMarker(_) => {
                // Latency markers don't affect transactions
            }

            StreamMessage::End => {
                // Commit any pending transaction on stream end
                if let Some(id) = self.current_transaction.read().clone() {
                    self.sink.pre_commit(&id).await?;
                    self.sink.commit(&id).await?;
                    *self.current_transaction.write() = None;
                }
            }
        }

        Ok(())
    }

    /// Handle a checkpoint barrier.
    async fn handle_barrier(&self, barrier: CheckpointBarrier) -> Result<()> {
        debug!(
            checkpoint_id = barrier.checkpoint_id,
            "Sink received checkpoint barrier"
        );

        // Pre-commit current transaction
        if let Some(id) = self.current_transaction.write().take() {
            self.sink.pre_commit(&id).await?;
            self.pending_commits.write().push(id);
        }

        // Start new transaction for the next checkpoint interval
        let new_id = TransactionId::new(barrier.checkpoint_id, self.sink.sink_id());
        self.sink.begin_transaction(new_id.clone()).await?;
        *self.current_transaction.write() = Some(new_id);

        Ok(())
    }

    /// Notify that a checkpoint completed (called by checkpoint coordinator).
    pub async fn notify_checkpoint_complete(&self, checkpoint_id: u64) -> Result<()> {
        let mut pending = self.pending_commits.write();
        let mut to_commit = Vec::new();

        // Find all transactions up to and including this checkpoint
        pending.retain(|id| {
            if id.checkpoint_id <= checkpoint_id {
                to_commit.push(id.clone());
                false
            } else {
                true
            }
        });

        drop(pending);

        // Commit transactions
        for id in to_commit {
            info!(
                checkpoint_id = id.checkpoint_id,
                sink_id = %id.sink_id,
                "Committing transaction"
            );
            self.sink.commit(&id).await?;
            let _ = self.commit_notifier.send(id).await;
        }

        Ok(())
    }

    /// Handle checkpoint failure - abort pending transactions.
    pub async fn notify_checkpoint_failed(&self, checkpoint_id: u64) -> Result<()> {
        let mut pending = self.pending_commits.write();
        let mut to_abort = Vec::new();

        // Find transactions for this checkpoint
        pending.retain(|id| {
            if id.checkpoint_id == checkpoint_id {
                to_abort.push(id.clone());
                false
            } else {
                true
            }
        });

        drop(pending);

        // Abort transactions
        for id in to_abort {
            warn!(
                checkpoint_id = id.checkpoint_id,
                sink_id = %id.sink_id,
                "Aborting transaction due to checkpoint failure"
            );
            self.sink.abort(&id).await?;
        }

        Ok(())
    }

    /// Recover from a restart.
    pub async fn recover(&self, pending_transactions: Vec<TransactionId>) -> Result<()> {
        self.sink.recover(pending_transactions).await
    }
}

/// A simple in-memory transactional sink for testing.
pub struct InMemoryTransactionalSink<T: Clone + Send + 'static> {
    sink_id: String,
    transactions: RwLock<HashMap<TransactionId, Transaction<T>>>,
    committed_records: RwLock<Vec<T>>,
    current_transaction_id: RwLock<Option<TransactionId>>,
}

impl<T: Clone + Send + 'static> InMemoryTransactionalSink<T> {
    pub fn new(sink_id: impl Into<String>) -> Self {
        Self {
            sink_id: sink_id.into(),
            transactions: RwLock::new(HashMap::new()),
            committed_records: RwLock::new(Vec::new()),
            current_transaction_id: RwLock::new(None),
        }
    }

    /// Get all committed records.
    pub fn get_committed_records(&self) -> Vec<T> {
        self.committed_records.read().clone()
    }

    /// Get the number of committed records.
    pub fn committed_count(&self) -> usize {
        self.committed_records.read().len()
    }
}

#[async_trait]
impl<T: Clone + Send + Sync + 'static> TransactionalSink for InMemoryTransactionalSink<T> {
    type Record = T;

    async fn begin_transaction(&self, id: TransactionId) -> Result<()> {
        let transaction = Transaction::new(id.clone());
        self.transactions.write().insert(id.clone(), transaction);
        *self.current_transaction_id.write() = Some(id);
        Ok(())
    }

    async fn write(&self, record: Self::Record) -> Result<()> {
        let current_id = self
            .current_transaction_id
            .read()
            .clone()
            .ok_or_else(|| anyhow::anyhow!("No active transaction"))?;

        let mut transactions = self.transactions.write();
        let transaction = transactions
            .get_mut(&current_id)
            .ok_or_else(|| anyhow::anyhow!("Transaction not found"))?;

        transaction.add_record(record);
        Ok(())
    }

    async fn pre_commit(&self, id: &TransactionId) -> Result<()> {
        let mut transactions = self.transactions.write();
        let transaction = transactions
            .get_mut(id)
            .ok_or_else(|| anyhow::anyhow!("Transaction not found"))?;

        transaction.pre_commit();
        Ok(())
    }

    async fn commit(&self, id: &TransactionId) -> Result<()> {
        let mut transactions = self.transactions.write();
        let transaction = transactions
            .get_mut(id)
            .ok_or_else(|| anyhow::anyhow!("Transaction not found"))?;

        // Move records to committed
        let records = std::mem::take(&mut transaction.records);
        self.committed_records.write().extend(records);

        transaction.commit();
        Ok(())
    }

    async fn abort(&self, id: &TransactionId) -> Result<()> {
        let mut transactions = self.transactions.write();
        if let Some(transaction) = transactions.get_mut(id) {
            transaction.abort();
            transaction.records.clear();
        }
        Ok(())
    }

    async fn recover(&self, pending_transactions: Vec<TransactionId>) -> Result<()> {
        // For in-memory sink, just commit any pending transactions
        for id in pending_transactions {
            self.commit(&id).await?;
        }
        Ok(())
    }

    fn sink_id(&self) -> &str {
        &self.sink_id
    }
}

/// Idempotent write tracker for deduplication.
#[derive(Debug, Default)]
pub struct IdempotentWriteTracker {
    /// Set of processed record IDs per checkpoint.
    processed: RwLock<HashMap<u64, std::collections::HashSet<String>>>,
    /// Maximum checkpoint to retain.
    max_retained_checkpoints: usize,
}

impl IdempotentWriteTracker {
    pub fn new(max_retained: usize) -> Self {
        Self {
            processed: RwLock::new(HashMap::new()),
            max_retained_checkpoints: max_retained,
        }
    }

    /// Check if a record was already processed.
    pub fn is_duplicate(&self, checkpoint_id: u64, record_id: &str) -> bool {
        self.processed
            .read()
            .get(&checkpoint_id)
            .map(|set| set.contains(record_id))
            .unwrap_or(false)
    }

    /// Mark a record as processed.
    pub fn mark_processed(&self, checkpoint_id: u64, record_id: String) {
        let mut processed = self.processed.write();
        processed
            .entry(checkpoint_id)
            .or_insert_with(std::collections::HashSet::new)
            .insert(record_id);

        // Cleanup old checkpoints
        self.cleanup_old_checkpoints(&mut processed);
    }

    /// Cleanup checkpoints older than the retention limit.
    fn cleanup_old_checkpoints(
        &self,
        processed: &mut HashMap<u64, std::collections::HashSet<String>>,
    ) {
        if processed.len() > self.max_retained_checkpoints {
            let min_checkpoint = processed
                .keys()
                .copied()
                .max()
                .map(|max| max.saturating_sub(self.max_retained_checkpoints as u64))
                .unwrap_or(0);

            processed.retain(|&id, _| id >= min_checkpoint);
        }
    }

    /// Get state for checkpointing.
    pub fn snapshot(&self) -> Vec<u8> {
        let processed = self.processed.read();
        let data: HashMap<u64, Vec<String>> = processed
            .iter()
            .map(|(k, v)| (*k, v.iter().cloned().collect()))
            .collect();
        bincode::serialize(&data).unwrap_or_default()
    }

    /// Restore state from checkpoint.
    pub fn restore(&self, state: &[u8]) -> Result<()> {
        if state.is_empty() {
            return Ok(());
        }

        let data: HashMap<u64, Vec<String>> = bincode::deserialize(state)?;
        let mut processed = self.processed.write();
        *processed = data
            .into_iter()
            .map(|(k, v)| (k, v.into_iter().collect()))
            .collect();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_in_memory_transactional_sink() {
        let sink = InMemoryTransactionalSink::<i32>::new("test-sink");

        // Begin transaction
        let id = TransactionId::new(1, "test-sink");
        sink.begin_transaction(id.clone()).await.unwrap();

        // Write some records
        sink.write(1).await.unwrap();
        sink.write(2).await.unwrap();
        sink.write(3).await.unwrap();

        // Pre-commit
        sink.pre_commit(&id).await.unwrap();

        // Records not yet visible
        assert_eq!(sink.committed_count(), 0);

        // Commit
        sink.commit(&id).await.unwrap();

        // Records now visible
        assert_eq!(sink.committed_count(), 3);
        assert_eq!(sink.get_committed_records(), vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn test_transaction_abort() {
        let sink = InMemoryTransactionalSink::<i32>::new("test-sink");

        let id = TransactionId::new(1, "test-sink");
        sink.begin_transaction(id.clone()).await.unwrap();

        sink.write(1).await.unwrap();
        sink.write(2).await.unwrap();

        // Abort instead of commit
        sink.abort(&id).await.unwrap();

        // No records committed
        assert_eq!(sink.committed_count(), 0);
    }

    #[test]
    fn test_idempotent_tracker() {
        let tracker = IdempotentWriteTracker::new(3);

        // Mark some records as processed
        tracker.mark_processed(1, "record-1".to_string());
        tracker.mark_processed(1, "record-2".to_string());

        // Check duplicates
        assert!(tracker.is_duplicate(1, "record-1"));
        assert!(tracker.is_duplicate(1, "record-2"));
        assert!(!tracker.is_duplicate(1, "record-3"));
        assert!(!tracker.is_duplicate(2, "record-1"));
    }
}
