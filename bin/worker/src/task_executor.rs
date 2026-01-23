//! Task execution engine for running streaming tasks.

use anyhow::Result;
use bicycle_core::StreamMessage;
use bicycle_protocol::control::{OperatorType, TaskDescriptor, TaskState};
use dashmap::DashMap;
use parking_lot::RwLock;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

/// Status information for a running task.
#[derive(Debug)]
pub struct TaskStatus {
    pub state: RwLock<TaskState>,
    pub records_processed: AtomicI64,
    pub bytes_processed: AtomicI64,
    pub error_message: RwLock<Option<String>>,
    pub started_at: Instant,
    pub cancel_tx: Option<mpsc::Sender<()>>,
}

impl TaskStatus {
    pub fn new() -> Self {
        Self {
            state: RwLock::new(TaskState::Created),
            records_processed: AtomicI64::new(0),
            bytes_processed: AtomicI64::new(0),
            error_message: RwLock::new(None),
            started_at: Instant::now(),
            cancel_tx: None,
        }
    }

    pub fn get_state(&self) -> TaskState {
        *self.state.read()
    }

    pub fn set_state(&self, state: TaskState) {
        *self.state.write() = state;
    }

    pub fn set_error(&self, msg: String) {
        *self.error_message.write() = Some(msg);
        self.set_state(TaskState::Failed);
    }

    pub fn increment_records(&self, count: i64) {
        self.records_processed.fetch_add(count, Ordering::Relaxed);
    }

    pub fn increment_bytes(&self, count: i64) {
        self.bytes_processed.fetch_add(count, Ordering::Relaxed);
    }
}

impl Default for TaskStatus {
    fn default() -> Self {
        Self::new()
    }
}

/// Executor for running streaming tasks.
pub struct TaskExecutor {
    /// Running tasks by task ID
    tasks: DashMap<String, Arc<TaskStatus>>,

    /// Worker ID
    worker_id: String,

    /// State directory
    state_dir: String,

    /// Available task slots
    total_slots: i32,
    used_slots: AtomicU64,
}

impl TaskExecutor {
    pub fn new(worker_id: String, state_dir: String, slots: i32) -> Self {
        Self {
            tasks: DashMap::new(),
            worker_id,
            state_dir,
            total_slots: slots,
            used_slots: AtomicU64::new(0),
        }
    }

    /// Get available slots.
    pub fn available_slots(&self) -> i32 {
        self.total_slots - self.used_slots.load(Ordering::Relaxed) as i32
    }

    /// Deploy and start a task.
    pub async fn deploy_task(
        &self,
        job_id: &str,
        task_id: &str,
        descriptor: TaskDescriptor,
    ) -> Result<()> {
        // Check if we have slots
        if self.available_slots() <= 0 {
            anyhow::bail!("No available slots");
        }

        // Check if task already exists
        if self.tasks.contains_key(task_id) {
            anyhow::bail!("Task {} already exists", task_id);
        }

        info!(
            task_id = %task_id,
            operator = %descriptor.operator_name,
            operator_type = ?OperatorType::try_from(descriptor.operator_type),
            "Deploying task"
        );

        // Create task status
        let status = Arc::new(TaskStatus::new());
        status.set_state(TaskState::Deploying);

        // Reserve slot
        self.used_slots.fetch_add(1, Ordering::Relaxed);
        self.tasks.insert(task_id.to_string(), status.clone());

        // Start task execution
        let task_id_clone = task_id.to_string();
        let status_clone = status.clone();

        tokio::spawn(async move {
            status_clone.set_state(TaskState::Running);

            // Execute the task (simplified - would connect to network layer)
            match Self::run_task(&task_id_clone, descriptor).await {
                Ok(()) => {
                    status_clone.set_state(TaskState::Finished);
                    info!(task_id = %task_id_clone, "Task finished successfully");
                }
                Err(e) => {
                    status_clone.set_error(e.to_string());
                    error!(task_id = %task_id_clone, error = %e, "Task failed");
                }
            }
        });

        Ok(())
    }

    /// Run a task (simplified execution loop).
    async fn run_task(task_id: &str, descriptor: TaskDescriptor) -> Result<()> {
        debug!(task_id = %task_id, "Task execution started");

        // In a real implementation, this would:
        // 1. Create network connections to upstream/downstream tasks
        // 2. Initialize the operator
        // 3. Process messages in a loop
        // 4. Handle checkpoints
        // 5. Handle cancellation

        // For now, simulate a running task
        let operator_type =
            OperatorType::try_from(descriptor.operator_type).unwrap_or(OperatorType::Unknown);

        match operator_type {
            OperatorType::Source => {
                // Source would generate data
                debug!(task_id = %task_id, "Source task - would generate data");
            }
            OperatorType::Sink => {
                // Sink would consume data
                debug!(task_id = %task_id, "Sink task - would consume data");
            }
            _ => {
                // Processing operators would transform data
                debug!(task_id = %task_id, "Processing task - would transform data");
            }
        }

        // Keep running until cancelled
        // In real impl, this would be the message processing loop
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            // Check for cancellation, barriers, etc.
        }
    }

    /// Cancel a task.
    pub async fn cancel_task(&self, task_id: &str) -> Result<()> {
        if let Some(status) = self.tasks.get(task_id) {
            status.set_state(TaskState::Canceling);

            // In real impl, would signal task to stop
            // For now, just mark as cancelled
            status.set_state(TaskState::Canceled);

            info!(task_id = %task_id, "Task cancelled");
        } else {
            anyhow::bail!("Task {} not found", task_id);
        }

        Ok(())
    }

    /// Get status of all tasks.
    pub fn get_all_task_statuses(&self) -> Vec<(String, TaskState, i64, i64, Option<String>)> {
        self.tasks
            .iter()
            .map(|entry| {
                let status = entry.value();
                (
                    entry.key().clone(),
                    status.get_state(),
                    status.records_processed.load(Ordering::Relaxed),
                    status.bytes_processed.load(Ordering::Relaxed),
                    status.error_message.read().clone(),
                )
            })
            .collect()
    }

    /// Get status of a specific task.
    pub fn get_task_status(&self, task_id: &str) -> Option<Arc<TaskStatus>> {
        self.tasks.get(task_id).map(|e| e.value().clone())
    }

    /// Remove completed/failed tasks.
    pub fn cleanup_finished_tasks(&self) {
        let to_remove: Vec<String> = self
            .tasks
            .iter()
            .filter(|entry| {
                let state = entry.value().get_state();
                matches!(
                    state,
                    TaskState::Finished | TaskState::Failed | TaskState::Canceled
                )
            })
            .map(|entry| entry.key().clone())
            .collect();

        for task_id in to_remove {
            if self.tasks.remove(&task_id).is_some() {
                self.used_slots.fetch_sub(1, Ordering::Relaxed);
                debug!(task_id = %task_id, "Cleaned up finished task");
            }
        }
    }
}
