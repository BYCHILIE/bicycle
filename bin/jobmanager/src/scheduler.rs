//! Task scheduler for distributing tasks across workers.

use crate::state::{JobManagerState, PhysicalTask, TaskInfo};
use bicycle_protocol::control::{
    DeployTaskRequest, InputGate, OperatorType, OutputGate, PartitionStrategy, TaskDescriptor,
    TaskState,
};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, error, info, warn};

/// Result of scheduling a job's tasks.
#[derive(Debug)]
pub struct ScheduleResult {
    /// Successfully scheduled tasks: (task_id, worker_id)
    pub scheduled: Vec<(String, String)>,
    /// Tasks that couldn't be scheduled (insufficient resources)
    pub pending: Vec<String>,
    /// Tasks that failed to schedule (error)
    pub failed: Vec<(String, String)>,
}

/// Scheduler for assigning tasks to workers.
pub struct Scheduler {
    state: Arc<JobManagerState>,
}

impl Scheduler {
    pub fn new(state: Arc<JobManagerState>) -> Self {
        Self { state }
    }

    /// Schedule all tasks for a job.
    pub fn schedule_job(&self, job_id: &str, tasks: &[PhysicalTask]) -> ScheduleResult {
        let mut result = ScheduleResult {
            scheduled: Vec::new(),
            pending: Vec::new(),
            failed: Vec::new(),
        };

        // First, check total available slots
        let available = self.state.total_available_slots();
        let required = tasks.len() as i32;

        if available < required {
            warn!(
                job_id,
                available, required, "Insufficient slots to schedule all tasks"
            );
        }

        // Build task location map for generating input/output gates
        let mut task_locations: HashMap<String, String> = HashMap::new();

        // Schedule source tasks first, then process in topological order
        let mut source_tasks: Vec<&PhysicalTask> = Vec::new();
        let mut other_tasks: Vec<&PhysicalTask> = Vec::new();

        for task in tasks {
            if task.operator_type == OperatorType::Source || task.upstream_tasks.is_empty() {
                source_tasks.push(task);
            } else {
                other_tasks.push(task);
            }
        }

        // Schedule sources first
        for task in source_tasks {
            match self.schedule_task(task, &task_locations) {
                Ok(worker_id) => {
                    task_locations.insert(task.task_id.clone(), worker_id.clone());
                    result.scheduled.push((task.task_id.clone(), worker_id));
                }
                Err(e) => {
                    if e.contains("No available") {
                        result.pending.push(task.task_id.clone());
                    } else {
                        result.failed.push((task.task_id.clone(), e));
                    }
                }
            }
        }

        // Schedule remaining tasks
        for task in other_tasks {
            match self.schedule_task(task, &task_locations) {
                Ok(worker_id) => {
                    task_locations.insert(task.task_id.clone(), worker_id.clone());
                    result.scheduled.push((task.task_id.clone(), worker_id));
                }
                Err(e) => {
                    if e.contains("No available") {
                        result.pending.push(task.task_id.clone());
                    } else {
                        result.failed.push((task.task_id.clone(), e));
                    }
                }
            }
        }

        info!(
            job_id,
            scheduled = result.scheduled.len(),
            pending = result.pending.len(),
            failed = result.failed.len(),
            "Job scheduling complete"
        );

        result
    }

    /// Schedule a single task.
    fn schedule_task(
        &self,
        task: &PhysicalTask,
        task_locations: &HashMap<String, String>,
    ) -> Result<String, String> {
        // Find best worker for this task
        // Prefer locality with upstream tasks when using Forward partition
        let preferred_worker = self.find_preferred_worker(task, task_locations);

        let worker_id = if let Some(worker) = preferred_worker {
            worker
        } else {
            self.state
                .find_worker_with_slots()
                .ok_or_else(|| "No available worker with slots".to_string())?
        };

        // Allocate slot
        if !self.state.allocate_slot(&worker_id, &task.task_id) {
            return Err("Failed to allocate slot".to_string());
        }

        // Create task info and store
        let task_info = TaskInfo::new(
            task.task_id.clone(),
            task.job_id.clone(),
            task.vertex_id.clone(),
            task.subtask_index,
            task.operator_type,
            worker_id.clone(),
        );

        self.state.tasks.insert(task.task_id.clone(), task_info);

        debug!(
            task_id = %task.task_id,
            worker_id = %worker_id,
            "Scheduled task"
        );

        Ok(worker_id)
    }

    /// Find a preferred worker based on data locality.
    fn find_preferred_worker(
        &self,
        task: &PhysicalTask,
        task_locations: &HashMap<String, String>,
    ) -> Option<String> {
        // For Forward partitioning, prefer the same worker as upstream
        for (upstream_id, strategy) in &task.upstream_tasks {
            if *strategy == PartitionStrategy::PartitionForward {
                if let Some(upstream_worker) = task_locations.get(upstream_id) {
                    if let Some(worker) = self.state.workers.get(upstream_worker) {
                        if worker.available_slots() > 0 {
                            return Some(upstream_worker.clone());
                        }
                    }
                }
            }
        }

        None
    }

    /// Create a deploy task request for a scheduled task.
    pub fn create_deploy_request(
        &self,
        task: &PhysicalTask,
        task_locations: &HashMap<String, String>,
    ) -> Option<DeployTaskRequest> {
        // Build input gates
        let input_gates: Vec<InputGate> = task
            .upstream_tasks
            .iter()
            .filter_map(|(upstream_id, strategy)| {
                let upstream_worker = task_locations.get(upstream_id)?;
                let worker = self.state.workers.get(upstream_worker)?;

                Some(InputGate {
                    upstream_task_id: upstream_id.clone(),
                    upstream_worker_host: worker.hostname.clone(),
                    upstream_worker_port: worker.data_port,
                    partition_strategy: *strategy as i32,
                })
            })
            .collect();

        // Build output gates
        let output_gates: Vec<OutputGate> = task
            .downstream_tasks
            .iter()
            .filter_map(|(downstream_id, strategy)| {
                let downstream_worker = task_locations.get(downstream_id)?;
                let worker = self.state.workers.get(downstream_worker)?;

                Some(OutputGate {
                    downstream_task_id: downstream_id.clone(),
                    downstream_worker_host: worker.hostname.clone(),
                    downstream_worker_port: worker.data_port,
                    partition_strategy: *strategy as i32,
                })
            })
            .collect();

        let descriptor = TaskDescriptor {
            operator_name: task.operator_name.clone(),
            operator_type: task.operator_type as i32,
            operator_config: task.operator_config.clone(),
            parallelism: task.parallelism,
            subtask_index: task.subtask_index,
            input_gates,
            output_gates,
        };

        Some(DeployTaskRequest {
            job_id: task.job_id.clone(),
            task_id: task.task_id.clone(),
            task: Some(descriptor),
        })
    }

    /// Handle task failure - potentially reschedule.
    pub fn handle_task_failure(&self, task_id: &str) -> Option<String> {
        if let Some(mut task) = self.state.tasks.get_mut(task_id) {
            // Release the slot
            self.state.release_slot(&task.worker_id, task_id);

            // Try to find a new worker
            if let Some(new_worker) = self.state.find_worker_with_slots() {
                if self.state.allocate_slot(&new_worker, task_id) {
                    task.worker_id = new_worker.clone();
                    task.state = TaskState::Created;
                    task.error_message = None;
                    return Some(new_worker);
                }
            }
        }
        None
    }

    /// Cancel all tasks for a job.
    pub fn cancel_job_tasks(&self, job_id: &str) -> Vec<String> {
        let mut cancelled = Vec::new();

        for mut entry in self.state.tasks.iter_mut() {
            if entry.job_id == job_id {
                let task_id = entry.task_id.clone();
                entry.state = TaskState::Canceling;
                self.state.release_slot(&entry.worker_id, &task_id);
                cancelled.push(task_id);
            }
        }

        cancelled
    }
}
