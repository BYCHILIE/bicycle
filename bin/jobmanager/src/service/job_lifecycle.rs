//! Job lifecycle helpers extracted from the main gRPC service.
//!
//! Contains task deployment logic and background tasks for checkpoint
//! triggering and stats collection that are spawned during job submission.

use crate::scheduler::Scheduler;
use crate::state::{JobManagerState, PhysicalTask};
use bicycle_checkpoint::CheckpointCoordinator;
use bicycle_protocol::control::{
    worker_control_client::WorkerControlClient, OperatorType, TaskState,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info, warn};

use super::ControlPlaneService;

impl ControlPlaneService {
    /// Deploy tasks to workers.
    pub(super) async fn deploy_tasks_to_workers(
        &self,
        job_id: &str,
        physical_tasks: &[PhysicalTask],
        task_locations: &HashMap<String, String>,
    ) -> Result<Vec<String>, String> {
        let mut deployed = Vec::new();
        let mut errors = Vec::new();

        // Group tasks by worker
        let mut tasks_by_worker: HashMap<String, Vec<&PhysicalTask>> = HashMap::new();
        for task in physical_tasks {
            if let Some(worker_id) = task_locations.get(&task.task_id) {
                tasks_by_worker
                    .entry(worker_id.clone())
                    .or_default()
                    .push(task);
            }
        }

        // Deploy to each worker
        for (worker_id, tasks) in tasks_by_worker {
            // Get worker address
            let worker_addr = if let Some(worker) = self.state.workers.get(&worker_id) {
                format!("http://{}:{}", worker.hostname, worker.data_port)
            } else {
                errors.push(format!("Worker {} not found", worker_id));
                continue;
            };

            // Connect to worker
            let mut client = match WorkerControlClient::connect(worker_addr.clone()).await {
                Ok(client) => client,
                Err(e) => {
                    errors.push(format!(
                        "Failed to connect to worker {}: {}",
                        worker_id, e
                    ));
                    continue;
                }
            };

            // Deploy each task
            for task in tasks {
                let request = self.scheduler.create_deploy_request(task, task_locations);

                if let Some(req) = request {
                    match client.deploy_task(req).await {
                        Ok(response) => {
                            let resp = response.into_inner();
                            if resp.success {
                                deployed.push(task.task_id.clone());
                                // Update task state
                                if let Some(mut task_info) =
                                    self.state.tasks.get_mut(&task.task_id)
                                {
                                    task_info.state = TaskState::Deploying;
                                }
                                info!(
                                    task_id = %task.task_id,
                                    worker_id = %worker_id,
                                    "Task deployed successfully"
                                );
                            } else {
                                errors.push(format!(
                                    "Worker rejected task {}: {}",
                                    task.task_id, resp.message
                                ));
                            }
                        }
                        Err(e) => {
                            errors.push(format!(
                                "Failed to deploy task {}: {}",
                                task.task_id, e
                            ));
                        }
                    }
                }
            }
        }

        if errors.is_empty() {
            Ok(deployed)
        } else {
            Err(errors.join("; "))
        }
    }
}

/// Spawn the checkpoint trigger bridge task.
///
/// Subscribes to the coordinator's trigger channel and forwards checkpoint
/// barriers to all source and sink workers for the given job.
pub(crate) fn spawn_checkpoint_trigger_bridge(
    state: Arc<JobManagerState>,
    coordinator: Arc<CheckpointCoordinator>,
    job_id: String,
    task_locations: HashMap<String, String>,
) {
    let mut trigger_rx = coordinator.subscribe_triggers();

    tokio::spawn(async move {
        loop {
            match trigger_rx.recv().await {
                Ok(barrier) => {
                    // Send checkpoint triggers to all source and sink tasks
                    for (task_id, worker_id) in &task_locations {
                        let should_trigger = state
                            .tasks
                            .get(task_id)
                            .map(|t| {
                                t.operator_type == OperatorType::Source
                                    || t.operator_type == OperatorType::Sink
                            })
                            .unwrap_or(false);

                        if !should_trigger {
                            continue;
                        }

                        // Get worker address
                        let worker_addr = if let Some(worker) = state.workers.get(worker_id) {
                            format!("http://{}:{}", worker.hostname, worker.data_port)
                        } else {
                            warn!(worker_id = %worker_id, "Worker not found for checkpoint trigger");
                            continue;
                        };

                        // Send trigger to worker
                        match WorkerControlClient::connect(worker_addr.clone()).await {
                            Ok(mut client) => {
                                let req = bicycle_protocol::control::TriggerTaskCheckpointRequest {
                                    task_id: task_id.clone(),
                                    checkpoint_id: barrier.checkpoint_id as i64,
                                    timestamp: barrier.timestamp as i64,
                                    checkpoint_type: 0, // Regular checkpoint
                                };
                                if let Err(e) = client.trigger_task_checkpoint(req).await {
                                    warn!(
                                        task_id = %task_id,
                                        error = %e,
                                        "Failed to trigger checkpoint on worker"
                                    );
                                }
                            }
                            Err(e) => {
                                warn!(
                                    worker_addr = %worker_addr,
                                    error = %e,
                                    "Failed to connect to worker for checkpoint trigger"
                                );
                            }
                        }
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    warn!(skipped = n, "Checkpoint trigger receiver lagged");
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    info!(job_id = %job_id, "Checkpoint trigger channel closed");
                    break;
                }
            }
        }
    });
}

/// Spawn the checkpoint stats updater task.
///
/// Periodically reads coordinator stats and updates the job manager state
/// with completed/failed checkpoint counts and last checkpoint timestamps.
pub(crate) fn spawn_checkpoint_stats_updater(
    state: Arc<JobManagerState>,
    coordinator: Arc<CheckpointCoordinator>,
    job_id: String,
) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(10));
        loop {
            interval.tick().await;
            let stats = coordinator.stats();
            state.metrics.checkpoints_completed.store(
                stats.completed_checkpoints as i64,
                std::sync::atomic::Ordering::Relaxed,
            );
            state.metrics.checkpoints_failed.store(
                stats.failed_checkpoints as i64,
                std::sync::atomic::Ordering::Relaxed,
            );

            // Update job-level checkpoint info
            if let Some(mut job) = state.jobs.get_mut(&job_id) {
                if let Some(last_time) = stats.last_checkpoint_time {
                    job.last_checkpoint_time = last_time as i64;
                }
                job.last_checkpoint_id = stats.completed_checkpoints as i64;
            }
        }
    });
}
