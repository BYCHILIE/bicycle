//! gRPC service implementation for the JobManager.

mod job_lifecycle;
use job_lifecycle::{spawn_checkpoint_stats_updater, spawn_checkpoint_trigger_bridge};

use crate::scheduler::Scheduler;
use crate::state::{JobInfo, JobManagerState, WorkerInfo};
use bicycle_checkpoint::{CheckpointConfig, CheckpointCoordinator};
use bicycle_core::JobId;
use bicycle_protocol::control::{
    control_plane_server::ControlPlane,
    AckCheckpointRequest, AckCheckpointResponse, CancelJobRequest, CancelJobResponse,
    CancelTaskRequest, CancelTaskResponse, DeployTaskRequest, DeployTaskResponse, OperatorType,
    GetClusterInfoRequest, GetClusterInfoResponse, GetJobStatusRequest, GetJobStatusResponse,
    GetMetricsRequest, GetMetricsResponse, HeartbeatRequest, HeartbeatResponse, JobMetrics,
    JobState, JobSummary, ListJobsRequest, ListJobsResponse, ListWorkersRequest,
    ListWorkersResponse, RegisterWorkerRequest, RegisterWorkerResponse, SubmitJobRequest,
    SubmitJobResponse, TaskCommand, TaskCommandType, TaskState, TaskStatus,
    TriggerCheckpointRequest, TriggerCheckpointResponse, WorkerResources,
    WorkerState, WorkerSummary,
};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tonic::{Request, Response, Status};
use tracing::{debug, error, info, warn};

/// gRPC service implementation for the control plane.
pub struct ControlPlaneService {
    state: Arc<JobManagerState>,
    scheduler: Arc<Scheduler>,
    checkpoint_coordinators: RwLock<HashMap<String, Arc<CheckpointCoordinator>>>,
    checkpoint_dir: PathBuf,
}

impl ControlPlaneService {
    pub fn new(state: Arc<JobManagerState>, checkpoint_dir: PathBuf) -> Self {
        let scheduler = Arc::new(Scheduler::new(state.clone()));

        Self {
            state,
            scheduler,
            checkpoint_coordinators: RwLock::new(HashMap::new()),
            checkpoint_dir,
        }
    }

    /// Create a checkpoint coordinator for a job.
    fn create_checkpoint_coordinator(&self, job_id: &str, interval_ms: i64) -> Arc<CheckpointCoordinator> {
        let config = CheckpointConfig {
            interval: Duration::from_millis(interval_ms as u64),
            timeout: Duration::from_secs(600),
            num_retained: 3,
            ..Default::default()
        };

        let job_checkpoint_dir = self.checkpoint_dir.join(job_id);
        let coordinator = Arc::new(CheckpointCoordinator::new(
            JobId::from_string(job_id),
            config,
            job_checkpoint_dir,
        ));

        self.checkpoint_coordinators
            .write()
            .insert(job_id.to_string(), coordinator.clone());

        coordinator
    }

    /// Get task commands for workers (cancellation, checkpoint triggers).
    fn get_task_commands(&self, worker_id: &str) -> Vec<TaskCommand> {
        let mut commands = Vec::new();

        // Check for tasks that need to be cancelled
        for entry in self.state.tasks.iter() {
            if entry.worker_id == worker_id && entry.state == TaskState::Canceling {
                commands.push(TaskCommand {
                    task_id: entry.task_id.clone(),
                    command: TaskCommandType::TaskCommandCancel as i32,
                });
            }
        }

        commands
    }

}


#[tonic::async_trait]
impl ControlPlane for ControlPlaneService {
    /// Register a new worker.
    async fn register_worker(
        &self,
        request: Request<RegisterWorkerRequest>,
    ) -> Result<Response<RegisterWorkerResponse>, Status> {
        let req = request.into_inner();

        info!(
            worker_id = %req.worker_id,
            hostname = %req.hostname,
            slots = req.available_slots,
            "Worker registration request"
        );

        let resources = req.resources.unwrap_or(WorkerResources {
            memory_mb: 2048,
            cpu_cores: 4,
        });

        let worker = WorkerInfo::new(
            req.worker_id.clone(),
            req.hostname,
            req.data_port,
            req.available_slots,
            resources,
        );

        self.state.register_worker(worker);

        info!(worker_id = %req.worker_id, "Worker registered successfully");

        Ok(Response::new(RegisterWorkerResponse {
            success: true,
            message: "Registered".to_string(),
            heartbeat_interval_ms: self.state.heartbeat_interval.as_millis() as i64,
        }))
    }

    /// Handle worker heartbeat.
    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let req = request.into_inner();

        // Update worker heartbeat
        if let Some(mut worker) = self.state.workers.get_mut(&req.worker_id) {
            worker.update_heartbeat(req.metrics);

            // Update task statuses
            for status in req.task_statuses {
                if let Some(mut task) = self.state.tasks.get_mut(&status.task_id) {
                    task.state = TaskState::try_from(status.state).unwrap_or(TaskState::Unknown);
                    task.records_processed = status.records_processed;
                    task.bytes_processed = status.bytes_processed;
                    if !status.error_message.is_empty() {
                        task.error_message = Some(status.error_message);
                    }
                }
            }
        } else {
            warn!(worker_id = %req.worker_id, "Heartbeat from unknown worker");
            return Err(Status::not_found("Worker not registered"));
        }

        // Get any commands for this worker
        let commands = self.get_task_commands(&req.worker_id);

        Ok(Response::new(HeartbeatResponse {
            success: true,
            commands,
        }))
    }

    /// Deploy a task to a worker (called internally).
    async fn deploy_task(
        &self,
        request: Request<bicycle_protocol::control::DeployTaskRequest>,
    ) -> Result<Response<DeployTaskResponse>, Status> {
        let req = request.into_inner();

        debug!(
            task_id = %req.task_id,
            job_id = %req.job_id,
            "Deploy task request"
        );

        // Update task state
        if let Some(mut task) = self.state.tasks.get_mut(&req.task_id) {
            task.state = TaskState::Deploying;
        }

        Ok(Response::new(DeployTaskResponse {
            success: true,
            message: "Deployed".to_string(),
        }))
    }

    /// Cancel a task.
    async fn cancel_task(
        &self,
        request: Request<CancelTaskRequest>,
    ) -> Result<Response<CancelTaskResponse>, Status> {
        let req = request.into_inner();

        if let Some(mut task) = self.state.tasks.get_mut(&req.task_id) {
            task.state = TaskState::Canceling;
            info!(task_id = %req.task_id, "Task cancellation initiated");
            Ok(Response::new(CancelTaskResponse {
                success: true,
                message: "Canceling".to_string(),
            }))
        } else {
            Err(Status::not_found("Task not found"))
        }
    }

    /// Trigger a checkpoint.
    async fn trigger_checkpoint(
        &self,
        request: Request<TriggerCheckpointRequest>,
    ) -> Result<Response<TriggerCheckpointResponse>, Status> {
        let req = request.into_inner();

        let coordinators = self.checkpoint_coordinators.read();
        if let Some(coordinator) = coordinators.get(&req.job_id) {
            let is_savepoint =
                req.checkpoint_type == bicycle_protocol::control::CheckpointType::Savepoint as i32;

            match coordinator.trigger_checkpoint(is_savepoint) {
                Ok(id) => {
                    info!(job_id = %req.job_id, checkpoint_id = id, "Checkpoint triggered");
                    Ok(Response::new(TriggerCheckpointResponse {
                        success: true,
                        message: format!("Checkpoint {} triggered", id),
                    }))
                }
                Err(e) => {
                    error!(job_id = %req.job_id, error = %e, "Failed to trigger checkpoint");
                    Ok(Response::new(TriggerCheckpointResponse {
                        success: false,
                        message: e.to_string(),
                    }))
                }
            }
        } else {
            Err(Status::not_found("Job not found or not running"))
        }
    }

    /// Acknowledge a checkpoint.
    async fn acknowledge_checkpoint(
        &self,
        request: Request<AckCheckpointRequest>,
    ) -> Result<Response<AckCheckpointResponse>, Status> {
        let req = request.into_inner();

        info!(
            job_id = %req.job_id,
            task_id = %req.task_id,
            checkpoint_id = req.checkpoint_id,
            "Checkpoint acknowledgment received"
        );

        // Forward to checkpoint coordinator
        // Extract ack_sender while holding the lock, then drop the lock before awaiting
        let ack_sender = {
            let coordinators = self.checkpoint_coordinators.read();
            coordinators.get(&req.job_id).map(|c| c.ack_sender())
        };

        if let Some(ack_sender) = ack_sender {
            // Build a TaskId from the ack request
            // Extract vertex_id and subtask_index from task_id (format: job_id-vertex_id-subtask_idx)
            let prefix = format!("{}-", req.job_id);
            let (vertex_id, subtask_index) = if let Some(rest) = req.task_id.strip_prefix(&prefix) {
                if let Some(last_dash) = rest.rfind('-') {
                    let vid = rest[..last_dash].to_string();
                    let idx = rest[last_dash + 1..].parse::<u32>().unwrap_or(0);
                    (vid, idx)
                } else {
                    (rest.to_string(), 0)
                }
            } else {
                (req.task_id.clone(), 0)
            };

            let task_id = bicycle_core::TaskId::new(
                JobId::from_string(&req.job_id),
                &vertex_id,
                subtask_index,
            );

            info!(
                raw_task_id = %req.task_id,
                parsed_vertex_id = %vertex_id,
                parsed_subtask_index = subtask_index,
                constructed_task_id = %task_id,
                "Parsed checkpoint ack task ID"
            );

            let metadata = req.metadata.as_ref();
            let ack = bicycle_checkpoint::CheckpointAck {
                task_id,
                checkpoint_id: req.checkpoint_id as u64,
                state_handle: bicycle_core::StateHandle {
                    path: metadata.map(|m| m.state_handle.clone()).unwrap_or_default(),
                    size: metadata.map(|m| m.state_size_bytes as u64).unwrap_or(0),
                    checksum: None,
                },
            };

            if let Err(e) = ack_sender.send(ack).await {
                warn!(error = %e, "Failed to forward checkpoint ack to coordinator");
            }
        } else {
            warn!(job_id = %req.job_id, "No checkpoint coordinator found for job");
        }

        Ok(Response::new(AckCheckpointResponse { success: true }))
    }

    /// Submit a new job.
    async fn submit_job(
        &self,
        request: Request<SubmitJobRequest>,
    ) -> Result<Response<SubmitJobResponse>, Status> {
        let req = request.into_inner();

        let job_id = uuid::Uuid::new_v4().to_string();

        info!(job_id = %job_id, name = %req.job_name, "Job submission request");

        let graph = req.job_graph.ok_or_else(|| Status::invalid_argument("Job graph required"))?;
        let config = req.config.unwrap_or_default();

        // Extract plugin info
        let plugin_module = req.plugin_module;
        let plugin_type = req.plugin_type;

        // Create job info with plugin
        let job_info = JobInfo::with_plugin(
            job_id.clone(),
            req.job_name.clone(),
            graph.clone(),
            config.clone(),
            plugin_module.clone(),
            plugin_type.clone(),
        );

        // Create physical tasks
        let physical_tasks = self.state.create_physical_tasks(
            &job_id,
            &graph,
            &plugin_module,
            &plugin_type,
        );

        if physical_tasks.is_empty() {
            return Err(Status::invalid_argument("Job graph has no vertices"));
        }

        // Schedule tasks
        let result = self.scheduler.schedule_job(&job_id, &physical_tasks);

        if result.scheduled.is_empty() {
            return Ok(Response::new(SubmitJobResponse {
                success: false,
                job_id: String::new(),
                message: "No workers available to schedule tasks".to_string(),
            }));
        }

        // Build task location map
        let task_locations: HashMap<String, String> = result
            .scheduled
            .iter()
            .cloned()
            .collect();

        // Store job
        let mut job = job_info;
        job.tasks = result.scheduled.iter().map(|(t, _)| t.clone()).collect();
        job.state = JobState::Running;
        self.state.jobs.insert(job_id.clone(), job);

        // Create checkpoint coordinator and register sink tasks (but don't start yet)
        let checkpoint_interval = config.checkpoint_interval_ms.max(1000);
        let coordinator = self.create_checkpoint_coordinator(&job_id, checkpoint_interval);

        // Register only sink tasks with checkpoint coordinator (they are the ack endpoints)
        for (task_id, _) in &result.scheduled {
            if let Some(task) = self.state.tasks.get(task_id) {
                if task.operator_type == OperatorType::Sink {
                    coordinator.register_task(bicycle_core::TaskId::new(
                        JobId::from_string(&job_id),
                        &task.vertex_id,
                        task.subtask_index as u32,
                    ));
                }
            }
        }

        // Deploy tasks to workers FIRST — coordinator must not trigger before tasks exist
        match self
            .deploy_tasks_to_workers(&job_id, &physical_tasks, &task_locations)
            .await
        {
            Ok(deployed) => {
                info!(
                    job_id = %job_id,
                    tasks = deployed.len(),
                    "Job submitted and tasks deployed successfully"
                );

                // Now that all tasks are deployed, start the checkpoint coordinator
                let coord_clone = coordinator.clone();
                tokio::spawn(async move {
                    if let Err(e) = coord_clone.run().await {
                        error!(error = %e, "Checkpoint coordinator exited with error");
                    }
                });

                // Spawn bridge task: subscribe to coordinator triggers and forward to source AND sink workers.
                // We send to sources (barriers propagate downstream) AND directly to sinks
                // (sinks with no upstream connection, e.g. due to Forward partition mismatch, get barriers).
                spawn_checkpoint_trigger_bridge(
                    self.state.clone(),
                    coordinator.clone(),
                    job_id.clone(),
                    task_locations.clone(),
                );

                // Spawn checkpoint stats updater
                spawn_checkpoint_stats_updater(
                    self.state.clone(),
                    coordinator.clone(),
                    job_id.clone(),
                );

                Ok(Response::new(SubmitJobResponse {
                    success: true,
                    job_id,
                    message: format!(
                        "Job submitted with {} tasks deployed",
                        deployed.len()
                    ),
                }))
            }
            Err(e) => {
                error!(job_id = %job_id, error = %e, "Failed to deploy some tasks");

                // Job is partially deployed - still return success but with warning
                Ok(Response::new(SubmitJobResponse {
                    success: true,
                    job_id,
                    message: format!(
                        "Job submitted with {} tasks scheduled, deployment issues: {}",
                        result.scheduled.len(),
                        e
                    ),
                }))
            }
        }
    }

    /// Get job status.
    async fn get_job_status(
        &self,
        request: Request<GetJobStatusRequest>,
    ) -> Result<Response<GetJobStatusResponse>, Status> {
        let req = request.into_inner();

        let job = self
            .state
            .jobs
            .get(&req.job_id)
            .ok_or_else(|| Status::not_found("Job not found"))?;

        // Build vertex_id -> vertex_name map from job graph
        let vertex_names: std::collections::HashMap<String, String> = job
            .graph
            .vertices
            .iter()
            .map(|v| (v.vertex_id.clone(), v.name.clone()))
            .collect();

        // Collect task statuses
        let task_statuses: Vec<TaskStatus> = job
            .tasks
            .iter()
            .filter_map(|task_id| {
                let task = self.state.tasks.get(task_id)?;
                let operator_name = vertex_names
                    .get(&task.vertex_id)
                    .cloned()
                    .unwrap_or_default();
                Some(TaskStatus {
                    task_id: task.task_id.clone(),
                    state: task.state as i32,
                    error_message: task.error_message.clone().unwrap_or_default(),
                    records_processed: task.records_processed,
                    bytes_processed: task.bytes_processed,
                    operator_name,
                })
            })
            .collect();

        Ok(Response::new(GetJobStatusResponse {
            job_id: job.job_id.clone(),
            state: job.state as i32,
            start_time: job.start_time_unix,
            end_time: job.end_time_unix.unwrap_or(0),
            task_statuses,
            metrics: Some(JobMetrics {
                records_in: job.records_in,
                records_out: job.records_out,
                bytes_in: job.bytes_in,
                bytes_out: job.bytes_out,
                last_checkpoint_id: job.last_checkpoint_id,
                last_checkpoint_time: job.last_checkpoint_time,
            }),
            job_graph: Some(job.graph.clone()),
            job_name: job.name.clone(),
        }))
    }

    /// Cancel a job.
    async fn cancel_job(
        &self,
        request: Request<CancelJobRequest>,
    ) -> Result<Response<CancelJobResponse>, Status> {
        let req = request.into_inner();

        info!(job_id = %req.job_id, savepoint = req.savepoint, "Job cancellation request");

        // Update job state
        if let Some(mut job) = self.state.jobs.get_mut(&req.job_id) {
            job.state = JobState::Canceling;
        } else {
            return Err(Status::not_found("Job not found"));
        }

        // Cancel all tasks
        let cancelled = self.scheduler.cancel_job_tasks(&req.job_id);

        // If savepoint requested, trigger one first
        let savepoint_path = if req.savepoint {
            let coordinators = self.checkpoint_coordinators.read();
            if let Some(coordinator) = coordinators.get(&req.job_id) {
                match coordinator.trigger_checkpoint(true) {
                    Ok(id) => {
                        Some(coordinator.checkpoint_path(id).to_string_lossy().to_string())
                    }
                    Err(e) => {
                        warn!(error = %e, "Failed to create savepoint");
                        None
                    }
                }
            } else {
                None
            }
        } else {
            None
        };

        // Update job state to canceled
        if let Some(mut job) = self.state.jobs.get_mut(&req.job_id) {
            job.state = JobState::Canceled;
            job.set_end_time();
        }

        info!(job_id = %req.job_id, tasks_cancelled = cancelled.len(), "Job cancelled");

        Ok(Response::new(CancelJobResponse {
            success: true,
            message: format!("Cancelled {} tasks", cancelled.len()),
            savepoint_path: savepoint_path.unwrap_or_default(),
        }))
    }

    /// List all jobs.
    async fn list_jobs(
        &self,
        request: Request<ListJobsRequest>,
    ) -> Result<Response<ListJobsResponse>, Status> {
        let req = request.into_inner();

        let jobs: Vec<JobSummary> = self
            .state
            .jobs
            .iter()
            .filter(|job| {
                if req.states.is_empty() {
                    true
                } else {
                    req.states.contains(&(job.state as i32))
                }
            })
            .map(|job| {
                let tasks_running = job
                    .tasks
                    .iter()
                    .filter(|t| {
                        self.state
                            .tasks
                            .get(*t)
                            .map(|task| task.state == TaskState::Running)
                            .unwrap_or(false)
                    })
                    .count() as i32;

                JobSummary {
                    job_id: job.job_id.clone(),
                    name: job.name.clone(),
                    state: job.state as i32,
                    start_time: job.start_time_unix,
                    tasks_total: job.tasks.len() as i32,
                    tasks_running,
                }
            })
            .collect();

        Ok(Response::new(ListJobsResponse { jobs }))
    }

    /// Get cluster information.
    async fn get_cluster_info(
        &self,
        _request: Request<GetClusterInfoRequest>,
    ) -> Result<Response<GetClusterInfoResponse>, Status> {
        let (total_workers, active_workers, total_slots, available_slots, running_jobs, total_tasks) =
            self.state.get_cluster_info();

        Ok(Response::new(GetClusterInfoResponse {
            total_workers,
            active_workers,
            total_slots,
            available_slots,
            running_jobs,
            total_tasks,
            uptime_ms: self.state.uptime_ms(),
        }))
    }

    /// List all workers.
    async fn list_workers(
        &self,
        _request: Request<ListWorkersRequest>,
    ) -> Result<Response<ListWorkersResponse>, Status> {
        let workers: Vec<WorkerSummary> = self
            .state
            .workers
            .iter()
            .map(|w| {
                let state = if w.is_alive(self.state.worker_timeout) {
                    WorkerState::Active
                } else {
                    WorkerState::Lost
                };

                WorkerSummary {
                    worker_id: w.worker_id.clone(),
                    hostname: w.hostname.clone(),
                    slots: w.total_slots,
                    slots_used: w.used_slots,
                    state: state as i32,
                    last_heartbeat_ms: w.last_heartbeat.elapsed().as_millis() as i64,
                    metrics: w.metrics.clone(),
                    running_tasks: w.assigned_tasks.iter().cloned().collect(),
                }
            })
            .collect();

        Ok(Response::new(ListWorkersResponse { workers }))
    }

    /// Get cluster metrics.
    async fn get_metrics(
        &self,
        _request: Request<GetMetricsRequest>,
    ) -> Result<Response<GetMetricsResponse>, Status> {
        use std::sync::atomic::Ordering;

        // Aggregate metrics from all tasks
        let mut total_records: i64 = 0;
        let mut total_bytes: i64 = 0;

        for task in self.state.tasks.iter() {
            total_records += task.records_processed;
            total_bytes += task.bytes_processed;
        }

        // Calculate throughput (records/bytes per second)
        let uptime_secs = self.state.start_time.elapsed().as_secs() as f64;
        let throughput_records = if uptime_secs > 0.0 {
            total_records as f64 / uptime_secs
        } else {
            0.0
        };
        let throughput_bytes = if uptime_secs > 0.0 {
            total_bytes as f64 / uptime_secs
        } else {
            0.0
        };

        // Build job records map
        let mut job_records = std::collections::HashMap::new();
        for job in self.state.jobs.iter() {
            let records: i64 = job
                .tasks
                .iter()
                .filter_map(|t| self.state.tasks.get(t))
                .map(|t| t.records_processed)
                .sum();
            job_records.insert(job.job_id.clone(), records);
        }

        Ok(Response::new(GetMetricsResponse {
            total_records_processed: total_records,
            total_bytes_processed: total_bytes,
            checkpoints_completed: self
                .state
                .metrics
                .checkpoints_completed
                .load(Ordering::Relaxed),
            checkpoints_failed: self
                .state
                .metrics
                .checkpoints_failed
                .load(Ordering::Relaxed),
            uptime_seconds: uptime_secs as i64,
            throughput_records_per_sec: throughput_records,
            throughput_bytes_per_sec: throughput_bytes,
            job_records,
        }))
    }
}
