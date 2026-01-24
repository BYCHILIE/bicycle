//! gRPC service implementation for the Worker.
//!
//! Workers expose this service to receive task deployment commands from the JobManager.

use crate::task_executor::TaskExecutor;
use bicycle_protocol::control::{
    worker_control_server::WorkerControl, CancelTaskRequest, CancelTaskResponse,
    DeployTaskRequest, DeployTaskResponse, TriggerTaskCheckpointRequest,
    TriggerTaskCheckpointResponse,
};
use std::sync::Arc;
use tonic::{Request, Response, Status};
use tracing::{error, info};

/// gRPC service implementation for worker control.
pub struct WorkerControlService {
    executor: Arc<TaskExecutor>,
}

impl WorkerControlService {
    pub fn new(executor: Arc<TaskExecutor>) -> Self {
        Self { executor }
    }
}

#[tonic::async_trait]
impl WorkerControl for WorkerControlService {
    /// Deploy a task to this worker.
    async fn deploy_task(
        &self,
        request: Request<DeployTaskRequest>,
    ) -> Result<Response<DeployTaskResponse>, Status> {
        let req = request.into_inner();

        info!(
            job_id = %req.job_id,
            task_id = %req.task_id,
            "Received deploy task request"
        );

        let descriptor = req.task.ok_or_else(|| {
            Status::invalid_argument("Task descriptor is required")
        })?;

        match self.executor.deploy_task(&req.job_id, &req.task_id, descriptor).await {
            Ok(()) => {
                info!(task_id = %req.task_id, "Task deployed successfully");
                Ok(Response::new(DeployTaskResponse {
                    success: true,
                    message: "Task deployed".to_string(),
                }))
            }
            Err(e) => {
                error!(task_id = %req.task_id, error = %e, "Failed to deploy task");
                Ok(Response::new(DeployTaskResponse {
                    success: false,
                    message: e.to_string(),
                }))
            }
        }
    }

    /// Cancel a task on this worker.
    async fn cancel_task(
        &self,
        request: Request<CancelTaskRequest>,
    ) -> Result<Response<CancelTaskResponse>, Status> {
        let req = request.into_inner();

        info!(task_id = %req.task_id, "Received cancel task request");

        match self.executor.cancel_task(&req.task_id).await {
            Ok(()) => {
                Ok(Response::new(CancelTaskResponse {
                    success: true,
                    message: "Task cancelled".to_string(),
                }))
            }
            Err(e) => {
                Ok(Response::new(CancelTaskResponse {
                    success: false,
                    message: e.to_string(),
                }))
            }
        }
    }

    /// Trigger a checkpoint for a task.
    async fn trigger_task_checkpoint(
        &self,
        request: Request<TriggerTaskCheckpointRequest>,
    ) -> Result<Response<TriggerTaskCheckpointResponse>, Status> {
        let req = request.into_inner();

        info!(
            task_id = %req.task_id,
            checkpoint_id = req.checkpoint_id,
            "Received checkpoint trigger request"
        );

        // For now, just acknowledge - actual checkpoint implementation in the executor
        // would involve signaling the task to take a checkpoint
        match self.executor.trigger_checkpoint(&req.task_id, req.checkpoint_id).await {
            Ok(()) => {
                Ok(Response::new(TriggerTaskCheckpointResponse {
                    success: true,
                    message: "Checkpoint triggered".to_string(),
                }))
            }
            Err(e) => {
                Ok(Response::new(TriggerTaskCheckpointResponse {
                    success: false,
                    message: e.to_string(),
                }))
            }
        }
    }
}
