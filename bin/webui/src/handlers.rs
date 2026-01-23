//! HTTP request handlers for the Web UI API.

use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tonic::transport::Channel;
use tracing::{error, info};

use bicycle_protocol::control::{
    control_plane_client::ControlPlaneClient, CancelJobRequest, GetClusterInfoRequest,
    GetJobStatusRequest, GetMetricsRequest, ListJobsRequest, ListWorkersRequest,
    SubmitJobRequest as ProtoSubmitJobRequest,
};

use crate::api::AppState;

// ============================================================================
// Response types
// ============================================================================

#[derive(Debug, Serialize)]
pub struct JobSummary {
    pub job_id: String,
    pub name: String,
    pub state: String,
    pub start_time: i64,
    pub tasks_total: usize,
    pub tasks_running: usize,
}

#[derive(Debug, Serialize)]
pub struct JobDetail {
    pub job_id: String,
    pub state: String,
    pub start_time: i64,
    pub end_time: Option<i64>,
    pub metrics: JobMetrics,
}

#[derive(Debug, Serialize)]
pub struct JobMetrics {
    pub records_in: i64,
    pub records_out: i64,
    pub bytes_in: i64,
    pub bytes_out: i64,
}

#[derive(Debug, Serialize)]
pub struct TaskSummary {
    pub task_id: String,
    pub state: String,
    pub records_processed: i64,
    pub bytes_processed: i64,
}

#[derive(Debug, Serialize)]
pub struct CheckpointInfo {
    pub checkpoint_id: i64,
    pub timestamp: i64,
    pub duration_ms: i64,
    pub state_size_bytes: i64,
}

#[derive(Debug, Serialize)]
pub struct ClusterInfo {
    pub workers: usize,
    pub total_slots: usize,
    pub available_slots: usize,
    pub running_jobs: usize,
}

#[derive(Debug, Serialize)]
pub struct WorkerInfo {
    pub worker_id: String,
    pub hostname: String,
    pub slots: i32,
    pub slots_used: i32,
    pub cpu_usage: f64,
    pub memory_used_mb: i64,
}

#[derive(Debug, Serialize)]
pub struct MetricsResponse {
    pub total_records_processed: i64,
    pub total_bytes_processed: i64,
    pub checkpoints_completed: i64,
    pub uptime_seconds: i64,
}

#[derive(Debug, Deserialize)]
pub struct SubmitJobRequest {
    pub name: String,
    pub parallelism: Option<i32>,
    pub checkpoint_interval_ms: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct SubmitJobResponse {
    pub job_id: String,
    pub message: String,
}

#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub version: String,
}

// ============================================================================
// Helper to get gRPC client
// ============================================================================

async fn get_client(state: &AppState) -> Result<ControlPlaneClient<Channel>, StatusCode> {
    let addr = format!("http://{}", state.jobmanager_addr);
    ControlPlaneClient::connect(addr)
        .await
        .map_err(|e| {
            error!("Failed to connect to JobManager: {}", e);
            StatusCode::SERVICE_UNAVAILABLE
        })
}

// ============================================================================
// Handlers
// ============================================================================

/// List all jobs.
pub async fn list_jobs(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Vec<JobSummary>>, StatusCode> {
    let mut client = get_client(&state).await?;

    let req = ListJobsRequest { states: vec![] };

    match client.list_jobs(req).await {
        Ok(response) => {
            let resp = response.into_inner();
            let jobs: Vec<JobSummary> = resp
                .jobs
                .into_iter()
                .map(|j| {
                    let state_enum =
                        bicycle_protocol::control::JobState::try_from(j.state).unwrap_or_default();
                    JobSummary {
                        job_id: j.job_id,
                        name: j.name,
                        state: format!("{:?}", state_enum),
                        start_time: j.start_time,
                        tasks_total: j.tasks_total as usize,
                        tasks_running: j.tasks_running as usize,
                    }
                })
                .collect();
            Ok(Json(jobs))
        }
        Err(e) => {
            error!("Failed to list jobs: {}", e);
            Ok(Json(vec![]))
        }
    }
}

/// Submit a new job.
pub async fn submit_job(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SubmitJobRequest>,
) -> Result<Json<SubmitJobResponse>, StatusCode> {
    let mut client = get_client(&state).await?;

    let proto_req = ProtoSubmitJobRequest {
        job_name: req.name.clone(),
        job_graph: None, // Would be populated from job definition
        config: None,
    };

    match client.submit_job(proto_req).await {
        Ok(response) => {
            let resp = response.into_inner();
            Ok(Json(SubmitJobResponse {
                job_id: resp.job_id,
                message: resp.message,
            }))
        }
        Err(e) => {
            error!("Failed to submit job: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

/// Get job details.
pub async fn get_job(
    State(state): State<Arc<AppState>>,
    Path(job_id): Path<String>,
) -> Result<Json<JobDetail>, StatusCode> {
    let mut client = get_client(&state).await?;

    let req = GetJobStatusRequest {
        job_id: job_id.clone(),
    };

    match client.get_job_status(req).await {
        Ok(response) => {
            let resp = response.into_inner();
            let state = format!("{:?}", resp.state());
            Ok(Json(JobDetail {
                job_id: resp.job_id,
                state,
                start_time: resp.start_time,
                end_time: if resp.end_time > 0 {
                    Some(resp.end_time)
                } else {
                    None
                },
                metrics: JobMetrics {
                    records_in: resp.metrics.as_ref().map(|m| m.records_in).unwrap_or(0),
                    records_out: resp.metrics.as_ref().map(|m| m.records_out).unwrap_or(0),
                    bytes_in: resp.metrics.as_ref().map(|m| m.bytes_in).unwrap_or(0),
                    bytes_out: resp.metrics.as_ref().map(|m| m.bytes_out).unwrap_or(0),
                },
            }))
        }
        Err(e) => {
            error!("Failed to get job status: {}", e);
            Err(StatusCode::NOT_FOUND)
        }
    }
}

/// Cancel a job.
pub async fn cancel_job(
    State(state): State<Arc<AppState>>,
    Path(job_id): Path<String>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let mut client = get_client(&state).await?;

    let req = CancelJobRequest {
        job_id: job_id.clone(),
        savepoint: false,
    };

    match client.cancel_job(req).await {
        Ok(response) => {
            let resp = response.into_inner();
            Ok(Json(serde_json::json!({
                "success": resp.success,
                "message": resp.message
            })))
        }
        Err(e) => {
            error!("Failed to cancel job: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

/// Get tasks for a job.
pub async fn get_job_tasks(
    State(state): State<Arc<AppState>>,
    Path(job_id): Path<String>,
) -> Result<Json<Vec<TaskSummary>>, StatusCode> {
    let mut client = get_client(&state).await?;

    let req = GetJobStatusRequest { job_id };

    match client.get_job_status(req).await {
        Ok(response) => {
            let resp = response.into_inner();
            let tasks: Vec<TaskSummary> = resp
                .task_statuses
                .iter()
                .map(|t| TaskSummary {
                    task_id: t.task_id.clone(),
                    state: format!("{:?}", t.state()),
                    records_processed: t.records_processed,
                    bytes_processed: t.bytes_processed,
                })
                .collect();
            Ok(Json(tasks))
        }
        Err(e) => {
            error!("Failed to get job tasks: {}", e);
            Err(StatusCode::NOT_FOUND)
        }
    }
}

/// Get checkpoints for a job.
pub async fn get_job_checkpoints(
    State(_state): State<Arc<AppState>>,
    Path(_job_id): Path<String>,
) -> Result<Json<Vec<CheckpointInfo>>, StatusCode> {
    // TODO: Implement when checkpoint history is available
    Ok(Json(vec![]))
}

/// Get cluster information.
pub async fn get_cluster_info(
    State(state): State<Arc<AppState>>,
) -> Result<Json<ClusterInfo>, StatusCode> {
    let mut client = get_client(&state).await?;

    match client.get_cluster_info(GetClusterInfoRequest {}).await {
        Ok(response) => {
            let resp = response.into_inner();
            Ok(Json(ClusterInfo {
                workers: resp.active_workers as usize,
                total_slots: resp.total_slots as usize,
                available_slots: resp.available_slots as usize,
                running_jobs: resp.running_jobs as usize,
            }))
        }
        Err(e) => {
            error!("Failed to get cluster info: {}", e);
            Ok(Json(ClusterInfo {
                workers: 0,
                total_slots: 0,
                available_slots: 0,
                running_jobs: 0,
            }))
        }
    }
}

/// List all workers.
pub async fn list_workers(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Vec<WorkerInfo>>, StatusCode> {
    let mut client = get_client(&state).await?;

    match client.list_workers(ListWorkersRequest {}).await {
        Ok(response) => {
            let resp = response.into_inner();
            let workers: Vec<WorkerInfo> = resp
                .workers
                .into_iter()
                .map(|w| {
                    let metrics = w.metrics.unwrap_or_default();
                    WorkerInfo {
                        worker_id: w.worker_id,
                        hostname: w.hostname,
                        slots: w.slots,
                        slots_used: w.slots_used,
                        cpu_usage: metrics.cpu_usage,
                        memory_used_mb: metrics.memory_used_mb,
                    }
                })
                .collect();
            Ok(Json(workers))
        }
        Err(e) => {
            error!("Failed to list workers: {}", e);
            Ok(Json(vec![]))
        }
    }
}

/// Get metrics.
pub async fn get_metrics(
    State(state): State<Arc<AppState>>,
) -> Result<Json<MetricsResponse>, StatusCode> {
    let mut client = get_client(&state).await?;

    match client.get_metrics(GetMetricsRequest {}).await {
        Ok(response) => {
            let resp = response.into_inner();
            Ok(Json(MetricsResponse {
                total_records_processed: resp.total_records_processed,
                total_bytes_processed: resp.total_bytes_processed,
                checkpoints_completed: resp.checkpoints_completed,
                uptime_seconds: resp.uptime_seconds,
            }))
        }
        Err(e) => {
            error!("Failed to get metrics: {}", e);
            Ok(Json(MetricsResponse {
                total_records_processed: 0,
                total_bytes_processed: 0,
                checkpoints_completed: 0,
                uptime_seconds: 0,
            }))
        }
    }
}

/// Health check endpoint.
pub async fn health_check() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "healthy".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
    })
}
