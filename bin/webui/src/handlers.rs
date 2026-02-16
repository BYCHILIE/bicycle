//! HTTP request handlers for the Web UI API.

use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tonic::transport::Channel;
use tracing::error;

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
    pub name: String,
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
    pub operator_name: String,
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
// Job Graph types
// ============================================================================

#[derive(Debug, Serialize)]
pub struct JobGraphVertex {
    pub id: String,
    pub uid: String,
    pub name: String,
    pub operator_type: String,
    pub parallelism: i32,
    pub slot_sharing_group: String,
    pub records_in: i64,
    pub records_out: i64,
    pub backpressure: f64,
}

#[derive(Debug, Serialize)]
pub struct JobGraphEdge {
    pub source_id: String,
    pub target_id: String,
    pub partition_strategy: String,
}

#[derive(Debug, Serialize)]
pub struct JobGraphResponse {
    pub vertices: Vec<JobGraphVertex>,
    pub edges: Vec<JobGraphEdge>,
}

// ============================================================================
// Job Exceptions types
// ============================================================================

#[derive(Debug, Serialize)]
pub struct JobException {
    pub timestamp: i64,
    pub task_id: String,
    pub task_index: Option<u32>,
    pub operator_name: String,
    pub message: String,
    pub stack_trace: String,
    pub root_cause: Option<String>,
    pub location: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct JobExceptionsResponse {
    pub exceptions: Vec<JobException>,
    pub truncated: bool,
}

// ============================================================================
// Metrics History types
// ============================================================================

#[derive(Debug, Serialize)]
pub struct MetricsHistoryPoint {
    pub timestamp: i64,
    pub records_per_sec: f64,
    pub bytes_per_sec: f64,
}

#[derive(Debug, Serialize)]
pub struct MetricsHistoryResponse {
    pub points: Vec<MetricsHistoryPoint>,
}

#[derive(Debug, Deserialize)]
pub struct MetricsHistoryQuery {
    pub minutes: Option<i32>,
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
        plugin_module: Vec::new(),
        plugin_type: String::new(),
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
                name: resp.job_name,
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
                    operator_name: t.operator_name.clone(),
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

/// Get job graph for visualization.
pub async fn get_job_graph(
    State(state): State<Arc<AppState>>,
    Path(job_id): Path<String>,
) -> Result<Json<JobGraphResponse>, StatusCode> {
    let mut client = get_client(&state).await?;

    let req = GetJobStatusRequest {
        job_id: job_id.clone(),
    };

    match client.get_job_status(req).await {
        Ok(response) => {
            let resp = response.into_inner();

            // Use the job graph from the response
            let job_graph = resp.job_graph.unwrap_or_default();

            // Build task metrics map for aggregating records per vertex
            let mut vertex_records: std::collections::HashMap<String, (i64, i64)> =
                std::collections::HashMap::new();

            for task in &resp.task_statuses {
                // Extract vertex_id from task_id (format: job_id-vertex_id-subtask_idx)
                // job_id is a UUID with hyphens, so we strip it as a prefix first
                let prefix = format!("{}-", job_id);
                if let Some(rest) = task.task_id.strip_prefix(&prefix) {
                    // rest is "vertex_id-subtask_idx", find last dash for subtask index
                    if let Some(last_dash) = rest.rfind('-') {
                        let vertex_id = rest[..last_dash].to_string();
                        let entry = vertex_records.entry(vertex_id).or_insert((0, 0));
                        entry.0 += task.records_processed;
                        entry.1 += task.bytes_processed;
                    }
                }
            }

            // Convert vertices from proto
            let vertices: Vec<JobGraphVertex> = job_graph
                .vertices
                .iter()
                .map(|v| {
                    let operator_type_enum =
                        bicycle_protocol::control::OperatorType::try_from(v.operator_type)
                            .unwrap_or_default();
                    let (records_in, _bytes) =
                        vertex_records.get(&v.vertex_id).cloned().unwrap_or((0, 0));

                    JobGraphVertex {
                        id: v.vertex_id.clone(),
                        uid: if v.uid.is_empty() { v.vertex_id.clone() } else { v.uid.clone() },
                        name: v.name.clone(),
                        operator_type: format!("{:?}", operator_type_enum),
                        parallelism: v.parallelism,
                        slot_sharing_group: "default".to_string(),
                        records_in,
                        records_out: records_in, // Approximation
                        backpressure: 0.0,
                    }
                })
                .collect();

            // Convert edges from proto
            let edges: Vec<JobGraphEdge> = job_graph
                .edges
                .iter()
                .map(|e| {
                    let partition_strategy_enum =
                        bicycle_protocol::control::PartitionStrategy::try_from(e.partition_strategy)
                            .unwrap_or_default();
                    JobGraphEdge {
                        source_id: e.source_vertex_id.clone(),
                        target_id: e.target_vertex_id.clone(),
                        partition_strategy: format!("{:?}", partition_strategy_enum),
                    }
                })
                .collect();

            Ok(Json(JobGraphResponse { vertices, edges }))
        }
        Err(e) => {
            error!("Failed to get job graph: {}", e);
            Err(StatusCode::NOT_FOUND)
        }
    }
}

/// Get job exceptions.
pub async fn get_job_exceptions(
    State(state): State<Arc<AppState>>,
    Path(job_id): Path<String>,
) -> Result<Json<JobExceptionsResponse>, StatusCode> {
    let mut client = get_client(&state).await?;

    let req = GetJobStatusRequest { job_id: job_id.clone() };

    match client.get_job_status(req).await {
        Ok(response) => {
            let resp = response.into_inner();

            // Extract exceptions from failed tasks
            let exceptions: Vec<JobException> = resp
                .task_statuses
                .iter()
                .filter(|t| {
                    t.state() == bicycle_protocol::control::TaskState::Failed
                        && !t.error_message.is_empty()
                })
                .map(|t| {
                    let msg = &t.error_message;

                    // Use operator_name from proto; fall back to task_id parsing
                    let operator_name = if !t.operator_name.is_empty() {
                        t.operator_name.clone()
                    } else {
                        let prefix = format!("{}-", job_id);
                        if let Some(rest) = t.task_id.strip_prefix(&prefix) {
                            if let Some(last_dash) = rest.rfind('-') {
                                rest[..last_dash].to_string()
                            } else {
                                rest.to_string()
                            }
                        } else {
                            t.task_id.clone()
                        }
                    };

                    // Extract subtask index from task_id (last segment after '-')
                    let task_index = t
                        .task_id
                        .rsplit('-')
                        .next()
                        .and_then(|s| s.parse::<u32>().ok());

                    // Try to extract a root cause and location from the error message.
                    // Convention: lines starting with "Caused by:" or "Location:" are parsed.
                    let mut root_cause = None;
                    let mut location = None;
                    let mut stack_lines = Vec::new();
                    for line in msg.lines() {
                        let trimmed = line.trim();
                        if trimmed.starts_with("Caused by:") {
                            root_cause =
                                Some(trimmed.trim_start_matches("Caused by:").trim().to_string());
                        } else if trimmed.starts_with("Location:") {
                            location =
                                Some(trimmed.trim_start_matches("Location:").trim().to_string());
                        }
                        stack_lines.push(line.to_string());
                    }

                    // If no explicit location, try to find a plugin/crate reference
                    if location.is_none() {
                        for line in msg.lines() {
                            let t = line.trim();
                            if t.contains("::") && (t.contains("Failed to") || t.contains("Error")) {
                                location = Some(t.to_string());
                                break;
                            }
                        }
                    }

                    JobException {
                        timestamp: std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis() as i64,
                        task_id: t.task_id.clone(),
                        task_index,
                        operator_name,
                        message: msg.clone(),
                        stack_trace: stack_lines.join("\n"),
                        root_cause,
                        location,
                    }
                })
                .collect();

            Ok(Json(JobExceptionsResponse {
                exceptions,
                truncated: false,
            }))
        }
        Err(e) => {
            error!("Failed to get job exceptions: {}", e);
            Err(StatusCode::NOT_FOUND)
        }
    }
}

/// Get metrics history for charts.
pub async fn get_metrics_history(
    State(_state): State<Arc<AppState>>,
    axum::extract::Query(query): axum::extract::Query<MetricsHistoryQuery>,
) -> Result<Json<MetricsHistoryResponse>, StatusCode> {
    let minutes = query.minutes.unwrap_or(15);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;

    // Generate synthetic data points for demo
    // In real implementation, would query from metrics store
    let points: Vec<MetricsHistoryPoint> = (0..minutes)
        .map(|i| {
            let timestamp = now - ((minutes - i) as i64 * 60 * 1000);
            MetricsHistoryPoint {
                timestamp,
                records_per_sec: 1000.0 + (i as f64 * 10.0) + (rand_simple() * 200.0),
                bytes_per_sec: 50000.0 + (i as f64 * 500.0) + (rand_simple() * 10000.0),
            }
        })
        .collect();

    Ok(Json(MetricsHistoryResponse { points }))
}

// Simple pseudo-random for demo data
fn rand_simple() -> f64 {
    use std::time::SystemTime;
    let nanos = SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos();
    (nanos % 1000) as f64 / 1000.0
}
