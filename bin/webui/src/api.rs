//! REST API server for Bicycle Web UI.

use anyhow::Result;
use axum::{
    routing::{delete, get, post},
    Router,
};
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::RwLock;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use tracing::{info, warn};

use bicycle_protocol::control::{
    control_plane_client::ControlPlaneClient, GetMetricsRequest,
};

use crate::handlers::{self, MetricsHistoryPoint};

/// Application state shared across handlers.
pub struct AppState {
    pub jobmanager_addr: String,
    /// Ring buffer of metrics history points, polled periodically
    pub metrics_history: RwLock<VecDeque<MetricsHistoryPoint>>,
}

/// Run the web server.
pub async fn run_server(bind: &str, jobmanager: &str) -> Result<()> {
    let state = Arc::new(AppState {
        jobmanager_addr: jobmanager.to_string(),
        metrics_history: RwLock::new(VecDeque::with_capacity(180)),
    });

    // Spawn background task to poll metrics every 10 seconds
    let poll_state = state.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));
        let mut prev_records: i64 = 0;
        let mut prev_bytes: i64 = 0;

        loop {
            interval.tick().await;

            let addr = format!("http://{}", poll_state.jobmanager_addr);
            match ControlPlaneClient::connect(addr).await {
                Ok(mut client) => {
                    if let Ok(response) = client.get_metrics(GetMetricsRequest {}).await {
                        let resp = response.into_inner();

                        let now = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis() as i64;

                        // Calculate rates (per second, 10s interval)
                        let records_delta = resp.total_records_processed - prev_records;
                        let bytes_delta = resp.total_bytes_processed - prev_bytes;
                        prev_records = resp.total_records_processed;
                        prev_bytes = resp.total_bytes_processed;

                        let point = MetricsHistoryPoint {
                            timestamp: now,
                            records_per_sec: records_delta as f64 / 10.0,
                            bytes_per_sec: bytes_delta as f64 / 10.0,
                        };

                        let mut history = poll_state.metrics_history.write().await;
                        if history.len() >= 180 {
                            history.pop_front();
                        }
                        history.push_back(point);
                    }
                }
                Err(e) => {
                    warn!("Failed to poll metrics from JobManager: {}", e);
                }
            }
        }
    });

    let app = Router::new()
        // Job management
        .route("/api/jobs", get(handlers::list_jobs))
        .route("/api/jobs", post(handlers::submit_job))
        .route("/api/jobs/:job_id", get(handlers::get_job))
        .route("/api/jobs/:job_id", delete(handlers::cancel_job))
        .route("/api/jobs/:job_id/tasks", get(handlers::get_job_tasks))
        .route("/api/jobs/:job_id/checkpoints", get(handlers::get_job_checkpoints))
        .route("/api/jobs/:job_id/graph", get(handlers::get_job_graph))
        .route("/api/jobs/:job_id/exceptions", get(handlers::get_job_exceptions))
        // Cluster info
        .route("/api/cluster", get(handlers::get_cluster_info))
        .route("/api/cluster/workers", get(handlers::list_workers))
        // Metrics
        .route("/api/metrics", get(handlers::get_metrics))
        .route("/api/metrics/history", get(handlers::get_metrics_history))
        // Health
        .route("/health", get(handlers::health_check))
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive())
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(bind).await?;
    info!("API server listening on {}", bind);

    axum::serve(listener, app).await?;
    Ok(())
}
