//! REST API server for Bicycle Web UI.

use anyhow::Result;
use axum::{
    response::Html,
    routing::{delete, get, post},
    Router,
};
use std::sync::Arc;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use tracing::info;

use crate::handlers;

/// Application state shared across handlers.
pub struct AppState {
    pub jobmanager_addr: String,
}

/// Embedded index.html for when static files are not available
const INDEX_HTML: &str = include_str!("../static/index.html");

/// Serve embedded index.html
async fn serve_index() -> Html<&'static str> {
    Html(INDEX_HTML)
}

/// Run the web server.
pub async fn run_server(bind: &str, jobmanager: &str) -> Result<()> {
    let state = Arc::new(AppState {
        jobmanager_addr: jobmanager.to_string(),
    });

    let app = Router::new()
        // Job management
        .route("/api/jobs", get(handlers::list_jobs))
        .route("/api/jobs", post(handlers::submit_job))
        .route("/api/jobs/{job_id}", get(handlers::get_job))
        .route("/api/jobs/{job_id}", delete(handlers::cancel_job))
        .route("/api/jobs/{job_id}/tasks", get(handlers::get_job_tasks))
        .route(
            "/api/jobs/{job_id}/checkpoints",
            get(handlers::get_job_checkpoints),
        )
        // Cluster info
        .route("/api/cluster", get(handlers::get_cluster_info))
        .route("/api/cluster/workers", get(handlers::list_workers))
        // Metrics
        .route("/api/metrics", get(handlers::get_metrics))
        // Health
        .route("/health", get(handlers::health_check))
        // Serve embedded dashboard
        .route("/", get(serve_index))
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive())
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(bind).await?;
    info!("Web UI server listening on {}", bind);
    info!("Dashboard available at http://{}", bind);

    axum::serve(listener, app).await?;
    Ok(())
}
