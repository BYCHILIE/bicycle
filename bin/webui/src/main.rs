//! Web UI for Bicycle job management.
//!
//! Provides a REST API and dashboard for:
//! - Viewing running jobs and their status
//! - Submitting new jobs
//! - Viewing checkpoints
//! - Monitoring metrics

use anyhow::Result;
use clap::Parser;
use tracing::info;
use tracing_subscriber::EnvFilter;

mod api;
mod handlers;

#[derive(Debug, Parser)]
#[command(name = "webui")]
struct Args {
    /// Address to bind the web server
    #[arg(long, default_value = "0.0.0.0:8081")]
    bind: String,

    /// JobManager address for API calls
    #[arg(long, default_value = "127.0.0.1:9000")]
    jobmanager: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse()?))
        .init();

    let args = Args::parse();
    info!(bind = %args.bind, jobmanager = %args.jobmanager, "Starting Bicycle Web UI");

    api::run_server(&args.bind, &args.jobmanager).await
}
