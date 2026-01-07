use anyhow::Result;
use clap::Parser;
use tracing::info;
use tracing_subscriber::EnvFilter;

/// Worker / TaskManager stub.
///
/// Next steps:
/// - register with JobManager
/// - accept Task deployments
/// - run tasks and report heartbeats
#[derive(Debug, Parser)]
#[command(name = "worker")] 
struct Args {
    /// JobManager address to connect to.
    #[arg(long, default_value = "127.0.0.1:9000")]
    jobmanager: String,

    /// Local bind address (for future data-plane/control-plane endpoints).
    #[arg(long, default_value = "0.0.0.0:0")]
    bind: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse()?))
        .init();

    let args = Args::parse();
    info!(jobmanager = %args.jobmanager, bind = %args.bind, "worker stub running (no-op)");

    tokio::signal::ctrl_c().await?;
    Ok(())
}
