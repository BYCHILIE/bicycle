use anyhow::Result;
use clap::Parser;
use tracing::info;
use tracing_subscriber::EnvFilter;

/// Control-plane stub.
///
/// Next steps:
/// - accept JobGraph submissions
/// - build PhysicalPlan
/// - assign tasks to workers
/// - coordinate checkpoints
#[derive(Debug, Parser)]
#[command(name = "jobmanager")] 
struct Args {
    /// Address to bind the control-plane server.
    #[arg(long, default_value = "0.0.0.0:9000")]
    bind: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse()?))
        .init();

    let args = Args::parse();
    info!(bind = %args.bind, "jobmanager stub running (no-op)");

    // Keep process alive for now.
    tokio::signal::ctrl_c().await?;
    Ok(())
}
