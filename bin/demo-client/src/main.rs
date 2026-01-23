//! Demo client that submits a sample job to the Bicycle cluster.
//!
//! This demonstrates:
//! - Connecting to the JobManager
//! - Submitting a streaming job with a job graph
//! - Monitoring job progress
//! - Viewing cluster status

use anyhow::Result;
use bicycle_protocol::control::{
    control_plane_client::ControlPlaneClient, GetClusterInfoRequest, GetJobStatusRequest,
    GetMetricsRequest, JobConfig, JobEdge, JobGraph, JobState, JobVertex, ListJobsRequest,
    ListWorkersRequest, OperatorType, PartitionStrategy, RestartStrategy, RestartStrategyType,
    SubmitJobRequest,
};
use clap::{Parser, Subcommand};
use std::time::Duration;
use tonic::transport::Channel;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(name = "demo-client")]
#[command(about = "Bicycle cluster demo client")]
struct Args {
    /// JobManager address
    #[arg(long, default_value = "127.0.0.1:9000")]
    jobmanager: String,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Submit a demo streaming job
    Submit {
        /// Job name
        #[arg(long, default_value = "demo-wordcount")]
        name: String,

        /// Parallelism for operators
        #[arg(long, default_value = "2")]
        parallelism: i32,

        /// Checkpoint interval in milliseconds
        #[arg(long, default_value = "10000")]
        checkpoint_interval: i64,
    },

    /// Show cluster status
    Status,

    /// List all workers
    Workers,

    /// List all jobs
    Jobs,

    /// Get job details
    Job {
        /// Job ID
        job_id: String,
    },

    /// Show cluster metrics
    Metrics,

    /// Run a full demo: submit job, monitor, and show results
    Demo {
        /// Duration to run the demo in seconds
        #[arg(long, default_value = "30")]
        duration: u64,
    },
}

async fn get_client(addr: &str) -> Result<ControlPlaneClient<Channel>> {
    let addr = format!("http://{}", addr);
    let client = ControlPlaneClient::connect(addr).await?;
    Ok(client)
}

/// Create a demo word count job graph.
fn create_demo_job_graph(parallelism: i32) -> JobGraph {
    // Simple pipeline: Source -> Map -> KeyBy -> Window -> Sink
    //
    // This represents a word count pipeline:
    // 1. Source: Generates events with words
    // 2. Map: Transforms each word to (word, 1)
    // 3. KeyBy: Partitions by word key (hash partitioning)
    // 4. Window: 5-second tumbling window that counts occurrences
    // 5. Sink: Outputs the results

    let vertices = vec![
        JobVertex {
            vertex_id: "source".to_string(),
            name: "WordSource".to_string(),
            operator_type: OperatorType::Source as i32,
            operator_config: vec![],
            parallelism,
        },
        JobVertex {
            vertex_id: "map".to_string(),
            name: "WordMapper".to_string(),
            operator_type: OperatorType::Map as i32,
            operator_config: vec![],
            parallelism,
        },
        JobVertex {
            vertex_id: "window".to_string(),
            name: "CountWindow".to_string(),
            operator_type: OperatorType::Window as i32,
            operator_config: vec![],
            parallelism,
        },
        JobVertex {
            vertex_id: "sink".to_string(),
            name: "ResultSink".to_string(),
            operator_type: OperatorType::Sink as i32,
            operator_config: vec![],
            parallelism: 1, // Single sink
        },
    ];

    let edges = vec![
        JobEdge {
            source_vertex_id: "source".to_string(),
            target_vertex_id: "map".to_string(),
            partition_strategy: PartitionStrategy::PartitionForward as i32,
        },
        JobEdge {
            source_vertex_id: "map".to_string(),
            target_vertex_id: "window".to_string(),
            partition_strategy: PartitionStrategy::PartitionHash as i32,
        },
        JobEdge {
            source_vertex_id: "window".to_string(),
            target_vertex_id: "sink".to_string(),
            partition_strategy: PartitionStrategy::PartitionRebalance as i32,
        },
    ];

    JobGraph { vertices, edges }
}

async fn cmd_submit(
    client: &mut ControlPlaneClient<Channel>,
    name: String,
    parallelism: i32,
    checkpoint_interval: i64,
) -> Result<String> {
    let job_graph = create_demo_job_graph(parallelism);

    let config = JobConfig {
        checkpoint_interval_ms: checkpoint_interval,
        max_parallelism: parallelism * 2,
        restart_strategy: Some(RestartStrategy {
            r#type: RestartStrategyType::RestartStrategyFixedDelay as i32,
            max_attempts: 3,
            delay_ms: 5000,
        }),
        properties: Default::default(),
    };

    let request = SubmitJobRequest {
        job_name: name.clone(),
        job_graph: Some(job_graph),
        config: Some(config),
    };

    info!(name = %name, parallelism = parallelism, "Submitting job");

    let response = client.submit_job(request).await?;
    let resp = response.into_inner();

    if resp.success {
        info!(job_id = %resp.job_id, "Job submitted successfully");
        println!("Job ID: {}", resp.job_id);
        println!("Message: {}", resp.message);
        Ok(resp.job_id)
    } else {
        error!(message = %resp.message, "Failed to submit job");
        anyhow::bail!("Failed to submit job: {}", resp.message)
    }
}

async fn cmd_status(client: &mut ControlPlaneClient<Channel>) -> Result<()> {
    let response = client.get_cluster_info(GetClusterInfoRequest {}).await?;
    let info = response.into_inner();

    println!("=== Cluster Status ===");
    println!("Workers:         {}/{} active", info.active_workers, info.total_workers);
    println!("Slots:           {}/{} available", info.available_slots, info.total_slots);
    println!("Running Jobs:    {}", info.running_jobs);
    println!("Total Tasks:     {}", info.total_tasks);
    println!("Uptime:          {}s", info.uptime_ms / 1000);

    Ok(())
}

async fn cmd_workers(client: &mut ControlPlaneClient<Channel>) -> Result<()> {
    let response = client.list_workers(ListWorkersRequest {}).await?;
    let workers = response.into_inner().workers;

    println!("=== Workers ({}) ===", workers.len());
    println!("{:<40} {:<15} {:<10} {:<15}", "ID", "Hostname", "Slots", "Status");
    println!("{}", "-".repeat(80));

    for w in workers {
        let status = match w.state {
            1 => "Registered",
            2 => "Active",
            3 => "Lost",
            _ => "Unknown",
        };
        println!(
            "{:<40} {:<15} {}/{}        {:<15}",
            w.worker_id, w.hostname, w.slots_used, w.slots, status
        );

        if !w.running_tasks.is_empty() {
            println!("  Tasks: {}", w.running_tasks.join(", "));
        }
    }

    Ok(())
}

async fn cmd_jobs(client: &mut ControlPlaneClient<Channel>) -> Result<()> {
    let response = client.list_jobs(ListJobsRequest { states: vec![] }).await?;
    let jobs = response.into_inner().jobs;

    println!("=== Jobs ({}) ===", jobs.len());
    println!("{:<40} {:<20} {:<15} {:<10}", "ID", "Name", "State", "Tasks");
    println!("{}", "-".repeat(85));

    for j in jobs {
        let state = JobState::try_from(j.state)
            .map(|s| format!("{:?}", s))
            .unwrap_or_else(|_| "Unknown".to_string());
        println!(
            "{:<40} {:<20} {:<15} {}/{}",
            j.job_id, j.name, state, j.tasks_running, j.tasks_total
        );
    }

    Ok(())
}

async fn cmd_job(client: &mut ControlPlaneClient<Channel>, job_id: String) -> Result<()> {
    let response = client
        .get_job_status(GetJobStatusRequest {
            job_id: job_id.clone(),
        })
        .await?;
    let job = response.into_inner();

    let state = JobState::try_from(job.state)
        .map(|s| format!("{:?}", s))
        .unwrap_or_else(|_| "Unknown".to_string());

    println!("=== Job: {} ===", job_id);
    println!("State:           {}", state);
    println!("Start Time:      {}ms ago", job.start_time);

    if let Some(metrics) = job.metrics {
        println!("Records In:      {}", metrics.records_in);
        println!("Records Out:     {}", metrics.records_out);
        println!("Bytes In:        {}", metrics.bytes_in);
        println!("Bytes Out:       {}", metrics.bytes_out);
        println!("Last Checkpoint: {}", metrics.last_checkpoint_id);
    }

    println!("\n=== Tasks ({}) ===", job.task_statuses.len());
    for task in job.task_statuses {
        let task_state = bicycle_protocol::control::TaskState::try_from(task.state)
            .map(|s| format!("{:?}", s))
            .unwrap_or_else(|_| "Unknown".to_string());
        println!(
            "  {} - {} (records: {}, bytes: {})",
            task.task_id, task_state, task.records_processed, task.bytes_processed
        );
    }

    Ok(())
}

async fn cmd_metrics(client: &mut ControlPlaneClient<Channel>) -> Result<()> {
    let response = client.get_metrics(GetMetricsRequest {}).await?;
    let m = response.into_inner();

    println!("=== Cluster Metrics ===");
    println!("Uptime:              {}s", m.uptime_seconds);
    println!("Records Processed:   {}", m.total_records_processed);
    println!("Bytes Processed:     {}", m.total_bytes_processed);
    println!("Checkpoints OK:      {}", m.checkpoints_completed);
    println!("Checkpoints Failed:  {}", m.checkpoints_failed);
    println!("Throughput:          {:.2} records/s", m.throughput_records_per_sec);
    println!("                     {:.2} bytes/s", m.throughput_bytes_per_sec);

    if !m.job_records.is_empty() {
        println!("\n=== Per-Job Records ===");
        for (job_id, records) in m.job_records {
            println!("  {}: {} records", job_id, records);
        }
    }

    Ok(())
}

async fn cmd_demo(client: &mut ControlPlaneClient<Channel>, duration: u64) -> Result<()> {
    println!("=== Bicycle Cluster Demo ===\n");

    // Show initial cluster status
    println!("1. Checking cluster status...\n");
    cmd_status(client).await?;
    println!();

    // Show workers
    println!("2. Listing workers...\n");
    cmd_workers(client).await?;
    println!();

    // Submit a demo job
    println!("3. Submitting demo job...\n");
    let job_id = cmd_submit(client, "demo-wordcount".to_string(), 2, 10000).await?;
    println!();

    // Monitor the job
    println!("4. Monitoring job for {}s...\n", duration);
    let start = std::time::Instant::now();
    let mut last_print = std::time::Instant::now();

    while start.elapsed().as_secs() < duration {
        tokio::time::sleep(Duration::from_secs(2)).await;

        if last_print.elapsed().as_secs() >= 5 {
            // Get job status
            let response = client
                .get_job_status(GetJobStatusRequest {
                    job_id: job_id.clone(),
                })
                .await?;
            let job = response.into_inner();

            let state = JobState::try_from(job.state)
                .map(|s| format!("{:?}", s))
                .unwrap_or_else(|_| "Unknown".to_string());

            let metrics = job.metrics.unwrap_or_default();
            println!(
                "[{:>3}s] Job {} - State: {}, Tasks: {}, Records: in={} out={}",
                start.elapsed().as_secs(),
                &job_id[..8],
                state,
                job.task_statuses.len(),
                metrics.records_in,
                metrics.records_out
            );

            last_print = std::time::Instant::now();
        }
    }

    println!();

    // Final status
    println!("5. Final cluster status...\n");
    cmd_status(client).await?;
    println!();
    cmd_metrics(client).await?;

    println!("\n=== Demo Complete ===");

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse()?))
        .init();

    let args = Args::parse();
    let mut client = get_client(&args.jobmanager).await?;

    match args.command {
        Command::Submit {
            name,
            parallelism,
            checkpoint_interval,
        } => {
            cmd_submit(&mut client, name, parallelism, checkpoint_interval).await?;
        }
        Command::Status => {
            cmd_status(&mut client).await?;
        }
        Command::Workers => {
            cmd_workers(&mut client).await?;
        }
        Command::Jobs => {
            cmd_jobs(&mut client).await?;
        }
        Command::Job { job_id } => {
            cmd_job(&mut client, job_id).await?;
        }
        Command::Metrics => {
            cmd_metrics(&mut client).await?;
        }
        Command::Demo { duration } => {
            cmd_demo(&mut client, duration).await?;
        }
    }

    Ok(())
}
