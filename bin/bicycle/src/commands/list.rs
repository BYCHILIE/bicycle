//! List command - List all jobs in the cluster.

use anyhow::Result;
use bicycle_protocol::control::{control_plane_client::ControlPlaneClient, ListJobsRequest};
use tonic::transport::Channel;

// Job state constants (from proto)
const JOB_STATE_CREATED: i32 = 1;
const JOB_STATE_RUNNING: i32 = 2;
const JOB_STATE_FAILING: i32 = 3;
const JOB_STATE_FAILED: i32 = 4;
const JOB_STATE_CANCELING: i32 = 5;
const JOB_STATE_CANCELED: i32 = 6;
const JOB_STATE_FINISHED: i32 = 7;
const JOB_STATE_RESTARTING: i32 = 8;
const JOB_STATE_SUSPENDED: i32 = 9;

pub async fn execute(
    client: &mut ControlPlaneClient<Channel>,
    state: Option<String>,
    format: String,
) -> Result<()> {
    // Parse state filter
    let states = match state.as_deref() {
        Some("running") => vec![JOB_STATE_RUNNING],
        Some("finished") => vec![JOB_STATE_FINISHED],
        Some("failed") => vec![JOB_STATE_FAILED],
        Some("cancelled" | "canceled") => vec![JOB_STATE_CANCELED],
        Some("created") => vec![JOB_STATE_CREATED],
        Some(s) => {
            eprintln!(
                "Unknown state filter: {}. Use: running, finished, failed, cancelled, created",
                s
            );
            vec![]
        }
        None => vec![],
    };

    let response = client.list_jobs(ListJobsRequest { states }).await?;
    let jobs = response.into_inner().jobs;

    if format == "json" {
        print_json(&jobs)?;
    } else {
        print_table(&jobs);
    }

    Ok(())
}

fn state_to_string(state: i32) -> &'static str {
    match state {
        JOB_STATE_CREATED => "Created",
        JOB_STATE_RUNNING => "Running",
        JOB_STATE_FAILING => "Failing",
        JOB_STATE_FAILED => "Failed",
        JOB_STATE_CANCELING => "Canceling",
        JOB_STATE_CANCELED => "Canceled",
        JOB_STATE_FINISHED => "Finished",
        JOB_STATE_RESTARTING => "Restarting",
        JOB_STATE_SUSPENDED => "Suspended",
        _ => "Unknown",
    }
}

fn print_table(jobs: &[bicycle_protocol::control::JobSummary]) {
    println!("Jobs ({})", jobs.len());
    println!("{}", "=".repeat(95));
    println!(
        "{:<25} {:<12} {:<12} {:<12} {}",
        "Name", "State", "Tasks", "Running", "ID"
    );
    println!("{}", "-".repeat(95));

    for j in jobs {
        println!(
            "{:<25} {:<12} {:<12} {:<12} {}",
            &j.name[..j.name.len().min(25)],
            state_to_string(j.state),
            j.tasks_total,
            j.tasks_running,
            &j.job_id,
        );
    }
}

fn print_json(jobs: &[bicycle_protocol::control::JobSummary]) -> Result<()> {
    let json: Vec<serde_json::Value> = jobs
        .iter()
        .map(|j| {
            serde_json::json!({
                "job_id": j.job_id,
                "name": j.name,
                "state": state_to_string(j.state),
                "start_time": j.start_time,
                "tasks_total": j.tasks_total,
                "tasks_running": j.tasks_running,
            })
        })
        .collect();

    println!("{}", serde_json::to_string_pretty(&json)?);
    Ok(())
}
