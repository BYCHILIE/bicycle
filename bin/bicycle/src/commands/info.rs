//! Info command - Get detailed information about a job.

use anyhow::Result;
use bicycle_protocol::control::{control_plane_client::ControlPlaneClient, GetJobStatusRequest};
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

// Task state constants (from proto)
const TASK_STATE_CREATED: i32 = 1;
const TASK_STATE_DEPLOYING: i32 = 2;
const TASK_STATE_RUNNING: i32 = 3;
const TASK_STATE_FINISHED: i32 = 4;
const TASK_STATE_FAILED: i32 = 5;
const TASK_STATE_CANCELING: i32 = 6;
const TASK_STATE_CANCELED: i32 = 7;

pub async fn execute(
    client: &mut ControlPlaneClient<Channel>,
    job_id: String,
    format: String,
) -> Result<()> {
    let response = client
        .get_job_status(GetJobStatusRequest {
            job_id: job_id.clone(),
        })
        .await?;
    let job = response.into_inner();

    if format == "json" {
        print_json(&job)?;
    } else {
        print_table(&job);
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

fn task_state_to_string(state: i32) -> &'static str {
    match state {
        TASK_STATE_CREATED => "Created",
        TASK_STATE_DEPLOYING => "Deploying",
        TASK_STATE_RUNNING => "Running",
        TASK_STATE_FINISHED => "Finished",
        TASK_STATE_FAILED => "Failed",
        TASK_STATE_CANCELING => "Canceling",
        TASK_STATE_CANCELED => "Canceled",
        _ => "Unknown",
    }
}

fn print_table(job: &bicycle_protocol::control::GetJobStatusResponse) {
    let name = if job.job_name.is_empty() { &job.job_id } else { &job.job_name };
    println!("Job: {}", name);
    println!("{}", "=".repeat(60));
    println!();
    println!("Name:            {}", name);
    println!("ID:              {}", job.job_id);
    println!("State:           {}", state_to_string(job.state));
    println!("Start Time:      {}ms ago", job.start_time);
    if job.end_time > 0 {
        println!("End Time:        {}ms ago", job.end_time);
    }

    if let Some(metrics) = &job.metrics {
        println!();
        println!("Metrics");
        println!("{}", "-".repeat(40));
        println!("Records In:      {}", metrics.records_in);
        println!("Records Out:     {}", metrics.records_out);
        println!("Bytes In:        {}", metrics.bytes_in);
        println!("Bytes Out:       {}", metrics.bytes_out);
        println!("Last Checkpoint: {}", metrics.last_checkpoint_id);
    }

    if !job.task_statuses.is_empty() {
        println!();
        println!("Tasks ({})", job.task_statuses.len());
        println!("{}", "-".repeat(80));
        println!(
            "{:<25} {:<12} {:<10} {:<10} {}",
            "Operator", "State", "Records", "Bytes", "Task ID"
        );

        for task in &job.task_statuses {
            let display_name = if task.operator_name.is_empty() {
                &task.task_id
            } else {
                &task.operator_name
            };
            println!(
                "{:<25} {:<12} {:<10} {:<10} {}",
                &display_name[..display_name.len().min(25)],
                task_state_to_string(task.state),
                task.records_processed,
                task.bytes_processed,
                &task.task_id,
            );
            if !task.error_message.is_empty() {
                println!("  Error: {}", task.error_message);
            }
        }
    }
}

fn print_json(job: &bicycle_protocol::control::GetJobStatusResponse) -> Result<()> {
    let json = serde_json::json!({
        "job_id": job.job_id,
        "name": job.job_name,
        "state": state_to_string(job.state),
        "start_time": job.start_time,
        "end_time": job.end_time,
        "metrics": job.metrics.as_ref().map(|m| serde_json::json!({
            "records_in": m.records_in,
            "records_out": m.records_out,
            "bytes_in": m.bytes_in,
            "bytes_out": m.bytes_out,
            "last_checkpoint_id": m.last_checkpoint_id,
            "last_checkpoint_time": m.last_checkpoint_time,
        })),
        "tasks": job.task_statuses.iter().map(|t| serde_json::json!({
            "task_id": t.task_id,
            "operator_name": t.operator_name,
            "state": task_state_to_string(t.state),
            "records_processed": t.records_processed,
            "bytes_processed": t.bytes_processed,
            "error_message": t.error_message,
        })).collect::<Vec<_>>(),
    });

    println!("{}", serde_json::to_string_pretty(&json)?);
    Ok(())
}
