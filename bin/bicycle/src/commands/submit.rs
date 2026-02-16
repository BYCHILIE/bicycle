//! Submit command - Submit a Docker-based job to the cluster.
//!
//! This command runs a job executable (either directly or via Docker) to extract
//! its JobGraph, then submits it to the cluster.

use anyhow::{Context, Result};
use bicycle_protocol::control::{
    control_plane_client::ControlPlaneClient, ConnectorConfig, JobConfig, JobEdge, JobGraph,
    JobVertex, RestartStrategy, RestartStrategyType, SubmitJobRequest,
};
use serde::Deserialize;
use std::collections::HashMap;
use std::process::Command;
use tonic::transport::Channel;

/// Job graph from API crate serialization
#[derive(Debug, Deserialize)]
pub struct ApiJobGraph {
    pub name: String,
    pub vertices: Vec<ApiVertex>,
    pub edges: Vec<ApiEdge>,
    #[serde(default)]
    pub config: ApiJobConfig,
}

#[derive(Debug, Deserialize)]
pub struct ApiVertex {
    pub id: String,
    #[serde(default)]
    pub uid: Option<String>,
    pub name: String,
    pub operator_type: ApiOperatorType,
    pub parallelism: u32,
    #[serde(default)]
    pub max_parallelism: Option<u32>,
    #[serde(default = "default_slot_sharing_group")]
    pub slot_sharing_group: String,
    #[serde(default = "default_chaining_enabled")]
    pub chaining_enabled: bool,
    pub plugin_function: Option<String>,
    #[serde(default)]
    pub config: Vec<u8>,
}

fn default_slot_sharing_group() -> String { "default".to_string() }
fn default_chaining_enabled() -> bool { true }

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum ApiOperatorType {
    Source { Source: ApiSourceOp },
    Sink { Sink: ApiSinkOp },
    Process { Process: ApiProcessOp },
    KeyBy { KeyBy: ApiKeyByOp },
    Map,
    Filter,
    FlatMap,
    Window { Window: ApiWindowOp },
    Count,
    Reduce,
    Union,
    Connect,
    CoProcess { CoProcess: ApiCoProcessOp },
    SideOutput { SideOutput: ApiSideOutputOp },
    AsyncIO { AsyncIO: ApiAsyncIOOp },
}

#[derive(Debug, Deserialize)]
pub struct ApiSourceOp {
    pub connector: ApiConnectorConfig,
}

#[derive(Debug, Deserialize)]
pub struct ApiSinkOp {
    pub connector: ApiConnectorConfig,
}

#[derive(Debug, Deserialize)]
pub struct ApiProcessOp {
    pub is_rich: bool,
}

#[derive(Debug, Deserialize)]
pub struct ApiKeyByOp {
    pub key_selector: String,
}

#[derive(Debug, Deserialize)]
pub struct ApiWindowOp {
    pub window_type: serde_json::Value,
}

#[derive(Debug, Deserialize)]
pub struct ApiCoProcessOp {
    pub is_rich: bool,
}

#[derive(Debug, Deserialize)]
pub struct ApiSideOutputOp {
    pub tag: String,
}

#[derive(Debug, Deserialize)]
pub struct ApiAsyncIOOp {
    pub ordered: bool,
    pub timeout_ms: u64,
    pub capacity: usize,
}

#[derive(Debug, Deserialize)]
pub struct ApiConnectorConfig {
    pub connector_type: String,
    pub properties: HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
pub struct ApiEdge {
    pub source_id: String,
    pub target_id: String,
    pub partition_strategy: String,
}

#[derive(Debug, Deserialize, Default)]
pub struct ApiJobConfig {
    #[serde(default = "default_parallelism")]
    pub parallelism: u32,
    #[serde(default = "default_checkpoint_interval")]
    pub checkpoint_interval_ms: u64,
    #[serde(default = "default_max_parallelism")]
    pub max_parallelism: u32,
    #[serde(default)]
    pub restart_strategy: Option<ApiRestartStrategy>,
    #[serde(default)]
    pub properties: HashMap<String, String>,
}

fn default_parallelism() -> u32 { 1 }

#[derive(Debug, Deserialize)]
pub struct ApiRestartStrategy {
    pub strategy_type: String,
    #[serde(default = "default_max_attempts")]
    pub max_attempts: i32,
    #[serde(default = "default_delay")]
    pub delay_ms: i64,
}

fn default_checkpoint_interval() -> u64 { 60000 }
fn default_max_parallelism() -> u32 { 128 }
fn default_max_attempts() -> i32 { 3 }
fn default_delay() -> i64 { 5000 }

/// Convert API operator type to protocol enum value
fn operator_type_to_proto(op: &ApiOperatorType) -> i32 {
    match op {
        ApiOperatorType::Source { .. } => 1,
        ApiOperatorType::Map => 2,
        ApiOperatorType::Filter => 3,
        ApiOperatorType::FlatMap => 4,
        ApiOperatorType::KeyBy { .. } => 5,
        ApiOperatorType::Window { .. } => 6,
        ApiOperatorType::Reduce => 7,
        ApiOperatorType::Sink { .. } => 8,
        ApiOperatorType::Count => 10,
        ApiOperatorType::Process { .. } => 11,
        ApiOperatorType::Union => 12,
        ApiOperatorType::Connect => 13,
        ApiOperatorType::CoProcess { .. } => 14,
        ApiOperatorType::SideOutput { .. } => 15,
        ApiOperatorType::AsyncIO { .. } => 16,
    }
}

/// Convert connector type string to protocol enum value
fn connector_type_to_proto(s: &str) -> i32 {
    match s {
        "Socket" => 1,
        "Kafka" => 2,
        "Generator" => 3,
        "Console" => 4,
        "File" => 5,
        _ => 0,
    }
}

/// Convert partition strategy string to protocol enum value
fn partition_strategy_to_proto(s: &str) -> i32 {
    match s {
        "Forward" => 0,
        "Rebalance" => 1,
        "Hash" => 2,
        "Broadcast" => 3,
        _ => 0,
    }
}

/// Convert API JobGraph to protocol JobGraph
fn to_proto_job_graph(api: &ApiJobGraph) -> JobGraph {
    let vertices = api.vertices.iter().map(|v| {
        let connector_config = match &v.operator_type {
            ApiOperatorType::Source { Source: op } => Some(ConnectorConfig {
                connector_type: connector_type_to_proto(&op.connector.connector_type),
                properties: op.connector.properties.clone(),
            }),
            ApiOperatorType::Sink { Sink: op } => Some(ConnectorConfig {
                connector_type: connector_type_to_proto(&op.connector.connector_type),
                properties: op.connector.properties.clone(),
            }),
            _ => None,
        };

        // Determine if this is a rich function (for Process operators)
        let (plugin_function, is_rich_function) = match &v.operator_type {
            ApiOperatorType::Process { Process: op } => {
                (v.plugin_function.clone().unwrap_or_default(), op.is_rich)
            }
            _ => (String::new(), false),
        };

        JobVertex {
            vertex_id: v.id.clone(),
            name: v.name.clone(),
            operator_type: operator_type_to_proto(&v.operator_type),
            operator_config: v.config.clone(),
            parallelism: v.parallelism as i32,
            connector_config,
            plugin_function,
            is_rich_function,
            uid: v.uid.clone().unwrap_or_default(),
        }
    }).collect();

    let edges = api.edges.iter().map(|e| JobEdge {
        source_vertex_id: e.source_id.clone(),
        target_vertex_id: e.target_id.clone(),
        partition_strategy: partition_strategy_to_proto(&e.partition_strategy),
    }).collect();

    JobGraph { vertices, edges }
}

/// Run a job executable (binary or Docker image) and capture its JobGraph output
fn run_job_executable(executable: &str, use_docker: bool, docker_image: Option<&str>) -> Result<ApiJobGraph> {
    let output = if use_docker {
        let image = docker_image.unwrap_or(executable);
        println!("Running Docker container: {}", image);

        Command::new("docker")
            .args(["run", "--rm", image])
            .output()
            .with_context(|| format!("Failed to run Docker container: {}", image))?
    } else {
        println!("Running executable: {}", executable);

        Command::new(executable)
            .output()
            .with_context(|| format!("Failed to run executable: {}", executable))?
    };

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("Job executable failed: {}", stderr);
    }

    let stdout = String::from_utf8_lossy(&output.stdout);

    // Parse the JSON output
    let job_graph: ApiJobGraph = serde_json::from_str(&stdout)
        .with_context(|| "Failed to parse job graph JSON from executable output")?;

    Ok(job_graph)
}

pub async fn execute(
    client: &mut ControlPlaneClient<Channel>,
    executable: String,
    name_override: Option<String>,
    parallelism_override: Option<i32>,
    docker: bool,
    image: Option<String>,
    plugin: Option<String>,
) -> Result<()> {
    // Run the job executable to get the JobGraph
    let api_job = run_job_executable(&executable, docker, image.as_deref())?;

    let job_name = name_override.unwrap_or_else(|| api_job.name.clone());
    let job_graph = to_proto_job_graph(&api_job);

    // Apply parallelism override if specified
    let final_graph = if let Some(p) = parallelism_override {
        JobGraph {
            vertices: job_graph.vertices.into_iter().map(|mut v| {
                v.parallelism = p;
                v
            }).collect(),
            edges: job_graph.edges,
        }
    } else {
        job_graph
    };

    let max_parallelism = api_job.config.max_parallelism as i32;
    let restart = api_job.config.restart_strategy.as_ref();

    let config = JobConfig {
        checkpoint_interval_ms: api_job.config.checkpoint_interval_ms as i64,
        max_parallelism,
        restart_strategy: Some(RestartStrategy {
            r#type: RestartStrategyType::RestartStrategyFixedDelay as i32,
            max_attempts: restart.map(|r| r.max_attempts).unwrap_or(3),
            delay_ms: restart.map(|r| r.delay_ms).unwrap_or(5000),
        }),
        properties: api_job.config.properties.clone(),
    };

    // Load plugin bytes if specified
    let (plugin_module, plugin_type) = if let Some(plugin_path) = plugin {
        println!("Loading plugin: {}", plugin_path);
        let plugin_bytes = std::fs::read(&plugin_path)
            .with_context(|| format!("Failed to read plugin file: {}", plugin_path))?;
        println!("  Plugin size: {} bytes", plugin_bytes.len());
        (plugin_bytes, "native".to_string())
    } else {
        (Vec::new(), String::new())
    };

    let request = SubmitJobRequest {
        job_name: job_name.clone(),
        job_graph: Some(final_graph),
        config: Some(config),
        plugin_module,
        plugin_type,
    };

    println!();
    println!("Submitting job '{}'...", job_name);

    // Print pipeline
    let pipeline: Vec<String> = api_job.vertices.iter().map(|v| v.name.clone()).collect();
    println!("Pipeline: {}", pipeline.join(" -> "));
    println!();

    let response = client.submit_job(request).await?;
    let resp = response.into_inner();

    if resp.success {
        println!("Job '{}' submitted successfully!", job_name);
        println!();
        println!("  Name: {}", job_name);
        println!("  ID:   {}", resp.job_id);
        println!();

        // Print connector info
        for v in &api_job.vertices {
            match &v.operator_type {
                ApiOperatorType::Source { Source: op } if op.connector.connector_type == "Socket" => {
                    if let Some(port) = op.connector.properties.get("port") {
                        let host = op.connector.properties.get("host").map(|s| s.as_str()).unwrap_or("localhost");
                        println!("  Input:  nc {} {}", host, port);
                    }
                }
                ApiOperatorType::Sink { Sink: op } if op.connector.connector_type == "Socket" => {
                    if let Some(port) = op.connector.properties.get("port") {
                        let host = op.connector.properties.get("host").map(|s| s.as_str()).unwrap_or("localhost");
                        println!("  Output: nc {} {}", host, port);
                    }
                }
                _ => {}
            }
        }
        println!();
        println!("Use 'bicycle info {}' to monitor the job.", resp.job_id);
    } else {
        eprintln!("Failed to submit job: {}", resp.message);
        std::process::exit(1);
    }

    Ok(())
}
