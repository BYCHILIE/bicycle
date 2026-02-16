//! Run command - Submit a job to the cluster from a definition file.

use anyhow::{Context, Result};
use bicycle_protocol::control::{
    control_plane_client::ControlPlaneClient, ConnectorConfig, JobConfig, JobEdge, JobGraph,
    JobVertex, RestartStrategy, RestartStrategyType, SubmitJobRequest,
};
use serde::Deserialize;
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use tonic::transport::Channel;

/// Job definition loaded from YAML file
#[derive(Debug, Deserialize)]
pub struct JobDefinition {
    pub name: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub config: JobConfigDef,
    pub vertices: Vec<VertexDef>,
    pub edges: Vec<EdgeDef>,
}

#[derive(Debug, Deserialize, Default)]
pub struct JobConfigDef {
    #[serde(default = "default_parallelism")]
    pub parallelism: i32,
    /// Maximum parallelism for rescaling. If not set, defaults to parallelism * 4.
    /// This determines the maximum number of key groups for state partitioning.
    #[serde(default)]
    pub max_parallelism: Option<i32>,
    #[serde(default = "default_checkpoint_interval")]
    pub checkpoint_interval_ms: i64,
    #[serde(default = "default_max_restarts")]
    pub max_restarts: i32,
    #[serde(default = "default_restart_delay")]
    pub restart_delay_ms: i64,
}

fn default_parallelism() -> i32 { 1 }
fn default_checkpoint_interval() -> i64 { 60000 }
fn default_max_restarts() -> i32 { 3 }
fn default_restart_delay() -> i64 { 5000 }

#[derive(Debug, Deserialize)]
pub struct VertexDef {
    pub id: String,
    pub name: String,
    pub operator: String,
    #[serde(default = "default_parallelism")]
    pub parallelism: i32,
    #[serde(default)]
    pub config: Option<serde_json::Value>,
    #[serde(default)]
    pub connector: Option<ConnectorDef>,
}

#[derive(Debug, Deserialize)]
pub struct ConnectorDef {
    #[serde(rename = "type")]
    pub connector_type: String,
    #[serde(default)]
    pub properties: HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
pub struct EdgeDef {
    pub from: String,
    pub to: String,
    #[serde(default = "default_partition")]
    pub partition: String,
}

fn default_partition() -> String { "forward".to_string() }

/// Convert operator string to proto enum value
fn operator_type_from_str(s: &str) -> i32 {
    match s.to_lowercase().as_str() {
        "source" => 1,
        "map" => 2,
        "filter" => 3,
        "flatmap" | "flat_map" => 4,
        "keyby" | "key_by" => 5,
        "window" => 6,
        "reduce" => 7,
        "sink" => 8,
        "join" => 9,
        "count" => 10,
        "process" => 11, // Custom function operator
        _ => 0, // Unknown
    }
}

/// Convert connector type string to proto enum value
fn connector_type_from_str(s: &str) -> i32 {
    match s.to_lowercase().as_str() {
        "socket" => 1,
        "kafka" => 2,
        "generator" => 3,
        "console" => 4,
        "file" => 5,
        _ => 0, // Unknown
    }
}

/// Convert partition strategy string to proto enum value
fn partition_strategy_from_str(s: &str) -> i32 {
    match s.to_lowercase().as_str() {
        "forward" => 0,
        "rebalance" => 1,
        "hash" => 2,
        "broadcast" => 3,
        _ => 0, // Default to forward
    }
}

/// Load job definition from YAML file
pub fn load_job_definition(path: &Path) -> Result<JobDefinition> {
    let content = fs::read_to_string(path)
        .with_context(|| format!("Failed to read job file: {}", path.display()))?;

    let job: JobDefinition = serde_yaml::from_str(&content)
        .with_context(|| format!("Failed to parse job file: {}", path.display()))?;

    Ok(job)
}

/// Convert job definition to protocol JobGraph
fn to_job_graph(def: &JobDefinition, parallelism_override: Option<i32>) -> JobGraph {
    let vertices = def.vertices.iter().map(|v| {
        let parallelism = parallelism_override.unwrap_or(v.parallelism);

        let connector_config = v.connector.as_ref().map(|c| ConnectorConfig {
            connector_type: connector_type_from_str(&c.connector_type),
            properties: c.properties.clone(),
        });

        let operator_config = v.config.as_ref()
            .map(|c| serde_json::to_vec(c).unwrap_or_default())
            .unwrap_or_default();

        // For process operators, use the vertex name as the function name
        let is_process = v.operator.to_lowercase() == "process";
        let plugin_function = if is_process {
            v.name.clone() // Function name from vertex name (e.g., "WordSplitter")
        } else {
            String::new()
        };

        JobVertex {
            vertex_id: v.id.clone(),
            name: v.name.clone(),
            operator_type: operator_type_from_str(&v.operator),
            operator_config,
            parallelism,
            connector_config,
            plugin_function,
            is_rich_function: is_process, // Process operators may be stateful
            uid: String::new(),
        }
    }).collect();

    let edges = def.edges.iter().map(|e| JobEdge {
        source_vertex_id: e.from.clone(),
        target_vertex_id: e.to.clone(),
        partition_strategy: partition_strategy_from_str(&e.partition),
    }).collect();

    JobGraph { vertices, edges }
}

pub async fn execute(
    client: &mut ControlPlaneClient<Channel>,
    job_file: String,
    name_override: Option<String>,
    parallelism_override: Option<i32>,
) -> Result<()> {
    let path = Path::new(&job_file);

    // Load job definition
    let job_def = load_job_definition(path)?;

    let job_name = name_override.unwrap_or_else(|| job_def.name.clone());
    let job_graph = to_job_graph(&job_def, parallelism_override);

    // Calculate effective parallelism and max_parallelism
    let effective_parallelism = parallelism_override.unwrap_or(job_def.config.parallelism);
    let effective_max_parallelism = job_def.config.max_parallelism
        .unwrap_or(effective_parallelism * 4)  // Default: 4x parallelism for rescaling headroom
        .max(effective_parallelism);  // max_parallelism must be >= parallelism

    let config = JobConfig {
        checkpoint_interval_ms: job_def.config.checkpoint_interval_ms,
        max_parallelism: effective_max_parallelism,
        restart_strategy: Some(RestartStrategy {
            r#type: RestartStrategyType::RestartStrategyFixedDelay as i32,
            max_attempts: job_def.config.max_restarts,
            delay_ms: job_def.config.restart_delay_ms,
        }),
        properties: Default::default(),
    };

    let request = SubmitJobRequest {
        job_name: job_name.clone(),
        job_graph: Some(job_graph),
        config: Some(config),
        plugin_module: Vec::new(),
        plugin_type: String::new(),
    };

    println!("Submitting job '{}' from {}...", job_name, job_file);
    if !job_def.description.is_empty() {
        println!("  {}", job_def.description);
    }
    println!();

    // Print pipeline
    let pipeline: Vec<String> = job_def.vertices.iter().map(|v| v.name.clone()).collect();
    println!("Pipeline: {}", pipeline.join(" -> "));
    println!();

    let response = client.submit_job(request).await?;
    let resp = response.into_inner();

    if resp.success {
        println!("Job submitted successfully!");
        println!();
        println!("  Job ID: {}", resp.job_id);
        println!("  Name:   {}", job_name);
        println!();

        // Print connector info if available
        for v in &job_def.vertices {
            if let Some(conn) = &v.connector {
                if conn.connector_type == "socket" {
                    if let Some(port) = conn.properties.get("port") {
                        let host = conn.properties.get("host").map(|s| s.as_str()).unwrap_or("localhost");
                        if v.operator == "source" {
                            println!("  Input:  nc {} {}", host, port);
                        } else if v.operator == "sink" {
                            println!("  Output: nc {} {}", host, port);
                        }
                    }
                }
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
