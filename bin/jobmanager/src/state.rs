//! Job and worker state management for the JobManager.

use bicycle_core::{JobId, TaskId};
use bicycle_protocol::control::{
    JobConfig, JobEdge, JobGraph, JobState, JobVertex, OperatorType, PartitionStrategy, TaskState,
    WorkerMetrics, WorkerResources,
};
use dashmap::DashMap;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Information about a registered worker.
#[derive(Debug, Clone)]
pub struct WorkerInfo {
    pub worker_id: String,
    pub hostname: String,
    pub data_port: i32,
    pub total_slots: i32,
    pub used_slots: i32,
    pub resources: WorkerResources,
    pub last_heartbeat: Instant,
    pub metrics: Option<WorkerMetrics>,
    pub assigned_tasks: HashSet<String>,
}

impl WorkerInfo {
    pub fn new(
        worker_id: String,
        hostname: String,
        data_port: i32,
        available_slots: i32,
        resources: WorkerResources,
    ) -> Self {
        Self {
            worker_id,
            hostname,
            data_port,
            total_slots: available_slots,
            used_slots: 0,
            resources,
            last_heartbeat: Instant::now(),
            metrics: None,
            assigned_tasks: HashSet::new(),
        }
    }

    pub fn available_slots(&self) -> i32 {
        self.total_slots - self.used_slots
    }

    pub fn is_alive(&self, timeout: Duration) -> bool {
        self.last_heartbeat.elapsed() < timeout
    }

    pub fn update_heartbeat(&mut self, metrics: Option<WorkerMetrics>) {
        self.last_heartbeat = Instant::now();
        self.metrics = metrics;
    }
}

/// Information about a deployed task.
#[derive(Debug, Clone)]
pub struct TaskInfo {
    pub task_id: String,
    pub job_id: String,
    pub vertex_id: String,
    pub subtask_index: i32,
    pub operator_type: OperatorType,
    pub worker_id: String,
    pub state: TaskState,
    pub records_processed: i64,
    pub bytes_processed: i64,
    pub error_message: Option<String>,
    pub deployed_at: Instant,
}

impl TaskInfo {
    pub fn new(
        task_id: String,
        job_id: String,
        vertex_id: String,
        subtask_index: i32,
        operator_type: OperatorType,
        worker_id: String,
    ) -> Self {
        Self {
            task_id,
            job_id,
            vertex_id,
            subtask_index,
            operator_type,
            worker_id,
            state: TaskState::Created,
            records_processed: 0,
            bytes_processed: 0,
            error_message: None,
            deployed_at: Instant::now(),
        }
    }
}

/// Information about a job.
#[derive(Debug, Clone)]
pub struct JobInfo {
    pub job_id: String,
    pub name: String,
    pub graph: JobGraph,
    pub config: JobConfig,
    pub state: JobState,
    pub start_time: Instant,
    pub end_time: Option<Instant>,
    pub tasks: Vec<String>, // Task IDs
    pub records_in: i64,
    pub records_out: i64,
    pub bytes_in: i64,
    pub bytes_out: i64,
    pub last_checkpoint_id: i64,
    pub last_checkpoint_time: i64,
}

impl JobInfo {
    pub fn new(job_id: String, name: String, graph: JobGraph, config: JobConfig) -> Self {
        Self {
            job_id,
            name,
            graph,
            config,
            state: JobState::Created,
            start_time: Instant::now(),
            end_time: None,
            tasks: Vec::new(),
            records_in: 0,
            records_out: 0,
            bytes_in: 0,
            bytes_out: 0,
            last_checkpoint_id: 0,
            last_checkpoint_time: 0,
        }
    }
}

/// Physical task to be deployed (execution graph node).
#[derive(Debug, Clone)]
pub struct PhysicalTask {
    pub task_id: String,
    pub job_id: String,
    pub vertex_id: String,
    pub subtask_index: i32,
    pub parallelism: i32,
    pub operator_name: String,
    pub operator_type: OperatorType,
    pub operator_config: Vec<u8>,
    pub upstream_tasks: Vec<(String, PartitionStrategy)>, // (task_id, partition)
    pub downstream_tasks: Vec<(String, PartitionStrategy)>,
}

/// Global metrics for the cluster.
#[derive(Debug, Default)]
pub struct ClusterMetrics {
    pub total_records_processed: std::sync::atomic::AtomicI64,
    pub total_bytes_processed: std::sync::atomic::AtomicI64,
    pub checkpoints_completed: std::sync::atomic::AtomicI64,
    pub checkpoints_failed: std::sync::atomic::AtomicI64,
}

/// Central state store for the JobManager.
pub struct JobManagerState {
    /// Registered workers by ID
    pub workers: DashMap<String, WorkerInfo>,

    /// Jobs by ID
    pub jobs: DashMap<String, JobInfo>,

    /// Tasks by ID
    pub tasks: DashMap<String, TaskInfo>,

    /// Heartbeat timeout for workers
    pub worker_timeout: Duration,

    /// Heartbeat interval to tell workers
    pub heartbeat_interval: Duration,

    /// Start time of the JobManager
    pub start_time: Instant,

    /// Global cluster metrics
    pub metrics: ClusterMetrics,
}

impl JobManagerState {
    pub fn new() -> Self {
        Self {
            workers: DashMap::new(),
            jobs: DashMap::new(),
            tasks: DashMap::new(),
            worker_timeout: Duration::from_secs(30),
            heartbeat_interval: Duration::from_secs(5),
            start_time: Instant::now(),
            metrics: ClusterMetrics::default(),
        }
    }

    /// Get cluster summary info.
    pub fn get_cluster_info(&self) -> (i32, i32, i32, i32, i32, i32) {
        let active_workers = self
            .workers
            .iter()
            .filter(|w| w.is_alive(self.worker_timeout))
            .count() as i32;

        let total_workers = self.workers.len() as i32;

        let total_slots: i32 = self.workers.iter().map(|w| w.total_slots).sum();

        let used_slots: i32 = self.workers.iter().map(|w| w.used_slots).sum();

        let running_jobs = self
            .jobs
            .iter()
            .filter(|j| j.state == JobState::Running)
            .count() as i32;

        let total_tasks = self.tasks.len() as i32;

        (
            total_workers,
            active_workers,
            total_slots,
            total_slots - used_slots,
            running_jobs,
            total_tasks,
        )
    }

    /// Get uptime in milliseconds.
    pub fn uptime_ms(&self) -> i64 {
        self.start_time.elapsed().as_millis() as i64
    }

    /// Register a new worker.
    pub fn register_worker(&self, info: WorkerInfo) -> bool {
        let worker_id = info.worker_id.clone();
        self.workers.insert(worker_id, info);
        true
    }

    /// Unregister a worker and mark its tasks as failed.
    pub fn unregister_worker(&self, worker_id: &str) -> Vec<String> {
        let mut failed_tasks = Vec::new();

        if let Some((_, worker)) = self.workers.remove(worker_id) {
            for task_id in worker.assigned_tasks {
                if let Some(mut task) = self.tasks.get_mut(&task_id) {
                    task.state = TaskState::Failed;
                    task.error_message = Some("Worker disconnected".to_string());
                    failed_tasks.push(task_id);
                }
            }
        }

        failed_tasks
    }

    /// Get available slots across all workers.
    pub fn total_available_slots(&self) -> i32 {
        self.workers
            .iter()
            .filter(|w| w.is_alive(self.worker_timeout))
            .map(|w| w.available_slots())
            .sum()
    }

    /// Find a worker with available slots.
    pub fn find_worker_with_slots(&self) -> Option<String> {
        self.workers
            .iter()
            .filter(|w| w.is_alive(self.worker_timeout) && w.available_slots() > 0)
            .max_by_key(|w| w.available_slots())
            .map(|w| w.worker_id.clone())
    }

    /// Allocate a slot on a worker.
    pub fn allocate_slot(&self, worker_id: &str, task_id: &str) -> bool {
        if let Some(mut worker) = self.workers.get_mut(worker_id) {
            if worker.available_slots() > 0 {
                worker.used_slots += 1;
                worker.assigned_tasks.insert(task_id.to_string());
                return true;
            }
        }
        false
    }

    /// Release a slot on a worker.
    pub fn release_slot(&self, worker_id: &str, task_id: &str) {
        if let Some(mut worker) = self.workers.get_mut(worker_id) {
            if worker.used_slots > 0 {
                worker.used_slots -= 1;
            }
            worker.assigned_tasks.remove(task_id);
        }
    }

    /// Get dead workers that haven't sent heartbeat.
    pub fn get_dead_workers(&self) -> Vec<String> {
        self.workers
            .iter()
            .filter(|w| !w.is_alive(self.worker_timeout))
            .map(|w| w.worker_id.clone())
            .collect()
    }

    /// Create physical tasks from a job graph.
    pub fn create_physical_tasks(&self, job_id: &str, graph: &JobGraph) -> Vec<PhysicalTask> {
        let mut tasks = Vec::new();

        // Build vertex map for lookups
        let vertex_map: HashMap<&str, &JobVertex> = graph
            .vertices
            .iter()
            .map(|v| (v.vertex_id.as_str(), v))
            .collect();

        // Build edge map (source -> targets)
        let mut downstream_edges: HashMap<&str, Vec<&JobEdge>> = HashMap::new();
        let mut upstream_edges: HashMap<&str, Vec<&JobEdge>> = HashMap::new();

        for edge in &graph.edges {
            downstream_edges
                .entry(edge.source_vertex_id.as_str())
                .or_default()
                .push(edge);
            upstream_edges
                .entry(edge.target_vertex_id.as_str())
                .or_default()
                .push(edge);
        }

        // Create tasks for each vertex
        for vertex in &graph.vertices {
            let parallelism = vertex.parallelism.max(1);

            for subtask_idx in 0..parallelism {
                let task_id = format!("{}-{}-{}", job_id, vertex.vertex_id, subtask_idx);

                // Determine upstream connections
                let upstream_tasks: Vec<(String, PartitionStrategy)> = upstream_edges
                    .get(vertex.vertex_id.as_str())
                    .map(|edges| {
                        edges
                            .iter()
                            .flat_map(|edge| {
                                let source_vertex = vertex_map.get(edge.source_vertex_id.as_str());
                                let source_parallelism =
                                    source_vertex.map(|v| v.parallelism.max(1)).unwrap_or(1);

                                let strategy =
                                    PartitionStrategy::try_from(edge.partition_strategy)
                                        .unwrap_or(PartitionStrategy::PartitionForward);

                                // Determine which upstream subtasks connect to this one
                                match strategy {
                                    PartitionStrategy::PartitionForward => {
                                        if subtask_idx < source_parallelism {
                                            vec![(
                                                format!(
                                                    "{}-{}-{}",
                                                    job_id, edge.source_vertex_id, subtask_idx
                                                ),
                                                strategy,
                                            )]
                                        } else {
                                            vec![]
                                        }
                                    }
                                    PartitionStrategy::PartitionHash
                                    | PartitionStrategy::PartitionRebalance
                                    | PartitionStrategy::PartitionBroadcast => {
                                        // Connect to all upstream subtasks
                                        (0..source_parallelism)
                                            .map(|i| {
                                                (
                                                    format!(
                                                        "{}-{}-{}",
                                                        job_id, edge.source_vertex_id, i
                                                    ),
                                                    strategy,
                                                )
                                            })
                                            .collect()
                                    }
                                }
                            })
                            .collect()
                    })
                    .unwrap_or_default();

                // Determine downstream connections
                let downstream_tasks: Vec<(String, PartitionStrategy)> = downstream_edges
                    .get(vertex.vertex_id.as_str())
                    .map(|edges| {
                        edges
                            .iter()
                            .flat_map(|edge| {
                                let target_vertex = vertex_map.get(edge.target_vertex_id.as_str());
                                let target_parallelism =
                                    target_vertex.map(|v| v.parallelism.max(1)).unwrap_or(1);

                                let strategy =
                                    PartitionStrategy::try_from(edge.partition_strategy)
                                        .unwrap_or(PartitionStrategy::PartitionForward);

                                match strategy {
                                    PartitionStrategy::PartitionForward => {
                                        if subtask_idx < target_parallelism {
                                            vec![(
                                                format!(
                                                    "{}-{}-{}",
                                                    job_id, edge.target_vertex_id, subtask_idx
                                                ),
                                                strategy,
                                            )]
                                        } else {
                                            vec![]
                                        }
                                    }
                                    PartitionStrategy::PartitionHash
                                    | PartitionStrategy::PartitionRebalance
                                    | PartitionStrategy::PartitionBroadcast => {
                                        // Connect to all downstream subtasks
                                        (0..target_parallelism)
                                            .map(|i| {
                                                (
                                                    format!(
                                                        "{}-{}-{}",
                                                        job_id, edge.target_vertex_id, i
                                                    ),
                                                    strategy,
                                                )
                                            })
                                            .collect()
                                    }
                                }
                            })
                            .collect()
                    })
                    .unwrap_or_default();

                tasks.push(PhysicalTask {
                    task_id,
                    job_id: job_id.to_string(),
                    vertex_id: vertex.vertex_id.clone(),
                    subtask_index: subtask_idx,
                    parallelism,
                    operator_name: vertex.name.clone(),
                    operator_type: OperatorType::try_from(vertex.operator_type)
                        .unwrap_or(OperatorType::Unknown),
                    operator_config: vertex.operator_config.clone(),
                    upstream_tasks,
                    downstream_tasks,
                });
            }
        }

        tasks
    }
}

impl Default for JobManagerState {
    fn default() -> Self {
        Self::new()
    }
}
