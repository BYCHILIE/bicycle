// Job types
export type JobState = 'Created' | 'Running' | 'Finished' | 'Failed' | 'Canceling' | 'Canceled'

export interface JobSummary {
  job_id: string
  name: string
  state: JobState
  start_time: number
  tasks_total: number
  tasks_running: number
}

export interface JobMetrics {
  records_in: number
  records_out: number
  bytes_in: number
  bytes_out: number
}

export interface JobDetail {
  job_id: string
  name: string
  state: JobState
  start_time: number
  end_time: number | null
  metrics: JobMetrics
}

// Task types
export type TaskState = 'Created' | 'Deploying' | 'Running' | 'Finished' | 'Failed' | 'Canceling' | 'Canceled'

export interface TaskSummary {
  task_id: string
  operator_name: string
  state: TaskState
  records_processed: number
  bytes_processed: number
}

// Checkpoint types
export interface CheckpointInfo {
  checkpoint_id: number
  timestamp: number
  duration_ms: number
  state_size_bytes: number
}

// Cluster types
export interface ClusterInfo {
  workers: number
  total_slots: number
  available_slots: number
  running_jobs: number
}

// Worker types
export interface WorkerInfo {
  worker_id: string
  hostname: string
  slots: number
  slots_used: number
  cpu_usage: number
  memory_used_mb: number
}

// Metrics types
export interface MetricsResponse {
  total_records_processed: number
  total_bytes_processed: number
  checkpoints_completed: number
  uptime_seconds: number
}

export interface MetricsHistoryPoint {
  timestamp: number
  records_per_sec: number
  bytes_per_sec: number
}

export interface MetricsHistory {
  points: MetricsHistoryPoint[]
}

// Job graph types
export type OperatorType = 'Source' | 'Sink' | 'Map' | 'Filter' | 'KeyBy' | 'Window' | 'Join' | 'Union' | 'Connect' | 'CoProcess' | 'SideOutput' | 'AsyncIO' | 'Process' | 'Reduce' | 'Count' | 'Unknown'
export type PartitionStrategy = 'Forward' | 'Hash' | 'Rebalance' | 'Broadcast'

export interface JobVertex {
  id: string
  uid: string
  name: string
  operator_type: OperatorType
  parallelism: number
  slot_sharing_group: string
  records_in: number
  records_out: number
  backpressure: number
}

export interface JobEdge {
  source_id: string
  target_id: string
  partition_strategy: PartitionStrategy
}

export interface JobGraph {
  vertices: JobVertex[]
  edges: JobEdge[]
}

// Exception types
export interface JobException {
  timestamp: number
  task_id: string
  task_index: number | null
  operator_name: string
  message: string
  stack_trace: string
  root_cause: string | null
  location: string | null
}

export interface JobExceptions {
  exceptions: JobException[]
  truncated: boolean
}
