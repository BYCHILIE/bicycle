// =========================================================================
// Processing Operators
// =========================================================================

use anyhow::Result;
use bicycle_core::StreamMessage;
use bicycle_protocol::control::TaskDescriptor;
use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::info;

use super::config::{FlatMapConfig, FilterConfig, MapConfig, WindowConfig};
use super::{TaskExecutor, TaskStatus};

impl TaskExecutor {
    /// Map operator - transforms each element
    pub(super) async fn run_map_operator(
        task_id: &str,
        descriptor: &TaskDescriptor,
        status: Arc<TaskStatus>,
        internal_channels: Arc<DashMap<String, mpsc::Sender<StreamMessage<Vec<u8>>>>>,
        internal_receivers: Arc<DashMap<String, Arc<tokio::sync::Mutex<mpsc::Receiver<StreamMessage<Vec<u8>>>>>>>,
    ) -> Result<()> {
        // Parse map configuration (what transformation to apply)
        let config: MapConfig = if descriptor.operator_config.is_empty() {
            MapConfig::default()
        } else {
            serde_json::from_slice(&descriptor.operator_config).unwrap_or_default()
        };

        info!(
            task_id = %task_id,
            function = %config.function,
            "Starting map operator"
        );

        let downstream_tasks: Vec<String> = descriptor
            .output_gates
            .iter()
            .map(|g| g.downstream_task_id.clone())
            .collect();

        // Get input receiver
        let rx_arc = internal_receivers
            .get(task_id)
            .ok_or_else(|| anyhow::anyhow!("No receiver for task {}", task_id))?
            .clone();

        loop {
            if status.is_cancelled() {
                return Ok(());
            }

            let mut rx = rx_arc.lock().await;
            match tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
                Ok(Some(StreamMessage::Data(bytes))) => {
                    drop(rx);
                    let input = String::from_utf8_lossy(&bytes);
                    status.increment_records(1);
                    status.increment_bytes(bytes.len() as i64);

                    // Apply transformation based on function
                    let output = match config.function.as_str() {
                        "uppercase" => input.to_uppercase(),
                        "lowercase" => input.to_lowercase(),
                        "word_count" => {
                            let count = input.split_whitespace().count();
                            format!("{} -> {} words", input, count)
                        }
                        "reverse" => input.chars().rev().collect(),
                        _ => input.to_string(), // identity
                    };

                    let output_bytes = output.as_bytes().to_vec();

                    // Forward to downstream
                    Self::send_to_downstream(output_bytes, &downstream_tasks, &internal_channels).await;
                }
                Ok(Some(msg @ StreamMessage::Barrier(_))) | Ok(Some(msg @ StreamMessage::Watermark(_))) => {
                    drop(rx);
                    Self::send_control_downstream(msg, &downstream_tasks, &internal_channels).await;
                }
                Ok(Some(_)) => {}
                Ok(None) => {
                    drop(rx);
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(_) => {}
            }
        }
    }

    /// FlatMap operator - transforms each element into zero or more elements
    pub(super) async fn run_flatmap_operator(
        task_id: &str,
        descriptor: &TaskDescriptor,
        status: Arc<TaskStatus>,
        internal_channels: Arc<DashMap<String, mpsc::Sender<StreamMessage<Vec<u8>>>>>,
        internal_receivers: Arc<DashMap<String, Arc<tokio::sync::Mutex<mpsc::Receiver<StreamMessage<Vec<u8>>>>>>>,
    ) -> Result<()> {
        let config: FlatMapConfig = if descriptor.operator_config.is_empty() {
            FlatMapConfig::default()
        } else {
            serde_json::from_slice(&descriptor.operator_config).unwrap_or_default()
        };

        info!(
            task_id = %task_id,
            function = %config.function,
            "Starting flat_map operator"
        );

        let downstream_tasks: Vec<String> = descriptor
            .output_gates
            .iter()
            .map(|g| g.downstream_task_id.clone())
            .collect();

        let rx_arc = internal_receivers
            .get(task_id)
            .ok_or_else(|| anyhow::anyhow!("No receiver for task {}", task_id))?
            .clone();

        loop {
            if status.is_cancelled() {
                return Ok(());
            }

            let mut rx = rx_arc.lock().await;
            match tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
                Ok(Some(StreamMessage::Data(bytes))) => {
                    drop(rx);
                    let input = String::from_utf8_lossy(&bytes);
                    status.increment_records(1);
                    status.increment_bytes(bytes.len() as i64);

                    // Apply flat map function
                    let outputs: Vec<String> = match config.function.as_str() {
                        "split_words" => input.split_whitespace().map(|s| s.to_string()).collect(),
                        "split_lines" => input.lines().map(|s| s.to_string()).collect(),
                        "split_csv" => input.split(',').map(|s| s.trim().to_string()).collect(),
                        _ => vec![input.to_string()],
                    };

                    // Forward each output to downstream
                    for output in outputs {
                        let output_bytes = output.as_bytes().to_vec();
                        Self::send_to_downstream(output_bytes, &downstream_tasks, &internal_channels).await;
                    }
                }
                Ok(Some(msg @ StreamMessage::Barrier(_))) | Ok(Some(msg @ StreamMessage::Watermark(_))) => {
                    drop(rx);
                    Self::send_control_downstream(msg, &downstream_tasks, &internal_channels).await;
                }
                Ok(Some(_)) => {}
                Ok(None) => {
                    drop(rx);
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(_) => {}
            }
        }
    }

    /// Filter operator - keeps elements matching a predicate
    pub(super) async fn run_filter_operator(
        task_id: &str,
        descriptor: &TaskDescriptor,
        status: Arc<TaskStatus>,
        internal_channels: Arc<DashMap<String, mpsc::Sender<StreamMessage<Vec<u8>>>>>,
        internal_receivers: Arc<DashMap<String, Arc<tokio::sync::Mutex<mpsc::Receiver<StreamMessage<Vec<u8>>>>>>>,
    ) -> Result<()> {
        let config: FilterConfig = if descriptor.operator_config.is_empty() {
            FilterConfig::default()
        } else {
            serde_json::from_slice(&descriptor.operator_config).unwrap_or_default()
        };

        info!(
            task_id = %task_id,
            predicate = %config.predicate,
            "Starting filter operator"
        );

        let downstream_tasks: Vec<String> = descriptor
            .output_gates
            .iter()
            .map(|g| g.downstream_task_id.clone())
            .collect();

        let rx_arc = internal_receivers
            .get(task_id)
            .ok_or_else(|| anyhow::anyhow!("No receiver for task {}", task_id))?
            .clone();

        loop {
            if status.is_cancelled() {
                return Ok(());
            }

            let mut rx = rx_arc.lock().await;
            match tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
                Ok(Some(StreamMessage::Data(bytes))) => {
                    drop(rx);
                    let input = String::from_utf8_lossy(&bytes);
                    status.increment_records(1);
                    status.increment_bytes(bytes.len() as i64);

                    // Apply filter predicate
                    let keep = match config.predicate.as_str() {
                        "non_empty" => !input.trim().is_empty(),
                        "contains" => input.contains(&config.value),
                        "starts_with" => input.starts_with(&config.value),
                        "ends_with" => input.ends_with(&config.value),
                        "min_length" => {
                            input.len() >= config.value.parse::<usize>().unwrap_or(0)
                        }
                        _ => true, // keep all by default
                    };

                    if keep {
                        Self::send_to_downstream(bytes.clone(), &downstream_tasks, &internal_channels).await;
                    }
                }
                Ok(Some(msg @ StreamMessage::Barrier(_))) | Ok(Some(msg @ StreamMessage::Watermark(_))) => {
                    drop(rx);
                    Self::send_control_downstream(msg, &downstream_tasks, &internal_channels).await;
                }
                Ok(Some(_)) => {}
                Ok(None) => {
                    drop(rx);
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(_) => {}
            }
        }
    }

    /// KeyBy operator - partitions data by key
    pub(super) async fn run_keyby_operator(
        task_id: &str,
        descriptor: &TaskDescriptor,
        status: Arc<TaskStatus>,
        internal_channels: Arc<DashMap<String, mpsc::Sender<StreamMessage<Vec<u8>>>>>,
        internal_receivers: Arc<DashMap<String, Arc<tokio::sync::Mutex<mpsc::Receiver<StreamMessage<Vec<u8>>>>>>>,
    ) -> Result<()> {
        info!(task_id = %task_id, "Starting key_by operator");

        let downstream_tasks: Vec<String> = descriptor
            .output_gates
            .iter()
            .map(|g| g.downstream_task_id.clone())
            .collect();

        let rx_arc = internal_receivers
            .get(task_id)
            .ok_or_else(|| anyhow::anyhow!("No receiver for task {}", task_id))?
            .clone();

        loop {
            if status.is_cancelled() {
                return Ok(());
            }

            let mut rx = rx_arc.lock().await;
            match tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
                Ok(Some(StreamMessage::Data(bytes))) => {
                    drop(rx);
                    status.increment_records(1);
                    status.increment_bytes(bytes.len() as i64);

                    // For now, just forward (in real impl, would partition by key hash)
                    Self::send_to_downstream(bytes.clone(), &downstream_tasks, &internal_channels).await;
                }
                Ok(Some(msg @ StreamMessage::Barrier(_))) | Ok(Some(msg @ StreamMessage::Watermark(_))) => {
                    drop(rx);
                    Self::send_control_downstream(msg, &downstream_tasks, &internal_channels).await;
                }
                Ok(Some(_)) => {}
                Ok(None) => {
                    drop(rx);
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(_) => {}
            }
        }
    }

    /// Window operator - groups elements into time windows
    pub(super) async fn run_window_operator(
        task_id: &str,
        descriptor: &TaskDescriptor,
        status: Arc<TaskStatus>,
        internal_channels: Arc<DashMap<String, mpsc::Sender<StreamMessage<Vec<u8>>>>>,
        internal_receivers: Arc<DashMap<String, Arc<tokio::sync::Mutex<mpsc::Receiver<StreamMessage<Vec<u8>>>>>>>,
    ) -> Result<()> {
        let config: WindowConfig = if descriptor.operator_config.is_empty() {
            WindowConfig::default()
        } else {
            serde_json::from_slice(&descriptor.operator_config).unwrap_or_default()
        };

        info!(
            task_id = %task_id,
            window_ms = config.window_ms,
            "Starting window operator"
        );

        let downstream_tasks: Vec<String> = descriptor
            .output_gates
            .iter()
            .map(|g| g.downstream_task_id.clone())
            .collect();

        let rx_arc = internal_receivers
            .get(task_id)
            .ok_or_else(|| anyhow::anyhow!("No receiver for task {}", task_id))?
            .clone();

        let mut window_buffer: Vec<Vec<u8>> = Vec::new();
        let mut window_start = Instant::now();
        let window_duration = Duration::from_millis(config.window_ms);

        loop {
            if status.is_cancelled() {
                return Ok(());
            }

            // Check if window should close
            if window_start.elapsed() >= window_duration && !window_buffer.is_empty() {
                // Emit window result
                let result = format!("Window: {} elements", window_buffer.len());
                let result_bytes = result.as_bytes().to_vec();

                Self::send_to_downstream(result_bytes.clone(), &downstream_tasks, &internal_channels).await;

                window_buffer.clear();
                window_start = Instant::now();
            }

            let mut rx = rx_arc.lock().await;
            match tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
                Ok(Some(StreamMessage::Data(bytes))) => {
                    drop(rx);
                    status.increment_records(1);
                    status.increment_bytes(bytes.len() as i64);
                    window_buffer.push(bytes);
                }
                Ok(Some(msg @ StreamMessage::Barrier(_))) | Ok(Some(msg @ StreamMessage::Watermark(_))) => {
                    drop(rx);
                    Self::send_control_downstream(msg, &downstream_tasks, &internal_channels).await;
                }
                Ok(Some(_)) => {}
                Ok(None) => {
                    drop(rx);
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(_) => {}
            }
        }
    }

    /// Count operator - counts elements by key
    pub(super) async fn run_count_operator(
        task_id: &str,
        descriptor: &TaskDescriptor,
        status: Arc<TaskStatus>,
        internal_channels: Arc<DashMap<String, mpsc::Sender<StreamMessage<Vec<u8>>>>>,
        internal_receivers: Arc<DashMap<String, Arc<tokio::sync::Mutex<mpsc::Receiver<StreamMessage<Vec<u8>>>>>>>,
    ) -> Result<()> {
        info!(task_id = %task_id, "Starting count operator");

        let downstream_tasks: Vec<String> = descriptor
            .output_gates
            .iter()
            .map(|g| g.downstream_task_id.clone())
            .collect();

        let rx_arc = internal_receivers
            .get(task_id)
            .ok_or_else(|| anyhow::anyhow!("No receiver for task {}", task_id))?
            .clone();

        let mut counts: HashMap<String, u64> = HashMap::new();

        loop {
            if status.is_cancelled() {
                return Ok(());
            }

            let mut rx = rx_arc.lock().await;
            match tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
                Ok(Some(StreamMessage::Data(bytes))) => {
                    drop(rx);
                    let key = String::from_utf8_lossy(&bytes).to_lowercase();
                    status.increment_records(1);
                    status.increment_bytes(bytes.len() as i64);

                    let count = counts.entry(key.clone()).or_insert(0);
                    *count += 1;

                    // Emit count update
                    let result = format!("{}:{}", key, count);
                    let result_bytes = result.as_bytes().to_vec();

                    for downstream_id in &downstream_tasks {
                        if let Some(tx) = internal_channels.get(downstream_id) {
                            let _ = tx.send(StreamMessage::Data(result_bytes.clone())).await;
                        }
                    }
                }
                Ok(Some(msg @ StreamMessage::Barrier(_))) | Ok(Some(msg @ StreamMessage::Watermark(_))) => {
                    drop(rx);
                    Self::send_control_downstream(msg, &downstream_tasks, &internal_channels).await;
                }
                Ok(Some(_)) => {}
                Ok(None) => {
                    drop(rx);
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(_) => {}
            }
        }
    }

    /// Reduce operator - reduces elements using an aggregation function
    pub(super) async fn run_reduce_operator(
        task_id: &str,
        descriptor: &TaskDescriptor,
        status: Arc<TaskStatus>,
        internal_channels: Arc<DashMap<String, mpsc::Sender<StreamMessage<Vec<u8>>>>>,
        internal_receivers: Arc<DashMap<String, Arc<tokio::sync::Mutex<mpsc::Receiver<StreamMessage<Vec<u8>>>>>>>,
    ) -> Result<()> {
        info!(task_id = %task_id, "Starting reduce operator");

        let downstream_tasks: Vec<String> = descriptor
            .output_gates
            .iter()
            .map(|g| g.downstream_task_id.clone())
            .collect();

        let rx_arc = internal_receivers
            .get(task_id)
            .ok_or_else(|| anyhow::anyhow!("No receiver for task {}", task_id))?
            .clone();

        loop {
            if status.is_cancelled() {
                return Ok(());
            }

            let mut rx = rx_arc.lock().await;
            match tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
                Ok(Some(StreamMessage::Data(bytes))) => {
                    drop(rx);
                    status.increment_records(1);
                    status.increment_bytes(bytes.len() as i64);

                    // Pass through for now
                    Self::send_to_downstream(bytes.clone(), &downstream_tasks, &internal_channels).await;
                }
                Ok(Some(msg @ StreamMessage::Barrier(_))) | Ok(Some(msg @ StreamMessage::Watermark(_))) => {
                    drop(rx);
                    Self::send_control_downstream(msg, &downstream_tasks, &internal_channels).await;
                }
                Ok(Some(_)) => {}
                Ok(None) => {
                    drop(rx);
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(_) => {}
            }
        }
    }

    /// Passthrough operator for unknown types
    pub(super) async fn run_passthrough_operator(
        task_id: &str,
        descriptor: &TaskDescriptor,
        status: Arc<TaskStatus>,
        internal_channels: Arc<DashMap<String, mpsc::Sender<StreamMessage<Vec<u8>>>>>,
        internal_receivers: Arc<DashMap<String, Arc<tokio::sync::Mutex<mpsc::Receiver<StreamMessage<Vec<u8>>>>>>>,
    ) -> Result<()> {
        info!(
            task_id = %task_id,
            operator_type = descriptor.operator_type,
            "Starting passthrough operator"
        );

        let downstream_tasks: Vec<String> = descriptor
            .output_gates
            .iter()
            .map(|g| g.downstream_task_id.clone())
            .collect();

        let rx_arc = internal_receivers
            .get(task_id)
            .ok_or_else(|| anyhow::anyhow!("No receiver for task {}", task_id))?
            .clone();

        loop {
            if status.is_cancelled() {
                return Ok(());
            }

            let mut rx = rx_arc.lock().await;
            match tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
                Ok(Some(StreamMessage::Data(bytes))) => {
                    drop(rx);
                    status.increment_records(1);
                    status.increment_bytes(bytes.len() as i64);

                    Self::send_to_downstream(bytes.clone(), &downstream_tasks, &internal_channels).await;
                }
                Ok(Some(msg @ StreamMessage::Barrier(_))) | Ok(Some(msg @ StreamMessage::Watermark(_))) => {
                    drop(rx);
                    Self::send_control_downstream(msg, &downstream_tasks, &internal_channels).await;
                }
                Ok(Some(_)) => {}
                Ok(None) => {
                    drop(rx);
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(_) => {}
            }
        }
    }
}
