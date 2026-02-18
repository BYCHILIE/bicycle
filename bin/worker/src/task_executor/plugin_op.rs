// =========================================================================
// Plugin Operator - Executes native (.so) plugin functions
// =========================================================================

use anyhow::Result;
use bicycle_core::StreamMessage;
use bicycle_protocol::control::TaskDescriptor;
use dashmap::DashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, error, info};

use crate::plugin_loader::{PluginCache, PluginContext, PluginFunction};

use super::{TaskExecutor, TaskStatus};

impl TaskExecutor {
    /// Plugin operator - executes native (.so) plugin functions
    pub(super) async fn run_plugin_operator(
        task_id: &str,
        job_id: &str,
        descriptor: &TaskDescriptor,
        status: Arc<TaskStatus>,
        internal_channels: Arc<DashMap<String, mpsc::Sender<StreamMessage<Vec<u8>>>>>,
        internal_receivers: Arc<DashMap<String, Arc<tokio::sync::Mutex<mpsc::Receiver<StreamMessage<Vec<u8>>>>>>>,
        plugin_cache: Arc<PluginCache>,
    ) -> Result<()> {
        info!(
            task_id = %task_id,
            job_id = %job_id,
            function = %descriptor.plugin_function,
            is_rich = descriptor.is_rich_function,
            "Starting plugin operator"
        );

        // Load the plugin
        let plugin = plugin_cache.get_or_load(job_id, &descriptor.plugin_module)?;

        // Create function instance
        let mut plugin_function = PluginFunction::new(
            plugin.clone(),
            &descriptor.plugin_function,
            &descriptor.operator_config,
        )?;

        // Create context
        let ctx = PluginContext {
            job_id: job_id.to_string(),
            task_id: task_id.to_string(),
            subtask_index: descriptor.subtask_index,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0),
            current_key: None,
        };

        // Open the function
        plugin_function.open(&ctx)?;

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
                plugin_function.close()?;
                return Ok(());
            }

            let mut rx = rx_arc.lock().await;
            match tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
                Ok(Some(StreamMessage::Data(input_bytes))) => {
                    drop(rx);
                    status.increment_records(1);
                    status.increment_bytes(input_bytes.len() as i64);

                    // Update context timestamp
                    let ctx = PluginContext {
                        job_id: job_id.to_string(),
                        task_id: task_id.to_string(),
                        subtask_index: descriptor.subtask_index,
                        timestamp: std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_millis() as i64)
                            .unwrap_or(0),
                        current_key: None,
                    };

                    // Process through plugin
                    match plugin_function.process(&input_bytes, &ctx) {
                        Ok(output_bytes) => {
                            debug!(
                                task_id = %task_id,
                                input_len = input_bytes.len(),
                                output_len = output_bytes.len(),
                                output_preview = %String::from_utf8_lossy(&output_bytes[..output_bytes.len().min(200)]),
                                "Plugin processed record"
                            );
                            // Output is a JSON array of outputs, deserialize and send each
                            if !output_bytes.is_empty() {
                                // Try to deserialize as array of outputs
                                if let Ok(outputs) = serde_json::from_slice::<Vec<serde_json::Value>>(&output_bytes) {
                                    for output in outputs {
                                        let output_data = serde_json::to_vec(&output).unwrap_or_default();
                                        Self::send_to_downstream(
                                            output_data,
                                            &downstream_tasks,
                                            &internal_channels,
                                        )
                                        .await;
                                    }
                                } else {
                                    // If not an array, send raw bytes
                                    Self::send_to_downstream(
                                        output_bytes,
                                        &downstream_tasks,
                                        &internal_channels,
                                    )
                                    .await;
                                }
                            }
                        }
                        Err(e) => {
                            error!(
                                task_id = %task_id,
                                error = %e,
                                "Plugin process error"
                            );
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
}
