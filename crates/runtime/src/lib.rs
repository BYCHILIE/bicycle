//! Minimal runtime for a streaming operator chain.
//!
//! MVP characteristics:
//! - single-input, single-output operators
//! - bounded channels (backpressure)
//! - watermarks and End are propagated
//! - checkpoint barriers
//!
//! Next steps (not in MVP): multi-input operators, network channels, barrier alignment.

use anyhow::Result;
use async_trait::async_trait;
use bicycle_core::{CheckpointBarrier, LatencyMarker, StreamMessage, Timestamp};
use tokio::sync::mpsc;
use tracing::{debug, error};

pub type Sender<T> = mpsc::Sender<StreamMessage<T>>;
pub type Receiver<T> = mpsc::Receiver<StreamMessage<T>>;

/// Outbound collector for an operator.
#[derive(Clone)]
pub struct Emitter<T> {
    tx: Sender<T>,
}

impl<T: Send + 'static> Emitter<T> {
    pub fn new(tx: Sender<T>) -> Self {
        Self { tx }
    }

    pub async fn emit(&mut self, msg: StreamMessage<T>) -> Result<()> {
        self.tx
            .send(msg)
            .await
            .map_err(|_| anyhow::anyhow!("downstream channel closed"))
    }

    pub async fn data(&mut self, item: T) -> Result<()> {
        self.emit(StreamMessage::Data(item)).await
    }

    pub async fn watermark(&mut self, ts: Timestamp) -> Result<()> {
        self.emit(StreamMessage::Watermark(ts)).await
    }

    pub async fn barrier(&mut self, barrier: CheckpointBarrier) -> Result<()> {
        self.emit(StreamMessage::Barrier(barrier)).await
    }

    pub async fn latency_marker(&mut self, marker: LatencyMarker) -> Result<()> {
        self.emit(StreamMessage::LatencyMarker(marker)).await
    }

    pub async fn end(&mut self) -> Result<()> {
        self.emit(StreamMessage::End).await
    }
}

/// An operator transforms an input stream into an output stream.
///
/// MVP: single input -> single output.
#[async_trait]
pub trait Operator: Send + 'static {
    type In: Send + 'static;
    type Out: Send + 'static;

    async fn on_message(
        &mut self,
        msg: StreamMessage<Self::In>,
        out: &mut Emitter<Self::Out>,
    ) -> Result<()>;

    /// Called when initializing the operator.
    async fn open(&mut self) -> Result<()> {
        Ok(())
    }

    /// Called when closing the operator.
    async fn close(&mut self) -> Result<()> {
        Ok(())
    }

    /// Snapshot state for checkpointing.
    fn snapshot(&self) -> Result<Vec<u8>> {
        Ok(vec![])
    }

    /// Restore state from checkpoint.
    fn restore(&mut self, _state: &[u8]) -> Result<()> {
        Ok(())
    }
}

/// Create a bounded channel for stream messages.
pub fn stream_channel<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    mpsc::channel(capacity)
}

/// Spawn an operator as an async task.
pub fn spawn_operator<O>(name: &'static str, mut op: O, mut rx: Receiver<O::In>, tx: Sender<O::Out>)
where
    O: Operator,
{
    tokio::spawn(async move {
        let mut out = Emitter::new(tx);
        debug!(%name, "operator started");

        // Initialize operator
        if let Err(e) = op.open().await {
            error!(%name, error = %e, "operator open failed");
            let _ = out.end().await;
            return;
        }

        while let Some(msg) = rx.recv().await {
            let is_end = matches!(msg, StreamMessage::End);

            if let Err(e) = op.on_message(msg, &mut out).await {
                error!(%name, error = %e, "operator error");
                let _ = out.end().await;
                break;
            }

            if is_end {
                break;
            }
        }

        // Cleanup
        if let Err(e) = op.close().await {
            error!(%name, error = %e, "operator close failed");
        }

        debug!(%name, "operator stopped");
    });
}

/// Spawn a sink: consumes messages and applies a handler.
pub fn spawn_sink<T, F>(name: &'static str, mut rx: Receiver<T>, mut f: F)
where
    T: Send + 'static,
    F: FnMut(StreamMessage<T>) -> Result<()> + Send + 'static,
{
    tokio::spawn(async move {
        debug!(%name, "sink started");
        while let Some(msg) = rx.recv().await {
            let is_end = matches!(msg, StreamMessage::End);
            if let Err(e) = f(msg) {
                error!(%name, error = %e, "sink handler error");
                break;
            }
            if is_end {
                break;
            }
        }
        debug!(%name, "sink stopped");
    });
}

/// Spawn a source that produces messages.
pub fn spawn_source<T, F, Fut>(name: &'static str, tx: Sender<T>, f: F)
where
    T: Send + 'static,
    F: FnOnce(Emitter<T>) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<()>> + Send,
{
    tokio::spawn(async move {
        debug!(%name, "source started");
        let emitter = Emitter::new(tx);
        if let Err(e) = f(emitter).await {
            error!(%name, error = %e, "source error");
        }
        debug!(%name, "source stopped");
    });
}

/// Chain two operators together.
pub fn chain_operators<O1, O2>(
    name: &'static str,
    op1: O1,
    op2: O2,
    rx: Receiver<O1::In>,
    tx: Sender<O2::Out>,
    buffer_size: usize,
) where
    O1: Operator,
    O2: Operator<In = O1::Out>,
{
    let (intermediate_tx, intermediate_rx) = stream_channel(buffer_size);
    spawn_operator(name, op1, rx, intermediate_tx);
    spawn_operator(name, op2, intermediate_rx, tx);
}

/// Metrics for an operator.
#[derive(Debug, Default, Clone)]
pub struct OperatorMetrics {
    pub records_in: u64,
    pub records_out: u64,
    pub bytes_in: u64,
    pub bytes_out: u64,
    pub watermark: Timestamp,
    pub last_checkpoint_id: u64,
    pub last_checkpoint_duration_ms: u64,
}

/// Task information passed to operators.
#[derive(Debug, Clone)]
pub struct TaskInfo {
    pub job_id: String,
    pub task_id: String,
    pub subtask_index: u32,
    pub parallelism: u32,
    pub operator_name: String,
}

impl TaskInfo {
    pub fn new(
        job_id: impl Into<String>,
        task_id: impl Into<String>,
        subtask_index: u32,
        parallelism: u32,
        operator_name: impl Into<String>,
    ) -> Self {
        Self {
            job_id: job_id.into(),
            task_id: task_id.into(),
            subtask_index,
            parallelism,
            operator_name: operator_name.into(),
        }
    }
}
