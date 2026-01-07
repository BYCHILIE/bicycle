//! Minimal runtime for a streaming operator chain.
//!
//! MVP characteristics:
//! - single-input, single-output operators
//! - bounded channels (backpressure)
//! - watermarks and End are propagated
//!
//! Next steps (not in MVP): multi-input operators, network channels, barrier alignment.

// use anyhow::Context;
use async_trait::async_trait;
use bicycle_core::StreamMessage;
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

    pub async fn emit(&mut self, msg: StreamMessage<T>) -> anyhow::Result<()> {
        self.tx
            .send(msg)
            .await
            .map_err(|_| anyhow::anyhow!("downstream channel closed"))
    }

    pub async fn data(&mut self, item: T) -> anyhow::Result<()> {
        self.emit(StreamMessage::Data(item)).await
    }

    pub async fn watermark(&mut self, ts: u64) -> anyhow::Result<()> {
        self.emit(StreamMessage::Watermark(ts)).await
    }

    pub async fn barrier(&mut self, id: u64) -> anyhow::Result<()> {
        self.emit(StreamMessage::Barrier(id)).await
    }

    pub async fn end(&mut self) -> anyhow::Result<()> {
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
    ) -> anyhow::Result<()>;
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

        while let Some(msg) = rx.recv().await {
            let is_end = matches!(msg, StreamMessage::End);

            if let Err(e) = op.on_message(msg, &mut out).await {
                error!(%name, error = %e, "operator error");
                // In a real runtime you'd trigger task failure/restart.
                let _ = out.end().await;
                break;
            }

            if is_end {
                break;
            }
        }

        debug!(%name, "operator stopped");
    });
}

/// Spawn a sink: consumes messages and applies a handler.
pub fn spawn_sink<T, F>(name: &'static str, mut rx: Receiver<T>, mut f: F)
where
    T: Send + 'static,
    F: FnMut(StreamMessage<T>) -> anyhow::Result<()> + Send + 'static,
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
