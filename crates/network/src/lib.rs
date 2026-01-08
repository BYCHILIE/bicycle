//! Network layer for distributed data exchange.
//!
//! This module provides:
//! - TCP-based network channels between operators
//! - Credit-based flow control (backpressure)
//! - Serialization/deserialization of stream messages
//! - Connection management and reconnection

use anyhow::{Context, Result};
use bicycle_core::{CheckpointBarrier, PartitionStrategy, StreamMessage, Timestamp};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use dashmap::DashMap;
use futures::{SinkExt, StreamExt};
use parking_lot::RwLock;
use serde::{de::DeserializeOwned, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot, Semaphore};
use tokio_util::codec::{Decoder, Encoder, Framed};
use tracing::{debug, error, info, warn};

// ============================================================================
// Message Types
// ============================================================================

/// Network message envelope.
#[derive(Debug, Clone)]
pub enum NetworkMessage {
    /// Data batch for a specific channel.
    Data {
        channel_id: ChannelId,
        sequence: u64,
        payload: Bytes,
    },
    /// Watermark propagation.
    Watermark {
        channel_id: ChannelId,
        timestamp: Timestamp,
    },
    /// Checkpoint barrier.
    Barrier {
        channel_id: ChannelId,
        barrier: CheckpointBarrier,
    },
    /// End of stream marker.
    EndOfStream { channel_id: ChannelId },
    /// Credit for flow control.
    Credit { channel_id: ChannelId, credit: u32 },
    /// Handshake message for connection setup.
    Handshake {
        source_task: String,
        target_task: String,
        channel_index: u32,
    },
    /// Acknowledgment.
    Ack {
        channel_id: ChannelId,
        sequence: u64,
    },
}

/// Identifies a logical channel between two tasks.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ChannelId {
    pub source_task: String,
    pub target_task: String,
    pub channel_index: u32,
}

impl ChannelId {
    pub fn new(
        source_task: impl Into<String>,
        target_task: impl Into<String>,
        channel_index: u32,
    ) -> Self {
        Self {
            source_task: source_task.into(),
            target_task: target_task.into(),
            channel_index,
        }
    }
}

// ============================================================================
// Codec for Network Messages
// ============================================================================

/// Length-prefixed codec for network messages.
pub struct NetworkCodec;

impl NetworkCodec {
    pub fn new() -> Self {
        Self
    }
}

impl Default for NetworkCodec {
    fn default() -> Self {
        Self::new()
    }
}

// Message type tags
const MSG_DATA: u8 = 1;
const MSG_WATERMARK: u8 = 2;
const MSG_BARRIER: u8 = 3;
const MSG_END: u8 = 4;
const MSG_CREDIT: u8 = 5;
const MSG_HANDSHAKE: u8 = 6;
const MSG_ACK: u8 = 7;

impl Encoder<NetworkMessage> for NetworkCodec {
    type Error = std::io::Error;

    fn encode(&mut self, item: NetworkMessage, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let mut buf = BytesMut::new();

        match item {
            NetworkMessage::Data {
                channel_id,
                sequence,
                payload,
            } => {
                buf.put_u8(MSG_DATA);
                encode_channel_id(&channel_id, &mut buf);
                buf.put_u64(sequence);
                buf.put_u32(payload.len() as u32);
                buf.put_slice(&payload);
            }
            NetworkMessage::Watermark {
                channel_id,
                timestamp,
            } => {
                buf.put_u8(MSG_WATERMARK);
                encode_channel_id(&channel_id, &mut buf);
                buf.put_u64(timestamp);
            }
            NetworkMessage::Barrier {
                channel_id,
                barrier,
            } => {
                buf.put_u8(MSG_BARRIER);
                encode_channel_id(&channel_id, &mut buf);
                buf.put_u64(barrier.checkpoint_id);
                buf.put_u64(barrier.timestamp);
                buf.put_u8(if barrier.options.is_savepoint { 1 } else { 0 });
                buf.put_u8(if barrier.options.is_unaligned { 1 } else { 0 });
            }
            NetworkMessage::EndOfStream { channel_id } => {
                buf.put_u8(MSG_END);
                encode_channel_id(&channel_id, &mut buf);
            }
            NetworkMessage::Credit { channel_id, credit } => {
                buf.put_u8(MSG_CREDIT);
                encode_channel_id(&channel_id, &mut buf);
                buf.put_u32(credit);
            }
            NetworkMessage::Handshake {
                source_task,
                target_task,
                channel_index,
            } => {
                buf.put_u8(MSG_HANDSHAKE);
                encode_string(&source_task, &mut buf);
                encode_string(&target_task, &mut buf);
                buf.put_u32(channel_index);
            }
            NetworkMessage::Ack {
                channel_id,
                sequence,
            } => {
                buf.put_u8(MSG_ACK);
                encode_channel_id(&channel_id, &mut buf);
                buf.put_u64(sequence);
            }
        }

        // Length prefix
        dst.put_u32(buf.len() as u32);
        dst.put_slice(&buf);

        Ok(())
    }
}

impl Decoder for NetworkCodec {
    type Item = NetworkMessage;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 4 {
            return Ok(None);
        }

        let len = u32::from_be_bytes([src[0], src[1], src[2], src[3]]) as usize;

        if src.len() < 4 + len {
            return Ok(None);
        }

        src.advance(4);
        let mut buf = src.split_to(len);

        let msg_type = buf.get_u8();

        let msg = match msg_type {
            MSG_DATA => {
                let channel_id = decode_channel_id(&mut buf)?;
                let sequence = buf.get_u64();
                let payload_len = buf.get_u32() as usize;
                let payload = buf.copy_to_bytes(payload_len);
                NetworkMessage::Data {
                    channel_id,
                    sequence,
                    payload,
                }
            }
            MSG_WATERMARK => {
                let channel_id = decode_channel_id(&mut buf)?;
                let timestamp = buf.get_u64();
                NetworkMessage::Watermark {
                    channel_id,
                    timestamp,
                }
            }
            MSG_BARRIER => {
                let channel_id = decode_channel_id(&mut buf)?;
                let checkpoint_id = buf.get_u64();
                let timestamp = buf.get_u64();
                let is_savepoint = buf.get_u8() != 0;
                let is_unaligned = buf.get_u8() != 0;
                NetworkMessage::Barrier {
                    channel_id,
                    barrier: CheckpointBarrier {
                        checkpoint_id,
                        timestamp,
                        options: bicycle_core::CheckpointOptions {
                            is_savepoint,
                            is_unaligned,
                            alignment_timeout_ms: None,
                        },
                    },
                }
            }
            MSG_END => {
                let channel_id = decode_channel_id(&mut buf)?;
                NetworkMessage::EndOfStream { channel_id }
            }
            MSG_CREDIT => {
                let channel_id = decode_channel_id(&mut buf)?;
                let credit = buf.get_u32();
                NetworkMessage::Credit { channel_id, credit }
            }
            MSG_HANDSHAKE => {
                let source_task = decode_string(&mut buf)?;
                let target_task = decode_string(&mut buf)?;
                let channel_index = buf.get_u32();
                NetworkMessage::Handshake {
                    source_task,
                    target_task,
                    channel_index,
                }
            }
            MSG_ACK => {
                let channel_id = decode_channel_id(&mut buf)?;
                let sequence = buf.get_u64();
                NetworkMessage::Ack {
                    channel_id,
                    sequence,
                }
            }
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Unknown message type: {}", msg_type),
                ));
            }
        };

        Ok(Some(msg))
    }
}

fn encode_channel_id(id: &ChannelId, buf: &mut BytesMut) {
    encode_string(&id.source_task, buf);
    encode_string(&id.target_task, buf);
    buf.put_u32(id.channel_index);
}

fn decode_channel_id(buf: &mut BytesMut) -> Result<ChannelId, std::io::Error> {
    let source_task = decode_string(buf)?;
    let target_task = decode_string(buf)?;
    let channel_index = buf.get_u32();
    Ok(ChannelId {
        source_task,
        target_task,
        channel_index,
    })
}

fn encode_string(s: &str, buf: &mut BytesMut) {
    let bytes = s.as_bytes();
    buf.put_u16(bytes.len() as u16);
    buf.put_slice(bytes);
}

fn decode_string(buf: &mut BytesMut) -> Result<String, std::io::Error> {
    let len = buf.get_u16() as usize;
    let bytes = buf.split_to(len);
    String::from_utf8(bytes.to_vec())
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

// ============================================================================
// Network Environment
// ============================================================================

/// Configuration for the network layer.
#[derive(Debug, Clone)]
pub struct NetworkConfig {
    /// Buffer size for send/receive.
    pub buffer_size: usize,
    /// Initial credits per channel.
    pub initial_credits: u32,
    /// Low watermark for credit replenishment.
    pub credit_low_watermark: u32,
    /// Connection timeout.
    pub connect_timeout: std::time::Duration,
    /// Reconnection attempts.
    pub max_reconnect_attempts: u32,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            buffer_size: 32 * 1024,
            initial_credits: 16,
            credit_low_watermark: 4,
            connect_timeout: std::time::Duration::from_secs(30),
            max_reconnect_attempts: 5,
        }
    }
}

/// Manages network connections for a worker.
pub struct NetworkEnvironment {
    config: NetworkConfig,
    /// Local bind address
    local_addr: RwLock<Option<SocketAddr>>,
    /// Outgoing connections (target_addr -> connection)
    outgoing: DashMap<SocketAddr, Arc<OutgoingConnection>>,
    /// Incoming channel handlers
    incoming_handlers: DashMap<ChannelId, mpsc::Sender<NetworkMessage>>,
    /// Statistics
    bytes_sent: Arc<AtomicU64>,
    bytes_received: Arc<AtomicU64>,
}

impl NetworkEnvironment {
    pub fn new(config: NetworkConfig) -> Self {
        Self {
            config,
            local_addr: RwLock::new(None),
            outgoing: DashMap::new(),
            incoming_handlers: DashMap::new(),
            bytes_sent: Arc::new(AtomicU64::new(0)),
            bytes_received: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Start listening for incoming connections.
    pub async fn start(&self, bind_addr: SocketAddr) -> Result<SocketAddr> {
        let listener = TcpListener::bind(bind_addr).await?;
        let actual_addr = listener.local_addr()?;
        *self.local_addr.write() = Some(actual_addr);

        info!(addr = %actual_addr, "Network environment listening");

        // Spawn acceptor task
        let handlers = self.incoming_handlers.clone();
        let bytes_received = self.bytes_received.clone();

        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, peer_addr)) => {
                        debug!(peer = %peer_addr, "Accepted connection");
                        let handlers = handlers.clone();
                        let bytes_received = bytes_received.clone();

                        tokio::spawn(async move {
                            if let Err(e) = handle_incoming(stream, handlers, bytes_received).await
                            {
                                error!(error = %e, "Error handling incoming connection");
                            }
                        });
                    }
                    Err(e) => {
                        error!(error = %e, "Error accepting connection");
                    }
                }
            }
        });

        Ok(actual_addr)
    }

    /// Register a handler for incoming messages on a channel.
    pub fn register_incoming(&self, channel_id: ChannelId, sender: mpsc::Sender<NetworkMessage>) {
        self.incoming_handlers.insert(channel_id, sender);
    }

    /// Create an outgoing channel to a remote task.
    pub async fn create_outgoing(
        &self,
        target_addr: SocketAddr,
        channel_id: ChannelId,
    ) -> Result<NetworkSender> {
        let conn = if let Some(existing) = self.outgoing.get(&target_addr) {
            existing.clone()
        } else {
            let conn =
                Arc::new(OutgoingConnection::connect(target_addr, self.config.clone()).await?);
            self.outgoing.insert(target_addr, conn.clone());
            conn
        };

        Ok(NetworkSender::new(
            conn,
            channel_id,
            self.config.initial_credits,
        ))
    }

    /// Get network statistics.
    pub fn stats(&self) -> (u64, u64) {
        (
            self.bytes_sent.load(Ordering::Relaxed),
            self.bytes_received.load(Ordering::Relaxed),
        )
    }
}

async fn handle_incoming(
    stream: TcpStream,
    handlers: DashMap<ChannelId, mpsc::Sender<NetworkMessage>>,
    bytes_received: Arc<AtomicU64>,
) -> Result<()> {
    let mut framed = Framed::new(stream, NetworkCodec::new());

    while let Some(result) = framed.next().await {
        let msg = result?;

        // Basic traffic accounting (counts payload bytes for Data messages).
        if let NetworkMessage::Data { payload, .. } = &msg {
            bytes_received.fetch_add(payload.len() as u64, Ordering::Relaxed);
        }

        let channel_id = match &msg {
            NetworkMessage::Data { channel_id, .. } => channel_id.clone(),
            NetworkMessage::Watermark { channel_id, .. } => channel_id.clone(),
            NetworkMessage::Barrier { channel_id, .. } => channel_id.clone(),
            NetworkMessage::EndOfStream { channel_id } => channel_id.clone(),
            NetworkMessage::Handshake {
                source_task,
                target_task,
                channel_index,
            } => ChannelId::new(source_task, target_task, *channel_index),
            _ => continue,
        };

        if let Some(handler) = handlers.get(&channel_id) {
            if let Err(e) = handler.send(msg).await {
                warn!(channel = ?channel_id, error = %e, "Failed to deliver message");
            }
        }
    }

    Ok(())
}

// ============================================================================
// Outgoing Connection
// ============================================================================

struct OutgoingConnection {
    sender: mpsc::Sender<NetworkMessage>,
}

impl OutgoingConnection {
    async fn connect(addr: SocketAddr, config: NetworkConfig) -> Result<Self> {
        let stream = TcpStream::connect(addr)
            .await
            .context("Failed to connect")?;

        let (tx, mut rx) = mpsc::channel::<NetworkMessage>(config.buffer_size);

        let mut framed = Framed::new(stream, NetworkCodec::new());

        tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                if let Err(e) = framed.send(msg).await {
                    error!(error = %e, "Failed to send message");
                    break;
                }
            }
        });

        Ok(Self { sender: tx })
    }

    async fn send(&self, msg: NetworkMessage) -> Result<()> {
        self.sender
            .send(msg)
            .await
            .map_err(|_| anyhow::anyhow!("Connection closed"))
    }
}

// ============================================================================
// Network Sender (with flow control)
// ============================================================================

/// Sender for a specific network channel with credit-based flow control.
pub struct NetworkSender {
    conn: Arc<OutgoingConnection>,
    channel_id: ChannelId,
    credits: Arc<Semaphore>,
    sequence: AtomicU64,
}

impl NetworkSender {
    fn new(conn: Arc<OutgoingConnection>, channel_id: ChannelId, initial_credits: u32) -> Self {
        Self {
            conn,
            channel_id,
            credits: Arc::new(Semaphore::new(initial_credits as usize)),
            sequence: AtomicU64::new(0),
        }
    }

    /// Send a data payload (blocks if no credits available).
    pub async fn send_data(&self, payload: Bytes) -> Result<()> {
        let _permit = self
            .credits
            .acquire()
            .await
            .map_err(|_| anyhow::anyhow!("Credits semaphore closed"))?;

        let sequence = self.sequence.fetch_add(1, Ordering::SeqCst);

        self.conn
            .send(NetworkMessage::Data {
                channel_id: self.channel_id.clone(),
                sequence,
                payload,
            })
            .await
    }

    /// Send a watermark.
    pub async fn send_watermark(&self, timestamp: Timestamp) -> Result<()> {
        self.conn
            .send(NetworkMessage::Watermark {
                channel_id: self.channel_id.clone(),
                timestamp,
            })
            .await
    }

    /// Send a checkpoint barrier.
    pub async fn send_barrier(&self, barrier: CheckpointBarrier) -> Result<()> {
        self.conn
            .send(NetworkMessage::Barrier {
                channel_id: self.channel_id.clone(),
                barrier,
            })
            .await
    }

    /// Send end of stream.
    pub async fn send_end(&self) -> Result<()> {
        self.conn
            .send(NetworkMessage::EndOfStream {
                channel_id: self.channel_id.clone(),
            })
            .await
    }

    /// Add credits (called when receiving credit messages).
    pub fn add_credits(&self, amount: u32) {
        self.credits.add_permits(amount as usize);
    }
}

// ============================================================================
// Network Receiver
// ============================================================================

/// Receiver for incoming network messages.
pub struct NetworkReceiver {
    receiver: mpsc::Receiver<NetworkMessage>,
    channel_id: ChannelId,
}

impl NetworkReceiver {
    pub fn new(receiver: mpsc::Receiver<NetworkMessage>, channel_id: ChannelId) -> Self {
        Self {
            receiver,
            channel_id,
        }
    }

    pub async fn recv(&mut self) -> Option<NetworkMessage> {
        self.receiver.recv().await
    }

    pub fn channel_id(&self) -> &ChannelId {
        &self.channel_id
    }
}

// ============================================================================
// Partitioner
// ============================================================================

/// Partitions data across multiple channels.
pub struct Partitioner {
    strategy: PartitionStrategy,
    num_channels: usize,
    round_robin_counter: AtomicU64,
}

impl Partitioner {
    pub fn new(strategy: PartitionStrategy, num_channels: usize) -> Self {
        Self {
            strategy,
            num_channels,
            round_robin_counter: AtomicU64::new(0),
        }
    }

    /// Select channel(s) for a record.
    pub fn select_channels(&self, key: Option<&[u8]>) -> Vec<usize> {
        match self.strategy {
            PartitionStrategy::Forward => vec![0],
            PartitionStrategy::Rebalance => {
                let idx = self.round_robin_counter.fetch_add(1, Ordering::Relaxed);
                vec![(idx as usize) % self.num_channels]
            }
            PartitionStrategy::Hash => {
                if let Some(key) = key {
                    let hash = hash_key(key);
                    vec![(hash as usize) % self.num_channels]
                } else {
                    vec![0]
                }
            }
            PartitionStrategy::Broadcast => (0..self.num_channels).collect(),
            PartitionStrategy::Custom => vec![0], // Custom requires external logic
        }
    }
}

fn hash_key(key: &[u8]) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish()
}

// ============================================================================
// Serialization Helpers
// ============================================================================

/// Serialize a stream message to bytes.
pub fn serialize_message<T: Serialize>(msg: &StreamMessage<T>) -> Result<Bytes> {
    let data = bincode::serialize(msg)?;
    Ok(Bytes::from(data))
}

/// Deserialize a stream message from bytes.
pub fn deserialize_message<T: DeserializeOwned>(data: &[u8]) -> Result<StreamMessage<T>> {
    let msg = bincode::deserialize(data)?;
    Ok(msg)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_codec_roundtrip() {
        let mut codec = NetworkCodec::new();
        let mut buf = BytesMut::new();

        let msg = NetworkMessage::Data {
            channel_id: ChannelId::new("task1", "task2", 0),
            sequence: 42,
            payload: Bytes::from("hello world"),
        };

        codec.encode(msg.clone(), &mut buf).unwrap();
        let decoded = codec.decode(&mut buf).unwrap().unwrap();

        match decoded {
            NetworkMessage::Data {
                sequence, payload, ..
            } => {
                assert_eq!(sequence, 42);
                assert_eq!(payload, Bytes::from("hello world"));
            }
            _ => panic!("Wrong message type"),
        }
    }

    #[test]
    fn test_partitioner_hash() {
        let partitioner = Partitioner::new(PartitionStrategy::Hash, 4);

        // Same key should always go to same partition
        let channels1 = partitioner.select_channels(Some(b"key1"));
        let channels2 = partitioner.select_channels(Some(b"key1"));
        assert_eq!(channels1, channels2);
    }

    #[test]
    fn test_partitioner_broadcast() {
        let partitioner = Partitioner::new(PartitionStrategy::Broadcast, 4);
        let channels = partitioner.select_channels(None);
        assert_eq!(channels, vec![0, 1, 2, 3]);
    }
}
