//! Job graph representation.
//!
//! This module defines the internal representation of a streaming job graph,
//! which is serialized to WASM for submission to the cluster.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A complete job graph ready for submission.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobGraph {
    /// Job name.
    pub name: String,
    /// Vertices (operators) in the graph.
    pub vertices: Vec<Vertex>,
    /// Edges (data channels) between vertices.
    pub edges: Vec<Edge>,
    /// Job configuration.
    pub config: JobConfig,
}

impl JobGraph {
    /// Create a new empty job graph.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            vertices: Vec::new(),
            edges: Vec::new(),
            config: JobConfig::default(),
        }
    }

    /// Add a vertex to the graph.
    pub fn add_vertex(&mut self, vertex: Vertex) -> &mut Self {
        self.vertices.push(vertex);
        self
    }

    /// Add an edge to the graph.
    pub fn add_edge(&mut self, edge: Edge) -> &mut Self {
        self.edges.push(edge);
        self
    }

    /// Validate the graph structure.
    pub fn validate(&self) -> Result<(), String> {
        // Check that all edge endpoints exist
        let vertex_ids: std::collections::HashSet<_> =
            self.vertices.iter().map(|v| &v.id).collect();

        for edge in &self.edges {
            if !vertex_ids.contains(&edge.source_id) {
                return Err(format!("Edge source '{}' not found", edge.source_id));
            }
            if !vertex_ids.contains(&edge.target_id) {
                return Err(format!("Edge target '{}' not found", edge.target_id));
            }
        }

        // Check that there's at least one source and one sink
        let has_source = self.vertices.iter().any(|v| matches!(v.operator_type, OperatorType::Source { .. }));
        let has_sink = self.vertices.iter().any(|v| matches!(v.operator_type, OperatorType::Sink { .. }));

        if !has_source {
            return Err("Job graph must have at least one source".to_string());
        }
        if !has_sink {
            return Err("Job graph must have at least one sink".to_string());
        }

        Ok(())
    }
}

/// A vertex (operator) in the job graph.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Vertex {
    /// Internal vertex identifier (auto-generated).
    pub id: String,
    /// User-defined unique identifier for state recovery.
    /// This should be stable across job restarts to enable state migration.
    pub uid: Option<String>,
    /// Display name (shown in UI).
    pub name: String,
    /// Operator type.
    pub operator_type: OperatorType,
    /// Parallelism (number of parallel instances).
    pub parallelism: u32,
    /// Whether parallelism was explicitly set by the user.
    #[serde(skip)]
    pub parallelism_set: bool,
    /// Maximum parallelism for this operator (for rescaling).
    /// If None, uses the job-level max_parallelism.
    pub max_parallelism: Option<u32>,
    /// Slot sharing group. Operators in the same group can share slots.
    /// Default group is "default". Use different groups to isolate operators.
    pub slot_sharing_group: String,
    /// Whether this operator can be chained with upstream/downstream operators.
    pub chaining_enabled: bool,
    /// Plugin function name (if using native plugin).
    pub plugin_function: Option<String>,
    /// Serialized operator configuration.
    pub config: Vec<u8>,
}

impl Vertex {
    /// Create a new vertex.
    pub fn new(id: impl Into<String>, name: impl Into<String>, operator_type: OperatorType) -> Self {
        Self {
            id: id.into(),
            uid: None,
            name: name.into(),
            operator_type,
            parallelism: 1,
            parallelism_set: false,
            max_parallelism: None,
            slot_sharing_group: "default".to_string(),
            chaining_enabled: true,
            plugin_function: None,
            config: Vec::new(),
        }
    }

    /// Set user-defined unique identifier for state recovery.
    pub fn with_uid(mut self, uid: impl Into<String>) -> Self {
        self.uid = Some(uid.into());
        self
    }

    /// Set display name.
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    /// Set parallelism.
    pub fn with_parallelism(mut self, parallelism: u32) -> Self {
        self.parallelism = parallelism;
        self
    }

    /// Set maximum parallelism for rescaling.
    pub fn with_max_parallelism(mut self, max_parallelism: u32) -> Self {
        self.max_parallelism = Some(max_parallelism);
        self
    }

    /// Set slot sharing group.
    pub fn with_slot_sharing_group(mut self, group: impl Into<String>) -> Self {
        self.slot_sharing_group = group.into();
        self
    }

    /// Disable chaining for this operator.
    pub fn with_chaining_disabled(mut self) -> Self {
        self.chaining_enabled = false;
        self
    }

    /// Set plugin function name.
    pub fn with_plugin_function(mut self, name: impl Into<String>) -> Self {
        self.plugin_function = Some(name.into());
        self
    }

    /// Set configuration.
    pub fn with_config(mut self, config: Vec<u8>) -> Self {
        self.config = config;
        self
    }
}

/// Operator type enumeration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OperatorType {
    /// Source operator - reads from external system.
    Source {
        connector: ConnectorConfig,
    },
    /// Sink operator - writes to external system.
    Sink {
        connector: ConnectorConfig,
    },
    /// Map operator - transforms elements 1:1.
    Map,
    /// FlatMap operator - transforms elements 1:N.
    FlatMap,
    /// Filter operator - filters elements.
    Filter,
    /// KeyBy operator - partitions by key.
    KeyBy {
        key_selector: String,
    },
    /// Window operator - groups elements into windows.
    Window {
        window_type: WindowType,
    },
    /// Reduce operator - reduces elements.
    Reduce,
    /// Process operator - custom processing (AsyncFunction/RichAsyncFunction).
    Process {
        /// Whether this uses RichAsyncFunction (stateful).
        is_rich: bool,
    },
    /// Count operator - counts elements.
    Count,
    /// Union operator - merge multiple same-typed streams.
    Union,
    /// Connect operator - combine two differently-typed streams.
    Connect,
    /// CoProcess operator - processing on connected streams.
    CoProcess {
        /// Whether this uses a rich (stateful) co-process function.
        is_rich: bool,
    },
    /// SideOutput operator - tagged output splitting.
    SideOutput {
        /// Output tag identifier.
        tag: String,
    },
    /// AsyncIO operator - async external I/O calls (DB lookups, REST, etc.).
    AsyncIO {
        /// Whether results should maintain input order.
        ordered: bool,
        /// Timeout in milliseconds for async calls.
        timeout_ms: u64,
        /// Maximum number of concurrent async requests.
        capacity: usize,
    },
}

/// Connector configuration for sources and sinks.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorConfig {
    /// Connector type.
    pub connector_type: ConnectorType,
    /// Connector properties.
    pub properties: HashMap<String, String>,
}

impl ConnectorConfig {
    /// Create a socket connector config.
    pub fn socket(host: impl Into<String>, port: u16) -> Self {
        let mut props = HashMap::new();
        props.insert("host".to_string(), host.into());
        props.insert("port".to_string(), port.to_string());
        Self {
            connector_type: ConnectorType::Socket,
            properties: props,
        }
    }

    /// Create a Kafka connector config.
    pub fn kafka(brokers: impl Into<String>, topic: impl Into<String>) -> Self {
        let mut props = HashMap::new();
        props.insert("bootstrap.servers".to_string(), brokers.into());
        props.insert("topic".to_string(), topic.into());
        Self {
            connector_type: ConnectorType::Kafka,
            properties: props,
        }
    }

    /// Create a Pulsar connector config.
    pub fn pulsar(service_url: impl Into<String>, topic: impl Into<String>) -> Self {
        let mut props = HashMap::new();
        props.insert("service.url".to_string(), service_url.into());
        props.insert("topic".to_string(), topic.into());
        Self {
            connector_type: ConnectorType::Pulsar,
            properties: props,
        }
    }

    /// Create a generator connector config.
    pub fn generator(rate_ms: u64) -> Self {
        let mut props = HashMap::new();
        props.insert("rate_ms".to_string(), rate_ms.to_string());
        Self {
            connector_type: ConnectorType::Generator,
            properties: props,
        }
    }
}

/// Supported connector types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConnectorType {
    None,
    Socket,
    Kafka,
    File,
    Generator,
    Pulsar,
}

/// Window type configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WindowType {
    /// Tumbling window (fixed size, non-overlapping).
    Tumbling { size_ms: u64 },
    /// Sliding window (fixed size, overlapping).
    Sliding { size_ms: u64, slide_ms: u64 },
    /// Session window (gap-based).
    Session { gap_ms: u64 },
    /// Count-based window.
    Count { count: u64 },
}

/// Configuration for ordered/unordered async processing.
///
/// Controls whether async results preserve input order, the timeout for
/// async operations, and the maximum number of concurrent async requests.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AsyncProcessConfig {
    /// Whether results should maintain input order.
    pub ordered: bool,
    /// Timeout in milliseconds for async processing.
    pub timeout_ms: u64,
    /// Maximum number of concurrent async requests.
    pub capacity: usize,
}

/// An edge (data channel) in the job graph.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Edge {
    /// Source vertex ID.
    pub source_id: String,
    /// Target vertex ID.
    pub target_id: String,
    /// Partitioning strategy.
    pub partition_strategy: PartitionStrategy,
}

impl Edge {
    /// Create a new edge.
    pub fn new(source_id: impl Into<String>, target_id: impl Into<String>) -> Self {
        Self {
            source_id: source_id.into(),
            target_id: target_id.into(),
            partition_strategy: PartitionStrategy::Forward,
        }
    }

    /// Set partition strategy.
    pub fn with_strategy(mut self, strategy: PartitionStrategy) -> Self {
        self.partition_strategy = strategy;
        self
    }
}

/// Partitioning strategy for data distribution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum PartitionStrategy {
    /// Forward to same parallel instance.
    #[default]
    Forward,
    /// Round-robin distribution.
    Rebalance,
    /// Hash-based partitioning by key.
    Hash,
    /// Send to all parallel instances.
    Broadcast,
}

/// Job configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobConfig {
    /// Default parallelism for operators (can be overridden per-operator).
    pub parallelism: u32,
    /// Maximum parallelism for rescaling (determines key group count).
    pub max_parallelism: u32,
    /// Checkpoint interval in milliseconds.
    pub checkpoint_interval_ms: u64,
    /// Restart strategy.
    pub restart_strategy: RestartStrategy,
    /// Additional properties.
    pub properties: HashMap<String, String>,
}

impl Default for JobConfig {
    fn default() -> Self {
        Self {
            parallelism: 1,
            checkpoint_interval_ms: 60000,
            max_parallelism: 128,
            restart_strategy: RestartStrategy::default(),
            properties: HashMap::new(),
        }
    }
}

/// Restart strategy configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestartStrategy {
    /// Strategy type.
    pub strategy_type: RestartStrategyType,
    /// Maximum restart attempts.
    pub max_attempts: u32,
    /// Delay between restarts.
    pub delay_ms: u64,
}

impl Default for RestartStrategy {
    fn default() -> Self {
        Self {
            strategy_type: RestartStrategyType::FixedDelay,
            max_attempts: 3,
            delay_ms: 5000,
        }
    }
}

/// Restart strategy types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum RestartStrategyType {
    /// No automatic restarts.
    None,
    /// Fixed delay between restarts.
    #[default]
    FixedDelay,
    /// Exponential backoff.
    ExponentialBackoff,
}
