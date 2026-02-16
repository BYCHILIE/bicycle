//! Stream execution environment.
//!
//! The StreamEnvironment is the entry point for creating streaming jobs.

use crate::datastream::{DataStream, StreamEnvInner};
use crate::graph::{ConnectorConfig, Edge, JobConfig, JobGraph, OperatorType, Vertex};
use crate::kafka::KafkaSourceBuilder;
use crate::Result;
use serde::{de::DeserializeOwned, Serialize};
use std::sync::Arc;

/// The entry point for creating streaming jobs.
///
/// The StreamEnvironment is used to:
/// - Create data sources
/// - Configure job parameters
/// - Execute the job
///
/// # Example
///
/// ```ignore
/// let env = StreamEnvironment::new();
///
/// env.socket_source("0.0.0.0", 9999)
///     .map(|s: String| s.to_uppercase())
///     .socket_sink("0.0.0.0", 9998);
///
/// env.execute("my-job").await?;
/// ```
pub struct StreamEnvironment {
    inner: Arc<StreamEnvInner>,
    config: JobConfig,
}

impl StreamEnvironment {
    /// Create a new stream environment.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(StreamEnvInner::new()),
            config: JobConfig::default(),
        }
    }

    /// Create a builder for configuring the environment.
    pub fn builder() -> StreamEnvBuilder {
        StreamEnvBuilder::new()
    }

    /// Create a socket source that reads lines from a TCP socket.
    pub fn socket_source(&self, host: &str, port: u16) -> DataStream<String> {
        let vertex_id = self.inner.next_vertex_id();
        let vertex = Vertex::new(
            &vertex_id,
            "SocketSource",
            OperatorType::Source {
                connector: ConnectorConfig::socket(host, port),
            },
        );

        self.inner.add_vertex(vertex);
        DataStream::new(self.inner.clone(), vertex_id)
    }

    /// Create a Kafka source that reads from a Kafka topic.
    pub fn kafka_source(&self, brokers: &str, topic: &str, group_id: &str) -> DataStream<String> {
        let vertex_id = self.inner.next_vertex_id();
        let mut props = std::collections::HashMap::new();
        props.insert("bootstrap.servers".to_string(), brokers.to_string());
        props.insert("topic".to_string(), topic.to_string());
        props.insert("group.id".to_string(), group_id.to_string());

        let vertex = Vertex::new(
            &vertex_id,
            "KafkaSource",
            OperatorType::Source {
                connector: ConnectorConfig {
                    connector_type: crate::graph::ConnectorType::Kafka,
                    properties: props,
                },
            },
        );

        self.inner.add_vertex(vertex);
        DataStream::new(self.inner.clone(), vertex_id)
    }

    /// Create a typed source from a Kafka source builder.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let stream: DataStream<MyEvent> = env.add_source(
    ///     KafkaSourceBuilder::<MyEvent>::new("localhost:9092", "events")
    ///         .group_id("my-group")
    ///         .starting_offsets("earliest")
    /// );
    /// ```
    pub fn add_source<T>(&self, builder: KafkaSourceBuilder<T>) -> DataStream<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync + 'static,
    {
        let vertex_id = self.inner.next_vertex_id();
        let connector = builder.build_connector();
        let vertex = Vertex::new(
            &vertex_id,
            "KafkaSource",
            OperatorType::Source { connector },
        );

        self.inner.add_vertex(vertex);
        DataStream::new(self.inner.clone(), vertex_id)
    }

    /// Create a generator source for testing.
    pub fn generator_source(&self, rate_ms: u64) -> DataStream<String> {
        let vertex_id = self.inner.next_vertex_id();
        let vertex = Vertex::new(
            &vertex_id,
            "GeneratorSource",
            OperatorType::Source {
                connector: ConnectorConfig::generator(rate_ms),
            },
        );

        self.inner.add_vertex(vertex);
        DataStream::new(self.inner.clone(), vertex_id)
    }

    /// Create a source from a collection (for testing).
    pub fn from_collection<T>(&self, _items: Vec<T>) -> DataStream<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync + 'static,
    {
        let vertex_id = self.inner.next_vertex_id();
        let vertex = Vertex::new(
            &vertex_id,
            "CollectionSource",
            OperatorType::Source {
                connector: ConnectorConfig::generator(100),
            },
        );

        self.inner.add_vertex(vertex);
        DataStream::new(self.inner.clone(), vertex_id)
    }

    /// Set the checkpoint interval.
    pub fn set_checkpoint_interval(&mut self, interval_ms: u64) -> &mut Self {
        self.config.checkpoint_interval_ms = interval_ms;
        self
    }

    /// Set the maximum parallelism.
    pub fn set_max_parallelism(&mut self, max_parallelism: u32) -> &mut Self {
        self.config.max_parallelism = max_parallelism;
        self
    }

    /// Set the default parallelism for all operators.
    pub fn set_parallelism(&mut self, parallelism: u32) -> &mut Self {
        self.config.parallelism = parallelism;
        self
    }

    /// Add a job property.
    pub fn set_property(&mut self, key: impl Into<String>, value: impl Into<String>) -> &mut Self {
        self.config.properties.insert(key.into(), value.into());
        self
    }

    /// Build the job graph.
    pub fn build_graph(&self, name: &str) -> JobGraph {
        let mut graph = JobGraph::new(name);

        // Get vertices and apply default parallelism to those that haven't been explicitly set
        let mut vertices = self.inner.get_vertices();
        for vertex in &mut vertices {
            if !vertex.parallelism_set && self.config.parallelism > 1 {
                vertex.parallelism = self.config.parallelism;
            }
        }

        graph.vertices = vertices;
        graph.edges = self.inner.get_edges();
        graph.config = self.config.clone();
        graph
    }

    /// Execute the job.
    ///
    /// This builds the job graph and returns it for submission.
    /// In a full implementation, this would submit the job to the cluster.
    pub fn execute(&self, name: &str) -> Result<JobGraph> {
        let graph = self.build_graph(name);

        // Validate the graph
        graph.validate().map_err(|e| crate::Error::Config(e))?;

        Ok(graph)
    }

    /// Execute the job with optimization.
    ///
    /// This builds the job graph, optimizes it (operator chaining, slot sharing),
    /// and returns both the original graph and the optimization result.
    pub fn execute_optimized(&self, name: &str) -> Result<(JobGraph, crate::optimizer::OptimizedJobGraph)> {
        let graph = self.execute(name)?;
        let optimizer = crate::optimizer::JobGraphOptimizer::new();
        let optimized = optimizer.optimize(&graph);
        Ok((graph, optimized))
    }

    /// Execute the job and return the serialized graph.
    ///
    /// This is used by the WASM runtime to extract the job graph.
    pub fn execute_and_serialize(&self, name: &str) -> Result<Vec<u8>> {
        let graph = self.execute(name)?;
        let data = serde_json::to_vec(&graph)?;
        Ok(data)
    }
}

impl Default for StreamEnvironment {
    fn default() -> Self {
        Self::new()
    }
}

/// Builder for creating StreamEnvironment with configuration.
pub struct StreamEnvBuilder {
    config: JobConfig,
}

impl StreamEnvBuilder {
    /// Create a new builder.
    pub fn new() -> Self {
        Self {
            config: JobConfig::default(),
        }
    }

    /// Set the checkpoint interval.
    pub fn checkpoint_interval(mut self, interval_ms: u64) -> Self {
        self.config.checkpoint_interval_ms = interval_ms;
        self
    }

    /// Set the default parallelism for all operators.
    pub fn parallelism(mut self, parallelism: u32) -> Self {
        self.config.parallelism = parallelism;
        self
    }

    /// Set the maximum parallelism.
    pub fn max_parallelism(mut self, max_parallelism: u32) -> Self {
        self.config.max_parallelism = max_parallelism;
        self
    }

    /// Build the environment.
    pub fn build(self) -> StreamEnvironment {
        let mut env = StreamEnvironment::new();
        env.config = self.config;
        env
    }
}

impl Default for StreamEnvBuilder {
    fn default() -> Self {
        Self::new()
    }
}
