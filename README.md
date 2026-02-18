# Bicycle

A **Rust-first distributed streaming engine** inspired by Apache Flink.

Bicycle provides exactly-once stream processing with a fluent DataStream API, native plugin system, Kafka integration, and a modern web UI.

---

## Features

- **Fluent DataStream API** — Flink-like typed pipeline builder with `AsyncFunction`, `RichAsyncFunction`, and `ProcessFunction`
- **Native plugin system** — Compile jobs as shared libraries (.so), loaded at runtime via `libloading`
- **Typed Kafka connectors** — `KafkaSourceBuilder<T>` / `KafkaSinkBuilder<T>` with JSON/String ser/de
- **Exactly-once delivery** — Kafka transactional sinks with two-phase commit, coordinated with checkpoints
- **Ordered/unordered async processing** — Control concurrency, timeouts, and result ordering
- **Operator chaining & optimization** — Automatic fusion, slot sharing, parallelism control
- **Distributed coordination** — JobManager + Workers with gRPC control plane, slot-based scheduling
- **End-to-end checkpointing** — Chandy-Lamport distributed snapshots with barrier propagation, coordinator-driven triggers, sink acknowledgments, and RocksDB state backend
- **Watermarks** — Periodic watermark generation from sources, propagated through the operator DAG
- **Web UI** — Next.js dashboard with job graph visualization, task tracking, live metrics
- **CLI** — Full cluster management (`bicycle status`, `submit`, `list`, `cancel`, etc.)

---

## Current State

Bicycle is under active development. The following is a snapshot of what works today and what is in progress.

### Fully Implemented

| Area | Details |
|------|---------|
| **DataStream API** | `map`, `flat_map`, `filter`, `key_by`, `process_unordered`, `process_ordered`, `process_fn`, `set_parallelism`, `uid`, `name`, `slot_sharing_group` |
| **Sources** | Socket (`socket_source`), Kafka (`add_source` with `KafkaSourceBuilder<T>`), Generator (`generator_source`), Collection (`from_collection`) |
| **Sinks** | Socket (`socket_sink`), Kafka (`sink_to` with `KafkaSinkBuilder<T>`), Console (`print`) |
| **Kafka** | Typed ser/de (JSON + String), consumer group management, offset control, transactional producer, exactly-once delivery via two-phase commit |
| **Plugin system** | `bicycle_plugin!` / `bicycle_plugin_rich!` macros, FFI boundary with `libloading`, WASM-based job graph extraction |
| **Checkpointing** | Coordinator triggers periodic barriers, barriers propagate through operator DAG, sinks acknowledge, coordinator completes checkpoint, deduplication of acks, timeout handling |
| **Operator optimization** | Operator chaining (fusion), slot sharing groups, parallelism per operator |
| **Scheduling** | Slot-based task scheduling, worker registration, heartbeats |
| **gRPC control plane** | Job submission, task deployment, checkpoint triggers/acks, worker management, job cancellation |
| **CLI** | `status`, `submit`, `run`, `list`, `cancel`, `savepoint` commands |
| **Web UI** | Job list, job detail with DAG visualization, task status, metrics dashboard, checkpoint tab |
| **Docker** | Multi-stage build, docker-compose with Kafka/KRaft profile, CI/CD pipeline |
| **State backends** | In-memory (`MemoryStateBackend`), RocksDB (`RocksDBStateBackend`) with get/put/delete/range/prefix scans |

### Partially Implemented (In Progress)

| Area | Status |
|------|--------|
| **Window operators** | `TumblingWindow`, `SlidingWindow`, `SessionWindow` operator structs exist but are not wired into the worker task executor |
| **Event-time processing** | Watermark generation from sources exists; window fire-by-watermark logic is not yet connected |
| **State snapshotting** | State backends exist but operator state is not yet serialized during checkpoint barriers |
| **Network data plane** | TCP data plane (`crates/network/`) exists but inter-worker shuffle goes through gRPC, not the data plane |
| **Web UI metrics** | Dashboard shows live metrics but historical metrics are synthetic; checkpoint tab queries real coordinator data |
| **Exactly-once sink wiring** | `TransactionalKafkaSink` and `ExactlyOnceSinkWriter` exist; worker currently uses basic Kafka sink for all modes |

---

## Project Roadmap

### Iteration 1 — Core Streaming (Complete)

Establish the foundational streaming engine with basic operators, sources, sinks, and cluster coordination.

- [x] DataStream API with typed operators
- [x] Socket and Kafka sources/sinks
- [x] Native plugin system (FFI shared libraries)
- [x] JobManager + Worker with gRPC control plane
- [x] Slot-based scheduling and task deployment
- [x] CLI for cluster management
- [x] Docker deployment with docker-compose
- [x] CI/CD pipeline (GitHub Actions)

### Iteration 2 — Checkpointing & Fault Tolerance (Complete)

End-to-end distributed snapshots with barrier propagation.

- [x] Checkpoint coordinator with periodic triggers
- [x] Barrier injection into source tasks via gRPC
- [x] Barrier forwarding through the entire operator DAG
- [x] Sink acknowledgment with deduplication
- [x] Checkpoint completion detection
- [x] Timeout and failure handling
- [x] Named UIDs for stable operator identity across restarts

### Iteration 3 — Exactly-Once & State (Current)

Wire up transactional sinks and state snapshotting for true exactly-once processing.

- [x] Kafka transactional producer (`TransactionalKafkaSink`)
- [x] Two-phase commit protocol (`ExactlyOnceSinkWriter`)
- [ ] Wire exactly-once sink into worker task executor (use transactional sink when `delivery.guarantee=exactly-once`)
- [ ] Snapshot operator state on checkpoint barrier (serialize `RichAsyncFunction` state to state backend)
- [ ] Restore operator state from latest checkpoint on job restart
- [ ] State backend integration with checkpoint storage (write state handles to checkpoint directory)

### Iteration 4 — Event-Time & Windows

Enable event-time processing with watermarks driving window evaluation.

- [ ] Wire window operators (`TumblingWindow`, `SlidingWindow`, `SessionWindow`) into worker task executor
- [ ] Window fire-by-watermark triggers
- [ ] Event-time Kafka source (extract timestamps from message metadata)
- [ ] Allowed lateness and late data handling
- [ ] Window state managed via state backend

### Iteration 5 — Scaling & Shuffling

Enable true distributed data shuffling and dynamic scaling.

- [ ] Network data plane for inter-worker record exchange (replace gRPC for data path)
- [ ] Hash-based and broadcast partitioning across workers
- [ ] Multi-worker `key_by` shuffle
- [ ] Dynamic rescaling (change parallelism without restarting)
- [ ] Backpressure propagation across network boundaries

### Iteration 6 — Recovery & Savepoints

Full fault tolerance with automatic recovery and manual savepoints.

- [ ] Automatic job restart from latest checkpoint on task failure
- [ ] Savepoint creation and restoration (`bicycle savepoint create/restore`)
- [ ] Worker failure detection and task redistribution
- [ ] Checkpoint garbage collection policies
- [ ] Incremental checkpoints (RocksDB SST file-based)

### Iteration 7 — Observability & Production Readiness

Production-grade monitoring, security, and operational tooling.

- [ ] Prometheus metrics exporter (records/s, bytes/s, latency, checkpoint duration)
- [ ] Real historical metrics in Web UI (replace synthetic data)
- [ ] Structured log aggregation
- [ ] Authentication and authorization (mTLS, RBAC)
- [ ] Resource quotas and fair scheduling
- [ ] Graceful job drain and upgrade

### Future — Advanced Features

- [ ] SQL / Table API (Flink-style relational layer)
- [ ] Complex Event Processing (CEP) pattern matching
- [ ] Batch processing mode (bounded streams)
- [ ] Side outputs and split streams
- [ ] Async I/O operator (external service calls)
- [ ] Schema registry integration (Avro, Protobuf)
- [ ] Kubernetes operator for cluster management
- [ ] Python API (PyBicycle)

---

## Quick Start (Docker)

```bash
# Start the cluster
docker compose up -d

# Check status
docker compose --profile cli run --rm cli status

# Submit the socket wordcount job
docker compose --profile cli run --rm cli run jobs/wordcount.yaml

# Connect (separate terminals)
nc localhost 9998    # Receive output
nc localhost 9999    # Send input

# Open Web UI
open http://localhost:3000

# Stop
docker compose down
```

### With Kafka Pipeline

```bash
# Start cluster + Kafka
docker compose --profile kafka up -d

# Create topics
./jobs/docker/create_topics.sh kafka:9092

# Submit the Kafka wordcount job
docker compose --profile cli run cli submit bin/wordcount-kafka --plugin lib/libwordcount_kafka.so

# Connect
nc localhost 9998    # Receive output
nc localhost 9999    # Send input
```

---

## Writing a Job

### Stateless Function

```rust
use bicycle_api::prelude::*;
use bicycle_plugin::bicycle_plugin;

#[derive(Default)]
pub struct WordSplitter;

#[async_trait]
impl AsyncFunction for WordSplitter {
    type In = String;
    type Out = String;

    async fn process(&mut self, input: String, _ctx: &Context) -> Vec<String> {
        input.split_whitespace().map(|w| w.to_lowercase()).collect()
    }

    fn name(&self) -> &str { "WordSplitter" }
}

bicycle_plugin!(WordSplitter);
```

### Stateful Function

```rust
use bicycle_plugin::bicycle_plugin_rich;

#[derive(Default)]
pub struct WordCounter {
    counts: HashMap<String, u64>,
}

#[async_trait]
impl RichAsyncFunction for WordCounter {
    type In = String;
    type Out = WordCount;

    async fn open(&mut self, _ctx: &RuntimeContext) -> Result<()> { Ok(()) }

    async fn process(&mut self, word: String, _ctx: &RuntimeContext) -> Vec<WordCount> {
        let count = self.counts.entry(word.clone()).or_insert(0);
        *count += 1;
        vec![WordCount { word, count: *count }]
    }

    fn name(&self) -> &str { "WordCounter" }
}

bicycle_plugin_rich!(WordCounter);
```

### Building a Pipeline

```rust
let env = StreamEnvironment::builder()
    .parallelism(2)
    .checkpoint_interval(30_000) // 30s checkpoints
    .build();

// Socket -> Kafka
env.socket_source("0.0.0.0", 9999)
    .uid("socket-source-v1")
    .name("Socket Input")
    .sink_to(KafkaSinkBuilder::<String>::new(&brokers, "raw-lines")
        .property("acks", "all"));

// Kafka -> Process -> Kafka (typed, exactly-once)
env.add_source(KafkaSourceBuilder::<String>::new(&brokers, "raw-lines").group_id("my-group"))
    .uid("kafka-source-v1")
    .process_unordered(WordSplitter::new(), Duration::from_secs(30), 100)
    .uid("splitter-v1")
    .set_parallelism(4)
    .key_by(|word: &String| word.clone())
    .process_ordered(WordCounter::new(), Duration::from_secs(60), 50)
    .uid("counter-v1")
    .sink_to(KafkaSinkBuilder::<WordCount>::new(&brokers, "word-counts")
        .delivery_guarantee(DeliveryGuarantee::ExactlyOnce));

env.execute("wordcount")?;
```

---

## Architecture

```
                    +----------------------+
                    |     JobManager       |  :9000 (gRPC)
                    |  scheduling, state,  |
                    |  checkpoint coord.   |
                    +----------+-----------+
                               |
              +----------------+----------------+
              v                v                 v
     +--------------+ +--------------+  +--------------+
     |   Worker 1   | |   Worker 2   |  |   Worker N   |
     |  tasks, FFI  | |  tasks, FFI  |  |  tasks, FFI  |
     |  plugin load | |  plugin load |  |  plugin load |
     +--------------+ +--------------+  +--------------+

     +--------------+ +--------------+
     |   REST API   | |   Web UI     |  :3000
     |   (Rust)     | |  (Next.js)   |
     +--------------+ +--------------+
```

---

## Checkpointing & Exactly-Once Delivery

Bicycle implements Chandy-Lamport distributed snapshots for fault tolerance, with an optional exactly-once delivery guarantee for Kafka sinks.

### How Checkpointing Works

1. **JobManager** spawns a `CheckpointCoordinator` per job, which triggers periodic checkpoint barriers
2. Barriers are sent to all **source and sink tasks** via gRPC (`TriggerTaskCheckpoint`)
3. Sources inject `StreamMessage::Barrier` into the data stream and forward downstream
4. Every operator **forwards barriers** through the DAG (not just data messages)
5. **Sink tasks** receive barriers and send acknowledgments back to the JobManager
6. The coordinator collects all acks — once every sink has acked, the checkpoint is complete
7. Duplicate acks are suppressed at the sink level (each sink tracks `last_acked_checkpoint`)

### Exactly-Once Kafka Sinks

When configured with `DeliveryGuarantee::ExactlyOnce`, Kafka sinks use a **two-phase commit** protocol:

1. Records are written within a Kafka transaction during each checkpoint interval
2. On checkpoint barrier, the transaction is **pre-committed** (flushed to Kafka)
3. On checkpoint completion (all acks received), the transaction is **committed**
4. On failure, uncommitted transactions are **aborted** — no duplicates

```rust
.sink_to(KafkaSinkBuilder::<WordCount>::new(&brokers, "word-counts")
    .delivery_guarantee(DeliveryGuarantee::ExactlyOnce))
```

Without `ExactlyOnce`, sinks use at-least-once delivery (default).

### Watermarks

Sources emit periodic watermarks (wall-clock based, with 5s lateness tolerance) that propagate through the operator DAG. Window operators use watermarks to determine when to fire.

---

## Project Structure

```
bicycle/
├── crates/
│   ├── core/           # StreamMessage, Event, Record, TaskId
│   ├── runtime/        # Operator trait, channels, emitter, exactly-once sink writer
│   ├── operators/      # Built-in operators (Map, Filter, Window)
│   ├── state/          # State backends (Memory, RocksDB)
│   ├── checkpoint/     # Chandy-Lamport snapshots
│   ├── network/        # TCP data plane
│   ├── protocol/       # gRPC protobuf definitions
│   ├── connectors/     # Kafka, Socket connectors
│   ├── api/            # DataStream API, Kafka builders, graph
│   ├── api-macros/     # Proc macros (#[bicycle_main])
│   └── plugin/         # FFI plugin system (bicycle_plugin!)
├── bin/
│   ├── jobmanager/     # Control plane server
│   ├── worker/         # Task executor + plugin loader
│   ├── bicycle/        # CLI
│   ├── webui/          # Rust REST API backend
│   ├── webui-next/     # Next.js frontend
│   ├── mini-runner/    # Local demo (no cluster)
│   ├── socket-test/    # Socket connector testing
│   └── wordcount-socket/ # Standalone wordcount
├── jobs/
│   ├── wordcount-kafka/  # Kafka pipeline example
│   ├── wordcount-plugin/ # Plugin example
│   └── *.yaml            # Job definitions
├── proto/              # gRPC service definitions
├── config/             # JobManager/Worker configs
├── Dockerfile          # Multi-stage build
├── docker-compose.yml  # Full cluster setup
└── .github/workflows/  # CI/CD pipeline
```

---

## Docker Services

| Service | Port | Profile | Description |
|---------|------|---------|-------------|
| `jobmanager` | 9000 | default | Control plane |
| `worker` | 9001, 9101, 9999, 9998 | default | Task execution |
| `webui` | 3000 | default | Next.js dashboard |
| `api` | — | default | REST API backend |
| `cli` | — | `cli` | Bicycle CLI |
| `kafka` | 9092 | `kafka` | KRaft broker |
| `kafka-ui` | 8085 | `kafka` | Kafka monitoring |
| `wordcount-kafka` | — | `kafka` | Kafka job demo |

---

## CI/CD

The GitHub Actions pipeline runs on every push/PR to `main`:

1. **Check** — `cargo check` + `cargo test` on Ubuntu with all system deps
2. **Build & Push** (main only) — Builds and pushes multi-arch Docker images:
   - `bicycle-jobmanager`
   - `bicycle-worker`
   - `bicycle-cli`
   - `bicycle-webui`

Images are tagged with `latest` and the commit SHA. Requires `DOCKER_USERNAME` and `DOCKER_PASSWORD` secrets.

---

## Local Development

```bash
# Prerequisites (Ubuntu/Debian)
sudo apt-get install -y build-essential cmake clang libclang-dev \
    libsnappy-dev liblz4-dev libzstd-dev libssl-dev pkg-config protobuf-compiler

# Build
cargo build --release

# Run local demo (no cluster)
cargo run -p mini-runner

# Run cluster manually
cargo run -p jobmanager -- --bind 0.0.0.0:9000
cargo run -p worker -- --jobmanager 127.0.0.1:9000 --slots 8
cargo run -p webui -- --jobmanager 127.0.0.1:9000

# Test
cargo test --workspace
```

---

## License

Apache-2.0

## Acknowledgments

Inspired by [Apache Flink](https://flink.apache.org/), [Timely Dataflow](https://github.com/TimelyDataflow/timely-dataflow), and [Arroyo](https://github.com/ArroyoSystems/arroyo).
