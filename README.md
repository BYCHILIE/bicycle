# Bicycle

A **Rust-first distributed streaming engine** inspired by Apache Flink.

Bicycle provides exactly-once stream processing semantics with a clean, idiomatic Rust API and production-grade state management via RocksDB.

## Features

### Core Capabilities
- **Streaming operators**: Map, Filter, FlatMap, KeyBy, Reduce
- **Window operations**: Tumbling, Sliding, Session windows
- **Event-time processing**: Watermarks, late event handling
- **Backpressure**: Bounded channels with credit-based flow control
- **State management**: Memory and RocksDB state backends
- **Checkpointing**: Chandy-Lamport distributed snapshots with barrier alignment
- **Network layer**: TCP-based data plane with efficient serialization

### Distributed Coordination
- **JobManager**: Full control plane with gRPC API for job submission, scheduling, and monitoring
- **Worker nodes**: Task executors with heartbeat-based health monitoring
- **Task scheduling**: Slot-based allocation with locality-aware placement
- **Cluster monitoring**: Real-time metrics, worker status, and job tracking

### Exactly-Once Semantics
- **Transactional sinks**: Two-phase commit protocol for exactly-once delivery
- **Idempotent writes**: Deduplication tracking for sink operations
- **Checkpoint coordination**: Barrier alignment with timeout-based fallback

### Connectors
- **Apache Kafka**: Source and sink with exactly-once transactional support
- **Apache Pulsar**: Source and sink with sequence-based deduplication (placeholder)

### Web UI & CLI
- **Dashboard**: Real-time cluster overview with job and worker status
- **Job management**: Submit, monitor, and cancel jobs via REST API
- **Metrics**: Records processed, throughput, checkpoints, and resource usage
- **Demo client**: CLI tool for cluster interaction and job submission

---

## Quick Start

### Option 1: Docker (Recommended)

```bash
# Start the full cluster (JobManager + 2 Workers + Web UI)
docker compose up -d

# Open the Web UI
open http://localhost:8081

# Run the interactive demo
docker compose --profile demo up demo

# View logs
docker compose logs -f

# Stop the cluster
docker compose down
```

### Option 2: Local Development

#### Prerequisites

**Ubuntu/Debian:**
```bash
sudo apt-get update
sudo apt-get install -y \
    build-essential \
    cmake \
    clang \
    libclang-dev \
    llvm-dev \
    libsnappy-dev \
    liblz4-dev \
    libzstd-dev \
    zlib1g-dev

# Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

**macOS:**
```bash
# Install Xcode command line tools
xcode-select --install

# Install dependencies via Homebrew
brew install cmake llvm snappy lz4 zstd

# Set LLVM path (add to ~/.zshrc or ~/.bashrc)
export LIBCLANG_PATH="$(brew --prefix llvm)/lib"
export PATH="$(brew --prefix llvm)/bin:$PATH"

# Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

**Fedora/RHEL:**
```bash
sudo dnf install -y \
    gcc gcc-c++ \
    cmake \
    clang clang-devel \
    llvm-devel \
    snappy-devel \
    lz4-devel \
    libzstd-devel \
    zlib-devel
```

**Arch Linux:**
```bash
sudo pacman -S base-devel cmake clang llvm snappy lz4 zstd
```

#### Build and Run

```bash
# Clone the repository
git clone https://github.com/yourusername/bicycle.git
cd bicycle

# Build (first build takes a while due to RocksDB)
cargo build --release

# Run the local demo pipeline (standalone, no cluster needed)
cargo run -p mini-runner

# Run with debug logging
RUST_LOG=debug cargo run -p mini-runner
```

Expected output:
```
INFO  Starting Bicycle streaming demo
window=[0..5000] key=a sum=4
window=[0..5000] key=b sum=2
window=[5000..10000] key=a sum=16
window=[5000..10000] key=b sum=4
window=[10000..15000] key=a sum=6
window=[10000..15000] key=b sum=2
window=[15000..20000] key=b sum=8
INFO  Demo complete
```

---

## Demo Tutorial

This tutorial walks you through running a complete Bicycle cluster and submitting jobs.

### Step 1: Start the Cluster

```bash
# Start all services
docker compose up -d

# Verify all containers are running
docker compose ps
```

You should see:
```
NAME                 STATUS
bicycle-jobmanager   Up (healthy)
bicycle-worker-1     Up
bicycle-worker-2     Up
bicycle-webui        Up
```

### Step 2: Open the Web UI

Navigate to http://localhost:8081 in your browser. You'll see:

- **Cluster Overview**: Workers count, available slots, running jobs
- **Workers Table**: List of registered workers with status
- **Jobs Table**: Running and completed jobs

### Step 3: Run the Demo Client

The demo client submits a sample job and monitors the cluster:

```bash
# Run the full demo (submits a job and monitors for 60 seconds)
docker compose --profile demo up demo
```

Output:
```
=== Bicycle Cluster Demo ===

1. Checking cluster status...

=== Cluster Status ===
Workers:         2/2 active
Slots:           8/8 available
Running Jobs:    0
Total Tasks:     0
Uptime:          5s

2. Listing workers...

=== Workers (2) ===
ID                                       Hostname        Slots      Status
--------------------------------------------------------------------------------
abc12345-...                             worker-1        0/4        Active
def67890-...                             worker-2        0/4        Active

3. Submitting demo job...

Job ID: 568ef8e1-fe2b-4791-8568-72876a5a3a01
Message: Job submitted with 7 tasks

4. Monitoring job for 60s...

[  6s] Job 568ef8e1 - State: Running, Tasks: 7, Records: in=0 out=0
[ 12s] Job 568ef8e1 - State: Running, Tasks: 7, Records: in=0 out=0
...

5. Final cluster status...

=== Cluster Metrics ===
Uptime:              65s
Records Processed:   0
Bytes Processed:     0
Checkpoints OK:      0
```

### Step 4: Use the Demo Client CLI

The demo client provides several commands:

```bash
# Check cluster status
docker compose exec jobmanager /opt/bicycle/bin/demo-client \
    --jobmanager localhost:9000 status

# List workers
docker compose exec jobmanager /opt/bicycle/bin/demo-client \
    --jobmanager localhost:9000 workers

# List jobs
docker compose exec jobmanager /opt/bicycle/bin/demo-client \
    --jobmanager localhost:9000 jobs

# Submit a custom job
docker compose exec jobmanager /opt/bicycle/bin/demo-client \
    --jobmanager localhost:9000 submit \
    --name my-wordcount \
    --parallelism 4

# Get job details
docker compose exec jobmanager /opt/bicycle/bin/demo-client \
    --jobmanager localhost:9000 job <job-id>

# View metrics
docker compose exec jobmanager /opt/bicycle/bin/demo-client \
    --jobmanager localhost:9000 metrics
```

### Step 5: Use the REST API

```bash
# Get cluster info
curl http://localhost:8081/api/cluster

# List workers
curl http://localhost:8081/api/cluster/workers

# List jobs
curl http://localhost:8081/api/jobs

# Get metrics
curl http://localhost:8081/api/metrics

# Submit a job (via POST)
curl -X POST http://localhost:8081/api/jobs \
    -H "Content-Type: application/json" \
    -d '{"name": "my-job", "parallelism": 2}'
```

### Step 6: Clean Up

```bash
docker compose down -v  # -v removes volumes
```

---

## Running a Cluster (Manual)

### Start the JobManager

```bash
cargo run -p jobmanager -- --bind 0.0.0.0:9000 --checkpoint-dir /tmp/bicycle/checkpoints
```

### Start Worker(s)

```bash
# Worker 1
cargo run -p worker -- --jobmanager 127.0.0.1:9000 --slots 4

# Worker 2 (on another terminal or machine)
cargo run -p worker -- --jobmanager 127.0.0.1:9000 --slots 4
```

### Start the Web UI

```bash
cargo run -p bicycle-webui -- --bind 0.0.0.0:8081 --jobmanager 127.0.0.1:9000
```

Then open http://localhost:8081 in your browser.

### Use the Demo Client

```bash
# Build the demo client
cargo build --release -p demo-client

# Check status
./target/release/demo-client --jobmanager 127.0.0.1:9000 status

# Submit a job
./target/release/demo-client --jobmanager 127.0.0.1:9000 submit --name test-job
```

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        Job Manager                          │
│  ┌─────────────┐  ┌──────────────┐  ┌───────────────────┐  │
│  │ Job Graph   │  │  Scheduler   │  │   Checkpoint      │  │
│  │ Management  │  │              │  │   Coordinator     │  │
│  └─────────────┘  └──────────────┘  └───────────────────┘  │
│  ┌─────────────┐  ┌──────────────┐                         │
│  │  Metrics    │  │   Worker     │                         │
│  │  Collector  │  │   Registry   │                         │
│  └─────────────┘  └──────────────┘                         │
└─────────────────────────────────────────────────────────────┘
              │ gRPC (port 9000)              │ REST (port 8081)
              │                               │
              │                        ┌──────┴──────┐
              │                        │   Web UI    │
              │                        │  Dashboard  │
              │                        └─────────────┘
              │
┌─────────────┼─────────────────┐
▼             ▼                 ▼
┌───────────────────┐ ┌───────────────────┐ ┌───────────────────┐
│     Worker 1      │ │     Worker 2      │ │     Worker 3      │
│  ┌─────────────┐  │ │  ┌─────────────┐  │ │  ┌─────────────┐  │
│  │   Task 1    │  │ │  │   Task 2    │  │ │  │   Task 3    │  │
│  │  (Source)   │──┼─┼─▶│  (Window)   │──┼─┼─▶│   (Sink)    │  │
│  └─────────────┘  │ │  └─────────────┘  │ │  └─────────────┘  │
│  ┌─────────────┐  │ │  ┌─────────────┐  │ │                   │
│  │  RocksDB    │  │ │  │  RocksDB    │  │ │                   │
│  │   State     │  │ │  │   State     │  │ │                   │
│  └─────────────┘  │ │  └─────────────┘  │ │                   │
└───────────────────┘ └───────────────────┘ └───────────────────┘
```

---

## Project Structure

```
bicycle/
├── crates/
│   ├── core/           # Core types: StreamMessage, Event, Window, TaskId
│   ├── runtime/        # Operator trait, channels, task execution, transactional sinks
│   ├── operators/      # Built-in operators (Map, Filter, Window, etc.)
│   ├── state/          # State backends (Memory, RocksDB)
│   ├── checkpoint/     # Checkpoint coordination and barrier tracking
│   ├── network/        # TCP data plane, serialization, flow control
│   ├── protocol/       # gRPC generated code from protobuf definitions
│   └── connectors/     # External system connectors (Kafka, Pulsar)
├── bin/
│   ├── mini-runner/    # Local demo pipeline (standalone)
│   ├── jobmanager/     # Control plane server (gRPC)
│   ├── worker/         # Task executor with heartbeat
│   ├── webui/          # Web dashboard and REST API
│   └── demo-client/    # CLI for cluster interaction
├── proto/              # gRPC service definitions
│   ├── control.proto   # Control plane RPC (job/task/cluster management)
│   └── data.proto      # Data plane messages
├── config/             # Configuration files
├── Dockerfile          # Multi-stage Docker build
└── docker-compose.yml  # Full cluster setup
```

---

## Usage Examples

### Operators

```rust
use bicycle_operators::{MapOperator, FilterOperator, TumblingWindowSum};
use bicycle_runtime::{spawn_operator, stream_channel};

// Create channels
let (tx1, rx1) = stream_channel::<Event<String, i64>>(32);
let (tx2, rx2) = stream_channel::<Event<String, i64>>(32);

// Filter: keep only positive values
spawn_operator(
    "filter",
    FilterOperator::new(|ev: &Event<String, i64>| ev.value > 0),
    rx1,
    tx2,
);

// Window: 5-second tumbling sum
spawn_operator(
    "window",
    TumblingWindowSum::new(5_000),
    rx2,
    tx3,
);
```

### State Management

```rust
use bicycle_state::{RocksDBStateBackend, ValueState, MapState};

// Create RocksDB backend
let backend = RocksDBStateBackend::new("/var/bicycle/state")?;
let store = backend.create_keyed_state_store("my-operator")?;

// Set current key (like Flink's KeyedProcessFunction)
store.set_current_key(b"user-123");

// Use ValueState
let counter: Box<dyn ValueState<i64>> = store.get_value_state("counter")?;
counter.set(42)?;
assert_eq!(counter.get()?, Some(42));

// Use MapState
let scores: Box<dyn MapState<String, i64>> = store.get_map_state("scores")?;
scores.put("game1".into(), 100)?;
scores.put("game2".into(), 200)?;
```

### Checkpointing

```rust
use bicycle_checkpoint::{CheckpointCoordinator, CheckpointConfig};
use std::time::Duration;

let config = CheckpointConfig {
    interval: Duration::from_secs(10),
    timeout: Duration::from_secs(600),
    num_retained: 3,
    ..Default::default()
};

let coordinator = CheckpointCoordinator::new(job_id, config, checkpoint_dir);

// Trigger checkpoint
let checkpoint_id = coordinator.trigger_checkpoint(false)?;

// Trigger savepoint (for upgrades/migrations)
let savepoint_id = coordinator.trigger_checkpoint(true)?;
```

### Kafka Connector

```rust
use bicycle_connectors::kafka::{KafkaSource, KafkaSink, KafkaConfig};

// Configure Kafka source
let source_config = KafkaConfig::new("localhost:9092")
    .with_group_id("my-consumer-group")
    .with_topics(vec!["input-topic".to_string()]);

let mut source = KafkaSource::new(source_config);
source.connect()?;

// Configure Kafka sink with exactly-once
let sink = KafkaSinkBuilder::new("localhost:9092")
    .topic("output-topic")
    .exactly_once("bicycle-sink")
    .build_transactional()?;
```

### Exactly-Once Sink

```rust
use bicycle_runtime::sink::{TransactionalSink, ExactlyOnceSinkWriter};

// Wrap any transactional sink for exactly-once semantics
let (commit_tx, commit_rx) = tokio::sync::mpsc::channel(16);
let writer = ExactlyOnceSinkWriter::new(sink, commit_tx);

// Process messages - transactions are managed automatically
writer.process(StreamMessage::Data(record)).await?;

// On checkpoint completion, commit pending transactions
writer.notify_checkpoint_complete(checkpoint_id).await?;
```

---

## Configuration

### JobManager (`config/jobmanager.yaml`)
```yaml
bind: "0.0.0.0:9000"
checkpoint_interval_ms: 10000
state_backend: "rocksdb"
state_path: "/var/bicycle/state"
checkpoint_path: "/var/bicycle/checkpoints"
```

### Worker (`config/worker.yaml`)
```yaml
jobmanager: "localhost:9000"
bind: "0.0.0.0:9001"
slots: 4
memory_mb: 4096
state_path: "/var/bicycle/state"
```

### Environment Variables
| Variable | Description | Default |
|----------|-------------|---------|
| `RUST_LOG` | Log level | `info` |
| `BICYCLE_STATE_DIR` | State storage path | `/var/bicycle/state` |
| `BICYCLE_CHECKPOINT_DIR` | Checkpoint path | `/var/bicycle/checkpoints` |
| `BICYCLE_JOBMANAGER` | JobManager address | `localhost:9000` |
| `BICYCLE_WORKER_SLOTS` | Task slots per worker | `4` |
| `BICYCLE_WORKER_MEMORY_MB` | Worker memory limit | `2048` |

---

## REST API

The Web UI provides a REST API for job and cluster management:

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/jobs` | GET | List all jobs |
| `/api/jobs` | POST | Submit a new job |
| `/api/jobs/{id}` | GET | Get job details |
| `/api/jobs/{id}` | DELETE | Cancel a job |
| `/api/jobs/{id}/tasks` | GET | Get tasks for a job |
| `/api/jobs/{id}/checkpoints` | GET | Get checkpoint history |
| `/api/cluster` | GET | Get cluster info (workers, slots, jobs) |
| `/api/cluster/workers` | GET | List workers with metrics |
| `/api/metrics` | GET | Get cluster metrics (throughput, records) |
| `/health` | GET | Health check |

### Example Responses

**GET /api/cluster**
```json
{
  "workers": 2,
  "total_slots": 8,
  "available_slots": 1,
  "running_jobs": 1
}
```

**GET /api/cluster/workers**
```json
[
  {
    "worker_id": "abc12345-...",
    "hostname": "worker-1",
    "slots": 4,
    "slots_used": 3,
    "cpu_usage": 45.2,
    "memory_used_mb": 512
  }
]
```

**GET /api/metrics**
```json
{
  "total_records_processed": 1250000,
  "total_bytes_processed": 52428800,
  "checkpoints_completed": 12,
  "uptime_seconds": 3600
}
```

---

## Development

```bash
# Build all crates
cargo build

# Run tests
cargo test

# Run specific test
cargo test -p bicycle-state

# Run with verbose logging
RUST_LOG=bicycle=debug cargo run -p mini-runner

# Format code
cargo fmt

# Lint
cargo clippy --all-targets

# Build documentation
cargo doc --open
```

---

## Troubleshooting

### Build Errors

**`libclang not found`**
```bash
# Ubuntu/Debian
sudo apt-get install libclang-dev

# macOS
brew install llvm
export LIBCLANG_PATH="$(brew --prefix llvm)/lib"
```

**`snappy/lz4/zstd not found`**
```bash
# Ubuntu/Debian
sudo apt-get install libsnappy-dev liblz4-dev libzstd-dev

# macOS
brew install snappy lz4 zstd
```

**Slow builds**

RocksDB and protobuf compilation is slow on first build. Subsequent builds use cached artifacts.

```bash
# Use more parallelism
CARGO_BUILD_JOBS=8 cargo build --release
```

### Docker Issues

**Workers not connecting**
```bash
# Check if JobManager is healthy
docker compose ps
docker compose logs jobmanager

# Verify network connectivity
docker compose exec worker-1 nc -z jobmanager 9000
```

**Demo not running**
```bash
# Make sure to use the profile flag
docker compose --profile demo up demo
```

---

## Roadmap

### Phase 1: Core Completion ✅
- [x] JobManager with gRPC API
- [x] Worker registration and heartbeat
- [x] Task scheduling with slot allocation
- [x] Web UI dashboard
- [x] Cluster monitoring and metrics
- [x] Demo client CLI
- [x] Task deployment from JobManager to Workers
- [x] Operator execution on Workers (demo operators)
- [x] Data plane communication setup (network layer)

### Phase 2: Production Features
- [ ] Checkpoint persistence and recovery
- [ ] Savepoint creation and restore
- [ ] Job restart on failure
- [ ] Backpressure propagation across network
- [ ] Dynamic scaling (add/remove workers)
- [ ] Resource isolation per task

### Phase 3: Advanced Features
- [ ] **SQL support** via Apache Calcite or DataFusion
- [ ] **More connectors**: Kinesis, RabbitMQ, Redis, PostgreSQL CDC
- [ ] **Auto-scaling** based on backpressure metrics
- [ ] **Savepoint management UI**
- [ ] **Job versioning** and rolling upgrades
- [ ] **Multi-tenancy** with resource quotas

### Phase 4: Enterprise Features
- [ ] High availability (HA) JobManager
- [ ] Kubernetes operator
- [ ] Prometheus metrics exporter
- [ ] Grafana dashboards
- [ ] Authentication and authorization
- [ ] Audit logging

### Contributing

Contributions are welcome! Priority areas:
1. Full data plane integration (connecting operators via network)
2. Checkpoint persistence and job recovery
3. Additional connectors (Kinesis, RabbitMQ, etc.)
4. SQL support via DataFusion
5. Documentation and examples

---

## Comparison with Apache Flink

| Feature | Flink | Bicycle |
|---------|-------|---------|
| Language | Java/Scala | Rust |
| State Backend | RocksDB, Memory | RocksDB, Memory |
| Checkpointing | Chandy-Lamport | Chandy-Lamport |
| Event Time | Yes | Yes |
| Exactly-Once | Yes | Yes |
| Kafka Connector | Yes | Yes |
| Web UI | Yes | Yes |
| SQL Support | Yes | Planned |
| Memory Safety | JVM GC | Rust ownership |
| Startup Time | Seconds | Milliseconds |
| Binary Size | ~100MB+ | ~20MB |

---

## License

Apache-2.0

---

## Acknowledgments

Inspired by:
- [Apache Flink](https://flink.apache.org/)
- [Timely Dataflow](https://github.com/TimelyDataflow/timely-dataflow)
- [Arroyo](https://github.com/ArroyoSystems/arroyo)
