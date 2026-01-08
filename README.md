# Bicycle 🚴

A **Rust-first distributed streaming engine** inspired by Apache Flink.

Bicycle provides exactly-once stream processing semantics with a clean, idiomatic Rust API and production-grade state management via RocksDB.

## Features

### Core Capabilities
- ✅ **Streaming operators**: Map, Filter, FlatMap, KeyBy, Reduce
- ✅ **Window operations**: Tumbling, Sliding, Session windows
- ✅ **Event-time processing**: Watermarks, late event handling
- ✅ **Backpressure**: Bounded channels with credit-based flow control
- ✅ **State management**: Memory and RocksDB state backends
- ✅ **Checkpointing**: Chandy-Lamport distributed snapshots with barrier alignment
- ✅ **Network layer**: TCP-based data plane with efficient serialization

### Planned
- 🔜 Code refactoring
- 🔜 Full JobManager/Worker coordination
- 🔜 Exactly-once sink guarantees
- 🔜 Kafka/Pulsar connectors
- 🔜 Web UI for job management

---

## Quick Start

### Option 1: Docker (Recommended)

```bash
# Build and run the demo
docker compose up demo

# Or run the full cluster
docker compose up -d

# View logs
docker compose logs -f

# Stop
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

# Run the demo pipeline
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

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        Job Manager                          │
│  ┌─────────────┐  ┌──────────────┐  ┌───────────────────┐  │
│  │ Job Graph   │  │  Scheduler   │  │   Checkpoint      │  │
│  │ Management  │  │              │  │   Coordinator     │  │
│  └─────────────┘  └──────────────┘  └───────────────────┘  │
└─────────────────────────────────────────────────────────────┘
                              │ gRPC (port 9000)
            ┌─────────────────┼─────────────────┐
            ▼                 ▼                 ▼
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
│   ├── runtime/        # Operator trait, channels, task execution
│   ├── operators/      # Built-in operators (Map, Filter, Window, etc.)
│   ├── state/          # State backends (Memory, RocksDB)
│   ├── checkpoint/     # Checkpoint coordination and barrier tracking
│   └── network/        # TCP data plane, serialization, flow control
├── bin/
│   ├── mini-runner/    # Local demo pipeline
│   ├── jobmanager/     # Control plane server
│   └── worker/         # Task executor
├── proto/              # gRPC service definitions
│   ├── control.proto   # Control plane RPC
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

RocksDB compilation is slow on first build. Subsequent builds use cached artifacts.

```bash
# Use more parallelism
CARGO_BUILD_JOBS=8 cargo build --release
```

---

## Comparison with Apache Flink

| Feature | Flink | Bicycle |
|---------|-------|---------|
| Language | Java/Scala | Rust |
| State Backend | RocksDB, Memory | RocksDB, Memory |
| Checkpointing | Chandy-Lamport | Chandy-Lamport |
| Event Time | ✅ | ✅ |
| Exactly-Once | ✅ | ✅ (in progress) |
| SQL Support | ✅ | 🔜 |
| Memory Safety | JVM GC | Rust ownership |
| Startup Time | Seconds | Milliseconds |

---

## License

Apache-2.0

---

## Acknowledgments

Inspired by:
- [Apache Flink](https://flink.apache.org/)
- [Timely Dataflow](https://github.com/TimelyDataflow/timely-dataflow)
- [Arroyo](https://github.com/ArroyoSystems/arroyo)
