# Bicycle

A **Rust-first distributed streaming engine** inspired by Apache Flink.

Bicycle provides exactly-once stream processing with a fluent DataStream API, native plugin system, Kafka integration, and a modern web UI.

---

## Features

- **Fluent DataStream API** — Flink-like typed pipeline builder with `AsyncFunction`, `RichAsyncFunction`, and `ProcessFunction`
- **Native plugin system** — Compile jobs as shared libraries (.so), loaded at runtime via `libloading`
- **Typed Kafka connectors** — `KafkaSourceBuilder<T>` / `KafkaSinkBuilder<T>` with JSON/String ser/de
- **Ordered/unordered async processing** — Control concurrency, timeouts, and result ordering
- **Operator chaining & optimization** — Automatic fusion, slot sharing, parallelism control
- **Distributed coordination** — JobManager + Workers with gRPC control plane, slot-based scheduling
- **Checkpointing** — Chandy-Lamport distributed snapshots, RocksDB state backend
- **Web UI** — Next.js dashboard with job graph visualization, task tracking, metrics
- **CLI** — Full cluster management (`bicycle status`, `submit`, `list`, `cancel`, etc.)

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
let env = StreamEnvironment::new();

// Socket -> Kafka
env.socket_source("0.0.0.0", 9999)
    .sink_to(KafkaSinkBuilder::<String>::new(&brokers, "raw-lines"));

// Kafka -> Process -> Kafka (typed)
env.add_source(KafkaSourceBuilder::<String>::new(&brokers, "raw-lines").group_id("my-group"))
    .process_plugin::<String>("WordSplitter")
    .set_parallelism(4)
    .key_by(|word: &String| word.clone())
    .process_ordered(WordCounter::new(), Duration::from_secs(60), 50)
    .sink_to(KafkaSinkBuilder::<WordCount>::new(&brokers, "word-counts"));

env.execute("wordcount")?;
```

---

## Architecture

```
                    ┌──────────────────────┐
                    │     JobManager       │  :9000 (gRPC)
                    │  scheduling, state,  │
                    │  checkpoint coord.   │
                    └──────────┬───────────┘
                               │
              ┌────────────────┼────────────────┐
              ▼                ▼                 ▼
     ┌──────────────┐ ┌──────────────┐  ┌──────────────┐
     │   Worker 1   │ │   Worker 2   │  │   Worker N   │
     │  tasks, FFI  │ │  tasks, FFI  │  │  tasks, FFI  │
     │  plugin load │ │  plugin load │  │  plugin load │
     └──────────────┘ └──────────────┘  └──────────────┘

     ┌──────────────┐ ┌──────────────┐
     │   REST API   │ │   Web UI     │  :3000
     │   (Rust)     │ │  (Next.js)   │
     └──────────────┘ └──────────────┘
```

---

## Project Structure

```
bicycle/
├── crates/
│   ├── core/           # StreamMessage, Event, Record, TaskId
│   ├── runtime/        # Operator trait, channels, emitter
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
