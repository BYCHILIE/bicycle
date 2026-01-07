# Bicycle (incipient)

This is an **incipient, Rust-first** distributed streaming engine project scaffold.

**What works today (MVP-0):**
- A *true streaming* in-process pipeline with:
  - bounded channels (backpressure)
  - watermarks
  - a keyed tumbling window sum operator

**What is stubbed (next):**
- JobManager + Worker processes (control plane)
- networking channels (data plane)
- checkpoints and state backends

## Prerequisites
- Rust stable (see `rust-toolchain.toml`)

## Quick start
Run the local mini pipeline:

```bash
cargo run -p mini-runner
```

You should see output similar to:

```
window=[0..5000] key=a sum=2
window=[0..5000] key=b sum=1
window=[5000..10000] key=a sum=5
...
```

### Logging

```bash
RUST_LOG=info cargo run -p mini-runner
RUST_LOG=debug cargo run -p mini-runner
```

## Repo layout
- `crates/core` – StreamMessage, Event, WindowResult
- `crates/runtime` – Operator trait, bounded channels, spawning utilities
- `crates/operators` – Map, TumblingWindowSum
- `bin/mini-runner` – runnable demo pipeline
- `bin/jobmanager` – control-plane stub
- `bin/worker` – worker stub

## Next milestone (MVP-1)
To move toward “Flink-like” distributed streaming:
1. Add a control-plane RPC between JobManager and Workers (task deployment, heartbeats).
2. Introduce network channels for shuffles (data plane), keeping StreamMessage framing.
3. Add checkpoint barriers + state snapshot for `TumblingWindowSum`.

