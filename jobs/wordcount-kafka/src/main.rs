//! Enhanced word count job with Kafka pipeline.
//!
//! Flow:
//!   socket source -> kafka sink 1 (raw-lines) ->
//!   kafka source 1 (raw-lines) -> word splitter (unordered) -> word counter (ordered) ->
//!   kafka sink 2 (word-counts as WordCount JSON) ->
//!   kafka source 2 (word-counts as WordCount) -> formatter (ProcessFunction) -> socket sink
//!
//! This demonstrates:
//! - Typed `DataStream<WordCount>` through Kafka with JSON ser/de
//! - Ordered/unordered async processing with timeout and capacity
//! - AsyncFunction, RichAsyncFunction, and ProcessFunction usage
//!
//! # Usage
//!
//! ```bash
//! # Build
//! cargo build -p wordcount-kafka --release
//!
//! # Submit to cluster
//! bicycle submit ./target/release/wordcount-kafka \
//!   --plugin ./target/release/libwordcount_kafka.so
//!
//! # Connect
//! nc localhost 9999   # Send input text
//! nc localhost 9998   # Receive word counts
//! ```

use bicycle_api::prelude::*;
use std::time::Duration;
use wordcount_kafka::{WordCount, WordCountFormatter, WordCounter, WordSplitter};

fn main() {
    let brokers = std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".into());
    let topic_raw = std::env::var("KAFKA_TOPIC_RAW").unwrap_or_else(|_| "raw-lines".into());
    let topic_counts = std::env::var("KAFKA_TOPIC_COUNTS").unwrap_or_else(|_| "word-counts".into());
    let group_id = std::env::var("KAFKA_GROUP_ID").unwrap_or_else(|_| "wordcount-kafka-group".into());
    let source_port: u16 = std::env::var("SOURCE_PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(9999);
    let sink_port: u16 = std::env::var("SINK_PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(9998);
    let parallelism: u32 = std::env::var("PARALLELISM").ok().and_then(|p| p.parse().ok()).unwrap_or(2);

    let env = StreamEnvironment::builder()
        .parallelism(parallelism)
        .max_parallelism(16)
        .checkpoint_interval(30_000)
        .build();

    // Stage 1: Socket input -> Kafka "raw-lines" topic (String)
    // Ingests raw text lines from a socket and publishes them to Kafka.
    // Socket source pinned to parallelism=1 so only one listener binds the port.
    env.socket_source("0.0.0.0", source_port)
        .uid("socket-source-v1")
        .name("Socket Input")
        .set_parallelism(1)
        .sink_to(
            KafkaSinkBuilder::<String>::new(&brokers, &topic_raw)
                .property("acks", "all"),
        )
        .uid("kafka-sink-raw-v1")
        .name("Kafka Sink (raw-lines)");

    // Stage 2: Kafka "raw-lines" -> split (unordered) -> count (ordered) -> Kafka "word-counts"
    //
    // WordSplitter (AsyncFunction) uses unordered processing: order doesn't matter
    // for splitting, and unordered gives better throughput.
    //   timeout=30s, capacity=100 concurrent
    //
    // WordCounter (RichAsyncFunction) uses ordered processing: we want counts to
    // arrive in order per key for consistency.
    //   timeout=60s, capacity=50 concurrent
    env.add_source(
        KafkaSourceBuilder::<String>::new(&brokers, &topic_raw)
            .group_id(&group_id)
            .property("auto.offset.reset", "earliest"),
    )
    .uid("kafka-source-raw-v1")
    .name("Kafka Source (raw-lines)")
    .process_unordered(WordSplitter::new(), Duration::from_secs(30), 100)
    .uid("splitter-v1")
    .name("Word Splitter")
    .set_parallelism(4)
    .slot_sharing_group("processing")
    .key_by(|word: &String| word.clone())
    .process_ordered(WordCounter::new(), Duration::from_secs(60), 50)
    .uid("counter-v1")
    .name("Word Counter")
    .sink_to(KafkaSinkBuilder::<WordCount>::new(&brokers, &topic_counts))
    .uid("kafka-sink-counts-v1")
    .name("Kafka Sink (word-counts)");

    // Stage 3: Kafka "word-counts" (WordCount) -> format (ProcessFunction) -> Socket output
    //
    // WordCountFormatter is a ProcessFunction — it has access to ProcessContext
    // with timestamps, timer service, and side output capabilities.
    // The Kafka source deserializes JSON into typed WordCount structs.
    env.add_source(
        KafkaSourceBuilder::<WordCount>::new(&brokers, &topic_counts)
            .group_id("wordcount-output-group"),
    )
    .uid("kafka-source-counts-v1")
    .name("Kafka Source (word-counts)")
    .process_fn(<WordCountFormatter as ProcessFunction>::new())
    .uid("formatter-v1")
    .name("Format Output")
    .set_parallelism(1)
    .socket_sink("0.0.0.0", sink_port)
    .uid("socket-sink-v1")
    .name("Socket Output")
    .set_parallelism(1);

    // Build and optimize the job graph
    let (graph, optimized) = env
        .execute_optimized("wordcount-kafka")
        .expect("Failed to build job graph");

    eprintln!();
    optimized.print_summary();
    eprintln!();

    // Output the graph as JSON for the CLI to capture
    let json = serde_json::to_string_pretty(&graph).expect("Failed to serialize graph");
    println!("{}", json);
}
