use anyhow::Result;
use bicycle_core::{Event, StreamMessage, Timestamp, WindowResult};
use bicycle_operators::{MapOperator, TumblingWindowSum};
use bicycle_runtime::{spawn_operator, spawn_sink, stream_channel};
use tokio::time::{sleep, Duration};
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse()?))
        .init();

    // Bounded channels enforce backpressure (at least locally).
    let (tx0, rx0) = stream_channel::<Event<String, i64>>(32);
    let (tx1, rx1) = stream_channel::<Event<String, i64>>(32);
    let (tx2, rx2) = stream_channel::<WindowResult<String, i64>>(32);

    // map: identity (placeholder to show how to add operators)
    spawn_operator("map", MapOperator::new(|ev: Event<String, i64>| ev), rx0, tx1);

    // window: 5s tumbling sum over event-time
    spawn_operator("tumbling_window_sum", TumblingWindowSum::new(5_000), rx1, tx2);

    // sink: print window results
    spawn_sink("stdout_sink", rx2, |msg| {
        match msg {
            StreamMessage::Data(WindowResult {
                window_start,
                window_end,
                key,
                value,
            }) => {
                println!(
                    "window=[{}..{}] key={} sum={}",
                    window_start, window_end, key, value
                );
            }
            StreamMessage::Watermark(wm) => {
                info!(watermark = wm, "watermark advanced");
            }
            StreamMessage::Barrier(id) => {
                info!(checkpoint_id = id, "barrier observed at sink");
            }
            StreamMessage::End => {
                info!("stream ended");
            }
        }
        Ok(())
    });

    // Source (in-process) producing a small event stream.
    source_task(tx0).await?;

    // Give operators a moment to drain (MVP).
    sleep(Duration::from_millis(200)).await;
    Ok(())
}

async fn source_task(tx: tokio::sync::mpsc::Sender<StreamMessage<Event<String, i64>>>) -> Result<()> {
    // Synthetic out-of-order stream.
    let events: Vec<(Timestamp, &str, i64)> = vec![
        (1_000, "a", 1),
        (2_000, "b", 1),
        (6_000, "a", 5),
        (3_000, "a", 1), // out-of-order but within watermark lateness
        (7_000, "b", 2),
        (10_000, "a", 3),
        (12_000, "b", 1),
        (4_000, "b", 10), // likely late depending on watermark
    ];

    let mut max_ts: Timestamp = 0;
    let lateness_ms: Timestamp = 2_000;

    for (i, (ts, key, value)) in events.into_iter().enumerate() {
        max_ts = max_ts.max(ts);

        tx.send(StreamMessage::Data(Event {
            ts,
            key: key.to_string(),
            value,
        }))
        .await?;

        // Periodically emit a watermark.
        if i % 3 == 2 {
            let wm = max_ts.saturating_sub(lateness_ms);
            tx.send(StreamMessage::Watermark(wm)).await?;
        }

        sleep(Duration::from_millis(50)).await;
    }

    // Final watermark to flush all windows.
    tx.send(StreamMessage::Watermark(max_ts + 10_000)).await?;
    tx.send(StreamMessage::End).await?;

    Ok(())
}
