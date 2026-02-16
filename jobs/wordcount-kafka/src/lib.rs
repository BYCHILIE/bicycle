//! Word count Kafka plugin for Bicycle.
//!
//! Enhanced word count that uses Kafka as intermediate transport:
//!   socket source -> kafka sink 1 -> kafka source 1 -> word count -> kafka sink 2 -> socket sink
//!
//! Exports:
//! - `WordSplitter`: Splits input lines into words (AsyncFunction)
//! - `WordCounter`: Counts word occurrences, outputs WordCount (RichAsyncFunction - stateful)
//! - `WordCountFormatter`: Formats WordCount to display string (ProcessFunction - with context/timers)

use bicycle_api::prelude::*;
use bicycle_plugin::bicycle_plugin_all;
use std::collections::HashMap;

/// A typed word count record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WordCount {
    pub word: String,
    pub count: u64,
}

/// Splits input strings into lowercase words.
#[derive(Default)]
pub struct WordSplitter;

#[async_trait]
impl AsyncFunction for WordSplitter {
    type In = String;
    type Out = String;

    async fn process(&mut self, input: String, _ctx: &Context) -> Vec<String> {
        input
            .split_whitespace()
            .map(|word| word.to_lowercase())
            .filter(|word| !word.is_empty())
            .collect()
    }
}

/// Counts occurrences of each word (stateful). Outputs typed WordCount records.
#[derive(Default)]
pub struct WordCounter {
    counts: HashMap<String, u64>,
}

#[async_trait]
impl RichAsyncFunction for WordCounter {
    type In = String;
    type Out = WordCount;

    async fn open(&mut self, ctx: &RuntimeContext) -> Result<()> {
        if let Ok(Some(state)) = ctx.get_state::<HashMap<String, u64>>("counts") {
            self.counts = state;
            eprintln!("[WordCounter] Restored {} words from checkpoint", self.counts.len());
        }
        Ok(())
    }

    async fn process(&mut self, word: String, _ctx: &RuntimeContext) -> Vec<WordCount> {
        let count = self.counts.entry(word.clone()).or_insert(0);
        *count += 1;
        vec![WordCount {
            word,
            count: *count,
        }]
    }

    async fn snapshot(&self, ctx: &RuntimeContext) -> Result<()> {
        ctx.save_state("counts", &self.counts)
    }
}

/// Formats a WordCount record to a display string.
///
/// Implements both ProcessFunction (for process_fn() API with timestamps/timers)
/// and AsyncFunction (for plugin export).
#[derive(Default)]
pub struct WordCountFormatter;

#[async_trait]
impl ProcessFunction for WordCountFormatter {
    type In = WordCount;
    type Out = String;

    async fn process_element(&mut self, value: WordCount, ctx: &ProcessContext) -> Vec<String> {
        let ts = ctx.timestamp.unwrap_or(0);
        vec![format!("{}:{} (ts={})", value.word, value.count, ts)]
    }
}

#[async_trait]
impl AsyncFunction for WordCountFormatter {
    type In = WordCount;
    type Out = String;

    async fn process(&mut self, value: WordCount, _ctx: &Context) -> Vec<String> {
        vec![format!("{}:{}", value.word, value.count)]
    }
}

// Export all plugin functions (async + rich)
bicycle_plugin_all! {
    async: [WordSplitter, WordCountFormatter],
    rich: [WordCounter],
}
