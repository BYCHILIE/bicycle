//! Function traits for user-defined operators.
//!
//! This module defines the core traits that users implement to create
//! custom streaming operators, similar to Flink's AsyncFunction and
//! RichAsyncFunction.

use crate::context::{Context, RuntimeContext};
use crate::Result;
use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};

/// A stateless async function for processing stream elements.
///
/// This is the simplest function type, suitable for operations that don't
/// need to maintain state across elements or access runtime context.
///
/// # Example
///
/// ```ignore
/// use bicycle_api::prelude::*;
///
/// struct ToUppercase;
///
/// #[async_trait]
/// impl AsyncFunction for ToUppercase {
///     type In = String;
///     type Out = String;
///
///     async fn process(&mut self, input: String, _ctx: &Context) -> Vec<String> {
///         vec![input.to_uppercase()]
///     }
/// }
/// ```
#[async_trait]
pub trait AsyncFunction: Send + Sync + 'static {
    /// Input type.
    type In: Serialize + DeserializeOwned + Send + Sync + 'static;

    /// Output type.
    type Out: Serialize + DeserializeOwned + Send + Sync + 'static;

    /// Process a single input element and produce zero or more outputs.
    async fn process(&mut self, input: Self::In, ctx: &Context) -> Vec<Self::Out>;

    /// Called once when the operator starts.
    async fn open(&mut self, _ctx: &Context) -> Result<()> {
        Ok(())
    }

    /// Called once when the operator closes.
    async fn close(&mut self) -> Result<()> {
        Ok(())
    }

    /// Returns the name of this function for debugging/logging.
    fn name(&self) -> &str {
        std::any::type_name::<Self>()
    }

    /// Create a new instance. Available on any type that derives Default.
    fn new() -> Self where Self: Default {
        Self::default()
    }
}

/// A stateful async function with access to keyed state and runtime context.
///
/// This is the more powerful function type, suitable for operations that need
/// to maintain state (like aggregations, joins, or sessions) or access
/// runtime information.
///
/// State is automatically checkpointed and restored on failure recovery.
///
/// # Example
///
/// ```ignore
/// use bicycle_api::prelude::*;
/// use std::collections::HashMap;
///
/// struct WordCounter {
///     counts: HashMap<String, u64>,
/// }
///
/// #[async_trait]
/// impl RichAsyncFunction for WordCounter {
///     type In = String;
///     type Out = String;
///
///     async fn open(&mut self, ctx: &RuntimeContext) -> Result<()> {
///         // Restore state from checkpoint if available
///         if let Some(state) = ctx.get_state("counts")? {
///             self.counts = state;
///         }
///         Ok(())
///     }
///
///     async fn process(&mut self, word: String, ctx: &RuntimeContext) -> Vec<String> {
///         let count = self.counts.entry(word.clone()).or_insert(0);
///         *count += 1;
///         vec![format!("{}:{}", word, count)]
///     }
///
///     async fn snapshot(&self, ctx: &RuntimeContext) -> Result<()> {
///         ctx.save_state("counts", &self.counts)
///     }
/// }
/// ```
#[async_trait]
pub trait RichAsyncFunction: Send + Sync + 'static {
    /// Input type.
    type In: Serialize + DeserializeOwned + Send + Sync + 'static;

    /// Output type.
    type Out: Serialize + DeserializeOwned + Send + Sync + 'static;

    /// Process a single input element with full runtime context.
    ///
    /// This method is called for each element in the input stream.
    /// Use the RuntimeContext to access keyed state.
    async fn process(&mut self, input: Self::In, ctx: &RuntimeContext) -> Vec<Self::Out>;

    /// Called once when the operator starts.
    ///
    /// This is where you should restore state from a checkpoint if available.
    async fn open(&mut self, _ctx: &RuntimeContext) -> Result<()> {
        Ok(())
    }

    /// Called once when the operator closes.
    ///
    /// Override this to perform cleanup logic.
    async fn close(&mut self) -> Result<()> {
        Ok(())
    }

    /// Called on checkpoint to save state.
    ///
    /// This method is called during checkpoint barriers. Use the RuntimeContext
    /// to persist any state that should survive failures.
    async fn snapshot(&self, _ctx: &RuntimeContext) -> Result<()> {
        Ok(())
    }

    /// Returns the name of this function for debugging/logging.
    fn name(&self) -> &str {
        std::any::type_name::<Self>()
    }

    /// Create a new instance. Available on any type that derives Default.
    fn new() -> Self where Self: Default {
        Self::default()
    }
}

/// A simple map function that transforms each element one-to-one.
#[async_trait]
pub trait MapFunction: Send + Sync + 'static {
    type In: Serialize + DeserializeOwned + Send + Sync + 'static;
    type Out: Serialize + DeserializeOwned + Send + Sync + 'static;

    /// Transform a single element.
    async fn map(&mut self, input: Self::In) -> Self::Out;
}

/// A flat map function that transforms each element into zero or more elements.
#[async_trait]
pub trait FlatMapFunction: Send + Sync + 'static {
    type In: Serialize + DeserializeOwned + Send + Sync + 'static;
    type Out: Serialize + DeserializeOwned + Send + Sync + 'static;

    /// Transform a single element into zero or more elements.
    async fn flat_map(&mut self, input: Self::In) -> Vec<Self::Out>;
}

/// A filter function that decides whether to keep elements.
#[async_trait]
pub trait FilterFunction: Send + Sync + 'static {
    type T: Serialize + DeserializeOwned + Send + Sync + 'static;

    /// Return true to keep the element, false to filter it out.
    async fn filter(&mut self, input: &Self::T) -> bool;
}

/// A key selector function for partitioning streams.
pub trait KeySelector: Send + Sync + 'static {
    type T: Send + Sync + 'static;
    type Key: Serialize + DeserializeOwned + Send + Sync + Clone + std::hash::Hash + Eq + 'static;

    /// Extract a key from an element.
    fn get_key(&self, input: &Self::T) -> Self::Key;
}

/// A reduce function for combining elements.
#[async_trait]
pub trait ReduceFunction: Send + Sync + 'static {
    type T: Serialize + DeserializeOwned + Send + Sync + Clone + 'static;

    /// Combine two elements into one.
    async fn reduce(&mut self, a: Self::T, b: Self::T) -> Self::T;
}

/// An aggregate function for computing aggregates over windows.
#[async_trait]
pub trait AggregateFunction: Send + Sync + 'static {
    /// Input type.
    type In: Serialize + DeserializeOwned + Send + Sync + 'static;
    /// Accumulator type.
    type Acc: Serialize + DeserializeOwned + Send + Sync + Default + Clone + 'static;
    /// Output type.
    type Out: Serialize + DeserializeOwned + Send + Sync + 'static;

    /// Create the initial accumulator value.
    fn create_accumulator(&self) -> Self::Acc {
        Self::Acc::default()
    }

    /// Add an element to the accumulator.
    async fn add(&mut self, acc: &mut Self::Acc, input: Self::In);

    /// Get the result from the accumulator.
    async fn get_result(&self, acc: &Self::Acc) -> Self::Out;

    /// Merge two accumulators (for session window merging).
    async fn merge(&mut self, a: Self::Acc, b: Self::Acc) -> Self::Acc;
}

// Blanket implementation: MapFunction -> AsyncFunction
#[async_trait]
impl<F: MapFunction> AsyncFunction for MapFunctionAdapter<F> {
    type In = F::In;
    type Out = F::Out;

    async fn process(&mut self, input: Self::In, _ctx: &Context) -> Vec<Self::Out> {
        vec![self.0.map(input).await]
    }
}

/// Adapter to use MapFunction as AsyncFunction.
pub struct MapFunctionAdapter<F>(pub F);

// Blanket implementation: FlatMapFunction -> AsyncFunction
#[async_trait]
impl<F: FlatMapFunction> AsyncFunction for FlatMapFunctionAdapter<F> {
    type In = F::In;
    type Out = F::Out;

    async fn process(&mut self, input: Self::In, _ctx: &Context) -> Vec<Self::Out> {
        self.0.flat_map(input).await
    }
}

/// Adapter to use FlatMapFunction as AsyncFunction.
pub struct FlatMapFunctionAdapter<F>(pub F);

// Blanket implementation: FilterFunction -> AsyncFunction
#[async_trait]
impl<F: FilterFunction> AsyncFunction for FilterFunctionAdapter<F> {
    type In = F::T;
    type Out = F::T;

    async fn process(&mut self, input: Self::In, _ctx: &Context) -> Vec<Self::Out> {
        if self.0.filter(&input).await {
            vec![input]
        } else {
            vec![]
        }
    }
}

/// Adapter to use FilterFunction as AsyncFunction.
pub struct FilterFunctionAdapter<F>(pub F);

// ============================================================================
// Process Functions (like Flink's ProcessFunction hierarchy)
// ============================================================================

/// Context for process functions providing timestamp and side output capabilities.
pub struct ProcessContext {
    /// Current element timestamp (event time).
    pub timestamp: Option<i64>,
    /// Timer service for registering timers.
    pub timer_service: TimerService,
    side_outputs: std::sync::Mutex<Vec<(String, Vec<u8>)>>,
}

impl ProcessContext {
    pub fn new() -> Self {
        Self {
            timestamp: None,
            timer_service: TimerService::new(),
            side_outputs: std::sync::Mutex::new(Vec::new()),
        }
    }

    /// Emit a value to a side output identified by the given tag.
    pub fn side_output<T: Serialize>(&self, tag: &str, value: &T) {
        if let Ok(bytes) = serde_json::to_vec(value) {
            self.side_outputs.lock().unwrap().push((tag.to_string(), bytes));
        }
    }

    /// Drain collected side outputs.
    pub fn take_side_outputs(&self) -> Vec<(String, Vec<u8>)> {
        std::mem::take(&mut *self.side_outputs.lock().unwrap())
    }
}

/// Keyed process context extending ProcessContext with current key access.
pub struct KeyedProcessContext<K> {
    /// The base process context.
    pub ctx: ProcessContext,
    /// The current key being processed.
    pub current_key: K,
}

impl<K> KeyedProcessContext<K> {
    pub fn new(key: K) -> Self {
        Self {
            ctx: ProcessContext::new(),
            current_key: key,
        }
    }
}

/// Timer service for registering event-time and processing-time timers.
pub struct TimerService {
    event_time_timers: std::sync::Mutex<Vec<i64>>,
    processing_time_timers: std::sync::Mutex<Vec<i64>>,
}

impl TimerService {
    pub fn new() -> Self {
        Self {
            event_time_timers: std::sync::Mutex::new(Vec::new()),
            processing_time_timers: std::sync::Mutex::new(Vec::new()),
        }
    }

    /// Register an event-time timer.
    pub fn register_event_time_timer(&self, timestamp: i64) {
        self.event_time_timers.lock().unwrap().push(timestamp);
    }

    /// Register a processing-time timer.
    pub fn register_processing_time_timer(&self, timestamp: i64) {
        self.processing_time_timers.lock().unwrap().push(timestamp);
    }

    /// Delete an event-time timer.
    pub fn delete_event_time_timer(&self, timestamp: i64) {
        self.event_time_timers.lock().unwrap().retain(|&t| t != timestamp);
    }

    /// Delete a processing-time timer.
    pub fn delete_processing_time_timer(&self, timestamp: i64) {
        self.processing_time_timers.lock().unwrap().retain(|&t| t != timestamp);
    }
}

/// Context for timer callbacks.
pub struct OnTimerContext {
    /// The firing timestamp.
    pub timestamp: i64,
    /// Timer service for registering new timers.
    pub timer_service: TimerService,
}

/// Keyed timer callback context.
pub struct KeyedOnTimerContext<K> {
    /// The firing timestamp.
    pub timestamp: i64,
    /// The current key.
    pub current_key: K,
    /// Timer service for registering new timers.
    pub timer_service: TimerService,
}

/// A low-level process function with access to context, timers, and side outputs.
///
/// Similar to Flink's ProcessFunction.
#[async_trait]
pub trait ProcessFunction: Send + Sync + 'static {
    type In: Serialize + DeserializeOwned + Send + Sync + 'static;
    type Out: Serialize + DeserializeOwned + Send + Sync + 'static;

    /// Process a single element.
    async fn process_element(&mut self, value: Self::In, ctx: &ProcessContext) -> Vec<Self::Out>;

    /// Called when a registered timer fires.
    async fn on_timer(&mut self, _timestamp: i64, _ctx: &OnTimerContext) -> Vec<Self::Out> {
        vec![]
    }

    fn name(&self) -> &str {
        std::any::type_name::<Self>()
    }

    /// Create a new instance. Available on any type that derives Default.
    fn new() -> Self where Self: Default {
        Self::default()
    }
}

/// A keyed process function with access to keyed state, timers, and side outputs.
///
/// Similar to Flink's KeyedProcessFunction.
#[async_trait]
pub trait KeyedProcessFunction: Send + Sync + 'static {
    type In: Serialize + DeserializeOwned + Send + Sync + 'static;
    type Key: Serialize + DeserializeOwned + Send + Sync + Clone + std::hash::Hash + Eq + 'static;
    type Out: Serialize + DeserializeOwned + Send + Sync + 'static;

    /// Process a single element with keyed context.
    async fn process_element(&mut self, value: Self::In, ctx: &KeyedProcessContext<Self::Key>) -> Vec<Self::Out>;

    /// Called when a registered timer fires.
    async fn on_timer(&mut self, _timestamp: i64, _ctx: &KeyedOnTimerContext<Self::Key>) -> Vec<Self::Out> {
        vec![]
    }

    fn name(&self) -> &str {
        std::any::type_name::<Self>()
    }
}

// ============================================================================
// Async I/O Functions (like Flink's AsyncDataStream)
// ============================================================================

/// An async I/O function for external calls (DB lookups, REST APIs, etc.).
///
/// Similar to Flink's AsyncFunction / AsyncDataStream.
#[async_trait]
pub trait AsyncIOFunction: Send + Sync + 'static {
    type In: Serialize + DeserializeOwned + Send + Sync + 'static;
    type Out: Serialize + DeserializeOwned + Send + Sync + 'static;

    /// Perform an async invocation (e.g., database lookup, HTTP request).
    async fn async_invoke(&mut self, input: Self::In) -> Vec<Self::Out>;

    /// Called when the async invocation times out.
    async fn timeout(&mut self, _input: Self::In) -> Vec<Self::Out> {
        vec![]
    }

    fn name(&self) -> &str {
        std::any::type_name::<Self>()
    }
}

// ============================================================================
// Connected Stream Functions
// ============================================================================

/// A co-map function that processes elements from two connected streams.
///
/// Similar to Flink's CoMapFunction.
#[async_trait]
pub trait CoMapFunction: Send + Sync + 'static {
    type In1: Serialize + DeserializeOwned + Send + Sync + 'static;
    type In2: Serialize + DeserializeOwned + Send + Sync + 'static;
    type Out: Serialize + DeserializeOwned + Send + Sync + 'static;

    /// Process an element from the first input.
    async fn map1(&mut self, value: Self::In1) -> Self::Out;

    /// Process an element from the second input.
    async fn map2(&mut self, value: Self::In2) -> Self::Out;
}

/// A co-process function for stateful processing on connected streams.
///
/// Similar to Flink's CoProcessFunction.
#[async_trait]
pub trait CoProcessFunction: Send + Sync + 'static {
    type In1: Serialize + DeserializeOwned + Send + Sync + 'static;
    type In2: Serialize + DeserializeOwned + Send + Sync + 'static;
    type Out: Serialize + DeserializeOwned + Send + Sync + 'static;

    /// Process an element from the first input.
    async fn process_element1(&mut self, value: Self::In1, ctx: &RuntimeContext) -> Vec<Self::Out>;

    /// Process an element from the second input.
    async fn process_element2(&mut self, value: Self::In2, ctx: &RuntimeContext) -> Vec<Self::Out>;

    fn name(&self) -> &str {
        std::any::type_name::<Self>()
    }
}

/// A join function that combines matched elements from a join.
///
/// Similar to Flink's JoinFunction.
#[async_trait]
pub trait JoinFunction: Send + Sync + 'static {
    type In1: Serialize + DeserializeOwned + Send + Sync + 'static;
    type In2: Serialize + DeserializeOwned + Send + Sync + 'static;
    type Out: Serialize + DeserializeOwned + Send + Sync + 'static;

    /// Combine a pair of joined elements.
    async fn join(&mut self, first: Self::In1, second: Self::In2) -> Self::Out;
}

// ============================================================================
// Serialization / Deserialization Schemas (like Flink's SerializationSchema)
// ============================================================================

/// Schema for deserializing bytes from external sources (like Flink's DeserializationSchema).
pub trait DeserializationSchema: Send + Sync + 'static {
    type Out: Serialize + DeserializeOwned + Send + Sync + 'static;
    fn deserialize(&self, bytes: &[u8]) -> Option<Self::Out>;
}

/// Schema for serializing records to bytes for external sinks (like Flink's SerializationSchema).
pub trait SerializationSchema: Send + Sync + 'static {
    type In: Serialize + DeserializeOwned + Send + Sync + 'static;
    fn serialize(&self, value: &Self::In) -> Vec<u8>;
}

/// Connector-level schema that describes how to encode/decode at the Kafka boundary.
///
/// Used with `.with_schema()` on `KafkaSourceBuilder` / `KafkaSinkBuilder` to insert
/// the appropriate property key into the connector configuration.
pub trait ConnectorSchema: Send + Sync + 'static {
    /// Returns `(property_key, property_value)` for the source (decoder) side.
    fn decoder_property(&self) -> (&'static str, String);
    /// Returns `(property_key, property_value)` for the sink (encoder) side.
    fn encoder_property(&self) -> (&'static str, String);
}

impl ConnectorSchema for StringSchema {
    fn decoder_property(&self) -> (&'static str, String) {
        ("value.deserializer", "string".to_string())
    }
    fn encoder_property(&self) -> (&'static str, String) {
        ("value.serializer", "string".to_string())
    }
}

impl<T: Send + Sync + 'static> ConnectorSchema for JsonSchema<T> {
    fn decoder_property(&self) -> (&'static str, String) {
        ("value.deserializer", "json".to_string())
    }
    fn encoder_property(&self) -> (&'static str, String) {
        ("value.serializer", "json".to_string())
    }
}

/// UTF-8 string passthrough schema (default for socket/kafka string streams).
pub struct StringSchema;

impl DeserializationSchema for StringSchema {
    type Out = String;
    fn deserialize(&self, bytes: &[u8]) -> Option<String> {
        std::str::from_utf8(bytes).ok().map(String::from)
    }
}

impl SerializationSchema for StringSchema {
    type In = String;
    fn serialize(&self, value: &String) -> Vec<u8> {
        value.as_bytes().to_vec()
    }
}

/// JSON serialization/deserialization schema for typed structs.
pub struct JsonSchema<T>(std::marker::PhantomData<T>);

impl<T> JsonSchema<T> {
    pub fn new() -> Self {
        Self(std::marker::PhantomData)
    }
}

impl<T> Default for JsonSchema<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Serialize + DeserializeOwned + Send + Sync + 'static> DeserializationSchema
    for JsonSchema<T>
{
    type Out = T;
    fn deserialize(&self, bytes: &[u8]) -> Option<T> {
        serde_json::from_slice(bytes).ok()
    }
}

impl<T: Serialize + DeserializeOwned + Send + Sync + 'static> SerializationSchema
    for JsonSchema<T>
{
    type In = T;
    fn serialize(&self, value: &T) -> Vec<u8> {
        serde_json::to_vec(value).unwrap_or_default()
    }
}

/// Bincode serialization schema for compact binary encoding.
#[cfg(feature = "bincode-serde")]
pub struct BincodeSchema<T> {
    /// Optional plugin function name for schema dispatch via `.named("my_fn")`.
    pub fn_name: Option<String>,
    _marker: std::marker::PhantomData<T>,
}

#[cfg(feature = "bincode-serde")]
impl<T> BincodeSchema<T> {
    pub fn new() -> Self {
        Self { fn_name: None, _marker: std::marker::PhantomData }
    }

    /// Create a schema that dispatches to a plugin-exported schema function named
    /// `bicycle_schema_decode_{name}` / `bicycle_schema_encode_{name}`.
    pub fn named(name: &str) -> Self {
        Self { fn_name: Some(name.to_string()), _marker: std::marker::PhantomData }
    }
}

#[cfg(feature = "bincode-serde")]
impl<T> Default for BincodeSchema<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(feature = "bincode-serde")]
impl<T: Send + Sync + 'static> ConnectorSchema for BincodeSchema<T> {
    fn decoder_property(&self) -> (&'static str, String) {
        if let Some(ref name) = self.fn_name {
            ("schema.decode.fn", format!("bicycle_schema_decode_{}", name))
        } else {
            ("value.deserializer", "bincode".to_string())
        }
    }
    fn encoder_property(&self) -> (&'static str, String) {
        if let Some(ref name) = self.fn_name {
            ("schema.encode.fn", format!("bicycle_schema_encode_{}", name))
        } else {
            ("value.serializer", "bincode".to_string())
        }
    }
}

#[cfg(feature = "bincode-serde")]
impl<T: Serialize + DeserializeOwned + Send + Sync + 'static> DeserializationSchema
    for BincodeSchema<T>
{
    type Out = T;
    fn deserialize(&self, bytes: &[u8]) -> Option<T> {
        bincode::deserialize(bytes).ok()
    }
}

#[cfg(feature = "bincode-serde")]
impl<T: Serialize + DeserializeOwned + Send + Sync + 'static> SerializationSchema
    for BincodeSchema<T>
{
    type In = T;
    fn serialize(&self, value: &T) -> Vec<u8> {
        bincode::serialize(value).unwrap_or_default()
    }
}

/// Avro serialization schema with explicit schema definition.
#[cfg(feature = "avro")]
pub struct AvroSchema<T> {
    schema: apache_avro::Schema,
    _marker: std::marker::PhantomData<T>,
    /// Optional plugin function name for schema dispatch via `.named()`.
    pub fn_name: Option<String>,
}

#[cfg(feature = "avro")]
impl<T> AvroSchema<T> {
    pub fn new(schema_json: &str) -> Self {
        let schema = apache_avro::Schema::parse_str(schema_json)
            .expect("Invalid Avro schema JSON");
        Self {
            schema,
            _marker: std::marker::PhantomData,
            fn_name: None,
        }
    }

    /// Create a schema that dispatches to a plugin-exported schema function named
    /// `bicycle_schema_decode_{name}` / `bicycle_schema_encode_{name}`.
    pub fn named(schema_json: &str, name: &str) -> Self {
        let schema = apache_avro::Schema::parse_str(schema_json)
            .expect("Invalid Avro schema JSON");
        Self {
            schema,
            _marker: std::marker::PhantomData,
            fn_name: Some(name.to_string()),
        }
    }
}

#[cfg(feature = "avro")]
impl<T: Send + Sync + 'static> ConnectorSchema for AvroSchema<T> {
    fn decoder_property(&self) -> (&'static str, String) {
        if let Some(ref name) = self.fn_name {
            ("schema.decode.fn", format!("bicycle_schema_decode_{}", name))
        } else {
            ("value.deserializer", "avro".to_string())
        }
    }
    fn encoder_property(&self) -> (&'static str, String) {
        if let Some(ref name) = self.fn_name {
            ("schema.encode.fn", format!("bicycle_schema_encode_{}", name))
        } else {
            ("value.serializer", "avro".to_string())
        }
    }
}

#[cfg(feature = "avro")]
impl<T: Serialize + DeserializeOwned + Send + Sync + 'static> DeserializationSchema
    for AvroSchema<T>
{
    type Out = T;
    fn deserialize(&self, bytes: &[u8]) -> Option<T> {
        use apache_avro::from_avro_datum;
        let mut reader = bytes;
        let value = from_avro_datum(&self.schema, &mut reader, None).ok()?;
        apache_avro::from_value::<T>(&value).ok()
    }
}

#[cfg(feature = "avro")]
impl<T: Serialize + DeserializeOwned + Send + Sync + 'static> SerializationSchema
    for AvroSchema<T>
{
    type In = T;
    fn serialize(&self, value: &T) -> Vec<u8> {
        use apache_avro::to_avro_datum;
        let avro_value = apache_avro::to_value(value).unwrap_or(apache_avro::types::Value::Null);
        to_avro_datum(&self.schema, avro_value).unwrap_or_default()
    }
}

/// Wrapper for closure-based key selectors.
pub struct FnKeySelector<T, K, F>
where
    F: Fn(&T) -> K + Send + Sync + 'static,
{
    f: F,
    _marker: std::marker::PhantomData<(T, K)>,
}

impl<T, K, F> FnKeySelector<T, K, F>
where
    F: Fn(&T) -> K + Send + Sync + 'static,
{
    pub fn new(f: F) -> Self {
        Self {
            f,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<T, K, F> KeySelector for FnKeySelector<T, K, F>
where
    T: Send + Sync + 'static,
    K: Serialize + DeserializeOwned + Send + Sync + Clone + std::hash::Hash + Eq + 'static,
    F: Fn(&T) -> K + Send + Sync + 'static,
{
    type T = T;
    type Key = K;

    fn get_key(&self, input: &T) -> K {
        (self.f)(input)
    }
}
