//! Internal wrapper types for plugin FFI bridge.
//!
//! These types wrap `AsyncFunction` and `RichAsyncFunction` implementations
//! behind a synchronous interface suitable for the C FFI boundary.

use crate::ffi::{BicycleBytes, BicycleResult, PluginContext};
use bicycle_api::{AsyncFunction, RichAsyncFunction};

// =============================================================================
// Helper traits for type extraction in macros
// =============================================================================

/// Extracts input/output types from `AsyncFunction` implementations.
#[doc(hidden)]
pub trait __FunctionTypes {
    type In: serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static;
    type Out: serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static;
}

impl<T: AsyncFunction> __FunctionTypes for T {
    type In = T::In;
    type Out = T::Out;
}

/// Extracts input/output types from `RichAsyncFunction` implementations.
#[doc(hidden)]
pub trait __RichFunctionTypes {
    type In: serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static;
    type Out: serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static;
}

impl<T: RichAsyncFunction> __RichFunctionTypes for T {
    type In = T::In;
    type Out = T::Out;
}

/// Returns whether a type is a rich (stateful) function. Always `false` for this trait bound.
#[doc(hidden)]
pub fn __is_rich_function<T>() -> bool {
    false
}

// =============================================================================
// AsyncFunction wrapper
// =============================================================================

/// Wraps an `AsyncFunction` for use across the FFI boundary.
///
/// Owns a Tokio runtime so async functions can be called from synchronous C code.
#[doc(hidden)]
pub struct __FunctionWrapper<F: AsyncFunction + Default> {
    function: F,
    runtime: tokio::runtime::Runtime,
}

impl<F: AsyncFunction + Default> __FunctionWrapper<F> {
    pub fn new(function: F) -> Self {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("Failed to create Tokio runtime");
        Self { function, runtime }
    }

    pub fn open(&mut self, ctx: &PluginContext) -> BicycleResult {
        let task_info =
            bicycle_api::TaskInfo::new(&ctx.job_id, "plugin", ctx.subtask_index as u32, 1);
        let api_ctx = bicycle_api::Context::new(task_info);
        match self.runtime.block_on(self.function.open(&api_ctx)) {
            Ok(()) => BicycleResult::ok(),
            Err(e) => BicycleResult::err(e.to_string()),
        }
    }

    pub fn process(&mut self, input: &[u8], ctx: &PluginContext) -> BicycleBytes {
        let input_value: F::In = match serde_json::from_slice(input) {
            Ok(v) => v,
            Err(e) => {
                return BicycleBytes::error(1, format!("Failed to deserialize input: {}", e))
            }
        };
        let task_info =
            bicycle_api::TaskInfo::new(&ctx.job_id, "plugin", ctx.subtask_index as u32, 1);
        let api_ctx = bicycle_api::Context::new(task_info);
        let outputs = self
            .runtime
            .block_on(self.function.process(input_value, &api_ctx));
        match serde_json::to_vec(&outputs) {
            Ok(bytes) => BicycleBytes::success(bytes),
            Err(e) => BicycleBytes::error(2, format!("Failed to serialize output: {}", e)),
        }
    }

    pub fn close(&mut self) -> BicycleResult {
        match self.runtime.block_on(self.function.close()) {
            Ok(()) => BicycleResult::ok(),
            Err(e) => BicycleResult::err(e.to_string()),
        }
    }

    pub fn snapshot(&self, _ctx: &PluginContext) -> BicycleBytes {
        BicycleBytes::empty()
    }

    pub fn restore(&mut self, _data: &[u8]) -> BicycleResult {
        BicycleResult::ok()
    }
}

// =============================================================================
// RichAsyncFunction wrapper
// =============================================================================

/// Wraps a `RichAsyncFunction` for use across the FFI boundary.
///
/// Supports stateful operations: open, snapshot, and restore.
#[doc(hidden)]
pub struct __RichFunctionWrapper<F: RichAsyncFunction + Default> {
    function: F,
    runtime: tokio::runtime::Runtime,
}

impl<F: RichAsyncFunction + Default> __RichFunctionWrapper<F> {
    pub fn new(function: F) -> Self {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("Failed to create Tokio runtime");
        Self { function, runtime }
    }

    pub fn open(&mut self, _ctx: &PluginContext) -> BicycleResult {
        let runtime_ctx = bicycle_api::RuntimeContext::standalone();
        match self.runtime.block_on(self.function.open(&runtime_ctx)) {
            Ok(()) => BicycleResult::ok(),
            Err(e) => BicycleResult::err(e.to_string()),
        }
    }

    pub fn process(&mut self, input: &[u8], _ctx: &PluginContext) -> BicycleBytes {
        let input_value: F::In = match serde_json::from_slice(input) {
            Ok(v) => v,
            Err(e) => {
                return BicycleBytes::error(1, format!("Failed to deserialize input: {}", e))
            }
        };
        let runtime_ctx = bicycle_api::RuntimeContext::standalone();
        let outputs = self
            .runtime
            .block_on(self.function.process(input_value, &runtime_ctx));
        match serde_json::to_vec(&outputs) {
            Ok(bytes) => BicycleBytes::success(bytes),
            Err(e) => BicycleBytes::error(2, format!("Failed to serialize output: {}", e)),
        }
    }

    pub fn close(&mut self) -> BicycleResult {
        match self.runtime.block_on(self.function.close()) {
            Ok(()) => BicycleResult::ok(),
            Err(e) => BicycleResult::err(e.to_string()),
        }
    }

    pub fn snapshot(&self, _ctx: &PluginContext) -> BicycleBytes {
        let runtime_ctx = bicycle_api::RuntimeContext::standalone();
        match self.runtime.block_on(self.function.snapshot(&runtime_ctx)) {
            Ok(()) => BicycleBytes::empty(),
            Err(e) => BicycleBytes::error(1, format!("Snapshot failed: {}", e)),
        }
    }

    pub fn restore(&mut self, _data: &[u8]) -> BicycleResult {
        // TODO: Restore state from data
        BicycleResult::ok()
    }
}
