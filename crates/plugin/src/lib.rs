//! Bicycle Native Plugin Support
//!
//! This crate provides the infrastructure for building Bicycle streaming jobs
//! as native shared libraries (.so on Linux, .dylib on macOS, .dll on Windows).
//!
//! # Overview
//!
//! Instead of using WASM, this approach compiles jobs as native code that is
//! dynamically loaded by workers using `libloading`. This provides:
//!
//! - Full native performance
//! - Access to system libraries and I/O
//! - Easier debugging
//!
//! # Usage
//!
//! ```ignore
//! use bicycle_api::prelude::*;
//! use bicycle_plugin::bicycle_plugin;
//!
//! pub struct WordSplitter;
//!
//! #[async_trait]
//! impl AsyncFunction for WordSplitter {
//!     type In = String;
//!     type Out = String;
//!
//!     async fn process(&mut self, input: String, _ctx: &Context) -> Vec<String> {
//!         input.split_whitespace().map(|s| s.to_lowercase()).collect()
//!     }
//! }
//!
//! bicycle_plugin!(WordSplitter);
//! ```
//!
//! # Plugin ABI
//!
//! Plugins export these `extern "C"` functions:
//!
//! - `bicycle_plugin_info()` - Returns plugin metadata
//! - `bicycle_create(name, config, len)` - Creates a function instance
//! - `bicycle_open(handle, ctx, len)` - Opens/initializes the function
//! - `bicycle_process(handle, input, in_len, ctx, ctx_len)` - Processes input
//! - `bicycle_close(handle)` - Closes the function
//! - `bicycle_snapshot(handle, ctx, len)` - Creates state snapshot
//! - `bicycle_restore(handle, data, len)` - Restores from snapshot
//! - `bicycle_destroy(handle)` - Destroys the function instance

pub mod ffi;
pub mod wrapper;

pub use ffi::{
    bicycle_alloc, bicycle_free, bicycle_free_bytes, bicycle_free_result, BicycleBytes,
    BicycleResult, FunctionInfo, PluginContext, PluginInfo,
};
pub use wrapper::{
    __FunctionTypes, __FunctionWrapper, __RichFunctionTypes, __RichFunctionWrapper,
    __is_rich_function,
};

// Re-export bicycle-api types for convenience
pub use bicycle_api::prelude::*;
pub use bicycle_api::{AsyncFunction, RichAsyncFunction};

/// Generate FFI exports for a plugin with only `AsyncFunction` implementations.
///
/// # Example
///
/// ```ignore
/// bicycle_plugin!(WordSplitter, WordCounter);
/// ```
#[macro_export]
macro_rules! bicycle_plugin {
    ($($func:ty),+ $(,)?) => {
        $crate::bicycle_plugin_all! {
            async: [$($func),+],
            rich: [],
        }
    };
}

/// Generate FFI exports for a plugin with only `RichAsyncFunction` implementations.
///
/// # Example
///
/// ```ignore
/// bicycle_plugin_rich!(StatefulCounter);
/// ```
#[macro_export]
macro_rules! bicycle_plugin_rich {
    ($($func:ty),+ $(,)?) => {
        $crate::bicycle_plugin_all! {
            async: [],
            rich: [$($func),+],
        }
    };
}

/// Generate FFI exports for a plugin with both async and rich (stateful) functions.
///
/// This is the core macro — `bicycle_plugin!` and `bicycle_plugin_rich!` delegate here.
///
/// # Example
///
/// ```ignore
/// bicycle_plugin_all! {
///     async: [WordSplitter, WordFormatter],
///     rich: [WordCounter],
/// }
/// ```
#[macro_export]
macro_rules! bicycle_plugin_all {
    (async: [$($async_func:ty),* $(,)?], rich: [$($rich_func:ty),* $(,)?] $(,)?) => {
        static PLUGIN_INFO: std::sync::OnceLock<$crate::PluginInfo> = std::sync::OnceLock::new();

        fn get_plugin_info() -> &'static $crate::PluginInfo {
            PLUGIN_INFO.get_or_init(|| {
                let mut info = $crate::PluginInfo::new(
                    env!("CARGO_PKG_NAME"),
                    env!("CARGO_PKG_VERSION"),
                );
                $(
                    info.add_function($crate::FunctionInfo {
                        name: std::any::type_name::<$async_func>().to_string(),
                        is_rich: false,
                        input_type: std::any::type_name::<<$async_func as $crate::__FunctionTypes>::In>().to_string(),
                        output_type: std::any::type_name::<<$async_func as $crate::__FunctionTypes>::Out>().to_string(),
                    });
                )*
                $(
                    info.add_function($crate::FunctionInfo {
                        name: std::any::type_name::<$rich_func>().to_string(),
                        is_rich: true,
                        input_type: std::any::type_name::<<$rich_func as $crate::__RichFunctionTypes>::In>().to_string(),
                        output_type: std::any::type_name::<<$rich_func as $crate::__RichFunctionTypes>::Out>().to_string(),
                    });
                )*
                info
            })
        }

        #[no_mangle]
        pub extern "C" fn bicycle_plugin_info() -> $crate::BicycleBytes {
            let info = get_plugin_info();
            $crate::BicycleBytes::success(info.to_bytes())
        }

        #[no_mangle]
        pub unsafe extern "C" fn bicycle_create(
            name: *const std::os::raw::c_char,
            config: *const u8,
            config_len: usize,
        ) -> *mut std::ffi::c_void {
            let name_str = $crate::ffi::c_str_to_string(name);
            let _config_bytes = $crate::ffi::read_bytes(config, config_len);

            $(
                if name_str == std::any::type_name::<$async_func>() || name_str.ends_with(stringify!($async_func)) {
                    let instance = <$async_func>::default();
                    let wrapper = Box::new($crate::__FunctionWrapper::<$async_func>::new(instance));
                    return Box::into_raw(wrapper) as *mut std::ffi::c_void;
                }
            )*
            $(
                if name_str == std::any::type_name::<$rich_func>() || name_str.ends_with(stringify!($rich_func)) {
                    let instance = <$rich_func>::default();
                    let wrapper = Box::new($crate::__RichFunctionWrapper::<$rich_func>::new(instance));
                    return Box::into_raw(wrapper) as *mut std::ffi::c_void;
                }
            )*

            std::ptr::null_mut()
        }

        #[no_mangle]
        pub unsafe extern "C" fn bicycle_open(
            handle: *mut std::ffi::c_void,
            ctx: *const u8,
            ctx_len: usize,
        ) -> $crate::BicycleResult {
            if handle.is_null() {
                return $crate::BicycleResult::err("Null handle".to_string());
            }

            let ctx_bytes = $crate::ffi::read_bytes(ctx, ctx_len);
            let plugin_ctx = match $crate::PluginContext::from_bytes(&ctx_bytes) {
                Ok(c) => c,
                Err(e) => return $crate::BicycleResult::err(format!("Invalid context: {}", e)),
            };

            $(
                {
                    let wrapper = handle as *mut $crate::__FunctionWrapper<$async_func>;
                    if let Some(w) = wrapper.as_mut() {
                        return w.open(&plugin_ctx);
                    }
                }
            )*
            $(
                {
                    let wrapper = handle as *mut $crate::__RichFunctionWrapper<$rich_func>;
                    if let Some(w) = wrapper.as_mut() {
                        return w.open(&plugin_ctx);
                    }
                }
            )*

            $crate::BicycleResult::err("Invalid handle".to_string())
        }

        #[no_mangle]
        pub unsafe extern "C" fn bicycle_process(
            handle: *mut std::ffi::c_void,
            input: *const u8,
            input_len: usize,
            ctx: *const u8,
            ctx_len: usize,
        ) -> $crate::BicycleBytes {
            if handle.is_null() {
                return $crate::BicycleBytes::error(1, "Null handle".to_string());
            }

            let input_bytes = $crate::ffi::read_bytes(input, input_len);
            let ctx_bytes = $crate::ffi::read_bytes(ctx, ctx_len);

            let plugin_ctx = match $crate::PluginContext::from_bytes(&ctx_bytes) {
                Ok(c) => c,
                Err(e) => return $crate::BicycleBytes::error(2, format!("Invalid context: {}", e)),
            };

            $(
                {
                    let wrapper = handle as *mut $crate::__FunctionWrapper<$async_func>;
                    if let Some(w) = wrapper.as_mut() {
                        return w.process(&input_bytes, &plugin_ctx);
                    }
                }
            )*
            $(
                {
                    let wrapper = handle as *mut $crate::__RichFunctionWrapper<$rich_func>;
                    if let Some(w) = wrapper.as_mut() {
                        return w.process(&input_bytes, &plugin_ctx);
                    }
                }
            )*

            $crate::BicycleBytes::error(3, "Invalid handle".to_string())
        }

        #[no_mangle]
        pub unsafe extern "C" fn bicycle_close(handle: *mut std::ffi::c_void) -> $crate::BicycleResult {
            if handle.is_null() {
                return $crate::BicycleResult::err("Null handle".to_string());
            }

            $(
                {
                    let wrapper = handle as *mut $crate::__FunctionWrapper<$async_func>;
                    if let Some(w) = wrapper.as_mut() {
                        return w.close();
                    }
                }
            )*
            $(
                {
                    let wrapper = handle as *mut $crate::__RichFunctionWrapper<$rich_func>;
                    if let Some(w) = wrapper.as_mut() {
                        return w.close();
                    }
                }
            )*

            $crate::BicycleResult::err("Invalid handle".to_string())
        }

        #[no_mangle]
        pub unsafe extern "C" fn bicycle_snapshot(
            handle: *mut std::ffi::c_void,
            ctx: *const u8,
            ctx_len: usize,
        ) -> $crate::BicycleBytes {
            if handle.is_null() {
                return $crate::BicycleBytes::error(1, "Null handle".to_string());
            }

            let ctx_bytes = $crate::ffi::read_bytes(ctx, ctx_len);
            let plugin_ctx = match $crate::PluginContext::from_bytes(&ctx_bytes) {
                Ok(c) => c,
                Err(e) => return $crate::BicycleBytes::error(2, format!("Invalid context: {}", e)),
            };

            $(
                {
                    let wrapper = handle as *mut $crate::__FunctionWrapper<$async_func>;
                    if let Some(w) = wrapper.as_mut() {
                        return w.snapshot(&plugin_ctx);
                    }
                }
            )*
            $(
                {
                    let wrapper = handle as *mut $crate::__RichFunctionWrapper<$rich_func>;
                    if let Some(w) = wrapper.as_mut() {
                        return w.snapshot(&plugin_ctx);
                    }
                }
            )*

            $crate::BicycleBytes::error(3, "Invalid handle".to_string())
        }

        #[no_mangle]
        pub unsafe extern "C" fn bicycle_restore(
            handle: *mut std::ffi::c_void,
            data: *const u8,
            data_len: usize,
        ) -> $crate::BicycleResult {
            if handle.is_null() {
                return $crate::BicycleResult::err("Null handle".to_string());
            }

            let data_bytes = $crate::ffi::read_bytes(data, data_len);

            $(
                {
                    let wrapper = handle as *mut $crate::__FunctionWrapper<$async_func>;
                    if let Some(w) = wrapper.as_mut() {
                        return w.restore(&data_bytes);
                    }
                }
            )*
            $(
                {
                    let wrapper = handle as *mut $crate::__RichFunctionWrapper<$rich_func>;
                    if let Some(w) = wrapper.as_mut() {
                        return w.restore(&data_bytes);
                    }
                }
            )*

            $crate::BicycleResult::err("Invalid handle".to_string())
        }

        #[no_mangle]
        pub unsafe extern "C" fn bicycle_destroy(handle: *mut std::ffi::c_void) {
            if handle.is_null() {
                return;
            }

            $(
                {
                    let wrapper = handle as *mut $crate::__FunctionWrapper<$async_func>;
                    if !wrapper.is_null() {
                        let _ = Box::from_raw(wrapper);
                        return;
                    }
                }
            )*
            $(
                {
                    let wrapper = handle as *mut $crate::__RichFunctionWrapper<$rich_func>;
                    if !wrapper.is_null() {
                        let _ = Box::from_raw(wrapper);
                        return;
                    }
                }
            )*
        }
    };
}
