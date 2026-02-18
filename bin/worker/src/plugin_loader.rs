//! Native plugin loading for Bicycle workers.
//!
//! This module handles dynamic loading of native (.so) plugins using libloading.
//! Plugins are cached by job_id and written to disk for loading.

use anyhow::{Context, Result};
use libloading::{Library, Symbol};
use std::collections::HashMap;
use std::ffi::{c_void, CString};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use tracing::{debug, error, info, warn};

/// FFI types matching those in bicycle-plugin crate.
/// We re-define them here to avoid linking against the plugin crate.
#[repr(C)]
pub struct BicycleBytes {
    pub ptr: *mut u8,
    pub len: usize,
    pub error_code: i32,
    pub error_msg_ptr: *mut u8,
    pub error_msg_len: usize,
}

impl BicycleBytes {
    /// Check if this is an error result.
    pub fn is_error(&self) -> bool {
        self.error_code != 0
    }

    /// Convert to Vec<u8>, consuming the data.
    ///
    /// # Safety
    /// The pointers must be valid and the data must have been allocated by the plugin.
    pub unsafe fn into_vec(self) -> Result<Vec<u8>, String> {
        if self.is_error() {
            let error_msg = if !self.error_msg_ptr.is_null() && self.error_msg_len > 0 {
                let slice = std::slice::from_raw_parts(self.error_msg_ptr, self.error_msg_len);
                String::from_utf8_lossy(slice).to_string()
            } else {
                format!("Error code: {}", self.error_code)
            };
            Err(error_msg)
        } else if self.ptr.is_null() || self.len == 0 {
            Ok(Vec::new())
        } else {
            let slice = std::slice::from_raw_parts(self.ptr, self.len);
            Ok(slice.to_vec())
        }
    }
}

#[repr(C)]
pub struct BicycleResult {
    pub success: bool,
    pub error_msg_ptr: *mut u8,
    pub error_msg_len: usize,
}

impl BicycleResult {
    /// Get error message if this is an error.
    ///
    /// # Safety
    /// The error message pointer must be valid if success is false.
    pub unsafe fn error_message(&self) -> Option<String> {
        if self.success {
            None
        } else if !self.error_msg_ptr.is_null() && self.error_msg_len > 0 {
            let slice = std::slice::from_raw_parts(self.error_msg_ptr, self.error_msg_len);
            Some(String::from_utf8_lossy(slice).to_string())
        } else {
            Some("Unknown error".to_string())
        }
    }
}

/// Plugin context passed to plugin functions.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PluginContext {
    pub job_id: String,
    pub task_id: String,
    pub subtask_index: i32,
    pub timestamp: i64,
    pub current_key: Option<String>,
}

impl Default for PluginContext {
    fn default() -> Self {
        Self {
            job_id: String::new(),
            task_id: String::new(),
            subtask_index: 0,
            timestamp: 0,
            current_key: None,
        }
    }
}

impl PluginContext {
    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap_or_default()
    }
}

/// Type aliases for plugin function signatures.
type PluginInfoFn = unsafe extern "C" fn() -> BicycleBytes;
type CreateFn = unsafe extern "C" fn(*const i8, *const u8, usize) -> *mut c_void;
type OpenFn = unsafe extern "C" fn(*mut c_void, *const u8, usize) -> BicycleResult;
type ProcessFn = unsafe extern "C" fn(*mut c_void, *const u8, usize, *const u8, usize) -> BicycleBytes;
type CloseFn = unsafe extern "C" fn(*mut c_void) -> BicycleResult;
type SnapshotFn = unsafe extern "C" fn(*mut c_void, *const u8, usize) -> BicycleBytes;
type RestoreFn = unsafe extern "C" fn(*mut c_void, *const u8, usize) -> BicycleResult;
type DestroyFn = unsafe extern "C" fn(*mut c_void);
type FreeBytsesFn = unsafe extern "C" fn(BicycleBytes);
type FreeResultFn = unsafe extern "C" fn(BicycleResult);
/// Schema conversion function: `fn(ptr, len) -> BicycleBytes`
pub type SchemaFn = unsafe extern "C" fn(*const u8, usize) -> BicycleBytes;

/// A loaded native plugin library.
pub struct LoadedPlugin {
    _library: Library,
    plugin_info: PluginInfoFn,
    create: CreateFn,
    open: OpenFn,
    process: ProcessFn,
    close: CloseFn,
    snapshot: SnapshotFn,
    restore: RestoreFn,
    destroy: DestroyFn,
    free_bytes: Option<FreeBytsesFn>,
    free_result: Option<FreeResultFn>,
}

impl LoadedPlugin {
    /// Load a plugin from a file path.
    pub fn load(path: &std::path::Path) -> Result<Self> {
        info!(path = %path.display(), "Loading native plugin");

        // Safety: Loading shared libraries is inherently unsafe
        let library = unsafe {
            Library::new(path).with_context(|| format!("Failed to load plugin: {}", path.display()))?
        };

        // Load required symbols - we copy the function pointers so they don't hold borrows
        let plugin_info: PluginInfoFn = unsafe {
            *library.get::<PluginInfoFn>(b"bicycle_plugin_info")
                .context("Missing symbol: bicycle_plugin_info")?
        };
        let create: CreateFn = unsafe {
            *library.get::<CreateFn>(b"bicycle_create")
                .context("Missing symbol: bicycle_create")?
        };
        let open: OpenFn = unsafe {
            *library.get::<OpenFn>(b"bicycle_open")
                .context("Missing symbol: bicycle_open")?
        };
        let process: ProcessFn = unsafe {
            *library.get::<ProcessFn>(b"bicycle_process")
                .context("Missing symbol: bicycle_process")?
        };
        let close: CloseFn = unsafe {
            *library.get::<CloseFn>(b"bicycle_close")
                .context("Missing symbol: bicycle_close")?
        };
        let snapshot: SnapshotFn = unsafe {
            *library.get::<SnapshotFn>(b"bicycle_snapshot")
                .context("Missing symbol: bicycle_snapshot")?
        };
        let restore: RestoreFn = unsafe {
            *library.get::<RestoreFn>(b"bicycle_restore")
                .context("Missing symbol: bicycle_restore")?
        };
        let destroy: DestroyFn = unsafe {
            *library.get::<DestroyFn>(b"bicycle_destroy")
                .context("Missing symbol: bicycle_destroy")?
        };

        // Load optional memory management symbols
        let free_bytes: Option<FreeBytsesFn> = unsafe {
            library.get::<FreeBytsesFn>(b"bicycle_free_bytes").ok().map(|s| *s)
        };
        let free_result: Option<FreeResultFn> = unsafe {
            library.get::<FreeResultFn>(b"bicycle_free_result").ok().map(|s| *s)
        };

        Ok(Self {
            _library: library,
            plugin_info,
            create,
            open,
            process,
            close,
            snapshot,
            restore,
            destroy,
            free_bytes,
            free_result,
        })
    }

    /// Get plugin info.
    pub fn get_info(&self) -> Result<serde_json::Value> {
        let result = unsafe { (self.plugin_info)() };
        let data = unsafe {
            result.into_vec().map_err(|e| anyhow::anyhow!("Plugin info failed: {}", e))?
        };
        let info: serde_json::Value = serde_json::from_slice(&data)?;
        Ok(info)
    }

    /// Create a function instance.
    pub fn create_function(&self, name: &str, config: &[u8]) -> Result<*mut c_void> {
        let c_name = CString::new(name).context("Invalid function name")?;
        let handle = unsafe {
            (self.create)(
                c_name.as_ptr(),
                config.as_ptr(),
                config.len(),
            )
        };
        if handle.is_null() {
            anyhow::bail!("Failed to create function instance: {}", name);
        }
        Ok(handle)
    }

    /// Open a function instance.
    pub fn open_function(&self, handle: *mut c_void, ctx: &PluginContext) -> Result<()> {
        let ctx_bytes = ctx.to_bytes();
        let result = unsafe {
            (self.open)(handle, ctx_bytes.as_ptr(), ctx_bytes.len())
        };

        if !result.success {
            let error = unsafe { result.error_message() }.unwrap_or_else(|| "Unknown error".to_string());
            if let Some(free) = self.free_result {
                unsafe { free(result) };
            }
            anyhow::bail!("Failed to open function: {}", error);
        }

        if let Some(free) = self.free_result {
            unsafe { free(result) };
        }
        Ok(())
    }

    /// Process input through a function.
    pub fn process_function(
        &self,
        handle: *mut c_void,
        input: &[u8],
        ctx: &PluginContext,
    ) -> Result<Vec<u8>> {
        let ctx_bytes = ctx.to_bytes();
        let result = unsafe {
            (self.process)(
                handle,
                input.as_ptr(),
                input.len(),
                ctx_bytes.as_ptr(),
                ctx_bytes.len(),
            )
        };

        let data = unsafe {
            result.into_vec().map_err(|e| anyhow::anyhow!("Process failed: {}", e))?
        };

        Ok(data)
    }

    /// Close a function instance.
    pub fn close_function(&self, handle: *mut c_void) -> Result<()> {
        let result = unsafe { (self.close)(handle) };

        if !result.success {
            let error = unsafe { result.error_message() }.unwrap_or_else(|| "Unknown error".to_string());
            if let Some(free) = self.free_result {
                unsafe { free(result) };
            }
            anyhow::bail!("Failed to close function: {}", error);
        }

        if let Some(free) = self.free_result {
            unsafe { free(result) };
        }
        Ok(())
    }

    /// Snapshot function state.
    pub fn snapshot_function(&self, handle: *mut c_void, ctx: &PluginContext) -> Result<Vec<u8>> {
        let ctx_bytes = ctx.to_bytes();
        let result = unsafe {
            (self.snapshot)(handle, ctx_bytes.as_ptr(), ctx_bytes.len())
        };

        let data = unsafe {
            result.into_vec().map_err(|e| anyhow::anyhow!("Snapshot failed: {}", e))?
        };

        Ok(data)
    }

    /// Restore function state.
    pub fn restore_function(&self, handle: *mut c_void, data: &[u8]) -> Result<()> {
        let result = unsafe {
            (self.restore)(handle, data.as_ptr(), data.len())
        };

        if !result.success {
            let error = unsafe { result.error_message() }.unwrap_or_else(|| "Unknown error".to_string());
            if let Some(free) = self.free_result {
                unsafe { free(result) };
            }
            anyhow::bail!("Failed to restore function: {}", error);
        }

        if let Some(free) = self.free_result {
            unsafe { free(result) };
        }
        Ok(())
    }

    /// Destroy a function instance.
    pub fn destroy_function(&self, handle: *mut c_void) {
        unsafe { (self.destroy)(handle) };
    }

    /// Look up a schema conversion function by name (e.g., `"bicycle_schema_decode_my_fn"`).
    ///
    /// Returns `None` if the symbol is not exported by the plugin.
    pub fn get_schema_fn(&self, fn_name: &str) -> Option<SchemaFn> {
        unsafe {
            self._library
                .get::<SchemaFn>(fn_name.as_bytes())
                .ok()
                .map(|f| *f)
        }
    }
}

// Safety: LoadedPlugin is Send + Sync because we only call functions through FFI
// and the library itself is thread-safe once loaded.
unsafe impl Send for LoadedPlugin {}
unsafe impl Sync for LoadedPlugin {}

/// A function instance from a loaded plugin.
pub struct PluginFunction {
    plugin: Arc<LoadedPlugin>,
    handle: *mut c_void,
    function_name: String,
    is_open: bool,
}

impl PluginFunction {
    /// Create a new function instance.
    pub fn new(plugin: Arc<LoadedPlugin>, function_name: &str, config: &[u8]) -> Result<Self> {
        let handle = plugin.create_function(function_name, config)?;
        Ok(Self {
            plugin,
            handle,
            function_name: function_name.to_string(),
            is_open: false,
        })
    }

    /// Open the function.
    pub fn open(&mut self, ctx: &PluginContext) -> Result<()> {
        self.plugin.open_function(self.handle, ctx)?;
        self.is_open = true;
        Ok(())
    }

    /// Process input.
    pub fn process(&self, input: &[u8], ctx: &PluginContext) -> Result<Vec<u8>> {
        if !self.is_open {
            anyhow::bail!("Function not opened: {}", self.function_name);
        }
        self.plugin.process_function(self.handle, input, ctx)
    }

    /// Close the function.
    pub fn close(&mut self) -> Result<()> {
        if self.is_open {
            self.plugin.close_function(self.handle)?;
            self.is_open = false;
        }
        Ok(())
    }

    /// Snapshot state.
    pub fn snapshot(&self, ctx: &PluginContext) -> Result<Vec<u8>> {
        self.plugin.snapshot_function(self.handle, ctx)
    }

    /// Restore state.
    pub fn restore(&mut self, data: &[u8]) -> Result<()> {
        self.plugin.restore_function(self.handle, data)
    }
}

impl Drop for PluginFunction {
    fn drop(&mut self) {
        // Close if still open
        if self.is_open {
            let _ = self.close();
        }
        // Destroy the handle
        self.plugin.destroy_function(self.handle);
    }
}

// Safety: PluginFunction is Send because the handle is only accessed through
// the plugin's FFI functions which are thread-safe.
unsafe impl Send for PluginFunction {}

/// Cache for loaded plugins, keyed by job_id.
pub struct PluginCache {
    /// Directory for storing plugin files.
    plugin_dir: PathBuf,
    /// Loaded plugins by job_id.
    plugins: RwLock<HashMap<String, Arc<LoadedPlugin>>>,
}

impl PluginCache {
    /// Create a new plugin cache.
    pub fn new(state_dir: &str) -> Result<Self> {
        let plugin_dir = PathBuf::from(state_dir).join("plugins");
        std::fs::create_dir_all(&plugin_dir)?;

        Ok(Self {
            plugin_dir,
            plugins: RwLock::new(HashMap::new()),
        })
    }

    /// Get or load a plugin for a job.
    pub fn get_or_load(&self, job_id: &str, plugin_bytes: &[u8]) -> Result<Arc<LoadedPlugin>> {
        // Check cache first
        {
            let plugins = self.plugins.read().unwrap();
            if let Some(plugin) = plugins.get(job_id) {
                return Ok(plugin.clone());
            }
        }

        // Write plugin to disk and load
        let plugin_path = self.plugin_dir.join(format!("{}.so", job_id));

        // Only write if file doesn't exist or is different
        if !plugin_path.exists() {
            info!(job_id, path = %plugin_path.display(), "Writing plugin to disk");
            std::fs::write(&plugin_path, plugin_bytes)
                .with_context(|| format!("Failed to write plugin: {}", plugin_path.display()))?;
        }

        // Load the plugin
        let plugin = Arc::new(LoadedPlugin::load(&plugin_path)?);

        // Cache it
        {
            let mut plugins = self.plugins.write().unwrap();
            plugins.insert(job_id.to_string(), plugin.clone());
        }

        Ok(plugin)
    }

    /// Remove a plugin from the cache.
    pub fn remove(&self, job_id: &str) {
        let mut plugins = self.plugins.write().unwrap();
        plugins.remove(job_id);

        // Optionally remove the file
        let plugin_path = self.plugin_dir.join(format!("{}.so", job_id));
        if plugin_path.exists() {
            if let Err(e) = std::fs::remove_file(&plugin_path) {
                warn!(job_id, error = %e, "Failed to remove plugin file");
            }
        }
    }

    /// Clear all cached plugins.
    pub fn clear(&self) {
        let mut plugins = self.plugins.write().unwrap();
        plugins.clear();
    }
}
