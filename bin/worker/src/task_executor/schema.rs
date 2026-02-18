// ============================================================================
// Schema helpers — apply at Kafka connector boundary
// ============================================================================

use std::collections::HashMap;

/// Apply the configured source schema to convert Kafka payload bytes into internal
/// JSON bytes that downstream plugin operators expect.
///
/// - `schema.decode.fn` present → delegate to plugin symbol
/// - `value.deserializer = "string"` → wrap raw UTF-8 as a JSON string
/// - `value.deserializer = "json"` (or absent) → pass-through (already JSON)
pub(super) fn apply_source_schema(
    bytes: Vec<u8>,
    props: &HashMap<String, String>,
    plugin: Option<&crate::plugin_loader::LoadedPlugin>,
) -> Vec<u8> {
    if let Some(fn_name) = props.get("schema.decode.fn") {
        if let Some(schema_fn) = plugin.and_then(|p| p.get_schema_fn(fn_name)) {
            let result = unsafe { schema_fn(bytes.as_ptr(), bytes.len()) };
            return unsafe { result.into_vec().unwrap_or(bytes) };
        }
    }
    match props.get("value.deserializer").map(|s| s.as_str()).unwrap_or("json") {
        "string" => {
            let s: String = String::from_utf8_lossy(&bytes).into_owned();
            serde_json::to_vec(&s).unwrap_or(bytes)
        }
        _ => bytes, // "json" or unknown: pass-through
    }
}

/// Apply the configured sink schema to convert internal JSON bytes into the
/// external format written to Kafka.
///
/// - `schema.encode.fn` present → delegate to plugin symbol
/// - `value.serializer = "string"` → unwrap JSON string to raw UTF-8 bytes
/// - `value.serializer = "json"` (or absent) → pass-through (already JSON)
pub(super) fn apply_sink_schema(
    bytes: Vec<u8>,
    props: &HashMap<String, String>,
    plugin: Option<&crate::plugin_loader::LoadedPlugin>,
) -> Vec<u8> {
    if let Some(fn_name) = props.get("schema.encode.fn") {
        if let Some(schema_fn) = plugin.and_then(|p| p.get_schema_fn(fn_name)) {
            let result = unsafe { schema_fn(bytes.as_ptr(), bytes.len()) };
            return unsafe { result.into_vec().unwrap_or(bytes) };
        }
    }
    match props.get("value.serializer").map(|s| s.as_str()).unwrap_or("json") {
        "string" => serde_json::from_slice::<String>(&bytes)
            .map(|s| s.into_bytes())
            .unwrap_or(bytes),
        _ => bytes, // "json" or unknown: pass-through
    }
}
