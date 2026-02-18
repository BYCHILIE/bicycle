// ============================================================================
// Operator Configuration Structs
// ============================================================================

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub(crate) struct MapConfig {
    /// Function to apply: "uppercase", "lowercase", "word_count", "reverse", or "identity"
    pub function: String,
}

impl Default for MapConfig {
    fn default() -> Self {
        Self {
            function: "identity".to_string(),
        }
    }
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub(crate) struct FlatMapConfig {
    /// Function to apply: "split_words", "split_lines", "split_csv"
    pub function: String,
}

impl Default for FlatMapConfig {
    fn default() -> Self {
        Self {
            function: "split_words".to_string(),
        }
    }
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub(crate) struct FilterConfig {
    /// Predicate: "non_empty", "contains", "starts_with", "ends_with", "min_length"
    pub predicate: String,
    /// Value for predicates that need it
    pub value: String,
}

impl Default for FilterConfig {
    fn default() -> Self {
        Self {
            predicate: "non_empty".to_string(),
            value: String::new(),
        }
    }
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub(crate) struct WindowConfig {
    /// Window size in milliseconds
    pub window_ms: u64,
}

impl Default for WindowConfig {
    fn default() -> Self {
        Self { window_ms: 5000 }
    }
}
