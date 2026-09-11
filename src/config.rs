use crate::error::{QuebecError, Result};
#[cfg(feature = "python")]
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::LazyLock;

/// A memory size written in queue.yml. Accepts a bare number (bytes), a
/// suffixed string (`512MiB`, `1GB`), or `max`. Parsing happens in
/// [`SizeSpec::bytes`] so a malformed value degrades to a warning at use
/// time instead of failing the whole config load.
#[derive(Debug, Clone, Serialize)]
pub struct SizeSpec(pub String);

impl SizeSpec {
    /// Parsed byte count. `None` means either `max` (explicitly unlimited,
    /// which is the kernel default anyway) or an unparseable value; callers
    /// distinguish the two via [`SizeSpec::is_valid`].
    pub fn bytes(&self) -> Option<u64> {
        match parse_size(&self.0) {
            Some(SizeValue::Bytes(n)) => Some(n),
            _ => None,
        }
    }

    pub fn is_valid(&self) -> bool {
        parse_size(&self.0).is_some()
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> serde::Deserialize<'de> for SizeSpec {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error;

        let value: serde_yaml::Value = serde::Deserialize::deserialize(deserializer)?;
        match value {
            serde_yaml::Value::String(s) => Ok(SizeSpec(s)),
            serde_yaml::Value::Number(n) => Ok(SizeSpec(n.to_string())),
            _ => Err(D::Error::custom(
                "memory size must be a number of bytes or a string like '512MiB' / 'max'",
            )),
        }
    }
}

/// Result of parsing a memory size. `Max` is the kernel's "no limit"
/// sentinel and is kept distinct from a parse failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SizeValue {
    Bytes(u64),
    Max,
}

/// Parse a cgroup memory size. Bare numbers are bytes; `K`/`Ki`/`KiB` style
/// suffixes are binary, `KB`/`MB`/`GB` are decimal. Returns `None` when the
/// input cannot be understood.
pub fn parse_size(raw: &str) -> Option<SizeValue> {
    let s = raw.trim();
    if s.is_empty() {
        return None;
    }
    if s.eq_ignore_ascii_case("max") {
        return Some(SizeValue::Max);
    }

    let split = s
        .find(|c: char| !c.is_ascii_digit() && c != '.')
        .unwrap_or(s.len());
    let (number, unit) = s.split_at(split);
    let number: f64 = number.parse().ok()?;
    if !number.is_finite() || number < 0.0 {
        return None;
    }

    let multiplier: f64 = match unit.trim().to_ascii_lowercase().as_str() {
        "" | "b" => 1.0,
        "k" | "ki" | "kib" => 1024.0,
        "kb" => 1_000.0,
        "m" | "mi" | "mib" => 1024.0 * 1024.0,
        "mb" => 1_000_000.0,
        "g" | "gi" | "gib" => 1024.0 * 1024.0 * 1024.0,
        "gb" => 1_000_000_000.0,
        "t" | "ti" | "tib" => 1024.0 * 1024.0 * 1024.0 * 1024.0,
        "tb" => 1_000_000_000_000.0,
        _ => return None,
    };

    let bytes = number * multiplier;
    if bytes > u64::MAX as f64 {
        return None;
    }
    Some(SizeValue::Bytes(bytes.round() as u64))
}

/// How a worker entry configures the soft-recycle threshold. Kept as an enum
/// so "the key is absent" stays distinct from "the key says `max`": the first
/// inherits the constructor/env `worker_max_rss_mb`, the second switches the
/// soft recycle off, and only the second must also stop `memory.max` being
/// derived from it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecycleThreshold {
    /// Key absent: inherit whatever the constructor/env set.
    Inherit,
    /// Explicit `max` or `0`: no soft recycle, and nothing to derive from.
    Disabled,
    Bytes(u64),
    /// Present but unparseable; callers warn and fall back to `Inherit`.
    Invalid,
}

/// Worker configuration
/// Compatible with Solid Queue's worker config
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "python", pyclass(from_py_object))]
pub struct WorkerConfig {
    /// Queue names to process (internal)
    #[serde(default)]
    pub queues: Option<QueueSelector>,

    /// Number of threads in worker pool
    pub threads: Option<u32>,

    /// Polling interval in seconds
    pub polling_interval: Option<f64>,

    /// Number of processes to fork (for compatibility, Quebec uses single process)
    pub processes: Option<u32>,

    /// RSS soft limit for the planned-recycle path (`200MiB`). Also the
    /// derivation source for `memory_max` when the latter is not set
    /// explicitly. Named for what it does rather than for its unit, so it
    /// reads the same way as the `memory_*` fields below.
    pub memory_recycle_at: Option<SizeSpec>,

    /// cgroup v2 `memory.max` for this worker's leaf cgroup.
    pub memory_max: Option<SizeSpec>,

    /// cgroup v2 `memory.high` (throttle, no OOM kill).
    pub memory_high: Option<SizeSpec>,

    /// cgroup v2 `memory.swap.max`. Defaults to 0 when a memory limit is set.
    pub memory_swap_max: Option<SizeSpec>,

    /// cgroup v2 `memory.oom.group`. Defaults to true when a memory limit is set.
    pub memory_oom_group: Option<bool>,
}

impl WorkerConfig {
    /// Classify `memory_recycle_at` for this entry.
    pub fn recycle_threshold(&self) -> RecycleThreshold {
        let Some(spec) = self.memory_recycle_at.as_ref() else {
            return RecycleThreshold::Inherit;
        };
        match parse_size(spec.as_str()) {
            Some(SizeValue::Bytes(0)) | Some(SizeValue::Max) => RecycleThreshold::Disabled,
            Some(SizeValue::Bytes(n)) => RecycleThreshold::Bytes(n),
            None => RecycleThreshold::Invalid,
        }
    }
}

#[cfg(feature = "python")]
#[pymethods]
impl WorkerConfig {
    #[getter]
    fn threads(&self) -> Option<u32> {
        self.threads
    }

    #[getter]
    fn polling_interval(&self) -> Option<f64> {
        self.polling_interval
    }

    #[getter]
    fn processes(&self) -> Option<u32> {
        self.processes
    }

    fn __repr__(&self) -> String {
        format!(
            "WorkerConfig(queues={}, threads={:?}, polling_interval={:?})",
            self.queues
                .as_ref()
                .map_or_else(|| "None".to_string(), |q| q.to_string()),
            self.threads,
            self.polling_interval
        )
    }

    /// Get queue names as list
    #[pyo3(name = "get_queues")]
    fn get_queues_py(&self) -> Option<Vec<String>> {
        self.queues.as_ref().map(|q| q.to_list())
    }

    /// Check if processes all queues
    fn is_all_queues(&self) -> bool {
        self.queues.as_ref().is_some_and(|q| q.is_all())
    }
}

/// Queue selector
/// Handles different queue specification formats from Solid Queue
#[derive(Debug, Clone)]
pub enum QueueSelector {
    All,                   // "*"
    Single(String),        // "default"
    Multiple(Vec<String>), // ["real_time", "background"]
}

impl QueueSelector {
    pub fn to_list(&self) -> Vec<String> {
        match self {
            QueueSelector::All => vec!["*".to_string()],
            QueueSelector::Single(q) => vec![q.clone()],
            QueueSelector::Multiple(qs) => qs.clone(),
        }
    }

    pub fn is_all(&self) -> bool {
        matches!(self, QueueSelector::All)
    }

    pub fn to_string(&self) -> String {
        match self {
            QueueSelector::All => "*".to_string(),
            QueueSelector::Single(q) => q.clone(),
            QueueSelector::Multiple(qs) => format!("[{}]", qs.join(", ")),
        }
    }

    /// Get exact queue names (non-wildcard)
    pub fn exact_names(&self) -> Vec<String> {
        match self {
            QueueSelector::All => vec![],
            QueueSelector::Single(q) if !q.ends_with('*') => vec![q.clone()],
            QueueSelector::Single(_) => vec![],
            QueueSelector::Multiple(qs) => {
                qs.iter().filter(|q| !q.ends_with('*')).cloned().collect()
            }
        }
    }

    /// Get wildcard prefixes (e.g., "customer_*" -> "customer_")
    pub fn wildcard_prefixes(&self) -> Vec<String> {
        match self {
            QueueSelector::All => vec![],
            QueueSelector::Single(q) if q.ends_with('*') => {
                vec![q.trim_end_matches('*').to_string()]
            }
            QueueSelector::Single(_) => vec![],
            QueueSelector::Multiple(qs) => qs
                .iter()
                .filter(|q| q.ends_with('*'))
                .map(|q| q.trim_end_matches('*').to_string())
                .collect(),
        }
    }

    /// Check if has any wildcard patterns
    pub fn has_wildcards(&self) -> bool {
        !self.wildcard_prefixes().is_empty()
    }

    /// Get ordered queue patterns for processing in configuration order
    /// Returns a list of (is_wildcard, pattern) tuples
    /// This preserves queue ordering semantics from Solid Queue
    pub fn ordered_patterns(&self) -> Vec<(bool, String)> {
        match self {
            QueueSelector::All => vec![(false, "*".to_string())], // Special marker for all
            QueueSelector::Single(q) => {
                let is_wildcard = q.ends_with('*');
                let pattern = if is_wildcard {
                    q.trim_end_matches('*').to_string()
                } else {
                    q.clone()
                };
                vec![(is_wildcard, pattern)]
            }
            QueueSelector::Multiple(qs) => qs
                .iter()
                .map(|q| {
                    let is_wildcard = q.ends_with('*');
                    let pattern = if is_wildcard {
                        q.trim_end_matches('*').to_string()
                    } else {
                        q.clone()
                    };
                    (is_wildcard, pattern)
                })
                .collect(),
        }
    }
}

// Custom deserializer for QueueSelector to handle different YAML formats
impl<'de> serde::Deserialize<'de> for QueueSelector {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error;

        let value: serde_yaml::Value = serde::Deserialize::deserialize(deserializer)?;

        match value {
            // "*" -> All
            serde_yaml::Value::String(s) if s == "*" => Ok(QueueSelector::All),
            // "default" -> Single
            serde_yaml::Value::String(s) => Ok(QueueSelector::Single(s)),
            // [queue1, queue2] -> Multiple
            serde_yaml::Value::Sequence(seq) => {
                let queues: std::result::Result<Vec<String>, _> = seq
                    .into_iter()
                    .map(|v| {
                        v.as_str()
                            .ok_or_else(|| D::Error::custom("Queue name must be a string"))
                            .map(|s| s.to_string())
                    })
                    .collect();
                queues.map(QueueSelector::Multiple)
            }
            _ => Err(D::Error::custom(
                "Queues must be '*', a string, or a list of strings",
            )),
        }
    }
}

impl serde::Serialize for QueueSelector {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            QueueSelector::All => serializer.serialize_str("*"),
            QueueSelector::Single(q) => serializer.serialize_str(q),
            QueueSelector::Multiple(qs) => qs.serialize(serializer),
        }
    }
}

/// Dispatcher configuration
/// Compatible with Solid Queue's dispatcher config
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "python", pyclass(from_py_object))]
pub struct DispatcherConfig {
    /// Polling interval in seconds
    pub polling_interval: Option<f64>,

    /// Batch size for dispatching jobs
    pub batch_size: Option<u64>,

    /// Concurrency maintenance interval in seconds
    pub concurrency_maintenance_interval: Option<f64>,

    /// Whether to perform concurrency maintenance
    pub concurrency_maintenance: Option<bool>,

    /// Number of processes to fork (supervisor mode)
    pub processes: Option<u32>,

    /// cgroup v2 `memory.max` for this dispatcher's leaf cgroup.
    pub memory_max: Option<SizeSpec>,

    /// cgroup v2 `memory.high` (throttle, no OOM kill).
    pub memory_high: Option<SizeSpec>,

    /// cgroup v2 `memory.swap.max`. Defaults to 0 when a memory limit is set.
    pub memory_swap_max: Option<SizeSpec>,

    /// cgroup v2 `memory.oom.group`. Defaults to true when a memory limit is set.
    pub memory_oom_group: Option<bool>,
}

#[cfg(feature = "python")]
#[pymethods]
impl DispatcherConfig {
    #[getter]
    fn polling_interval(&self) -> Option<f64> {
        self.polling_interval
    }

    #[getter]
    fn batch_size(&self) -> Option<u64> {
        self.batch_size
    }

    #[getter]
    fn concurrency_maintenance_interval(&self) -> Option<f64> {
        self.concurrency_maintenance_interval
    }

    #[getter]
    fn concurrency_maintenance(&self) -> Option<bool> {
        self.concurrency_maintenance
    }

    #[getter]
    fn processes(&self) -> Option<u32> {
        self.processes
    }

    fn __repr__(&self) -> String {
        format!(
            "DispatcherConfig(polling_interval={:?}, batch_size={:?})",
            self.polling_interval, self.batch_size
        )
    }
}

/// Database configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "python", pyclass(from_py_object))]
pub struct DatabaseConfig {
    pub url: String,
}

#[cfg(feature = "python")]
#[pymethods]
impl DatabaseConfig {
    #[getter]
    fn url(&self) -> &str {
        &self.url
    }

    fn __repr__(&self) -> String {
        // Don't print full URL (may contain password)
        "DatabaseConfig(url='***')".to_string()
    }
}

/// Main queue configuration
/// Compatible with Solid Queue's queue.yml format
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "python", pyclass(from_py_object))]
pub struct QueueConfig {
    /// Application name for NOTIFY channel isolation (default: "quebec")
    pub name: Option<String>,

    /// Database configuration
    pub database: Option<DatabaseConfig>,

    /// Worker configurations
    pub workers: Option<Vec<WorkerConfig>>,

    /// Dispatcher configurations
    pub dispatchers: Option<Vec<DispatcherConfig>>,

    /// cgroup v2 `memory.max` for the `workers` pool that holds every worker
    /// leaf. Worker limits may overcommit against it; a pool-level OOM then
    /// picks a worker rather than a control-plane process. Overridden by the
    /// `QUEBEC_WORKERS_POOL_MEMORY_MAX` environment variable only when absent
    /// here, matching how `memory_recycle_at` wins over `worker_max_rss_mb`.
    pub workers_pool_memory_max: Option<SizeSpec>,
}

impl QueueConfig {
    /// Find and load configuration file automatically
    ///
    /// Searches for configuration in the following order:
    /// 1. QUEBEC_CONFIG environment variable
    /// 2. ./queue.yml (current directory)
    /// 3. ./config/queue.yml (Solid Queue compatible)
    pub fn find(env: Option<&str>) -> Result<Self> {
        let path = Self::find_config_file()?;
        let path_str = path.to_str().ok_or_else(|| {
            QuebecError::Config(format!(
                "Config path is not valid UTF-8: {}",
                path.display()
            ))
        })?;
        Self::load(path_str, env)
    }

    /// Load configuration from file
    pub fn load(path: &str, env: Option<&str>) -> Result<Self> {
        let path_obj = Path::new(path);

        if !path_obj.exists() {
            return Err(QuebecError::Config(format!(
                "Config file not found: {}",
                path_obj.display()
            )));
        }

        let content = std::fs::read_to_string(path_obj)?;
        let content = Self::expand_env_vars(&content);

        Self::parse_yaml(&content, env)
    }

    /// Parse YAML string into configuration
    pub fn parse_yaml(yaml_str: &str, env: Option<&str>) -> Result<Self> {
        let yaml_str = Self::expand_env_vars(yaml_str);
        let mut value: serde_yaml::Value = serde_yaml::from_str(&yaml_str)?;
        // Expand YAML merge keys (`<<: *anchor`) so configs that factor a
        // default block via anchors work. serde_yaml only honors merge keys
        // when explicitly asked.
        value.apply_merge()?;

        match &value {
            serde_yaml::Value::Mapping(map) => {
                // Check if this is a direct config (has workers/dispatchers/database keys)
                let workers_key = serde_yaml::Value::String("workers".to_string());
                let dispatchers_key = serde_yaml::Value::String("dispatchers".to_string());
                let database_key = serde_yaml::Value::String("database".to_string());

                if map.contains_key(&workers_key)
                    || map.contains_key(&dispatchers_key)
                    || map.contains_key(&database_key)
                {
                    // This is a direct config, parse it
                    let config: QueueConfig = serde_yaml::from_value(value)?;
                    return Ok(config);
                }

                // Try to find environment-specific config
                let env_map: std::collections::HashMap<String, serde_yaml::Value> = map
                    .iter()
                    .filter_map(|(k, v)| k.as_str().map(|s| (s.to_string(), v.clone())))
                    .collect();

                // Use shared strict environment parser
                let env_value = crate::utils::parse_env_config_strict(env_map, env)?;
                let config: QueueConfig = serde_yaml::from_value(env_value)?;
                Ok(config)
            }
            _ => {
                // Not a mapping, try to parse directly
                let config: QueueConfig = serde_yaml::from_value(value)?;
                Ok(config)
            }
        }
    }

    /// Find configuration file
    fn find_config_file() -> Result<std::path::PathBuf> {
        // Check environment variable first
        if let Ok(env_path) = std::env::var("QUEBEC_CONFIG") {
            let path = Path::new(&env_path);
            if path.exists() {
                return Ok(path.to_path_buf());
            } else {
                return Err(QuebecError::Config(format!(
                    "Config file specified in QUEBEC_CONFIG not found: {env_path}"
                )));
            }
        }

        // Check default paths
        for default_path in DEFAULT_CONFIG_PATHS {
            let path = Path::new(default_path);
            if path.exists() {
                return Ok(path.to_path_buf());
            }
        }

        Err(QuebecError::Config(format!(
            "Config file not found. Searched: QUEBEC_CONFIG env var, {}",
            DEFAULT_CONFIG_PATHS.join(", ")
        )))
    }

    /// Expand environment variables in string
    fn expand_env_vars(content: &str) -> String {
        use tracing::debug;

        static ENV_VAR_RE: LazyLock<regex::Regex> = LazyLock::new(|| {
            regex::Regex::new(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}|\$([A-Za-z_][A-Za-z0-9_]*)")
                .expect("env var regex is a valid constant pattern")
        });

        let mut result = content.to_string();

        debug!("expand_env_vars: checking content for env vars");

        for cap in ENV_VAR_RE.captures_iter(content) {
            let Some(var_name) = cap.get(1).or_else(|| cap.get(2)).map(|m| m.as_str()) else {
                continue;
            };
            debug!("Found env var reference: {}", var_name);

            match std::env::var(var_name) {
                Ok(value) => {
                    let Some(pattern) = cap.get(0).map(|m| m.as_str()) else {
                        continue;
                    };
                    debug!("Replacing {} with {}", pattern, value);
                    result = result.replace(pattern, &value);
                }
                Err(_) => {
                    debug!("Env var {} not found, keeping as-is", var_name);
                }
            }
        }

        result
    }
}

// Python-exposed methods (only for data access, not loading)
#[cfg(feature = "python")]
#[pymethods]
impl QueueConfig {
    #[getter]
    fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    #[getter]
    fn database(&self) -> Option<DatabaseConfig> {
        self.database.clone()
    }

    #[getter]
    fn workers(&self) -> Option<Vec<WorkerConfig>> {
        self.workers.clone()
    }

    #[getter]
    fn dispatchers(&self) -> Option<Vec<DispatcherConfig>> {
        self.dispatchers.clone()
    }

    fn __repr__(&self) -> String {
        format!(
            "QueueConfig(name={}, database={}, workers={}, dispatchers={})",
            self.name.as_deref().unwrap_or("quebec"),
            if self.database.is_some() {
                "configured"
            } else {
                "None"
            },
            self.workers.as_ref().map_or(0, |w| w.len()),
            self.dispatchers.as_ref().map_or(0, |d| d.len())
        )
    }

    /// Get worker configuration by index
    fn get_worker(&self, index: usize) -> Option<WorkerConfig> {
        self.workers.as_ref()?.get(index).cloned()
    }

    /// Get dispatcher configuration by index
    fn get_dispatcher(&self, index: usize) -> Option<DispatcherConfig> {
        self.dispatchers.as_ref()?.get(index).cloned()
    }
}

/// Default configuration file paths (in priority order)
const DEFAULT_CONFIG_PATHS: &[&str] = &[
    "queue.yml",        // Current directory (Python projects)
    "config/queue.yml", // Solid Queue compatible (Rails projects)
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_plain_byte_counts() {
        assert_eq!(parse_size("1073741824"), Some(SizeValue::Bytes(1073741824)));
        assert_eq!(parse_size("0"), Some(SizeValue::Bytes(0)));
    }

    #[test]
    fn parses_binary_and_decimal_suffixes() {
        for raw in ["1GiB", "1Gi", "1G", " 1gib "] {
            assert_eq!(
                parse_size(raw),
                Some(SizeValue::Bytes(1073741824)),
                "{raw} should be binary"
            );
        }
        for raw in ["512MiB", "512Mi", "512M"] {
            assert_eq!(
                parse_size(raw),
                Some(SizeValue::Bytes(536870912)),
                "{raw} should be binary"
            );
        }
        assert_eq!(parse_size("1GB"), Some(SizeValue::Bytes(1_000_000_000)));
        assert_eq!(parse_size("1MB"), Some(SizeValue::Bytes(1_000_000)));
    }

    #[test]
    fn parses_max_distinctly_from_failure() {
        assert_eq!(parse_size("max"), Some(SizeValue::Max));
        assert_eq!(parse_size("MAX"), Some(SizeValue::Max));
        assert_eq!(parse_size("abc"), None);
        assert_eq!(parse_size(""), None);
        assert_eq!(parse_size("12PB"), None);
        assert_eq!(parse_size("-1"), None);
    }

    #[test]
    fn size_spec_accepts_numbers_and_strings() {
        let yaml = r#"
production:
  workers:
    - queues: "*"
      processes: 2
      memory_max: 512MiB
      memory_swap_max: 0
      memory_oom_group: false
      memory_recycle_at: 200MiB
"#;
        let cfg = QueueConfig::parse_yaml(yaml, Some("production")).expect("must parse");
        let worker = &cfg.workers.as_ref().expect("workers")[0];
        assert_eq!(
            worker.memory_max.as_ref().and_then(|s| s.bytes()),
            Some(536870912)
        );
        assert_eq!(
            worker.memory_swap_max.as_ref().and_then(|s| s.bytes()),
            Some(0)
        );
        assert_eq!(worker.memory_oom_group, Some(false));
        assert_eq!(
            worker.memory_recycle_at.as_ref().and_then(|s| s.bytes()),
            Some(200 * 1024 * 1024)
        );
    }

    #[test]
    fn explicit_max_is_distinct_from_an_absent_field() {
        // Both reproduce a real mis-parse: collapsing `max` into "unset" made
        // memory_max: max derive a hard limit from memory_recycle_at, and
        // memory_swap_max: max turn swap off.
        let yaml = r#"
production:
  workers:
    - queues: "*"
      memory_max: max
      memory_swap_max: max
      memory_recycle_at: 100MiB
"#;
        let cfg = QueueConfig::parse_yaml(yaml, Some("production")).expect("must parse");
        let worker = &cfg.workers.as_ref().expect("workers")[0];

        assert_eq!(
            parse_size(worker.memory_max.as_ref().expect("present").as_str()),
            Some(SizeValue::Max),
            "an explicit max must stay distinguishable from an absent field"
        );
        assert_eq!(
            parse_size(worker.memory_swap_max.as_ref().expect("present").as_str()),
            Some(SizeValue::Max)
        );
        assert!(worker.memory_high.is_none(), "absent stays absent");
        assert_eq!(
            worker.memory_recycle_at.as_ref().and_then(|s| s.bytes()),
            Some(100 * 1024 * 1024)
        );
    }

    fn worker_with(field: &str) -> WorkerConfig {
        let yaml = format!("production:\n  workers:\n    - queues: \"*\"\n{field}");
        let cfg = QueueConfig::parse_yaml(&yaml, Some("production")).expect("must parse");
        cfg.workers.expect("workers").remove(0)
    }

    #[test]
    fn recycle_threshold_keeps_absent_and_max_apart() {
        // An absent key inherits the constructor/env worker_max_rss_mb...
        assert_eq!(
            worker_with("").recycle_threshold(),
            RecycleThreshold::Inherit
        );
        // ...but an explicit max switches the soft recycle off, so nothing may
        // be inherited and no memory.max may be derived from it.
        assert_eq!(
            worker_with("      memory_recycle_at: max\n").recycle_threshold(),
            RecycleThreshold::Disabled
        );
        assert_eq!(
            worker_with("      memory_recycle_at: 0\n").recycle_threshold(),
            RecycleThreshold::Disabled
        );
        assert_eq!(
            worker_with("      memory_recycle_at: 100MiB\n").recycle_threshold(),
            RecycleThreshold::Bytes(100 * 1024 * 1024)
        );
        assert_eq!(
            worker_with("      memory_recycle_at: nonsense\n").recycle_threshold(),
            RecycleThreshold::Invalid
        );
    }

    #[test]
    fn unknown_and_absent_cgroup_fields_are_tolerated() {
        let yaml = r#"
production:
  workers:
    - queues: "*"
      threads: 3
      some_future_solid_queue_field: 7
"#;
        let cfg = QueueConfig::parse_yaml(yaml, Some("production")).expect("must parse");
        let worker = &cfg.workers.as_ref().expect("workers")[0];
        assert!(worker.memory_max.is_none());
        assert!(worker.memory_recycle_at.is_none());
    }

    #[test]
    fn parses_yaml_merge_keys_in_queue_config() {
        let yaml = r#"
defaults: &defaults
  workers:
    - queues: "*"
      threads: 3
      polling_interval: 0.1

development:
  <<: *defaults

production:
  <<: *defaults
"#;
        let dev = QueueConfig::parse_yaml(yaml, Some("development"))
            .expect("merge key in queue config must parse");
        let workers = dev.workers.as_ref().expect("workers populated by merge");
        assert_eq!(workers.len(), 1);
        assert_eq!(workers[0].threads, Some(3));
    }
}
