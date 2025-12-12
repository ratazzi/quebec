use anyhow::Result;
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use std::path::Path;

/// Worker configuration
/// Compatible with Solid Queue's worker config
#[derive(Debug, Clone, Serialize, Deserialize)]
#[pyclass]
pub struct WorkerConfig {
    /// Queue names to process (internal)
    #[serde(default)]
    pub queues: Option<QueueSelector>,

    /// Number of threads in worker pool
    #[pyo3(get)]
    pub threads: Option<u32>,

    /// Polling interval in seconds
    #[pyo3(get)]
    pub polling_interval: Option<f64>,

    /// Number of processes to fork (for compatibility, Quebec uses single process)
    #[pyo3(get)]
    pub processes: Option<u32>,
}

#[pymethods]
impl WorkerConfig {
    fn __repr__(&self) -> String {
        format!(
            "WorkerConfig(queues={}, threads={:?}, polling_interval={:?})",
            self.queues
                .as_ref()
                .map(|q| q.to_string())
                .unwrap_or_else(|| "None".to_string()),
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
        self.queues.as_ref().map(|q| q.is_all()).unwrap_or(false)
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
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
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
                let queues: Result<Vec<String>, _> = seq
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
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
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
#[pyclass]
pub struct DispatcherConfig {
    /// Polling interval in seconds
    #[pyo3(get)]
    pub polling_interval: Option<f64>,

    /// Batch size for dispatching jobs
    #[pyo3(get)]
    pub batch_size: Option<u64>,

    /// Concurrency maintenance interval in seconds
    #[pyo3(get)]
    pub concurrency_maintenance_interval: Option<f64>,

    /// Whether to perform concurrency maintenance
    #[pyo3(get)]
    pub concurrency_maintenance: Option<bool>,
}

#[pymethods]
impl DispatcherConfig {
    fn __repr__(&self) -> String {
        format!(
            "DispatcherConfig(polling_interval={:?}, batch_size={:?})",
            self.polling_interval, self.batch_size
        )
    }
}

/// Database configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[pyclass]
pub struct DatabaseConfig {
    #[pyo3(get)]
    pub url: String,
}

#[pymethods]
impl DatabaseConfig {
    fn __repr__(&self) -> String {
        // Don't print full URL (may contain password)
        "DatabaseConfig(url='***')".to_string()
    }
}

/// Main queue configuration
/// Compatible with Solid Queue's queue.yml format
#[derive(Debug, Clone, Serialize, Deserialize)]
#[pyclass]
pub struct QueueConfig {
    /// Application name for NOTIFY channel isolation (default: "quebec")
    #[pyo3(get)]
    pub name: Option<String>,

    /// Database configuration
    #[pyo3(get)]
    pub database: Option<DatabaseConfig>,

    /// Worker configurations
    #[pyo3(get)]
    pub workers: Option<Vec<WorkerConfig>>,

    /// Dispatcher configurations
    #[pyo3(get)]
    pub dispatchers: Option<Vec<DispatcherConfig>>,
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
        Self::load(path.to_str().unwrap(), env)
    }

    /// Load configuration from file
    pub fn load(path: &str, env: Option<&str>) -> Result<Self> {
        let path_obj = Path::new(path);

        if !path_obj.exists() {
            anyhow::bail!("Config file not found: {}", path_obj.display());
        }

        let content = std::fs::read_to_string(path_obj)?;
        let content = Self::expand_env_vars(&content);

        Self::parse_yaml(&content, env)
    }

    /// Parse YAML string into configuration
    pub fn parse_yaml(yaml_str: &str, env: Option<&str>) -> Result<Self> {
        let yaml_str = Self::expand_env_vars(yaml_str);
        let value: serde_yaml::Value = serde_yaml::from_str(&yaml_str)?;

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
                anyhow::bail!(
                    "Config file specified in QUEBEC_CONFIG not found: {}",
                    env_path
                );
            }
        }

        // Check default paths
        for default_path in DEFAULT_CONFIG_PATHS {
            let path = Path::new(default_path);
            if path.exists() {
                return Ok(path.to_path_buf());
            }
        }

        anyhow::bail!(
            "Config file not found. Searched: QUEBEC_CONFIG env var, {}",
            DEFAULT_CONFIG_PATHS.join(", ")
        )
    }

    /// Expand environment variables in string
    fn expand_env_vars(content: &str) -> String {
        use tracing::debug;

        let mut result = content.to_string();

        // Match ${VAR} or $VAR
        let re = regex::Regex::new(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}|\$([A-Za-z_][A-Za-z0-9_]*)")
            .unwrap();

        debug!("expand_env_vars: checking content for env vars");

        for cap in re.captures_iter(content) {
            let var_name = cap.get(1).or_else(|| cap.get(2)).unwrap().as_str();
            debug!("Found env var reference: {}", var_name);

            match std::env::var(var_name) {
                Ok(value) => {
                    let pattern = cap.get(0).unwrap().as_str();
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
#[pymethods]
impl QueueConfig {
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
