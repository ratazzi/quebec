use anyhow::Result;
use croner::Cron;
use english_to_cron::str_cron_syntax;
use sea_orm::{ConnectOptions, Database, DatabaseConnection, DbErr};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::runtime::Handle;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use url::Url;

use tracing::{debug, error, trace, warn};

use pyo3::exceptions::PyException;
use pyo3::prelude::*;
pyo3::create_exception!(quebec, CustomError, PyException);

#[derive(Debug, Clone)]
pub struct ConcurrencyConstraint {
    pub key: String,
    pub limit: i32,
    pub duration: Option<chrono::Duration>,
}

/// Concurrency conflict strategy - what to do when concurrency limit is reached
/// Matches Solid Queue's concurrency_on_conflict option
#[pyclass(eq, eq_int)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ConcurrencyConflict {
    /// Block the job until a slot becomes available (default behavior)
    #[default]
    Block = 0,
    /// Discard the job silently without executing
    Discard = 1,
}

#[pymethods]
impl ConcurrencyConflict {
    #[staticmethod]
    fn block() -> Self {
        ConcurrencyConflict::Block
    }

    #[staticmethod]
    fn discard() -> Self {
        ConcurrencyConflict::Discard
    }

    fn __repr__(&self) -> String {
        match self {
            ConcurrencyConflict::Block => "ConcurrencyConflict.Block".to_string(),
            ConcurrencyConflict::Discard => "ConcurrencyConflict.Discard".to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TableConfig {
    pub jobs: String,
    pub ready_executions: String,
    pub claimed_executions: String,
    pub scheduled_executions: String,
    pub failed_executions: String,
    pub blocked_executions: String,
    pub recurring_executions: String,
    pub recurring_tasks: String,
    pub pauses: String,
    pub processes: String,
    pub semaphores: String,
}

impl Default for TableConfig {
    fn default() -> Self {
        Self {
            jobs: "solid_queue_jobs".to_string(),
            ready_executions: "solid_queue_ready_executions".to_string(),
            claimed_executions: "solid_queue_claimed_executions".to_string(),
            scheduled_executions: "solid_queue_scheduled_executions".to_string(),
            failed_executions: "solid_queue_failed_executions".to_string(),
            blocked_executions: "solid_queue_blocked_executions".to_string(),
            recurring_executions: "solid_queue_recurring_executions".to_string(),
            recurring_tasks: "solid_queue_recurring_tasks".to_string(),
            pauses: "solid_queue_pauses".to_string(),
            processes: "solid_queue_processes".to_string(),
            semaphores: "solid_queue_semaphores".to_string(),
        }
    }
}

impl TableConfig {
    pub fn with_prefix(prefix: &str) -> Self {
        // Strip trailing underscore to avoid double underscores (e.g., "hive_" -> "hive__jobs")
        let prefix = prefix.trim_end_matches('_');
        Self {
            jobs: format!("{}_jobs", prefix),
            ready_executions: format!("{}_ready_executions", prefix),
            claimed_executions: format!("{}_claimed_executions", prefix),
            scheduled_executions: format!("{}_scheduled_executions", prefix),
            failed_executions: format!("{}_failed_executions", prefix),
            blocked_executions: format!("{}_blocked_executions", prefix),
            recurring_executions: format!("{}_recurring_executions", prefix),
            recurring_tasks: format!("{}_recurring_tasks", prefix),
            pauses: format!("{}_pauses", prefix),
            processes: format!("{}_processes", prefix),
            semaphores: format!("{}_semaphores", prefix),
        }
    }
}

#[pyclass]
#[derive(Debug, Clone)]
pub struct ConcurrencyStrategy {
    pub to: i64,
    pub duration: Duration,
    pub key: Py<PyAny>,
}

#[pymethods]
impl ConcurrencyStrategy {
    #[new]
    fn new(key: Py<PyAny>, to: Option<i64>, duration: Option<Duration>) -> Self {
        ConcurrencyStrategy {
            to: to.unwrap_or(1),
            duration: duration.unwrap_or(Duration::from_secs(3)),
            key,
        }
    }

    #[getter]
    pub fn to(&self) -> i64 {
        self.to
    }

    #[getter]
    pub fn duration(&self) -> Duration {
        self.duration
    }

    #[getter]
    pub fn key(&self) -> Py<PyAny> {
        self.key.clone()
    }
}

#[pyclass]
#[derive(Debug, Clone)]
pub struct RescueStrategy {
    pub exceptions: Py<PyAny>,
    pub handler: Py<PyAny>,
}

#[pymethods]
impl RescueStrategy {
    #[new]
    fn new(exceptions: Py<PyAny>, handler: Py<PyAny>) -> Self {
        RescueStrategy {
            exceptions,
            handler,
        }
    }

    #[getter]
    pub fn get_exceptions(&self) -> Py<PyAny> {
        self.exceptions.clone()
    }

    #[getter]
    pub fn get_handler(&self) -> Py<PyAny> {
        self.handler.clone()
    }
}

#[pyclass]
#[derive(Debug, Clone)]
pub struct RetryStrategy {
    // pub wait: i64,
    pub wait: Duration,
    pub attempts: i64,
    pub exceptions: Py<PyAny>,
    pub handler: Option<Py<PyAny>>,
}

#[pymethods]
impl RetryStrategy {
    #[new]
    fn new(
        exceptions: Py<PyAny>,
        wait: Option<Duration>,
        attempts: Option<i64>,
        handler: Option<Py<PyAny>>,
    ) -> Self {
        RetryStrategy {
            wait: wait.unwrap_or(Duration::from_secs(3)),
            attempts: attempts.unwrap_or(5),
            exceptions,
            handler,
        }
    }

    #[getter]
    pub fn wait(&self) -> Duration {
        self.wait
    }

    #[getter]
    pub fn attempts(&self) -> i64 {
        self.attempts
    }

    #[getter]
    pub fn get_exceptions(&self) -> Py<PyAny> {
        self.exceptions.clone()
    }

    fn __repr__(&self) -> String {
        format!(
            "RetryStrategy(wait={:?}, attempts={}, exceptions={:?})",
            self.wait, self.attempts, self.exceptions
        )
    }
}

#[pyclass]
#[derive(Debug, Clone)]
pub struct DiscardStrategy {
    pub exceptions: Py<PyAny>,
    pub handler: Option<Py<PyAny>>,
}

#[pymethods]
impl DiscardStrategy {
    #[new]
    fn new(exceptions: Py<PyAny>, handler: Option<Py<PyAny>>) -> Self {
        DiscardStrategy {
            exceptions,
            handler,
        }
    }

    #[getter]
    pub fn get_exceptions(&self) -> Py<PyAny> {
        self.exceptions.clone()
    }

    #[getter]
    pub fn get_handler(&self) -> Option<Py<PyAny>> {
        self.handler.clone()
    }
}

#[derive(Debug)]
pub struct AppContext {
    pub cwd: std::path::PathBuf,
    pub dsn: Url,
    pub db: Option<Arc<DatabaseConnection>>, // Use shared connection for SQLite
    pub connect_options: ConnectOptions,     // For creating new connections
    pub name: String, // Application name for NOTIFY channel (default: "quebec")
    pub use_skip_locked: bool,
    pub process_heartbeat_interval: Duration,
    pub process_alive_threshold: Duration,
    pub shutdown_timeout: Duration,
    pub silence_polling: bool,
    pub preserve_finished_jobs: bool,
    pub clear_finished_jobs_after: Duration,
    pub default_concurrency_control_period: Duration,
    pub dispatcher_polling_interval: Duration,
    pub dispatcher_batch_size: u64,
    pub dispatcher_concurrency_maintenance_interval: Duration,
    pub worker_polling_interval: Duration,
    pub worker_threads: u64,
    pub worker_queues: Option<crate::config::QueueSelector>, // Queue configuration for worker
    pub graceful_shutdown: CancellationToken,
    pub force_quit: CancellationToken,
    pub runnables: Arc<RwLock<HashMap<String, crate::worker::Runnable>>>, // Store job class runnables
    pub concurrency_enabled: Arc<RwLock<HashSet<String>>>, // Store job classes with concurrency control enabled
    pub runtime_handle: Option<Handle>,
    pub table_config: TableConfig, // Dynamic table name configuration
    /// Optional notifier for idle worker threads - when set, signals main loop to poll for new jobs
    pub idle_notify: Arc<RwLock<Option<Arc<Notify>>>>,
}

impl AppContext {
    pub fn new(
        dsn: Url,
        db: Option<Arc<DatabaseConnection>>,
        connect_options: ConnectOptions,
        options: Option<HashMap<String, PyObject>>,
    ) -> Self {
        let mut ctx = Self {
            cwd: std::env::current_dir().unwrap_or_else(|_| std::path::PathBuf::from(".")),
            dsn,
            db,
            connect_options,
            name: std::env::var("QUEBEC_NAME").unwrap_or_else(|_| "quebec".to_string()),
            use_skip_locked: true,
            process_heartbeat_interval: Duration::from_secs(60),
            process_alive_threshold: Duration::from_secs(300),
            shutdown_timeout: Duration::from_secs(5),
            silence_polling: true,
            preserve_finished_jobs: true,
            clear_finished_jobs_after: Duration::from_secs(3600 * 24 * 14), // 14 days
            default_concurrency_control_period: Duration::from_secs(60),    // 1 minute
            dispatcher_polling_interval: Duration::from_secs(1),            // 1 seconds
            dispatcher_batch_size: 500,
            dispatcher_concurrency_maintenance_interval: Duration::from_secs(600),
            worker_polling_interval: Duration::from_millis(100),
            worker_threads: 3,
            worker_queues: None, // Default to all queues
            graceful_shutdown: CancellationToken::new(),
            force_quit: CancellationToken::new(),
            runnables: Arc::new(RwLock::new(HashMap::new())),
            concurrency_enabled: Arc::new(RwLock::new(HashSet::new())),
            runtime_handle: None,
            table_config: TableConfig::default(),
            idle_notify: Arc::new(RwLock::new(None)),
        };

        // Override default configuration if options are provided
        if let Some(options) = options {
            Python::with_gil(|py| {
                // Helper closures that warn on type mismatch
                let get_bool = |key: &str| -> Option<bool> {
                    options.get(key).and_then(|v| {
                        v.extract(py)
                            .map_err(|_| {
                                warn!(
                                    "Config '{}': expected bool, got {:?}",
                                    key,
                                    v.bind(py).get_type()
                                );
                            })
                            .ok()
                    })
                };
                let get_u64 = |key: &str| -> Option<u64> {
                    options.get(key).and_then(|v| {
                        v.extract(py)
                            .map_err(|_| {
                                warn!(
                                    "Config '{}': expected u64, got {:?}",
                                    key,
                                    v.bind(py).get_type()
                                );
                            })
                            .ok()
                    })
                };
                let get_duration = |key: &str| -> Option<Duration> {
                    options.get(key).and_then(|v| {
                        v.extract::<Duration>(py)
                            .or_else(|_| v.extract::<u64>(py).map(Duration::from_secs))
                            .map_err(|_| {
                                warn!(
                                    "Config '{}': expected Duration or u64, got {:?}",
                                    key,
                                    v.bind(py).get_type()
                                );
                            })
                            .ok()
                    })
                };

                if let Some(v) = get_bool("use_skip_locked") {
                    ctx.use_skip_locked = v;
                }
                if let Some(v) = get_duration("process_heartbeat_interval") {
                    ctx.process_heartbeat_interval = v;
                }
                if let Some(v) = get_duration("process_alive_threshold") {
                    ctx.process_alive_threshold = v;
                }
                if let Some(v) = get_duration("shutdown_timeout") {
                    ctx.shutdown_timeout = v;
                }
                if let Some(v) = get_bool("silence_polling") {
                    ctx.silence_polling = v;
                }
                if let Some(v) = get_bool("preserve_finished_jobs") {
                    ctx.preserve_finished_jobs = v;
                }
                if let Some(v) = get_duration("clear_finished_jobs_after") {
                    ctx.clear_finished_jobs_after = v;
                }
                if let Some(v) = get_duration("default_concurrency_control_period") {
                    ctx.default_concurrency_control_period = v;
                }
                if let Some(v) = get_duration("dispatcher_polling_interval") {
                    ctx.dispatcher_polling_interval = v;
                }
                if let Some(v) = get_u64("dispatcher_batch_size") {
                    ctx.dispatcher_batch_size = v;
                }
                if let Some(v) = get_duration("dispatcher_concurrency_maintenance_interval") {
                    ctx.dispatcher_concurrency_maintenance_interval = v;
                }
                // worker_polling_interval: also accepts f64 for sub-second precision
                if let Some(val) = options.get("worker_polling_interval") {
                    let result = val
                        .extract::<Duration>(py)
                        .or_else(|_| val.extract::<u64>(py).map(Duration::from_secs))
                        .or_else(|_| {
                            val.extract::<f64>(py).and_then(|f| {
                                if f.is_finite() && f >= 0.0 {
                                    Ok(Duration::from_secs_f64(f))
                                } else {
                                    warn!(
                                        "Config 'worker_polling_interval': invalid f64 value {}",
                                        f
                                    );
                                    Err(pyo3::PyErr::new::<pyo3::exceptions::PyValueError, _>(
                                        "invalid",
                                    ))
                                }
                            })
                        });
                    match result {
                        Ok(v) => ctx.worker_polling_interval = v,
                        Err(_) => warn!(
                            "Config 'worker_polling_interval': expected Duration, u64 or f64, got {:?}",
                            val.bind(py).get_type()
                        ),
                    }
                }
                if let Some(v) = get_u64("worker_threads") {
                    ctx.worker_threads = v;
                }
                // table_name_prefix: only apply if non-empty
                if let Some(val) = options.get("table_name_prefix") {
                    match val.extract::<String>(py) {
                        Ok(prefix) if !prefix.is_empty() => {
                            ctx.table_config = TableConfig::with_prefix(&prefix);
                        }
                        Ok(_) => {} // empty string, ignore
                        Err(_) => warn!(
                            "Config 'table_name_prefix': expected String, got {:?}",
                            val.bind(py).get_type()
                        ),
                    }
                }
            });
        }

        ctx
    }

    pub fn set_runtime_handle(&mut self, handle: Handle) {
        self.runtime_handle = Some(handle);
    }

    pub fn get_runtime_handle(&self) -> Option<Handle> {
        self.runtime_handle.clone()
    }

    // New method that returns Result type, allowing callers to handle errors
    pub async fn get_db_result(&self) -> Result<Arc<DatabaseConnection>, DbErr> {
        // For SQLite database, always return shared connection (SQLite can't handle multiple write connections)
        if self.dsn.scheme().contains("sqlite") {
            if let Some(db) = &self.db {
                return Ok(db.clone());
            }
        }

        // For PostgreSQL, use shared connection pool instead of creating new connections each time
        // This fixes the 6+ second connection delays we were seeing
        if let Some(db) = &self.db {
            return Ok(db.clone());
        }

        // Fallback: create new connection if no shared connection available
        let conn = Database::connect(self.connect_options.clone()).await?;
        Ok(Arc::new(conn))
    }

    // Keep original interface, but use new error handling internally
    // If error occurs, log error and attempt retry
    pub async fn get_db(&self) -> Arc<DatabaseConnection> {
        // Try to get connection, retry if failed
        for retry in 0..3 {
            match self.get_db_result().await {
                Ok(db) => return db,
                Err(e) => {
                    if retry < 2 {
                        // If there are retry attempts left, log error and wait before retrying
                        warn!(
                            "Failed to get database connection, retrying ({}/3): {}",
                            retry + 1,
                            e
                        );
                        tokio::time::sleep(tokio::time::Duration::from_millis(
                            100 * (retry + 1) as u64,
                        ))
                        .await;
                    } else {
                        // If all retries failed, log error and panic
                        error!("Failed to get database connection after 3 retries: {}", e);
                        panic!("Failed to get database connection: {}", e);
                    }
                }
            }
        }

        // This should never be reached, as panic occurs in loop if all retries fail
        unreachable!()
    }

    /// Check if the database is PostgreSQL
    pub fn is_postgres(&self) -> bool {
        self.dsn.scheme().starts_with("postgres")
    }

    /// Get a runnable by class name
    pub fn get_runnable(&self, class_name: &str) -> Result<crate::worker::Runnable, anyhow::Error> {
        let runnables = self
            .runnables
            .read()
            .map_err(|e| anyhow::anyhow!("Failed to acquire read lock: {}", e))?;
        let runnable = runnables
            .get(class_name)
            .ok_or_else(|| anyhow::anyhow!("Runnable not found for class: {}", class_name))?;

        // Clone with GIL since Runnable contains Py<PyAny>
        Python::with_gil(|py| Ok(runnable.clone_with_gil(py)))
    }

    /// Get all registered runnable class names
    pub fn get_runnable_names(&self) -> Vec<String> {
        self.runnables
            .read()
            .map(|r| r.keys().cloned().collect())
            .unwrap_or_else(|_| Vec::new())
    }

    /// Check if a job class has concurrency control enabled
    pub fn has_concurrency_control(&self, class_name: &str) -> bool {
        self.concurrency_enabled
            .read()
            .map(|c| c.contains(class_name))
            .unwrap_or(false)
    }

    /// Enable concurrency control for a job class
    pub fn enable_concurrency_control(&self, class_name: String) {
        if let Ok(mut concurrency_enabled) = self.concurrency_enabled.write() {
            concurrency_enabled.insert(class_name);
        }
    }

    /// Set process title for better visibility in system tools (htop, ps, etc.)
    /// Format: quebec-app_name [process_type:details]
    /// Examples:
    /// - quebec-myapp [worker:3]
    /// - quebec-myapp [dispatcher]
    /// - quebec-myapp [scheduler]
    pub fn set_proc_title(&self, process_type: &str, details: Option<&str>) {
        let title = if let Some(details) = details {
            format!("quebec-{} [{}:{}]", self.name, process_type, details)
        } else {
            format!("quebec-{} [{}]", self.name, process_type)
        };

        #[cfg(target_os = "macos")]
        {
            crate::proctitle_macos::set_title(&title);
        }

        #[cfg(any(
            target_os = "linux",
            target_os = "freebsd",
            target_os = "openbsd",
            target_os = "netbsd"
        ))]
        {
            crate::proctitle_unix::set_title(&title);
        }

        #[cfg(not(any(
            target_os = "macos",
            target_os = "linux",
            target_os = "freebsd",
            target_os = "openbsd",
            target_os = "netbsd"
        )))]
        {
            // Fallback to proctitle crate for other platforms
            proctitle::set_title(&title);
        }

        trace!("Set process title: {}", title);
    }

    /// Set the idle notifier for worker thread notifications
    pub fn set_idle_notify(&self, notify: Arc<Notify>) {
        if let Ok(mut idle_notify) = self.idle_notify.write() {
            *idle_notify = Some(notify);
        }
    }

    /// Notify that a worker thread has become idle (finished a job)
    /// This triggers the main loop to poll for new jobs immediately
    pub fn notify_idle(&self) {
        if let Ok(idle_notify) = self.idle_notify.read() {
            if let Some(ref notify) = *idle_notify {
                trace!("AppContext: notifying idle");
                notify.notify_one();
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduledEntry {
    pub class: String,
    pub schedule: String,
    pub args: Option<Vec<serde_yaml::Value>>,
    pub key: Option<String>,
    pub queue: Option<String>,
    pub priority: Option<i32>,
}

impl ScheduledEntry {
    pub fn as_cron(&self) -> Result<Cron> {
        let mut expr = self.schedule.clone();
        let _ = str_cron_syntax(&self.schedule).map(|s| {
            let parts: Vec<&str> = s.split_whitespace().collect();
            if parts.len() >= 7 {
                // https://docs.rs/cron/latest/cron: sec min hour day_of_month month day_of_week year
                expr = parts[..6].join(" ");
                debug!("Job({:?}): {} -> {}", self.key, self.schedule, expr);
            }
        });

        let cron = Cron::new(&expr)
            .with_seconds_optional()
            .with_dom_and_dow()
            .parse();
        Ok(cron?)
    }
}
