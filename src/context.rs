use anyhow::Result;
use croner::Cron;
use english_to_cron::str_cron_syntax;
use sea_orm::{ConnectOptions, Database, DatabaseConnection, DbErr};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::runtime::Handle;
use tokio_util::sync::CancellationToken;
use url::Url;

use tracing::{debug, error, warn};

use pyo3::exceptions::PyException;
use pyo3::prelude::*;
pyo3::create_exception!(quebec, CustomError, PyException);

#[derive(Debug, Clone)]
pub struct ConcurrencyConstraint {
    pub key: String,
    pub limit: i32,
    pub duration: Option<chrono::Duration>,
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
        RescueStrategy { exceptions, handler }
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
        exceptions: Py<PyAny>, wait: Option<Duration>, attempts: Option<i64>,
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
        DiscardStrategy { exceptions, handler }
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
    pub connect_options: ConnectOptions, // For creating new connections
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
    pub graceful_shutdown: CancellationToken,
    pub force_quit: CancellationToken,
    pub runnables: Arc<RwLock<HashMap<String, crate::worker::Runnable>>>, // Store job class runnables
    pub concurrency_enabled: Arc<RwLock<HashSet<String>>>, // Store job classes with concurrency control enabled
    pub runtime_handle: Option<Handle>,
}

impl AppContext {
    pub fn new(
        dsn: Url,
        db: Option<Arc<DatabaseConnection>>,
        connect_options: ConnectOptions,
        options: Option<HashMap<String, PyObject>>
    ) -> Self {
        let mut ctx = Self {
            cwd: std::env::current_dir().unwrap_or_else(|_| std::path::PathBuf::from(".")),
            dsn,
            db,
            connect_options,
            use_skip_locked: true,
            process_heartbeat_interval: Duration::from_secs(60),
            process_alive_threshold: Duration::from_secs(300),
            shutdown_timeout: Duration::from_secs(5),
            silence_polling: true,
            preserve_finished_jobs: true,
            clear_finished_jobs_after: Duration::from_secs(3600 * 24 * 14), // 14 days
            default_concurrency_control_period: Duration::from_secs(60),    // 1 minute
            dispatcher_polling_interval: Duration::from_secs(1),           // 1 seconds
            dispatcher_batch_size: 500,
            dispatcher_concurrency_maintenance_interval: Duration::from_secs(600),
            worker_polling_interval: Duration::from_millis(100),
            worker_threads: 3,
            graceful_shutdown: CancellationToken::new(),
            force_quit: CancellationToken::new(),
            runnables: Arc::new(RwLock::new(HashMap::new())),
            concurrency_enabled: Arc::new(RwLock::new(HashSet::new())),
            runtime_handle: None,
        };

        // Override default configuration if options are provided
        if let Some(options) = options {
            Python::with_gil(|py| {
                // Handle use_skip_locked
                if let Some(val) = options.get("use_skip_locked") {
                    if let Ok(value) = val.extract::<bool>(py) {
                        ctx.use_skip_locked = value;
                    }
                }

                // Handle process_heartbeat_interval
                if let Some(val) = options.get("process_heartbeat_interval") {
                    if let Ok(value) = val.extract::<Duration>(py) {
                        ctx.process_heartbeat_interval = value;
                    } else if let Ok(value) = val.extract::<u64>(py) {
                        ctx.process_heartbeat_interval = Duration::from_secs(value);
                    }
                }

                // Handle process_alive_threshold
                if let Some(val) = options.get("process_alive_threshold") {
                    if let Ok(value) = val.extract::<Duration>(py) {
                        ctx.process_alive_threshold = value;
                    } else if let Ok(value) = val.extract::<u64>(py) {
                        ctx.process_alive_threshold = Duration::from_secs(value);
                    }
                }

                // Handle shutdown_timeout
                if let Some(val) = options.get("shutdown_timeout") {
                    if let Ok(value) = val.extract::<Duration>(py) {
                        ctx.shutdown_timeout = value;
                    } else if let Ok(value) = val.extract::<u64>(py) {
                        ctx.shutdown_timeout = Duration::from_secs(value);
                    }
                }

                // Handle silence_polling
                if let Some(val) = options.get("silence_polling") {
                    if let Ok(value) = val.extract::<bool>(py) {
                        ctx.silence_polling = value;
                    }
                }

                // Handle preserve_finished_jobs
                if let Some(val) = options.get("preserve_finished_jobs") {
                    if let Ok(value) = val.extract::<bool>(py) {
                        ctx.preserve_finished_jobs = value;
                    }
                }

                // Handle clear_finished_jobs_after
                if let Some(val) = options.get("clear_finished_jobs_after") {
                    if let Ok(value) = val.extract::<Duration>(py) {
                        ctx.clear_finished_jobs_after = value;
                    } else if let Ok(value) = val.extract::<u64>(py) {
                        ctx.clear_finished_jobs_after = Duration::from_secs(value);
                    }
                }

                // Handle default_concurrency_control_period
                if let Some(val) = options.get("default_concurrency_control_period") {
                    if let Ok(value) = val.extract::<Duration>(py) {
                        ctx.default_concurrency_control_period = value;
                    } else if let Ok(value) = val.extract::<u64>(py) {
                        ctx.default_concurrency_control_period = Duration::from_secs(value);
                    }
                }

                // Handle dispatcher_polling_interval
                if let Some(val) = options.get("dispatcher_polling_interval") {
                    if let Ok(value) = val.extract::<Duration>(py) {
                        ctx.dispatcher_polling_interval = value;
                    } else if let Ok(value) = val.extract::<u64>(py) {
                        ctx.dispatcher_polling_interval = Duration::from_secs(value);
                    }
                }

                // Handle dispatcher_batch_size
                if let Some(val) = options.get("dispatcher_batch_size") {
                    if let Ok(value) = val.extract::<u64>(py) {
                        ctx.dispatcher_batch_size = value;
                    }
                }

                // Handle dispatcher_concurrency_maintenance_interval
                if let Some(val) = options.get("dispatcher_concurrency_maintenance_interval") {
                    if let Ok(value) = val.extract::<Duration>(py) {
                        ctx.dispatcher_concurrency_maintenance_interval = value;
                    } else if let Ok(value) = val.extract::<u64>(py) {
                        ctx.dispatcher_concurrency_maintenance_interval = Duration::from_secs(value);
                    }
                }

                // Handle worker_polling_interval
                if let Some(val) = options.get("worker_polling_interval") {
                    if let Ok(value) = val.extract::<Duration>(py) {
                        ctx.worker_polling_interval = value;
                    } else if let Ok(value) = val.extract::<u64>(py) {
                        ctx.worker_polling_interval = Duration::from_secs(value);
                    } else if let Ok(value) = val.extract::<f64>(py) {
                        ctx.worker_polling_interval = Duration::from_millis((value * 1000.0) as u64);
                    }
                }

                // Handle worker_threads
                if let Some(val) = options.get("worker_threads") {
                    if let Ok(value) = val.extract::<u64>(py) {
                        ctx.worker_threads = value;
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
                        warn!("Failed to get database connection, retrying ({}/3): {}", retry + 1, e);
                        tokio::time::sleep(tokio::time::Duration::from_millis(100 * (retry + 1) as u64)).await;
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
        let runnables = self.runnables.read()
            .map_err(|e| anyhow::anyhow!("Failed to acquire read lock: {}", e))?;
        let runnable = runnables.get(class_name)
            .ok_or_else(|| anyhow::anyhow!("Runnable not found for class: {}", class_name))?;

        // Clone with GIL since Runnable contains Py<PyAny>
        Python::with_gil(|py| {
            Ok(runnable.clone_with_gil(py))
        })
    }

    /// Get all registered runnable class names
    pub fn get_runnable_names(&self) -> Vec<String> {
        self.runnables.read()
            .map(|r| r.keys().cloned().collect())
            .unwrap_or_else(|_| Vec::new())
    }

    /// Check if a job class has concurrency control enabled
    pub fn has_concurrency_control(&self, class_name: &str) -> bool {
        self.concurrency_enabled.read()
            .map(|c| c.contains(class_name))
            .unwrap_or(false)
    }

    /// Enable concurrency control for a job class
    pub fn enable_concurrency_control(&self, class_name: String) {
        if let Ok(mut concurrency_enabled) = self.concurrency_enabled.write() {
            concurrency_enabled.insert(class_name);
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduledEntry {
    pub class: String,
    pub schedule: String,
    pub args: Option<Vec<serde_yaml::Value>>,
    pub key: Option<String>,
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

        let cron = Cron::new(&expr).with_seconds_optional().with_dom_and_dow().parse();
        Ok(cron?)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Schedule {
    pub entries: HashMap<String, ScheduledEntry>,
}
