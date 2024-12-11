use anyhow::Result;
use croner::Cron;
use english_to_cron::str_cron_syntax;
use sea_orm::{ConnectOptions, Database, DatabaseConnection};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use url::Url;

#[cfg(feature = "use-log")]
use log::{debug, error, info, trace, warn};

#[cfg(feature = "use-tracing")]
use tracing::{debug, error, info, trace, warn};

use pyo3::exceptions::PyException;
use pyo3::prelude::*;
pyo3::create_exception!(quebec, CustomError, PyException);

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
    pub db: Arc<DatabaseConnection>,
    pub connect_options: ConnectOptions,
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
}

impl AppContext {
    pub fn new(dsn: Url, db: Arc<DatabaseConnection>, connect_options: ConnectOptions) -> Self {
        Self {
            cwd: std::env::current_dir().unwrap(),
            dsn,
            db,
            connect_options,
            use_skip_locked: true,
            process_heartbeat_interval: Duration::from_secs(60),
            process_alive_threshold: Duration::from_secs(300),
            shutdown_timeout: Duration::from_secs(5),
            silence_polling: true,
            preserve_finished_jobs: true,
            clear_finished_jobs_after: Duration::from_secs(3600 * 24 * 14), // 1 day
            default_concurrency_control_period: Duration::from_secs(60),    // 3 minutes
            dispatcher_polling_interval: Duration::from_secs(30),           // 1s
            dispatcher_batch_size: 500,
            dispatcher_concurrency_maintenance_interval: Duration::from_secs(600),
            worker_polling_interval: Duration::from_secs(1), // 100ms
            // worker_polling_interval: Duration::from_millis(100),
            worker_threads: 3,
            graceful_shutdown: CancellationToken::new(),
            force_quit: CancellationToken::new(),
        }
    }

    pub async fn get_db(&self) -> Arc<DatabaseConnection> {
        // Database::connect(self.connect_options.clone()).await
        self.db.clone()
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
