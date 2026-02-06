use chrono;
use chrono::NaiveDateTime;

use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyDict, PyList, PyTuple, PyType};

use sea_orm::*;
use serde_json::Value;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;
use tokio::task::JoinHandle;
use tokio::time::Duration;

use url::Url;

/// Extract a value from kwargs, falling back to an environment variable, then to a default.
/// Priority: kwargs > env var > default
fn extract_kwarg_or_env<T>(
    kwargs: Option<&Bound<'_, PyDict>>,
    key: &str,
    env_key: &str,
    default: T,
) -> T
where
    T: for<'a, 'py> FromPyObject<'a, 'py> + FromStr,
{
    // 1. Try kwargs — warn on type mismatch when key is present
    if let Some(py_val) = kwargs.and_then(|kw| kw.get_item(key).ok().flatten()) {
        match py_val.extract::<T>() {
            Ok(val) => return val,
            Err(_) => warn!(
                "Config '{}': type mismatch, falling back to env/default",
                key
            ),
        }
    }
    // 2. Try environment variable — warn on parse failure when var is set
    if let Ok(raw) = std::env::var(env_key) {
        match raw.parse::<T>() {
            Ok(parsed) => return parsed,
            Err(_) => warn!(
                "Env '{}': failed to parse '{}', falling back to default",
                env_key, raw
            ),
        }
    }
    // 3. Default
    default
}

use crate::control_plane::{ControlPlane, ControlPlaneExt};

use crate::context::*;
use crate::core::{PreparedJob, Quebec};
use crate::dispatcher::Dispatcher;
use crate::scheduler::Scheduler;

use crate::worker::{Execution, Worker};

use tracing::{debug, error, info, trace, warn};

#[pyfunction]
fn signal_handler(
    py: Python<'_>,
    signum: i32,
    _frame: Py<PyAny>,
    quebec: Py<PyAny>,
) -> PyResult<()> {
    info!("Received signal {}, initiating graceful shutdown", signum);

    if let Err(e) = quebec.bind(py).call_method0("graceful_shutdown") {
        error!("Error calling graceful_shutdown: {:?}", e);
    }

    Ok(())
}

#[pyclass(name = "ActiveLogger", subclass, from_py_object)]
#[derive(Debug, Clone, Default)]
pub struct ActiveLogger;

#[pymethods]
impl ActiveLogger {
    #[new]
    pub fn new() -> Self {
        ActiveLogger
    }

    fn trace(&self, message: String) -> PyResult<()> {
        trace!("{}", message);
        Ok(())
    }

    fn debug(&self, message: String) -> PyResult<()> {
        debug!("{}", message);
        Ok(())
    }

    fn info(&self, message: String) -> PyResult<()> {
        info!("{}", message);
        Ok(())
    }

    fn warn(&self, message: String) -> PyResult<()> {
        warn!("{}", message);
        Ok(())
    }

    fn error(&self, message: String) -> PyResult<()> {
        error!("{}", message);
        Ok(())
    }
}

#[pyclass(from_py_object)]
#[derive(Debug, Clone)]
pub struct AsgiRequest {
    pub method: String,
    pub path: String,
    pub query_string: String,
    pub headers: Vec<(Vec<u8>, Vec<u8>)>,
    pub body: Vec<u8>,
    pub base_path: String,
}

#[pymethods]
impl AsgiRequest {
    #[new]
    #[pyo3(signature = (method, path, query_string, headers, body, base_path="".to_string()))]
    fn new(
        method: String,
        path: String,
        query_string: String,
        headers: Vec<(Vec<u8>, Vec<u8>)>,
        body: Vec<u8>,
        base_path: String,
    ) -> Self {
        Self {
            method,
            path,
            query_string,
            headers,
            body,
            base_path,
        }
    }
}

#[pyclass(name = "Quebec", subclass, from_py_object)]
#[derive(Debug, Clone)]
pub struct PyQuebec {
    pub ctx: Arc<AppContext>,
    pub rt: Arc<tokio::runtime::Runtime>,
    pub url: String,
    pub quebec: Arc<Quebec>,
    pub worker: Arc<Worker>,
    pub dispatcher: Arc<Dispatcher>,
    pub scheduler: Arc<Scheduler>,
    pub shutdown_handlers: Arc<RwLock<Vec<Py<PyAny>>>>,
    pub start_handlers: Arc<RwLock<Vec<Py<PyAny>>>>,
    pub stop_handlers: Arc<RwLock<Vec<Py<PyAny>>>>,
    pyqueue_mode: Arc<AtomicBool>,
    handles: Arc<Mutex<Vec<JoinHandle<()>>>>,
    control_plane_router: Arc<tokio::sync::Mutex<Option<axum::Router>>>,
}

/// Resolve `wait` / `wait_until` options from a Python dict into an optional
/// `NaiveDateTime`.  Returns `None` when neither key is present.
fn resolve_scheduled_at(
    _py: Python<'_>,
    options: &Bound<'_, PyDict>,
) -> PyResult<Option<chrono::NaiveDateTime>> {
    // wait_until takes precedence
    if let Some(val) = options.get_item("wait_until")?.filter(|v| !v.is_none()) {
        // If naive datetime (no tzinfo), treat as UTC — consistent with
        // JobBuilder._calculate_scheduled_at() in Python
        let val = if val.getattr("tzinfo")?.is_none() {
            let tz_utc = val
                .py()
                .import("datetime")?
                .getattr("timezone")?
                .getattr("utc")?;
            let kwargs = PyDict::new(val.py());
            kwargs.set_item("tzinfo", tz_utc)?;
            val.call_method("replace", (), Some(&kwargs))?
        } else {
            val
        };
        let ts: f64 = val
            .call_method0("timestamp")
            .map_err(|_| {
                pyo3::exceptions::PyValueError::new_err("wait_until must be a datetime object")
            })?
            .extract()?;
        let secs = ts as i64;
        let nsecs = ((ts - secs as f64) * 1_000_000_000.0) as u32;
        return chrono::DateTime::from_timestamp(secs, nsecs)
            .map(|dt| Some(dt.naive_utc()))
            .ok_or_else(|| {
                pyo3::exceptions::PyValueError::new_err(format!(
                    "wait_until timestamp out of range: {}",
                    ts
                ))
            });
    }

    if let Some(val) = options.get_item("wait")?.filter(|v| !v.is_none()) {
        let now = chrono::Utc::now().naive_utc();
        // Try as a number (seconds) first, then as timedelta
        if let Ok(secs) = val.extract::<f64>() {
            let duration = chrono::Duration::milliseconds((secs * 1000.0) as i64);
            return Ok(Some(now + duration));
        }
        // timedelta — call .total_seconds()
        if let Ok(total) = val.call_method0("total_seconds") {
            let secs: f64 = total.extract()?;
            let duration = chrono::Duration::milliseconds((secs * 1000.0) as i64);
            return Ok(Some(now + duration));
        }
        return Err(pyo3::exceptions::PyValueError::new_err(
            "wait must be a number (seconds) or timedelta",
        ));
    }

    Ok(None)
}

static INSTANCE_MAP: PyOnceLock<RwLock<HashMap<String, Py<PyQuebec>>>> = PyOnceLock::new();

#[pymethods]
impl PyQuebec {
    #[pyo3(signature = (url, **kwargs))]
    #[new]
    fn new(url: String, kwargs: Option<&Bound<'_, PyDict>>) -> PyResult<Self> {
        info!("PyQuebec<{}>", url);

        let rt = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(16)
                .enable_all()
                .build()
                .map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                        "Failed to build runtime: {}",
                        e
                    ))
                })?,
        );

        let dsn = Url::parse(&url).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid database URL: {}", e))
        })?;
        let mut opt: ConnectOptions = ConnectOptions::new(url.to_string());
        let is_sqlite = url.contains("sqlite");
        let min_conns: u32 = extract_kwarg_or_env(
            kwargs,
            "db_min_connections",
            "QUEBEC_DB_MIN_CONNECTIONS",
            if is_sqlite { 1 } else { 0 },
        );
        let max_conns: u32 = extract_kwarg_or_env(
            kwargs,
            "db_max_connections",
            "QUEBEC_DB_MAX_CONNECTIONS",
            if is_sqlite { 1 } else { 300 },
        );
        let acquire_timeout: u64 =
            extract_kwarg_or_env(kwargs, "db_acquire_timeout", "QUEBEC_DB_ACQUIRE_TIMEOUT", 5);
        let connect_timeout: u64 =
            extract_kwarg_or_env(kwargs, "db_connect_timeout", "QUEBEC_DB_CONNECT_TIMEOUT", 3);
        let idle_timeout: u64 =
            extract_kwarg_or_env(kwargs, "db_idle_timeout", "QUEBEC_DB_IDLE_TIMEOUT", 600);
        let max_lifetime: u64 =
            extract_kwarg_or_env(kwargs, "db_max_lifetime", "QUEBEC_DB_MAX_LIFETIME", 1800);
        if is_sqlite && (min_conns > 1 || max_conns > 1) {
            warn!(
                "SQLite with min_connections={} max_connections={}: \
                 multiple connections may cause 'database is locked' errors",
                min_conns, max_conns
            );
        }
        let max_conns = if max_conns == 0 {
            warn!("db_max_connections is 0, setting to 1");
            1
        } else {
            max_conns
        };
        let min_conns = if min_conns > max_conns {
            warn!(
                "db_min_connections ({}) > db_max_connections ({}), clamping to max",
                min_conns, max_conns
            );
            max_conns
        } else {
            min_conns
        };
        opt.min_connections(min_conns)
            .max_connections(max_conns)
            .acquire_timeout(Duration::from_secs(acquire_timeout))
            .connect_timeout(Duration::from_secs(connect_timeout))
            .idle_timeout(Duration::from_secs(idle_timeout))
            .max_lifetime(Duration::from_secs(max_lifetime))
            .sqlx_logging(true)
            .sqlx_logging_level(tracing::log::LevelFilter::Trace);

        // Pre-create a connection for all database types
        // This connection will be wrapped in Arc and used when needed
        // For high concurrency operations, new connections will be obtained through get_connection method
        let db = rt.block_on(async {
            // Probe with a single connection first to surface real errors
            // (pool timeout errors hide the actual cause)
            let mut probe_opt = opt.clone();
            probe_opt.min_connections(0).max_connections(1);

            let mut retry_count = 0;
            const MAX_RETRIES: usize = 3;

            loop {
                // Phase 1: probe connectivity with a single connection
                let probe = match Database::connect(probe_opt.clone()).await {
                    Ok(db) => db,
                    Err(e) => {
                        retry_count += 1;
                        if retry_count >= MAX_RETRIES {
                            error!(
                                "Failed to connect to database after {} retries: {}",
                                MAX_RETRIES, e
                            );
                            panic!("Database connection failed: {}", e);
                        }
                        warn!(
                            "Database connection attempt {}/{} failed: {}",
                            retry_count, MAX_RETRIES, e
                        );
                        tokio::time::sleep(std::time::Duration::from_millis(
                            1000 * retry_count as u64,
                        ))
                        .await;
                        continue;
                    }
                };

                // Phase 2: verify the connection actually works (ping)
                if let Err(e) = probe.ping().await {
                    retry_count += 1;
                    drop(probe);
                    if retry_count >= MAX_RETRIES {
                        error!("Database ping failed after {} retries: {}", MAX_RETRIES, e);
                        panic!("Database connection failed: {}", e);
                    }
                    warn!(
                        "Database ping attempt {}/{} failed: {}",
                        retry_count, MAX_RETRIES, e
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(1000 * retry_count as u64))
                        .await;
                    continue;
                }
                drop(probe);

                // Phase 3: create the actual connection pool
                match Database::connect(opt.clone()).await {
                    Ok(db) => return db,
                    Err(e) => {
                        retry_count += 1;
                        if retry_count >= MAX_RETRIES {
                            error!(
                                "Failed to create connection pool after {} retries: {}",
                                MAX_RETRIES, e
                            );
                            panic!("Database connection failed: {}", e);
                        }
                        warn!(
                            "Connection pool creation attempt {}/{} failed: {}",
                            retry_count, MAX_RETRIES, e
                        );
                        tokio::time::sleep(std::time::Duration::from_millis(
                            1000 * retry_count as u64,
                        ))
                        .await;
                    }
                }
            }
        });
        let db_option = Some(Arc::new(db));

        // Convert kwargs to HashMap<String, Py<PyAny>>
        let options = kwargs
            .map(|kw| -> PyResult<_> {
                let py = kw.py();
                kw.iter()
                    .map(|(k, v)| {
                        let key = k.extract::<String>().map_err(|_| {
                            PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                                "Invalid option key: {:?}",
                                k
                            ))
                        })?;
                        let obj: Py<PyAny> = v.into_pyobject(py)?.into();
                        Ok((key, obj))
                    })
                    .collect()
            })
            .transpose()?;

        // Try to load configuration file BEFORE creating AppContext
        // This allows code parameters to override config file values
        // Priority: code parameters > QUEBEC_ENV env var > defaults
        let env = kwargs
            .and_then(|kw| kw.get_item("env").ok().flatten())
            .and_then(|v| v.extract::<String>().ok())
            .or_else(|| std::env::var("QUEBEC_ENV").ok());

        let config = crate::config::QueueConfig::find(env.as_deref())
            .inspect(|_| info!("Loaded configuration successfully"))
            .inspect_err(|e| {
                let err_msg = e.to_string().to_lowercase();
                if err_msg.contains("not found") {
                    info!("No configuration file found, using defaults");
                } else {
                    warn!("Failed to load configuration: {}, using defaults", e);
                }
            })
            .ok();

        let mut _ctx = AppContext::new(dsn.clone(), db_option, opt.clone(), options);
        // Bind the runtime handle so other parts can reuse the single runtime
        _ctx.set_runtime_handle(rt.handle().clone());

        // Apply config file values ONLY if not already set by code parameters
        // Check if values are still at their defaults before overriding
        if let Some(config) = config {
            // Apply name if configured
            if let Some(name) = config.name {
                // Override with config file name (environment variable takes precedence in AppContext::new)
                if std::env::var("QUEBEC_NAME").is_err() {
                    _ctx.name = name;
                }
            }

            // Apply worker configuration
            if let Some(workers) = config.workers {
                // Warn if multiple workers are configured (not supported yet)
                if workers.len() > 1 {
                    warn!(
                        "Multiple worker configurations detected ({} workers). Quebec currently only supports a single worker configuration. Only the first worker configuration will be used. \
                        To run multiple workers, use separate config files or environments and start independent processes. \
                        Example: QUEBEC_ENV=realtime python worker1.py & QUEBEC_ENV=background python worker2.py",
                        workers.len()
                    );
                }

                if let Some(worker) = workers.first() {
                    // Only apply if still at default value
                    if let Some(threads) = worker.threads {
                        if _ctx.worker_threads == 3 {
                            // default value
                            _ctx.worker_threads = threads as u64;
                        }
                    }
                    if let Some(polling_interval) = worker.polling_interval {
                        if _ctx.worker_polling_interval == Duration::from_millis(100) {
                            // default value
                            _ctx.worker_polling_interval =
                                Duration::from_secs_f64(polling_interval);
                        }
                    }
                    // Apply queue configuration (always apply from config if present, as there's no code parameter for this)
                    if worker.queues.is_some() {
                        _ctx.worker_queues = worker.queues.clone();
                    }
                    if let Some(ref queues) = _ctx.worker_queues {
                        info!("Worker queues configured: {:?}", queues.to_list());
                    } else {
                        info!("Worker queues: None (will process all queues)");
                    }
                }
            }

            // Apply dispatcher configuration
            if let Some(dispatchers) = config.dispatchers {
                // Warn if multiple dispatchers are configured (not supported yet)
                if dispatchers.len() > 1 {
                    warn!(
                        "Multiple dispatcher configurations detected ({} dispatchers). Quebec currently only supports a single dispatcher configuration. Only the first dispatcher configuration will be used. \
                        To run multiple dispatchers, use separate config files or environments and start independent processes. \
                        Example: QUEBEC_CONFIG=config/queue_dispatcher1.yml python dispatcher1.py & QUEBEC_CONFIG=config/queue_dispatcher2.yml python dispatcher2.py",
                        dispatchers.len()
                    );
                }

                if let Some(dispatcher) = dispatchers.first() {
                    if let Some(polling_interval) = dispatcher.polling_interval {
                        if _ctx.dispatcher_polling_interval == Duration::from_secs(1) {
                            // default value
                            _ctx.dispatcher_polling_interval =
                                Duration::from_secs_f64(polling_interval);
                        }
                    }
                    if let Some(batch_size) = dispatcher.batch_size {
                        if _ctx.dispatcher_batch_size == 500 {
                            // default value
                            _ctx.dispatcher_batch_size = batch_size;
                        }
                    }
                    if let Some(interval) = dispatcher.concurrency_maintenance_interval {
                        if _ctx.dispatcher_concurrency_maintenance_interval
                            == Duration::from_secs(600)
                        {
                            // default value
                            _ctx.dispatcher_concurrency_maintenance_interval =
                                Duration::from_secs_f64(interval);
                        }
                    }
                }
            }
        }

        // All parameters have been processed in AppContext.new
        let ctx = Arc::new(_ctx);
        let quebec = Arc::new(Quebec::new(ctx.clone()));
        let worker = Arc::new(Worker::new(ctx.clone()));
        let dispatcher = Arc::new(Dispatcher::new(ctx.clone()));
        let scheduler = Arc::new(Scheduler::new(ctx.clone()));
        let shutdown_handlers = Arc::new(RwLock::new(Vec::<Py<PyAny>>::new()));
        let start_handlers = Arc::new(RwLock::new(Vec::<Py<PyAny>>::new()));
        let stop_handlers = Arc::new(RwLock::new(Vec::<Py<PyAny>>::new()));

        ctx.set_proc_title("worker", Some(&ctx.worker_threads.to_string()));

        Ok(PyQuebec {
            ctx,
            rt,
            url,
            quebec,
            worker,
            dispatcher,
            scheduler,
            shutdown_handlers,
            start_handlers,
            stop_handlers,
            pyqueue_mode: Arc::new(AtomicBool::new(false)),
            handles: Arc::new(Mutex::new(Vec::new())),
            control_plane_router: Arc::new(tokio::sync::Mutex::new(None)),
        })
    }

    #[staticmethod]
    fn get_instance(py: Python<'_>, url: String) -> PyResult<Py<PyQuebec>> {
        let instance_map = INSTANCE_MAP.get_or_init(py, || RwLock::new(HashMap::new()));

        let mut map = instance_map
            .write()
            .map_err(|_| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Lock poisoned"))?;
        if let Some(instance) = map.get(&url) {
            return Ok(instance.clone());
        }

        let instance = Py::new(py, PyQuebec::new(url.clone(), None)?)?;
        let cloned = instance.clone();
        map.insert(url, cloned);
        Ok(instance)
    }

    fn runner(&self) -> PyResult<()> {
        let worker = self.worker.clone();
        let handle = self.rt.spawn(async move {
            let _ = worker.run().await;
        });

        self.handles
            .lock()
            .map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                    "Failed to acquire lock for handles",
                )
            })?
            .push(handle);
        Ok(())
    }

    fn pick_job(&self, py: Python<'_>) -> PyResult<Execution> {
        let worker = self.worker.clone();

        py.detach(|| {
            let ret = self.rt.block_on(async move { worker.pick_job().await });
            ret.map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "Failed to pick job: {}",
                    e
                ))
            })
        })
    }

    async fn post_job(&self) -> Result<(), anyhow::Error> {
        let _ = self.rt.spawn(async move {
            tokio::time::sleep(Duration::from_secs(5)).await;
        });

        Ok(())
    }

    fn bind_queue(&self, _py: Python<'_>, queue: Py<PyAny>) -> PyResult<()> {
        let worker = self.worker.clone();
        let queue = queue.clone();
        let queue1 = queue.clone();
        let graceful_shutdown = self.ctx.graceful_shutdown.clone();

        self.pyqueue_mode.store(true, Ordering::Relaxed);

        // Use tokio::spawn to start async task
        self.rt.spawn(async move {
            loop {
                let result = worker.pick_job().await;

                // Get GIL and send result to Python queue
                Python::attach(|py| {
                    let queue = queue.bind(py);

                    let Ok(res) = result else {
                        debug!("No job available");
                        return;
                    };

                    // Determine which method to use based on queue type
                    let method_name = if queue.hasattr("put_nowait").unwrap_or(false) {
                        "put_nowait" // asyncio.Queue
                    } else if queue.hasattr("put").unwrap_or(false) {
                        "put" // queue.Queue, multiprocessing.Queue
                    } else {
                        error!("Unsupported queue type");
                        return;
                    };

                    if let Err(e) = queue.getattr(method_name).and_then(|m| m.call1((res,))) {
                        error!("Failed to put job to queue via {}: {}", method_name, e);
                    }
                });
            }
        });

        let handle = self.rt.spawn(async move {
            graceful_shutdown.cancelled().await;

            Python::attach(|py| {
                let queue = queue1.bind(py);

                // Determine shutdown method based on queue type
                let (method_name, success_msg) = if queue.hasattr("close").unwrap_or(false) {
                    ("close", Some("<asyncio.Queue> shutdown"))
                } else if queue.hasattr("join").unwrap_or(false) {
                    ("join", None)
                } else {
                    error!("Unsupported queue type");
                    return;
                };

                match queue.getattr(method_name).and_then(|m| m.call0()) {
                    Ok(_) => {
                        if let Some(msg) = success_msg {
                            info!("{}", msg);
                        }
                    }
                    Err(e) => error!("Failed to {} queue: {}", method_name, e),
                }
            });
        });
        self.handles
            .lock()
            .map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                    "Failed to acquire lock for handles",
                )
            })?
            .push(handle);

        Ok(())
    }

    fn spawn_job_claim_poller(&self) -> PyResult<()> {
        let worker = self.worker.clone();
        let _ = self.rt.spawn(async move {
            let _ = worker.run_main_loop().await;
        });

        Ok(())
    }

    fn spawn_dispatcher(&self) -> PyResult<()> {
        let dispatcher = self.dispatcher.clone();
        let _ = self.rt.spawn(async move {
            let _ = dispatcher.run().await;
        });

        Ok(())
    }

    fn spawn_scheduler(&self) -> PyResult<()> {
        let scheduler = self.scheduler.clone();
        let _ = self.rt.spawn(async move {
            let _ = scheduler.run().await;
        });

        Ok(())
    }

    fn ping(&self) -> PyResult<bool> {
        self.rt.block_on(async move {
            let db = self.ctx.get_db().await;
            Ok(db
                .ping()
                .await
                .inspect_err(|e| error!("Ping failed: {:?}", e))
                .is_ok())
        })
    }

    fn create_tables(&self) -> PyResult<bool> {
        self.rt.block_on(async move {
            let db = self.ctx.get_db().await;

            // Use schema_builder with dynamic table names from TableConfig
            let success =
                crate::schema_builder::setup_database(db.as_ref(), &self.ctx.table_config)
                    .await
                    .inspect(|()| {
                        let prefix = self
                            .ctx
                            .table_config
                            .jobs
                            .strip_suffix("_jobs")
                            .unwrap_or("solid_queue");
                        info!(
                            "Database tables created successfully with prefix: {}",
                            prefix
                        );
                    })
                    .inspect_err(|err| error!("Failed to create tables: {:?}", err))
                    .is_ok();

            Ok(success)
        })
    }

    fn on_start(&mut self, py: Python<'_>, handler: Py<PyAny>) -> PyResult<Py<PyAny>> {
        let callable = handler.bind(py);
        if !callable.is_callable() {
            return Err(PyTypeError::new_err(
                "Expected a callable object for start handler",
            ));
        }

        {
            let mut handlers = self.start_handlers.write().map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                    "Failed to acquire lock for start handlers",
                )
            })?;
            handlers.push(handler.clone());
            trace!("Start handler registered");
        }

        Ok(handler)
    }

    fn on_stop(&mut self, py: Python<'_>, handler: Py<PyAny>) -> PyResult<Py<PyAny>> {
        let callable = handler.bind(py);
        if !callable.is_callable() {
            return Err(PyTypeError::new_err(
                "Expected a callable object for stop handler",
            ));
        }

        {
            let mut handlers = self.stop_handlers.write().map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                    "Failed to acquire lock for stop handlers",
                )
            })?;
            handlers.push(handler.clone());
            trace!("Stop handler registered");
        }

        Ok(handler)
    }

    fn on_worker_start(&mut self, py: Python<'_>, handler: Py<PyAny>) -> PyResult<Py<PyAny>> {
        self.worker.register_start_handler(py, handler.clone())?;
        Ok(handler)
    }

    fn on_worker_stop(&mut self, py: Python<'_>, handler: Py<PyAny>) -> PyResult<Py<PyAny>> {
        self.worker.register_stop_handler(py, handler.clone())?;
        Ok(handler)
    }

    fn register_job_class(&self, py: Python, job_class: Py<PyAny>) -> PyResult<()> {
        // Register with worker (this will store in AppContext.runnables)
        self.worker.register_job_class(py, job_class)?;
        Ok(())
    }

    /// Notify the main loop that a worker thread has become idle.
    /// This triggers an immediate poll for new jobs instead of waiting for the polling interval.
    /// Should be called after each job execution completes.
    fn notify_idle(&self) {
        self.worker.notify_idle();
    }

    #[pyo3(signature = (klass, *args, **kwargs))]
    pub fn perform_later(
        &self,
        py: Python<'_>,
        klass: &Bound<'_, PyType>,
        args: &Bound<'_, PyTuple>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<ActiveJob> {
        let bound = klass;
        let class_name = bound.cast::<PyType>()?.qualname()?;

        let queue_name = match bound.getattr("queue_as") {
            Ok(attr) => attr.extract::<String>()?,
            Err(e) if e.is_instance_of::<pyo3::exceptions::PyAttributeError>(py) => {
                "default".to_string()
            }
            Err(e) => return Err(e),
        };

        let priority = match bound.getattr("queue_with_priority") {
            Ok(attr) => attr.extract::<i32>()?,
            Err(e) if e.is_instance_of::<pyo3::exceptions::PyAttributeError>(py) => 0,
            Err(e) => return Err(e),
        };

        let instance = bound.call0()?;

        // Check if this job class has concurrency control without needing GIL
        let (concurrency_key, concurrency_limit) = if self
            .worker
            .ctx
            .has_concurrency_control(&class_name.to_string())
        {
            // If concurrency_info exists, concurrency control is definitely enabled
            let runnable = self
                .worker
                .ctx
                .get_runnable(&class_name.to_string())
                .map_err(|e| {
                    pyo3::exceptions::PyRuntimeError::new_err(format!(
                        "Failed to get runnable: {:?}",
                        e
                    ))
                })?;

            let concurrency_constraint = runnable
                .get_concurrency_constraint(Some(args), kwargs)
                .map_err(|e| {
                    pyo3::exceptions::PyRuntimeError::new_err(format!(
                        "Failed to get concurrency info: {:?}",
                        e
                    ))
                })?;

            if let Some(constraint) = concurrency_constraint {
                (Some(constraint.key), Some(constraint.limit))
            } else {
                (None, None)
            }
        } else {
            (None, None)
        };

        // Convert Python args and kwargs to JSON for job arguments storage
        let args_json = crate::utils::python_object(&args).into_json()?;
        let kwargs_json = kwargs
            .map(|k| crate::utils::python_object(&k).into_json())
            .transpose()?;

        // Build arguments array with kwargs marker if needed
        let mut arguments_array = if let Value::Array(arr) = args_json {
            arr
        } else {
            vec![]
        };

        // Filter out internal parameters (prefixed with _) and add kwargs marker
        if let Some(Value::Object(kwargs_map)) = &kwargs_json {
            let real_kwargs: serde_json::Map<String, Value> = kwargs_map
                .iter()
                .filter(|(key, _)| !key.starts_with('_'))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();

            if !real_kwargs.is_empty() {
                // Wrap kwargs in a "_kwargs" marker to distinguish from positional dicts
                arguments_array.push(Value::Object(serde_json::Map::from_iter(vec![(
                    "_kwargs".to_string(),
                    Value::Object(real_kwargs),
                )])));
            }
        }

        // Generate job_id early so we can include it in the serialization
        let job_id = crate::utils::generate_job_id();

        // Convert Python strings to Rust strings for serialization
        let class_name_str = class_name.to_string();
        let queue_name_str = queue_name.clone();

        // Build complete job serialization (like Solid Queue format)
        // This ensures arguments column is always a dict with known structure
        let job_data = serde_json::json!({
            "job_class": class_name_str,
            "job_id": job_id,
            "queue_name": queue_name_str,
            "priority": priority,
            "arguments": arguments_array,
            "continuation": {},
            "resumptions": 0
        });

        if let Some(ref key) = concurrency_key {
            debug!("concurrency_key: {:?} for {}", key, class_name);
        }
        if let Some(limit) = concurrency_limit {
            debug!("concurrency_limit: {:?}", limit);
        }

        let arguments = serde_json::to_string(&job_data).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Failed to serialize job data: {}",
                e
            ))
        })?;

        let mut obj = instance.clone().extract::<ActiveJob>()?;
        obj.class_name = class_name.to_string();
        obj.arguments = arguments;
        obj.active_job_id = job_id; // Use the same job_id from serialization
        obj.queue_name = queue_name;
        obj.priority = priority;
        obj.concurrency_key = concurrency_key;
        obj.concurrency_limit = concurrency_limit;

        // Check for internal options in kwargs (used by JobBuilder.set())
        // These are prefixed with _ and will be filtered out from job arguments
        if let Some(kw) = kwargs {
            // _scheduled_at: Override scheduled execution time
            if let Some(dt) = kw
                .get_item("_scheduled_at")
                .ok()
                .flatten()
                .and_then(|val| val.extract::<f64>().ok())
                .and_then(|ts| {
                    let secs = ts as i64;
                    let nsecs = ((ts - secs as f64) * 1_000_000_000.0) as u32;
                    chrono::DateTime::from_timestamp(secs, nsecs)
                })
            {
                obj.scheduled_at = dt.naive_utc();
                debug!("Job scheduled for: {:?}", obj.scheduled_at);
            }

            // _queue: Override queue name
            if let Some(q) = kw
                .get_item("_queue")
                .ok()
                .flatten()
                .and_then(|val| val.extract::<String>().ok())
            {
                obj.queue_name = q;
                debug!("Job queue overridden to: {}", obj.queue_name);
            }

            // _priority: Override priority
            if let Some(p) = kw
                .get_item("_priority")
                .ok()
                .flatten()
                .and_then(|val| val.extract::<i32>().ok())
            {
                obj.priority = p;
                debug!("Job priority overridden to: {}", obj.priority);
            }
        }

        let start_time = Instant::now();
        // Release GIL during database operations to prevent blocking other Python threads
        py.detach(|| {
            self.rt.block_on(async {
                let job = self.quebec.perform_later(obj.clone()).await;
                job
            })
        })
        .map(|job| {
            obj.id.replace(job.id);
            debug!("Job queued in {:?}: {:?}", start_time.elapsed(), obj);
            obj
        })
        .map_err(|e| {
            error!("Error: {:?}", e);
            pyo3::exceptions::PyRuntimeError::new_err(format!("Error: {:?}", e))
        })
    }

    /// Bulk enqueue jobs from a list of JobDescriptor objects.
    /// Each descriptor has: job_class, args, kwargs, options.
    /// All class attribute extraction, JSON serialization and concurrency
    /// resolution happens here (GIL held), then DB operations run with
    /// GIL released.  Returns the number of enqueued jobs.
    fn perform_all_later(&self, py: Python<'_>, descriptors: &Bound<'_, PyList>) -> PyResult<u64> {
        let start_time = Instant::now();

        // Phase 1 (GIL held): extract everything from Python JobDescriptor objects
        let mut prepared: Vec<PreparedJob> = Vec::with_capacity(descriptors.len());
        for item in descriptors.iter() {
            let job_class = item.getattr("job_class")?;
            let py_args = item.getattr("args")?;
            let py_kwargs = item.getattr("kwargs")?;
            let py_options = item.getattr("options")?;

            // Extract class attributes (same as perform_later)
            let class_name = job_class.cast::<PyType>()?.qualname()?.to_string();

            let queue_name = match job_class.getattr("queue_as") {
                Ok(attr) => attr.extract::<String>()?,
                Err(e) if e.is_instance_of::<pyo3::exceptions::PyAttributeError>(py) => {
                    "default".to_string()
                }
                Err(e) => return Err(e),
            };

            let priority = match job_class.getattr("queue_with_priority") {
                Ok(attr) => attr.extract::<i32>()?,
                Err(e) if e.is_instance_of::<pyo3::exceptions::PyAttributeError>(py) => 0,
                Err(e) => return Err(e),
            };

            // Apply option overrides
            let options = py_options.cast::<PyDict>()?;
            let queue_name = options
                .get_item("queue")?
                .and_then(|v| v.extract::<String>().ok())
                .unwrap_or(queue_name);
            let priority = options
                .get_item("priority")?
                .and_then(|v| v.extract::<i32>().ok())
                .unwrap_or(priority);

            // Calculate scheduled_at from wait/wait_until options
            let scheduled_at = resolve_scheduled_at(py, &options)?;

            // Serialize args/kwargs to JSON (reuse perform_later logic)
            let args_bound = py_args.cast::<PyTuple>()?;
            let args_json = crate::utils::python_object(&args_bound).into_json()?;

            let kwargs_bound = py_kwargs.cast::<PyDict>()?;
            let kwargs_json = if kwargs_bound.is_empty() {
                None
            } else {
                Some(crate::utils::python_object(&kwargs_bound).into_json()?)
            };

            let mut arguments_array = if let Value::Array(arr) = args_json {
                arr
            } else {
                vec![]
            };

            if let Some(Value::Object(kwargs_map)) = &kwargs_json {
                let real_kwargs: serde_json::Map<String, Value> = kwargs_map
                    .iter()
                    .filter(|(key, _)| !key.starts_with('_'))
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                if !real_kwargs.is_empty() {
                    arguments_array.push(Value::Object(serde_json::Map::from_iter(vec![(
                        "_kwargs".to_string(),
                        Value::Object(real_kwargs),
                    )])));
                }
            }

            let job_id = crate::utils::generate_job_id();

            let job_data = serde_json::json!({
                "job_class": class_name,
                "job_id": job_id,
                "queue_name": queue_name,
                "priority": priority,
                "arguments": arguments_array,
                "continuation": {},
                "resumptions": 0
            });

            let arguments = serde_json::to_string(&job_data).map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                    "Failed to serialize job data: {}",
                    e
                ))
            })?;

            // Resolve concurrency (if registered)
            let (concurrency_key, concurrency_limit) =
                if self.worker.ctx.has_concurrency_control(&class_name) {
                    if let Ok(runnable) = self.worker.ctx.get_runnable(&class_name) {
                        let kwargs_opt = if kwargs_bound.is_empty() {
                            None
                        } else {
                            Some(kwargs_bound)
                        };
                        let constraint = runnable
                            .get_concurrency_constraint(Some(args_bound), kwargs_opt)
                            .map_err(|e| {
                                pyo3::exceptions::PyRuntimeError::new_err(format!(
                                    "Failed to get concurrency info: {:?}",
                                    e
                                ))
                            })?;
                        constraint.map_or((None, None), |c| (Some(c.key), Some(c.limit)))
                    } else {
                        (None, None)
                    }
                } else {
                    (None, None)
                };

            prepared.push(PreparedJob {
                class_name,
                queue_name,
                priority,
                active_job_id: job_id,
                arguments,
                scheduled_at,
                concurrency_key,
                concurrency_limit,
                concurrency_on_conflict: ConcurrencyConflict::default(),
            });
        }

        // Phase 2 (GIL released): perform database operations
        let count = py
            .detach(|| {
                self.rt
                    .block_on(async { self.quebec.perform_all_later(prepared).await })
            })
            .map(|models| models.len() as u64)
            .map_err(|e| {
                error!("perform_all_later error: {:?}", e);
                pyo3::exceptions::PyRuntimeError::new_err(format!("Error: {:?}", e))
            })?;

        debug!("Bulk enqueued {} jobs in {:?}", count, start_time.elapsed());
        Ok(count)
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("Quebec(url={})", self.url))
    }

    // implment a decorator to register job class
    fn register_job(&self, py: Python, job_class: Py<PyAny>) -> PyResult<Py<PyAny>> {
        // Check if the passed object is a class
        if !job_class.bind(py).is_instance_of::<PyType>() {
            return Err(PyTypeError::new_err(
                "Expected a class, but got an instance",
            ));
        }

        let binding = job_class.clone();
        let bound = binding.bind(py);

        let instance = bound.call0()?;
        if !instance.is_instance_of::<ActiveJob>() {
            return Err(pyo3::exceptions::PyTypeError::new_err(
                "Job class must be a subclass of ActiveJob",
            ));
        }

        let _ = job_class.setattr(py, "quebec", self.clone().into_pyobject(py)?);
        let dup = job_class.clone();
        let _ = self.worker.register_job_class(py, job_class);
        Ok(dup)
    }

    fn setup_signal_handler(&self, py: Python) -> PyResult<Py<PyAny>> {
        info!("Setting up signal handlers");
        let signal = py.import("signal")?;

        let handler_func = wrap_pyfunction!(signal_handler)(py)?;
        let functools = py.import("functools")?;
        let quebec_obj: Py<PyAny> = self.clone().into_pyobject(py)?.into();

        let kwargs = PyDict::new(py);
        kwargs.set_item("quebec", quebec_obj)?;

        let wrapped_handler = functools
            .getattr("partial")?
            .call((handler_func,), Some(&kwargs))?;

        signal.call_method1(
            "signal",
            (signal.getattr("SIGINT")?, wrapped_handler.clone()),
        )?;
        signal.call_method1(
            "signal",
            (signal.getattr("SIGTERM")?, wrapped_handler.clone()),
        )?;
        signal.call_method1(
            "signal",
            (signal.getattr("SIGQUIT")?, wrapped_handler.clone()),
        )?;

        info!("Signal handlers registered for SIGINT and SIGTERM and SIGQUIT");
        Ok(wrapped_handler.into_pyobject(py)?.into())
    }

    fn spawn_all(&mut self) -> PyResult<()> {
        // Call start handlers first (they may create tables or do migrations)
        if let Err(e) = self.invoke_start_handlers() {
            error!("Error calling start handlers: {:?}", e);
        }

        // Check if database tables exist after start handlers have run
        let ctx = self.ctx.clone();
        let check_result = self.rt.block_on(async {
            let db = ctx.get_db().await;
            crate::schema_builder::check_tables_exist(db.as_ref(), &ctx.table_config).await
        });

        match check_result {
            Ok(true) => {}
            Ok(false) => {
                error!(
                    "Database tables not found! Run create_tables() or set create_tables=True before starting."
                );
                return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                    "Database tables not found. Run create_tables() or set create_tables=True before starting.",
                ));
            }
            Err(e) => {
                error!("Database error while checking tables: {}", e);
                return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "Database error: {}",
                    e
                )));
            }
        }

        // Spawn all components
        self.spawn_dispatcher()?;
        self.spawn_scheduler()?;
        self.spawn_job_claim_poller()?;

        Ok(())
    }

    fn graceful_shutdown(&self, py: Python) -> PyResult<()> {
        info!("Graceful shutdown initiated");

        // Call stop handlers first
        if let Err(e) = self.invoke_stop_handlers() {
            error!("Error calling stop handlers: {:?}", e);
        }

        // Then call shutdown handlers
        let handlers = self
            .shutdown_handlers
            .read()
            .map_err(|_| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Lock poisoned"))?;
        for handler in handlers.iter() {
            debug!("Invoke shutdown handler: {:?}", handler);
            // Call the handler and log any errors, but continue to the next handler
            if let Err(e) = handler.bind(py).call0() {
                error!("Error calling shutdown handler: {:?}", e);
            }
        }

        // Cancel the token to initiate component shutdown
        self.ctx.graceful_shutdown.cancel();

        let timeout = self.ctx.shutdown_timeout;
        let quit = self.ctx.force_quit.clone();
        let handles = self.handles.clone();
        let rt = self.rt.clone();

        // Temporarily release GIL
        py.detach(|| {
            // block call would continuously occupy GIL preventing other threads from executing
            rt.block_on(async move {
                let result = tokio::time::timeout(timeout, async {
                    let handles = {
                        let mut handles = handles.lock().expect("Failed to lock handles");
                        debug!("Waiting for {} tasks to exit gracefully", handles.len());
                        handles.drain(..).collect::<Vec<_>>()
                    };
                    for handle in handles {
                        if let Err(e) = handle.await {
                            warn!("Task failed: {:?}", e);
                        }
                    }
                })
                .await;

                match result {
                    Ok(_) => info!("All tasks have exited gracefully"),
                    Err(_) => {
                        quit.cancel(); // Send a signal to force quit
                        warn!("Force quit due to timeout");
                    }
                }
            });
        });

        std::process::exit(0);
    }

    pub fn invoke_start_handlers(&self) -> PyResult<()> {
        Python::attach(|py| {
            let handlers = self
                .start_handlers
                .read()
                .map_err(|_| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Lock poisoned"))?;
            for handler in handlers.iter() {
                if let Err(e) = handler.bind(py).call0() {
                    error!("Error calling start handler: {:?}", e);
                }
            }
            Ok(())
        })
    }

    pub fn invoke_stop_handlers(&self) -> PyResult<()> {
        Python::attach(|py| {
            let handlers = self
                .stop_handlers
                .read()
                .map_err(|_| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Lock poisoned"))?;
            for handler in handlers.iter() {
                if let Err(e) = handler.bind(py).call0() {
                    error!("Error calling stop handler: {:?}", e);
                }
            }
            Ok(())
        })
    }

    fn on_shutdown(&mut self, py: Python, handler: Py<PyAny>) -> PyResult<Py<PyAny>> {
        let callable = handler.bind(py);
        if !callable.is_callable() {
            return Err(PyTypeError::new_err(
                "Expected a callable object for shutdown handler",
            ));
        }

        {
            let mut handlers = self.shutdown_handlers.write().map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                    "Failed to acquire lock for shutdown handlers",
                )
            })?;
            handlers.push(handler.clone());
            debug!("Shutdown handler: {:?} registered", handler);
        }

        Ok(handler)
    }

    fn handle_control_plane_request(
        &self,
        py: Python<'_>,
        req: &AsgiRequest,
    ) -> PyResult<(u16, Vec<(Vec<u8>, Vec<u8>)>, Vec<u8>)> {
        let uri = if req.query_string.is_empty() {
            req.path.clone()
        } else {
            format!("{}?{}", req.path, req.query_string)
        };

        let rt = self.rt.clone();
        let router_lock = self.control_plane_router.clone();
        let ctx = self.ctx.clone();
        let base_path = req.base_path.clone();
        let method = req.method.clone();
        let headers = req.headers.clone();
        let body = req.body.clone();

        // Release GIL first, then clone the router for concurrent request handling.
        // The mutex is only held briefly for init/clone, not for the entire request.
        Ok(py.detach(|| {
            rt.block_on(async {
                let mut router = {
                    let mut guard = router_lock.lock().await;
                    if guard.is_none() {
                        let cp = Arc::new(ControlPlane::new(ctx).with_base_path(base_path));
                        *guard = Some(cp.build_router());
                    }
                    guard.as_ref().unwrap().clone()
                };
                ControlPlane::handle_request(&mut router, &method, &uri, headers, body).await
            })
        }))
    }

    fn start_control_plane(&self, addr: String) -> PyResult<()> {
        let ctx = self.ctx.clone();
        let rt = self.rt.clone();
        let handle = rt.spawn(async move {
            ControlPlaneExt::start_control_plane(&ctx, addr)
                .await
                .inspect(|_| debug!("Control plane server task completed"))
                .inspect_err(|e| error!("Control plane server task failed: {}", e))
                .ok();
        });
        self.handles
            .lock()
            .map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                    "Failed to acquire lock for handles",
                )
            })?
            .push(handle);
        Ok(())
    }

    /// Set process title for better visibility in system tools (htop, ps, etc.)
    /// This should be called from Python's main thread
    /// Format: quebec-app_name [process_type:details]
    /// Examples:
    /// - quebec.set_proc_title("worker", "3")
    /// - quebec.set_proc_title("dispatcher", None)
    /// - quebec.set_proc_title("scheduler", None)
    #[pyo3(signature = (process_type, details=None))]
    fn set_proc_title(&self, process_type: &str, details: Option<String>) -> PyResult<()> {
        self.ctx.set_proc_title(process_type, details.as_deref());
        Ok(())
    }

    /// Clear finished jobs older than the configured threshold.
    /// Returns the total number of jobs deleted.
    ///
    /// Args:
    ///     batch_size: Optional batch size for deletion (default: cleanup_batch_size config)
    ///     finished_before: Optional timestamp - delete jobs finished before this time
    ///                      (default: now - clear_finished_jobs_after config)
    ///
    /// Returns:
    ///     int: Number of deleted jobs
    #[pyo3(signature = (batch_size=None, finished_before=None))]
    fn clear_finished_jobs(
        &self,
        py: Python<'_>,
        batch_size: Option<u64>,
        finished_before: Option<f64>,
    ) -> PyResult<u64> {
        let batch_size = batch_size.unwrap_or(self.ctx.cleanup_batch_size);
        let clear_after = self.ctx.clear_finished_jobs_after;
        let table_config = self.ctx.table_config.clone();

        // Calculate the cutoff timestamp
        let finished_before = match finished_before {
            Some(ts) => {
                // Validate timestamp: reject NaN and infinity
                if ts.is_nan() {
                    return Err(pyo3::exceptions::PyValueError::new_err(
                        "finished_before cannot be NaN",
                    ));
                }
                if ts.is_infinite() {
                    return Err(pyo3::exceptions::PyValueError::new_err(
                        "finished_before cannot be infinite",
                    ));
                }
                let secs = ts as i64;
                let nsecs = ((ts - secs as f64) * 1_000_000_000.0) as u32;
                chrono::DateTime::from_timestamp(secs, nsecs)
                    .map(|dt| dt.naive_utc())
                    .ok_or_else(|| {
                        pyo3::exceptions::PyValueError::new_err(format!(
                            "Invalid timestamp: {} is out of range",
                            ts
                        ))
                    })?
            }
            None => {
                let duration = chrono::Duration::from_std(clear_after).map_err(|e| {
                    pyo3::exceptions::PyValueError::new_err(format!(
                        "Invalid clear_finished_jobs_after duration: {}",
                        e
                    ))
                })?;
                chrono::Utc::now().naive_utc() - duration
            }
        };

        py.detach(|| {
            self.rt.block_on(async {
                let db = self.ctx.get_db().await;
                let mut total_deleted: u64 = 0;

                loop {
                    let deleted = crate::query_builder::jobs::delete_finished_before(
                        db.as_ref(),
                        &table_config,
                        finished_before,
                        batch_size,
                    )
                    .await
                    .map_err(|e| {
                        pyo3::exceptions::PyRuntimeError::new_err(format!(
                            "Failed to delete finished jobs: {}",
                            e
                        ))
                    })?;

                    total_deleted += deleted;

                    if deleted == 0 {
                        break;
                    }

                    // Sleep briefly between batches (same as worker's clear_finished_jobs)
                    tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;
                }

                Ok(total_deleted)
            })
        })
    }

    /// Count finished jobs older than the configured threshold that can be cleared.
    ///
    /// Args:
    ///     finished_before: Optional timestamp - count jobs finished before this time
    ///                      (default: now - clear_finished_jobs_after config)
    ///
    /// Returns:
    ///     int: Number of clearable jobs
    #[pyo3(signature = (finished_before=None))]
    fn count_clearable_jobs(&self, py: Python<'_>, finished_before: Option<f64>) -> PyResult<u64> {
        let clear_after = self.ctx.clear_finished_jobs_after;
        let table_config = self.ctx.table_config.clone();

        // Calculate the cutoff timestamp
        let finished_before = match finished_before {
            Some(ts) => {
                // Validate timestamp: reject NaN and infinity
                if ts.is_nan() {
                    return Err(pyo3::exceptions::PyValueError::new_err(
                        "finished_before cannot be NaN",
                    ));
                }
                if ts.is_infinite() {
                    return Err(pyo3::exceptions::PyValueError::new_err(
                        "finished_before cannot be infinite",
                    ));
                }
                let secs = ts as i64;
                let nsecs = ((ts - secs as f64) * 1_000_000_000.0) as u32;
                chrono::DateTime::from_timestamp(secs, nsecs)
                    .map(|dt| dt.naive_utc())
                    .ok_or_else(|| {
                        pyo3::exceptions::PyValueError::new_err(format!(
                            "Invalid timestamp: {} is out of range",
                            ts
                        ))
                    })?
            }
            None => {
                let duration = chrono::Duration::from_std(clear_after).map_err(|e| {
                    pyo3::exceptions::PyValueError::new_err(format!(
                        "Invalid clear_finished_jobs_after duration: {}",
                        e
                    ))
                })?;
                chrono::Utc::now().naive_utc() - duration
            }
        };

        py.detach(|| {
            self.rt.block_on(async {
                let db = self.ctx.get_db().await;
                crate::query_builder::jobs::count_finished_before(
                    db.as_ref(),
                    &table_config,
                    finished_before,
                )
                .await
                .map_err(|e| {
                    pyo3::exceptions::PyRuntimeError::new_err(format!(
                        "Failed to count clearable jobs: {}",
                        e
                    ))
                })
            })
        })
    }
}

#[pyclass(name = "ActiveJob", subclass, from_py_object)]
#[derive(Debug, Clone)]
pub struct ActiveJob {
    pub logger: ActiveLogger,
    pub id: Option<i64>,
    pub queue_name: String,
    pub class_name: String,
    pub arguments: String,
    pub priority: i32,
    pub executions: i32,
    pub active_job_id: String,
    pub scheduled_at: chrono::NaiveDateTime,
    pub finished_at: Option<chrono::NaiveDateTime>,
    pub concurrency_key: Option<String>,
    pub concurrency_limit: Option<i32>,
    pub concurrency_on_conflict: crate::context::ConcurrencyConflict,
    pub created_at: Option<chrono::NaiveDateTime>,
    pub updated_at: Option<chrono::NaiveDateTime>,
}

#[pymethods]
impl ActiveJob {
    #[classattr]
    fn rescue_strategies() -> Vec<RescueStrategy> {
        vec![]
    }

    #[new]
    fn new() -> Self {
        let logger = ActiveLogger::new();
        Self {
            logger,
            id: None,
            queue_name: "default".to_string(),
            class_name: "".to_string(),
            arguments: "[]".to_string(),
            priority: 0,
            executions: 0,
            active_job_id: "".to_string(),
            scheduled_at: chrono::Utc::now().naive_utc(),
            finished_at: None,
            concurrency_key: None,
            concurrency_limit: None,
            concurrency_on_conflict: crate::context::ConcurrencyConflict::default(),
            created_at: None,
            updated_at: None,
        }
    }

    #[getter]
    pub fn get_id(&self) -> Option<i64> {
        self.id
    }

    #[setter]
    fn set_id(&mut self, id: Option<i64>) {
        self.id = id;
    }

    #[getter]
    pub fn get_queue_name(&self) -> &str {
        &self.queue_name
    }

    #[getter]
    pub fn get_class_name(&self) -> &str {
        &self.class_name
    }

    #[getter]
    pub fn get_arguments(&self) -> &str {
        &self.arguments
    }

    #[getter]
    pub fn get_priority(&self) -> i32 {
        self.priority
    }

    #[getter]
    pub fn get_executions(&self) -> i32 {
        self.executions
    }

    #[setter]
    fn set_executions(&mut self, executions: i32) {
        self.executions = executions;
    }

    #[getter]
    pub fn get_active_job_id(&self) -> &str {
        &self.active_job_id
    }

    #[getter]
    pub fn get_scheduled_at(&self) -> NaiveDateTime {
        self.scheduled_at
    }

    #[getter]
    pub fn get_finished_at(&self) -> Option<NaiveDateTime> {
        self.finished_at
    }

    #[getter]
    pub fn get_concurrency_key(&self) -> Option<&str> {
        self.concurrency_key.as_deref()
    }

    #[getter]
    pub fn get_concurrency_on_conflict(&self) -> crate::context::ConcurrencyConflict {
        self.concurrency_on_conflict
    }

    #[setter]
    pub fn set_concurrency_on_conflict(&mut self, value: crate::context::ConcurrencyConflict) {
        self.concurrency_on_conflict = value;
    }

    #[getter]
    pub fn get_created_at(&self) -> Option<NaiveDateTime> {
        self.created_at
    }

    #[getter]
    pub fn get_updated_at(&self) -> Option<NaiveDateTime> {
        self.updated_at
    }

    #[classmethod]
    pub fn register_rescue_strategy(
        cls: &Bound<'_, PyType>,
        py: Python<'_>,
        strategy: Py<PyAny>,
    ) -> PyResult<()> {
        let strategy = strategy.extract::<RescueStrategy>(py)?;
        let rescue_strategies = cls.getattr("rescue_strategies")?;
        rescue_strategies.call_method1("append", (strategy.clone(),))?;
        Ok(())
    }

    pub fn __repr__(&self) -> PyResult<String> {
        Ok(format!(
            "ActiveJob(id={:?}, queue_name={}, class_name={}, arguments={}, priority={}, active_job_id={}, scheduled_at={})",
            self.id, self.queue_name, self.class_name, self.arguments, self.priority, self.active_job_id, self.scheduled_at
        ))
    }

    fn before_enqueue(&self) {
        debug!("before_enqueue");
    }

    fn after_enqueue(&self) {
        debug!("after_enqueue");
    }

    fn before_perform(&self) {
        debug!("before_perform");
    }

    fn after_perform(&self) {
        debug!("after_perform");
    }

    #[getter]
    fn get_logger(&self) -> PyResult<ActiveLogger> {
        Ok(self.logger.clone())
    }

    #[classmethod]
    #[pyo3(signature = ( quebec, *args, **kwargs))]
    fn perform_later(
        cls: &Bound<'_, PyType>,
        py: Python<'_>,
        quebec: &PyQuebec,
        args: &Bound<'_, PyTuple>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<ActiveJob> {
        quebec.perform_later(py, cls, args, kwargs)
    }

    #[classmethod]
    #[pyo3(signature = (*args, **kwargs))]
    fn perform_later1(
        cls: &Bound<'_, PyType>,
        py: Python<'_>,
        args: &Bound<'_, PyTuple>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<ActiveJob> {
        let quebec = cls.getattr("quebec")?.extract::<PyQuebec>()?;
        quebec.perform_later(py, cls, args, kwargs)
    }
}
