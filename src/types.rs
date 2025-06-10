use chrono;
use chrono::NaiveDateTime;

use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::sync::GILOnceCell;
use pyo3::types::{PyDict, PyTuple, PyType};
use pyo3::types::{PyFloat, PyInt, PyList, PyString};

// use pyo3_asyncio::tokio::future_into_py;
use sea_orm::sea_query::TableCreateStatement;
use sea_orm::*;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;
use tokio::task::JoinHandle;
use tokio::time::Duration;

use url::Url;
use uuid;

use crate::control_plane::ControlPlaneExt;

use crate::context::*;
use crate::core::Quebec;
use crate::dispatcher::Dispatcher;
use crate::entities::*;
use crate::scheduler::Scheduler;
use crate::worker::{Execution, Worker};

use tracing::{debug, error, info, trace, warn};

trait EntityTrait {
    fn create_table_statement(&self, schema: &Schema) -> TableCreateStatement;
}

impl EntityTrait for solid_queue_blocked_executions::Entity {
    fn create_table_statement(&self, schema: &Schema) -> TableCreateStatement {
        schema.create_table_from_entity(*self)
    }
}

impl EntityTrait for solid_queue_claimed_executions::Entity {
    fn create_table_statement(&self, schema: &Schema) -> TableCreateStatement {
        schema.create_table_from_entity(*self)
    }
}

impl EntityTrait for solid_queue_failed_executions::Entity {
    fn create_table_statement(&self, schema: &Schema) -> TableCreateStatement {
        schema.create_table_from_entity(*self)
    }
}

impl EntityTrait for solid_queue_jobs::Entity {
    fn create_table_statement(&self, schema: &Schema) -> TableCreateStatement {
        schema.create_table_from_entity(*self)
    }
}

impl EntityTrait for solid_queue_pauses::Entity {
    fn create_table_statement(&self, schema: &Schema) -> TableCreateStatement {
        schema.create_table_from_entity(*self)
    }
}

impl EntityTrait for solid_queue_processes::Entity {
    fn create_table_statement(&self, schema: &Schema) -> TableCreateStatement {
        schema.create_table_from_entity(*self)
    }
}

impl EntityTrait for solid_queue_ready_executions::Entity {
    fn create_table_statement(&self, schema: &Schema) -> TableCreateStatement {
        schema.create_table_from_entity(*self)
    }
}

impl EntityTrait for solid_queue_recurring_executions::Entity {
    fn create_table_statement(&self, schema: &Schema) -> TableCreateStatement {
        schema.create_table_from_entity(*self)
    }
}

impl EntityTrait for solid_queue_recurring_tasks::Entity {
    fn create_table_statement(&self, schema: &Schema) -> TableCreateStatement {
        schema.create_table_from_entity(*self)
    }
}

impl EntityTrait for solid_queue_scheduled_executions::Entity {
    fn create_table_statement(&self, schema: &Schema) -> TableCreateStatement {
        schema.create_table_from_entity(*self)
    }
}

impl EntityTrait for solid_queue_semaphores::Entity {
    fn create_table_statement(&self, schema: &Schema) -> TableCreateStatement {
        schema.create_table_from_entity(*self)
    }
}

#[pyfunction]
fn signal_handler(py: Python<'_>, signum: i32, _frame: PyObject, quebec: Py<PyAny>) -> PyResult<()> {
    info!("Received signal {}, initiating graceful shutdown", signum);

    if let Err(e) = quebec.bind(py).call_method0("graceful_shutdown") {
        error!("Error calling graceful_shutdown: {:?}", e);
    }

    Ok(())
}

#[pyclass(name = "ActiveLogger", subclass)]
#[derive(Debug, Clone)]
pub struct ActiveLogger {
    span: tracing::Span,
}

#[pymethods]
impl ActiveLogger {
    #[new]
    pub fn new() -> Self {
        let thread_id = std::thread::current().id();
        let span =
            tracing::span!(tracing::Level::INFO, "execution", tid = format!("{:?}", thread_id));
        ActiveLogger { span }
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

#[pyclass(name = "Quebec", subclass)]
#[derive(Debug, Clone)]
pub struct PyQuebec {
    pub ctx: Arc<AppContext>,
    pub rt: Arc<tokio::runtime::Runtime>,
    pub url: String,
    pub quebec: Arc<Quebec>,
    pub worker: Arc<Worker>,
    pub dispatcher: Arc<Dispatcher>,
    pub scheduler: Arc<Scheduler>,
    pub job_classes: Arc<RwLock<HashMap<String, Py<PyAny>>>>,
    pub shutdown_handlers: Arc<RwLock<Vec<Py<PyAny>>>>,
    pub start_handlers: Arc<RwLock<Vec<Py<PyAny>>>>,
    pub stop_handlers: Arc<RwLock<Vec<Py<PyAny>>>>,
    pyqueue_mode: Arc<AtomicBool>,
    handles: Arc<Mutex<Vec<JoinHandle<()>>>>,
}

// static LIST_CELL: GILOnceCell<Py<PyQuebec>> = GILOnceCell::new();
static INSTANCE_MAP: GILOnceCell<RwLock<HashMap<String, Py<PyQuebec>>>> = GILOnceCell::new();

#[pymethods]
impl PyQuebec {
    #[pyo3(signature = (url, **kwargs))]
    #[new]
    fn new(url: String, kwargs: Option<&Bound<'_, PyDict>>) -> Self {
        info!("PyQuebec<{}>", url);

        let rt = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(16)
                .enable_all()
                .build()
                .unwrap(),
        );

        let dsn = Url::parse(&url).unwrap();
        let mut opt: ConnectOptions = ConnectOptions::new(url.to_string());
        let min_conns = if url.contains("sqlite") { 1 } else { 2 };
        let max_conns = if url.contains("sqlite") { 1 } else { 30 };
        opt.min_connections(min_conns)
            .max_connections(max_conns)
            .acquire_timeout(Duration::from_secs(5))
            .connect_timeout(Duration::from_secs(3))
            .idle_timeout(Duration::from_secs(600))
            .sqlx_logging(true)
            .sqlx_logging_level(tracing::log::LevelFilter::Trace);

        // Pre-create a connection for all database types
        // This connection will be wrapped in Arc and used when needed
        // For high concurrency operations, new connections will be obtained through get_connection method
        let db = rt.block_on(async { Database::connect(opt.clone()).await.unwrap() });
        let db_option = Some(Arc::new(db));

        // Convert kwargs to HashMap<String, PyObject>
        let options = if let Some(kwargs) = kwargs {
            let mut options_map = HashMap::new();
            kwargs.iter().for_each(|(k, v)| {
                let key = k.extract::<String>().unwrap();
                // Use Python::with_gil to get GIL
                Python::with_gil(|py| {
                    options_map.insert(key, v.into_pyobject(py).unwrap().into());
                });
            });
            Some(options_map)
        } else {
            None
        };

        let mut _ctx = AppContext::new(dsn.clone(), db_option, opt.clone(), options);
        // All parameters have been processed in AppContext.new

        let ctx = Arc::new(_ctx);
        let quebec = Arc::new(Quebec::new(ctx.clone()));
        let worker = Arc::new(Worker::new(ctx.clone()));
        let dispatcher = Arc::new(Dispatcher::new(ctx.clone()));
        let scheduler = Arc::new(Scheduler::new(ctx.clone()));


        let job_classes = Arc::new(RwLock::new(HashMap::<String, Py<PyAny>>::new()));
        let shutdown_handlers = Arc::new(RwLock::new(Vec::<Py<PyAny>>::new()));
        let start_handlers = Arc::new(RwLock::new(Vec::<Py<PyAny>>::new()));
        let stop_handlers = Arc::new(RwLock::new(Vec::<Py<PyAny>>::new()));

        PyQuebec {
            ctx,
            rt,
            url,
            quebec,
            worker,
            dispatcher,
            scheduler,
            job_classes,
            shutdown_handlers,
            start_handlers,
            stop_handlers,
            pyqueue_mode: Arc::new(AtomicBool::new(false)),
            handles: Arc::new(Mutex::new(Vec::new())),
        }
    }

    // #[staticmethod]
    // fn get_instance(py: Python<'_>) -> &pyo3::Bound<'_, PyQuebec> {
    //     LIST_CELL
    //         .get_or_init(py, || {
    //             PyQuebec::new("postgres://ratazzi:@localhost:5432/helvetica_test".to_string())
    //                 .to_object(py)
    //                 .into_bound(py)
    //                 .unbind()
    //                 .extract(py)
    //                 .unwrap()
    //         })
    //         .bind(py)
    // }

    #[staticmethod]
    fn get_instance(py: Python<'_>, url: String) -> PyResult<Py<PyQuebec>> {
        let instance_map = INSTANCE_MAP.get_or_init(py, || RwLock::new(HashMap::new()));

        let mut map = instance_map.write().unwrap();
        if let Some(instance) = map.get(&url) {
            return Ok(instance.clone());
        }

        let instance = Py::new(py, PyQuebec::new(url.clone(), None))?;
        map.insert(url, instance.clone().extract(py).unwrap());
        Ok(instance.extract(py).unwrap())
    }

    fn runner(&self) -> PyResult<()> {
        let worker = self.worker.clone();
        let handle = self.rt.spawn(async move {
            let _ = worker.run().await;
        });

        self.handles.lock().unwrap().push(handle);
        Ok(())
    }

    fn pick_job(&self, py: Python<'_>) -> PyResult<Execution> {
        let worker = self.worker.clone();

        py.allow_threads(|| {
            let ret = self.rt.block_on(async move { worker.pick_job().await });
            Ok(ret.unwrap())
        })


    }

    async fn post_job(&self) -> Result<(), anyhow::Error> {
        let _ = self.rt.spawn(async move {
            tokio::time::sleep(Duration::from_secs(5)).await;
        });

        Ok(())
    }

    fn feed_jobs_to_queue(&self, py: Python<'_>, queue: PyObject) -> PyResult<()> {

        let worker = self.worker.clone();
        let queue = queue.clone_ref(py);
        let queue1 = queue.clone_ref(py);
        let graceful_shutdown = self.ctx.graceful_shutdown.clone();

        self.pyqueue_mode.store(true, Ordering::Relaxed);

        // Use tokio::spawn to start async task
        self.rt.spawn(async move {
            loop {
                let result = worker.pick_job().await;

                // Get GIL and send result to Python queue
                Python::with_gil(|py| {
                    let queue = queue.bind(py);
                    // Check queue type and choose appropriate method
                    if queue.hasattr("put_nowait").unwrap() {
                        // Handle asyncio.Queue
                        let put_method = queue.getattr("put_nowait").unwrap();
                        let _ = put_method.call1((result.unwrap(),));
                    } else if queue.hasattr("put").unwrap() {
                        // Handle queue.Queue, multiprocessing.Queue
                        let put_method = queue.getattr("put").unwrap();
                        let _ = put_method.call1((result.unwrap(),));
                    } else {
                        error!("Unsupported queue type");
                    }
                });
            }
        });

        let handle = self.rt.spawn(async move {
            tokio::select! {
                _ = graceful_shutdown.cancelled() => {
                    Python::with_gil(|py| {
                        let queue = queue1.bind(py);
                        // Check queue type and choose appropriate method
                        if queue.hasattr("close").unwrap() {
                            // Handle asyncio.Queue
                            let close_method = queue.getattr("close").unwrap();
                            let _ = close_method.call0();
                            info!("<asyncio.Queue> shutdown");
                        } else if queue.hasattr("join").unwrap() {
                            // Handle queue.Queue
                            let join_method = queue.getattr("join").unwrap();
                            let _ = join_method.call0();
                        } else {
                            error!("Unsupported queue type");
                        }
                    });
                }
            }

            // info!("sink job poller shutdown");
        });
        self.handles.lock().unwrap().push(handle);

        Ok(())
    }

    // fn post_job(&self, job: Py<PyAny>) -> PyResult<()> {
    //     let job = job.extract::<Job>().unwrap();
    //     let dispatcher = self.dispatcher.clone();
    //     let handle = self.rt.spawn(async move {
    //         let _ = dispatcher.post_job(job).await;
    //     });

    //     self.handles.lock().unwrap().push(handle);
    //     Ok(())
    // }

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
            // match db {
            match db.ping().await {
                Ok(_) => Ok(true),
                Err(err) => {
                    error!("Ping failed: {:?}", err);
                    Ok(false)
                }
            }
            // Err(err) => {
            //     eprintln!("Failed to connect to database: {:?}", err);
            //     Ok(false)
            // }
            // }
        })
    }

    fn create_table(&self) -> PyResult<bool> {
        self.rt.block_on(async move {
            let db = self.ctx.get_db().await;
            let schema = Schema::new(db.get_database_backend());

            let tables: Vec<Box<dyn EntityTrait>> = vec![
                Box::new(solid_queue_blocked_executions::Entity),
                Box::new(solid_queue_claimed_executions::Entity),
                Box::new(solid_queue_failed_executions::Entity),
                Box::new(solid_queue_jobs::Entity),
                Box::new(solid_queue_pauses::Entity),
                Box::new(solid_queue_processes::Entity),
                Box::new(solid_queue_ready_executions::Entity),
                Box::new(solid_queue_recurring_executions::Entity),
                Box::new(solid_queue_recurring_tasks::Entity),
                Box::new(solid_queue_scheduled_executions::Entity),
                Box::new(solid_queue_semaphores::Entity),
            ];

            for table in tables {
                let stmt: TableCreateStatement = table.create_table_statement(&schema);
                let ret = db.execute(db.get_database_backend().build(&stmt)).await;
                if ret.is_err() {
                    error!("Failed to create table: {:?}", ret);
                    return Ok(false);
                }
            }

            Ok(true)
        })
    }

    fn on_start(&mut self, py: Python<'_>, handler: Py<PyAny>) -> PyResult<Py<PyAny>> {
        let callable = handler.bind(py);
        if !callable.is_callable() {
            return Err(PyTypeError::new_err("Expected a callable object for start handler"));
        }

        {
            self.start_handlers.write().unwrap().push(handler.clone_ref(py));
            trace!("Start handler registered");
        }

        Ok(handler)
    }

    fn on_stop(&mut self, py: Python<'_>, handler: Py<PyAny>) -> PyResult<Py<PyAny>> {
        let callable = handler.bind(py);
        if !callable.is_callable() {
            return Err(PyTypeError::new_err("Expected a callable object for stop handler"));
        }

        {
            self.stop_handlers.write().unwrap().push(handler.clone_ref(py));
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
        let _ = self.worker.register_job_class(py, job_class).unwrap();
        Ok(())
    }

    #[pyo3(signature = (klass, *args, **kwargs))]
    pub fn perform_later(
        &self, py: Python<'_>, klass: &Bound<'_, PyType>, args: &Bound<'_, PyTuple>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<ActiveJob> {
        let bound = klass;
        let class_name = bound.downcast::<PyType>().unwrap().qualname().unwrap();

        let mut queue_name = "default".to_string();
        if bound.hasattr("queue_as")? {
            queue_name = bound.getattr("queue_as")?.extract::<String>()?;
        }

        let mut priority = 0;
        if bound.hasattr("queue_with_priority")? {
            priority = bound.getattr("queue_with_priority")?.extract::<i32>()?;
        }

        let instance = bound.call0()?;

        // if instance.hasattr("concurrency_key")? {
        //     // let concurrency_key = klass.getattr("concurrency_key")?.extract::<String>()?;
        //     // debug!("concurrency_key: {:?}", concurrency_key);
        //     debug!("-------------- concurrency enabled: {}", class_name);
        //     let func = instance.getattr("concurrency_key")?;
        //     concurrency_key = Some(func.call(args, kwargs)?.extract::<String>().unwrap());
        //     debug!("concurrency_key: {:?}", concurrency_key);
        // }

        let mut concurrency_key: Option<String> = None;
        if instance.hasattr("limits_concurrency")? {
            let strategy =
                instance.getattr("limits_concurrency")?.extract::<ConcurrencyStrategy>()?;
            debug!("limits_concurrency enabled: {}, {:?}", class_name, strategy);

            let func = strategy.key.into_bound(py);
            concurrency_key = Some(func.call(args, kwargs)?.extract::<String>().map_err(|e| {
                PyErr::new::<CustomError, _>(format!("Failed to extract concurrency_key: {}", e))
            })?);
            debug!("concurrency_key: {:?}", concurrency_key);
        }

        // let args_json = py_to_json_value(py, &args);
        // let kwargs_json = kwargs.map(|kwargs| py_to_json_value(py, &kwargs));

        // let arguments = vec![args_json, kwargs_json.unwrap_or(Value::Null)];
        // debug!("arguments: {:?}", arguments);
        // let arguments = serde_json::to_string(&arguments).unwrap();
        // // debug!("arguments: {:?}", arguments);

        let mut args_json = py_to_json_value(py, &args);
        if let Some(kwargs) = kwargs {
            let kwargs_json = py_to_json_value(py, &kwargs);
            if let Value::Array(ref mut args_array) = args_json {
                if let Value::Object(kwargs_map) = kwargs_json {
                    for (key, value) in kwargs_map {
                        args_array
                            .push(Value::Object(serde_json::Map::from_iter(vec![(key, value)])));
                    }
                }
            }
        }
        // debug!("args_json: {:?}", args_json);
        let arguments = serde_json::to_string(&args_json).unwrap();

        let mut obj = instance.clone().extract::<ActiveJob>()?;
        obj.class_name = class_name.to_string();
        obj.arguments = arguments;
        obj.active_job_id = uuid::Uuid::new_v4().to_string().replace("-", "");
        obj.queue_name = queue_name;
        obj.priority = priority;
        obj.concurrency_key = concurrency_key;

        let start_time = Instant::now();
        self.rt
            .block_on(async {
                let job = self.quebec.perform_later(klass, obj.clone()).await;
                job
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

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("Quebec(url={})", self.url))
    }

    // implment a decorator to register job class
    fn register_job(&self, py: Python, job_class: Py<PyAny>) -> PyResult<Py<PyAny>> {
        // Check if the passed object is a class
        if !job_class.bind(py).is_instance_of::<PyType>() {
            return Err(PyTypeError::new_err("Expected a class, but got an instance"));
        }

        let binding = job_class.clone();
        let bound = binding.bind(py);

        let instance = bound.call0()?;
        if !instance.is_instance_of::<ActiveJob>() {
            return Err(pyo3::exceptions::PyTypeError::new_err(
                "Job class must be a subclass of ActiveJob",
            ));
        }

        // let _ = job_class.setattr(py, pyo3::intern!(py, "quebec"), self.clone());

        let _ = job_class.setattr(py, "quebec", self.clone().into_pyobject(py)?);
        let dup = job_class.clone();
        let _ = self.worker.register_job_class(py, job_class);
        Ok(dup)
    }

    fn print_classes(&self) {
        // debug!("runnables: {:?}", self.worker.runnables);
    }

    fn setup_signal_handler(&self, py: Python) -> PyResult<Py<PyAny>> {
        info!("Setting up signal handlers");
        let signal = py.import("signal")?;

        let handler_func = wrap_pyfunction!(signal_handler)(py)?;
        let functools = py.import("functools")?;
        let quebec_obj: Py<PyAny> = self.clone().into_pyobject(py).unwrap().into();

        let kwargs = PyDict::new(py);
        kwargs.set_item("quebec", quebec_obj)?;

        let wrapped_handler = functools.getattr("partial")?.call(
            (handler_func,),
            Some(&kwargs)
        )?;

        signal.call_method1("signal", (signal.getattr("SIGINT")?, wrapped_handler.clone()))?;
        signal.call_method1("signal", (signal.getattr("SIGTERM")?, wrapped_handler.clone()))?;
        signal.call_method1("signal", (signal.getattr("SIGQUIT")?, wrapped_handler.clone()))?;

        info!("Signal handlers registered for SIGINT and SIGTERM and SIGQUIT");
        Ok(wrapped_handler.into_pyobject(py).unwrap().into())
    }

    fn spawn_all(&mut self) -> PyResult<()> {
        // Call start handlers before spawning components
        if let Err(e) = self.invoke_start_handlers() {
            error!("Error calling start handlers: {:?}", e);
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
        let handlers = self.shutdown_handlers.read().unwrap();
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
        let rt = Arc::new(tokio::runtime::Runtime::new().unwrap());

        // Temporarily release GIL
        py.allow_threads(|| {
            // block call would continuously occupy GIL preventing other threads from executing
            rt.block_on(async move {
                let result = tokio::time::timeout(timeout, async {
                    let handles = {
                        let mut handles = handles.lock().unwrap();
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
        Python::with_gil(|py| {
            let handlers = self.start_handlers.read().unwrap();
            for handler in handlers.iter() {
                if let Err(e) = handler.bind(py).call0() {
                    error!("Error calling start handler: {:?}", e);
                }
            }
            Ok(())
        })
    }

    pub fn invoke_stop_handlers(&self) -> PyResult<()> {
        Python::with_gil(|py| {
            let handlers = self.stop_handlers.read().unwrap();
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
            return Err(PyTypeError::new_err("Expected a callable object for shutdown handler"));
        }

        {
            self.shutdown_handlers.write().unwrap().push(handler.clone_ref(py));
            debug!("Shutdown handler: {:?} registered", handler);
        }

        Ok(handler)
    }

    fn start_control_plane(&self, addr: String) -> PyResult<()> {
        let ctx = self.ctx.clone();
        let rt = self.rt.clone();
        let handle = rt.spawn(async move {
            let server_handle = ControlPlaneExt::start_control_plane(&ctx, addr);
            match server_handle.await {
                Ok(_) => debug!("Control plane server task completed"),
                Err(e) => error!("Control plane server task failed: {}", e),
            }
        });
        self.handles.lock().unwrap().push(handle);
        Ok(())
    }
}

// impl PyQuebec {
    //     // This method is private and won't be exposed to Python
//     fn internal_method(&self) {
//         println!("This is an internal method");
//     }
// }

fn py_to_json_value(_py: Python, obj: &Bound<'_, PyAny>) -> Value {
    if obj.is_instance_of::<PyInt>() {
        Value::Number(obj.extract::<i64>().unwrap().into())
    } else if obj.is_instance_of::<PyFloat>() {
        Value::Number(serde_json::Number::from_f64(obj.extract::<f64>().unwrap()).unwrap())
    } else if obj.is_instance_of::<PyString>() {
        Value::String(obj.extract::<String>().unwrap())
    } else if obj.is_instance_of::<PyDict>() {
        let dict = obj.downcast::<PyDict>().unwrap();
        let mut map = serde_json::Map::new();
        for (key, value) in dict {
            let key: String = key.extract().unwrap();
            let value = py_to_json_value(_py, &value);
            map.insert(key, value);
        }
        Value::Object(map)
    } else if obj.is_instance_of::<PyList>() {
        let list = obj.downcast::<PyList>().unwrap();
        let vec: Vec<Value> = list.iter().map(|item| py_to_json_value(_py, &item)).collect();
        Value::Array(vec)
    } else if obj.is_instance_of::<PyTuple>() {
        let tuple = obj.downcast::<PyTuple>().unwrap();
        let vec: Vec<Value> = tuple.iter().map(|item| py_to_json_value(_py, &item)).collect();
        Value::Array(vec)
    } else if obj.is_none() {
        Value::Null
    } else {
        panic!("Unsupported Python type")
    }
}

#[pyclass(name = "ActiveJob", subclass)]
#[derive(Debug, Clone)]
pub struct ActiveJob {
    pub logger: ActiveLogger,
    pub id: Option<i64>,
    pub queue_name: String,
    pub class_name: String,
    pub arguments: String,
    pub priority: i32,
    pub failed_attempts: i32,
    pub active_job_id: String,
    pub scheduled_at: chrono::NaiveDateTime,
    pub finished_at: Option<chrono::NaiveDateTime>,
    pub concurrency_key: Option<String>,
    pub created_at: Option<chrono::NaiveDateTime>,
    pub updated_at: Option<chrono::NaiveDateTime>,
    // pub rescue_strategies: Vec<RescueStrategy>,
}

#[pymethods]
impl ActiveJob {
    // #[new]
    // pub fn new(
    //     queue_name: String, class_name: String, arguments: String, priority: i32,
    //     active_job_id: String, scheduled_at: chrono::NaiveDateTime, id: Option<i64>,
    //     concurrency_key: Option<String>,
    // ) -> Self {
    //     let logger = ActiveLogger::new();
    //     Self {
    //         logger,
    //         queue_name,
    //         class_name,
    //         arguments,
    //         priority,
    //         active_job_id,
    //         scheduled_at,
    //         id,
    //         finished_at: None,
    //         concurrency_key: concurrency_key,
    //         created_at: None,
    //         updated_at: None,
    //     }
    // }

    // #[classattr]
    // rescue_strategies: Vec<RescueStrategy> = vec![];
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
            failed_attempts: 0,
            active_job_id: "".to_string(),
            scheduled_at: chrono::Utc::now().naive_utc(),
            finished_at: None,
            concurrency_key: None,
            created_at: None,
            updated_at: None,
            // rescue_strategies: vec![],
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
    pub fn get_failed_attempts(&self) -> i32 {
        self.failed_attempts
    }

    #[setter]
    fn set_failed_attempts(&mut self, failed_attempts: i32) {
        self.failed_attempts = failed_attempts;
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
    pub fn get_created_at(&self) -> Option<NaiveDateTime> {
        self.created_at
    }

    #[getter]
    pub fn get_updated_at(&self) -> Option<NaiveDateTime> {
        self.updated_at
    }

    // #[getter]
    // pub fn get_rescue_strategies(&self) -> Vec<RescueStrategy> {
    //     self.rescue_strategies.clone()
    // }

    #[classmethod]
    pub fn register_rescue_strategy(cls: &Bound<'_, PyType>, py: Python<'_>, strategy: Py<PyAny>) {
        let strategy = strategy.extract::<RescueStrategy>(py).unwrap();
        let rescue_strategies = cls.getattr("rescue_strategies").unwrap();
        rescue_strategies.call_method1("append", (strategy.clone(),)).unwrap();
        // debug!("register_rescue_strategy: {:?}", rescue_strategies);
        // debug!("register_rescue_strategy: {:?}", strategy);
    }

    pub fn __repr__(&self) -> PyResult<String> {
        Ok(format!(
            "ActiveJob(id={:?}, queue_name={}, class_name={}, arguments={}, priority={}, failed_attempts={}, active_job_id={}, scheduled_at={})",
            self.id, self.queue_name, self.class_name, self.arguments, self.priority, self.failed_attempts, self.active_job_id, self.scheduled_at
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

    // #[pyo3(signature = (*args, **kwargs))]
    // fn perform(&self, args: &Bound<'_, PyTuple>, kwargs: Option<&Bound<'_, PyDict>>) {
    //     debug!("perform");
    // }

    #[getter]
    fn get_logger(&self) -> PyResult<ActiveLogger> {
        Ok(self.logger.clone())
    }

    #[classmethod]
    #[pyo3(signature = ( quebec, *args, **kwargs))]
    fn perform_later(
        cls: &Bound<'_, PyType>, py: Python<'_>, quebec: &PyQuebec, args: &Bound<'_, PyTuple>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<ActiveJob> {
        quebec.perform_later(py, cls, args, kwargs)
    }

    #[classmethod]
    #[pyo3(signature = (*args, **kwargs))]
    fn perform_later1(
        cls: &Bound<'_, PyType>, py: Python<'_>, args: &Bound<'_, PyTuple>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<ActiveJob> {
        let quebec = cls.getattr("quebec").unwrap().extract::<PyQuebec>().unwrap();
        // debug!("------------ {:?}", quebec);
        quebec.perform_later(py, cls, args, kwargs)
    }

    // #[classmethod]
    // #[pyo3(signature = (*args))]
    // fn rescue_from(cls: &PyType, py: Python<'_>, args: &Bound<'_, PyTuple>) -> PyResult<Py<PyAny>> {
    //     debug!("----------------------- rescue_from: {:?}", args);
    //     // get last item
    //     let last = args.get_item(args.len() - 1);
    //     debug!("----------------------- last: {:?}", last);
    //     Ok(last?.unbind())
    // }
}
