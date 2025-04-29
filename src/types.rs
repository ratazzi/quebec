use chrono;
use chrono::NaiveDateTime;
use pyo3::exceptions::PyException;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::sync::GILOnceCell;
use pyo3::types::{PyDict, PyTuple, PyType};
use pyo3::types::{PyFloat, PyInt, PyList, PyString};
use pyo3::PyTypeInfo;
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
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid;

use crate::context::*;
use crate::core::Quebec;
use crate::dispatcher::Dispatcher;
use crate::entities::{prelude::*, *};
use crate::scheduler::Scheduler;
use crate::worker::{Execution, Metric, Worker};

#[cfg(feature = "use-log")]
use log::{debug, error, info, trace, warn};

#[cfg(feature = "use-tracing")]
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
                .worker_threads(4) // 配置线程数量
                .enable_all()
                .build()
                .unwrap(),
        );

        let dsn = Url::parse(&url).unwrap();
        let mut opt: ConnectOptions = ConnectOptions::new(url.to_string());
        let min_conns = if url.contains("sqlite") { 1 } else { 2 };
        let max_conns = if url.contains("sqlite") { 1 } else { 30 };
        opt.min_connections(min_conns) // 设置最小空闲连接数
            .max_connections(max_conns) // 设置最大连接数
            // .acquire_timeout(Duration::from_millis(100))
            .connect_timeout(Duration::from_secs(3)) // 设置连接超时时间
            .idle_timeout(Duration::from_secs(600)) // 设置空闲连接超时时间
            .sqlx_logging(true)
            .sqlx_logging_level(log::LevelFilter::Trace);

        let db = rt.block_on(async { Database::connect(opt.clone()).await.unwrap() });

        let mut _ctx = AppContext::new(dsn.clone(), Arc::new(db), opt.clone());
        if let Some(kwargs) = kwargs {
            debug!("========================= kwargs: {:?}", kwargs);
            kwargs.iter().for_each(|(k, v)| {
                debug!("========================= k: {:?} => {:?}", k, v);

                let binding = k.extract::<String>().unwrap();
                let field_name = binding.as_str();
                match field_name {
                    "use_skip_locked" => _ctx.use_skip_locked = v.extract::<bool>().unwrap_or(true),
                    &_ => todo!(),
                }
            });
        }

        let ctx = Arc::new(_ctx);
        let quebec = Arc::new(Quebec::new(ctx.clone()));
        let worker = Arc::new(Worker::new(ctx.clone()));
        let dispatcher = Arc::new(Dispatcher::new(ctx.clone()));
        let scheduler = Arc::new(Scheduler::new(ctx.clone()));
        // debug!("Quebec: {:?}", quebec);

        let job_classes = Arc::new(RwLock::new(HashMap::<String, Py<PyAny>>::new()));

        PyQuebec {
            ctx,
            rt,
            url,
            quebec,
            worker,
            dispatcher,
            scheduler,
            job_classes,
            pyqueue_mode: Arc::new(AtomicBool::new(false)),
            handles: Arc::new(Mutex::new(Vec::new())),
        }
    }

    // #[staticmethod]
    // fn get_instance(py: Python<'_>) -> &pyo3::Bound<'_, PyQuebec> {
    //     LIST_CELL
    //         .get_or_init(py, || {
    //             PyQuebec::new("postgres://ratazzi:@localhost:5432/helvetica_test".to_string())
    //                 .into_py(py)
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

        let instance = PyQuebec::new(url.clone(), None).into_py(py);
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

        // 创建一个 oneshot 通道
        // let (tx, rx) = tokio::sync::oneshot::channel();

        // // 使用 tokio::spawn 启动异步任务
        // self.rt.spawn(async move {
        //     let result = worker.pick_job().await;
        //     let _ = tx.send(result);
        // });

        // // 释放 GIL 并等待异步任务完成
        // py.allow_threads(|| {
        //     let job_result = self.rt.block_on(rx).map_err(|e| {
        //         PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Task failed: {:?}", e))
        //     })??;

        //     Ok(job_result)
        // })

        // 将异步任务转换为 Python Future
        // future_into_py(py, async move {
        //     let result = worker.pick_job().await;
        //     result.map_err(|e| {
        //         PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Task failed: {:?}", e))
        //     })
        // })
    }

    async fn post_job(&self) -> Result<(), anyhow::Error> {
        // let job = job.extract::<Job>().unwrap();
        // let dispatcher = self.dispatcher.clone();
        let _ = self.rt.spawn(async move {
            // let _ = dispatcher.dispatch(job).await;
            tokio::time::sleep(Duration::from_secs(5)).await;
            debug!("----------------------------- async fn post_job");
        });

        // self.handles.lock().unwrap().push(handle);
        debug!("----------------------------- async fn post_job");
        Ok(())
    }

    fn poll_job(&self, py: Python<'_>, queue: PyObject) -> PyResult<()> {
        if self.pyqueue_mode.load(Ordering::Relaxed) {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Already in pyqueue mode",
            ));
        }

        let worker = self.worker.clone();
        let queue = queue.clone_ref(py);
        let queue1 = queue.clone_ref(py);
        let graceful_shutdown = self.ctx.graceful_shutdown.clone();

        self.pyqueue_mode.store(true, Ordering::Relaxed);

        // 使用 tokio::spawn 启动异步任务
        self.rt.spawn(async move {
            loop {
                let result = worker.pick_job().await;

                // 获取 GIL 并将结果发送到 Python 的队列中
                Python::with_gil(|py| {
                    let queue = queue.bind(py);
                    // 检查队列类型并选择合适的方法
                    if queue.hasattr("put_nowait").unwrap() {
                        // 处理 asyncio.Queue
                        let put_method = queue.getattr("put_nowait").unwrap();
                        let _ = put_method.call1((result.unwrap(),));
                    } else if queue.hasattr("put").unwrap() {
                        // 处理 queue.Queue
                        let put_method = queue.getattr("put").unwrap();
                        let _ = put_method.call1((result.unwrap(),));
                    } else {
                        error!("Unsupported queue type");
                    }
                });
            }
        });

        let handle = self.rt.spawn(async move {
            loop {
                tokio::select! {
                  _ = graceful_shutdown.cancelled() => {
                      Python::with_gil(|py| {
                          let queue = queue1.bind(py);
                          // 检查队列类型并选择合适的方法
                          if queue.hasattr("close").unwrap() {
                              // 处理 asyncio.Queue
                              let close_method = queue.getattr("close").unwrap();
                              let _ = close_method.call0();
                              info!("<asyncio.Queue> are shutdown");
                          } else if queue.hasattr("join").unwrap() {
                              // 处理 queue.Queue
                              let join_method = queue.getattr("join").unwrap();
                              let _ = join_method.call0();
                              info!("<queue.Queue> are shutdown");
                          } else {
                              error!("Unsupported queue type");
                          }
                      });
                      break;
                  }
                }
            }

            info!("sink job poller shutdown");
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

    fn non_blocking_run_worker_polling(&self) -> PyResult<()> {
        let worker = self.worker.clone();
        let _ = self.rt.spawn(async move {
            let _ = worker.run_main_loop().await;
        });

        Ok(())
    }

    fn non_blocking_run_dispatcher(&self) -> PyResult<()> {
        let dispatcher = self.dispatcher.clone();
        let _ = self.rt.spawn(async move {
            let _ = dispatcher.run().await;
        });

        Ok(())
    }

    fn non_blocking_run_scheduler(&self) -> PyResult<()> {
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
                    eprintln!("Ping failed: {:?}", err);
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

    // fn on_start(&self, py: Python<'_>, klass: Py<PyAny>) -> PyResult<()> {
    //     Ok(())
    // }

    // fn on_stop(&self, py: Python<'_>, klass: Py<PyAny>) -> PyResult<()> {
    //     Ok(())
    // }

    // fn on_worker_start(&self, py: Python<'_>, klass: Py<PyAny>) -> PyResult<()> {
    //     Ok(())
    // }

    // fn on_worker_stop(&self, py: Python<'_>, klass: Py<PyAny>) -> PyResult<()> {
    //     Ok(())
    // }

    fn register_job_class(&self, py: Python, job_class: Py<PyAny>) -> PyResult<()> {
        let bound = job_class.bind(py);
        // let job_instance = bound.call1((23,))?;
        // // let job_class = job_class.getattr("new")?;
        // // let job_instance = job_class.call1((self.secret_number,))?;
        // let job_instance = job_instance.cast_as::<ActiveJob>(py)?;

        // // call perform
        // job_instance.call_method0("perform")?;

        // let binding = bound.to_string();
        // let mut class_name = binding.split(".").last().unwrap().to_string();
        // class_name.truncate(class_name.len() - 2);
        // debug!("Registered job class: {:?}", class_name);

        // self.job_classes.write().unwrap().insert(class_name, job_class);
        // debug!("job classes: {:?}", self.job_classes);

        // debug!("----------------------- {:?}", bound.downcast::<PyType>().unwrap().qualname());
        // if let Ok(ty) = any.cast_as::<PyType>() {
        //     // If successful, get the type name
        //     debug!(ty.name());
        // }

        // debug!("=================queue_as: {:?}", bound.hasattr("queue_as"));
        // job_class.setattr(py, "quebec", *self);
        // Python::with_gil(|_py| {
        let _ = self.worker.register_job_class(py, job_class).unwrap();
        // });
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
            debug!("-------------- limits_concurrency enabled: {}, {:?}", class_name, strategy);

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

        let _ = job_class.setattr(py, "quebec", self.clone().into_py(py));
        let dup = job_class.clone();
        let ret = self.worker.register_job_class(py, job_class);
        debug!("register_job_class: {:?}", ret);
        Ok(dup)
    }

    fn print_classes(&self) {
        // debug!("runnables: {:?}", self.worker.runnables);
    }

    fn graceful_shutdown(&self, py: Python) {
        info!("Graceful shutdown initiated");
        self.ctx.graceful_shutdown.cancel();
        let timeout = self.ctx.shutdown_timeout.clone();
        let quit = self.ctx.force_quit.clone();

        // let _ = self.rt.spawn(async move {
        //     let result = tokio::time::timeout(timeout, async {
        //         tokio::time::sleep(timeout).await;
        //     })
        //     .await;

        //     match result {
        //         Ok(_) => info!("All tasks have exited gracefully"),
        //         Err(_) => {
        //             quit.cancel();
        //             warn!("Force quit");
        //         }
        //     }
        // });

        let handles = self.handles.clone();
        let rt = Arc::new(tokio::runtime::Runtime::new().unwrap());

        // 临时释放 GIL
        py.allow_threads(|| {
            // block 调用会持续占用 GIL 导致其他线程无法执行
            rt.block_on(async move {
                let result = tokio::time::timeout(timeout, async {
                    let mut handles = handles.lock().unwrap();
                    debug!("Waiting for {} tasks to exit gracefully", handles.len());
                    for handle in handles.drain(..) {
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
    }
}

// impl PyQuebec {
//     // 这个方法是私有的，不会暴露给 Python
//     fn internal_method(&self) {
//         println!("This is an internal method");
//     }
// }

fn py_to_json_value(py: Python, obj: &Bound<'_, PyAny>) -> Value {
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
            let value = py_to_json_value(py, &value);
            map.insert(key, value);
        }
        Value::Object(map)
    } else if obj.is_instance_of::<PyList>() {
        let list = obj.downcast::<PyList>().unwrap();
        let vec: Vec<Value> = list.iter().map(|item| py_to_json_value(py, &item)).collect();
        Value::Array(vec)
    } else if obj.is_instance_of::<PyTuple>() {
        let tuple = obj.downcast::<PyTuple>().unwrap();
        let vec: Vec<Value> = tuple.iter().map(|item| py_to_json_value(py, &item)).collect();
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

    #[pyo3(signature = (*args, **kwargs))]
    fn perform(&self, args: &Bound<'_, PyTuple>, kwargs: Option<&Bound<'_, PyDict>>) {
        debug!("perform");
    }

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
