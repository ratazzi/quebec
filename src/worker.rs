use crate::context::*;
use crate::entities::*;
use crate::process::ProcessTrait;
use anyhow::Result;
use async_trait::async_trait;

use sea_query::LockBehavior;
use tracing::{debug, error, info, trace, warn};

use pyo3::exceptions::PyException;
use pyo3::prelude::*;

use sea_orm::TransactionTrait;
use sea_orm::*;
use std::sync::{Arc, RwLock};
use std::time::Instant;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::Mutex;

use colored::*;
use tracing::{info_span, Instrument};

use pyo3::types::{PyBool, PyDict, PyList, PyTuple, PyType};
use pyo3::exceptions::PyTypeError;

use crate::notify::NotifyManager;



fn json_to_py(py: Python<'_>, value: &serde_json::Value) -> PyResult<PyObject> {
    match value {
        serde_json::Value::String(s) => Ok(s.into_pyobject(py)?.into()),
        serde_json::Value::Number(n) => {
            if let Some(val) = n.as_i64() {
                Ok(val.into_pyobject(py)?.into())
            } else if let Some(val) = n.as_u64() {
                Ok(val.into_pyobject(py)?.into())
            } else if let Some(val) = n.as_f64() {
                Ok(val.into_pyobject(py)?.into())
            } else {
                Ok(py.None())
            }
        }
        serde_json::Value::Bool(b) => Ok(PyBool::new(py, *b).as_ref().into_pyobject(py)?.into()),
        serde_json::Value::Array(arr) => {
            let py_list = PyList::empty(py);
            for item in arr {
                py_list.append(json_to_py(py, item)?)?;
            }
            Ok(py_list.into_pyobject(py)?.into())
        }
        serde_json::Value::Object(obj) => {
            let py_dict = PyDict::new(py);
            for (key, value) in obj {
                py_dict.set_item(key, json_to_py(py, value)?)?;
            }
            Ok(py_dict.into_pyobject(py)?.into())
        }
        serde_json::Value::Null => Ok(py.None()),
    }
}

async fn runner(
    ctx: Arc<AppContext>, _thread_id: String, _state: Arc<Mutex<i32>>,
    rx: Arc<Mutex<Receiver<Execution>>>,
) -> Result<()> {
    let mut tid = format!("{:?}", std::thread::current().id());
    let graceful_shutdown = ctx.graceful_shutdown.clone();
    let force_quit = ctx.force_quit.clone();


    if true {
        let mut thread_id: u64 = 0;
        Python::with_gil(|py| {
            if let Ok(threading) = PyModule::import(py, "threading") {
                if let Ok(bound) = threading.getattr("get_ident") {
                    if let Ok(ident) = bound.call0() {
                        thread_id = ident.extract::<u64>().unwrap_or(0);
                    }
                }
            }
        });

        tid = format!("{}", thread_id);
        trace!("python thread_id: {:?}", tid)
    }

    loop {
        let mut receiver = rx.lock().await;
        tokio::select! {
          _ = graceful_shutdown.cancelled() => {
              info!("Graceful shutdown");
              break;
          }
          execution = receiver.recv() => {
              drop(receiver);
              if execution.is_none() {
                  continue;
              }
              let mut execution = match execution {
                  Some(exec) => exec,
                  None => continue,
              };
              execution.tid = tid.clone();
              let _job_id = execution.claimed.job_id;

              // async {
                  // Try to acquire lock, process task if successful
                  // let _lock = state.lock().await;
                  let class_name = execution.job.class_name.clone();

                  info!("Job `{}' started", class_name);

                  let timeout_duration = std::time::Duration::from_secs(10);

                  let result = tokio::select! {
                      _ = graceful_shutdown.cancelled() => {
                          info!("Graceful cancelling");

                          Err(anyhow::anyhow!("Job `{}' cancelling", class_name))
                      }
                      _ = force_quit.cancelled() => {
                          error!("Job `{}' cancelled", class_name);
                          break;
                      }
                      _ = tokio::time::sleep(timeout_duration) => {
                          error!("Job `{}' timeout", class_name);
                          Err(anyhow::anyhow!("Job `{}' timeout", class_name))
                      }
                      result = tokio::task::spawn_blocking(move || {
                          tokio::runtime::Handle::current().block_on(execution.invoke())
                      }) => {
                          match result {
                              Ok(result) => result,
                              Err(e) => Err(anyhow::anyhow!("Job `{}' failed: {:?}", class_name, e)),
                          }
                      }
                  };

                  match result {
                      Ok(_) => info!("Job `{}' completed successfully", class_name),
                      Err(e) => error!("Job `{}' failed: {:?}", class_name, e),
                  }

                  if graceful_shutdown.is_cancelled() {
                      trace!("Current job executed, shutdown gracefully");
                      break;
                  }
              // }
              // .instrument(tracing::info_span!("rust_runner", jid = job_id))
              // .await;
          }
        }
    }

    trace!("Worker thread stopped");
    Ok(())
}

#[pyclass(name = "Runnable", subclass)]
#[derive(Debug)]
pub struct Runnable {
    pub class_name: String,
    pub handler: Py<PyAny>,
    pub queue_as: String,
    pub priority: i64,
    pub retry_info: Option<RetryInfo>,
    pub concurrency_limit: Option<i32>,
    pub concurrency_duration: Option<i32>, // in seconds
}

#[derive(Debug, Clone)]
struct RetryInfo {
    scheduled_at: chrono::NaiveDateTime,
    failed_attempts: i32,
}

impl Runnable {
    pub fn new(class_name: String, handler: Py<PyAny>, queue_as: String, priority: i64) -> Self {
        Self {
            class_name,
            handler,
            queue_as,
            priority,
            retry_info: None,
            concurrency_limit: None,
            concurrency_duration: None,

        }
    }

    /// Safe clone method that requires GIL
    pub fn clone_with_gil(&self, py: Python<'_>) -> Self {
        Self {
            class_name: self.class_name.clone(),
            handler: self.handler.clone_ref(py),
            queue_as: self.queue_as.clone(),
            priority: self.priority,
            retry_info: self.retry_info.clone(),
            concurrency_limit: self.concurrency_limit,
            concurrency_duration: self.concurrency_duration,

        }
    }

    /// Get the concurrency constraint for this job with given arguments
    /// Returns Some(ConcurrencyConstraint) if concurrency control is enabled, None otherwise
    /// Handles Python GIL internally and performs all checks once
    pub fn get_concurrency_constraint<T, K>(&self, args: Option<T>, kwargs: Option<K>) -> Result<Option<ConcurrencyConstraint>>
    where
        T: crate::utils::IntoPython,
        K: crate::utils::IntoPython,
    {
        Python::with_gil(|py| {
            let bound = self.handler.bind(py);
            let instance = bound.call0()?;

            // Check if the instance has a concurrency_key method (not just the property)
            if !instance.hasattr("concurrency_key")? {
                return Ok(None);
            }

            // Get the concurrency_key attribute and check if it's callable
            let concurrency_key_attr = instance.getattr("concurrency_key")?;
            if concurrency_key_attr.is_none() || !concurrency_key_attr.is_callable() {
                return Ok(None);
            }

            // Convert args to Python tuple
            let py_args: Py<pyo3::types::PyTuple> = if let Some(args_value) = args {
                let args_py = args_value.into_python(py)?;
                let args_bound = args_py.bind(py);

                if args_bound.is_instance_of::<pyo3::types::PyTuple>() {
                    args_bound.downcast::<pyo3::types::PyTuple>()?.clone().into()
                } else if args_bound.is_instance_of::<pyo3::types::PyList>() {
                    let list = args_bound.downcast::<pyo3::types::PyList>()?;
                    pyo3::types::PyTuple::new(py, list)?.into()
                } else {
                    // Single value - wrap in tuple
                    pyo3::types::PyTuple::new(py, &[args_bound])?.into()
                }
            } else {
                pyo3::types::PyTuple::empty(py).into()
            };

            // Convert kwargs to Python dict
            let py_kwargs: Py<pyo3::types::PyDict> = if let Some(kwargs_value) = kwargs {
                let kwargs_py = kwargs_value.into_python(py)?;
                let kwargs_bound = kwargs_py.bind(py);

                if kwargs_bound.is_instance_of::<pyo3::types::PyDict>() {
                    kwargs_bound.downcast::<pyo3::types::PyDict>()?.clone().into()
                } else {
                    // If not a dict, create empty dict
                    pyo3::types::PyDict::new(py).into()
                }
            } else {
                pyo3::types::PyDict::new(py).into()
            };

            // Call the concurrency_key method with the job arguments
            let result = concurrency_key_attr.call(py_args.bind(py), Some(&py_kwargs.bind(py)))?;

            // Check if the result is None or empty string - if so, no concurrency control
            if result.is_none() {
                return Ok(None);
            }

            let raw_key = result.extract::<String>()?;
            if raw_key.is_empty() {
                return Ok(None);
            }

            // Get concurrency_group (defaults to class name)
            let concurrency_group = if instance.hasattr("concurrency_group")? {
                instance.getattr("concurrency_group")?.extract::<String>()?
            } else {
                self.class_name.clone()
            };

            // Build final concurrency_key as "group/key"
            let key = format!("{}/{}", concurrency_group, raw_key);

            // Return explicit limit or default to 1 (per Solid Queue spec)
            let limit = self.concurrency_limit.unwrap_or(1);

            // Return duration (convert from seconds to chrono::Duration)
            let duration = self.concurrency_duration.map(|seconds| chrono::Duration::seconds(seconds as i64));

            Ok(Some(ConcurrencyConstraint { key, limit, duration }))
        }).map_err(|e: PyErr| anyhow::anyhow!("Python error in get_concurrency_constraint: {}", e))
    }

    /// Check if should retry, return matching retry strategy
    fn should_retry(
        &self, py: Python, bound: &Bound<PyAny>, error: &PyErr, failed_attempts: i32,
    ) -> PyResult<Option<RetryStrategy>> {
        if !bound.hasattr("retry_on")? {
            return Ok(None);
        }

        let retry_strategies = bound.getattr("retry_on")?.extract::<Vec<RetryStrategy>>()?;

        for strategy in retry_strategies {
            if i64::from(failed_attempts) >= strategy.attempts {
                continue; // Exceeded maximum retry count
            }

            if self.is_exception_match(py, &strategy.exceptions, error)? {
                return Ok(Some(strategy));
            }
        }

        Ok(None)
    }

    /// Check if should discard, return matching discard strategy
    fn should_discard(&self, py: Python, bound: &Bound<PyAny>, error: &PyErr) -> PyResult<Option<DiscardStrategy>> {
        if !bound.hasattr("discard_on")? {
            return Ok(None);
        }

        let discard_strategies = bound.getattr("discard_on")?.extract::<Vec<DiscardStrategy>>()?;

        for strategy in discard_strategies {
            if self.is_exception_match(py, &strategy.exceptions, error)? {
                return Ok(Some(strategy));
            }
        }

        Ok(None)
    }

    /// Check if has rescue handling, return matching rescue strategy
    fn should_rescue(&self, py: Python, bound: &Bound<PyAny>, error: &PyErr) -> PyResult<Option<RescueStrategy>> {
        if !bound.hasattr("rescue_from")? {
            return Ok(None);
        }

        let rescue_strategies = bound.getattr("rescue_from")?.extract::<Vec<RescueStrategy>>()?;

        for strategy in rescue_strategies {
            if self.is_exception_match(py, &strategy.exceptions, error)? {
                return Ok(Some(strategy));
            }
        }

        Ok(None)
    }

    /// Check if exception matches
    fn is_exception_match(&self, py: Python, exceptions: &Py<PyAny>, error: &PyErr) -> PyResult<bool> {
        let exceptions_bound = exceptions.bind(py);


        if let Ok(exception_type) = exceptions_bound.downcast::<PyType>() {
            return Ok(error.is_instance(py, exception_type));
        }


        if let Ok(exception_tuple) = exceptions_bound.downcast::<PyTuple>() {
            for item in exception_tuple.iter() {
                if let Ok(exception_type) = item.downcast::<PyType>() {
                    if error.is_instance(py, exception_type) {
                        return Ok(true);
                    }
                }
            }
        }

        Ok(false)
    }



    /// Handle discard - directly mark task as completed without recording failure information
    fn handle_discard(
        &self, py: Python, strategy: &DiscardStrategy, error: &PyErr, job: solid_queue_jobs::Model,
    ) -> Result<solid_queue_jobs::Model> {
        // Call discard handler (if any)
        if let Some(handler) = &strategy.handler {
            if let Err(handler_error) = handler.call1(py, (error.value(py),)) {
                warn!("Error in discard handler: {}", handler_error);
            }
        }

        let error_name = error.get_type(py).name()
            .map(|s| s.to_string())
            .unwrap_or_else(|_| "unknown error".to_string());
        info!("Job discarded due to {}", error_name);

        // Return success directly, indicating task was discarded and marked as completed
        Ok(job)
    }

    /// Create error payload
    fn create_error_payload(&self, py: Python, error: &PyErr) -> String {
        let mut backtrace: Vec<String> = vec![];
        if let Some(tb) = error.traceback(py) {
            if let Ok(formatted) = tb.format() {
                backtrace = formatted.to_string().lines().map(String::from).collect();
            }
        }

        let exception_class = error.get_type(py).name()
            .map(|s| s.to_string())
            .unwrap_or_else(|_| "UnknownError".to_string());

        let error_payload = serde_json::json!({
            "exception_class": exception_class,
            "message": error.value(py).to_string(),
            "backtrace": backtrace,
        });

        serde_json::to_string(&error_payload).unwrap_or_else(|_| "Failed to serialize error".to_string())
    }

    fn invoke(&mut self, job: &mut solid_queue_jobs::Model) -> Result<solid_queue_jobs::Model> {
        // Execute Python task and handle any errors in a single GIL acquisition
        Python::with_gil(|py| {
            // Execute Python task within the same GIL session
            match self.execute_python_task(py, job) {
                Ok(_) => Ok(job.clone()),
                Err(error) => {
                    // Handle execution error within the same GIL session
                    // This prevents other threads from acquiring GIL between task execution and error handling
                    self.handle_execution_error(py, job, &error)
                }
            }
        })
    }

    /// Execute Python task (parameter parsing + invocation)
    fn execute_python_task(&self, py: Python, job: &solid_queue_jobs::Model) -> PyResult<()> {
        // Parse task parameters
        let (args, kwargs) = self.parse_job_arguments(py, job)?;

        // Create Python instance and invoke
        let bound = self.handler.bind(py);
        let instance = bound.call0()?;
        instance.setattr("id", job.id)?;
        instance.setattr("failed_attempts", job.failed_attempts)?;

        let func = instance.getattr("perform")?;
        func.call(&args, Some(kwargs.bind(py)))?;

        Ok(())
    }

    /// Parse task parameters
    fn parse_job_arguments(&self, py: Python, job: &solid_queue_jobs::Model) -> PyResult<(Py<PyTuple>, Py<PyDict>)> {
        // Deserialize JSON parameters
        let mut v = serde_json::Value::Array(vec![]);
        if let Some(arguments) = job.arguments.as_ref() {
            v = serde_json::from_str(arguments)
                .map_err(|e| PyException::new_err(format!("Failed to parse arguments: {}", e)))?;
        }

        if let serde_json::Value::Object(ref o) = v {
            if let Some(serde_json::Value::Array(_)) = o.get("arguments") {
                v = o["arguments"].clone();
            } else {
                return Err(PyException::new_err("'arguments' is not an array"));
            }
        }

        let binding = json_to_py(py, &v)?;
        let args = binding.downcast_bound::<pyo3::types::PyList>(py)
            .map_err(|e| PyException::new_err(format!("Failed to convert arguments to PyList: {:?}", e)))?;

        // Initialize kwargs
        let kwargs = PyDict::new(py);

        // Handle the last parameter as kwargs if it's a dictionary
        if args.len() > 1 {
            let last_index = args.len() - 1;
            let last = args.get_item(last_index)?;

            if last.is_instance_of::<pyo3::types::PyDict>() {
                let last_dict = last.downcast::<pyo3::types::PyDict>()?;
                for (key, value) in last_dict {
                    kwargs.set_item(key, value)?;
                }
                args.del_item(last_index)?;
            }
        }

        let args_tuple = PyTuple::new(py, args)?;
        Ok((args_tuple.into(), kwargs.into()))
    }

    /// Handle execution error
    fn handle_execution_error(&mut self, py: Python, job: &mut solid_queue_jobs::Model, error: &PyErr) -> Result<solid_queue_jobs::Model> {
        error!("Job execution error: {:?}", error);

        let bound = self.handler.bind(py);

        // Check error handling strategies by priority

        // 1. Check if should retry
        if let Some(retry_strategy) = self.should_retry(py, &bound, error, job.failed_attempts)? {
            return self.apply_retry_strategy(job, retry_strategy);
        }

        // 2. Check if should discard
        if let Some(discard_strategy) = self.should_discard(py, &bound, error)? {
            warn!("Job will be discarded");
            return self.handle_discard(py, &discard_strategy, error, job.clone());
        }

        // 3. Check if there's rescue handling
        if let Some(rescue_strategy) = self.should_rescue(py, &bound, error)? {
            return self.apply_rescue_strategy(py, job, &rescue_strategy, error);
        }

        // 4. Default failure handling
        self.handle_job_failure(py, job, error)
    }

    /// Apply retry strategy
    fn apply_retry_strategy(&mut self, job: &solid_queue_jobs::Model, strategy: RetryStrategy) -> Result<solid_queue_jobs::Model> {
        warn!("Job will be retried (attempt #{})", job.failed_attempts + 1);

        let delay = strategy.wait;
        let scheduled_at = chrono::Utc::now().naive_utc() + chrono::Duration::from_std(delay).map_err(|e| anyhow::anyhow!("Invalid delay duration: {}", e))?;
        let failed_attempts = job.failed_attempts + 1;

        // Set retry information to runnable
        self.retry_info = Some(RetryInfo {
            scheduled_at,
            failed_attempts,
        });

        Ok(job.clone())
    }

    /// Apply rescue strategy
    fn apply_rescue_strategy(&self, py: Python, job: &mut solid_queue_jobs::Model, strategy: &RescueStrategy, error: &PyErr) -> Result<solid_queue_jobs::Model> {
        match strategy.handler.call1(py, (error.value(py),)) {
            Ok(_) => {
                info!("Job rescued from error");
                Ok(job.clone())
            }
            Err(rescue_error) => {
                warn!("Error in rescue handler: {}", rescue_error);
                // Rescue failed, continue to failure handling
                self.handle_job_failure(py, job, error)
            }
        }
    }

    /// Handle task failure
    fn handle_job_failure(&self, py: Python, job: &mut solid_queue_jobs::Model, error: &PyErr) -> Result<solid_queue_jobs::Model> {
        job.failed_attempts += 1;
        let error_payload = self.create_error_payload(py, error);
        Err(anyhow::anyhow!(error_payload))
    }
}

#[pymethods]
impl Runnable {
    #[getter]
    fn get_handler(&self) -> Py<PyAny> {
        self.handler.clone()
    }

    fn perform(&mut self, job: &mut solid_queue_jobs::Model) -> Result<solid_queue_jobs::Model> {
        self.invoke(job)
    }

    fn __repr__(&self) -> String {
        format!(
            "Runnable(class_name={}, queue_as={}, priority={})",
            self.class_name, self.queue_as, self.priority
        )
    }
}

#[pyclass(name = "Metric", subclass)]
#[derive(Debug, Clone)]
pub struct Metric {
    id: i64,
    success: bool,
    duration: tokio::time::Duration,
}

#[pymethods]
impl Metric {
    #[getter]
    fn get_duration(&self) -> tokio::time::Duration {
        self.duration
    }

    fn __repr__(&self) -> String {
        format!("Metric(id={}, success={}, duration={:?})", self.id, self.success, self.duration)
    }
}

#[pyclass(name = "Execution", subclass)]
#[derive(Debug)]
pub struct Execution {
    ctx: Arc<AppContext>,
    timer: Instant,
    tid: String,
    claimed: solid_queue_claimed_executions::Model,
    job: solid_queue_jobs::Model,
    runnable: Runnable,
    metric: Option<Metric>,
    result: Option<Result<solid_queue_jobs::Model>>,
    retry_info: Option<RetryInfo>,
}

impl Execution {
    pub fn new(
        ctx: Arc<AppContext>, claimed: solid_queue_claimed_executions::Model,
        job: solid_queue_jobs::Model, runnable: Runnable,
    ) -> Self {
        // Get current thread's ThreadId
        let thread_id = std::thread::current().id();

        // Convert ThreadId to string
        let thread_id_str = format!("{:?}", thread_id);

        // Extract numeric part
        let thread_id_num: u64 = thread_id_str
            .trim_start_matches("ThreadId(")
            .trim_end_matches(")")
            .parse()
            .expect("Failed to parse ThreadId");
        Self {
            ctx,
            timer: Instant::now(),
            tid: format!("{}", thread_id_num),
            claimed,
            job,
            runnable,
            metric: None,
            result: None,
            retry_info: None,
        }
    }

    fn post_result(&mut self, result: Result<solid_queue_jobs::Model>) {
        self.result = Some(result);
    }

    async fn invoke(&mut self) -> Result<solid_queue_jobs::Model> {
        self.timer = Instant::now();
        let mut job = self.job.clone();
        let job_id = self.claimed.job_id;
        let span = tracing::info_span!("runner", queue=self.runnable.queue_as, jid = job_id, tid = self.tid.clone());
        // let result = self.runnable.invoke(&mut job).instrument(span.clone()).await;
        let result = async {
            let invoke_result = self.runnable.invoke(&mut job);
            // Move retry information from runnable to execution
            if let Some(retry_info) = self.runnable.retry_info.take() {
                self.retry_info = Some(retry_info);
            }
            invoke_result
        }.instrument(span.clone()).await;

        let failed = result.is_err();
        let ret = self.after_executed(result).instrument(span).await;

        if failed {
            return ret;
        }

        Ok(job.clone())
    }

    async fn after_executed(
        &mut self, result: Result<solid_queue_jobs::Model>,
    ) -> Result<solid_queue_jobs::Model> {
        let class_name = self.runnable.class_name.clone();

        let claimed = self.claimed.clone();
        let job_id = self.claimed.job_id;
        let job = self.job.clone();

        let eplased = self.timer.elapsed();
        async {
            if result.is_ok() {
                info!(
                    "Job `{}' executed in: {}",
                    self.runnable.class_name,
                    format!("{:?}", eplased).bright_purple(),
                );
            } else {
                error!("Job `{}' failed in: {:?}", self.runnable.class_name, eplased);
            }

            let metric = Metric { id: job_id, success: result.is_ok(), duration: eplased };
            self.metric = Some(metric);
        }
        // .instrument(tracing::info_span!("runner", jid = job_id, tid = self.tid.clone()))
        .await;

        let db = self.ctx.get_db().await;
        let failed = result.is_err();
        let err = result.as_ref().err().map(|e| e.to_string());

        // Check if retry is needed (determined by execution.retry_info)
        let retry_job_data = if let Some(ref retry_info) = self.retry_info {
            Some((retry_info.scheduled_at, retry_info.failed_attempts))
        } else {
            None
        };

        db.transaction::<_, solid_queue_jobs::Model, DbErr>(|txn| {

            Box::pin(async move {
                let claimed_id = claimed.id;

                // Handle retry logic
                if let Some((scheduled_at, failed_attempts)) = retry_job_data {
                    let retry_job = solid_queue_jobs::ActiveModel {
                        id: ActiveValue::NotSet,
                        queue_name: ActiveValue::Set(job.queue_name.clone()),
                        class_name: ActiveValue::Set(job.class_name.clone()),
                        arguments: ActiveValue::Set(job.arguments.clone()),
                        priority: ActiveValue::Set(job.priority),
                        failed_attempts: ActiveValue::Set(failed_attempts),
                        active_job_id: ActiveValue::Set(job.active_job_id.clone()),
                        scheduled_at: ActiveValue::Set(Some(scheduled_at)),
                        finished_at: ActiveValue::Set(None),
                        concurrency_key: ActiveValue::Set(job.concurrency_key.clone()),
                        created_at: ActiveValue::Set(chrono::Utc::now().naive_utc()),
                        updated_at: ActiveValue::Set(chrono::Utc::now().naive_utc()),
                    };
                    let saved_job = retry_job.save(txn).await?;
                    let new_job_id = match saved_job.id.clone() {
                        ActiveValue::Set(id) => id,
                        _ => return Err(DbErr::Custom("Failed to get job ID after save".into())),
                    };

                    // Create scheduled_execution record
                    let scheduled_execution = solid_queue_scheduled_executions::ActiveModel {
                        id: ActiveValue::NotSet,
                        job_id: ActiveValue::Set(new_job_id),
                        queue_name: ActiveValue::Set(job.queue_name.clone()),
                        priority: ActiveValue::Set(job.priority),
                        scheduled_at: ActiveValue::Set(scheduled_at),
                        created_at: ActiveValue::Set(chrono::Utc::now().naive_utc()),
                    };
                    scheduled_execution.save(txn).await?;

                    info!("Retry job {} scheduled for {}", new_job_id, scheduled_at);
                }

                if failed {
                    error!("Job failed: {:?}", err);
                    // SolidQueue strategy: pass exception info as arguments to create a new Job
                    // My strategy: increase failed_attempts field, stop execution if attempts exceed limit

                    // Write to failed_executions table
                    let failed_execution = solid_queue_failed_executions::ActiveModel {
                        id: ActiveValue::NotSet,
                        job_id: ActiveValue::Set(job_id),
                        error: ActiveValue::Set(err.map(|e| e.to_string())),
                        created_at: ActiveValue::Set(chrono::Utc::now().naive_utc()),
                    };
                    failed_execution.save(txn).await?;

                    // let job = solid_queue_jobs::Entity::find_by_id(job_id).one(txn).await.unwrap();
                    // return Ok(job.unwrap().into());
                }

                // Update jobs table
                let mut job: solid_queue_jobs::ActiveModel = job.clone().into();
                job.finished_at = ActiveValue::Set(Some(chrono::Utc::now().naive_utc()));
                job.updated_at = ActiveValue::Set(chrono::Utc::now().naive_utc());
                let updated = job.update(txn).await?;

                // Directly delete record from claimed_executions table
                let delete_result = solid_queue_claimed_executions::Entity::delete_many()
                    .filter(solid_queue_claimed_executions::Column::Id.eq(claimed_id))
                    .exec(txn)
                    .await?;

                if delete_result.rows_affected == 0 {
                    return Err(DbErr::Custom("Claimed job not found".into()));
                }

                Ok(updated)
            })
        })
        .await
        .map(|job| {
            let duration = self.timer.elapsed();
            if let Ok(job_model) = job.try_into_model() {
                if job_model.finished_at.is_none() {
                    error!("Job `{}' processed in: {:?}", class_name, duration);
                } else {
                    debug!("Job `{}' processed in: {:?}", class_name, duration)
                }
            }
        })
        .map_err(|e| {
            let duration = self.timer.elapsed();
            error!("Job processing failed in {:?}: {:?}", duration, e)
        })
        .ok();


        result
    }
}

#[pymethods]
impl Execution {
    #[getter]
    fn get_id(&self) -> i64 {
        self.job.id
    }

    #[getter]
    fn get_job(&self) -> solid_queue_jobs::Model {
        self.job.clone()
    }

    #[getter]
    fn get_jid(&self) -> i64 {
        self.job.id
    }

    #[getter]
    fn get_tid(&self) -> String {
        self.tid.clone()
    }

    #[getter]
    fn get_metric(&self) -> Option<Metric> {
        self.metric.clone()
    }

    #[setter]
    fn set_tid(&mut self, tid: String) {
        self.tid = tid;
    }

    #[getter]
    fn get_runnable(&self) -> Runnable {
        Python::with_gil(|py| {
            self.runnable.clone_with_gil(py)
        })
    }

    fn perform(&mut self) -> PyResult<()> {
        // Reuse the shared runtime handle when blocking on async work
        let handle = self.ctx.get_runtime_handle()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Tokio runtime handle not initialized"))?;
        let ret = handle.block_on(async { self.invoke().await });

        if let Err(e) = ret {
            return Err(e.into());
        }

        Ok(())
    }


    fn post(&mut self, py: Python, exc: &Bound<'_, PyAny>, traceback: &str) {
        let result = if exc.is_instance_of::<PyException>() {
            let e = match exc.downcast::<PyException>() {
                Ok(ex) => ex,
                Err(_) => {
                    error!("Failed to downcast exception");
                    return;
                }
            };
            error!("error: {:?}", e);
            debug!("is PyException: {:?}", e.is_instance_of::<PyException>());
            // debug!("is CustomError: {:?}", e.is_instance_of::<CustomError>(py));

            // let mut backtrace: Vec<String> = vec![];
            let backtrace: Vec<_> = traceback.lines().map(String::from).collect();

            // if let Some(tb) = traceback {
            //     backtrace = tb.format().unwrap().to_string().lines().map(String::from).collect();
            // }

            error!("error_type: {}", e.get_type().str().map(|s| s.to_string()).unwrap_or_else(|_| "Unknown".to_string()));
            error!("error_description: {}", e.to_string());

            let error_payload = serde_json::json!({
                "exception_class": e.get_type().str().map(|s| s.to_string()).unwrap_or_else(|_| "Unknown".to_string()),
                "message": e.to_string(),
                "backtrace": backtrace,
            });

            Err(anyhow::anyhow!(serde_json::to_string(&error_payload).unwrap_or_else(|_| "Error serialization failed".to_string())))
        } else {
            // info!("Job done");
            Ok(self.job.clone())
        };

        py.allow_threads(|| {
            // Use the shared runtime to run the async post handler
            if let Some(handle) = self.ctx.get_runtime_handle() {
                handle.block_on(async move {
                    let ret = self.after_executed(result).await;
                    debug!("Job result post: {:?}", ret);
                });
            } else {
                error!("Tokio runtime handle not initialized; cannot post job result");
            }
        });
    }

    fn retry(
        &mut self, py: Python, strategy: RetryStrategy, exc: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let job = self.job.clone();
        let scheduled_at = chrono::Utc::now().naive_utc() + strategy.wait();
        let args: serde_json::Value =
            serde_json::from_str(job.arguments.as_ref().ok_or_else(|| PyErr::new::<pyo3::exceptions::PyValueError, _>("Job arguments missing"))?.as_str())
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid job arguments: {}", e)))?;
        let e = exc.downcast::<PyException>()?;
        let error_type = e.get_type().qualname().map(|q| q.to_string()).unwrap_or_else(|_| "Unknown".to_string());

        let params = serde_json::json!({
            "job_class": job.class_name,
            "job_id": job.id,
            "provider_job_id": "",
            "queue_name": job.queue_name,
            "priority": job.priority,
            "arguments": args["arguments"],
            "executions": job.failed_attempts + 1,
            "exception_executions": {
              format!("[{}]", error_type): job.failed_attempts + 1
            },
            "locale": "en",
            "timezone": "UTC",
            "scheduled_at": scheduled_at,
            "enqueued_at": scheduled_at,
        });

        let span = tracing::info_span!("runner", queue=self.runnable.queue_as, jid = job.id, tid = self.tid.clone());
        let _enter = span.enter();

        if (job.failed_attempts as i64) >= strategy.attempts {
            error!("Job `{}' failed after {} attempts", job.class_name, job.failed_attempts);
            return Ok(());
        }

        py.allow_threads(|| {
            let span = span.clone();
            let job = self.ctx.get_runtime_handle()
                .map(|h| h.block_on(async {
                    warn!(
                        "Attempt {} scheduled due to `{}' on {:?}",
                        format!("#{}", job.failed_attempts + 1).bright_purple(),
                        error_type,
                        scheduled_at
                    );

                    let db = self.ctx.get_db().await;
                    db.transaction::<_, solid_queue_jobs::ActiveModel, DbErr>(|txn| {
                        Box::pin(async move {
                            let concurrency_key =
                                job.concurrency_key.clone().unwrap_or("".to_string());
                            let job = solid_queue_jobs::ActiveModel {
                                id: ActiveValue::NotSet,
                                queue_name: ActiveValue::Set(job.queue_name),
                                class_name: ActiveValue::Set(job.class_name),
                                arguments: ActiveValue::Set(Some(params.to_string())),
                                priority: ActiveValue::Set(job.priority),
                                failed_attempts: ActiveValue::Set(job.failed_attempts + 1),
                                active_job_id: ActiveValue::Set(job.active_job_id.clone()),
                                scheduled_at: ActiveValue::Set(Some(scheduled_at)),
                                finished_at: ActiveValue::Set(None),
                                concurrency_key: ActiveValue::Set(Some(concurrency_key.clone())),
                                created_at: ActiveValue::Set(chrono::Utc::now().naive_utc()),
                                updated_at: ActiveValue::Set(chrono::Utc::now().naive_utc()),
                            }
                            .save(txn)
                            .await?;

                            let job_id = match job.id.clone() {
                                ActiveValue::Set(id) => id,
                                _ => return Err(DbErr::Custom("Failed to get job ID".into())),
                            };
                            let _scheduled_execution =
                                solid_queue_scheduled_executions::ActiveModel {
                                    id: ActiveValue::not_set(),
                                    job_id: ActiveValue::Set(job_id),
                                    queue_name: match job.queue_name.clone() {
                                        ActiveValue::Set(q) => ActiveValue::Set(q),
                                        _ => return Err(DbErr::Custom("Queue name missing".into())),
                                    },
                                    priority: match job.priority.clone() {
                                        ActiveValue::Set(p) => ActiveValue::Set(p),
                                        _ => return Err(DbErr::Custom("Priority missing".into())),
                                    },
                                    scheduled_at: ActiveValue::Set(scheduled_at),
                                    created_at: ActiveValue::Set(chrono::Utc::now().naive_utc()),
                                }
                                .save(txn)
                                .await?;

                            // debug!("{:?}", scheduled_execution);
                            Ok(job)
                        })
                    })
                    .await
                }
                .instrument(span)))
                .unwrap_or_else(|| Err(sea_orm::TransactionError::Connection(DbErr::Custom("Runtime handle unavailable".into()))));

            // debug!("scheduled: {:?}", job);
            if job.is_err() {
                error!("Job failed to schedule: {:?}", job.err());
            }
        });

        Ok(())
    }
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct Worker {
    pub ctx: Arc<AppContext>,
    pub start_handlers: Arc<RwLock<Vec<Py<PyAny>>>>,
    pub stop_handlers: Arc<RwLock<Vec<Py<PyAny>>>>,
    polling: Arc<tokio::sync::Mutex<i32>>,
    token: Arc<tokio::sync::Mutex<i32>>,
    dispatch_receiver: Arc<tokio::sync::Mutex<tokio::sync::mpsc::Receiver<Execution>>>,
    dispatch_sender: Arc<tokio::sync::Mutex<tokio::sync::mpsc::Sender<Execution>>>,
    sink_receiver: Arc<tokio::sync::Mutex<tokio::sync::mpsc::Receiver<Execution>>>,
    sink_sender: Arc<tokio::sync::Mutex<tokio::sync::mpsc::Sender<Execution>>>,
}

impl Worker {
    pub fn new(ctx: Arc<AppContext>) -> Self {
        let start_handlers = Arc::new(RwLock::new(Vec::<Py<PyAny>>::new()));
        let stop_handlers = Arc::new(RwLock::new(Vec::<Py<PyAny>>::new()));
        let polling = Arc::new(tokio::sync::Mutex::new(0));
        let token = Arc::new(tokio::sync::Mutex::new(0));
        let (tx, mut _rx) = tokio::sync::mpsc::channel::<Execution>(ctx.worker_threads as usize);
        let dispatch_receiver = Arc::new(tokio::sync::Mutex::new(_rx));
        let dispatch_sender = Arc::new(tokio::sync::Mutex::new(tx));

        let (tx1, mut _rx1) = tokio::sync::mpsc::channel::<Execution>(ctx.worker_threads as usize);
        let sink_receiver = Arc::new(tokio::sync::Mutex::new(_rx1));
        let sink_sender = Arc::new(tokio::sync::Mutex::new(tx1));

        Self {
            ctx,
            start_handlers,
            stop_handlers,
            polling,
            token,
            dispatch_receiver,
            dispatch_sender,
            sink_sender,
            sink_receiver,
        }
    }

    pub fn register_start_handler(&self, py: Python, handler: Py<PyAny>) -> PyResult<()> {
        let callable = handler.bind(py);
        if !callable.is_callable() {
            return Err(PyTypeError::new_err("Expected a callable object for worker start handler"));
        }

        {
            if let Ok(mut handlers) = self.start_handlers.write() {
                handlers.push(handler.clone_ref(py));
            }
            debug!("Worker start handler: {:?} registered", handler);
        }

        Ok(())
    }

    pub fn register_stop_handler(&self, py: Python, handler: Py<PyAny>) -> PyResult<()> {
        let callable = handler.bind(py);
        if !callable.is_callable() {
            return Err(PyTypeError::new_err("Expected a callable object for worker stop handler"));
        }

        {
            if let Ok(mut handlers) = self.stop_handlers.write() {
                handlers.push(handler.clone_ref(py));
            }
            debug!("Worker stop handler: {:?} registered", handler);
        }

        Ok(())
    }





    // fn get_job_class(&self, class_name: &str) -> PyResult<Py<PyAny>> {
    //     let runnables = self.runnables.read().unwrap();
    //     let runnable = runnables.get(class_name).unwrap();
    //     let job_class = runnable.handler.clone();
    //     Ok(job_class.clone())
    // }

    pub fn register_job_class(&self, _py: Python, klass: Py<PyAny>) -> PyResult<()> {
        Python::with_gil(|py| -> PyResult<()> {
            let bound = klass.bind(py);
            let class_name = bound.downcast::<PyType>()?.qualname()?;
            debug!("Registered job class: {:?}", class_name);

            let mut queue_name = "default".to_string();
            if bound.hasattr("queue_as")? {
                queue_name = bound.getattr("queue_as")?.extract::<String>()?;
            }

            let mut concurrency_limit: Option<i32> = None;
            let mut concurrency_duration: Option<i32> = None;

            // Extract concurrency_limit if exists
            if bound.hasattr("concurrency_limit")? {
                concurrency_limit = Some(bound.getattr("concurrency_limit")?.extract::<i32>()?);
            }

            // Extract concurrency_duration if exists (convert to seconds)
            if bound.hasattr("concurrency_duration")? {
                concurrency_duration = Some(bound.getattr("concurrency_duration")?.extract::<i32>()?);
            }

            // Check if job has concurrency control (concurrency_key attribute exists and is not None/empty)
            let has_concurrency_control = if bound.hasattr("concurrency_key")? {
                let concurrency_key_attr = bound.getattr("concurrency_key")?;
                if concurrency_key_attr.is_none() {
                    false
                } else if concurrency_key_attr.is_callable() {
                    // If it's a method, assume it provides concurrency control
                    true
                } else {
                    // If it's a property, check if it's not empty string
                    match concurrency_key_attr.extract::<String>() {
                        Ok(key) => !key.is_empty(),
                        Err(_) => false
                    }
                }
            } else {
                false
            };

            let runnable = Runnable {
                class_name: class_name.to_string().clone(),
                handler: klass,
                queue_as: queue_name,
                priority: 0,
                retry_info: None,
                concurrency_limit,
                concurrency_duration,
            };
            info!("Registered job: {:?}", runnable);

            if let Ok(mut runnables) = self.ctx.runnables.write() {
                runnables.insert(class_name.to_string(), runnable);
            }

            // Only store concurrency info if concurrency control is actually enabled
            if has_concurrency_control {
                self.ctx.enable_concurrency_control(class_name.to_string());
            }
            Ok(())
        })
    }

    pub async fn claim_job(&self) -> Result<solid_queue_claimed_executions::Model, anyhow::Error> {
        let _opts = self.ctx.connect_options.clone();
        // if self.ctx.silence_polling {
        //     opts.sqlx_logging(false); //.sqlx_logging_level(log::LevelFilter::Error);
        //                               // warn!("-------------------- opts: {:?}", opts);
        // }
        let db = self.ctx.get_db().await;

        let job = db
            .transaction::<_, solid_queue_claimed_executions::ActiveModel, DbErr>(|txn| {
                Box::pin(async move {
                    // Get an unlocked task
                    // let record: Option<solid_queue_ready_executions::Model> = solid_queue_ready_executions::Entity::find()
                    //     .from_raw_sql(Statement::from_sql_and_values(
                    //         DbBackend::Postgres,
                    //         r#"SELECT * FROM "solid_queue_ready_executions" ORDER BY "priority" ASC, "job_id" ASC LIMIT $1 FOR UPDATE SKIP LOCKED"#,
                    //         [1.into()],
                    //     ))
                    //     .one(txn)
                    //     .await?;

                    let record = solid_queue_ready_executions::Entity::find()
                        .order_by_asc(solid_queue_ready_executions::Column::Priority)
                        .order_by_asc(solid_queue_ready_executions::Column::JobId)
                        .lock_with_behavior(sea_query::LockType::Update, LockBehavior::SkipLocked)
                        .limit(1)
                        .one(txn)
                        .await?;

                    if let Some(execution) = record {
                        // debug!("---------- execution: {:?}", execution);
                        let claimed = solid_queue_claimed_executions::ActiveModel {
                            id: ActiveValue::NotSet,
                            job_id: ActiveValue::Set(execution.job_id),
                            process_id: ActiveValue::NotSet,
                            created_at: ActiveValue::Set(chrono::Utc::now().naive_utc()),
                        }
                        .save(txn)
                        .await
                        ?;

                        execution.delete(txn).await?;
                        Ok(claimed)
                    } else {
                        Err(DbErr::Custom("No job found".into()))
                    }
                })
            })
            .await?;

        Ok(job.try_into_model()?)
    }

    // #[tracing::instrument]
    pub async fn claim_jobs(
        &self, batch_size: u64,
    ) -> Result<Vec<solid_queue_claimed_executions::Model>, anyhow::Error> {
        let timer = Instant::now();
        let db = self.ctx.get_db().await;
        let _use_skip_locked = self.ctx.use_skip_locked;

        let jobs = db
            .transaction::<_, Vec<solid_queue_claimed_executions::ActiveModel>, DbErr>(|txn| {
                Box::pin(async move {
                    // Get list of paused queues
                    let paused_queues: Vec<String> = solid_queue_pauses::Entity::find()
                        .select_only()
                        .column(solid_queue_pauses::Column::QueueName)
                        .into_tuple()
                        .all(txn)
                        .await?;

                    if !paused_queues.is_empty() {
                        debug!("Paused queues: {:?}", paused_queues);
                    }

                    // Get jobs from non-paused queues
                    let db_backend = txn.get_database_backend();
                    let records: Vec<solid_queue_ready_executions::Model> = match db_backend {
                        DbBackend::Sqlite => {
                            // For SQLite, filter out paused queues in application code
                            let mut records = solid_queue_ready_executions::Entity::find()
                                .from_raw_sql(Statement::from_sql_and_values(
                                    DbBackend::Sqlite,
                                    "SELECT * FROM solid_queue_ready_executions ORDER BY priority ASC, job_id ASC LIMIT ?",
                                    [batch_size.into()],
                                ))
                                .all(txn)
                                .await?;

                            records.retain(|record| !paused_queues.contains(&record.queue_name));
                            records
                        },
                        _ => {
                            // For PostgreSQL and MySQL, filter in SQL
                            let mut stmt = format!(
                                r#"SELECT * FROM "solid_queue_ready_executions" WHERE "queue_name" NOT IN ({}) ORDER BY "priority" ASC, "job_id" ASC LIMIT $1 FOR UPDATE SKIP LOCKED"#,
                                paused_queues.iter().enumerate().map(|(i, _)| format!("${}", i + 2)).collect::<Vec<_>>().join(",")
                            );

                            if paused_queues.is_empty() {
                                stmt = r#"SELECT * FROM "solid_queue_ready_executions" ORDER BY "priority" ASC, "job_id" ASC LIMIT $1 FOR UPDATE SKIP LOCKED"#.to_string();
                            }

                            let mut values: Vec<Value> = vec![batch_size.into()];
                            values.extend(paused_queues.iter().map(|q| q.clone().into()));

                            solid_queue_ready_executions::Entity::find()
                                .from_raw_sql(Statement::from_sql_and_values(
                                    db_backend,
                                    &stmt,
                                    values,
                                ))
                                .all(txn)
                                .await?
                        }
                    };

                    // let records: Vec<solid_queue_ready_executions::Model> =
                    // solid_queue_ready_executions::Entity::find()
                    //     // .select_only()
                    //     // .column(solid_queue_ready_executions::Column::JobId)
                    //     .order_by_asc(solid_queue_ready_executions::Column::Priority)
                    //     .order_by_asc(solid_queue_ready_executions::Column::JobId)
                    //     .lock_with_behavior(
                    //         sea_query::LockType::Update,
                    //         LockBehavior::SkipLocked,
                    //     )
                    //     .limit(batch_size)
                    //     .all(txn)
                    //     .await?;

                    let mut claimed_jobs = Vec::new();

                    for execution in records {
                        let claimed = solid_queue_claimed_executions::ActiveModel {
                            id: ActiveValue::NotSet,
                            job_id: ActiveValue::Set(execution.job_id),
                            process_id: ActiveValue::NotSet,
                            created_at: ActiveValue::Set(chrono::Utc::now().naive_utc()),
                        }
                        .save(txn)
                        .await
                        ?;

                        execution.delete(txn).await?;
                        claimed_jobs.push(claimed);
                    }
                    Ok(claimed_jobs)
                })
            })
            .await?;

        let claimed_models: Result<Vec<solid_queue_claimed_executions::Model>, _> =
            jobs.into_iter().map(|job| job.try_into_model()).collect();
        trace!("elpased: {:?}", timer.elapsed());
        claimed_models.map_err(|e| anyhow::Error::new(e))
    }

    pub async fn run_main_loop(&self) -> Result<(), anyhow::Error> {
        // Don't acquire long-term connections here, get them when needed
        let mut polling_interval = tokio::time::interval(self.ctx.worker_polling_interval);
        let mut heartbeat_interval = tokio::time::interval(self.ctx.process_heartbeat_interval);
        let worker_threads = self.ctx.worker_threads;
        let tx = self.dispatch_sender.clone();
        let ctx = self.ctx.clone();

        // Ensure only one main loop is running
        let _ = self.polling.lock().await;
        let thread_id = Self::get_tid();

        let quit = self.ctx.graceful_shutdown.clone();

        // Initialize process record
        let init_db = self.ctx.get_db().await;
        let process = self.on_start(&init_db, "Worker".to_string(), "worker".to_string()).await?;
        info!(">> Process started: {:?}", process);

        // Set up PostgreSQL LISTEN if available
        // Now using real sqlx PgListener for immediate notifications
        let mut notify_rx = None;
        if self.ctx.is_postgres() {
            info!("PostgreSQL detected, enabling LISTEN/NOTIFY for immediate job processing");
            let notify_manager = NotifyManager::new(self.ctx.clone(), "default");
            match notify_manager.start_listener().await {
                Ok(rx) => {
                    notify_rx = Some(rx);
                    info!("LISTEN started - jobs will be processed immediately when notified");
                }
                Err(e) => {
                    warn!("Failed to start real LISTEN, falling back to polling only: {}", e);
                }
            }
        } else {
            info!("Non-PostgreSQL database detected, using polling only");
        }

        // Call worker start handlers before starting the main loop
        Python::with_gil(|py| {
            let handlers = self.start_handlers.read().expect("Lock poisoned");
            for handler in handlers.iter() {
                match handler.bind(py).call0() {
                    Ok(_) => debug!("Worker start handler executed successfully in main loop"),
                    Err(e) => error!("Error calling worker start handler in main loop: {:?}", e)
                }
            }
        });

        // Producer sends tasks
        loop {
            tokio::select! {
                _ = heartbeat_interval.tick() => {
                    let heartbeat_db = self.ctx.get_db().await;
                    self.heartbeat(&heartbeat_db, &process).await?;
                }
                _ = quit.cancelled() => {
                    info!("Graceful shutdown, stop polling");

                    // Call worker stop handlers before exiting
                    Python::with_gil(|py| {
                        let handlers = self.stop_handlers.read().expect("Lock poisoned");
                        for handler in handlers.iter() {
                            match handler.bind(py).call0() {
                                Ok(_) => debug!("Worker stop handler executed successfully in main loop"),
                                Err(e) => error!("Error calling worker stop handler in main loop: {:?}", e)
                            }
                        }
                    });

                    // Clean up process record
                    let stop_db = self.ctx.get_db().await;
                    self.on_stop(&stop_db, &process).await?;

                    return Ok(());
                }
                // Handle real PostgreSQL NOTIFY messages for immediate job processing
                notify_msg = async {
                    if let Some(ref mut rx) = notify_rx {
                        rx.recv().await
                    } else {
                        // If no LISTEN is active, this branch will never be taken
                        std::future::pending().await
                    }
                } => {
                    if notify_msg.is_some() {
                        // Simple batching: consume additional immediate messages
                        let mut total_notifies = 1;
                        while let Some(rx) = notify_rx.as_mut() {
                            if rx.try_recv().is_err() {
                                break;
                            }
                            total_notifies += 1;
                        }

                        async { debug!("Processing jobs immediately (batched {} notifications)", total_notifies); }.instrument(tracing::info_span!("listener", consumed=total_notifies)).await;
                        trace!("Received {} NOTIFY message(s), processing jobs immediately", total_notifies);

                        // Process jobs with timeout protection to prevent blocking main loop
                        let process_future = self.process_available_jobs(worker_threads, &tx, &ctx, &thread_id, "NOTIFY");
                        let timeout_duration = tokio::time::Duration::from_millis(100); // 100ms max

                        match tokio::time::timeout(timeout_duration, process_future).await {
                            Ok(_) => {
                                trace!("NOTIFY job processing completed within timeout");
                            }
                            Err(_) => {
                                warn!("NOTIFY job processing timed out after {}ms - will rely on polling", timeout_duration.as_millis());
                            }
                        }
                    }
                }
                _ = polling_interval.tick() => {
                    // debug!("⏰ POLLING triggered - regular interval check");
                    // Regular polling at configured interval (backup for reliability) - get fresh connection
                    self.process_available_jobs(worker_threads, &tx, &ctx, &thread_id, "POLLING").await;
                }
            }
        }
    }

    /// Extract job processing logic to avoid duplication
    async fn process_available_jobs(
        &self,
        worker_threads: u64,
        tx: &Arc<Mutex<Sender<Execution>>>,
        ctx: &Arc<AppContext>,
        thread_id: &str,
        source: &str,
    ) {
        let claimed = self.claim_jobs(worker_threads).instrument(tracing::info_span!("polling", tid=thread_id)).await;

        if let Err(e) = claimed {
            debug!("[{}] no job found: {:?}", source, e);
            return;
        }

        let claimed = match claimed {
            Ok(c) => c,
            Err(_) => return,
        };
        if claimed.is_empty() {
            // debug!("[{}] no jobs available", source);
            return;
        }

        async { debug!("found {} job(s) to process", claimed.len()); }.instrument(tracing::info_span!("polling", tid=thread_id, source=source)).await;

        // Process claimed jobs without holding a long transaction
        // First collect all job data quickly, then create executions
        let job_ids: Vec<i64> = claimed.iter().map(|row| row.job_id).collect();
        let job_data = {
            let processing_db = ctx.get_db().await;
            // Use a single quick transaction to fetch all job data
            let jobs_result = processing_db.transaction::<_, Vec<solid_queue_jobs::Model>, DbErr>(|txn| {
                Box::pin(async move {
                    let mut jobs = Vec::new();
                    for job_id in job_ids {
                        if let Some(job) = solid_queue_jobs::Entity::find_by_id(job_id).one(txn).await? {
                            jobs.push(job);
                        }
                    }
                    Ok(jobs)
                })
            }).await;

            match jobs_result {
                Ok(jobs) => jobs,
                Err(e) => {
                    error!("Failed to fetch job data: {:?}", e);
                    return;
                }
            }
        }; // processing_db is released here

        // Now create executions without holding any database connections
        for (row, job) in claimed.into_iter().zip(job_data.into_iter()) {
            // Get runnable with proper GIL handling
            let runnable = Python::with_gil(|_py| {
                self.ctx.get_runnable(&job.class_name)
            });

            if runnable.is_err() {
                error!("Job handler not found: {:?}", &job.class_name);
                continue;
            }

            let execution = match runnable {
                Ok(r) => Execution::new(ctx.clone(), row, job, r),
                Err(_) => continue,
            };

            // Send execution to worker thread - this may block but no database connections are held
            if let Err(e) = tx.lock().await.send(execution).await {
                error!("Failed to send execution to worker thread: {:?}", e);
            }
        }
    }

    pub async fn run(&self) -> Result<(), anyhow::Error> {
        let rx = self.dispatch_receiver.clone();
        let state = self.token.clone();
        let tid = Self::get_tid();

        debug!("Worker started: {:?}", tid);

        // Call worker start handlers
        Python::with_gil(|py| {
            let handlers = self.start_handlers.read().expect("Lock poisoned");
            for handler in handlers.iter() {
                match handler.bind(py).call0() {
                    Ok(_) => debug!("Worker start handler executed successfully"),
                    Err(e) => error!("Error calling worker start handler: {:?}", e)
                }
            }
        });

        let ret = runner(self.ctx.clone(), tid.clone(), state, rx)
            .instrument(info_span!("runner", tid = tid.clone()))
            .await;

        // Call worker stop handlers
        Python::with_gil(|py| {
            let handlers = self.stop_handlers.read().expect("Lock poisoned");
            for handler in handlers.iter() {
                match handler.bind(py).call0() {
                    Ok(_) => debug!("Worker stop handler executed successfully"),
                    Err(e) => error!("Error calling worker stop handler: {:?}", e)
                }
            }
        });

        ret
    }

    pub async fn pick_job(&self) -> Result<Execution, anyhow::Error> {
        let rx = self.dispatch_receiver.clone();
        let mut receiver = rx.lock().await;
        let execution = receiver.recv().await;
        if execution.is_none() {
            return Err(anyhow::Error::msg("No job found"));
        }
        let execution = execution.ok_or_else(|| anyhow::Error::msg("No job found"))?;
        Ok(execution)
    }

    pub async fn post_execution(&self) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl ProcessTrait for Worker {}
