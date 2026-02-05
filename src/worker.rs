use crate::context::*;
use crate::entities::*;
use crate::process::{ProcessInfo, ProcessTrait};
use crate::query_builder;
use crate::semaphore::release_semaphore;
use anyhow::Result;
use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

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

use pyo3::exceptions::PyTypeError;
use pyo3::types::{PyBool, PyDict, PyList, PyModule, PyTuple, PyType};

use crate::notify::NotifyManager;

fn json_to_py(py: Python<'_>, value: &serde_json::Value) -> PyResult<Py<PyAny>> {
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

fn python_thread_ident() -> Option<u64> {
    Python::attach(|py| -> PyResult<u64> {
        let threading = PyModule::import(py, "threading")?;
        let get_ident = threading.getattr("get_ident")?;
        let ident = get_ident.call0()?;
        ident.extract::<u64>()
    })
    .ok()
}

async fn runner(
    ctx: Arc<AppContext>,
    _thread_id: String,
    _state: Arc<Mutex<i32>>,
    rx: Arc<Mutex<Receiver<Execution>>>,
) -> Result<()> {
    let tid = python_thread_ident()
        .map(|thread_id| {
            trace!("python thread_id: {:?}", thread_id);
            thread_id.to_string()
        })
        .unwrap_or_else(|| format!("{:?}", std::thread::current().id()));
    let graceful_shutdown = ctx.graceful_shutdown.clone();
    let force_quit = ctx.force_quit.clone();

    loop {
        let mut receiver = rx.lock().await;
        tokio::select! {
          _ = graceful_shutdown.cancelled() => {
              info!("Graceful shutdown");
              break;
          }
          execution = receiver.recv() => {
              drop(receiver);
              let Some(mut execution) = execution else { continue };
              execution.tid = tid.clone();

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
    pub(crate) retry_info: Option<RetryInfo>,
    pub concurrency_limit: Option<i32>,
    pub concurrency_duration: Option<i32>, // in seconds
    /// Continuation info for interrupted jobs (step name, cursor, original args)
    pub(crate) continuation_info: Option<ContinuationInfo>,
}

#[derive(Debug, Clone)]
pub(crate) struct ContinuationInfo {
    pub(crate) state: crate::continuation::ContinuationState,
    pub(crate) resumptions: i32,
    pub(crate) original_args: serde_json::Value,
}

#[derive(Debug, Clone)]
pub(crate) struct RetryInfo {
    pub(crate) scheduled_at: chrono::NaiveDateTime,
    pub(crate) arguments: String,
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
            continuation_info: None,
        }
    }

    /// Safe clone method that requires GIL
    pub fn clone_with_gil(&self, _py: Python<'_>) -> Self {
        Self {
            class_name: self.class_name.clone(),
            handler: self.handler.clone(),
            queue_as: self.queue_as.clone(),
            priority: self.priority,
            retry_info: self.retry_info.clone(),
            concurrency_limit: self.concurrency_limit,
            concurrency_duration: self.concurrency_duration,
            continuation_info: self.continuation_info.clone(),
        }
    }

    /// Get the concurrency constraint for this job with given arguments
    /// Returns Some(ConcurrencyConstraint) if concurrency control is enabled, None otherwise
    /// Handles Python GIL internally and performs all checks once
    pub fn get_concurrency_constraint<T, K>(
        &self,
        args: Option<T>,
        kwargs: Option<K>,
    ) -> Result<Option<ConcurrencyConstraint>>
    where
        T: crate::utils::IntoPython,
        K: crate::utils::IntoPython,
    {
        Python::attach(|py| {
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
                    args_bound.cast::<pyo3::types::PyTuple>()?.clone().into()
                } else if args_bound.is_instance_of::<pyo3::types::PyList>() {
                    let list = args_bound.cast::<pyo3::types::PyList>()?;
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
                    kwargs_bound.cast::<pyo3::types::PyDict>()?.clone().into()
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
            let duration = self
                .concurrency_duration
                .map(|seconds| chrono::Duration::seconds(seconds as i64));

            Ok(Some(ConcurrencyConstraint {
                key,
                limit,
                duration,
            }))
        })
        .map_err(|e: PyErr| anyhow::anyhow!("Python error in get_concurrency_constraint: {}", e))
    }

    /// Check if should retry, return matching retry strategy
    fn should_retry(
        &self,
        py: Python,
        bound: &Bound<PyAny>,
        error: &PyErr,
        executions: i32,
    ) -> PyResult<Option<RetryStrategy>> {
        if !bound.hasattr("retry_on")? {
            return Ok(None);
        }

        let retry_strategies = bound.getattr("retry_on")?.extract::<Vec<RetryStrategy>>()?;

        for strategy in retry_strategies {
            if i64::from(executions) >= strategy.attempts {
                continue; // Exceeded maximum retry count
            }

            if self.is_exception_match(py, &strategy.exceptions, error)? {
                return Ok(Some(strategy));
            }
        }

        Ok(None)
    }

    /// Check if should discard, return matching discard strategy
    fn should_discard(
        &self,
        py: Python,
        bound: &Bound<PyAny>,
        error: &PyErr,
    ) -> PyResult<Option<DiscardStrategy>> {
        if !bound.hasattr("discard_on")? {
            return Ok(None);
        }

        let discard_strategies = bound
            .getattr("discard_on")?
            .extract::<Vec<DiscardStrategy>>()?;

        for strategy in discard_strategies {
            if self.is_exception_match(py, &strategy.exceptions, error)? {
                return Ok(Some(strategy));
            }
        }

        Ok(None)
    }

    /// Check if has rescue handling, return matching rescue strategy
    fn should_rescue(
        &self,
        py: Python,
        bound: &Bound<PyAny>,
        error: &PyErr,
    ) -> PyResult<Option<RescueStrategy>> {
        if !bound.hasattr("rescue_from")? {
            return Ok(None);
        }

        let rescue_strategies = bound
            .getattr("rescue_from")?
            .extract::<Vec<RescueStrategy>>()?;

        for strategy in rescue_strategies {
            if self.is_exception_match(py, &strategy.exceptions, error)? {
                return Ok(Some(strategy));
            }
        }

        Ok(None)
    }

    /// Check if exception matches
    fn is_exception_match(
        &self,
        py: Python,
        exceptions: &Py<PyAny>,
        error: &PyErr,
    ) -> PyResult<bool> {
        let exceptions_bound = exceptions.bind(py);

        if let Ok(exception_type) = exceptions_bound.cast::<PyType>() {
            return Ok(error.is_instance(py, exception_type));
        }

        if let Ok(exception_tuple) = exceptions_bound.cast::<PyTuple>() {
            let matched = exception_tuple.iter().any(|item| {
                item.cast::<PyType>()
                    .is_ok_and(|exception_type| error.is_instance(py, exception_type))
            });
            if matched {
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Handle discard - directly mark task as completed without recording failure information
    fn handle_discard(
        &self,
        py: Python,
        strategy: &DiscardStrategy,
        error: &PyErr,
        job: quebec_jobs::Model,
    ) -> Result<quebec_jobs::Model> {
        // Call discard handler (if any)
        if let Some(handler) = &strategy.handler {
            if let Err(handler_error) = handler.call1(py, (error.value(py),)) {
                warn!("Error in discard handler: {}", handler_error);
            }
        }

        let error_name = error
            .get_type(py)
            .name()
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

        let exception_class = error
            .get_type(py)
            .name()
            .map(|s| s.to_string())
            .unwrap_or_else(|_| "UnknownError".to_string());

        let error_payload = serde_json::json!({
            "exception_class": exception_class,
            "message": error.value(py).to_string(),
            "backtrace": backtrace,
        });

        serde_json::to_string(&error_payload)
            .unwrap_or_else(|_| "Failed to serialize error".to_string())
    }

    fn invoke(
        &mut self,
        job: &mut quebec_jobs::Model,
        cancellation_token: Option<CancellationToken>,
    ) -> Result<quebec_jobs::Model> {
        // Execute Python task and handle any errors in a single GIL acquisition
        Python::attach(|py| {
            // Execute Python task within the same GIL session
            match self.execute_python_task(py, job, cancellation_token) {
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
    fn execute_python_task(
        &mut self,
        py: Python,
        job: &quebec_jobs::Model,
        cancellation_token: Option<CancellationToken>,
    ) -> PyResult<()> {
        use crate::continuation::{parse_continuation_from_arguments, Continuable};

        // Parse continuation state from arguments
        let (cont_state, resumptions, original_args) =
            parse_continuation_from_arguments(job.arguments.as_deref());

        // Store original args for potential re-enqueue
        self.continuation_info = Some(ContinuationInfo {
            state: cont_state.clone(),
            resumptions,
            original_args: original_args.clone(),
        });

        // Parse task parameters from original args (without continuation metadata)
        let (args, kwargs) = self.parse_job_arguments_from_json(py, &original_args)?;

        // Create Python instance and invoke
        let bound = self.handler.bind(py);
        let instance = bound.call0()?;
        instance.setattr("id", job.id)?;
        let executions = crate::utils::get_executions(job.arguments.as_deref());
        instance.setattr("executions", executions)?;

        // Check if the job class inherits from Continuable mixin
        // We check for _continuation attribute (defined in Continuable class) and verify it's None
        // (indicating it's from the class definition, not accidentally set)
        let is_continuable = if let Ok(cont_attr) = instance.getattr("_continuation") {
            // Has _continuation attribute - check if it's the class default (None)
            // This is a reliable indicator that the class inherits from Continuable
            cont_attr.is_none()
        } else {
            false
        };

        if is_continuable {
            // Create a Continuable instance with the restored state
            // Pass the cancellation token so checkpoint() can detect SIGTERM
            let continuable = Continuable::with_state(cont_state, resumptions, cancellation_token);

            // Inject the continuation context into the instance
            let cont_py = continuable.into_pyobject(py)?;
            instance.setattr("_continuation", cont_py.clone())?;

            // Make step method available on instance (delegates to _continuation.step)
            // This is handled by the Python Continuable mixin
        }

        let func = instance.getattr("perform")?;
        let result = func.call(&args, Some(kwargs.bind(py)));

        // On error (including JobInterrupted), capture continuation state from the instance
        // BEFORE the instance goes out of scope
        if result.is_err() {
            if let Ok(cont_attr) = instance.getattr("_continuation") {
                if let Ok(continuable) = cont_attr.extract::<crate::continuation::Continuable>() {
                    if let Ok((state, resump, dirty)) = continuable.get_state() {
                        if dirty {
                            // Update continuation_info with the actual state from the running job
                            if let Some(ref mut info) = self.continuation_info {
                                info.state = state;
                                info.resumptions = resump;
                                debug!(
                                    "Captured continuation state: completed={:?}, current={:?}",
                                    info.state.completed, info.state.current
                                );
                            }
                        }
                    }
                }
            }
        }

        // Propagate the result (error or success)
        result.map(|_| ())?;

        // After successful execution, clear continuation info
        self.continuation_info = None;

        Ok(())
    }

    /// Parse job arguments from JSON value (used for continuation support)
    fn parse_job_arguments_from_json(
        &self,
        py: Python,
        json_args: &serde_json::Value,
    ) -> PyResult<(Py<PyTuple>, Py<PyDict>)> {
        let mut v = json_args.clone();

        // Handle wrapped format: {"arguments": [...]}
        if let serde_json::Value::Object(ref o) = v {
            if let Some(serde_json::Value::Array(_)) = o.get("arguments") {
                v = o["arguments"].clone();
            }
        }

        // If not an array, wrap it
        if !v.is_array() {
            v = serde_json::Value::Array(vec![]);
        }

        let binding = json_to_py(py, &v)?;
        let args = binding.cast_bound::<pyo3::types::PyList>(py).map_err(|e| {
            PyException::new_err(format!("Failed to convert arguments to PyList: {:?}", e))
        })?;

        // Initialize kwargs
        let kwargs = PyDict::new(py);

        // Check for "_kwargs" marker in the last element
        // Format: [arg1, arg2, {"_kwargs": {"key": "value"}}]
        // This distinguishes kwargs from positional dict arguments
        if !args.is_empty() {
            let last_index = args.len() - 1;
            let last = args.get_item(last_index)?;

            if last.is_instance_of::<pyo3::types::PyDict>() {
                let last_dict = last.cast::<pyo3::types::PyDict>()?;

                // Check for "_kwargs" marker
                if let Ok(Some(kwargs_value)) = last_dict.get_item("_kwargs") {
                    // Found "_kwargs" marker - extract the actual kwargs
                    if let Ok(inner_dict) = kwargs_value.cast::<pyo3::types::PyDict>() {
                        for (key, value) in inner_dict {
                            kwargs.set_item(key, value)?;
                        }
                    }
                    args.del_item(last_index)?;
                }
                // If no "_kwargs" marker, treat as positional dict argument (don't extract)
            }
        }

        let args_tuple = PyTuple::new(py, args)?;
        Ok((args_tuple.into(), kwargs.into()))
    }

    /// Handle execution error
    fn handle_execution_error(
        &mut self,
        py: Python,
        job: &mut quebec_jobs::Model,
        error: &PyErr,
    ) -> Result<quebec_jobs::Model> {
        // Check for JobInterrupted first (continuation checkpoint)
        let job_interrupted = py.get_type::<crate::continuation::JobInterrupted>();
        if error.is_instance(py, &job_interrupted) {
            info!(
                "Job `{}' interrupted for continuation, will be re-enqueued",
                self.class_name
            );
            return self.handle_job_interrupted(py, job);
        }

        error!("Job execution error: {:?}", error);

        // Check if continuation has made progress (like Rails' resume_errors_after_advancing)
        // If the job has advanced, treat it like an interruption - resume from checkpoint
        // Read class attributes for configuration (like Rails' class attributes)
        let bound = self.handler.bind(py);

        // Get max_resumptions from class attribute (default None = use 25)
        let max_resumptions: i32 = bound
            .getattr("max_resumptions")
            .ok()
            .and_then(|attr| {
                if attr.is_none() {
                    None
                } else {
                    attr.extract::<i32>().ok()
                }
            })
            .unwrap_or(25); // Default to 25 like Rails

        // Get resume_errors_after_advancing from class attribute (default True)
        let resume_errors_after_advancing: bool = bound
            .getattr("resume_errors_after_advancing")
            .ok()
            .and_then(|attr| attr.extract::<bool>().ok())
            .unwrap_or(true);

        let (has_continuation_progress, current_resumptions) = self
            .continuation_info
            .as_ref()
            .map(|info| {
                // Check if state has any completed steps or current step with cursor
                let has_progress = !info.state.completed.is_empty() || info.state.current.is_some();
                (has_progress, info.resumptions)
            })
            .unwrap_or((false, 0));

        if resume_errors_after_advancing && has_continuation_progress {
            if current_resumptions >= max_resumptions {
                error!(
                    "Job `{}' exceeded max resumptions ({}), will not retry",
                    self.class_name, max_resumptions
                );
                // Fall through to normal error handling
            } else {
                info!(
                    "Job `{}' failed but has continuation progress (resumptions: {}), will resume from checkpoint",
                    self.class_name, current_resumptions
                );
                return self.handle_job_interrupted(py, job);
            }
        }

        // Check error handling strategies by priority

        // 1. Check if should retry
        let executions = crate::utils::get_executions(job.arguments.as_deref());
        if let Some(retry_strategy) = self.should_retry(py, &bound, error, executions)? {
            let error_type = error
                .get_type(py)
                .qualname()
                .map(|q| q.to_string())
                .unwrap_or_else(|_| "Unknown".to_string());
            return self.apply_retry_strategy(job, retry_strategy, &error_type);
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

    /// Handle JobInterrupted - re-enqueue with continuation state
    fn handle_job_interrupted(
        &mut self,
        _py: Python,
        job: &quebec_jobs::Model,
    ) -> Result<quebec_jobs::Model> {
        use crate::continuation::serialize_continuation_to_arguments;

        // Get the continuation state from the stored info (already updated by execute_python_task)
        let Some(cont_info) = self.continuation_info.take() else {
            warn!("JobInterrupted raised but no continuation info available");
            return Ok(job.clone());
        };

        // State was already captured in execute_python_task before the instance went out of scope
        // Just increment resumptions for the next run
        let final_state = cont_info.state;
        let resumptions = cont_info.resumptions + 1;

        // Serialize the updated continuation state back to arguments
        let new_arguments = serialize_continuation_to_arguments(
            &cont_info.original_args,
            &final_state,
            resumptions,
        );

        debug!(
            "Job will be re-enqueued with continuation state: completed={:?}, current={:?}",
            final_state.completed, final_state.current
        );

        // Set retry_info to trigger immediate re-enqueue (scheduled_at = now)
        // Don't increment executions for interruption - keep current arguments
        self.retry_info = Some(RetryInfo {
            scheduled_at: chrono::Utc::now().naive_utc(),
            arguments: job.arguments.clone().unwrap_or_default(),
        });

        // Update the continuation_info with new arguments for the re-enqueue
        self.continuation_info = Some(ContinuationInfo {
            state: final_state,
            resumptions,
            original_args: new_arguments,
        });

        Ok(job.clone())
    }

    /// Apply retry strategy
    fn apply_retry_strategy(
        &mut self,
        job: &quebec_jobs::Model,
        strategy: RetryStrategy,
        error_type: &str,
    ) -> Result<quebec_jobs::Model> {
        let executions = crate::utils::get_executions(job.arguments.as_deref());
        warn!("Job will be retried (attempt #{})", executions + 1);

        let delay = strategy.wait;
        let scheduled_at = chrono::Utc::now().naive_utc()
            + chrono::Duration::from_std(delay)
                .map_err(|e| anyhow::anyhow!("Invalid delay duration: {}", e))?;
        let new_arguments =
            crate::utils::increment_executions(job.arguments.as_deref(), Some(error_type));

        // Set retry information to runnable
        self.retry_info = Some(RetryInfo {
            scheduled_at,
            arguments: new_arguments,
        });

        Ok(job.clone())
    }

    /// Apply rescue strategy
    fn apply_rescue_strategy(
        &self,
        py: Python,
        job: &mut quebec_jobs::Model,
        strategy: &RescueStrategy,
        error: &PyErr,
    ) -> Result<quebec_jobs::Model> {
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
    fn handle_job_failure(
        &self,
        py: Python,
        job: &mut quebec_jobs::Model,
        error: &PyErr,
    ) -> Result<quebec_jobs::Model> {
        let error_type = error
            .get_type(py)
            .qualname()
            .map(|q| q.to_string())
            .unwrap_or_else(|_| "Unknown".to_string());
        job.arguments = Some(crate::utils::increment_executions(
            job.arguments.as_deref(),
            Some(&error_type),
        ));
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

    fn perform(&mut self, job: &mut quebec_jobs::Model) -> Result<quebec_jobs::Model> {
        // Direct Python invocation doesn't have cancellation context
        self.invoke(job, None)
    }

    fn __repr__(&self) -> String {
        format!(
            "Runnable(class_name={}, queue_as={}, priority={})",
            self.class_name, self.queue_as, self.priority
        )
    }
}

#[pyclass(name = "Metric", subclass, from_py_object)]
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
        format!(
            "Metric(id={}, success={}, duration={:?})",
            self.id, self.success, self.duration
        )
    }
}

#[pyclass(name = "Execution", subclass)]
#[derive(Debug)]
pub struct Execution {
    ctx: Arc<AppContext>,
    timer: Instant,
    tid: String,
    claimed: quebec_claimed_executions::Model,
    job: quebec_jobs::Model,
    runnable: Runnable,
    metric: Option<Metric>,
    retry_info: Option<RetryInfo>,
    /// Continuation info for interrupted jobs
    continuation_info: Option<ContinuationInfo>,
    /// Direct reference to idle notifier - avoids RwLock access in async context
    idle_notify: Option<Arc<tokio::sync::Notify>>,
}

impl Execution {
    pub fn new(
        ctx: Arc<AppContext>,
        claimed: quebec_claimed_executions::Model,
        job: quebec_jobs::Model,
        runnable: Runnable,
    ) -> Self {
        // Get current thread's ThreadId
        let thread_id = std::thread::current().id();

        // Convert ThreadId to string
        let thread_id_str = format!("{:?}", thread_id);

        // Extract numeric part (fallback to 0 if parsing fails)
        let thread_id_num: u64 = thread_id_str
            .trim_start_matches("ThreadId(")
            .trim_end_matches(")")
            .parse()
            .unwrap_or(0);
        Self {
            ctx,
            timer: Instant::now(),
            tid: format!("{}", thread_id_num),
            claimed,
            job,
            runnable,
            metric: None,
            retry_info: None,
            continuation_info: None,
            idle_notify: None,
        }
    }

    /// Create execution with idle notifier for on_idle wake-up
    pub fn with_idle_notify(
        ctx: Arc<AppContext>,
        claimed: quebec_claimed_executions::Model,
        job: quebec_jobs::Model,
        runnable: Runnable,
        idle_notify: Arc<tokio::sync::Notify>,
    ) -> Self {
        let mut exec = Self::new(ctx, claimed, job, runnable);
        exec.idle_notify = Some(idle_notify);
        exec
    }

    async fn invoke(&mut self) -> Result<quebec_jobs::Model> {
        self.timer = Instant::now();
        let mut job = self.job.clone();
        let jid = job.active_job_id.clone().unwrap_or_default();
        let span = tracing::info_span!(
            "runner",
            queue = job.queue_name,
            jid = jid,
            tid = self.tid.clone()
        );
        // Get cancellation token from context for continuation support
        let cancellation_token = Some(self.ctx.graceful_shutdown.clone());
        let result = async {
            let invoke_result = self.runnable.invoke(&mut job, cancellation_token);
            // Move retry information from runnable to execution
            if let Some(retry_info) = self.runnable.retry_info.take() {
                self.retry_info = Some(retry_info);
            }
            // Move continuation information from runnable to execution
            if let Some(cont_info) = self.runnable.continuation_info.take() {
                self.continuation_info = Some(cont_info);
            }
            invoke_result
        }
        .instrument(span.clone())
        .await;

        let failed = result.is_err();
        // Sync updated arguments (e.g. incremented executions) back to self.job
        // so after_executed can persist them to the database
        self.job.arguments = job.arguments.clone();
        let ret = self.after_executed(result).instrument(span).await;

        if failed {
            return ret;
        }

        Ok(job.clone())
    }

    /// Schedule a retry job with the given parameters
    /// If `override_arguments` is provided, it will be used instead of the job's original arguments
    /// (used for continuation support)
    async fn schedule_retry_job<C: ConnectionTrait>(
        txn: &C,
        table_config: &crate::context::TableConfig,
        job: &quebec_jobs::Model,
        scheduled_at: chrono::NaiveDateTime,
        arguments: &str,
        override_arguments: Option<&str>,
    ) -> Result<(), DbErr> {
        let arguments = override_arguments.unwrap_or(arguments);

        let new_job_id = query_builder::jobs::insert(
            txn,
            table_config,
            &job.queue_name,
            &job.class_name,
            Some(arguments),
            job.priority,
            job.active_job_id.as_deref(),
            Some(scheduled_at),
            job.concurrency_key.as_deref(),
        )
        .await?;

        query_builder::scheduled_executions::insert(
            txn,
            table_config,
            new_job_id,
            &job.queue_name,
            job.priority,
            scheduled_at,
        )
        .await?;

        info!("Retry job {} scheduled for {}", new_job_id, scheduled_at);
        Ok(())
    }

    async fn after_executed(
        &mut self,
        result: Result<quebec_jobs::Model>,
    ) -> Result<quebec_jobs::Model> {
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
                error!(
                    "Job `{}' failed in: {:?}",
                    self.runnable.class_name, eplased
                );
            }

            let metric = Metric {
                id: job_id,
                success: result.is_ok(),
                duration: eplased,
            };
            self.metric = Some(metric);
        }
        .await;

        let db = self.ctx.get_db().await;
        let failed = result.is_err();
        let err = result.as_ref().err().map(|e| e.to_string());

        // Check if retry is needed (determined by execution.retry_info)
        let retry_job_data = self
            .retry_info
            .as_ref()
            .map(|info| (info.scheduled_at, info.arguments.clone()));

        // Get continuation arguments if available (for interrupted jobs)
        let continuation_arguments = self
            .continuation_info
            .as_ref()
            .map(|info| serde_json::to_string(&info.original_args).unwrap_or_default());

        // Capture concurrency info for semaphore release
        let concurrency_key = job.concurrency_key.clone();
        let concurrency_limit = self.runnable.concurrency_limit.unwrap_or(1);
        let concurrency_duration = self
            .runnable
            .concurrency_duration
            .map(|s| chrono::Duration::seconds(s as i64));
        let table_config = self.ctx.table_config.clone();
        let ctx = self.ctx.clone();

        let transaction_result = db
            .transaction::<_, quebec_jobs::Model, DbErr>(|txn| {
                let continuation_arguments = continuation_arguments.clone();
                Box::pin(async move {
                    let claimed_id = claimed.id;

                    if let Some((scheduled_at, ref arguments)) = retry_job_data {
                        Self::schedule_retry_job(
                            txn,
                            &table_config,
                            &job,
                            scheduled_at,
                            arguments,
                            continuation_arguments.as_deref(),
                        )
                        .await?;
                    }

                    if failed {
                        error!("Job failed: {:?}", err);
                        // Persist updated arguments (executions/exception_executions counters)
                        if let Some(ref args) = job.arguments {
                            query_builder::jobs::update_arguments(txn, &table_config, job_id, args)
                                .await?;
                        }
                        // Write to failed_executions table
                        // Note: Like Solid Queue, failed jobs do NOT get finished_at set
                        query_builder::failed_executions::insert(
                            txn,
                            &table_config,
                            job_id,
                            err.map(|e| e.to_string()).as_deref(),
                        )
                        .await?;
                    } else {
                        // Only mark as finished for successful jobs (like Solid Queue)
                        query_builder::jobs::mark_finished(txn, &table_config, job_id).await?;
                    }

                    // Directly delete record from claimed_executions table
                    let delete_result = query_builder::claimed_executions::delete_by_id(
                        txn,
                        &table_config,
                        claimed_id,
                    )
                    .await?;

                    if delete_result == 0 {
                        return Err(DbErr::Custom("Claimed job not found".into()));
                    }

                    // Fetch updated job model
                    let updated = query_builder::jobs::find_by_id(txn, &table_config, job_id)
                        .await?
                        .ok_or_else(|| DbErr::Custom("Job not found after update".to_string()))?;

                    // Release semaphore and unblock next job if job has concurrency control
                    // This must happen AFTER job execution completes (ensure block in Solid Queue)
                    if let Some(key) = concurrency_key.as_ref().filter(|k| !k.is_empty()) {
                        // Step 1: Release semaphore (increment value)
                        let released = release_semaphore(
                            txn,
                            &table_config,
                            key.clone(),
                            concurrency_limit,
                            concurrency_duration,
                        )
                        .await
                        .inspect(|&released| {
                            if released {
                                trace!("Released semaphore for key: {}", key);
                            }
                        })
                        .inspect_err(|e| {
                            warn!("Failed to release semaphore for key {}: {:?}", key, e)
                        })
                        .unwrap_or(false);

                        // Step 2: Immediately try to release next blocked job (like Solid Queue)
                        if released {
                            Worker::release_next_blocked_job(&ctx, txn, key, concurrency_limit)
                                .await
                                .inspect_err(|e| {
                                    warn!(
                                        "Failed to release next blocked job for key {}: {:?}",
                                        key, e
                                    )
                                })
                                .ok();
                        }
                    }

                    Ok(updated)
                })
            })
            .await;

        // Log the result
        let transaction_result = transaction_result
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
                error!("Job processing failed in {:?}: {:?}", duration, e);
                e
            });

        // Notify main loop that this thread is now idle and ready for new jobs
        // Uses direct Arc<Notify> reference to avoid RwLock access in async context
        if let Some(ref notify) = self.idle_notify {
            trace!("Execution: notifying idle");
            notify.notify_one();
        }

        // Return success after processing, even if the job failed
        // Job failures are recorded in failed_executions, not propagated as errors
        // Only return error if the database transaction failed
        match transaction_result {
            Ok(_) => Ok(self.job.clone()),
            Err(e) => Err(anyhow::anyhow!(
                "Database error during job processing: {}",
                e
            )),
        }
    }
}

#[pymethods]
impl Execution {
    #[getter]
    fn get_id(&self) -> i64 {
        self.job.id
    }

    #[getter]
    fn get_job(&self) -> quebec_jobs::Model {
        self.job.clone()
    }

    #[getter]
    fn get_jid(&self) -> String {
        self.job.active_job_id.clone().unwrap_or_default()
    }

    #[getter]
    fn get_queue(&self) -> String {
        self.job.queue_name.clone()
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
        Python::attach(|py| self.runnable.clone_with_gil(py))
    }

    fn perform(&mut self, py: Python<'_>) -> PyResult<()> {
        // Reuse the shared runtime handle when blocking on async work
        let handle = self.ctx.get_runtime_handle().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Tokio runtime handle not initialized",
            )
        })?;

        // CRITICAL: Release GIL during block_on to avoid deadlock!
        // The async code calls notify_idle() which wakes the main loop,
        // and the main loop needs GIL for Python::attach() calls.
        let ret = py.detach(|| handle.block_on(async { self.invoke().await }));

        if let Err(e) = ret {
            return Err(e.into());
        }

        Ok(())
    }

    fn post(&mut self, py: Python, exc: &Bound<'_, PyAny>, traceback: &str) {
        let result = if !exc.is_instance_of::<PyException>() {
            Ok(self.job.clone())
        } else {
            let Some(e) = exc.cast::<PyException>().ok() else {
                error!("Failed to downcast exception");
                return;
            };

            let exception_class = e
                .get_type()
                .str()
                .map(|s| s.to_string())
                .unwrap_or_else(|_| "Unknown".to_string());

            error!("error: {:?}", e);
            error!("error_type: {}", exception_class);
            error!("error_description: {}", e);

            let backtrace: Vec<_> = traceback.lines().map(String::from).collect();
            let error_payload = serde_json::json!({
                "exception_class": exception_class,
                "message": e.to_string(),
                "backtrace": backtrace,
            });

            Err(anyhow::anyhow!(serde_json::to_string(&error_payload)
                .unwrap_or_else(
                    |_| "Error serialization failed".to_string()
                )))
        };

        py.detach(|| {
            let Some(handle) = self.ctx.get_runtime_handle() else {
                error!("Tokio runtime handle not initialized; cannot post job result");
                return;
            };
            handle.block_on(async move {
                let ret = self.after_executed(result).await;
                debug!("Job result post: {:?}", ret);
            });
        });
    }

    fn retry(
        &mut self,
        py: Python,
        strategy: RetryStrategy,
        exc: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let job = self.job.clone();
        let scheduled_at = chrono::Utc::now().naive_utc() + strategy.wait();
        let e = exc.cast::<PyException>()?;
        let error_type = e
            .get_type()
            .qualname()
            .map(|q| q.to_string())
            .unwrap_or_else(|_| "Unknown".to_string());

        let executions = crate::utils::get_executions(job.arguments.as_deref());
        let new_arguments =
            crate::utils::increment_executions(job.arguments.as_deref(), Some(&error_type));

        let jid = job.active_job_id.clone().unwrap_or_default();
        let span = tracing::info_span!(
            "runner",
            queue = job.queue_name,
            jid = jid,
            tid = self.tid.clone()
        );
        let _enter = span.enter();

        if (executions as i64) >= strategy.attempts {
            error!(
                "Job `{}' failed after {} attempts",
                job.class_name, executions
            );
            return Ok(());
        }

        py.detach(|| {
            let retry_future = async {
                warn!(
                    "Attempt {} scheduled due to `{}' on {:?}",
                    format!("#{}", executions + 1).bright_purple(),
                    error_type,
                    scheduled_at
                );

                let db = self.ctx.get_db().await;
                let table_config = self.ctx.table_config.clone();
                db.transaction::<_, (), DbErr>(|txn| {
                    let table_config = table_config.clone();
                    let queue_name = job.queue_name.clone();
                    let class_name = job.class_name.clone();
                    let new_arguments = new_arguments.clone();
                    let priority = job.priority;
                    let active_job_id = job.active_job_id.clone();
                    let concurrency_key = job.concurrency_key.clone().unwrap_or_default();

                    Box::pin(async move {
                        let job_id = query_builder::jobs::insert(
                            txn,
                            &table_config,
                            &queue_name,
                            &class_name,
                            Some(&new_arguments),
                            priority,
                            active_job_id.as_deref(),
                            Some(scheduled_at),
                            Some(&concurrency_key),
                        )
                        .await?;

                        query_builder::scheduled_executions::insert(
                            txn,
                            &table_config,
                            job_id,
                            &queue_name,
                            priority,
                            scheduled_at,
                        )
                        .await?;

                        Ok(())
                    })
                })
                .await
            }
            .instrument(span.clone());

            let job = self
                .ctx
                .get_runtime_handle()
                .map(|h| h.block_on(retry_future))
                .unwrap_or_else(|| {
                    Err(sea_orm::TransactionError::Connection(DbErr::Custom(
                        "Runtime handle unavailable".into(),
                    )))
                });

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
    /// Notify for idle wake-up - when a worker thread finishes a job
    idle_notify: Arc<tokio::sync::Notify>,
    /// Process ID for this worker (set after on_start)
    process_id: Arc<tokio::sync::Mutex<Option<i64>>>,
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

        // Idle notification - simple notify for wake-up, no data needed
        let idle_notify = Arc::new(tokio::sync::Notify::new());

        // Register idle_notify with AppContext so Execution can use it
        ctx.set_idle_notify(idle_notify.clone());

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
            idle_notify,
            process_id: Arc::new(tokio::sync::Mutex::new(None)),
        }
    }

    fn register_handler(
        &self,
        py: Python,
        handler: Py<PyAny>,
        handlers: &RwLock<Vec<Py<PyAny>>>,
        handler_type: &str,
    ) -> PyResult<()> {
        if !handler.bind(py).is_callable() {
            return Err(PyTypeError::new_err(format!(
                "Expected a callable object for worker {} handler",
                handler_type
            )));
        }
        if let Ok(mut h) = handlers.write() {
            h.push(handler.clone());
        }
        debug!("Worker {} handler: {:?} registered", handler_type, handler);
        Ok(())
    }

    pub fn register_start_handler(&self, py: Python, handler: Py<PyAny>) -> PyResult<()> {
        self.register_handler(py, handler, &self.start_handlers, "start")
    }

    pub fn register_stop_handler(&self, py: Python, handler: Py<PyAny>) -> PyResult<()> {
        self.register_handler(py, handler, &self.stop_handlers, "stop")
    }

    pub fn register_job_class(&self, _py: Python, klass: Py<PyAny>) -> PyResult<()> {
        Python::attach(|py| -> PyResult<()> {
            let bound = klass.bind(py);
            let class_name = bound.cast::<PyType>()?.qualname()?;
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
                concurrency_duration =
                    Some(bound.getattr("concurrency_duration")?.extract::<i32>()?);
            }

            // Check if job has concurrency control (concurrency_key attribute exists and is not None/empty)
            let has_concurrency_control = (|| -> PyResult<bool> {
                if !bound.hasattr("concurrency_key")? {
                    return Ok(false);
                }
                let attr = bound.getattr("concurrency_key")?;
                if attr.is_none() {
                    return Ok(false);
                }
                if attr.is_callable() {
                    return Ok(true); // Method implies concurrency control
                }
                // Property: check if not empty string
                Ok(attr
                    .extract::<String>()
                    .map(|k| !k.is_empty())
                    .unwrap_or(false))
            })()
            .unwrap_or(false);

            let runnable = Runnable {
                class_name: class_name.to_string(),
                handler: klass,
                queue_as: queue_name,
                priority: 0,
                retry_info: None,
                concurrency_limit,
                concurrency_duration,
                continuation_info: None,
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

    /// Helper: claim a single execution (insert claimed, delete ready, return model)
    async fn claim_execution<C: ConnectionTrait>(
        txn: &C,
        table_config: &crate::context::TableConfig,
        execution: &quebec_ready_executions::Model,
        process_id: Option<i64>,
    ) -> Result<Option<quebec_claimed_executions::Model>, DbErr> {
        query_builder::claimed_executions::insert(txn, table_config, execution.job_id, process_id)
            .await?;
        query_builder::ready_executions::delete_by_id(txn, table_config, execution.id).await?;
        query_builder::claimed_executions::find_by_job_id(txn, table_config, execution.job_id).await
    }

    pub async fn claim_job(&self) -> Result<quebec_claimed_executions::Model, anyhow::Error> {
        let db = self.ctx.get_db().await;
        let table_config = self.ctx.table_config.clone();
        let queue_selector = self.ctx.worker_queues.clone();
        let use_skip_locked = self.ctx.use_skip_locked;
        let process_id = *self.process_id.lock().await;

        let job = db
            .transaction::<_, quebec_claimed_executions::Model, DbErr>(|txn| {
                let table_config = table_config.clone();
                Box::pin(async move {
                    let paused_queues =
                        query_builder::pauses::find_all_queue_names(txn, &table_config).await?;

                    if !paused_queues.is_empty() {
                        debug!("Paused queues: {:?}", paused_queues);
                    }

                    let queue_patterns = queue_selector
                        .as_ref()
                        .map(|q| q.ordered_patterns())
                        .unwrap_or_else(|| vec![(false, "*".to_string())]);

                    // Helper macro to avoid duplicating find + claim logic
                    macro_rules! try_claim_one {
                        ($filter:expr, $exclude:expr) => {{
                            let record = query_builder::ready_executions::find_one_for_update(
                                txn,
                                &table_config,
                                $filter,
                                $exclude,
                                use_skip_locked,
                            )
                            .await?;
                            if let Some(ref execution) = record {
                                if let Some(claimed) =
                                    Self::claim_execution(txn, &table_config, execution, process_id)
                                        .await?
                                {
                                    return Ok(claimed);
                                }
                            }
                        }};
                    }

                    for (is_wildcard, pattern) in queue_patterns {
                        if is_wildcard {
                            let matching_queues =
                                query_builder::ready_executions::find_matching_queue_names(
                                    txn,
                                    &table_config,
                                    &pattern,
                                )
                                .await?;

                            for queue_name in &matching_queues {
                                if paused_queues.contains(queue_name) {
                                    continue;
                                }
                                try_claim_one!(Some(queue_name.as_str()), &[]);
                            }
                        } else {
                            if pattern != "*" && paused_queues.contains(&pattern) {
                                continue;
                            }
                            if pattern != "*" {
                                try_claim_one!(Some(pattern.as_str()), &[]);
                            } else {
                                try_claim_one!(None, &paused_queues);
                            }
                        }
                    }

                    Err(DbErr::Custom("No job found".into()))
                })
            })
            .await?;

        Ok(job)
    }

    pub async fn claim_jobs(
        &self,
        batch_size: u64,
    ) -> Result<Vec<quebec_claimed_executions::Model>, anyhow::Error> {
        let timer = Instant::now();
        let db = self.ctx.get_db().await;
        let use_skip_locked = self.ctx.use_skip_locked;
        let table_config = self.ctx.table_config.clone();
        let queue_selector = self.ctx.worker_queues.clone();
        let process_id = *self.process_id.lock().await;

        let jobs = db
            .transaction::<_, Vec<quebec_claimed_executions::Model>, DbErr>(|txn| {
                let table_config = table_config.clone();
                Box::pin(async move {
                    // Get list of paused queues
                    let paused_queues =
                        query_builder::pauses::find_all_queue_names(txn, &table_config).await?;

                    if !paused_queues.is_empty() {
                        debug!("Paused queues: {:?}", paused_queues);
                    }

                    let mut claimed_jobs = Vec::new();
                    let mut remaining = batch_size;

                    // Get ordered queue patterns from selector
                    // This preserves Solid Queue's queue ordering semantics
                    let queue_patterns = queue_selector
                        .as_ref()
                        .map(|q| q.ordered_patterns())
                        .unwrap_or_else(|| vec![(false, "*".to_string())]); // Default to all

                    // Helper macro to claim jobs from a queue
                    macro_rules! claim_from_queue {
                        ($filter:expr, $exclude:expr) => {{
                            let records = query_builder::ready_executions::find_many_for_update(
                                txn,
                                &table_config,
                                $filter,
                                $exclude,
                                use_skip_locked,
                                remaining,
                            )
                            .await?;

                            for execution in records {
                                if let Some(claimed) = Self::claim_execution(
                                    txn,
                                    &table_config,
                                    &execution,
                                    process_id,
                                )
                                .await?
                                {
                                    claimed_jobs.push(claimed);
                                }
                                remaining = remaining.saturating_sub(1);
                            }
                        }};
                    }

                    for (is_wildcard, pattern) in queue_patterns {
                        if remaining == 0 {
                            break;
                        }

                        if is_wildcard {
                            let matching_queues =
                                query_builder::ready_executions::find_matching_queue_names(
                                    txn,
                                    &table_config,
                                    &pattern,
                                )
                                .await?;

                            for queue_name in &matching_queues {
                                if remaining == 0 {
                                    break;
                                }
                                if paused_queues.contains(queue_name) {
                                    continue;
                                }
                                claim_from_queue!(Some(queue_name.as_str()), &[]);
                            }
                        } else {
                            if pattern != "*" && paused_queues.contains(&pattern) {
                                continue;
                            }
                            if pattern != "*" {
                                claim_from_queue!(Some(pattern.as_str()), &[]);
                            } else {
                                claim_from_queue!(None, &paused_queues);
                            }
                        }
                    }

                    Ok(claimed_jobs)
                })
            })
            .await?;

        trace!("elpased: {:?}", timer.elapsed());
        Ok(jobs)
    }

    /// Check if this worker should handle the notify message for the given queue
    fn should_handle_notify(&self, msg: &str) -> bool {
        let notify_msg = match serde_json::from_str::<crate::notify::NotifyMessage>(msg) {
            Ok(m) => m,
            Err(e) => {
                warn!("Failed to parse NOTIFY message '{}': {}", msg, e);
                return true; // If we can't parse, process anyway to be safe
            }
        };

        trace!("Received NOTIFY for queue: {}", notify_msg.queue);

        let Some(ref queues) = self.ctx.worker_queues else {
            return true; // No queue config, process all queues
        };

        if queues.is_all() {
            return true;
        }

        let exact = queues.exact_names();
        let wildcards = queues.wildcard_prefixes();

        exact.contains(&notify_msg.queue)
            || wildcards
                .iter()
                .any(|prefix| notify_msg.queue.starts_with(prefix))
    }

    /// Drain pending notify messages from the channel, return count drained
    fn drain_pending_notifies(
        notify_rx: &mut Option<tokio::sync::mpsc::Receiver<String>>,
    ) -> usize {
        let mut count = 0;
        while let Some(rx) = notify_rx.as_mut() {
            if rx.try_recv().is_err() {
                break;
            }
            count += 1;
        }
        count
    }

    /// Fail all orphaned claimed executions (jobs claimed by non-existent processes)
    /// Called at startup to clean up jobs left by crashed workers
    async fn fail_orphaned_executions(&self) -> Result<usize, anyhow::Error> {
        let db = self.ctx.get_db().await;
        let table_config = self.ctx.table_config.clone();

        let orphaned =
            query_builder::claimed_executions::find_orphaned(db.as_ref(), &table_config).await?;

        if orphaned.is_empty() {
            return Ok(0);
        }

        let count = orphaned.len();
        info!(
            "Found {} orphaned claimed execution(s) from crashed workers, marking as failed",
            count
        );

        // Process each orphaned execution in a transaction (like Solid Queue's failed_with)
        for execution in orphaned {
            let table_config = table_config.clone();
            let job_id = execution.job_id;
            let execution_id = execution.id;
            let process_id = execution.process_id;

            db.transaction::<_, (), DbErr>(|txn| {
                Box::pin(async move {
                    // Insert into failed_executions with error message
                    query_builder::failed_executions::insert(
                        txn,
                        &table_config,
                        job_id,
                        Some("Process crashed or was killed before job completion"),
                    )
                    .await?;

                    // Delete from claimed_executions
                    query_builder::claimed_executions::delete_by_id(
                        txn,
                        &table_config,
                        execution_id,
                    )
                    .await?;

                    Ok(())
                })
            })
            .await?;

            debug!(
                "Marked orphaned job {} as failed (was claimed by process {:?})",
                job_id, process_id
            );
        }

        Ok(count)
    }

    /// Prune dead processes and fail their claimed executions
    /// Called periodically to clean up stale processes
    async fn prune_dead_processes(
        &self,
        exclude_process_id: Option<i64>,
    ) -> Result<usize, anyhow::Error> {
        let db = self.ctx.get_db().await;
        let table_config = self.ctx.table_config.clone();

        // Calculate threshold: processes with heartbeat older than this are considered dead
        // Use 3x heartbeat interval as threshold (same as Solid Queue's default)
        let threshold = chrono::Utc::now().naive_utc()
            - chrono::Duration::from_std(self.ctx.process_heartbeat_interval * 3)?;

        let stale_processes = query_builder::processes::find_prunable(
            db.as_ref(),
            &table_config,
            threshold,
            exclude_process_id,
        )
        .await?;

        if stale_processes.is_empty() {
            return Ok(0);
        }

        let count = stale_processes.len();
        info!(
            "Found {} stale process(es) (no heartbeat since {}), pruning",
            count, threshold
        );

        for process in stale_processes {
            let table_config = table_config.clone();
            let process_id = process.id;
            let process_pid = process.pid;
            let process_hostname = process.hostname.clone();
            let error_msg = format!(
                "Worker process {} (pid={}, host={:?}) stopped responding",
                process_id, process_pid, process_hostname
            );

            // Wrap all operations for this process in a transaction
            let deleted_count = db
                .transaction::<_, u64, DbErr>(|txn| {
                    let table_config = table_config.clone();
                    let error_msg = error_msg.clone();
                    Box::pin(async move {
                        // Find all claimed executions for this process
                        let claimed = query_builder::claimed_executions::find_by_process_id(
                            txn,
                            &table_config,
                            process_id,
                        )
                        .await?;

                        // Fail each claimed execution
                        for execution in &claimed {
                            query_builder::failed_executions::insert(
                                txn,
                                &table_config,
                                execution.job_id,
                                Some(&error_msg),
                            )
                            .await?;
                        }

                        // Delete all claimed executions for this process
                        let deleted_count =
                            query_builder::claimed_executions::delete_by_process_id(
                                txn,
                                &table_config,
                                process_id,
                            )
                            .await?;

                        // Delete the stale process
                        query_builder::processes::prune(txn, &table_config, process_id).await?;

                        Ok(deleted_count)
                    })
                })
                .await?;

            warn!(
                "Pruned stale process {} (pid={}, host={:?}), failed {} claimed job(s)",
                process_id, process_pid, process_hostname, deleted_count
            );
        }

        Ok(count)
    }

    /// Clear finished jobs older than the configured threshold.
    /// Called periodically to clean up completed jobs.
    /// Returns the total number of jobs deleted.
    async fn clear_finished_jobs(&self) -> Result<u64, anyhow::Error> {
        // Skip if preserve_finished_jobs is false (jobs are deleted immediately after completion)
        if !self.ctx.preserve_finished_jobs {
            return Ok(0);
        }

        let db = self.ctx.get_db().await;
        let table_config = self.ctx.table_config.clone();
        let batch_size = self.ctx.cleanup_batch_size;
        let clear_after = self.ctx.clear_finished_jobs_after;
        let graceful_shutdown = self.ctx.graceful_shutdown.clone();

        // Calculate the cutoff timestamp
        let finished_before = chrono::Utc::now().naive_utc()
            - chrono::Duration::from_std(clear_after).map_err(|e| {
                anyhow::anyhow!("Invalid clear_finished_jobs_after duration: {}", e)
            })?;

        let mut total_deleted: u64 = 0;

        // Loop: delete batch → sleep → repeat until 0 deleted or shutdown requested
        loop {
            // Check for graceful shutdown between batches
            if graceful_shutdown.is_cancelled() {
                debug!("Clear finished jobs interrupted by shutdown");
                break;
            }

            let deleted = query_builder::jobs::delete_finished_before(
                db.as_ref(),
                &table_config,
                finished_before,
                batch_size,
            )
            .await?;

            total_deleted += deleted;

            if deleted == 0 {
                break;
            }

            debug!(
                "Cleared {} finished job(s) (total: {})",
                deleted, total_deleted
            );

            // Sleep briefly between batches to reduce database pressure
            tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;
        }

        if total_deleted > 0 {
            info!(
                "Cleared {} finished job(s) older than {:?}",
                total_deleted, clear_after
            );
        }

        Ok(total_deleted)
    }

    /// Release all claimed executions for a process back to ready state.
    /// Used for maintenance/admin purposes - re-queues jobs for other workers.
    /// Unlike fail_orphaned_executions, this does NOT mark jobs as failed.
    #[allow(dead_code)]
    async fn release_all_claimed_executions(
        &self,
        process_id: i64,
    ) -> Result<usize, anyhow::Error> {
        let db = self.ctx.get_db().await;
        let table_config = self.ctx.table_config.clone();

        let claimed = query_builder::claimed_executions::find_by_process_id(
            db.as_ref(),
            &table_config,
            process_id,
        )
        .await?;

        if claimed.is_empty() {
            return Ok(0);
        }

        let count = claimed.len();
        info!("Releasing {} claimed job(s) back to ready state", count);

        for execution in claimed {
            // Get the job to retrieve queue_name and priority
            if let Some(job) =
                query_builder::jobs::find_by_id(db.as_ref(), &table_config, execution.job_id)
                    .await?
            {
                // Re-insert into ready_executions (bypass concurrency limits like Solid Queue)
                query_builder::ready_executions::insert(
                    db.as_ref(),
                    &table_config,
                    job.id,
                    &job.queue_name,
                    job.priority,
                )
                .await?;

                debug!(
                    "Released job {} back to ready state (queue: {})",
                    job.id, job.queue_name
                );
            }

            // Delete from claimed_executions
            query_builder::claimed_executions::delete_by_id(
                db.as_ref(),
                &table_config,
                execution.id,
            )
            .await?;
        }

        Ok(count)
    }

    /// Release the next blocked job for a given concurrency key.
    /// Called after a job completes to immediately unblock waiting jobs.
    /// This matches Solid Queue's `unblock_next_blocked_job` behavior.
    async fn release_next_blocked_job<C>(
        ctx: &Arc<AppContext>,
        db: &C,
        concurrency_key: &str,
        concurrency_limit: i32,
    ) -> Result<bool, DbErr>
    where
        C: ConnectionTrait,
    {
        use crate::semaphore::acquire_semaphore;

        let table_config = &ctx.table_config;

        // Find the next blocked execution for this concurrency key (with FOR UPDATE SKIP LOCKED)
        let blocked = query_builder::blocked_executions::find_one_by_key_for_update(
            db,
            table_config,
            concurrency_key,
        )
        .await?;

        let Some(execution) = blocked else {
            // No blocked jobs waiting for this key
            return Ok(false);
        };

        // Get the job first to access class_name for runnable lookup
        let Some(job) = query_builder::jobs::find_by_id(db, table_config, execution.job_id).await?
        else {
            warn!(
                "Job {} not found for blocked execution {}",
                execution.job_id, execution.id
            );
            return Ok(false);
        };

        // Get concurrency_duration from the runnable (like Solid Queue's job.concurrency_duration)
        let concurrency_duration = Python::attach(|_py| {
            ctx.get_runnable(&job.class_name)
                .ok()
                .and_then(|r| r.concurrency_duration)
                .map(|s| chrono::Duration::seconds(s as i64))
        });

        // Try to acquire semaphore for this blocked job
        let acquired = acquire_semaphore(
            db,
            table_config,
            concurrency_key.to_string(),
            concurrency_limit,
            concurrency_duration,
        )
        .await?;

        if !acquired {
            // Semaphore not available (shouldn't happen normally since we just released one)
            trace!(
                "Could not acquire semaphore for blocked job {} (key: {})",
                execution.job_id,
                concurrency_key
            );
            return Ok(false);
        }

        // Move to ready_executions
        query_builder::ready_executions::insert(
            db,
            table_config,
            job.id,
            &job.queue_name,
            job.priority,
        )
        .await?;

        // Delete from blocked_executions
        query_builder::blocked_executions::delete_by_id(db, table_config, execution.id).await?;

        debug!(
            "Released blocked job {} to ready state (key: {})",
            job.id, concurrency_key
        );

        Ok(true)
    }

    /// Set up PostgreSQL LISTEN/NOTIFY for immediate job notifications
    async fn setup_notify_listener(&self) -> Option<tokio::sync::mpsc::Receiver<String>> {
        if !self.ctx.is_postgres() {
            info!("Non-PostgreSQL database detected, using polling only");
            return None;
        }

        info!("PostgreSQL detected, enabling LISTEN/NOTIFY for immediate job processing");
        let notify_manager = NotifyManager::new(self.ctx.clone());

        match notify_manager.start_listener().await {
            Ok(rx) => {
                info!(
                    "LISTEN started on '{}_jobs' channel - jobs will be processed immediately when notified",
                    self.ctx.name
                );
                Some(rx)
            }
            Err(e) => {
                warn!(
                    "Failed to start real LISTEN, falling back to polling only: {}",
                    e
                );
                None
            }
        }
    }

    pub async fn run_main_loop(&self) -> Result<(), anyhow::Error> {
        // Don't acquire long-term connections here, get them when needed
        let mut polling_interval = tokio::time::interval(self.ctx.worker_polling_interval);
        let mut heartbeat_interval = tokio::time::interval(self.ctx.process_heartbeat_interval);
        // Maintenance interval: 3x heartbeat interval (same as Solid Queue's process_alive_threshold)
        let mut maintenance_interval =
            tokio::time::interval(self.ctx.process_heartbeat_interval * 3);
        // Cleanup interval for clearing finished jobs (disabled if zero)
        let cleanup_enabled = !self.ctx.cleanup_interval.is_zero();
        let cleanup_duration = if cleanup_enabled {
            self.ctx
                .cleanup_interval
                .max(std::time::Duration::from_secs(1))
        } else {
            std::time::Duration::from_secs(3600) // dummy interval when disabled
        };
        let mut cleanup_interval = tokio::time::interval(cleanup_duration);
        let worker_threads = self.ctx.worker_threads;
        let tx = self.dispatch_sender.clone();
        let ctx = self.ctx.clone();

        // Ensure only one main loop is running
        let _ = self.polling.lock().await;
        let thread_id = Self::get_tid();

        let quit = self.ctx.graceful_shutdown.clone();

        // Set process title for visibility in htop/ps
        self.ctx
            .set_proc_title("worker", Some(&format!("{}", worker_threads)));

        // Initialize process record
        let init_db = self.ctx.get_db().await;
        let process = self.on_start(&init_db).await?;
        info!(">> Process started: {:?}", process);

        // Store process_id for use in claim_jobs
        *self.process_id.lock().await = Some(process.id);

        // Clean up orphaned executions from crashed workers at startup
        match self.fail_orphaned_executions().await {
            Ok(count) if count > 0 => {
                info!("Cleaned up {} orphaned job(s) at startup", count);
            }
            Err(e) => {
                warn!("Failed to clean up orphaned executions at startup: {}", e);
            }
            _ => {}
        }

        let mut notify_rx = self.setup_notify_listener().await;

        // Get idle notifier for on_idle wake-up
        let idle_notify = self.idle_notify.clone();

        // Call worker start handlers before starting the main loop
        Python::attach(|py| {
            let handlers = self.start_handlers.read().expect("Lock poisoned");
            for handler in handlers.iter() {
                match handler.bind(py).call0() {
                    Ok(_) => debug!("Worker start handler executed successfully in main loop"),
                    Err(e) => error!("Error calling worker start handler in main loop: {:?}", e),
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
                // Periodic maintenance: prune dead processes and fail their claimed jobs
                _ = maintenance_interval.tick() => {
                    if let Err(e) = self.prune_dead_processes(Some(process.id)).await {
                        warn!("Failed to prune dead processes: {}", e);
                    }
                }
                // Periodic cleanup: clear finished jobs older than threshold (if enabled)
                _ = cleanup_interval.tick(), if cleanup_enabled => {
                    if let Err(e) = self.clear_finished_jobs().await {
                        warn!("Failed to clear finished jobs: {}", e);
                    }
                }
                _ = quit.cancelled() => {
                    info!("Graceful shutdown, stop polling");

                    // Call worker stop handlers before exiting
                    Python::attach(|py| {
                        let handlers = self.stop_handlers.read().expect("Lock poisoned");
                        for handler in handlers.iter() {
                            match handler.bind(py).call0() {
                                Ok(_) => debug!("Worker stop handler executed successfully in main loop"),
                                Err(e) => error!("Error calling worker stop handler in main loop: {:?}", e)
                            }
                        }
                    });

                    // NOTE: Do NOT release claimed executions here!
                    // Like Solid Queue, we wait for runner threads to complete their current jobs.
                    // Python side calls t.join() to wait for all runners.
                    // Jobs complete normally via after_executed(), which cleans up claimed_executions.
                    // Only orphaned executions (from crashed workers) are released via prune_dead_processes.

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
                    if let Some(msg) = notify_msg {
                        if self.should_handle_notify(&msg) {
                            let total_notifies = 1 + Self::drain_pending_notifies(&mut notify_rx);

                            async { debug!("Processing jobs immediately (batched {} notifications)", total_notifies); }
                                .instrument(tracing::info_span!("listener", consumed = total_notifies))
                                .await;

                            let process_future = self.process_available_jobs(worker_threads, &tx, &ctx, &thread_id, "NOTIFY");
                            let timeout_duration = tokio::time::Duration::from_secs(1);

                            if tokio::time::timeout(timeout_duration, process_future).await.is_err() {
                                warn!("NOTIFY job processing timed out after {}ms - will rely on polling", timeout_duration.as_millis());
                            }
                        } else {
                            trace!("Ignoring NOTIFY for queue not in worker config");
                        }
                    }
                }
                // Handle idle notifications from worker threads - wake up immediately when a thread finishes a job
                _ = idle_notify.notified() => {
                    trace!("Worker thread idle, checking for new jobs");
                    self.process_available_jobs(worker_threads, &tx, &ctx, &thread_id, "IDLE").await;
                }
                _ = polling_interval.tick() => {
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
        let claimed = self
            .claim_jobs(worker_threads)
            .instrument(tracing::info_span!("polling", tid = thread_id))
            .await;

        if let Err(e) = claimed {
            debug!("[{}] no job found: {:?}", source, e);
            return;
        }

        let claimed = match claimed {
            Ok(c) => c,
            Err(_) => return,
        };
        if claimed.is_empty() {
            return;
        }

        async {
            debug!("found {} job(s) to process", claimed.len());
        }
        .instrument(tracing::info_span!(
            "polling",
            tid = thread_id,
            source = source
        ))
        .await;

        // Process claimed jobs without holding a long transaction
        // First collect all job data quickly, then create executions
        let job_ids: Vec<i64> = claimed.iter().map(|row| row.job_id).collect();
        let table_config = ctx.table_config.clone();
        let job_data = {
            let processing_db = ctx.get_db().await;
            // Use a single quick transaction to fetch all job data
            let jobs_result = processing_db
                .transaction::<_, Vec<quebec_jobs::Model>, DbErr>(|txn| {
                    let table_config = table_config.clone();
                    Box::pin(async move {
                        query_builder::jobs::find_by_ids(txn, &table_config, job_ids).await
                    })
                })
                .await;

            match jobs_result {
                Ok(jobs) => jobs,
                Err(e) => {
                    error!("Failed to fetch job data: {:?}", e);
                    return;
                }
            }
        }; // processing_db is released here

        // Build a HashMap to match jobs by ID, since find_by_ids doesn't guarantee order
        let job_map: std::collections::HashMap<i64, quebec_jobs::Model> =
            job_data.into_iter().map(|job| (job.id, job)).collect();

        // Now create executions without holding any database connections
        for row in claimed.into_iter() {
            // Look up the job by ID from the HashMap
            let job = match job_map.get(&row.job_id) {
                Some(j) => j.clone(),
                None => {
                    error!("Job not found for claimed execution: job_id={}", row.job_id);
                    continue;
                }
            };

            // Get runnable with proper GIL handling
            let runnable = Python::attach(|_py| self.ctx.get_runnable(&job.class_name));

            if runnable.is_err() {
                error!("Job handler not found: {:?}", &job.class_name);
                continue;
            }

            let execution = match runnable {
                Ok(r) => {
                    Execution::with_idle_notify(ctx.clone(), row, job, r, self.idle_notify.clone())
                }
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
        Python::attach(|py| {
            let handlers = self.start_handlers.read().expect("Lock poisoned");
            for handler in handlers.iter() {
                match handler.bind(py).call0() {
                    Ok(_) => debug!("Worker start handler executed successfully"),
                    Err(e) => error!("Error calling worker start handler: {:?}", e),
                }
            }
        });

        let ret = runner(self.ctx.clone(), tid.clone(), state, rx)
            .instrument(info_span!("runner", tid = tid.clone()))
            .await;

        // Call worker stop handlers
        Python::attach(|py| {
            let handlers = self.stop_handlers.read().expect("Lock poisoned");
            for handler in handlers.iter() {
                match handler.bind(py).call0() {
                    Ok(_) => debug!("Worker stop handler executed successfully"),
                    Err(e) => error!("Error calling worker stop handler: {:?}", e),
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

    /// Notify the main loop that a worker thread has become idle.
    /// This triggers an immediate poll for new jobs instead of waiting for the polling interval.
    ///
    /// Uses notify_one() which stores a permit if no task is waiting, ensuring the notification
    /// isn't lost when the main loop is busy processing jobs.
    pub fn notify_idle(&self) {
        trace!("notify_idle called");
        self.idle_notify.notify_one();
        trace!("notify_idle completed");
    }
}

#[async_trait]
impl ProcessTrait for Worker {
    fn ctx(&self) -> &Arc<AppContext> {
        &self.ctx
    }

    fn process_info(&self) -> ProcessInfo {
        ProcessInfo::new("Worker", "worker")
    }
}
