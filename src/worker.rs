use crate::context::*;
use crate::entities::{prelude::*, *};
use crate::process::ProcessTrait;
use anyhow::Result;
use async_trait::async_trait;

#[cfg(feature = "use-log")]
use log::{debug, error, info, trace, warn};

use sea_query::LockBehavior;
#[cfg(feature = "use-tracing")]
use tracing::{debug, error, info, trace, warn};

use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use rand::Rng;
use sea_orm::TransactionTrait;
use sea_orm::*;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Instant;
use tokio::sync::mpsc::Receiver;
use tokio::sync::Mutex;

use colored::*;
use tracing::{info_span, Instrument};

use pyo3::types::{PyBool, PyDict, PyList, PyTuple, PyType};
use pyo3::exceptions::PyTypeError;

fn json_to_py(py: Python<'_>, value: &serde_json::Value) -> PyObject {
    match value {
        serde_json::Value::String(s) => s.into_pyobject(py).unwrap().into(),
        serde_json::Value::Number(n) => {
            if n.is_i64() {
                let val: i64 = n.as_i64().unwrap();
                val.into_pyobject(py).unwrap().into()
            } else if n.is_u64() {
                let val: u64 = n.as_u64().unwrap();
                val.into_pyobject(py).unwrap().into()
            } else if n.is_f64() {
                let val: f64 = n.as_f64().unwrap();
                val.into_pyobject(py).unwrap().into()
            } else {
                py.None()
            }
        }
        serde_json::Value::Bool(b) => PyBool::new(py, *b).as_ref().into_pyobject(py).unwrap().into(),
        serde_json::Value::Array(arr) => {
            let py_list = PyList::empty(py);
            for item in arr {
                py_list.append(json_to_py(py, item)).unwrap();
            }
            py_list.into_pyobject(py).unwrap().into()
        }
        serde_json::Value::Object(obj) => {
            let py_dict = PyDict::new(py);
            for (key, value) in obj {
                py_dict.set_item(key, json_to_py(py, value)).unwrap();
            }
            py_dict.into_pyobject(py).unwrap().into()
        }
        serde_json::Value::Null => py.None(),
    }
}

async fn runner(
    ctx: Arc<AppContext>, thread_id: String, state: Arc<Mutex<i32>>,
    rx: Arc<Mutex<Receiver<Execution>>>,
) -> Result<()> {
    let mut tid = format!("{:?}", std::thread::current().id());
    let graceful_shutdown = ctx.graceful_shutdown.clone();
    let force_quit = ctx.force_quit.clone();
    // let opts = ctx.connect_options.clone();

    if true {
        let mut thread_id: u64 = 0;
        Python::with_gil(|py| {
            let threading = PyModule::import(py, "threading").unwrap();
            let bound = threading.getattr("get_ident").unwrap();
            let ident = bound.call0().unwrap();
            thread_id = ident.extract::<u64>().unwrap();
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
              let mut execution = execution.unwrap();
              execution.tid = tid.clone();
              let job_id = execution.claimed.job_id;

              // async {
                  // 尝试获取锁，如果成功则处理任务
                  // let _lock = state.lock().await;
                  let class_name = execution.job.class_name.clone();

                  info!("Job `{}' started", class_name);

                  let timeout_duration = std::time::Duration::from_secs(10); // 设置超时时间

                  let result = tokio::select! {
                      _ = graceful_shutdown.cancelled() => {
                          info!("Graceful cancelling");
                          // break;
                          Err(anyhow::anyhow!("Job `{}' cancelling", class_name))
                      }
                      _ = force_quit.cancelled() => {
                          error!("Job `{}' cancelled", class_name);
                          break;
                          Err(anyhow::anyhow!("Job `{}' cancelled", class_name))
                      }
                      _ = tokio::time::sleep(timeout_duration) => {
                          error!("Job `{}' timeout", class_name);
                          Err(anyhow::anyhow!("Job `{}' timeout", class_name))
                      }
                      // result = execution.invoke() => {
                      //     result
                      // }
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
                      debug!("Current job executed, shutdown gracefully");
                      break;
                  }
              // }
              // .instrument(tracing::info_span!("rust_runner", jid = job_id))
              // .await;
          }
        }
    }

    debug!("Stopped");
    Ok(())
}

#[pyclass(name = "Runnable", subclass)]
#[derive(Debug, Clone)]
pub struct Runnable {
    class_name: String,
    handler: Py<PyAny>,
    queue_as: String,
    priority: i64,
    retry_info: Option<RetryInfo>,
}

#[derive(Debug, Clone)]
struct RetryInfo {
    scheduled_at: chrono::NaiveDateTime,
    failed_attempts: i32,
}

impl Runnable {
    pub fn new(class_name: String, handler: Py<PyAny>, queue_as: String, priority: i64) -> Self {
        Self { class_name, handler, queue_as, priority, retry_info: None }
    }

    /// 检查是否应该重试，返回匹配的重试策略
    fn should_retry(
        &self, py: Python, bound: &Bound<PyAny>, error: &PyErr, failed_attempts: i32,
    ) -> PyResult<Option<RetryStrategy>> {
        if !bound.hasattr("retry_on")? {
            return Ok(None);
        }

        let retry_strategies = bound.getattr("retry_on")?.extract::<Vec<RetryStrategy>>()?;

        for strategy in retry_strategies {
            if i64::from(failed_attempts) >= strategy.attempts {
                continue; // 超过最大重试次数
            }

            if self.is_exception_match(py, &strategy.exceptions, error)? {
                return Ok(Some(strategy));
            }
        }

        Ok(None)
    }

    /// 检查是否应该丢弃
    fn should_discard(&self, py: Python, bound: &Bound<PyAny>, error: &PyErr) -> PyResult<Option<bool>> {
        if !bound.hasattr("discard_on")? {
            return Ok(None);
        }

        let discard_strategies = bound.getattr("discard_on")?.extract::<Vec<DiscardStrategy>>()?;

        for strategy in discard_strategies {
            if self.is_exception_match(py, &strategy.exceptions, error)? {
                return Ok(Some(true));
            }
        }

        Ok(Some(false))
    }

    /// 检查是否有救援处理，返回匹配的救援策略
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

    /// 检查异常是否匹配
    fn is_exception_match(&self, py: Python, exceptions: &Py<PyAny>, error: &PyErr) -> PyResult<bool> {
        let exceptions_bound = exceptions.bind(py);

        // 处理单个异常类型
        if let Ok(exception_type) = exceptions_bound.downcast::<PyType>() {
            return Ok(error.is_instance(py, exception_type));
        }

        // 处理异常类型元组
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



    /// 处理丢弃 - 直接标记任务为完成，不记录失败信息
    fn handle_discard(
        &self, py: Python, bound: &Bound<PyAny>, error: &PyErr, job: solid_queue_jobs::Model,
    ) -> Result<solid_queue_jobs::Model> {
        if let Ok(discard_strategies) = bound.getattr("discard_on").and_then(|d| d.extract::<Vec<DiscardStrategy>>()) {
            for strategy in discard_strategies {
                if self.is_exception_match(py, &strategy.exceptions, error).unwrap_or(false) {
                    // 调用丢弃处理器（如果有）
                    if let Some(handler) = &strategy.handler {
                        if let Err(handler_error) = handler.call1(py, (error.value(py),)) {
                            warn!("Error in discard handler: {}", handler_error);
                        }
                    }

                    let error_name = error.get_type(py).name()
                        .map(|s| s.to_string())
                        .unwrap_or_else(|_| "unknown error".to_string());
                    info!("Job discarded due to {}", error_name);

                    // 直接返回成功，表示任务被丢弃并标记为完成
                    return Ok(job);
                }
            }
        }

        // 如果没有找到匹配的丢弃策略，返回原始错误
        let error_payload = self.create_error_payload(py, error);
        Err(anyhow::anyhow!(error_payload))
    }

    /// 创建错误负载
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
        Python::with_gil(|py| {
            // py.allow_threads(|| {
            // deserialize json arguments
            let mut v = serde_json::Value::Array(vec![]);
            if let Some(arguments) = job.arguments.as_ref() {
                // debug!("arguments: {:?}", arguments);
                v = serde_json::from_str(arguments).unwrap();
            }

            if let serde_json::Value::Object(ref o) = v {
                if let Some(serde_json::Value::Array(_)) = o.get("arguments") {
                    v = o["arguments"].clone();
                } else {
                    return Err(anyhow::anyhow!("'arguments' is not an array"));
                }
            }

            let binding = json_to_py(py, &v);
            // debug!("binding: {:?}", binding);
            let args = binding.downcast_bound::<pyo3::types::PyList>(py);
            // debug!("arguments: {:?}", args);
            if let Err(e) = args {
                return Err(anyhow::anyhow!("failed to convert arguments to PyList: {:?}", e));
            }
            let args = args.unwrap();

            // 初始化 kwargs
            let kwargs = PyDict::new(py);

            if args.len() > 1 {
                let last_index = args.len() - 1;
                let last = args.get_item(last_index).unwrap();

                if last.is_instance_of::<pyo3::types::PyDict>() {
                    let last_dict = last.downcast::<pyo3::types::PyDict>().unwrap();
                    for (key, value) in last_dict {
                        kwargs.set_item(key, value).unwrap();
                    }
                    args.del_item(last_index).unwrap();
                }
            }

            // debug!("args: {:?}", args);
            // debug!("kwargs: {:?}", kwargs);
            let args = PyTuple::new(py, args);

            let bound = self.handler.bind(py);
            let instance = bound.call0().unwrap();
            // let ret = instance.call_method0("perform");
            instance.setattr("id", job.id).unwrap();
            instance.setattr("failed_attempts", job.failed_attempts).unwrap();
            let func = instance.getattr("perform").unwrap();
            let ret = func.call((&args.unwrap(),), Some(&kwargs));

            match ret {
                Ok(_) => {
                    // 返回成功
                    return Ok(job.clone());
                }
                Err(e) => {
                    // 返回失败
                    error!("------------------- [DEBUG] error: {:?}", e);
                    // debug!("is PyException: {:?}", e.is_instance_of::<PyException>(py));
                    // debug!("is CustomError: {:?}", e.is_instance_of::<CustomError>(py));
                    // debug!("bound: {:?}", bound);

                    // apply discard_on
                    // if bound.hasattr("discard_on")? {
                    //     let values = bound.getattr("discard_on")?;
                    //     let strategies = if let Ok(tuple) = values.extract::<&PyTuple>() {
                    //         tuple
                    //             .iter()
                    //             .map(|item| item.extract::<DiscardStrategy>())
                    //             .collect::<Result<Vec<_>, _>>()?
                    //     } else if let Ok(list) = values.extract::<&PyList>() {
                    //         list.iter()
                    //             .map(|item| item.extract::<DiscardStrategy>())
                    //             .collect::<Result<Vec<_>, _>>()?
                    //     } else {
                    //         vec![]
                    //     };

                    //     for strategy in strategies.iter() {
                    //         let exceptions = strategy.exceptions.extract::<&PyTuple>(py)?;
                    //         debug!("exceptions: {:?}", exceptions);

                    //         for exception in exceptions.iter() {
                    //             if let Ok(exception_type) = exception.downcast::<PyType>() {
                    //                 let exception_name = exception_type.qualname()?;

                    //                 let exception_any = exception.into_pyobject(py).into_bound(py);
                    //                 if e.is_instance_bound(py, &exception_any) {
                    //                     // deal with handler
                    //                     warn!("Job was discarded due to: {}", exception_name);
                    //                     return Ok(job.clone());
                    //                 }
                    //             } else {
                    //                 error!("{:?} is not a PyType", exception);
                    //             }
                    //         }
                    //     }
                    // }

                    // // apply rescue_from
                    // let rescue_from_name = "rescue_from";
                    // if bound.hasattr(rescue_from_name)? {
                    //     // let rescue_strategies =
                    //     //     bound.getattr("rescue_from")?.extract::<&PyList>()?;
                    //     let values = bound.getattr(rescue_from_name)?;
                    //     let rescue_strategies = if let Ok(tuple) = values.extract::<&PyTuple>() {
                    //         tuple
                    //             .iter()
                    //             .map(|item| item.extract::<RescueStrategy>())
                    //             .collect::<Result<Vec<_>, _>>()?
                    //     } else if let Ok(list) = values.extract::<&PyList>() {
                    //         list.iter()
                    //             .map(|item| item.extract::<RescueStrategy>())
                    //             .collect::<Result<Vec<_>, _>>()?
                    //     } else {
                    //         vec![]
                    //     };

                    //     debug!("############# apply rescue_strategies: {:?}", rescue_strategies);

                    //     for strategy in rescue_strategies.iter() {
                    //         // let strategy = strategy.extract::<RescueStrategy>()?;
                    //         debug!("strategy: {:?}", strategy);

                    //         let exceptions = strategy.exceptions.extract::<&PyTuple>(py)?;
                    //         debug!("exceptions: {:?}", exceptions);
                    //         let handler = strategy.handler.clone().into_bound(py);
                    //         // let handler = strategy.handler;
                    //         // debug!("handler: {:?}", handler);

                    //         // let mut value: Result<pyo3::Bound<'_, pyo3::PyAny>, E> = Ok(handler);
                    //         for exception in exceptions.iter() {
                    //             if let Ok(exception_type) = exception.downcast::<PyType>() {
                    //                 let exception_name = exception_type.qualname()?;

                    //                 let exception_any = exception.into_pyobject(py).into_bound(py);
                    //                 if e.is_instance_bound(py, &exception_any) {
                    //                     // warn!("Job rescued due to: {}", exception_name);

                    //                     if handler.is_instance_of::<pyo3::types::PyString>() {
                    //                         // debug!("handler is a PyString");
                    //                     } else if handler.is_callable() {
                    //                         // debug!("handler is a PyFunction");
                    //                     } else {
                    //                         // error!("rescue_from handler is not a PyFunction");
                    //                         continue;
                    //                     }

                    //                     let func =
                    //                         if let Ok(func_name) = handler.extract::<&PyString>() {
                    //                             instance.getattr(func_name.to_str()?)
                    //                         } else {
                    //                             Ok(handler.clone())
                    //                         };

                    //                     if func.is_err() {
                    //                         error!("rescue_from handler is not a PyFunction");
                    //                         continue;
                    //                     }
                    //                     let func = func.unwrap();
                    //                     // debug!("handler: {:?}", func);
                    //                     warn!(
                    //                         "Job was rescued by {:?} due to: {}",
                    //                         handler, exception_name
                    //                     );
                    //                     // let args = PyList::new_bound(py, vec![&instance]);
                    //                     let mut args = PyList::new_bound(py, vec![&e]);
                    //                     // let _ = args.append(&e);

                    //                     if func.hasattr("__self__")? {
                    //                         debug!("............ func is instance method");
                    //                     } else {
                    //                         debug!("............ func is not instance method");
                    //                         args = PyList::new_bound(py, vec![&instance]);
                    //                         let _ = args.append(&e);
                    //                     }

                    //                     let args = Py::new(py, PyTuple::new(py, args)).unwrap();
                    //                     // debug!("args: {:?}", args);
                    //                     let ret = func.call(args, None);
                    //                     debug!("rescue_from ret: {:?}", ret);

                    //                     if ret.is_ok() {
                    //                         return Ok(job.clone());
                    //                     }
                    //                 }
                    //             } else {
                    //                 error!("{:?} is not a PyType", exception);
                    //             }
                    //         }
                    //     }
                    // }

                    // 检查是否应该重试
                    if let Some(retry_strategy) = self.should_retry(py, &bound, &e, job.failed_attempts)? {
                        warn!("Job will be retried (attempt {})", job.failed_attempts + 1);

                        let delay = retry_strategy.wait;
                        let scheduled_at = chrono::Utc::now().naive_utc() + chrono::Duration::from_std(delay).unwrap();
                        let failed_attempts = job.failed_attempts + 1;

                        // 设置重试信息到 runnable
                        self.retry_info = Some(RetryInfo {
                            scheduled_at,
                            failed_attempts,
                        });

                        return Ok(job.clone());
                    }

                    // 检查是否应该丢弃
                    if let Some(discard_result) = self.should_discard(py, &bound, &e)? {
                        if discard_result {
                            warn!("Job will be discarded");
                            return self.handle_discard(py, &bound, &e, job.clone());
                        }
                    }

                    // 检查是否有救援处理
                    if let Some(rescue_strategy) = self.should_rescue(py, &bound, &e)? {
                        // 调用救援处理器
                        match rescue_strategy.handler.call1(py, (e.value(py),)) {
                            Ok(_) => {
                                info!("Job rescued from error");
                                return Ok(job.clone());
                            }
                            Err(rescue_error) => {
                                warn!("Error in rescue handler: {}", rescue_error);
                                // 救援失败，继续到失败处理
                            }
                        }
                    }

                    // 如果没有任何错误处理策略匹配，则标记为失败
                    job.failed_attempts += 1;
                    let error_payload = self.create_error_payload(py, &e);
                    Err(anyhow::anyhow!(error_payload))
                }
            }

            // }) // end allow_threads
        })
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
}

impl Execution {
    pub fn new(
        ctx: Arc<AppContext>, claimed: solid_queue_claimed_executions::Model,
        job: solid_queue_jobs::Model, runnable: Runnable,
    ) -> Self {
        // 获取当前线程的 ThreadId
        let thread_id = std::thread::current().id();

        // 将 ThreadId 转换为字符串
        let thread_id_str = format!("{:?}", thread_id);

        // 提取数字部分
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
        let result = async { self.runnable.invoke(&mut job) }.instrument(span.clone()).await;

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

        // 检查是否需要重试（通过 runnable.retry_info 判断）
        let retry_job_data = if let Some(ref retry_info) = self.runnable.retry_info {
            Some((retry_info.scheduled_at, retry_info.failed_attempts))
        } else {
            None
        };

        db.transaction::<_, solid_queue_jobs::Model, DbErr>(|txn| {
            // Box::pin(async move { Ok(after_executed(txn, claimed, result).await.unwrap()) })
            Box::pin(async move {
                let claimed_id = claimed.id;

                // 处理重试逻辑
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
                    let new_job_id = saved_job.id.clone().unwrap();

                    // 创建 scheduled_execution 记录
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
                    // claimed.delete(txn).await?;

                    // SolidQueue 的策略是 将异常信息作为参数传递创建一个新的 Job
                    // 我的策略是增加 failed_attempts 字段，如果失败次数超过一定次数则不再执行

                    // 写入 failed_executions 表
                    let failed_execution = solid_queue_failed_executions::ActiveModel {
                        id: ActiveValue::NotSet,
                        job_id: ActiveValue::Set(job_id),
                        error: ActiveValue::Set(Some(err.unwrap().to_string())),
                        created_at: ActiveValue::Set(chrono::Utc::now().naive_utc()),
                    };
                    failed_execution.save(txn).await?;

                    // let job = solid_queue_jobs::Entity::find_by_id(job_id).one(txn).await.unwrap();
                    // return Ok(job.unwrap().into());
                }

                // 更新 jobs 表
                let mut job: solid_queue_jobs::ActiveModel = job.clone().into();
                job.finished_at = ActiveValue::Set(Some(chrono::Utc::now().naive_utc()));
                job.updated_at = ActiveValue::Set(chrono::Utc::now().naive_utc());
                let updated = job.update(txn).await?;

                // 直接删除 claimed_executions 表中的记录
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
            // info!("Job `{}' processed in: {:?}", duration);
            let job_model = job.try_into_model().unwrap();
            // debug!("Job: {:?}", job_model);

            if job_model.finished_at.is_none() {
                error!("Job `{}' processed in: {:?}", class_name, duration);
            } else {
                debug!("Job `{}' processed in: {:?}", class_name, duration)
            }
        })
        .map_err(|e| {
            let duration = self.timer.elapsed();
            error!("Job processing failed in {:?}: {:?}", duration, e)
        })
        .ok();

        // let job = self.job.clone();
        // Ok(job)
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
        self.runnable.clone()
    }

    fn perform(&mut self) -> PyResult<()> {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let ret = rt.block_on(async { self.invoke().await });
        // debug!("-------------------------- Job result: {:?}", ret);
        if let Err(e) = ret {
            return Err(e.into());
        }

        Ok(())
    }

    // post execution result
    fn post(&mut self, py: Python, exc: &Bound<'_, PyAny>, traceback: &str) {
        let result = if exc.is_instance_of::<PyException>() {
            let e = exc.downcast::<PyException>().unwrap();
            error!("error: {:?}", e);
            debug!("is PyException: {:?}", e.is_instance_of::<PyException>());
            // debug!("is CustomError: {:?}", e.is_instance_of::<CustomError>(py));

            // let mut backtrace: Vec<String> = vec![];
            let backtrace: Vec<_> = traceback.lines().map(String::from).collect();

            // if let Some(tb) = traceback {
            //     backtrace = tb.format().unwrap().to_string().lines().map(String::from).collect();
            // }

            error!("error_type: {}", e.get_type().str().unwrap().to_string());
            error!("error_description: {}", e.to_string());

            let error_payload = serde_json::json!({
                "exception_class": e.get_type().str().unwrap().to_string(),
                "message": e.to_string(),
                "backtrace": backtrace,
            });

            Err(anyhow::anyhow!(serde_json::to_string(&error_payload).unwrap()))
        } else {
            // info!("Job done");
            Ok(self.job.clone())
        };

        py.allow_threads(|| {
            let rt = Arc::new(tokio::runtime::Runtime::new().unwrap());
            rt.block_on(async move {
                let ret = self.after_executed(result).await;
                debug!("Job result post: {:?}", ret);
            });
        });
    }

    fn retry(
        &mut self, py: Python, strategy: RetryStrategy, exc: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let job = self.job.clone();
        let scheduled_at = chrono::Utc::now().naive_utc() + strategy.wait();
        let args: serde_json::Value =
            serde_json::from_str(job.arguments.unwrap().as_str()).unwrap();
        let e = exc.downcast::<PyException>().unwrap();
        let error_type = e.get_type().qualname().unwrap().to_string();

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
            let rt = Arc::new(tokio::runtime::Runtime::new().unwrap());
            let span = span.clone();
            let job = rt.block_on(
                async {
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
                                active_job_id: ActiveValue::Set(Some(job.active_job_id.unwrap())),
                                scheduled_at: ActiveValue::Set(Some(scheduled_at)),
                                finished_at: ActiveValue::Set(None),
                                concurrency_key: ActiveValue::Set(Some(concurrency_key.clone())),
                                created_at: ActiveValue::Set(chrono::Utc::now().naive_utc()),
                                updated_at: ActiveValue::Set(chrono::Utc::now().naive_utc()),
                            }
                            .save(txn)
                            .await?;

                            let job_id = job.id.clone().unwrap();
                            let scheduled_execution =
                                solid_queue_scheduled_executions::ActiveModel {
                                    id: ActiveValue::not_set(),
                                    job_id: ActiveValue::Set(job_id),
                                    queue_name: ActiveValue::Set(job.queue_name.clone().unwrap()),
                                    priority: ActiveValue::Set(job.priority.clone().unwrap()),
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
                .instrument(span),
            );

            // debug!("scheduled: {:?}", job);
            if job.is_err() {
                error!("Job `{}' failed to schedule", job.err().unwrap());
            }
        });

        Ok(())
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct Worker {
    pub ctx: Arc<AppContext>,
    pub runnables: Arc<RwLock<HashMap<String, Runnable>>>,
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
        let runnables = Arc::new(RwLock::new(HashMap::<String, Runnable>::new()));
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
            runnables,
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
            self.start_handlers.write().unwrap().push(handler.clone_ref(py));
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
            self.stop_handlers.write().unwrap().push(handler.clone_ref(py));
            debug!("Worker stop handler: {:?} registered", handler);
        }

        Ok(())
    }

    fn get_runnables(&self) -> PyResult<Vec<String>> {
        Ok(self.runnables.read().unwrap().keys().cloned().collect())
    }

    fn get_runnable(&self, class_name: &str) -> Result<Runnable> {
        let runnables = self.runnables.read().unwrap();
        let runnable = runnables.get(class_name).unwrap();
        Python::with_gil(|_py| Ok(runnable.clone()))
    }

    // fn get_job_class(&self, class_name: &str) -> PyResult<Py<PyAny>> {
    //     let runnables = self.runnables.read().unwrap();
    //     let runnable = runnables.get(class_name).unwrap();
    //     let job_class = runnable.handler.clone();
    //     Ok(job_class.clone())
    // }

    pub fn register_job_class(&self, py: Python, klass: Py<PyAny>) -> PyResult<()> {
        Python::with_gil(|py| {
            let bound = klass.bind(py);
            let class_name = bound.downcast::<PyType>().unwrap().qualname().unwrap();
            debug!("Registered job class: {:?}", class_name);

            let mut queue_name = "default".to_string();
            if bound.hasattr("queue_as").unwrap() {
                queue_name = bound.getattr("queue_as").unwrap().extract::<String>().unwrap();
            }

            let runnable = Runnable {
                class_name: class_name.to_string().clone(),
                handler: klass,
                queue_as: queue_name,
                priority: 0,
                retry_info: None,
            };
            info!("Registered job: {:?}", runnable);

            self.runnables.write().unwrap().insert(class_name.to_string(), runnable);
        });
        Ok(())
    }

    pub async fn claim_job(&self) -> Result<solid_queue_claimed_executions::Model, anyhow::Error> {
        let mut opts = self.ctx.connect_options.clone();
        // if self.ctx.silence_polling {
        //     opts.sqlx_logging(false); //.sqlx_logging_level(log::LevelFilter::Error);
        //                               // warn!("-------------------- opts: {:?}", opts);
        // }
        let db = self.ctx.get_db().await;

        let job = db
            .transaction::<_, solid_queue_claimed_executions::ActiveModel, DbErr>(|txn| {
                Box::pin(async move {
                    // 获取一个未被锁定的任务
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
                        .unwrap();

                        execution.delete(txn).await.unwrap();
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
        let use_skip_locked = self.ctx.use_skip_locked;

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
                        .unwrap();

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
        let db = self.ctx.get_db().await;
        let mut polling_interval = tokio::time::interval(self.ctx.worker_polling_interval);
        let worker_threads = self.ctx.worker_threads;
        let tx = self.dispatch_sender.clone();
        let ctx = self.ctx.clone();

        // 确保只有一个主循环在运行
        let _ = self.polling.lock().await;
        let thread_id = Self::get_tid();

        let quit = self.ctx.graceful_shutdown.clone();

        // Call worker start handlers before starting the main loop
        Python::with_gil(|py| {
            let handlers = self.start_handlers.read().unwrap();
            for handler in handlers.iter() {
                match handler.bind(py).call0() {
                    Ok(_) => debug!("Worker start handler executed successfully in main loop"),
                    Err(e) => error!("Error calling worker start handler in main loop: {:?}", e)
                }
            }
        });

        // 生产者发送任务
        loop {
            tokio::select! {
                // _ = heartbeat_interval.tick() => {
                //   // self.heartbeat(&db, &process).await?;
                // }
                // _ = tokio::signal::ctrl_c() => {
                //     info!("ctrl-c received");
                //     // return Ok(());
                //     quit.cancel();
                // }
                _ = quit.cancelled() => {
                    info!("Graceful shutdown, stop polling");

                    // Call worker stop handlers before exiting
                    Python::with_gil(|py| {
                        let handlers = self.stop_handlers.read().unwrap();
                        for handler in handlers.iter() {
                            match handler.bind(py).call0() {
                                Ok(_) => debug!("Worker stop handler executed successfully in main loop"),
                                Err(e) => error!("Error calling worker stop handler in main loop: {:?}", e)
                            }
                        }
                    });

                    return Ok(());
                }
                _ = polling_interval.tick() => {
                    let claimed = self.claim_jobs(worker_threads).instrument(tracing::info_span!("polling", tid=thread_id)).await;

                    if let Err(e) = claimed {
                        // if let Some(io_err) = e.downcast_ref::<QueryError>() {
                        //   error!("db error: {:?}", io_err);
                        // } else {
                        //   error!("unknown error: {:?}", e);
                        // }

                        error!("no job found: {:?}", e);
                        continue;
                    }

                    let claimed = claimed.unwrap();
                    let txn = db.begin().await.unwrap();
                    for row in claimed {
                        let job_id = row.job_id;
                        let job = solid_queue_jobs::Entity::find_by_id(job_id).one(&txn).await.unwrap();
                        // debug!("Job claimed: {:?}", job);
                        // info!("Job `{}' started", job.clone().unwrap().class_name);
                        let job = job.unwrap();

                        let runnable = self.get_runnable(&job.class_name);
                        if runnable.is_err() {
                            error!("Job handler not found: {:?}", &job.class_name);
                            continue;
                        }

                        // debug!("------------------- runnable: {:?}", runnable);
                        // let handler = self.get_job_class(&job.class_name);
                        // if handler.is_err() {
                        //     error!("Job handler not found: {:?}", &job.class_name);
                        //     continue;
                        // }

                        let execution = Execution::new(ctx.clone(), row.clone(), job, runnable.unwrap());

                        tx.lock().await.send(execution).await.unwrap();
                    }
                    txn.commit().await.unwrap();
                }
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
            let handlers = self.start_handlers.read().unwrap();
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
            let handlers = self.stop_handlers.read().unwrap();
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
        let execution = execution.unwrap();
        Ok(execution)
    }

    pub async fn post_execution(&self) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl ProcessTrait for Worker {}
