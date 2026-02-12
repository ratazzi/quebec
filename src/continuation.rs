//! Job Continuation Support
//!
//! This module implements resumable job steps, inspired by Rails' ActiveJob::Continuable.
//! Jobs can be split into multiple steps, with progress saved between steps.
//! If a job is interrupted (e.g., during shutdown), it can resume from the last checkpoint.
//!
//! # Serialization Format (stored in job arguments)
//!
//! ```json
//! {
//!     "arguments": [original, args],
//!     "continuation": {
//!         "completed": ["step1", "step2"],
//!         "current": ["step3", 42]  // [step_name, cursor]
//!     },
//!     "resumptions": 2
//! }
//! ```

#[cfg(feature = "python")]
use pyo3::create_exception;
#[cfg(feature = "python")]
use pyo3::exceptions::PyException;
#[cfg(feature = "python")]
use pyo3::prelude::*;
#[cfg(feature = "python")]
use pyo3::types::PyDict;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use tokio_util::sync::CancellationToken;
use tracing::{debug, trace};

// Exception raised when a job needs to be interrupted and resumed later
#[cfg(feature = "python")]
create_exception!(quebec, JobInterrupted, PyException);

// Exception raised when step validation fails (like Rails' InvalidStepError)
#[cfg(feature = "python")]
create_exception!(quebec, InvalidStepError, PyException);

/// State of a job's continuation progress
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ContinuationState {
    /// Names of completed steps
    #[serde(default)]
    pub completed: Vec<String>,

    /// Current step being executed: (step_name, cursor)
    /// cursor can be any JSON-serializable value
    #[serde(default)]
    pub current: Option<(String, serde_json::Value)>,
}

impl ContinuationState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Check if a step has been completed
    pub fn is_completed(&self, step_name: &str) -> bool {
        self.completed.contains(&step_name.to_string())
    }

    /// Get the cursor for current step if it matches
    pub fn get_cursor(&self, step_name: &str) -> Option<serde_json::Value> {
        self.current
            .as_ref()
            .filter(|(name, _)| name == step_name)
            .map(|(_, cursor)| cursor.clone())
    }

    /// Mark a step as completed
    pub fn complete_step(&mut self, step_name: &str) {
        if !self.completed.contains(&step_name.to_string()) {
            self.completed.push(step_name.to_string());
        }
        // Clear current if it matches
        if self
            .current
            .as_ref()
            .is_some_and(|(name, _)| name == step_name)
        {
            self.current = None;
        }
    }

    /// Set current step and cursor (for checkpointing)
    pub fn set_current(&mut self, step_name: &str, cursor: serde_json::Value) {
        self.current = Some((step_name.to_string(), cursor));
    }

    /// Clear the current step (when entering a new step)
    pub fn clear_current(&mut self) {
        self.current = None;
    }
}

/// Context for a single step, exposed to Python
#[cfg(feature = "python")]
#[pyclass(name = "StepContext", from_py_object)]
#[derive(Debug, Clone)]
pub struct StepContext {
    /// Name of this step
    step_name: String,

    /// Current cursor value (None for first execution)
    cursor: Option<serde_json::Value>,

    /// Initial cursor value (for tracking if cursor has advanced)
    initial_cursor: Option<serde_json::Value>,

    /// Whether this step was already completed (noop mode)
    noop: bool,

    /// Whether this step was resumed from a previous execution
    resumed: bool,

    /// Reference to parent continuation context for updates
    continuation_ctx: Arc<Mutex<ContinuationContext>>,
}

#[cfg(feature = "python")]
#[pymethods]
impl StepContext {
    /// Get the current cursor value
    /// Returns None on first execution, or the saved cursor value when resuming
    #[getter]
    fn cursor(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match &self.cursor {
            None => Ok(py.None()),
            Some(v) => json_to_pyobject(py, v),
        }
    }

    /// Check if this step should be skipped (already completed in a previous run)
    /// Use this to wrap code that shouldn't re-run:
    ///
    ///     with self.step("prepare") as step:
    ///         if not step.skip:
    ///             do_expensive_setup()
    #[getter]
    fn skip(&self) -> bool {
        self.noop
    }

    /// Check if this step was resumed from a previous execution
    /// Like Rails' Step#resumed?
    #[getter]
    fn resumed(&self) -> bool {
        self.resumed
    }

    /// Check if the cursor has been advanced during this execution
    /// Like Rails' Step#advanced?
    #[getter]
    fn advanced(&self) -> bool {
        self.initial_cursor != self.cursor
    }

    /// Directly set the cursor value and check for interruption.
    /// Like Rails' Step#set!, this automatically calls checkpoint after setting.
    fn set(&mut self, py: Python<'_>, value: &Bound<'_, pyo3::PyAny>) -> PyResult<()> {
        let json_value = pyobject_to_json(value)?;
        self.cursor = Some(json_value.clone());

        // Update the continuation context
        {
            let mut ctx = self
                .continuation_ctx
                .lock()
                .map_err(|_| PyException::new_err("Lock poisoned"))?;
            ctx.state.set_current(&self.step_name, json_value);
            ctx.mark_dirty();
        }

        // Automatically checkpoint after setting cursor (like Rails)
        self.checkpoint(py)
    }

    /// Advance the cursor and check for interruption.
    /// Like Rails' Step#advance!, uses from_.succ (or +1 for integers).
    /// Automatically calls checkpoint after advancing.
    #[pyo3(signature = (from_=None))]
    fn advance(&mut self, py: Python<'_>, from_: Option<&Bound<'_, pyo3::PyAny>>) -> PyResult<()> {
        let new_cursor = match from_ {
            Some(val) => {
                // Try to extract as i64 and increment (like Ruby's succ)
                if let Ok(n) = val.extract::<i64>() {
                    serde_json::Value::Number((n + 1).into())
                } else if let Ok(s) = val.extract::<String>() {
                    // For strings, try to implement succ-like behavior
                    // This is a simplified version - just increment last char
                    let char_count = s.chars().count();
                    if char_count > 0 {
                        let last_char = s.chars().last().unwrap();
                        let next_char = char::from_u32(last_char as u32 + 1).unwrap_or(last_char);
                        let new_str: String = s.chars().take(char_count - 1).collect::<String>()
                            + &next_char.to_string();
                        serde_json::Value::String(new_str)
                    } else {
                        serde_json::Value::String(s)
                    }
                } else {
                    // For other types, use the value as-is (caller should use set instead)
                    pyobject_to_json(val)?
                }
            }
            None => {
                // No value provided, increment current cursor if it's a number
                match &self.cursor {
                    Some(serde_json::Value::Number(n)) => {
                        if let Some(i) = n.as_i64() {
                            serde_json::Value::Number((i + 1).into())
                        } else {
                            return Err(PyException::new_err(
                                "Cannot advance non-integer cursor without from_ value",
                            ));
                        }
                    }
                    Some(serde_json::Value::String(s)) => {
                        // String succ - use char count, not byte length
                        let char_count = s.chars().count();
                        if char_count > 0 {
                            let last_char = s.chars().last().unwrap();
                            let next_char =
                                char::from_u32(last_char as u32 + 1).unwrap_or(last_char);
                            let new_str: String =
                                s.chars().take(char_count - 1).collect::<String>()
                                    + &next_char.to_string();
                            serde_json::Value::String(new_str)
                        } else {
                            serde_json::Value::String(s.clone())
                        }
                    }
                    None => serde_json::Value::Number(1.into()), // Start from 1
                    _ => {
                        return Err(PyException::new_err(
                            "Cannot advance cursor of this type without from_ value",
                        ))
                    }
                }
            }
        };

        self.cursor = Some(new_cursor.clone());

        // Update the continuation context
        {
            let mut ctx = self
                .continuation_ctx
                .lock()
                .map_err(|_| PyException::new_err("Lock poisoned"))?;
            ctx.state.set_current(&self.step_name, new_cursor);
            ctx.mark_dirty();
        }

        // Automatically checkpoint after advancing cursor (like Rails)
        self.checkpoint(py)
    }

    /// Check if the job should be interrupted (e.g., due to shutdown signal)
    /// Raises JobInterrupted if interruption is needed
    fn checkpoint(&self, _py: Python<'_>) -> PyResult<()> {
        let ctx = self
            .continuation_ctx
            .lock()
            .map_err(|_| PyException::new_err("Lock poisoned"))?;

        if ctx.should_interrupt() {
            debug!(
                "Checkpoint: interrupting job at step '{}' with cursor {:?}",
                self.step_name, self.cursor
            );
            // Drop the lock before raising exception
            drop(ctx);

            // Raise JobInterrupted - the worker will catch this and re-enqueue
            return Err(JobInterrupted::new_err(format!(
                "Job interrupted at step '{}' for graceful shutdown",
                self.step_name
            )));
        }

        Ok(())
    }

    fn __repr__(&self) -> String {
        format!(
            "StepContext(step='{}', cursor={:?}, noop={})",
            self.step_name, self.cursor, self.noop
        )
    }
}

#[cfg(feature = "python")]
impl StepContext {
    fn new(
        step_name: String,
        cursor: Option<serde_json::Value>,
        noop: bool,
        resumed: bool,
        continuation_ctx: Arc<Mutex<ContinuationContext>>,
    ) -> Self {
        Self {
            step_name,
            initial_cursor: cursor.clone(),
            cursor,
            noop,
            resumed,
            continuation_ctx,
        }
    }

    /// Check if this step should be skipped (already completed)
    pub fn is_noop(&self) -> bool {
        self.noop
    }

    /// Check if the cursor has been advanced (for continuation tracking)
    pub fn has_advanced(&self) -> bool {
        self.initial_cursor != self.cursor
    }
}

/// Internal context for managing continuation state during job execution
#[derive(Debug)]
pub struct ContinuationContext {
    /// The continuation state
    pub state: ContinuationState,

    /// Number of times this job has been resumed
    pub resumptions: i32,

    /// Cancellation token for checking shutdown
    cancellation_token: Option<CancellationToken>,

    /// Whether the job was modified (needs re-serialization)
    pub dirty: bool,

    /// Steps encountered during this execution (for validation)
    pub encountered: Vec<String>,

    /// Name of the currently running step (for nesting validation)
    pub running_step: Option<String>,

    /// Whether we're in isolating mode (should interrupt before next step)
    pub isolating: bool,

    /// Whether any cursor has been advanced during this execution
    pub advanced: bool,
}

impl ContinuationContext {
    pub fn new(
        state: ContinuationState,
        resumptions: i32,
        cancellation_token: Option<CancellationToken>,
    ) -> Self {
        Self {
            state,
            resumptions,
            cancellation_token,
            dirty: false,
            encountered: Vec::new(),
            running_step: None,
            isolating: false,
            advanced: false,
        }
    }

    /// Check if the job should be interrupted
    pub fn should_interrupt(&self) -> bool {
        self.cancellation_token
            .as_ref()
            .is_some_and(|token| token.is_cancelled())
    }

    /// Mark the context as dirty (needs to be saved)
    pub fn mark_dirty(&mut self) {
        self.dirty = true;
    }

    /// Mark that progress has been made (cursor advanced or step completed)
    pub fn mark_advanced(&mut self) {
        self.advanced = true;
    }

    /// Check if progress has been made during this execution
    pub fn has_advanced(&self) -> bool {
        self.advanced
    }
}

/// Main continuation manager, exposed to Python as a mixin
#[cfg(feature = "python")]
#[pyclass(name = "Continuable", from_py_object)]
#[derive(Debug, Clone)]
pub struct Continuable {
    /// Shared context for this continuation
    ctx: Arc<Mutex<ContinuationContext>>,
}

#[cfg(feature = "python")]
#[pymethods]
impl Continuable {
    #[new]
    fn new() -> Self {
        Self {
            ctx: Arc::new(Mutex::new(ContinuationContext::new(
                ContinuationState::new(),
                0,
                None,
            ))),
        }
    }

    /// Get the number of times this job has been resumed
    #[getter]
    fn resumptions(&self) -> PyResult<i32> {
        let ctx = self
            .ctx
            .lock()
            .map_err(|_| PyException::new_err("Lock poisoned"))?;
        Ok(ctx.resumptions)
    }

    /// Create a step context for the given step name.
    ///
    /// Can be used in two ways:
    ///
    /// 1. Context manager style (for complex steps with cursor):
    ///    ```python
    ///    with self.step("process") as step:
    ///        for i in range(step.cursor or 0, total):
    ///            process(i)
    ///            step.advance(from_=i)
    ///    ```
    ///
    /// 2. Callback style (for simple steps, more like Rails):
    ///    ```python
    ///    self.step("validate", self.do_validate)
    ///    self.step("prepare", lambda: prepare_data())
    ///    ```
    ///
    /// Parameters:
    /// - `name`: Unique name for this step
    /// - `callback`: Optional callable to execute (if None, returns context manager)
    /// - `start`: Initial cursor value (like Rails' `start:` parameter)
    /// - `isolated`: If True, ensures this step runs in its own execution
    #[pyo3(signature = (name, callback=None, *, start=None, isolated=false))]
    fn step(
        &self,
        py: Python<'_>,
        name: &str,
        callback: Option<&Bound<'_, pyo3::PyAny>>,
        start: Option<&Bound<'_, pyo3::PyAny>>,
        isolated: bool,
    ) -> PyResult<Py<PyAny>> {
        // Perform validation and get step state
        let (noop, cursor, resumed) = {
            let mut ctx = self
                .ctx
                .lock()
                .map_err(|_| PyException::new_err("Lock poisoned"))?;

            // Validation 1: Step can't be nested inside another step
            if let Some(ref running) = ctx.running_step {
                return Err(InvalidStepError::new_err(format!(
                    "Step '{}' is nested inside step '{}'",
                    name, running
                )));
            }

            // Validation 2: Step can't be repeated in same execution
            if ctx.encountered.contains(&name.to_string()) {
                return Err(InvalidStepError::new_err(format!(
                    "Step '{}' has already been encountered",
                    name
                )));
            }

            // Validation 3: If resuming with current step, must match
            if let Some((current_name, _)) = &ctx.state.current {
                if current_name != name && !ctx.state.is_completed(name) {
                    return Err(InvalidStepError::new_err(format!(
                        "Step '{}' found, expected to resume from '{}'",
                        name, current_name
                    )));
                }
            }

            // Validation 4: Step order must match completion order
            let encountered_count = ctx.encountered.len();
            if ctx.state.completed.len() > encountered_count {
                let expected = &ctx.state.completed[encountered_count];
                if expected != name {
                    return Err(InvalidStepError::new_err(format!(
                        "Step '{}' found, expected to see '{}'",
                        name, expected
                    )));
                }
            }

            // Record this step as encountered
            ctx.encountered.push(name.to_string());

            // Handle isolated mode - if we're isolating and have made progress,
            // interrupt to let this step run in its own execution
            if isolated {
                ctx.isolating = true;
            }
            if ctx.isolating && ctx.advanced {
                // Drop lock before raising exception
                drop(ctx);
                return Err(JobInterrupted::new_err(format!(
                    "Isolated step '{}' requires separate execution",
                    name
                )));
            }

            // Check if step is already completed
            let noop = ctx.state.is_completed(name);

            // Determine if this step is being resumed (has saved cursor)
            let resumed = ctx.state.get_cursor(name).is_some();

            // Get cursor: either saved cursor or start value
            let cursor = if noop {
                None
            } else {
                ctx.state.get_cursor(name).or_else(|| {
                    // Use start value if provided
                    start.and_then(|s| pyobject_to_json(s).ok())
                })
            };

            (noop, cursor, resumed)
        };

        let step_ctx = StepContext::new(name.to_string(), cursor, noop, resumed, self.ctx.clone());

        // If callback is provided, execute it directly (Rails style)
        if let Some(cb) = callback {
            if noop {
                // Step already completed, skip
                trace!("Step '{}' already completed, skipping callback", name);
                return Ok(py.None());
            }

            // Mark as dirty and set running_step
            {
                let mut ctx = self
                    .ctx
                    .lock()
                    .map_err(|_| PyException::new_err("Lock poisoned"))?;
                ctx.mark_dirty();
                ctx.running_step = Some(name.to_string());
            }

            // Check how many arguments the callback accepts
            let sig = py.import("inspect")?.getattr("signature")?.call1((cb,))?;
            let params = sig.getattr("parameters")?;
            let param_count = params.call_method0("__len__")?.extract::<usize>()?;

            // Call the callback
            let result = if param_count > 0 {
                // Callback wants the step context
                cb.call1((step_ctx.clone(),))
            } else {
                // Callback takes no arguments
                cb.call0()
            };

            // Clear running_step and handle result
            {
                let mut ctx = self
                    .ctx
                    .lock()
                    .map_err(|_| PyException::new_err("Lock poisoned"))?;
                ctx.running_step = None;

                // Track if step made progress
                if step_ctx.has_advanced() {
                    ctx.mark_advanced();
                }
            }

            // Handle the result
            match result {
                Ok(_) => {
                    // Mark step as completed
                    let mut ctx = self
                        .ctx
                        .lock()
                        .map_err(|_| PyException::new_err("Lock poisoned"))?;
                    ctx.state.complete_step(name);
                    ctx.mark_advanced(); // Completing a step counts as progress
                    trace!("Step '{}' completed via callback", name);
                    Ok(py.None())
                }
                Err(e) => {
                    // Check if it's JobInterrupted
                    let job_interrupted = py.get_type::<JobInterrupted>();
                    if e.is_instance(py, &job_interrupted) {
                        // Propagate interruption
                        return Err(e);
                    }
                    // Other error - propagate
                    Err(e)
                }
            }
        } else {
            // No callback - return context manager
            let cm = StepContextManager {
                step_ctx,
                step_name: name.to_string(),
                continuation_ctx: self.ctx.clone(),
                entered: false,
            };
            Ok(cm.into_pyobject(py)?.into())
        }
    }

    fn __repr__(&self) -> String {
        let ctx = self.ctx.lock().ok();
        match ctx {
            Some(c) => format!(
                "Continuable(resumptions={}, completed={:?}, current={:?})",
                c.resumptions,
                c.state.completed,
                c.state.current.as_ref().map(|(name, _)| name)
            ),
            None => "Continuable(<locked>)".to_string(),
        }
    }
}

#[cfg(feature = "python")]
impl Continuable {
    /// Create a Continuable with existing state (for job restoration)
    pub fn with_state(
        state: ContinuationState,
        resumptions: i32,
        cancellation_token: Option<CancellationToken>,
    ) -> Self {
        Self {
            ctx: Arc::new(Mutex::new(ContinuationContext::new(
                state,
                resumptions,
                cancellation_token,
            ))),
        }
    }

    /// Get the current continuation state for serialization
    pub fn get_state(&self) -> Result<(ContinuationState, i32, bool), String> {
        let ctx = self.ctx.lock().map_err(|_| "Lock poisoned".to_string())?;
        Ok((ctx.state.clone(), ctx.resumptions, ctx.dirty))
    }

    /// Set the cancellation token
    pub fn set_cancellation_token(&self, token: CancellationToken) -> Result<(), String> {
        let mut ctx = self.ctx.lock().map_err(|_| "Lock poisoned".to_string())?;
        ctx.cancellation_token = Some(token);
        Ok(())
    }
}

/// Context manager for step execution
#[cfg(feature = "python")]
#[pyclass(name = "StepContextManager", from_py_object)]
#[derive(Debug, Clone)]
pub struct StepContextManager {
    step_ctx: StepContext,
    step_name: String,
    continuation_ctx: Arc<Mutex<ContinuationContext>>,
    entered: bool,
}

#[cfg(feature = "python")]
#[pymethods]
impl StepContextManager {
    fn __enter__(mut slf: PyRefMut<'_, Self>) -> PyResult<StepContext> {
        slf.entered = true;

        // If step is already completed, we'll return a noop context
        if slf.step_ctx.is_noop() {
            trace!("Step '{}' already completed, skipping", slf.step_name);
        } else {
            trace!("Entering step '{}'", slf.step_name);
            // Mark as dirty and set running_step for nesting validation
            let mut ctx = slf
                .continuation_ctx
                .lock()
                .map_err(|_| PyException::new_err("Lock poisoned"))?;
            ctx.mark_dirty();
            ctx.running_step = Some(slf.step_name.clone());
        }

        Ok(slf.step_ctx.clone())
    }

    #[pyo3(signature = (exc_type, _exc_val, _exc_tb))]
    fn __exit__(
        &mut self,
        py: Python<'_>,
        exc_type: Option<&Bound<'_, pyo3::PyAny>>,
        _exc_val: Option<&Bound<'_, pyo3::PyAny>>,
        _exc_tb: Option<&Bound<'_, pyo3::PyAny>>,
    ) -> PyResult<bool> {
        // Clear running_step first
        {
            let mut ctx = self
                .continuation_ctx
                .lock()
                .map_err(|_| PyException::new_err("Lock poisoned"))?;
            ctx.running_step = None;

            // Track if step made progress
            if self.step_ctx.has_advanced() {
                ctx.mark_advanced();
            }
        }

        // If step was noop (already completed), just return
        if self.step_ctx.is_noop() {
            return Ok(false); // Don't suppress exceptions
        }

        // Check if JobInterrupted was raised
        if let Some(exc_type) = exc_type {
            let job_interrupted = py.get_type::<JobInterrupted>();
            if exc_type.is(&job_interrupted) {
                // JobInterrupted: save current state and propagate
                debug!(
                    "Step '{}' interrupted, state saved for resumption",
                    self.step_name
                );
                return Ok(false); // Propagate the exception
            }

            // Other exception: propagate without completing the step
            return Ok(false);
        }

        // Normal exit: mark step as completed
        {
            let mut ctx = self
                .continuation_ctx
                .lock()
                .map_err(|_| PyException::new_err("Lock poisoned"))?;
            ctx.state.complete_step(&self.step_name);
            ctx.mark_dirty();
            ctx.mark_advanced(); // Completing a step counts as progress
            trace!("Step '{}' completed", self.step_name);
        }

        Ok(false) // Don't suppress exceptions
    }

    fn __repr__(&self) -> String {
        format!(
            "StepContextManager(step='{}', entered={})",
            self.step_name, self.entered
        )
    }
}

// Helper functions for JSON <-> Python conversion

#[cfg(feature = "python")]
fn json_to_pyobject(py: Python<'_>, value: &serde_json::Value) -> PyResult<Py<PyAny>> {
    match value {
        serde_json::Value::Null => Ok(py.None()),
        serde_json::Value::Bool(b) => Ok((*b).into_pyobject(py)?.to_owned().unbind().into()),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(i.into_pyobject(py)?.into())
            } else if let Some(f) = n.as_f64() {
                Ok(f.into_pyobject(py)?.into())
            } else {
                Ok(py.None())
            }
        }
        serde_json::Value::String(s) => Ok(s.into_pyobject(py)?.into()),
        serde_json::Value::Array(arr) => {
            let list = pyo3::types::PyList::empty(py);
            for item in arr {
                list.append(json_to_pyobject(py, item)?)?;
            }
            Ok(list.into_pyobject(py)?.into())
        }
        serde_json::Value::Object(obj) => {
            let dict = PyDict::new(py);
            for (k, v) in obj {
                dict.set_item(k, json_to_pyobject(py, v)?)?;
            }
            Ok(dict.into_pyobject(py)?.into())
        }
    }
}

#[cfg(feature = "python")]
fn pyobject_to_json(obj: &Bound<'_, pyo3::PyAny>) -> PyResult<serde_json::Value> {
    if obj.is_none() {
        return Ok(serde_json::Value::Null);
    }
    if let Ok(b) = obj.extract::<bool>() {
        return Ok(serde_json::Value::Bool(b));
    }
    if let Ok(i) = obj.extract::<i64>() {
        return Ok(serde_json::Value::Number(i.into()));
    }
    if let Ok(f) = obj.extract::<f64>() {
        return Ok(serde_json::json!(f));
    }
    if let Ok(s) = obj.extract::<String>() {
        return Ok(serde_json::Value::String(s));
    }
    if let Ok(list) = obj.cast::<pyo3::types::PyList>() {
        let mut arr = Vec::new();
        for item in list.iter() {
            arr.push(pyobject_to_json(&item)?);
        }
        return Ok(serde_json::Value::Array(arr));
    }
    if let Ok(dict) = obj.cast::<PyDict>() {
        let mut map = serde_json::Map::new();
        for (k, v) in dict.iter() {
            let key = k.extract::<String>()?;
            map.insert(key, pyobject_to_json(&v)?);
        }
        return Ok(serde_json::Value::Object(map));
    }

    // Fallback: try to convert to string
    let s = obj.str()?.extract::<String>()?;
    Ok(serde_json::Value::String(s))
}

/// Parse continuation state from job arguments JSON
pub fn parse_continuation_from_arguments(
    arguments: Option<&str>,
) -> (ContinuationState, i32, serde_json::Value) {
    let default_args = serde_json::Value::Array(vec![]);

    let Some(args_str) = arguments else {
        return (ContinuationState::new(), 0, default_args);
    };

    let Ok(mut json) = serde_json::from_str::<serde_json::Value>(args_str) else {
        return (ContinuationState::new(), 0, default_args);
    };

    // Extract continuation state if present
    let state = json
        .get("continuation")
        .and_then(|c| serde_json::from_value::<ContinuationState>(c.clone()).ok())
        .unwrap_or_default();

    // Extract resumptions count
    let resumptions = json
        .get("resumptions")
        .and_then(|r| r.as_i64())
        .unwrap_or(0) as i32;

    // Get the original arguments (remove continuation metadata)
    if let serde_json::Value::Object(ref mut obj) = json {
        obj.remove("continuation");
        obj.remove("resumptions");
    }

    (state, resumptions, json)
}

/// Serialize continuation state back into job arguments
pub fn serialize_continuation_to_arguments(
    original_args: &serde_json::Value,
    state: &ContinuationState,
    resumptions: i32,
) -> serde_json::Value {
    let mut result = original_args.clone();

    // Ensure it's an object
    if !result.is_object() {
        result = serde_json::json!({
            "arguments": original_args
        });
    }

    // Add continuation state
    if let serde_json::Value::Object(ref mut obj) = result {
        obj.insert(
            "continuation".to_string(),
            serde_json::to_value(state).unwrap_or(serde_json::Value::Null),
        );
        obj.insert("resumptions".to_string(), serde_json::json!(resumptions));
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_continuation_state() {
        let mut state = ContinuationState::new();

        assert!(!state.is_completed("step1"));

        state.complete_step("step1");
        assert!(state.is_completed("step1"));

        state.set_current("step2", serde_json::json!(42));
        assert_eq!(state.get_cursor("step2"), Some(serde_json::json!(42)));
        assert_eq!(state.get_cursor("step1"), None);
    }

    #[test]
    fn test_parse_continuation() {
        let args = r#"{"arguments": [1, 2], "continuation": {"completed": ["step1"], "current": ["step2", 10]}, "resumptions": 2}"#;

        let (state, resumptions, _) = parse_continuation_from_arguments(Some(args));

        assert_eq!(resumptions, 2);
        assert!(state.is_completed("step1"));
        assert_eq!(
            state.current,
            Some(("step2".to_string(), serde_json::json!(10)))
        );
    }

    #[test]
    fn test_serialize_continuation() {
        let args = serde_json::json!({"arguments": [1, 2]});
        let mut state = ContinuationState::new();
        state.complete_step("step1");
        state.set_current("step2", serde_json::json!(42));

        let result = serialize_continuation_to_arguments(&args, &state, 1);

        assert_eq!(result["resumptions"], 1);
        assert!(result["continuation"]["completed"]
            .as_array()
            .unwrap()
            .contains(&serde_json::json!("step1")));
    }
}
