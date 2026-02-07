#[cfg(feature = "python")]
use pyo3::prelude::*;
#[cfg(feature = "python")]
use pyo3::types::{PyDict, PyFloat, PyInt, PyList, PyString, PyTuple};
use serde_json::Value;
#[cfg(feature = "python")]
use serde_yaml;

/// Alphabet for generating lowercase alphanumeric nanoid (a-z, 0-9)
const NANOID_ALPHABET: [char; 36] = [
    'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's',
    't', 'u', 'v', 'w', 'x', 'y', 'z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
];

/// Generate a 12-character lowercase alphanumeric job ID using nanoid
pub fn generate_job_id() -> String {
    nanoid::nanoid!(12, &NANOID_ALPHABET)
}

/// Convert YAML value to Python object
#[cfg(feature = "python")]
pub fn yaml_value_to_python(py: Python<'_>, value: &serde_yaml::Value) -> PyResult<Py<PyAny>> {
    match value {
        serde_yaml::Value::Null => Ok(py.None()),
        serde_yaml::Value::Bool(b) => Ok(b.into_pyobject(py)?.as_any().clone().unbind()),
        serde_yaml::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(i.into_pyobject(py)?.as_any().clone().unbind())
            } else if let Some(f) = n.as_f64() {
                Ok(f.into_pyobject(py)?.as_any().clone().unbind())
            } else {
                Ok(py.None())
            }
        }
        serde_yaml::Value::String(s) => Ok(s.into_pyobject(py)?.as_any().clone().unbind()),
        serde_yaml::Value::Sequence(seq) => {
            let py_list = pyo3::types::PyList::empty(py);
            seq.iter().try_for_each(|item| -> PyResult<()> {
                py_list.append(yaml_value_to_python(py, item)?)?;
                Ok(())
            })?;
            Ok(py_list.as_any().clone().unbind())
        }
        serde_yaml::Value::Mapping(map) => {
            let py_dict = pyo3::types::PyDict::new(py);
            for (k, v) in map {
                let py_key = yaml_value_to_python(py, k)?;
                let py_value = yaml_value_to_python(py, v)?;
                py_dict.set_item(py_key, py_value)?;
            }
            Ok(py_dict.as_any().clone().unbind())
        }
        serde_yaml::Value::Tagged(_) => Ok(py.None()), // Simplified handling
    }
}

/// Convert Python object to JSON value
#[cfg(feature = "python")]
pub fn python_to_json_value(obj: &Bound<'_, PyAny>) -> PyResult<Value> {
    if obj.is_instance_of::<PyInt>() {
        Ok(Value::Number(obj.extract::<i64>()?.into()))
    } else if obj.is_instance_of::<PyFloat>() {
        let f = obj.extract::<f64>()?;
        Ok(Value::Number(serde_json::Number::from_f64(f).ok_or_else(
            || PyErr::new::<pyo3::exceptions::PyValueError, _>("Invalid float value"),
        )?))
    } else if obj.is_instance_of::<PyString>() {
        Ok(Value::String(obj.extract::<String>()?))
    } else if obj.is_instance_of::<PyDict>() {
        let dict = obj.cast::<PyDict>()?;
        let mut map = serde_json::Map::with_capacity(dict.len());
        for (key, value) in dict {
            let key: String = key.extract()?;
            let value = python_to_json_value(&value)?;
            map.insert(key, value);
        }
        Ok(Value::Object(map))
    } else if obj.is_instance_of::<PyList>() {
        let list = obj.cast::<PyList>()?;
        let vec = list
            .iter()
            .map(|item| python_to_json_value(&item))
            .collect::<PyResult<Vec<_>>>()?;
        Ok(Value::Array(vec))
    } else if obj.is_instance_of::<PyTuple>() {
        let tuple = obj.cast::<PyTuple>()?;
        let vec = tuple
            .iter()
            .map(|item| python_to_json_value(&item))
            .collect::<PyResult<Vec<_>>>()?;
        Ok(Value::Array(vec))
    } else if obj.is_none() {
        Ok(Value::Null)
    } else {
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "Unsupported Python type",
        ))
    }
}

/// Convert JSON value to Python object
#[cfg(feature = "python")]
pub fn json_value_to_python(py: Python<'_>, value: &Value) -> PyResult<Py<PyAny>> {
    match value {
        Value::Null => Ok(py.None()),
        Value::Bool(b) => Ok(b.into_pyobject(py)?.as_any().clone().unbind()),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(i.into_pyobject(py)?.as_any().clone().unbind())
            } else if let Some(f) = n.as_f64() {
                Ok(f.into_pyobject(py)?.as_any().clone().unbind())
            } else {
                Ok(py.None())
            }
        }
        Value::String(s) => Ok(s.into_pyobject(py)?.as_any().clone().unbind()),
        Value::Array(arr) => {
            let py_list = pyo3::types::PyList::empty(py);
            arr.iter().try_for_each(|item| -> PyResult<()> {
                py_list.append(json_value_to_python(py, item)?)?;
                Ok(())
            })?;
            Ok(py_list.as_any().clone().unbind())
        }
        Value::Object(obj) => {
            let py_dict = pyo3::types::PyDict::new(py);
            for (k, v) in obj {
                let py_key = k.into_pyobject(py)?.as_any().clone().unbind();
                let py_value = json_value_to_python(py, v)?;
                py_dict.set_item(py_key, py_value)?;
            }
            Ok(py_dict.as_any().clone().unbind())
        }
    }
}

// Wrapper types for implementing Into-like trait patterns
// These provide a more idiomatic Rust API for type conversions

/// Wrapper for Python objects that provides idiomatic conversion methods
#[cfg(feature = "python")]
#[derive(Debug)]
pub struct PythonObject<'a>(pub &'a Bound<'a, PyAny>);

#[cfg(feature = "python")]
impl<'a> PythonObject<'a> {
    /// Convert Python object to JSON value using idiomatic Rust pattern
    pub fn into_json(self) -> PyResult<Value> {
        python_to_json_value(self.0)
    }
}

// Trait for types that can be converted to Python objects
#[cfg(feature = "python")]
pub trait IntoPython {
    fn into_python(self, py: Python<'_>) -> PyResult<Py<PyAny>>;
}

// Implement for direct value references
#[cfg(feature = "python")]
impl IntoPython for &serde_yaml::Value {
    fn into_python(self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        yaml_value_to_python(py, self)
    }
}

#[cfg(feature = "python")]
impl IntoPython for &serde_json::Value {
    fn into_python(self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        json_value_to_python(py, self)
    }
}

// Implement for Vec<serde_yaml::Value> and Vec<serde_json::Value>
#[cfg(feature = "python")]
impl IntoPython for &Vec<serde_yaml::Value> {
    fn into_python(self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let py_list = pyo3::types::PyList::empty(py);
        for item in self {
            let py_item = yaml_value_to_python(py, item)?;
            py_list.append(py_item)?;
        }
        Ok(py_list.as_any().clone().unbind())
    }
}

#[cfg(feature = "python")]
impl IntoPython for &Vec<serde_json::Value> {
    fn into_python(self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let py_list = pyo3::types::PyList::empty(py);
        for item in self {
            let py_item = json_value_to_python(py, item)?;
            py_list.append(py_item)?;
        }
        Ok(py_list.as_any().clone().unbind())
    }
}

// Implement for Python Bound types (already Python objects)
#[cfg(feature = "python")]
impl<'a> IntoPython for &Bound<'a, pyo3::types::PyTuple> {
    fn into_python(self, _py: Python<'_>) -> PyResult<Py<PyAny>> {
        Ok(self.as_any().clone().unbind())
    }
}

#[cfg(feature = "python")]
impl<'a> IntoPython for &Bound<'a, pyo3::types::PyDict> {
    fn into_python(self, _py: Python<'_>) -> PyResult<Py<PyAny>> {
        Ok(self.as_any().clone().unbind())
    }
}

#[cfg(feature = "python")]
impl<'a> IntoPython for &Bound<'a, pyo3::types::PyList> {
    fn into_python(self, _py: Python<'_>) -> PyResult<Py<PyAny>> {
        Ok(self.as_any().clone().unbind())
    }
}

#[cfg(feature = "python")]
impl<'a> IntoPython for &Bound<'a, pyo3::PyAny> {
    fn into_python(self, _py: Python<'_>) -> PyResult<Py<PyAny>> {
        Ok(self.clone().unbind())
    }
}

/// Create a PythonObject wrapper for idiomatic conversions
#[cfg(feature = "python")]
pub fn python_object<'a>(obj: &'a Bound<'a, PyAny>) -> PythonObject<'a> {
    PythonObject(obj)
}

/// Extract the `executions` count from an arguments JSON string.
/// Handles both the wrapped format `{"arguments": [...], "executions": N, ...}`
/// and legacy plain arrays `[...]` (returns 0).
pub fn get_executions(arguments: Option<&str>) -> i32 {
    arguments
        .and_then(|s| serde_json::from_str::<Value>(s).ok())
        .and_then(|v| v.get("executions")?.as_i64())
        .unwrap_or(0) as i32
}

/// Get the per-exception-type execution count from `exception_executions`.
/// Matches ActiveJob's `executions_for(exceptions)` which tracks retries independently
/// per retry_on declaration.
pub fn get_exception_executions(arguments: Option<&str>, key: &str) -> i32 {
    arguments
        .and_then(|s| serde_json::from_str::<Value>(s).ok())
        .and_then(|v| v.get("exception_executions")?.get(key)?.as_i64())
        .unwrap_or(0) as i32
}

/// Return a new arguments JSON string with `executions` incremented by 1
/// and `exception_key` accumulated into `exception_executions`.
/// The key should be the strategy's exception list (e.g. "[RuntimeError, IOError]")
/// matching ActiveJob's `executions_for(exceptions)` format.
/// If the input is a plain array, wraps it first.
pub fn increment_executions(arguments: Option<&str>, exception_key: Option<&str>) -> String {
    let mut obj = arguments
        .and_then(|s| serde_json::from_str::<Value>(s).ok())
        .unwrap_or(Value::Array(vec![]));

    // Wrap non-object values into the canonical format
    if !obj.is_object() {
        let args = if obj.is_array() {
            obj
        } else {
            Value::Array(vec![])
        };
        obj = serde_json::json!({
            "arguments": args,
            "executions": 0,
            "exception_executions": {},
        });
    }

    // Increment executions
    let executions = obj.get("executions").and_then(|v| v.as_i64()).unwrap_or(0) + 1;
    obj["executions"] = Value::from(executions);

    // Accumulate exception_executions
    if let Some(key) = exception_key {
        // Ensure exception_executions is an object (reset if missing/wrong type)
        if !obj
            .get("exception_executions")
            .is_some_and(|v| v.is_object())
        {
            obj["exception_executions"] = serde_json::json!({});
        }
        let current = obj["exception_executions"]
            .get(&key)
            .and_then(|v| v.as_i64())
            .unwrap_or(0);
        obj["exception_executions"][&key] = Value::from(current + 1);
    }

    serde_json::to_string(&obj).unwrap_or_else(|_| "{}".to_string())
}

/// Parse environment-specific configuration from a HashMap with fallback behavior
///
/// This is a generic helper for parsing environment-based YAML configs like:
/// ```yaml
/// production:
///   task1: {...}
///   task2: {...}
/// development:
///   task1: {...}
/// ```
///
/// Returns the config for the specified environment, falling back to the first
/// available environment if the specified one isn't found.
pub fn parse_env_config_cloneable<T>(
    env_config: std::collections::HashMap<String, T>,
) -> anyhow::Result<T>
where
    T: Clone + std::fmt::Debug,
{
    use tracing::{info, warn};

    let env = std::env::var("QUEBEC_ENV").unwrap_or_else(|_| "development".to_string());
    info!("Using environment: {}", env);

    if let Some(config) = env_config.get(&env) {
        info!("Loaded configuration from environment '{}'", env);
        return Ok(config.clone());
    }

    warn!(
        "Environment '{}' not found in config, using first environment",
        env
    );

    if let Some((first_env, config)) = env_config.iter().next() {
        info!("Using environment '{}' instead", first_env);
        return Ok(config.clone());
    }

    warn!("No environments found in configuration");
    Err(anyhow::anyhow!("No environments found in configuration"))
}

/// Parse environment-specific configuration from a HashMap (strict mode)
///
/// This version returns an error if the specified environment is not found.
/// Used when you want to enforce that the environment must exist.
pub fn parse_env_config_strict<T>(
    env_config: std::collections::HashMap<String, T>,
    env: Option<&str>,
) -> anyhow::Result<T>
where
    T: Clone + std::fmt::Debug,
{
    use tracing::info;

    let environment = env
        .map(|s| s.to_string())
        .or_else(|| std::env::var("QUEBEC_ENV").ok())
        .unwrap_or_else(|| "development".to_string());

    info!("Using environment: {}", environment);

    if let Some(config) = env_config.get(&environment) {
        info!("Loaded configuration from environment '{}'", environment);
        return Ok(config.clone());
    }

    // Environment not found, return error with available environments
    let available: Vec<String> = env_config.keys().cloned().collect();
    anyhow::bail!(
        "Environment '{}' not found in config. Available environments: {:?}",
        environment,
        available
    )
}
