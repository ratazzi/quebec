use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple, PyInt, PyFloat, PyString};
use serde_json::Value;
use serde_yaml;

/// Convert YAML value to Python object
pub fn yaml_value_to_python(py: Python<'_>, value: &serde_yaml::Value) -> PyResult<PyObject> {
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
            for item in seq {
                let py_item = yaml_value_to_python(py, item)?;
                py_list.append(py_item)?;
            }
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
pub fn python_to_json_value(py: Python, obj: &Bound<'_, PyAny>) -> PyResult<Value> {
    if obj.is_instance_of::<PyInt>() {
        Ok(Value::Number(obj.extract::<i64>()?.into()))
    } else if obj.is_instance_of::<PyFloat>() {
        let f = obj.extract::<f64>()?;
        Ok(Value::Number(serde_json::Number::from_f64(f)
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyValueError, _>("Invalid float value"))?))
    } else if obj.is_instance_of::<PyString>() {
        Ok(Value::String(obj.extract::<String>()?))
    } else if obj.is_instance_of::<PyDict>() {
        let dict = obj.downcast::<PyDict>()?;
        let mut map = serde_json::Map::new();
        for (key, value) in dict {
            let key: String = key.extract()?;
            let value = python_to_json_value(py, &value)?;
            map.insert(key, value);
        }
        Ok(Value::Object(map))
    } else if obj.is_instance_of::<PyList>() {
        let list = obj.downcast::<PyList>()?;
        let mut vec = Vec::new();
        for item in list.iter() {
            vec.push(python_to_json_value(py, &item)?);
        }
        Ok(Value::Array(vec))
    } else if obj.is_instance_of::<PyTuple>() {
        let tuple = obj.downcast::<PyTuple>()?;
        let mut vec = Vec::new();
        for item in tuple.iter() {
            vec.push(python_to_json_value(py, &item)?);
        }
        Ok(Value::Array(vec))
    } else if obj.is_none() {
        Ok(Value::Null)
    } else {
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>("Unsupported Python type"))
    }
}

/// Convert JSON value to Python object
pub fn json_value_to_python(py: Python<'_>, value: &Value) -> PyResult<PyObject> {
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
            for item in arr {
                let py_item = json_value_to_python(py, item)?;
                py_list.append(py_item)?;
            }
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

/// Wrapper for YAML values that provides idiomatic conversion methods
///
/// # Example
/// ```rust
/// use quebec::utils::{yaml_value, YamlValue};
/// use serde_yaml::Value as YamlValue;
///
/// let yaml_val = YamlValue::String("hello".to_string());
/// let wrapper = yaml_value(&yaml_val);
///
/// Python::with_gil(|py| {
///     let py_obj = wrapper.into_python(py)?;
///     // py_obj is now a Python string
/// });
/// ```
#[derive(Debug, Clone)]
pub struct YamlValue<'a>(pub &'a serde_yaml::Value);

/// Wrapper for JSON values that provides idiomatic conversion methods
///
/// # Example
/// ```rust
/// use quebec::utils::{json_value, JsonValue};
/// use serde_json::Value as JsonValue;
///
/// let json_val = JsonValue::String("hello".to_string());
/// let wrapper = json_value(&json_val);
///
/// Python::with_gil(|py| {
///     let py_obj = wrapper.into_python(py)?;
///     // py_obj is now a Python string
/// });
/// ```
#[derive(Debug, Clone)]
pub struct JsonValue<'a>(pub &'a Value);

/// Wrapper for Python objects that provides idiomatic conversion methods
///
/// # Example
/// ```rust
/// use quebec::utils::{python_object, PythonObject};
///
/// Python::with_gil(|py| {
///     let py_str = py.eval("'hello'", None, None)?;
///     let wrapper = python_object(&py_str);
///     let json_val = wrapper.into_json(py);
///     // json_val is now a JSON string
/// });
/// ```
#[derive(Debug)]
pub struct PythonObject<'a>(pub &'a Bound<'a, PyAny>);

// Idiomatic Rust conversion methods

impl<'a> YamlValue<'a> {
    /// Convert YAML value to Python object using idiomatic Rust pattern
    ///
    /// This method provides a more ergonomic API compared to the standalone function
    pub fn into_python(self, py: Python<'_>) -> PyResult<PyObject> {
        yaml_value_to_python(py, self.0)
    }
}

impl<'a> JsonValue<'a> {
    /// Convert JSON value to Python object using idiomatic Rust pattern
    ///
    /// This method provides a more ergonomic API compared to the standalone function
    pub fn into_python(self, py: Python<'_>) -> PyResult<PyObject> {
        json_value_to_python(py, self.0)
    }
}

impl<'a> PythonObject<'a> {
    /// Convert Python object to JSON value using idiomatic Rust pattern
    ///
    /// This method provides a more ergonomic API compared to the standalone function
    pub fn into_json(self, py: Python) -> PyResult<Value> {
        python_to_json_value(py, self.0)
    }
}

// Trait for types that can be converted to Python objects
pub trait IntoPython {
    fn into_python(self, py: Python<'_>) -> PyResult<PyObject>;
}

// Implement the trait for our wrapper types
impl<'a> IntoPython for YamlValue<'a> {
    fn into_python(self, py: Python<'_>) -> PyResult<PyObject> {
        yaml_value_to_python(py, self.0)
    }
}

impl<'a> IntoPython for JsonValue<'a> {
    fn into_python(self, py: Python<'_>) -> PyResult<PyObject> {
        json_value_to_python(py, self.0)
    }
}

// Implement for direct value references too
impl IntoPython for &serde_yaml::Value {
    fn into_python(self, py: Python<'_>) -> PyResult<PyObject> {
        yaml_value_to_python(py, self)
    }
}

impl IntoPython for &serde_json::Value {
    fn into_python(self, py: Python<'_>) -> PyResult<PyObject> {
        json_value_to_python(py, self)
    }
}

// Implement for Vec<serde_yaml::Value> and Vec<serde_json::Value>
impl IntoPython for &Vec<serde_yaml::Value> {
    fn into_python(self, py: Python<'_>) -> PyResult<PyObject> {
        let py_list = pyo3::types::PyList::empty(py);
        for item in self {
            let py_item = yaml_value_to_python(py, item)?;
            py_list.append(py_item)?;
        }
        Ok(py_list.as_any().clone().unbind())
    }
}

impl IntoPython for &Vec<serde_json::Value> {
    fn into_python(self, py: Python<'_>) -> PyResult<PyObject> {
        let py_list = pyo3::types::PyList::empty(py);
        for item in self {
            let py_item = json_value_to_python(py, item)?;
            py_list.append(py_item)?;
        }
        Ok(py_list.as_any().clone().unbind())
    }
}

// Implement for Python Bound types (already Python objects)
impl<'a> IntoPython for &Bound<'a, pyo3::types::PyTuple> {
    fn into_python(self, _py: Python<'_>) -> PyResult<PyObject> {
        Ok(self.as_any().clone().unbind())
    }
}

impl<'a> IntoPython for &Bound<'a, pyo3::types::PyDict> {
    fn into_python(self, _py: Python<'_>) -> PyResult<PyObject> {
        Ok(self.as_any().clone().unbind())
    }
}

impl<'a> IntoPython for &Bound<'a, pyo3::types::PyList> {
    fn into_python(self, _py: Python<'_>) -> PyResult<PyObject> {
        Ok(self.as_any().clone().unbind())
    }
}

impl<'a> IntoPython for &Bound<'a, pyo3::PyAny> {
    fn into_python(self, _py: Python<'_>) -> PyResult<PyObject> {
        Ok(self.clone().unbind())
    }
}

// Convenience constructor functions for wrapper types

/// Create a YamlValue wrapper for idiomatic conversions
///
/// # Example
/// ```rust
/// let yaml_val = serde_yaml::Value::String("test".to_string());
/// let wrapper = yaml_value(&yaml_val);
///
/// Python::with_gil(|py| {
///     let py_obj = wrapper.into_python(py)?;
/// });
/// ```
pub fn yaml_value(value: &serde_yaml::Value) -> YamlValue<'_> {
    YamlValue(value)
}

/// Create a JsonValue wrapper for idiomatic conversions
///
/// # Example
/// ```rust
/// let json_val = serde_json::Value::String("test".to_string());
/// let wrapper = json_value(&json_val);
///
/// Python::with_gil(|py| {
///     let py_obj = wrapper.into_python(py)?;
/// });
/// ```
pub fn json_value(value: &Value) -> JsonValue<'_> {
    JsonValue(value)
}

/// Create a PythonObject wrapper for idiomatic conversions
///
/// # Example
/// ```rust
/// Python::with_gil(|py| {
///     let py_str = py.eval("'test'", None, None)?;
///     let wrapper = python_object(&py_str);
///     let json_val = wrapper.into_json(py);
/// });
/// ```
pub fn python_object<'a>(obj: &'a Bound<'a, PyAny>) -> PythonObject<'a> {
    PythonObject(obj)
}