use sea_orm::DbErr;
use thiserror::Error;

/// Unified error type for Quebec
#[derive(Error, Debug)]
pub enum QuebecError {
    /// Database related errors
    #[error("Database error: {0}")]
    Database(#[from] DbErr),
    
    /// Transaction errors
    #[error("Transaction error: {0}")]
    Transaction(String),
    
    /// JSON serialization/deserialization errors
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    
    /// YAML serialization/deserialization errors
    #[error("YAML error: {0}")]
    Yaml(#[from] serde_yaml::Error),
    
    /// URL parsing errors
    #[error("URL parse error: {0}")]
    UrlParse(#[from] url::ParseError),
    
    /// IO errors
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    /// Python interop errors
    #[error("Python error: {0}")]
    Python(String),
    
    /// Job not found
    #[error("Job not found: {id}")]
    JobNotFound { id: i32 },
    
    /// Job class not registered
    #[error("Job class not registered: {class_name}")]
    JobClassNotRegistered { class_name: String },
    
    /// Concurrency limit exceeded
    #[error("Concurrency limit exceeded for key: {key}")]
    ConcurrencyLimitExceeded { key: String },
    
    /// Configuration errors
    #[error("Configuration error: {0}")]
    Config(String),
    
    /// Invalid cron expression
    #[error("Invalid cron expression: {0}")]
    InvalidCron(String),
    
    /// Runtime errors
    #[error("Runtime error: {0}")]
    Runtime(String),
    
    /// Generic errors (for conversion from anyhow::Error)
    #[error("{0}")]
    Generic(String),
    
    /// Timeout errors
    #[error("Operation timed out: {0}")]
    Timeout(String),
    
    /// Validation errors
    #[error("Validation error: {0}")]
    Validation(String),
    
    /// Unsupported operations
    #[error("Unsupported operation: {0}")]
    Unsupported(String),
}

/// Type alias for simplified usage
pub type Result<T> = std::result::Result<T, QuebecError>;

impl QuebecError {
    /// Create a configuration error
    pub fn config<S: Into<String>>(msg: S) -> Self {
        Self::Config(msg.into())
    }
    
    /// Create a runtime error
    pub fn runtime<S: Into<String>>(msg: S) -> Self {
        Self::Runtime(msg.into())
    }
    
    /// Create a validation error
    pub fn validation<S: Into<String>>(msg: S) -> Self {
        Self::Validation(msg.into())
    }
    
    /// Create a generic error
    pub fn generic<S: Into<String>>(msg: S) -> Self {
        Self::Generic(msg.into())
    }
}

/// Convert from anyhow::Error
impl From<anyhow::Error> for QuebecError {
    fn from(err: anyhow::Error) -> Self {
        Self::Generic(err.to_string())
    }
}

/// Convert from pyo3::PyErr
impl From<pyo3::PyErr> for QuebecError {
    fn from(err: pyo3::PyErr) -> Self {
        Self::Python(err.to_string())
    }
}

/// Convert to pyo3::PyErr
impl From<QuebecError> for pyo3::PyErr {
    fn from(err: QuebecError) -> pyo3::PyErr {
        use pyo3::exceptions::PyRuntimeError;
        PyRuntimeError::new_err(err.to_string())
    }
}

/// Convert from croner errors
impl From<croner::errors::CronError> for QuebecError {
    fn from(err: croner::errors::CronError) -> Self {
        Self::InvalidCron(err.to_string())
    }
}

/// Convert from tokio_postgres errors
impl From<tokio_postgres::Error> for QuebecError {
    fn from(err: tokio_postgres::Error) -> Self {
        Self::Database(DbErr::Custom(err.to_string()))
    }
}

/// Convert from TransactionError
impl<E> From<sea_orm::TransactionError<E>> for QuebecError 
where 
    E: std::error::Error + Send + Sync + 'static,
{
    fn from(err: sea_orm::TransactionError<E>) -> Self {
        match err {
            sea_orm::TransactionError::Connection(e) => Self::Database(e),
            sea_orm::TransactionError::Transaction(e) => Self::Transaction(e.to_string()),
        }
    }
}

/// Helper macro for quickly creating errors
#[macro_export]
macro_rules! quebec_error {
    ($variant:ident, $($arg:tt)*) => {
        $crate::error::QuebecError::$variant(format!($($arg)*))
    };
}

/// Helper macro for quickly returning errors
#[macro_export]
macro_rules! bail {
    ($($arg:tt)*) => {
        return Err($crate::error::QuebecError::Runtime(format!($($arg)*)))
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_error_display() {
        let err = QuebecError::JobNotFound { id: 42 };
        assert_eq!(err.to_string(), "Job not found: 42");
        
        let err = QuebecError::config("invalid setting");
        assert_eq!(err.to_string(), "Configuration error: invalid setting");
    }
    
    #[test]
    fn test_error_conversion() {
        let db_err = DbErr::Custom("test error".to_string());
        let quebec_err: QuebecError = db_err.into();
        match quebec_err {
            QuebecError::Database(_) => {}
            _ => panic!("Expected Database variant"),
        }
    }
}