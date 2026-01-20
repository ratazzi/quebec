mod config;
mod context;
mod control_plane;
mod core;
mod dispatcher;
mod entities;
mod error;
mod notify;
mod process;
#[cfg(target_os = "macos")]
mod proctitle_macos;
#[cfg(any(
    target_os = "linux",
    target_os = "freebsd",
    target_os = "openbsd",
    target_os = "netbsd"
))]
mod proctitle_unix;
pub mod query_builder;
mod scheduler;
pub mod schema_builder;
mod semaphore;
mod supervisor;
mod types;
mod utils;
mod worker;

use context::*;
pub use control_plane::ControlPlaneExt;
use entities::quebec_claimed_executions;
use entities::quebec_jobs;
pub use error::{QuebecError, Result};

use types::*;
use worker::{Execution, Runnable};

use pyo3::prelude::*;

// pyo3::create_exception!(quebec, CustomError, PyException);

// Initialize process title system early based on platform
#[cfg(target_os = "macos")]
fn init_proctitle() {
    crate::proctitle_macos::init();
}

#[cfg(any(
    target_os = "linux",
    target_os = "freebsd",
    target_os = "openbsd",
    target_os = "netbsd"
))]
fn init_proctitle() {
    crate::proctitle_unix::init();
}

#[cfg(not(any(
    target_os = "macos",
    target_os = "linux",
    target_os = "freebsd",
    target_os = "openbsd",
    target_os = "netbsd"
)))]
fn init_proctitle() {
    // No initialization needed for other platforms
}

/// Initialize logging with format support via QUEBEC_LOG_FORMAT environment variable.
///
/// Supported formats:
/// - "console" (default): Human-readable colored output
/// - "json": JSON structured logs
/// - "logfmt": key=value format
fn init_logging() {
    use tracing_subscriber::prelude::*;

    // Default to "info" level for production; use RUST_LOG env var to override
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    let format = std::env::var("QUEBEC_LOG_FORMAT").unwrap_or_else(|_| "console".to_string());

    let use_ansi = match std::env::var("QUEBEC_COLOR").as_deref() {
        Ok("always") => true,
        Ok("never") => false,
        _ => atty::is(atty::Stream::Stdout),
    };

    let result = match format.as_str() {
        "json" => {
            // Disable colored crate output for machine-readable format
            colored::control::set_override(false);
            let subscriber = tracing_subscriber::fmt()
                .with_env_filter(env_filter)
                .json()
                .flatten_event(true)
                .with_current_span(false)
                .with_line_number(true)
                .finish();
            tracing::subscriber::set_global_default(subscriber)
        }
        "logfmt" => {
            // Disable colored crate output - tracing-logfmt escapes ANSI codes
            colored::control::set_override(false);
            let subscriber = tracing_subscriber::registry()
                .with(env_filter)
                .with(tracing_logfmt::layer());
            tracing::subscriber::set_global_default(subscriber)
        }
        _ => {
            // console (default)
            let subscriber = tracing_subscriber::fmt()
                .with_env_filter(env_filter)
                .with_line_number(true)
                .with_ansi(use_ansi)
                .finish();
            tracing::subscriber::set_global_default(subscriber)
        }
    };

    if result.is_err() {
        eprintln!(
            "quebec: tracing subscriber already initialized, QUEBEC_LOG_FORMAT={} ignored",
            format
        );
    }
}

/// A Python module implemented in Rust. The name of this function must match
/// the `lib.name` setting in the `Cargo.toml`, else Python will not be able to
/// import the module.
#[pymodule]
fn quebec(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Initialize process title system early
    init_proctitle();

    // pyo3_log::init(); // Can cause mysterious deadlocks

    // Initialize logging with format support (console/json/logfmt)
    init_logging();

    // debug!("duration: {:?}", parse_duration("1s"));
    // debug!("-------------- is_running_in_pyo3: {:?}", is_running_in_pyo3());

    m.add_class::<PyQuebec>()?;
    m.add_class::<ActiveJob>()?;
    m.add_class::<ConcurrencyStrategy>()?;
    m.add_class::<ConcurrencyConflict>()?;
    m.add_class::<RescueStrategy>()?;
    m.add_class::<RetryStrategy>()?;
    m.add_class::<DiscardStrategy>()?;
    m.add_class::<quebec_jobs::Model>()?;
    m.add_class::<quebec_claimed_executions::Model>()?;
    m.add_class::<Runnable>()?;
    m.add_class::<Execution>()?;

    // Configuration classes
    m.add_class::<config::QueueConfig>()?;
    m.add_class::<config::WorkerConfig>()?;
    m.add_class::<config::DispatcherConfig>()?;
    m.add_class::<config::DatabaseConfig>()?;

    // Add version information
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;

    // Configuration functions

    Ok(())
}
