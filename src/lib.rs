mod config;
mod context;
mod control_plane;
mod core;
mod dispatcher;
mod entities;
mod error;
mod notify;
mod process;
mod proctitle_macos;
mod scheduler;
mod semaphore;
mod supervisor;
mod types;
mod utils;
mod worker;

pub use control_plane::ControlPlaneExt;
pub use error::{QuebecError, Result};
use context::*;
use entities::quebec_claimed_executions;
use entities::quebec_jobs;

use types::*;
use worker::{Execution, Runnable};

use pyo3::prelude::*;



// pyo3::create_exception!(quebec, CustomError, PyException);

// Initialize macOS process title system early
#[cfg(target_os = "macos")]
fn init_proctitle() {
    crate::proctitle_macos::init();
}

#[cfg(not(target_os = "macos"))]
fn init_proctitle() {
    // No initialization needed for other platforms
}

/// A Python module implemented in Rust. The name of this function must match
/// the `lib.name` setting in the `Cargo.toml`, else Python will not be able to
/// import the module.
#[pymodule]
fn quebec(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Initialize process title system early
    init_proctitle();

    // pyo3_log::init(); // Can cause mysterious deadlocks

    // Use tracing
    {
        // Use custom EnvFilter, default to DEBUG level
        let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("debug"));

        // Check for QUEBEC_COLOR environment variable to override ANSI detection
        let use_ansi = match std::env::var("QUEBEC_COLOR").as_deref() {
            Ok("always") => true,
            Ok("never") => false,
            _ => atty::is(atty::Stream::Stdout), // default behavior
        };

        let subscriber = tracing_subscriber::FmtSubscriber::builder()
            .with_max_level(tracing::Level::DEBUG)
            .with_env_filter(env_filter)
            .with_line_number(true)
            .with_ansi(use_ansi)
            .finish();

        tracing::subscriber::set_global_default(subscriber)
            .expect("setting default subscriber failed");
    }

    // debug!("duration: {:?}", parse_duration("1s"));
    // debug!("-------------- is_running_in_pyo3: {:?}", is_running_in_pyo3());

    m.add_class::<PyQuebec>()?;
    m.add_class::<ActiveJob>()?;
    m.add_class::<ConcurrencyStrategy>()?;
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

    // Configuration functions

    Ok(())
}
