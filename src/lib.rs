mod config;
pub mod context;
pub mod continuation;
mod control_plane;
#[cfg(feature = "python")]
mod core;
mod dispatcher;
pub mod entities;
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
pub mod semaphore;
mod supervisor;
#[cfg(feature = "python")]
mod types;
pub mod utils;
#[cfg(feature = "python")]
mod worker;

#[cfg(feature = "python")]
use context::*;
pub use control_plane::ControlPlaneExt;
#[cfg(feature = "python")]
use entities::quebec_claimed_executions;
#[cfg(feature = "python")]
use entities::quebec_jobs;
pub use error::{QuebecError, Result};

#[cfg(feature = "python")]
use types::*;
#[cfg(feature = "python")]
use worker::{Execution, Runnable};

#[cfg(feature = "python")]
use pyo3::prelude::*;

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

/// Global flag for `silence_polling` — checked by [`SilencePollingFilter`].
/// Defaults to `true`; [`AppContext`] syncs it on init.
static SILENCE_POLLING: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(true);

/// Update the global silence_polling flag (called from AppContext init).
pub fn set_silence_polling(enabled: bool) {
    SILENCE_POLLING.store(enabled, std::sync::atomic::Ordering::Relaxed);
}

/// Filter that suppresses sqlx/sea_orm query logging inside "polling" spans.
/// Similar to Solid Queue's `silence_polling` option.
struct SilencePollingFilter;

impl<S> tracing_subscriber::layer::Filter<S> for SilencePollingFilter
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    fn enabled(
        &self,
        meta: &tracing::Metadata<'_>,
        cx: &tracing_subscriber::layer::Context<'_, S>,
    ) -> bool {
        if !SILENCE_POLLING.load(std::sync::atomic::Ordering::Relaxed) {
            return true;
        }
        if !meta.target().starts_with("sqlx") && !meta.target().starts_with("sea_orm") {
            return true;
        }
        let Some(current) = cx.lookup_current() else {
            return true;
        };
        !current.scope().any(|span| span.name() == "polling")
    }
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
            colored::control::set_override(false);
            let subscriber = tracing_subscriber::registry()
                .with(
                    tracing_subscriber::fmt::layer()
                        .json()
                        .flatten_event(true)
                        .with_current_span(false)
                        .with_line_number(true)
                        .with_filter(SilencePollingFilter),
                )
                .with(env_filter);
            tracing::subscriber::set_global_default(subscriber)
        }
        "logfmt" => {
            colored::control::set_override(false);
            let subscriber = tracing_subscriber::registry()
                .with(tracing_logfmt::layer().with_filter(SilencePollingFilter))
                .with(env_filter);
            tracing::subscriber::set_global_default(subscriber)
        }
        _ => {
            let subscriber = tracing_subscriber::registry()
                .with(
                    tracing_subscriber::fmt::layer()
                        .with_line_number(true)
                        .with_ansi(use_ansi)
                        .with_filter(SilencePollingFilter),
                )
                .with(env_filter);
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
#[cfg(feature = "python")]
#[pymodule]
fn quebec(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Initialize process title system early
    init_proctitle();

    // pyo3_log::init(); // Can cause mysterious deadlocks

    // Initialize logging with format support (console/json/logfmt)
    init_logging();

    m.add_class::<PyQuebec>()?;
    m.add_class::<ActiveJob>()?;
    m.add_class::<AsgiRequest>()?;
    m.add_class::<ConcurrencyStrategy>()?;
    m.add_class::<ConcurrencyConflict>()?;
    m.add_class::<RescueStrategy>()?;
    m.add_class::<RetryStrategy>()?;
    m.add_class::<DiscardStrategy>()?;
    m.add_class::<quebec_jobs::Model>()?;
    m.add_class::<quebec_claimed_executions::Model>()?;
    m.add_class::<Runnable>()?;
    m.add_class::<Execution>()?;

    // Continuation classes
    m.add_class::<continuation::Continuable>()?;
    m.add_class::<continuation::StepContext>()?;
    m.add_class::<continuation::StepContextManager>()?;
    m.add(
        "JobInterrupted",
        <continuation::JobInterrupted as pyo3::PyTypeInfo>::type_object(_py),
    )?;
    m.add(
        "InvalidStepError",
        <continuation::InvalidStepError as pyo3::PyTypeInfo>::type_object(_py),
    )?;

    // Configuration classes
    m.add_class::<config::QueueConfig>()?;
    m.add_class::<config::WorkerConfig>()?;
    m.add_class::<config::DispatcherConfig>()?;
    m.add_class::<config::DatabaseConfig>()?;

    // Add version information
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;

    Ok(())
}
