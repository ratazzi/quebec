mod context;
mod core;
mod dispatcher;
mod entities;
mod process;
mod scheduler;
mod semaphore;
mod supervisor;
mod types;
mod worker;
mod web;

use context::*;
use entities::solid_queue_claimed_executions;
use entities::solid_queue_jobs;
use process::is_running_in_pyo3;
use types::*;
use worker::{Execution, Metric, Runnable};

use pyo3::prelude::*;



// pyo3::create_exception!(quebec, CustomError, PyException);

/// A Python module implemented in Rust. The name of this function must match
/// the `lib.name` setting in the `Cargo.toml`, else Python will not be able to
/// import the module.
#[pymodule]
fn quebec(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    // 会导致莫名其妙的死锁
    // pyo3_log::init();

    // 使用 tracing
    {
        // 使用自定义的 EnvFilter，默认开启 DEBUG 级别
        let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("debug"));

        let subscriber = tracing_subscriber::FmtSubscriber::builder()
            .with_max_level(tracing::Level::DEBUG)
            .with_env_filter(env_filter)
            .with_line_number(true)
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
    m.add_class::<solid_queue_jobs::Model>()?;
    m.add_class::<solid_queue_claimed_executions::Model>()?;
    m.add_class::<Runnable>()?;
    m.add_class::<Execution>()?;
    // m.add_class::<Metric>()?;

    // m.add("CustomError", py.get_type_bound::<CustomError>())?;

    Ok(())
}
