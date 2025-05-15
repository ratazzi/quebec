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
pub mod web;

use context::*;
use entities::solid_queue_claimed_executions;
use entities::solid_queue_jobs;
use process::is_running_in_pyo3;
use types::*;
use worker::{Execution, Metric, Runnable};

use pyo3::prelude::*;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

#[cfg(feature = "use-log")]
use log::{debug, error, info, trace, warn};

#[cfg(feature = "use-tracing")]
use tracing::{debug, error, info, trace, warn};

use tracing::{instrument, span, Level};
use tracing_log::LogTracer;
use tracing_subscriber::FmtSubscriber;

use colored::*;
use std::fmt;
use tracing::Event;
use tracing::Subscriber;
use tracing_subscriber::fmt::format;
use tracing_subscriber::fmt::format::{FormatEvent, FormatFields, Writer};
use tracing_subscriber::fmt::FmtContext;
use tracing_subscriber::fmt::FormattedFields;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{fmt::Formatter, fmt::MakeWriter};

struct SqlHighlighter;

impl<S, N> FormatEvent<S, N> for SqlHighlighter
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self, ctx: &FmtContext<'_, S, N>, mut writer: format::Writer<'_>, event: &Event<'_>,
    ) -> fmt::Result {
        // Format values from the event's's metadata:
        let metadata = event.metadata();
        let now = chrono::Utc::now();
        // 格式化为指定格式
        let formatted_time = now.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string().bright_black();

        let level = match *metadata.level() {
            tracing::Level::ERROR => "ERROR".red(),
            tracing::Level::WARN => "WARN".yellow(),
            tracing::Level::INFO => "INFO".green(),
            tracing::Level::DEBUG => "DEBUG".blue(),
            tracing::Level::TRACE => "TRACE".purple(),
        };

        write!(&mut writer, "{} {} {}: ", formatted_time, level, metadata.target())?;

        // Format all the spans in the event's span context.
        if let Some(scope) = ctx.event_scope() {
            for span in scope.from_root() {
                write!(writer, "{}", span.name())?;

                // `FormattedFields` is a formatted representation of the span's
                // fields, which is stored in its extensions by the `fmt` layer's
                // `new_span` method. The fields will have been formatted
                // by the same field formatter that's provided to the event
                // formatter in the `FmtContext`.
                let ext = span.extensions();
                let fields = &ext.get::<FormattedFields<N>>().expect("will never be `None`");

                // Skip formatting the fields if the span had no fields.
                if !fields.is_empty() {
                    write!(writer, "{{{}}}", fields)?;
                }
                write!(writer, ": ")?;
            }
        }

        // Write fields on the event
        ctx.field_format().format_fields(writer.by_ref(), event)?;

        // 获取日志信息
        // let mut log = String::new();
        // let mut visitor = FormattedFields::default();
        // event.record(&mut visitor);
        // visitor.finish(&mut log)?;

        // // 检查日志信息是否包含 SQL 关键字并添加高亮
        // if log.contains("SELECT") || log.contains("INSERT") || log.contains("UPDATE") || log.contains("DELETE") {
        //     write!(writer, "{}", log.blue())?;
        // } else {
        //     write!(writer, "{}", log)?;
        // }

        writeln!(writer)
    }
}

#[pyfunction]
fn run_with_signal_handling() -> PyResult<()> {
    println!("---------------- run_with_signal_handling -----------------");
    let term = Arc::new(AtomicBool::new(false));
    let term_clone = Arc::clone(&term);
    signal_hook::flag::register(signal_hook::consts::SIGINT, term_clone)
        .expect("Failed to register SIGINT handler");
    println!("Running... Press Ctrl+C to stop.");
    Ok(())
}

// pyo3::create_exception!(quebec, CustomError, PyException);

/// A Python module implemented in Rust. The name of this function must match
/// the `lib.name` setting in the `Cargo.toml`, else Python will not be able to
/// import the module.
#[pymodule]
fn quebec(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    // 会导致莫名其妙的死锁
    // pyo3_log::init();

    // 暂时未发现问题
    #[cfg(feature = "use-log")]
    {
        env_logger::Builder::from_default_env().format_timestamp_micros().init();
        debug!("Using log crate");
    }

    // 部分日志不输出
    // let subscriber = FmtSubscriber::builder().with_max_level(Level::DEBUG).finish();
    // tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    #[cfg(feature = "use-tracing")]
    {
        // tracing_subscriber::fmt::init();
        let subscriber = tracing_subscriber::FmtSubscriber::builder()
            // .event_format(SqlHighlighter)
            .with_max_level(tracing::Level::DEBUG)
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .finish();
        tracing::subscriber::set_global_default(subscriber)
            .expect("setting default subscriber failed");
        // debug!("Using tracing crate");
    }

    // debug!("duration: {:?}", parse_duration("1s"));
    // debug!("-------------- is_running_in_pyo3: {:?}", is_running_in_pyo3());

    m.add_function(wrap_pyfunction!(run_with_signal_handling, m)?)?;

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
