use crate::context::AppContext;
use crate::entities::*;
use anyhow::Error;
use anyhow::Result;
use async_trait::async_trait;
use sea_orm::TransactionTrait;
use sea_orm::*;
use std::sync::Arc;

use pyo3::prelude::*;

use tracing::{info, trace};

pub fn is_running_in_pyo3() -> bool {
    let result = std::panic::catch_unwind(|| {
        Python::with_gil(|_py| {
            // If GIL can be acquired, assume running in PyO3 environment
            true
        })
    });
    result.unwrap_or(false) // Return false if panic occurs
}

/// Process information for registration
#[derive(Debug, Clone)]
pub struct ProcessInfo {
    /// Process kind (e.g., "Worker", "Dispatcher", "Scheduler")
    pub kind: String,
    /// Process name (e.g., "worker", "dispatcher", "scheduler")
    pub name: String,
}

impl ProcessInfo {
    pub fn new(kind: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            kind: kind.into(),
            name: name.into(),
        }
    }
}

/// Trait for background processes with lifecycle management
///
/// Implementors should provide:
/// - `ctx()`: Access to the application context
/// - `process_info()`: Process identification info
///
/// Default implementations are provided for:
/// - `on_start()`: Register process in database
/// - `heartbeat()`: Update process heartbeat
/// - `on_stop()`: Deregister process from database
#[async_trait]
pub trait ProcessTrait: Send + Sync {
    /// Get the application context
    fn ctx(&self) -> &Arc<AppContext>;

    /// Get process identification info
    fn process_info(&self) -> ProcessInfo;

    /// Called when process starts - registers in database
    async fn on_start(&self, db: &DatabaseConnection) -> Result<quebec_processes::Model, Error> {
        let info = self.process_info();
        let process = db
            .transaction::<_, quebec_processes::ActiveModel, DbErr>(|txn| {
                let hostname = Self::get_hostname();
                let pid = Self::get_pid();
                let supervisor_id = Self::get_supervisor_id();
                let metadata = Self::get_metadata();
                let kind = info.kind.clone();
                let name = info.name.clone();

                Box::pin(async move {
                    let process = quebec_processes::ActiveModel {
                        id: ActiveValue::NotSet,
                        kind: ActiveValue::Set(kind),
                        name: ActiveValue::Set(name),
                        supervisor_id: ActiveValue::Set(supervisor_id),
                        metadata: ActiveValue::Set(metadata),
                        last_heartbeat_at: ActiveValue::Set(chrono::Utc::now().naive_utc()),
                        pid: ActiveValue::Set(pid),
                        hostname: ActiveValue::Set(hostname),
                        created_at: ActiveValue::Set(chrono::Utc::now().naive_utc()),
                    }
                    .save(txn)
                    .await?;

                    Ok(process)
                })
            })
            .await?;

        let process = process.try_into_model()?;
        info!(
            "Process started: name={} pid={} hostname={:?}",
            process.name, process.pid, process.hostname
        );

        Ok(process)
    }

    fn get_hostname() -> Option<String> {
        hostname::get().map(|h| h.to_string_lossy().to_string()).ok()
    }

    fn get_pid() -> i32 {
        std::process::id() as i32
    }

    fn get_tid() -> String {
        let mut tid = format!("{:?}", std::thread::current().id());

        if is_running_in_pyo3() {
            let mut thread_id: u64 = 0;
            Python::with_gil(|py| {
                if let Ok(threading) = PyModule::import(py, "threading") {
                    if let Ok(bound) = threading.getattr("get_ident") {
                        if let Ok(ident) = bound.call0() {
                            thread_id = ident.extract::<u64>().unwrap_or(0);
                        }
                    }
                }
            });

            tid = format!("{}", thread_id);
            trace!("python thread_id: {:?}", tid)
        }

        tid
    }

    fn get_supervisor_id() -> Option<i64> {
        None
    }

    fn get_metadata() -> Option<String> {
        None
    }

    async fn heartbeat(
        &self, db: &DatabaseConnection, process: &quebec_processes::Model,
    ) -> Result<(), Error> {
        let mut process = process.clone().into_active_model();
        process.last_heartbeat_at = ActiveValue::Set(chrono::Utc::now().naive_utc());
        process.clone().update(db).await?;

        Ok(())
    }

    async fn on_stop(
        &self, db: &DatabaseConnection, process: &quebec_processes::Model,
    ) -> Result<(), Error> {
        let process_name = process.name.clone();
        let process_pid = process.pid;
        let process_hostname = process.hostname.clone();

        // Delete the process record
        process.clone().delete(db).await?;

        info!(
            "Process stopped: name={} pid={} hostname={:?}",
            process_name, process_pid, process_hostname
        );

        Ok(())
    }
}
