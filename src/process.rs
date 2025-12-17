use crate::context::AppContext;
use crate::entities::quebec_processes;
use crate::query_builder;
use anyhow::Error;
use anyhow::Result;
use async_trait::async_trait;
use sea_orm::{DatabaseConnection, DbErr, TransactionTrait};
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
        let ctx = self.ctx();
        let table_config = ctx.table_config.clone();

        let process = db
            .transaction::<_, quebec_processes::Model, DbErr>(|txn| {
                let hostname = Self::get_hostname();
                let pid = Self::get_pid();
                let metadata = Self::get_metadata();
                let kind = info.kind.clone();
                let name = info.name.clone();

                Box::pin(async move {
                    let process_id = query_builder::processes::insert(
                        txn,
                        &table_config,
                        &kind,
                        pid,
                        hostname.as_deref(),
                        metadata.as_deref(),
                        &name,
                    )
                    .await?;

                    let process =
                        query_builder::processes::find_by_id(txn, &table_config, process_id)
                            .await?
                            .ok_or_else(|| {
                                DbErr::Custom("Failed to find inserted process".to_string())
                            })?;

                    Ok(process)
                })
            })
            .await?;

        info!(
            "Process started: name={} pid={} hostname={:?}",
            process.name, process.pid, process.hostname
        );

        Ok(process)
    }

    fn get_hostname() -> Option<String> {
        hostname::get()
            .map(|h| h.to_string_lossy().to_string())
            .ok()
    }

    fn get_pid() -> i32 {
        std::process::id() as i32
    }

    fn get_tid() -> String {
        let mut tid = format!("{:?}", std::thread::current().id());

        if is_running_in_pyo3() {
            let mut thread_id: u64 = 0;
            Python::with_gil(|py| {
                thread_id = PyModule::import(py, "threading")
                    .and_then(|threading| threading.getattr("get_ident"))
                    .and_then(|bound| bound.call0())
                    .and_then(|ident| ident.extract::<u64>())
                    .unwrap_or(0);
            });

            tid = format!("{}", thread_id);
            trace!("python thread_id: {:?}", tid)
        }

        tid
    }

    // TODO: Will be used when Supervisor process management is implemented
    #[allow(dead_code)]
    fn get_supervisor_id() -> Option<i64> {
        None
    }

    fn get_metadata() -> Option<String> {
        None
    }

    async fn heartbeat(
        &self,
        db: &DatabaseConnection,
        process: &quebec_processes::Model,
    ) -> Result<(), Error> {
        let ctx = self.ctx();
        query_builder::processes::update_heartbeat(db, &ctx.table_config, process.id).await?;
        Ok(())
    }

    async fn on_stop(
        &self,
        db: &DatabaseConnection,
        process: &quebec_processes::Model,
    ) -> Result<(), Error> {
        let process_name = process.name.clone();
        let process_pid = process.pid;
        let process_hostname = process.hostname.clone();

        // Delete the process record
        let ctx = self.ctx();
        query_builder::processes::delete_by_id(db, &ctx.table_config, process.id).await?;

        info!(
            "Process stopped: name={} pid={} hostname={:?}",
            process_name, process_pid, process_hostname
        );

        Ok(())
    }
}
