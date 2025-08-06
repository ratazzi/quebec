use crate::entities::*;
use anyhow::Error;
use anyhow::Result;
use async_trait::async_trait;
use sea_orm::TransactionTrait;
use sea_orm::*;

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

#[async_trait]
pub trait ProcessTrait {
    async fn on_start(
        &self, db: &DatabaseConnection, kind: String, name: String,
    ) -> Result<solid_queue_processes::Model, Error> {
        let process = db
            .transaction::<_, solid_queue_processes::ActiveModel, DbErr>(|txn| {
                let hostname = Self::get_hostname();
                let pid = Self::get_pid();
                let supervisor_id = Self::get_supervisor_id();
                let metadata = Self::get_metadata();

                Box::pin(async move {
                    let process = solid_queue_processes::ActiveModel {
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
        &self, db: &DatabaseConnection, process: &solid_queue_processes::Model,
    ) -> Result<(), Error> {
        let mut process = process.clone().into_active_model();
        process.last_heartbeat_at = ActiveValue::Set(chrono::Utc::now().naive_utc());
        process.clone().update(db).await?;

        Ok(())
    }

    async fn on_stop(
        &self, db: &DatabaseConnection, process: &solid_queue_processes::Model,
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
