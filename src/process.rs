use crate::context::*;
use crate::entities::{prelude::*, *};
use anyhow::Error;
use anyhow::Result;
use async_trait::async_trait;
use sea_orm::TransactionTrait;
use sea_orm::*;

use pyo3::prelude::*;

use tracing::{debug, error, info, trace, warn};

pub fn is_running_in_pyo3() -> bool {
    let result = std::panic::catch_unwind(|| {
        Python::with_gil(|_py| {
            // 如果能够获取到 GIL，则认为是在 PyO3 环境中
            true
        })
    });
    result.unwrap_or(false) // 如果发生 panic，则返回 false
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

        // debug!("process: {:?}", process.clone().try_into_model());
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
                let threading = PyModule::import(py, "threading").unwrap();
                let bound = threading.getattr("get_ident").unwrap();
                let ident = bound.call0().unwrap();
                thread_id = ident.extract::<u64>().unwrap();
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
        // debug!("process: {:?}", process.clone().try_into_model());

        Ok(())
    }
}
