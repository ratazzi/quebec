//! Supervisor: DB-layer operations for fork-based multi-process mode.
//!
//! Provides methods used by the Python-side Supervisor to:
//!   - register itself in the `processes` table
//!   - heartbeat
//!   - mark claimed executions of crashed children as failed
//!   - prune dead process rows

#![allow(dead_code)]

use crate::context::AppContext;
use crate::process::{ProcessInfo, ProcessTrait};
use crate::query_builder;
use crate::worker::Worker;
use anyhow::Result;
use async_trait::async_trait;
use sea_orm::{DatabaseConnection, DbErr, TransactionTrait};
use std::sync::Arc;

use tracing::{info, warn};

#[derive(Debug)]
pub struct Supervisor {
    pub ctx: Arc<AppContext>,
}

impl Supervisor {
    pub fn new(ctx: Arc<AppContext>) -> Self {
        Self { ctx }
    }

    /// Register the supervisor process in the `processes` table.
    /// Returns the new row id.
    pub async fn register(&self) -> Result<i64> {
        let db = self.ctx.get_db().await?;
        let process = self.on_start(db.as_ref()).await?;
        info!(
            "Supervisor registered: id={} pid={} hostname={:?}",
            process.id, process.pid, process.hostname
        );
        Ok(process.id)
    }

    /// Update heartbeat for a process row.
    pub async fn heartbeat_process(&self, process_id: i64) -> Result<()> {
        let db = self.ctx.get_db().await?;
        query_builder::processes::update_heartbeat(db.as_ref(), &self.ctx.table_config, process_id)
            .await?;
        Ok(())
    }

    /// Remove a process row.
    pub async fn deregister_process(&self, process_id: i64) -> Result<()> {
        let db = self.ctx.get_db().await?;
        query_builder::processes::delete_by_id(db.as_ref(), &self.ctx.table_config, process_id)
            .await?;
        Ok(())
    }

    /// Find the process row id for a given (pid, hostname), or None if not found.
    pub async fn lookup_process_id_by_pid(&self, pid: i32, hostname: &str) -> Result<Option<i64>> {
        let db = self.ctx.get_db().await?;
        let row = query_builder::processes::find_by_pid(
            db.as_ref(),
            &self.ctx.table_config,
            pid,
            hostname,
        )
        .await?;
        Ok(row.map(|m| m.id))
    }

    /// Mark all claimed executions for the given process row as failed, then prune the row.
    /// Returns number of claimed executions failed.
    pub async fn fail_claimed_by_process_id(&self, process_id: i64) -> Result<u64> {
        let db = self.ctx.get_db().await?;
        fail_claimed_by_process_id_inner(&self.ctx, db.as_ref(), process_id).await
    }

    /// Convenience: look up by (pid, hostname) and fail. Returns 0 if no such row.
    pub async fn fail_claimed_by_pid(&self, pid: i32, hostname: &str) -> Result<u64> {
        let Some(process_id) = self.lookup_process_id_by_pid(pid, hostname).await? else {
            return Ok(0);
        };
        self.fail_claimed_by_process_id(process_id).await
    }
}

async fn fail_claimed_by_process_id_inner(
    ctx: &Arc<AppContext>,
    db: &DatabaseConnection,
    process_id: i64,
) -> Result<u64> {
    let table_config = ctx.table_config.clone();
    let process = query_builder::processes::find_by_id(db, &table_config, process_id).await?;
    let (process_pid, process_hostname) = match &process {
        Some(p) => (p.pid, p.hostname.clone()),
        None => {
            warn!(
                "Supervisor: process row {} not found, nothing to fail",
                process_id
            );
            return Ok(0);
        }
    };

    let error_msg = format!(
        "Child process {} (pid={}, host={:?}) crashed",
        process_id, process_pid, process_hostname
    );

    let ctx = ctx.clone();
    let tc = table_config.clone();
    let deleted_count = db
        .transaction::<_, u64, DbErr>(|txn| {
            let ctx = ctx.clone();
            let tc = tc.clone();
            let error_msg = error_msg.clone();
            Box::pin(async move {
                let claimed =
                    query_builder::claimed_executions::find_by_process_id(txn, &tc, process_id)
                        .await?;
                let deleted_count = claimed.len() as u64;

                for execution in &claimed {
                    Worker::fail_claimed_execution(
                        &ctx,
                        txn,
                        &tc,
                        execution.job_id,
                        execution.id,
                        &error_msg,
                    )
                    .await?;
                }

                query_builder::processes::prune(txn, &tc, process_id).await?;
                Ok(deleted_count)
            })
        })
        .await?;

    warn!(
        "Supervisor reaped process {} (pid={}, host={:?}), failed {} claimed job(s)",
        process_id, process_pid, process_hostname, deleted_count
    );

    Ok(deleted_count)
}

#[async_trait]
impl ProcessTrait for Supervisor {
    fn ctx(&self) -> &Arc<AppContext> {
        &self.ctx
    }

    fn process_info(&self) -> ProcessInfo {
        ProcessInfo::new("Supervisor", "supervisor")
    }
}
