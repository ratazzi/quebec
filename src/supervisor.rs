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

    /// Periodic maintenance run by the supervisor itself. Mirrors Solid Queue's
    /// `Supervisor#launch_maintenance_task`: prune processes whose heartbeat has
    /// gone stale (failing their claimed executions) and then sweep any leftover
    /// orphaned claims whose process row is already gone.
    ///
    /// `exclude_process_id` is the supervisor's own row id, so it is not pruned.
    /// Returns (pruned_processes, orphaned_claims_failed).
    pub async fn run_maintenance(&self, exclude_process_id: Option<i64>) -> Result<(usize, usize)> {
        let db = self.ctx.get_db().await?;
        let table_config = self.ctx.table_config.clone();

        let threshold = chrono::Utc::now().naive_utc()
            - chrono::Duration::from_std(self.ctx.process_alive_threshold)?;
        let stale = query_builder::processes::find_prunable(
            db.as_ref(),
            &table_config,
            threshold,
            exclude_process_id,
            self.ctx.use_skip_locked,
        )
        .await?;

        let pruned = stale.len();
        if pruned > 0 {
            info!(
                "Supervisor maintenance: pruning {} stale process(es) (no heartbeat since {})",
                pruned, threshold
            );
        }
        for p in stale {
            if let Err(e) = fail_claimed_by_process_id_inner(&self.ctx, db.as_ref(), p.id).await {
                warn!(
                    "Supervisor maintenance: failed to prune process {}: {}",
                    p.id, e
                );
            }
        }

        // Any claimed row whose process is already gone is orphaned; fail it so
        // the job can be retried. Safety net for cases where process cleanup
        // raced or skipped releasing claims.
        let orphaned =
            query_builder::claimed_executions::find_orphaned(db.as_ref(), &table_config).await?;
        let orphaned_count = orphaned.len();
        if orphaned_count > 0 {
            info!(
                "Supervisor maintenance: failing {} orphaned claimed execution(s)",
                orphaned_count
            );
        }
        for execution in orphaned {
            let ctx = self.ctx.clone();
            let tc = table_config.clone();
            let job_id = execution.job_id;
            let exec_id = execution.id;
            db.transaction::<_, (), DbErr>(|txn| {
                Box::pin(async move {
                    Worker::fail_claimed_execution(
                        &ctx,
                        txn,
                        &tc,
                        job_id,
                        exec_id,
                        "Process crashed or was killed before job completion",
                    )
                    .await
                })
            })
            .await?;
        }

        Ok((pruned, orphaned_count))
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
