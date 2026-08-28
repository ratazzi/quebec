//! Solid Queue-compatible batches.
//!
//! A batch groups jobs so their collective progress can be tracked and
//! callback jobs fired when the set finishes. The bookkeeping mirrors Solid
//! Queue >= 1.5 exactly, so a batch can be started by Rails and finished by
//! Quebec or vice versa:
//!
//! - `batch_executions` holds one row per *outstanding attempt* of a batched
//!   job. The row is created with the job and removed when the job reaches a
//!   terminal state (`finished_at` set, a `failed_executions` row created, or
//!   the job row deleted).
//! - Removing the last row lets exactly one caller win the completion CAS in
//!   [`try_finish`], which fixes the final counters and enqueues the
//!   `on_success` / `on_failure` and `on_finish` callback jobs.
//! - `total_jobs` is bumped *before* tracking rows are inserted, and only
//!   while the batch is unfinished; a zero-row update means the batch has
//!   already finished and the enqueue is refused with
//!   [`BatchAlreadyFinished`].
//!
//! Callback jobs are serialized once at batch creation (with the GIL, so the
//! job class's queue / priority / concurrency settings can be read) and
//! enqueued here without touching Python.

use std::sync::Arc;

use pyo3::create_exception;
use pyo3::exceptions::PyException;
use sea_orm::{ConnectionTrait, DatabaseConnection, DbBackend, DbErr, TransactionTrait};
use tracing::{debug, info, warn};

use crate::context::{AppContext, ConcurrencyConflict};
use crate::error::{QuebecError, Result};
use crate::notify::NotifyManager;
use crate::query_builder::{batch_executions, batches};
use crate::types::ActiveJob;

create_exception!(quebec, BatchAlreadyFinished, PyException);

/// Marker carried inside `DbErr::Custom` so an "already finished" refusal can
/// cross a `db.transaction` closure (typed `DbErr`) and be recognised at the
/// Python boundary.
const ALREADY_FINISHED_MARKER: &str = "quebec.batch.already_finished:";

/// Internal rollback signal for the Postgres re-check in [`try_finish`].
const FINISH_RECHECK_MARKER: &str = "quebec.batch.finish_recheck";

pub fn already_finished_err(batch_id: i64) -> DbErr {
    DbErr::Custom(format!("{ALREADY_FINISHED_MARKER}{batch_id}"))
}

/// The batch id if `err`'s message carries the already-finished marker.
pub fn already_finished_id(err: &dyn std::fmt::Display) -> Option<i64> {
    let text = err.to_string();
    let start = text.find(ALREADY_FINISHED_MARKER)? + ALREADY_FINISHED_MARKER.len();
    text[start..]
        .chars()
        .take_while(|c| c.is_ascii_digit())
        .collect::<String>()
        .parse()
        .ok()
}

/// Reserve counter space for `jobs` in `batch_id` and create their tracking
/// rows. `jobs` is `(job_id, executions, active_job_id)`; only first attempts
/// (`executions == 0`) count towards `total_jobs`, so a retry keeps the
/// logical total while still holding the batch open with its own row.
///
/// The counter update comes first on purpose: inserting a tracking row takes a
/// shared FK lock on the batch row, and incrementing afterwards can deadlock
/// concurrent adders on MySQL.
pub async fn track_jobs<C>(
    txn: &C,
    ctx: &AppContext,
    batch_id: i64,
    jobs: &[(i64, i32, &str)],
) -> std::result::Result<(), DbErr>
where
    C: ConnectionTrait,
{
    if jobs.is_empty() {
        return Ok(());
    }
    let mut first_attempts: Vec<&str> = jobs
        .iter()
        .filter(|(_, executions, _)| *executions == 0)
        .map(|(_, _, active_job_id)| *active_job_id)
        .collect();
    first_attempts.sort_unstable();
    first_attempts.dedup();

    let table_config = &ctx.table_config;
    if !batches::add_jobs(txn, table_config, batch_id, first_attempts.len() as i64).await? {
        return Err(already_finished_err(batch_id));
    }
    let job_ids: Vec<i64> = jobs.iter().map(|(id, _, _)| *id).collect();
    batch_executions::insert_all(txn, table_config, batch_id, &job_ids).await?;
    debug!(
        batch_id,
        jobs = job_ids.len(),
        new = first_attempts.len(),
        "batch: tracked jobs"
    );
    Ok(())
}

/// Drop the tracking row for a job that reached a terminal state. Returns the
/// batch the job belonged to so the caller can run [`try_finish`] once its
/// own transaction has committed. Cheap when the batches schema is absent.
pub async fn release_job<C>(
    ctx: &AppContext,
    txn: &C,
    job_id: i64,
) -> std::result::Result<Option<i64>, DbErr>
where
    C: ConnectionTrait,
{
    if !ctx.ensure_batches(txn).await {
        return Ok(None);
    }
    batch_executions::delete_by_job_id_returning_batch(txn, &ctx.table_config, job_id).await
}

/// [`release_job`] for callers holding the jobs row: a `NULL` `batch_id`
/// means no tracking row can exist, and a non-`NULL` one proves the batches
/// schema is installed, so no probe is needed.
pub async fn release_batched_job<C>(
    txn: &C,
    table_config: &crate::context::TableConfig,
    job: &crate::entities::quebec_jobs::Model,
) -> std::result::Result<Option<i64>, DbErr>
where
    C: ConnectionTrait,
{
    if job.batch_id.is_none() {
        return Ok(None);
    }
    batch_executions::delete_by_job_id_returning_batch(txn, table_config, job.id).await
}

/// Mark the batch as started, then let an empty batch finish right away.
pub async fn start(ctx: &Arc<AppContext>, db: &DatabaseConnection, batch_id: i64) -> Result<bool> {
    batches::mark_enqueued(db, &ctx.table_config, batch_id).await?;
    try_finish(ctx, db, batch_id).await
}

/// Completion check for a batch whose tracking rows may all be gone. Safe to
/// call from any number of workers at once: only the winner of the
/// `finished_at` CAS finalizes, and it does so in its own transaction so a
/// failure here never rolls back the job outcome that triggered it.
///
/// Returns whether *this* call finished the batch.
pub async fn try_finish(
    ctx: &Arc<AppContext>,
    db: &DatabaseConnection,
    batch_id: i64,
) -> Result<bool> {
    let table_config = &ctx.table_config;

    // Early exits without a transaction, mirroring Solid Queue's `finish`.
    match batches::find_status(db, table_config, batch_id).await? {
        None => return Ok(false),
        Some((_, Some(_))) => return Ok(false),
        Some((None, _)) => return Ok(false),
        Some((Some(_), None)) => {}
    }
    if batch_executions::exists_for_batch(db, table_config, batch_id).await? {
        return Ok(false);
    }

    let ctx_for_txn = ctx.clone();
    let outcome = db
        .transaction::<_, Option<Vec<String>>, DbErr>(|txn| {
            let ctx = ctx_for_txn.clone();
            Box::pin(async move { finalize(txn, &ctx, batch_id).await })
        })
        .await;

    let queues = match outcome {
        Ok(Some(queues)) => queues,
        Ok(None) => return Ok(false),
        Err(sea_orm::TransactionError::Transaction(DbErr::Custom(ref m)))
            if m == FINISH_RECHECK_MARKER =>
        {
            debug!(
                batch_id,
                "batch: completion re-check found new work, not finishing"
            );
            return Ok(false);
        }
        Err(e) => return Err(QuebecError::from(e)),
    };

    for queue_name in &queues {
        if !crate::notify::should_send_notify(ctx, queue_name) {
            continue;
        }
        NotifyManager::send_notify(&ctx.name, db, queue_name)
            .await
            .inspect_err(|e| warn!("Failed to send NOTIFY: {}", e))
            .ok();
    }
    Ok(true)
}

/// Body of the finishing transaction: win the CAS, fix counters, enqueue
/// callbacks. Returns the queues that received ready callback jobs, or `None`
/// when another caller won.
async fn finalize(
    txn: &sea_orm::DatabaseTransaction,
    ctx: &Arc<AppContext>,
    batch_id: i64,
) -> std::result::Result<Option<Vec<String>>, DbErr> {
    let table_config = &ctx.table_config;
    let now = chrono::Utc::now().naive_utc();

    if batches::try_mark_finished(txn, table_config, batch_id, now).await? == 0 {
        return Ok(None);
    }

    // PostgreSQL can let a blocked CAS win from a stale NOT EXISTS snapshot:
    // after a lock wait, READ COMMITTED re-checks the target row against the
    // latest data but keeps the original snapshot for subqueries. Re-check in
    // a new statement, which gets a fresh snapshot while this transaction's
    // row lock keeps adders out (they increment before inserting). MySQL reads
    // DML subqueries from the latest committed data, SQLite serializes writers.
    if txn.get_database_backend() == DbBackend::Postgres
        && batch_executions::exists_for_batch(txn, table_config, batch_id).await?
    {
        return Err(DbErr::Custom(FINISH_RECHECK_MARKER.to_string()));
    }

    let Some(batch) = batches::find_by_id(txn, table_config, batch_id).await? else {
        return Ok(None);
    };

    let failed_jobs = batches::count_failed_jobs(txn, table_config, batch_id).await?;
    let failed_at = (failed_jobs > 0).then_some(now);
    let completed_jobs = (i64::from(batch.total_jobs) - failed_jobs).max(0);
    batches::finalize_counters(
        txn,
        table_config,
        batch_id,
        failed_jobs,
        completed_jobs,
        failed_at,
    )
    .await?;

    let mut queues = Vec::new();
    let status_callback = if failed_jobs > 0 {
        batch.on_failure.as_deref()
    } else {
        batch.on_success.as_deref()
    };
    for callback in [status_callback, batch.on_finish.as_deref()]
        .into_iter()
        .flatten()
    {
        if let Some(queue) = enqueue_callback(txn, ctx, batch_id, callback, now).await? {
            queues.push(queue);
        }
    }

    info!(
        batch_id,
        total_jobs = batch.total_jobs,
        completed_jobs,
        failed_jobs,
        "batch: finished"
    );
    Ok(Some(queues))
}

/// Enqueue one serialized callback job. Returns its queue when it landed in
/// `ready_executions` (so the caller can NOTIFY).
async fn enqueue_callback(
    txn: &sea_orm::DatabaseTransaction,
    ctx: &Arc<AppContext>,
    batch_id: i64,
    serialized: &str,
    now: chrono::NaiveDateTime,
) -> std::result::Result<Option<String>, DbErr> {
    let job = deserialize_callback(ctx, serialized, batch_id, now)?;
    let duration = chrono::Duration::from_std(ctx.default_concurrency_control_period)
        .unwrap_or_else(|_| chrono::Duration::seconds(60));
    let (model, destination, _) = crate::core::enqueue_job(txn, ctx, &job, duration).await?;
    debug!(
        batch_id,
        job_id = model.id,
        class_name = %model.class_name,
        ?destination,
        "batch: callback enqueued"
    );
    Ok(destination.should_notify().then(|| model.queue_name))
}

/// Rebuild an [`ActiveJob`] from the ActiveJob-shaped JSON stored in a
/// batch's `on_*` column. The stored `arguments` array is re-wrapped into the
/// inner job envelope the worker's argument parser expects.
fn deserialize_callback(
    ctx: &AppContext,
    serialized: &str,
    batch_id: i64,
    now: chrono::NaiveDateTime,
) -> std::result::Result<ActiveJob, DbErr> {
    let data: serde_json::Value = serde_json::from_str(serialized)
        .map_err(|e| DbErr::Custom(format!("batch {batch_id}: invalid callback JSON: {e}")))?;
    let text = |key: &str| data.get(key).and_then(|v| v.as_str()).map(str::to_string);

    let class_name = text("job_class")
        .ok_or_else(|| DbErr::Custom(format!("batch {batch_id}: callback lacks job_class")))?;
    let active_job_id = text("job_id").unwrap_or_else(crate::utils::generate_job_id);
    let mut queue_name = text("queue_name").unwrap_or_else(|| "default".to_string());
    if let Some(force_q) = ctx.force_override_queue.as_deref() {
        queue_name = force_q.to_string();
    }
    let priority = data.get("priority").and_then(|v| v.as_i64()).unwrap_or(0) as i32;
    let scheduled_at = data
        .get("scheduled_at")
        .filter(|v| !v.is_null())
        .and_then(|v| serde_json::from_value::<chrono::NaiveDateTime>(v.clone()).ok())
        .unwrap_or(now);
    let arguments = data
        .get("arguments")
        .cloned()
        .unwrap_or_else(|| serde_json::Value::Array(vec![]));
    let concurrency_on_conflict = match text("concurrency_on_conflict").as_deref() {
        Some("discard") => ConcurrencyConflict::Discard,
        _ => ConcurrencyConflict::Block,
    };

    let job_data = serde_json::json!({
        "job_class": class_name,
        "job_id": active_job_id,
        "queue_name": queue_name,
        "priority": priority,
        "arguments": arguments,
        "continuation": {},
        "resumptions": 0,
    });

    Ok(ActiveJob {
        logger: crate::types::ActiveLogger,
        id: None,
        queue_name,
        class_name,
        arguments: job_data.to_string(),
        priority,
        executions: 0,
        active_job_id,
        scheduled_at,
        finished_at: None,
        concurrency_key: text("concurrency_key").filter(|k| !k.is_empty()),
        concurrency_limit: data
            .get("concurrency_limit")
            .and_then(|v| v.as_i64())
            .map(|v| v as i32),
        concurrency_on_conflict,
        created_at: None,
        updated_at: None,
        batch_id: None,
        callback_batch_id: Some(batch_id),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn already_finished_marker_round_trips() {
        let err = already_finished_err(42);
        assert_eq!(already_finished_id(&err), Some(42));
        let wrapped = format!("Transaction error: {err} (rolled back)");
        assert_eq!(already_finished_id(&wrapped), Some(42));
        assert_eq!(already_finished_id(&"unrelated".to_string()), None);
    }
}
