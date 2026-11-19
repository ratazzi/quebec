use crate::context::*;
use crate::dialect::Dialect;
use crate::process::{ProcessInfo, ProcessTrait};
use crate::query_builder;
use crate::semaphore::{acquire_semaphore, reacquire_released_semaphore};

use crate::error::Result;
use async_trait::async_trait;
use sea_orm::TransactionTrait;
use sea_orm::*;
use std::sync::Arc;

use sea_orm::{DbErr, Statement};
use tracing::Instrument;
use tracing::{info, trace, warn};

#[derive(Debug)]
pub struct Dispatcher {
    pub ctx: Arc<AppContext>,
    /// Process ID for this dispatcher (set after on_start)
    process_id: Arc<tokio::sync::Mutex<Option<i64>>>,
}

impl Dispatcher {
    pub fn new(ctx: Arc<AppContext>) -> Self {
        Self {
            ctx,
            process_id: Arc::new(tokio::sync::Mutex::new(None)),
        }
    }

    pub fn process_id_handle(&self) -> Arc<tokio::sync::Mutex<Option<i64>>> {
        self.process_id.clone()
    }

    pub async fn run(&self) -> Result<()> {
        let mut polling_interval = tokio::time::interval(self.ctx.dispatcher_polling_interval);
        let mut heartbeat_interval = tokio::time::interval(self.ctx.process_heartbeat_interval);
        // Orphan check: detect supervisor death via ppid change (Solid Queue parity).
        // Only armed when running as a supervisor-managed child.
        let supervised = self
            .ctx
            .supervisor_pid
            .load(std::sync::atomic::Ordering::Relaxed)
            != 0;
        let mut parent_check_interval = tokio::time::interval(std::time::Duration::from_secs(1));
        let batch_size = self.ctx.dispatcher_batch_size;
        // Concurrency maintenance runs on its own cadence (Solid Queue's
        // ConcurrencyMaintenance timer, default 600s), separate from polling.
        // Clamp to >= 1s: a zero period would panic tokio's interval (same
        // guard as the worker's cleanup_interval).
        let maintenance_enabled = self.ctx.dispatcher_concurrency_maintenance;
        // Batch maintenance shares the same timer (Solid Queue parity).
        let batch_maintenance = self.ctx.dispatcher_batch_maintenance;
        let mut batch_schema_warned = false;
        let mut maintenance_interval = tokio::time::interval(
            self.ctx
                .dispatcher_concurrency_maintenance_interval
                .max(std::time::Duration::from_secs(1)),
        );

        let init_db = self.ctx.get_db().await?;
        let process = self.on_start(&init_db).await?;
        info!(">> Process started: {:?}", process);
        *self.process_id.lock().await = Some(process.id);

        let quit = self.ctx.graceful_shutdown.clone();

        loop {
            tokio::select! {
                _ = heartbeat_interval.tick() => {
                    let Ok(heartbeat_db) = self.ctx.get_db().await.inspect_err(|e| {
                        warn!("Failed to get DB for heartbeat: {}", e);
                    }) else { continue };
                    if let Err(e) = self.heartbeat(&heartbeat_db, &process).await {
                        warn!("Failed to flush dispatcher heartbeat: {}", e);
                    }
                }
                // Detect supervisor death: if our ppid changed we were reparented.
                _ = parent_check_interval.tick(), if supervised => {
                    if self.ctx.is_orphaned() {
                        warn!("Supervisor went away (ppid changed), initiating graceful shutdown");
                        self.ctx.graceful_shutdown.cancel();
                    }
                }
                _ = quit.cancelled() => {
                    info!("Stopped");
                    let stop_db = self.ctx.get_db().await?;
                    self.on_stop(&stop_db, &process).await?;
                    return Ok(());
                }
                // Concurrency maintenance: expire stale semaphores + unblock
                // blocked jobs. Runs every `concurrency_maintenance_interval`
                // (Solid Queue parity — its ConcurrencyMaintenance TimerTask,
                // `run_now: true`, which tokio's immediate first tick matches),
                // not every polling tick. The completion path releases the
                // semaphore and unblocks the next job directly (after_executed),
                // so this sweep only reclaims semaphores whose holder crashed.
                // Skipped entirely when `concurrency_maintenance: false`.
                _ = maintenance_interval.tick(), if maintenance_enabled || batch_maintenance => {
                    let Ok(maintenance_db) = self.ctx.get_db().await.inspect_err(|e| {
                        warn!("Failed to get DB for maintenance: {}", e);
                    }) else { continue };
                    let ctx = self.ctx.clone();

                    if maintenance_enabled {
                        // Clean up expired semaphores (matches Solid Queue's
                        // `expire_semaphores`). Solid Queue runs expire + unblock in
                        // a single maintenance task, so a failed expire aborts the
                        // task before unblock runs — mirror that by skipping unblock
                        // when expire fails.
                        let expire_ok =
                            match query_builder::semaphores::delete_expired(&*maintenance_db, &ctx.table_config).await {
                                Ok(n) => {
                                    if n > 0 {
                                        info!("Cleaned up {} expired semaphores", n);
                                    }
                                    true
                                }
                                Err(e) => {
                                    warn!("Error cleaning up expired semaphores: {:?}", e);
                                    false
                                }
                            };

                        // Unblock jobs with expired concurrency keys — one transaction
                        // per key, matching Solid Queue's `BlockedExecution.unblock`.
                        if expire_ok {
                            if let Err(e) =
                                Self::unblock_blocked_executions(&maintenance_db, &ctx, batch_size).await
                            {
                                warn!("Error unblocking blocked executions: {:?}", e);
                            }
                        }
                    }

                    // Batch sweep (Solid Queue's `Batch.sweep_stalled`), skipped
                    // quietly while the batches schema is absent.
                    if batch_maintenance {
                        if ctx.ensure_batches(maintenance_db.as_ref()).await {
                            if let Err(e) = crate::batch::sweep_stalled(
                                &ctx,
                                &maintenance_db,
                                std::time::Duration::from_secs(300),
                                batch_size,
                            )
                            .await
                            {
                                warn!("Error sweeping stalled batches: {:?}", e);
                            }
                        } else if !batch_schema_warned {
                            batch_schema_warned = true;
                            info!("Batch maintenance is on but the batches schema is not installed; skipping the sweep");
                        }
                    }
                }
                _ = polling_interval.tick() => {
                    let Ok(polling_db) = self.ctx.get_db().await.inspect_err(|e| {
                        warn!("Failed to get DB for polling: {}", e);
                    }) else { continue };
                    let transaction_result =
                        Self::dispatch_due_batch(polling_db.as_ref(), self.ctx.clone(), batch_size)
                            .await;

                    // Send NOTIFY for each unique queue after transaction commits.
                    // `should_send_notify` enforces backend + use_listen_notify + per-queue throttle.
                    let Ok((queues, released_batches)) = transaction_result else { continue };
                    crate::core::finish_released_batches(&self.ctx, &polling_db, released_batches).await;

                    for queue_name in queues {
                        if !crate::notify::should_send_notify(&self.ctx, &queue_name) {
                            continue;
                        }
                        crate::notify::NotifyManager::send_notify(&self.ctx.name, &*polling_db, &queue_name)
                            .await
                            .inspect_err(|e| warn!("Failed to send NOTIFY for queue {}: {}", queue_name, e))
                            .ok();
                    }
                }
            }
        }
    }

    /// Promote one batch of due scheduled executions in its own transaction.
    async fn dispatch_due_batch(
        polling_db: &DatabaseConnection,
        ctx: Arc<AppContext>,
        batch_size: u64,
    ) -> std::result::Result<(std::collections::HashSet<String>, Vec<i64>), TransactionError<DbErr>>
    {
        // Dispatch scheduled jobs in their own transaction.
        polling_db
            .transaction::<_, (std::collections::HashSet<String>, Vec<i64>), DbErr>(|txn| {
                Box::pin(async move {
                    // Dispatch scheduled jobs
                    // Use FOR UPDATE SKIP LOCKED to avoid conflicts between multiple dispatchers
                    // This matches Solid Queue's implementation
                    let scheduled_executions = query_builder::scheduled_executions::find_due(
                        txn,
                        &ctx.table_config,
                        batch_size,
                        ctx.use_skip_locked,
                    )
                    .await;

                    if scheduled_executions.is_err() {
                        warn!(
                            "Error fetching scheduled jobs: {:?}",
                            scheduled_executions.err()
                        );
                        return Ok((std::collections::HashSet::new(), Vec::new()));
                    }
                    let scheduled_executions = scheduled_executions?;
                    let size = scheduled_executions.len();

                    // Collect queue names for NOTIFY
                    let mut notified_queues = std::collections::HashSet::new();
                    // Batched jobs discarded on promotion, re-checked after commit.
                    let mut released_batches: Vec<i64> = Vec::new();

                    // Batch fetch all jobs at once (eliminates N+1)
                    let job_ids: Vec<i64> =
                        scheduled_executions.iter().map(|se| se.job_id).collect();
                    let jobs =
                        query_builder::jobs::find_by_ids(txn, &ctx.table_config, job_ids.clone())
                            .await?;
                    let job_map: std::collections::HashMap<i64, _> =
                        jobs.into_iter().map(|j| (j.id, j)).collect();

                    // Split scheduled-due jobs into two groups so we can
                    // bulk-promote the unrestricted ones but still enforce
                    // concurrency limits on the rest (matches Solid Queue's
                    // `Job.dispatch_all` partition between
                    // `dispatch_all_at_once` and `dispatch_all_one_by_one`).
                    // Without this split, every retry / wait_until / wait
                    // path bypassed the semaphore at promotion time and
                    // could violate the declared concurrency_limit.
                    let mut ready_data: Vec<(i64, &str, i32, chrono::NaiveDateTime)> = Vec::new();
                    let mut concurrency_limited: Vec<&_> = Vec::new();
                    for se in &scheduled_executions {
                        let Some(job) = job_map.get(&se.job_id) else {
                            warn!(
                                "Job {} not found for scheduled execution {}",
                                se.job_id, se.id
                            );
                            continue;
                        };
                        let has_key = job
                            .concurrency_key
                            .as_deref()
                            .map(|k| !k.is_empty())
                            .unwrap_or(false);
                        if has_key {
                            concurrency_limited.push(job);
                        } else {
                            ready_data.push((
                                se.job_id,
                                &job.queue_name,
                                job.priority,
                                chrono::Utc::now().naive_utc(),
                            ));
                            notified_queues.insert(job.queue_name.clone());
                        }
                    }

                    // Concurrency-limited ones go one-by-one so each can
                    // acquire the semaphore and route to ready / blocked /
                    // discarded per its on_conflict policy.
                    for job in concurrency_limited {
                        let concurrency_key = job.concurrency_key.as_deref().unwrap_or("");
                        let Some(class) = ctx.class_concurrency(&job.class_name) else {
                            // Class not registered here: skip its limits and
                            // record that it holds no slot, like Solid Queue
                            // does for a job whose class doesn't load.
                            warn!(
                                job_id = job.id,
                                concurrency_key,
                                "Job class `{}' is not registered; dispatching without its concurrency limit",
                                job.class_name
                            );
                            crate::concurrency_lock::record_no_slot(txn, &ctx.table_config, job)
                                .await?;
                            ready_data.push((
                                job.id,
                                &job.queue_name,
                                job.priority,
                                chrono::Utc::now().naive_utc(),
                            ));
                            notified_queues.insert(job.queue_name.clone());
                            continue;
                        };

                        let acquired = acquire_semaphore(
                            txn,
                            &ctx.table_config,
                            concurrency_key.to_string(),
                            class.limit,
                            class.duration,
                        )
                        .await?;

                        if let Some(semaphore_id) = acquired {
                            crate::concurrency_lock::record(
                                txn,
                                &ctx.table_config,
                                job,
                                semaphore_id,
                                class.limit,
                            )
                            .await?;
                            ready_data.push((
                                job.id,
                                &job.queue_name,
                                job.priority,
                                chrono::Utc::now().naive_utc(),
                            ));
                            notified_queues.insert(job.queue_name.clone());
                        } else {
                            match class.on_conflict {
                                ConcurrencyConflict::Discard => {
                                    warn!(
                                        job_id = job.id,
                                        concurrency_key,
                                        "Scheduled job `{}' discarded due to concurrency limit",
                                        job.class_name
                                    );
                                    query_builder::jobs::mark_finished(
                                        txn,
                                        &ctx.table_config,
                                        job.id,
                                    )
                                    .await?;
                                    released_batches.extend(
                                        crate::batch::release_batched_job(
                                            txn,
                                            &ctx.table_config,
                                            job,
                                        )
                                        .await?,
                                    );
                                }
                                ConcurrencyConflict::Block => {
                                    let now = chrono::Utc::now().naive_utc();
                                    info!(
                                        job_id = job.id,
                                        concurrency_key,
                                        "Scheduled job `{}' blocked due to concurrency limit",
                                        job.class_name
                                    );
                                    query_builder::blocked_executions::insert(
                                        txn,
                                        &ctx.table_config,
                                        job.id,
                                        &job.queue_name,
                                        job.priority,
                                        concurrency_key,
                                        now + class.duration,
                                    )
                                    .await?;
                                }
                            }
                        }
                    }

                    // The semaphore decision remains per job; all admitted jobs
                    // can share one ready insert in this transaction.
                    if !ready_data.is_empty() {
                        query_builder::ready_executions::insert_all_with_created_at(
                            txn,
                            &ctx.table_config,
                            &ready_data,
                        )
                        .await?;
                    }

                    // Delete all scheduled_executions rows we processed
                    // (regardless of whether each landed in ready, blocked,
                    // or finished).
                    query_builder::scheduled_executions::delete_by_job_ids(
                        txn,
                        &ctx.table_config,
                        &job_ids,
                    )
                    .await?;

                    if size > 0 {
                        info!("Dispatch scheduled jobs size: {}", size);
                    }

                    Ok((notified_queues, released_batches))
                })
            })
            .instrument(tracing::info_span!("polling", component = "dispatcher"))
            .await
    }

    /// Unblock jobs whose concurrency keys have expired, mirroring Solid Queue's
    /// `BlockedExecution.unblock`: pick the releasable expired keys, then release
    /// one blocked execution per key, each in its own transaction.
    async fn unblock_blocked_executions(
        db: &DatabaseConnection,
        ctx: &Arc<AppContext>,
        batch_size: u64,
    ) -> Result<usize, DbErr> {
        // Fetch distinct expired concurrency keys, ordered by key
        // (matches Solid Queue's `expired.order(:concurrency_key).distinct.limit`).
        let backend = db.get_database_backend();
        let (p1, p2) = match Dialect::from(backend) {
            Dialect::Postgres => ("$1", "$2"),
            Dialect::MySql | Dialect::Sqlite => ("?", "?"),
        };
        let sql = format!(
            "SELECT DISTINCT concurrency_key FROM {} WHERE expires_at < {} ORDER BY concurrency_key LIMIT {}",
            ctx.table_config.blocked_executions, p1, p2
        );
        let now = chrono::Utc::now().naive_utc();
        let rows = db
            .query_all_raw(Statement::from_sql_and_values(
                backend,
                sql,
                vec![now.into(), batch_size.into()],
            ))
            .await?;
        let expired_keys: Vec<String> = rows
            .iter()
            .filter_map(|row| row.try_get::<String>("", "concurrency_key").ok())
            .collect();

        if expired_keys.is_empty() {
            trace!("No expired concurrency keys found to unblock");
            return Ok(0);
        }
        info!(
            "Found {} expired concurrency keys to unblock: {:?}",
            expired_keys.len(),
            expired_keys
        );

        // Pre-filter releasable keys with a single batched semaphore lookup
        // (matches Solid Queue's `releasable`): keys without a semaphore OR with
        // an open slot (value > 0).
        let semaphore_map =
            query_builder::semaphores::find_values_by_keys(db, &ctx.table_config, &expired_keys)
                .await?;
        let releasable_keys = expired_keys
            .iter()
            .filter(|k| semaphore_map.get(*k).map_or(true, |&v| v > 0));

        // Release one blocked execution per key, each in its own transaction
        // (matches Solid Queue's `release_many` / `release_one`).
        let mut released = 0usize;
        for concurrency_key in releasable_keys {
            // Let a DbErr propagate to abort the loop, mirroring Solid Queue's
            // `release_many` (`count { release_one }`), which lets a failing
            // `release_one` bubble up rather than rescuing per key.
            if Self::release_one(db, ctx, concurrency_key).await? {
                released += 1;
            }
        }
        Ok(released)
    }

    /// Release a single blocked execution for `concurrency_key` in its own
    /// transaction, mirroring Solid Queue's `BlockedExecution.release_one` +
    /// `#release`: lock the head blocked row (FOR UPDATE SKIP LOCKED), acquire
    /// the semaphore, then promote it to ready and delete the blocked row
    /// (see [`release_next_blocked`]).
    async fn release_one(
        db: &DatabaseConnection,
        ctx: &Arc<AppContext>,
        concurrency_key: &str,
    ) -> Result<bool, DbErr> {
        let ctx = ctx.clone();
        let key = concurrency_key.to_string();
        db.transaction::<_, bool, DbErr>(|txn| {
            Box::pin(async move { release_next_blocked(&ctx, txn, &key, false).await })
        })
        .await
        .map_err(flatten_txn_err)
    }
}

fn flatten_txn_err(e: TransactionError<DbErr>) -> DbErr {
    match e {
        TransactionError::Connection(e) | TransactionError::Transaction(e) => e,
    }
}

/// Solid Queue's `BlockedExecution#release`: acquire the concurrency
/// semaphore for the job and, only if that succeeds, promote it to ready and
/// delete the blocked row. Without a free slot the job stays blocked.
///
/// The caller must already hold `execution`'s row lock in `db`'s transaction.
/// This is the only way a blocked job becomes ready, so every job that
/// reaches ready through it holds a slot that its completion later releases.
///
/// A job whose class isn't registered in this process is promoted without
/// its limits and recorded as holding no slot, as dispatching or retrying it
/// does. Solid Queue fails it with `ClassMissingError` here, but it can do so
/// because every Rails process loads every job class; a Quebec process only
/// knows the classes registered in it, and a dispatcher or control plane
/// that lacks one must not fail a job a worker could run. A worker without
/// the class fails it when it claims it, like Solid Queue's
/// `ClaimedExecution#perform`.
///
/// `reuse_released_slot` is for a caller that just gave a slot back to this
/// key's semaphore in the same transaction (see
/// [`crate::semaphore::reacquire_released_semaphore`]).
pub(crate) async fn release_blocked_execution<C>(
    ctx: &AppContext,
    db: &C,
    execution: &crate::entities::quebec_blocked_executions::Model,
    reuse_released_slot: bool,
) -> Result<BlockedRelease, DbErr>
where
    C: ConnectionTrait,
{
    let Some(job) =
        query_builder::jobs::find_by_id(db, &ctx.table_config, execution.job_id).await?
    else {
        // The FK cascade removes blocked rows with their job, so this is a
        // row deleted under us; nothing to release.
        return Ok(BlockedRelease::StillBlocked);
    };
    let key = execution.concurrency_key.clone();

    let Some(class) = ctx.class_concurrency(&job.class_name) else {
        return promote_without_limits(ctx, db, execution, &job).await;
    };
    // MySQL: a slot given back in this transaction already locked an
    // existing row. Re-acquire by UPDATE so this blocked-row lock
    // cannot cycle with a producer's UPSERT insert-intention lock.
    let acquired =
        if reuse_released_slot && db.get_database_backend() == sea_orm::DatabaseBackend::MySql {
            reacquire_released_semaphore(db, &ctx.table_config, &key, class.duration).await?
        } else {
            acquire_semaphore(
                db,
                &ctx.table_config,
                key.clone(),
                class.limit,
                class.duration,
            )
            .await?
        };
    let Some(semaphore_id) = acquired else {
        trace!("Failed to acquire semaphore for key: {key} (no available slots)");
        return Ok(BlockedRelease::StillBlocked);
    };
    crate::concurrency_lock::record(db, &ctx.table_config, &job, semaphore_id, class.limit).await?;
    promote_to_ready(ctx, db, execution).await?;
    info!(
        "Unblocked job {} for concurrency key: {}",
        execution.job_id, key
    );
    Ok(BlockedRelease::Promoted)
}

/// Move a blocked execution to ready (Solid Queue's `promote_to_ready` +
/// `destroy!`).
async fn promote_to_ready<C>(
    ctx: &AppContext,
    db: &C,
    execution: &crate::entities::quebec_blocked_executions::Model,
) -> Result<(), DbErr>
where
    C: ConnectionTrait,
{
    query_builder::ready_executions::insert(
        db,
        &ctx.table_config,
        execution.job_id,
        &execution.queue_name,
        execution.priority,
    )
    .await?;
    query_builder::blocked_executions::delete_by_id(db, &ctx.table_config, execution.id).await?;
    Ok(())
}

/// What [`release_blocked_execution`] did with a blocked job.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BlockedRelease {
    /// Took a slot; the job is now ready.
    Promoted,
    /// Its class isn't registered here: now ready without taking a slot.
    Bypassed,
    /// No free slot (or the row is gone); nothing changed.
    StillBlocked,
}

impl BlockedRelease {
    /// Whether the blocked row left blocked.
    pub(crate) fn released(self) -> bool {
        self != Self::StillBlocked
    }
}

/// Promote a blocked job whose class isn't registered in this process past
/// its limits, recorded as holding no slot (see [`release_blocked_execution`]).
async fn promote_without_limits<C>(
    ctx: &AppContext,
    db: &C,
    execution: &crate::entities::quebec_blocked_executions::Model,
    job: &crate::entities::quebec_jobs::Model,
) -> Result<BlockedRelease, DbErr>
where
    C: ConnectionTrait,
{
    warn!(
        job_id = job.id,
        concurrency_key = execution.concurrency_key,
        "Job class `{}' is not registered; releasing the blocked job without its concurrency limit",
        job.class_name
    );
    crate::concurrency_lock::record_no_slot(db, &ctx.table_config, job).await?;
    promote_to_ready(ctx, db, execution).await?;
    Ok(BlockedRelease::Bypassed)
}

/// Release the next blocked job for `concurrency_key`, in blocked order
/// (Solid Queue's `BlockedExecution.release_one`). `reuse_released_slot` is
/// set when a slot on it was given back in this transaction.
///
/// A job promoted without its limits ([`BlockedRelease::Bypassed`]) took no
/// slot, so the one that may be free goes on to the job behind it rather
/// than staying idle until concurrency maintenance. Returns whether any job
/// left blocked.
pub(crate) async fn release_next_blocked<C>(
    ctx: &AppContext,
    db: &C,
    concurrency_key: &str,
    reuse_released_slot: bool,
) -> Result<bool, DbErr>
where
    C: ConnectionTrait,
{
    let mut released = false;
    while let Some(execution) = query_builder::blocked_executions::find_one_by_key_for_update(
        db,
        &ctx.table_config,
        concurrency_key,
        ctx.use_skip_locked,
    )
    .await?
    {
        match release_blocked_execution(ctx, db, &execution, reuse_released_slot).await? {
            BlockedRelease::Bypassed => released = true,
            outcome => return Ok(released || outcome.released()),
        }
    }
    Ok(released)
}

/// Outcome of an explicit unblock request for one blocked execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UnblockOutcome {
    /// Semaphore acquired; the job is now ready.
    Released,
    /// No free concurrency slot; the job stays blocked until one frees up.
    StillBlocked,
    /// No blocked execution matched.
    NotFound,
}

/// Release one blocked execution, looked up by `id` or `job_id`, in its own
/// transaction (Solid Queue's `release_one` + `#release` for a chosen row).
pub(crate) async fn release_blocked_by(
    ctx: &Arc<AppContext>,
    db: &DatabaseConnection,
    lookup: query_builder::blocked_executions::BlockedLookup,
    value: i64,
) -> Result<UnblockOutcome, DbErr> {
    let ctx = ctx.clone();
    db.transaction::<_, UnblockOutcome, DbErr>(|txn| {
        Box::pin(async move {
            let Some(execution) =
                query_builder::blocked_executions::lock_by(txn, &ctx.table_config, lookup, value)
                    .await?
            else {
                return Ok(UnblockOutcome::NotFound);
            };
            Ok(
                match release_blocked_execution(&ctx, txn, &execution, false).await? {
                    BlockedRelease::Promoted | BlockedRelease::Bypassed => UnblockOutcome::Released,
                    BlockedRelease::StillBlocked => UnblockOutcome::StillBlocked,
                },
            )
        })
    })
    .await
    .map_err(flatten_txn_err)
}

/// Release every blocked execution matching the optional filters, one row per
/// transaction in blocked order (priority, then job_id), like Solid Queue's
/// `release_many`. Rows whose key has no free slot stay blocked.
///
/// Returns how many were promoted to ready.
pub(crate) async fn release_blocked_matching(
    ctx: &Arc<AppContext>,
    db: &DatabaseConnection,
    class_name: Option<&str>,
    queue_name: Option<&str>,
) -> Result<u64, DbErr> {
    let mut candidates = query_builder::blocked_executions::find_all_filtered(
        db,
        &ctx.table_config,
        class_name,
        queue_name,
    )
    .await?;
    candidates.sort_by_key(|e| (e.priority, e.job_id));

    let mut released = 0;
    for candidate in candidates {
        let outcome = release_blocked_by(
            ctx,
            db,
            query_builder::blocked_executions::BlockedLookup::Id,
            candidate.id,
        )
        .await?;
        if outcome == UnblockOutcome::Released {
            released += 1;
        }
    }
    Ok(released)
}

#[async_trait]
impl ProcessTrait for Dispatcher {
    fn ctx(&self) -> &Arc<AppContext> {
        &self.ctx
    }

    fn process_info(&self) -> ProcessInfo {
        ProcessInfo::new("Dispatcher", "dispatcher")
    }
}
