use crate::context::*;
use crate::entities::quebec_jobs;
use crate::error::Result;
use crate::notify::NotifyManager;
use crate::query_builder;
use crate::semaphore::acquire_semaphore;
use crate::types::ActiveJob;
use tracing::{info, trace, warn};

use sea_orm::TransactionTrait;
use sea_orm::*;
use std::collections::HashSet;
use std::sync::Arc;

/// A fully-prepared job ready for bulk insert (all Python interaction done).
#[derive(Debug, Clone)]
pub struct PreparedJob {
    pub class_name: String,
    pub queue_name: String,
    pub priority: i32,
    pub active_job_id: String,
    pub arguments: String,
    pub scheduled_at: Option<chrono::NaiveDateTime>,
    pub concurrency_key: Option<String>,
    pub concurrency_limit: Option<i32>,
    pub concurrency_on_conflict: ConcurrencyConflict,
    pub batch_id: Option<i64>,
}

/// Where the job was routed after enqueue
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum JobDestination {
    /// Job is ready for immediate execution
    Ready,
    /// Job is scheduled for future execution
    Scheduled,
    /// Job is blocked by concurrency limit
    Blocked,
    /// Job was discarded due to concurrency limit
    Discarded,
}

impl JobDestination {
    pub(crate) fn should_notify(&self) -> bool {
        matches!(self, JobDestination::Ready)
    }
}

#[derive(Debug)]
pub struct Quebec {
    pub ctx: Arc<AppContext>,
}

impl Quebec {
    pub fn new(ctx: Arc<AppContext>) -> Self {
        Self { ctx }
    }

    pub async fn perform_all_later(
        &self,
        jobs: Arc<Vec<PreparedJob>>,
        transaction: Option<Arc<crate::batch_transaction::TransactionState>>,
    ) -> Result<Vec<quebec_jobs::Model>> {
        if jobs.is_empty() {
            return Ok(vec![]);
        }

        if let Some(transaction) = transaction {
            let txn = transaction.connection()?;
            let duration = chrono::Duration::from_std(self.ctx.default_concurrency_control_period)
                .unwrap_or_else(|_| chrono::Duration::seconds(60));
            let (models, queues, released) =
                enqueue_all_jobs(&txn, &self.ctx, &jobs, duration).await?;
            transaction.after_enqueue(queues, released);
            return Ok(models);
        }

        let db = self.ctx.get_db().await?;
        let ctx = self.ctx.clone();

        let (job_models, ready_queues, released_batches) = db
            .transaction::<_, (Vec<quebec_jobs::Model>, HashSet<String>, Vec<i64>), DbErr>(|txn| {
                let ctx = ctx.clone();
                let duration = chrono::Duration::from_std(ctx.default_concurrency_control_period)
                    .unwrap_or_else(|_| chrono::Duration::seconds(60));
                let jobs = Arc::clone(&jobs);
                Box::pin(async move { enqueue_all_jobs(txn, &ctx, &jobs, duration).await })
            })
            .await
            .map_err(crate::error::QuebecError::from)?;

        // Send NOTIFY only for queues that have ready jobs (consistent with perform_later).
        // `should_send_notify` enforces backend + use_listen_notify + per-queue throttle.
        for queue_name in &ready_queues {
            if !crate::notify::should_send_notify(&self.ctx, queue_name) {
                continue;
            }
            NotifyManager::send_notify(&self.ctx.name, &*db, queue_name)
                .await
                .inspect_err(|e| warn!("Failed to send NOTIFY: {}", e))
                .ok();
        }

        finish_released_batches(&self.ctx, &db, released_batches).await;

        Ok(job_models)
    }

    pub async fn perform_later(
        &self,
        job: ActiveJob,
        transaction: Option<Arc<crate::batch_transaction::TransactionState>>,
    ) -> Result<quebec_jobs::Model> {
        if let Some(transaction) = transaction {
            let txn = transaction.connection()?;
            let duration = chrono::Duration::from_std(self.ctx.default_concurrency_control_period)
                .unwrap_or_else(|_| chrono::Duration::seconds(60));
            let (model, destination, released) =
                enqueue_job(&txn, &self.ctx, &job, duration).await?;
            transaction.after_enqueue(
                destination
                    .should_notify()
                    .then(|| model.queue_name.clone()),
                released,
            );
            return Ok(model);
        }
        let db = self.ctx.get_db().await?;
        let ctx = self.ctx.clone();
        trace!("job: {:?}", job);

        let (job_model, destination, released_batch) = db
            .transaction::<_, (quebec_jobs::Model, JobDestination, Option<i64>), DbErr>(|txn| {
                let ctx = ctx.clone();
                let duration = chrono::Duration::from_std(ctx.default_concurrency_control_period)
                    .unwrap_or_else(|_| chrono::Duration::seconds(60));
                let job = job.clone();
                Box::pin(async move { enqueue_job(txn, &ctx, &job, duration).await })
            })
            .await
            .map_err(crate::error::QuebecError::from)?;

        if destination.should_notify()
            && crate::notify::should_send_notify(&self.ctx, &job_model.queue_name)
        {
            NotifyManager::send_notify(&self.ctx.name, &*db, &job_model.queue_name)
                .await
                .inspect_err(|e| warn!("Failed to send NOTIFY: {}", e))
                .ok();
        }

        finish_released_batches(&self.ctx, &db, released_batch).await;

        Ok(job_model)
    }
}

/// Run the batch completion check for every batch whose job was released
/// (discarded on enqueue) inside a transaction that has now committed.
/// Failures are logged: the sweep repairs anything missed here.
pub(crate) async fn finish_released_batches(
    ctx: &Arc<AppContext>,
    db: &DatabaseConnection,
    batch_ids: impl IntoIterator<Item = i64>,
) {
    let mut seen = HashSet::new();
    for batch_id in batch_ids {
        if !seen.insert(batch_id) {
            continue;
        }
        if let Err(e) = crate::batch::try_finish(ctx, db, batch_id).await {
            warn!(batch_id, "batch: completion check failed: {e}");
        }
    }
}

/// Core job enqueue logic, runs inside a transaction. The third element is
/// the batch to re-check for completion once the transaction commits, set
/// when a batched job was discarded on enqueue.
pub(crate) async fn enqueue_job(
    txn: &DatabaseTransaction,
    ctx: &AppContext,
    job: &ActiveJob,
    concurrency_duration: chrono::Duration,
) -> std::result::Result<(quebec_jobs::Model, JobDestination, Option<i64>), DbErr> {
    let table_config = &ctx.table_config;
    let now = chrono::Utc::now().naive_utc();

    // Validate arguments JSON without full parsing (zero-copy validation)
    let args: Box<serde_json::value::RawValue> = serde_json::from_str(&job.arguments)
        .map_err(|e| DbErr::Custom(format!("Invalid JSON in arguments: {e}")))?;

    let mut overrides = serde_json::json!({
        "job_class": job.class_name,
        "job_id": job.active_job_id,
        "provider_job_id": job.active_job_id,
        "queue_name": job.queue_name,
        "priority": job.priority,
        "arguments": args,
        "enqueued_at": now,
    });
    // Same keys Active Job's BatchId extension serializes; omitted when unset
    // so non-batched envelopes are unchanged.
    if let Some(batch_id) = job.batch_id {
        overrides["batch_id"] = serde_json::Value::from(batch_id);
    }
    if let Some(callback_batch_id) = job.callback_batch_id {
        overrides["callback_batch_id"] = serde_json::Value::from(callback_batch_id);
    }
    let params = crate::utils::build_job_params(overrides);

    let concurrency_key = job.concurrency_key.as_deref().unwrap_or_default();

    // Insert job record
    let job_model = query_builder::jobs::insert_returning(
        txn,
        table_config,
        &job.queue_name,
        &job.class_name,
        Some(&params.to_string()),
        job.priority,
        Some(&job.active_job_id),
        Some(job.scheduled_at),
        if concurrency_key.is_empty() {
            None
        } else {
            Some(concurrency_key)
        },
        job.batch_id,
    )
    .await?;

    // Track before routing so a job discarded by a concurrency conflict is
    // still counted, then released, exactly like Solid Queue.
    if let Some(batch_id) = job.batch_id {
        crate::batch::track_jobs(
            txn,
            ctx,
            batch_id,
            &[(job_model.id, job.executions, &job.active_job_id)],
        )
        .await?;
    }

    // Route job to appropriate destination
    let (destination, released_batch) = route_job(
        txn,
        ctx,
        &job_model,
        job,
        concurrency_key,
        now,
        concurrency_duration,
    )
    .await?;

    Ok((job_model, destination, released_batch))
}

/// Determine where the job should go based on scheduling and concurrency.
/// Also returns the batch to re-check when a batched job was discarded.
async fn route_job(
    txn: &DatabaseTransaction,
    ctx: &AppContext,
    job_model: &quebec_jobs::Model,
    job: &ActiveJob,
    concurrency_key: &str,
    now: chrono::NaiveDateTime,
    concurrency_duration: chrono::Duration,
) -> std::result::Result<(JobDestination, Option<i64>), DbErr> {
    let table_config = &ctx.table_config;
    let job_id = job_model.id;

    // Check if job is scheduled for the future
    if job.scheduled_at > now {
        info!(job_id, scheduled_at = ?job.scheduled_at, "Job scheduled for future execution");
        query_builder::scheduled_executions::insert(
            txn,
            table_config,
            job_id,
            &job_model.queue_name,
            job_model.priority,
            job.scheduled_at,
        )
        .await?;
        return Ok((JobDestination::Scheduled, None));
    }

    // Handle concurrency control if configured
    if !concurrency_key.is_empty() {
        let acquired = acquire_semaphore(
            txn,
            table_config,
            concurrency_key.to_string(),
            job.concurrency_limit.unwrap_or(1),
            None,
        )
        .await?;

        if !acquired {
            return handle_concurrency_conflict(
                txn,
                ctx,
                job_model,
                job,
                concurrency_key,
                now,
                concurrency_duration,
            )
            .await;
        }
        info!(job_id, concurrency_key, "Semaphore acquired");
    }

    // Job is ready for immediate execution
    query_builder::ready_executions::insert(
        txn,
        table_config,
        job_id,
        &job_model.queue_name,
        job_model.priority,
    )
    .await?;

    Ok((JobDestination::Ready, None))
}

/// Handle the case when concurrency limit is reached
async fn handle_concurrency_conflict(
    txn: &DatabaseTransaction,
    ctx: &AppContext,
    job_model: &quebec_jobs::Model,
    job: &ActiveJob,
    concurrency_key: &str,
    now: chrono::NaiveDateTime,
    concurrency_duration: chrono::Duration,
) -> std::result::Result<(JobDestination, Option<i64>), DbErr> {
    let table_config = &ctx.table_config;
    let job_id = job_model.id;

    match job.concurrency_on_conflict {
        ConcurrencyConflict::Discard => {
            warn!(
                job_id,
                "Job `{}' discarded due to: {{key={:?}, limit={}, duration={}s}}",
                job.class_name,
                concurrency_key,
                job.concurrency_limit.unwrap_or(1),
                concurrency_duration.num_seconds()
            );
            query_builder::jobs::mark_finished(txn, table_config, job_id).await?;
            let released = if job.batch_id.is_some() {
                crate::batch::release_job(ctx, txn, job_id).await?
            } else {
                None
            };
            Ok((JobDestination::Discarded, released))
        }
        ConcurrencyConflict::Block => {
            info!(
                job_id,
                "Job `{}' blocked due to: {{key={:?}, limit={}, duration={}s}}",
                job.class_name,
                concurrency_key,
                job.concurrency_limit.unwrap_or(1),
                concurrency_duration.num_seconds()
            );
            let expires_at = now + concurrency_duration;
            query_builder::blocked_executions::insert(
                txn,
                table_config,
                job_id,
                &job_model.queue_name,
                job_model.priority,
                concurrency_key,
                expires_at,
            )
            .await?;
            Ok((JobDestination::Blocked, None))
        }
    }
}

/// Bulk enqueue logic, runs inside a single transaction.
/// Returns (job_models, ready_queue_names) so the caller can NOTIFY only
/// queues that actually have ready jobs.
async fn enqueue_all_jobs(
    txn: &DatabaseTransaction,
    ctx: &AppContext,
    jobs: &[PreparedJob],
    concurrency_duration: chrono::Duration,
) -> std::result::Result<(Vec<quebec_jobs::Model>, HashSet<String>, Vec<i64>), DbErr> {
    let table_config = &ctx.table_config;
    let now = chrono::Utc::now().naive_utc();

    // Phase 1: bulk INSERT all jobs
    // Default scheduled_at to now (matching Solid Queue's `scheduled_at ||= Time.current`
    // and Quebec's own single-enqueue path)
    let bulk_rows: Vec<query_builder::jobs::BulkJobRow> = jobs
        .iter()
        .map(|j| query_builder::jobs::BulkJobRow {
            queue_name: j.queue_name.clone(),
            class_name: j.class_name.clone(),
            arguments: Some(j.arguments.clone()),
            priority: j.priority,
            active_job_id: j.active_job_id.clone(),
            scheduled_at: Some(j.scheduled_at.unwrap_or(now)),
            concurrency_key: j.concurrency_key.clone(),
            batch_id: j.batch_id,
        })
        .collect();

    let job_models =
        query_builder::jobs::insert_all_returning(txn, table_config, &bulk_rows, now).await?;

    // Track batch membership before dispatch (Solid Queue's `batch_all`), so
    // jobs discarded by a concurrency conflict below still count.
    let mut by_batch: std::collections::BTreeMap<i64, Vec<(i64, i32, &str)>> =
        std::collections::BTreeMap::new();
    for (model, prepared) in job_models.iter().zip(jobs.iter()) {
        if let Some(batch_id) = prepared.batch_id {
            by_batch.entry(batch_id).or_default().push((
                model.id,
                0,
                prepared.active_job_id.as_str(),
            ));
        }
    }
    for (batch_id, tracked) in &by_batch {
        crate::batch::track_jobs(txn, ctx, *batch_id, tracked).await?;
    }
    let mut released_batches: Vec<i64> = Vec::new();

    // Phase 2: route each job to ready / scheduled / concurrency
    // Collect bulk inserts for ready and scheduled, route concurrency jobs individually
    let mut ready_data: Vec<(i64, &str, i32)> = Vec::new();
    let mut scheduled_data: Vec<(i64, &str, i32, chrono::NaiveDateTime)> = Vec::new();
    let mut ready_queues: HashSet<String> = HashSet::new();

    for (model, prepared) in job_models.iter().zip(jobs.iter()) {
        let concurrency_key = prepared.concurrency_key.as_deref().unwrap_or_default();

        // Check if job is scheduled for the future
        if let Some(sched) = prepared.scheduled_at {
            if sched > now {
                scheduled_data.push((model.id, &model.queue_name, model.priority, sched));
                continue;
            }
        }

        // Handle concurrency control if configured
        if !concurrency_key.is_empty() {
            // Concurrency jobs go through the existing per-job route
            let dummy_active_job = ActiveJob {
                logger: crate::types::ActiveLogger,
                id: Some(model.id),
                queue_name: prepared.queue_name.clone(),
                class_name: prepared.class_name.clone(),
                arguments: prepared.arguments.clone(),
                priority: prepared.priority,
                executions: 0,
                active_job_id: prepared.active_job_id.clone(),
                scheduled_at: prepared.scheduled_at.unwrap_or(now),
                finished_at: None,
                concurrency_key: prepared.concurrency_key.clone(),
                concurrency_limit: prepared.concurrency_limit,
                concurrency_on_conflict: prepared.concurrency_on_conflict,
                created_at: None,
                updated_at: None,
                batch_id: prepared.batch_id,
                callback_batch_id: None,
            };
            let (destination, released) = route_job(
                txn,
                ctx,
                model,
                &dummy_active_job,
                concurrency_key,
                now,
                concurrency_duration,
            )
            .await?;
            if destination.should_notify() {
                ready_queues.insert(model.queue_name.clone());
            }
            released_batches.extend(released);
            continue;
        }

        // No concurrency, immediately ready
        ready_queues.insert(model.queue_name.clone());
        ready_data.push((model.id, &model.queue_name, model.priority));
    }

    // Phase 3: bulk insert ready and scheduled executions
    if !ready_data.is_empty() {
        query_builder::ready_executions::insert_all(txn, table_config, &ready_data).await?;
    }
    if !scheduled_data.is_empty() {
        query_builder::scheduled_executions::insert_all(txn, table_config, &scheduled_data).await?;
    }

    Ok((job_models, ready_queues, released_batches))
}
