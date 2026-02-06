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
}

/// Where the job was routed after enqueue
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JobDestination {
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
    fn should_notify(&self) -> bool {
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
        jobs: Vec<PreparedJob>,
    ) -> Result<Vec<quebec_jobs::Model>> {
        if jobs.is_empty() {
            return Ok(vec![]);
        }

        let db = self.ctx.get_db().await;
        let ctx = self.ctx.clone();

        let (job_models, ready_queues) = db
            .transaction::<_, (Vec<quebec_jobs::Model>, HashSet<String>), DbErr>(|txn| {
                let table_config = ctx.table_config.clone();
                let duration = chrono::Duration::from_std(ctx.default_concurrency_control_period)
                    .unwrap_or_else(|_| chrono::Duration::seconds(60));
                Box::pin(async move { enqueue_all_jobs(txn, &table_config, &jobs, duration).await })
            })
            .await
            .map_err(crate::error::QuebecError::from)?;

        // Send NOTIFY only for queues that have ready jobs (consistent with perform_later)
        if self.ctx.is_postgres() {
            for queue_name in &ready_queues {
                NotifyManager::send_notify(&self.ctx.name, &*db, queue_name, "new_job")
                    .await
                    .inspect_err(|e| warn!("Failed to send NOTIFY: {}", e))
                    .ok();
            }
        }

        Ok(job_models)
    }

    pub async fn perform_later(&self, job: ActiveJob) -> Result<quebec_jobs::Model> {
        let db = self.ctx.get_db().await;
        let ctx = self.ctx.clone();
        trace!("job: {:?}", job);

        let (job_model, destination) = db
            .transaction::<_, (quebec_jobs::Model, JobDestination), DbErr>(|txn| {
                let table_config = ctx.table_config.clone();
                let duration = chrono::Duration::from_std(ctx.default_concurrency_control_period)
                    .unwrap_or_else(|_| chrono::Duration::seconds(60));
                let job = job.clone();
                Box::pin(async move { enqueue_job(txn, &table_config, &job, duration).await })
            })
            .await
            .map_err(crate::error::QuebecError::from)?;

        if destination.should_notify() && self.ctx.is_postgres() {
            NotifyManager::send_notify(&self.ctx.name, &*db, &job_model.queue_name, "new_job")
                .await
                .inspect_err(|e| warn!("Failed to send NOTIFY: {}", e))
                .ok();
        }

        Ok(job_model)
    }
}

/// Core job enqueue logic, runs inside a transaction
async fn enqueue_job(
    txn: &DatabaseTransaction,
    table_config: &TableConfig,
    job: &ActiveJob,
    concurrency_duration: chrono::Duration,
) -> std::result::Result<(quebec_jobs::Model, JobDestination), DbErr> {
    let now = chrono::Utc::now().naive_utc();

    // Validate arguments JSON without full parsing (zero-copy validation)
    let args: Box<serde_json::value::RawValue> = serde_json::from_str(&job.arguments)
        .map_err(|e| DbErr::Custom(format!("Invalid JSON in arguments: {}", e)))?;

    let params = serde_json::json!({
        "job_class": job.class_name,
        "job_id": null,
        "provider_job_id": "",
        "queue_name": job.queue_name,
        "priority": job.priority,
        "arguments": args,
        "executions": 0,
        "exception_executions": {},
        "locale": "en",
        "timezone": "UTC",
        "scheduled_at": null,
        "enqueued_at": now,
    });

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
    )
    .await?;

    // Route job to appropriate destination
    let destination = route_job(
        txn,
        table_config,
        &job_model,
        job,
        concurrency_key,
        now,
        concurrency_duration,
    )
    .await?;

    Ok((job_model, destination))
}

/// Determine where the job should go based on scheduling and concurrency
async fn route_job(
    txn: &DatabaseTransaction,
    table_config: &TableConfig,
    job_model: &quebec_jobs::Model,
    job: &ActiveJob,
    concurrency_key: &str,
    now: chrono::NaiveDateTime,
    concurrency_duration: chrono::Duration,
) -> std::result::Result<JobDestination, DbErr> {
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
        return Ok(JobDestination::Scheduled);
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
                table_config,
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

    Ok(JobDestination::Ready)
}

/// Handle the case when concurrency limit is reached
async fn handle_concurrency_conflict(
    txn: &DatabaseTransaction,
    table_config: &TableConfig,
    job_model: &quebec_jobs::Model,
    job: &ActiveJob,
    concurrency_key: &str,
    now: chrono::NaiveDateTime,
    concurrency_duration: chrono::Duration,
) -> std::result::Result<JobDestination, DbErr> {
    let job_id = job_model.id;

    match job.concurrency_on_conflict {
        ConcurrencyConflict::Discard => {
            warn!(
                job_id,
                concurrency_key,
                class_name = %job.class_name,
                "Job discarded due to concurrency limit"
            );
            query_builder::jobs::mark_finished(txn, table_config, job_id).await?;
            Ok(JobDestination::Discarded)
        }
        ConcurrencyConflict::Block => {
            info!(
                job_id,
                concurrency_key, "Job blocked, adding to blocked queue"
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
            Ok(JobDestination::Blocked)
        }
    }
}

/// Bulk enqueue logic, runs inside a single transaction.
/// Returns (job_models, ready_queue_names) so the caller can NOTIFY only
/// queues that actually have ready jobs.
async fn enqueue_all_jobs(
    txn: &DatabaseTransaction,
    table_config: &TableConfig,
    jobs: &[PreparedJob],
    concurrency_duration: chrono::Duration,
) -> std::result::Result<(Vec<quebec_jobs::Model>, HashSet<String>), DbErr> {
    let now = chrono::Utc::now().naive_utc();

    // Phase 1: bulk INSERT all jobs
    let bulk_rows: Vec<query_builder::jobs::BulkJobRow> = jobs
        .iter()
        .map(|j| query_builder::jobs::BulkJobRow {
            queue_name: j.queue_name.clone(),
            class_name: j.class_name.clone(),
            arguments: Some(j.arguments.clone()),
            priority: j.priority,
            active_job_id: j.active_job_id.clone(),
            scheduled_at: j.scheduled_at,
            concurrency_key: j.concurrency_key.clone(),
        })
        .collect();

    let job_models =
        query_builder::jobs::insert_all_returning(txn, table_config, &bulk_rows, now).await?;

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
            };
            let destination = route_job(
                txn,
                table_config,
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

    Ok((job_models, ready_queues))
}
