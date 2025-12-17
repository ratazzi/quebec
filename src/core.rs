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
use std::sync::Arc;

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
