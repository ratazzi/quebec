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
        let _ = db.ping().await?;
        let duration = self.ctx.default_concurrency_control_period;
        let ctx = self.ctx.clone(); // Clone ctx for the async closure
        trace!("job: {:?}", job);

        // Transaction returns (job_model, should_notify)
        // should_notify is true only when job goes to ready_executions
        let (job_model, should_notify) = db
            .transaction::<_, (quebec_jobs::Model, bool), DbErr>(|txn| {
                Box::pin(async move {
                    let now = chrono::Utc::now().naive_utc();
                    let args: serde_json::Value = serde_json::from_str(job.arguments.as_str())
                        .map_err(|e| DbErr::Custom(format!("Serialization error: {}", e)))?;

                    let params = serde_json::json!({
                        "job_class": job.class_name.clone(),
                        "job_id": null,
                        "provider_job_id": "",
                        "queue_name": job.queue_name.clone(),
                        "priority": job.priority,
                        "arguments": args,
                        "executions": 0,
                        "exception_executions": {},
                        "locale": "en",
                        "timezone": "UTC",
                        "scheduled_at": null,
                        "enqueued_at": now,
                    });

                    let concurrency_key = job.concurrency_key.clone().unwrap_or_default();
                    let concurrency_limit = job.concurrency_limit.unwrap_or(1);

                    // Insert job using query_builder with RETURNING * optimization
                    // This avoids a second round-trip for Postgres/SQLite
                    let job_model = query_builder::jobs::insert_returning(
                        txn,
                        &ctx.table_config,
                        &job.queue_name,
                        &job.class_name,
                        Some(params.to_string()).as_deref(),
                        job.priority,
                        Some(job.active_job_id.as_str()),
                        Some(job.scheduled_at),
                        if concurrency_key.is_empty() { None } else { Some(concurrency_key.as_str()) },
                    )
                    .await?;

                    let job_id = job_model.id;

                    let job_priority = job_model.priority;

                    // Get queue_name from job for proper propagation
                    let job_queue_name = job_model.queue_name.clone();

                    // Get conflict strategy from job
                    let conflict_strategy = job.concurrency_on_conflict;

                    // Check if job is scheduled for the future
                    // Note: When no scheduled_at is specified, it defaults to job creation time.
                    // By the time we reach this check, `now` will be >= creation time,
                    // so only explicitly future-scheduled jobs will take this branch.
                    let is_scheduled = job.scheduled_at > now;

                    if is_scheduled {
                        // Job is scheduled for future, add to scheduled_executions
                        // Don't notify - job is not ready yet
                        info!(
                            "Job scheduled for future execution at {:?}",
                            job.scheduled_at
                        );
                        query_builder::scheduled_executions::insert(
                            txn,
                            &ctx.table_config,
                            job_id,
                            &job_queue_name,
                            job_priority,
                            job.scheduled_at,
                        )
                        .await?;
                        return Ok((job_model, false));
                    }

                    if !concurrency_key.is_empty() {
                        // Try to acquire the semaphore
                        let expires_at = now + duration;
                        if acquire_semaphore(txn, &ctx.table_config, concurrency_key.clone(), concurrency_limit, None).await? {
                            info!("Semaphore acquired for key: {}", concurrency_key);
                        } else {
                            // Handle based on conflict strategy
                            match conflict_strategy {
                                crate::context::ConcurrencyConflict::Discard => {
                                    // Discard strategy: mark job as finished without execution.
                                    // This is the expected behavior when user chooses "discard" -
                                    // the job is intentionally dropped when concurrency limit is reached.
                                    // Note: The job will appear as "finished" but was never executed.
                                    // For observability, monitor this log or use external metrics.
                                    // Don't notify - job was discarded
                                    warn!(
                                        job_id = job_id,
                                        concurrency_key = %concurrency_key,
                                        class_name = %job.class_name,
                                        "Job discarded due to concurrency limit (conflict_strategy=discard)"
                                    );
                                    query_builder::jobs::mark_finished(txn, &ctx.table_config, job_id).await?;
                                    return Ok((job_model, false));
                                }
                                crate::context::ConcurrencyConflict::Block => {
                                    // Block strategy: add to blocked queue (default behavior)
                                    // Don't notify - job is blocked
                                    info!("Failed to acquire semaphore for key: {}, adding to blocked queue", concurrency_key);

                                    query_builder::blocked_executions::insert(
                                        txn,
                                        &ctx.table_config,
                                        job_id,
                                        &job_queue_name,
                                        job_priority,
                                        &concurrency_key,
                                        expires_at,
                                    )
                                    .await?;

                                    // Job is blocked, don't add to ready queue
                                    return Ok((job_model, false));
                                }
                            }
                        }
                    }

                    query_builder::ready_executions::insert(
                        txn,
                        &ctx.table_config,
                        job_id,
                        &job_queue_name,
                        job_priority,
                    )
                    .await?;

                    // Job is ready - should notify
                    Ok((job_model, true))
                })
            })
            .await
            .map_err(|e| crate::error::QuebecError::from(e))?;

        // Send PostgreSQL NOTIFY only when job is ready for immediate execution
        if should_notify && self.ctx.is_postgres() {
            if let Err(e) =
                NotifyManager::send_notify(&self.ctx.name, &*db, &job_model.queue_name, "new_job")
                    .await
            {
                warn!("Failed to send NOTIFY: {}", e);
            }
        }

        Ok(job_model)
    }
}
