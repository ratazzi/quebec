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

        let job = db
            .transaction::<_, quebec_jobs::Model, DbErr>(|txn| {
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

                    // Insert job using query_builder with dynamic table names
                    let job_id = query_builder::jobs::insert(
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

                    // Fetch the inserted job model
                    let job_model = query_builder::jobs::find_by_id(txn, &ctx.table_config, job_id)
                        .await?
                        .ok_or_else(|| DbErr::Custom("Failed to find inserted job".to_string()))?;

                    let job_priority = job_model.priority;

                    // Get queue_name from job for proper propagation
                    let job_queue_name = job_model.queue_name.clone();

                    // Get conflict strategy from job
                    let conflict_strategy = job.concurrency_on_conflict;

                    if !concurrency_key.is_empty() {
                        // Try to acquire the semaphore
                        let expires_at = now + duration;
                        if acquire_semaphore(txn, &ctx.table_config, concurrency_key.clone(), concurrency_limit, None).await? {
                            info!("Semaphore acquired for key: {}", concurrency_key);
                        } else {
                            // Handle based on conflict strategy
                            match conflict_strategy {
                                crate::context::ConcurrencyConflict::Discard => {
                                    // Discard strategy: mark job as finished to avoid dangling job
                                    info!("Concurrency limit reached for key: {}, discarding job (conflict strategy: discard)", concurrency_key);
                                    query_builder::jobs::mark_finished(txn, &ctx.table_config, job_id).await?;
                                    return Ok(job_model);
                                }
                                crate::context::ConcurrencyConflict::Block => {
                                    // Block strategy: add to blocked queue (default behavior)
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
                                    return Ok(job_model);
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

                    // info!("Job created: {:?}", job_model);
                    Ok(job_model)
                })
            })
            .await
            .map_err(|e| crate::error::QuebecError::from(e))?;

        let job_model = job;

        // Send PostgreSQL NOTIFY after job is successfully created
        if self.ctx.is_postgres() {
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
