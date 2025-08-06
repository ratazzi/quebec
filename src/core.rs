use crate::context::*;
use crate::entities::*;
use crate::error::Result;
use crate::notify::NotifyManager;
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

    pub async fn perform_later(&self, job: ActiveJob) -> Result<solid_queue_jobs::Model> {
        let db = self.ctx.get_db().await;
        let _ = db.ping().await?;
        let duration = self.ctx.default_concurrency_control_period;
        trace!("job: {:?}", job);

        let job = db
            .transaction::<_, solid_queue_jobs::Model, DbErr>(|txn| {
                Box::pin(async move {
                    let args: serde_json::Value = serde_json::from_str(job.arguments.as_str())
                        .map_err(|e| DbErr::Custom(format!("Serialization error: {}", e)))?;
                let params = serde_json::json!({
                    "job_class": job.class_name.clone(),
                    "job_id": null,
                    "provider_job_id": "",
                    "queue_name": "default",
                    "priority": job.priority,
                    "arguments": args,
                    "executions": 0,
                    "exception_executions": {},
                    "locale": "en",
                    "timezone": "UTC",
                    "scheduled_at": null,
                    "enqueued_at": chrono::Utc::now().naive_utc(),
                });

                    let concurrency_key = job.concurrency_key.clone().unwrap_or_default();
                    let concurrency_limit = job.concurrency_limit.unwrap_or(1);
                    
                    // Insert job and get the model with generated ID
                    let job_model = solid_queue_jobs::ActiveModel {
                        id: ActiveValue::NotSet,
                        queue_name: ActiveValue::Set(job.queue_name),
                        class_name: ActiveValue::Set(job.class_name),
                        arguments: ActiveValue::Set(Some(params.to_string())),
                        priority: ActiveValue::Set(job.priority),
                        failed_attempts: ActiveValue::Set(0),
                        active_job_id: ActiveValue::Set(Some(job.active_job_id)),
                        scheduled_at: ActiveValue::Set(Some(job.scheduled_at)),
                        finished_at: ActiveValue::Set(None),
                        concurrency_key: ActiveValue::Set(Some(concurrency_key.clone())),
                        created_at: ActiveValue::Set(chrono::Utc::now().naive_utc()),
                        updated_at: ActiveValue::Set(chrono::Utc::now().naive_utc()),
                    }
                    .save(txn)
                    .await?;
                    
                    // Convert ActiveModel to Model
                    let job_model = job_model.try_into_model()?;
                    let job_id = job_model.id;
                    let job_priority = job_model.priority;

                    if !concurrency_key.is_empty() {
                        // Try to acquire the semaphore
                        let now = chrono::Utc::now().naive_utc();
                        let expires_at = now + duration;
                        if acquire_semaphore(txn, concurrency_key.clone(), concurrency_limit, None).await? {
                            info!("Semaphore acquired for key: {}", concurrency_key);
                        } else {
                            info!("Failed to acquire semaphore for key: {}, adding to blocked queue", concurrency_key);

                            let _blocked_execution = solid_queue_blocked_executions::ActiveModel {
                                id: ActiveValue::NotSet,
                                queue_name: ActiveValue::Set("default".to_string()),
                                job_id: ActiveValue::Set(job_id),
                                priority: ActiveValue::Set(job_priority),
                                concurrency_key: ActiveValue::Set(concurrency_key.clone()),
                                expires_at: ActiveValue::Set(expires_at),
                                created_at: ActiveValue::Set(chrono::Utc::now().naive_utc()),
                            }
                            .save(txn)
                            .await?;

                            // Job is blocked, don't add to ready queue
                            return Ok(job_model);
                        }
                    }

                    let _ready_execution = solid_queue_ready_executions::ActiveModel {
                        id: ActiveValue::NotSet,
                        queue_name: ActiveValue::Set("default".to_string()),
                        job_id: ActiveValue::Set(job_id),
                        priority: ActiveValue::Set(job_priority),
                        created_at: ActiveValue::Set(chrono::Utc::now().naive_utc()),
                    }
                    .save(txn)
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
            if let Err(e) = NotifyManager::send_notify(&*db, &job_model.queue_name, "new_job").await {
                warn!("Failed to send NOTIFY: {}", e);
            }
        }

        Ok(job_model)
    }
}
