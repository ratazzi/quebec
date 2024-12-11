use crate::context::*;
use crate::entities::{prelude::*, *};
use crate::semaphore::acquire_semaphore;
use crate::types::ActiveJob;
use log::{debug, error, info, trace, warn};
use pyo3::prelude::*;
use pyo3::types::PyType;
use pyo3::PyResult;
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

    pub async fn perform_later(
        &self, klass: &Bound<'_, PyType>, job: ActiveJob,
    ) -> Result<solid_queue_jobs::Model, anyhow::Error> {
        let db = self.ctx.get_db().await;
        let _ = db.ping().await?;
        let duration = self.ctx.default_concurrency_control_period.clone();
        println!("------------ job: {:?}", job);

        let job = db
            .transaction::<_, solid_queue_jobs::ActiveModel, DbErr>(|txn| {
                let args: serde_json::Value = serde_json::from_str(job.arguments.as_str()).unwrap();
                let params = serde_json::json!({
                    "job_class": "FakeJob",
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

                Box::pin(async move {
                    let concurrency_key = job.concurrency_key.clone().unwrap_or("".to_string());
                    let job = solid_queue_jobs::ActiveModel {
                        id: ActiveValue::NotSet,
                        queue_name: ActiveValue::Set(job.queue_name),
                        class_name: ActiveValue::Set(job.class_name),
                        arguments: ActiveValue::Set(Some(params.to_string())),
                        priority: ActiveValue::Set(job.priority),
                        active_job_id: ActiveValue::Set(Some(job.active_job_id)),
                        scheduled_at: ActiveValue::Set(Some(job.scheduled_at)),
                        finished_at: ActiveValue::Set(None),
                        concurrency_key: ActiveValue::Set(Some(concurrency_key.clone())),
                        created_at: ActiveValue::Set(chrono::Utc::now().naive_utc()),
                        updated_at: ActiveValue::Set(chrono::Utc::now().naive_utc()),
                    }
                    .save(txn)
                    .await?;

                    if !concurrency_key.is_empty() {
                        // Try to acquire the semaphore
                        let now = chrono::Utc::now().naive_utc();
                        let expires_at = now + duration;
                        if acquire_semaphore(txn, concurrency_key.clone()).await.unwrap() {
                            info!("Semaphore acquired");

                            // Do some work...

                            // Release the semaphore
                            // quebec.release_semaphore(&txn, key).await?;
                            // println!("--------------- Semaphore released!");
                        } else {
                            warn!("Failed to acquire semaphore");

                            let blocked_execution = solid_queue_blocked_executions::ActiveModel {
                                id: ActiveValue::NotSet,
                                queue_name: ActiveValue::Set("default".to_string()),
                                job_id: ActiveValue::Set(job.id.clone().unwrap()),
                                priority: job.priority.clone(),
                                concurrency_key: ActiveValue::Set(concurrency_key.clone()),
                                expires_at: ActiveValue::Set(expires_at),
                                created_at: ActiveValue::Set(chrono::Utc::now().naive_utc()),
                            }
                            .save(txn)
                            .await?;

                            return Ok(job);
                        }
                    }

                    let ready_execution = solid_queue_ready_executions::ActiveModel {
                        id: ActiveValue::NotSet,
                        queue_name: ActiveValue::Set("default".to_string()),
                        job_id: ActiveValue::Set(job.id.clone().unwrap()),
                        priority: job.priority.clone(),
                        created_at: ActiveValue::Set(chrono::Utc::now().naive_utc()),
                    }
                    .save(txn)
                    .await?;

                    // info!("Job created: {:?}", job.clone().try_into_model());
                    Ok(job)
                })
            })
            .await?;

        Ok(job.try_into_model()?)
    }
}
