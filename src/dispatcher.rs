use crate::context::*;
use crate::entities::*;
use crate::process::ProcessTrait;
use crate::semaphore::acquire_semaphore;

use anyhow::Result;
use async_trait::async_trait;
use sea_orm::TransactionTrait;
use sea_orm::*;
use std::sync::Arc;

use tracing::Instrument;
use tracing::{trace, info, warn};
use sea_orm::{DbErr, Statement};

#[derive(Debug)]
pub struct Dispatcher {
    pub ctx: Arc<AppContext>,
}

impl Dispatcher {
    pub fn new(ctx: Arc<AppContext>) -> Self {
        Self { ctx }
    }

    pub async fn run(&self) -> Result<(), anyhow::Error> {
        let mut polling_interval = tokio::time::interval(self.ctx.dispatcher_polling_interval);
        let mut heartbeat_interval = tokio::time::interval(self.ctx.process_heartbeat_interval);
        let batch_size = self.ctx.dispatcher_batch_size;

        let kind = "Dispatcher".to_string();
        let name = "dispatcher".to_string();

        let init_db = self.ctx.get_db().await;
        let process = self.on_start(&init_db, kind, name).await?;
        info!(">> Process started: {:?}", process);

        let quit = self.ctx.graceful_shutdown.clone();

        loop {
            tokio::select! {
                _ = heartbeat_interval.tick() => {
                    let heartbeat_db = self.ctx.get_db().await;
                    self.heartbeat(&heartbeat_db, &process).await?;
                }
                _ = quit.cancelled() => {
                    info!("Stopped");
                    let stop_db = self.ctx.get_db().await;
                    self.on_stop(&stop_db, &process).await?;
                    return Ok(());
                }
                // _ = tokio::signal::ctrl_c() => {
                //   info!("ctrl-c received");
                //   return Ok(());
                // }
                _ = polling_interval.tick() => {
                    let polling_db = self.ctx.get_db().await;
                    let ctx = self.ctx.clone(); // Clone ctx for the async closure
                    let _ = polling_db.transaction::<_, (), DbErr>(|txn| {
                        Box::pin(async move {
                          // Clean up expired semaphores
                          let expired_semaphores_result = solid_queue_semaphores::Entity::delete_many()
                              .filter(
                                  solid_queue_semaphores::Column::ExpiresAt.lt(chrono::Utc::now().naive_utc())
                              )
                              .exec(txn)
                              .await?;

                          if expired_semaphores_result.rows_affected > 0 {
                              info!("Cleaned up {} expired semaphores", expired_semaphores_result.rows_affected);
                          }

                          // Unblock jobs with expired concurrency keys
                          let sql = "SELECT DISTINCT concurrency_key FROM solid_queue_blocked_executions WHERE expires_at < $1 LIMIT $2";
                          let now = chrono::Utc::now().naive_utc();
                          let expired_keys_result = txn.query_all(Statement::from_sql_and_values(
                              txn.get_database_backend(),
                              sql,
                              vec![now.into(), batch_size.into()],
                          )).await?;

                          if expired_keys_result.is_empty() {
                              trace!("No expired concurrency keys found to unblock");
                          } else {
                              info!("Found {} expired concurrency keys to unblock", expired_keys_result.len());
                          }

                          for row in expired_keys_result {
                              let concurrency_key = row.try_get::<String>("", "concurrency_key")
                                  .map_err(|e| DbErr::Custom(format!("Failed to get concurrency_key: {}", e)))?;

                              let blocked_execution = solid_queue_blocked_executions::Entity::find()
                                  .from_raw_sql(Statement::from_sql_and_values(
                                      txn.get_database_backend(),
                                      r#"SELECT * FROM "solid_queue_blocked_executions" WHERE "concurrency_key" = $1 ORDER BY "priority" ASC, "job_id" ASC LIMIT $2 FOR UPDATE SKIP LOCKED"#,
                                      [concurrency_key.clone().into(), 1.into()],
                                  ))
                                  .one(txn)
                                  .await?;

                              if let Some(execution) = blocked_execution {
                                  // Get the job to access concurrency information (like original Solid Queue)
                                  let job = solid_queue_jobs::Entity::find_by_id(execution.job_id)
                                      .one(txn)
                                      .await?;

                                  if let Some(job) = job {
                                      // Get concurrency_limit from registered runnable
                                      let concurrency_limit = {
                                          let runnables = match ctx.runnables.read() {
                                              Ok(r) => r,
                                              Err(e) => {
                                                  warn!("Failed to acquire read lock: {}", e);
                                                  continue;
                                              }
                                          };
                                          runnables.get(&job.class_name)
                                              .and_then(|runnable| runnable.concurrency_limit)
                                              .unwrap_or(1) // Default to 1 if not found
                                      };

                                      // Try to acquire semaphore exactly like original Solid Queue's BlockedExecution.release
                                      match acquire_semaphore(txn, concurrency_key.clone(), concurrency_limit, None).await {
                                          Ok(true) => {
                                              info!("Semaphore acquired for key: {}", concurrency_key);

                                              // Move blocked execution to ready execution
                                              let ready_execution = solid_queue_ready_executions::ActiveModel {
                                                  id: ActiveValue::NotSet,
                                                  queue_name: ActiveValue::Set("default".to_string()),
                                                  job_id: ActiveValue::Set(execution.job_id),
                                                  priority: ActiveValue::Set(0),
                                                  created_at: ActiveValue::Set(chrono::Utc::now().naive_utc()),
                                              };

                                              ready_execution.save(txn).await?;

                                              // Remove from blocked executions
                                              solid_queue_blocked_executions::Entity::delete_by_id(execution.id)
                                                  .exec(txn)
                                                  .await?;

                                              info!("Unblocked job {} for concurrency key: {}", execution.job_id, concurrency_key);
                                          },
                                          Ok(false) => {
                                              trace!("Failed to acquire semaphore for key: {} (no available slots)", concurrency_key);
                                          },
                                          Err(e) => {
                                              warn!("Error acquiring semaphore for key {}: {:?}", concurrency_key, e);
                                          }
                                      }
                                  } else {
                                      warn!("Job {} not found for blocked execution", execution.job_id);
                                  }
                              }
                          }

                          // Dispatch scheduled jobs
                          let now = chrono::Utc::now().naive_utc();
                          let scheduled_executions = solid_queue_scheduled_executions::Entity::find()
                              .filter(solid_queue_scheduled_executions::Column::ScheduledAt.lt(now))
                              .all(txn)
                              .await;

                          if scheduled_executions.is_err() {
                              warn!("Error fetching scheduled jobs: {:?}", scheduled_executions.err());
                              return Ok(());
                          }
                          let scheduled_executions = scheduled_executions?;
                          let size = scheduled_executions.len();

                          for scheduled_execution in scheduled_executions {
                              let _ = solid_queue_ready_executions::ActiveModel {
                                  id: ActiveValue::NotSet,
                                  queue_name: ActiveValue::Set("default".to_string()),
                                  job_id: ActiveValue::Set(scheduled_execution.job_id),
                                  priority: ActiveValue::Set(0),
                                  created_at: ActiveValue::Set(chrono::Utc::now().naive_utc()),
                              }
                              .save(txn)
                              .await?;

                              solid_queue_scheduled_executions::Entity::delete_by_id(scheduled_execution.id)
                                  .exec(txn)
                                  .await?;
                          }

                          if size > 0 {
                              info!("Dispatch scheduled jobs size: {}", size);
                          }

                          Ok(())
                        })
                    })
                    .instrument(tracing::info_span!("dispatcher",))
                    .await;
                }
            }
        }
    }
}

#[async_trait]
impl ProcessTrait for Dispatcher {}
