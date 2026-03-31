use crate::context::*;
use crate::process::{ProcessInfo, ProcessTrait};
use crate::query_builder;
use crate::semaphore::acquire_semaphore;

use anyhow::Result;
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
}

impl Dispatcher {
    pub fn new(ctx: Arc<AppContext>) -> Self {
        Self { ctx }
    }

    pub async fn run(&self) -> Result<(), anyhow::Error> {
        let mut polling_interval = tokio::time::interval(self.ctx.dispatcher_polling_interval);
        let mut heartbeat_interval = tokio::time::interval(self.ctx.process_heartbeat_interval);
        let batch_size = self.ctx.dispatcher_batch_size;

        let init_db = self.ctx.get_db().await?;
        let process = self.on_start(&init_db).await?;
        info!(">> Process started: {:?}", process);

        let quit = self.ctx.graceful_shutdown.clone();

        loop {
            tokio::select! {
                _ = heartbeat_interval.tick() => {
                    let Ok(heartbeat_db) = self.ctx.get_db().await.inspect_err(|e| {
                        warn!("Failed to get DB for heartbeat: {}", e);
                    }) else { continue };
                    self.heartbeat(&heartbeat_db, &process).await?;
                }
                _ = quit.cancelled() => {
                    info!("Stopped");
                    let stop_db = self.ctx.get_db().await?;
                    self.on_stop(&stop_db, &process).await?;
                    return Ok(());
                }
                _ = polling_interval.tick() => {
                    let Ok(polling_db) = self.ctx.get_db().await.inspect_err(|e| {
                        warn!("Failed to get DB for polling: {}", e);
                    }) else { continue };
                    let ctx = self.ctx.clone(); // Clone ctx for the async closure
                    let transaction_result = polling_db.transaction::<_, std::collections::HashSet<String>, DbErr>(|txn| {
                        Box::pin(async move {
                          // Clean up expired semaphores
                          let expired_semaphores_count =
                              query_builder::semaphores::delete_expired(txn, &ctx.table_config).await?;

                          if expired_semaphores_count > 0 {
                              info!("Cleaned up {} expired semaphores", expired_semaphores_count);
                          }

                          // Unblock jobs with expired concurrency keys
                          let backend = txn.get_database_backend();
                          let (p1, p2) = match backend {
                              DbBackend::Postgres => ("$1", "$2"),
                              DbBackend::MySql | DbBackend::Sqlite => ("?", "?"),
                          };
                          let sql = format!(
                              "SELECT DISTINCT concurrency_key FROM {} WHERE expires_at < {} LIMIT {}",
                              ctx.table_config.blocked_executions, p1, p2
                          );
                          let now = chrono::Utc::now().naive_utc();
                          let expired_keys_result = txn.query_all(Statement::from_sql_and_values(
                              backend,
                              sql,
                              vec![now.into(), batch_size.into()],
                          )).await?;

                          if expired_keys_result.is_empty() {
                              trace!("No expired concurrency keys found to unblock");
                          } else {
                              info!("Found {} expired concurrency keys to unblock", expired_keys_result.len());
                          }

                          // Collect expired concurrency keys
                          let expired_keys: Vec<String> = expired_keys_result
                              .iter()
                              .filter_map(|row| row.try_get::<String>("", "concurrency_key").ok())
                              .collect();

                          // Pre-filter: batch check semaphores (like Solid Queue's releasable)
                          // Keys without semaphore OR with value > 0 are releasable
                          let semaphore_map = query_builder::semaphores::find_values_by_keys(
                              txn, &ctx.table_config, &expired_keys,
                          ).await?;
                          let releasable_keys: Vec<&String> = expired_keys.iter().filter(|k| {
                              semaphore_map.get(*k).map_or(true, |&v| v > 0)
                          }).collect();

                          for concurrency_key in releasable_keys {
                              let blocked_execution = query_builder::blocked_executions::find_one_by_key_for_update(
                                  txn,
                                  &ctx.table_config,
                                  concurrency_key,
                              ).await?;

                              let Some(execution) = blocked_execution else { continue };

                              // Get concurrency_limit from registered runnable via job's class_name
                              let concurrency_limit = {
                                  #[cfg(feature = "python")]
                                  {
                                      let limit = query_builder::jobs::find_by_id(txn, &ctx.table_config, execution.job_id)
                                          .await?
                                          .and_then(|job| {
                                              ctx.runnables.read().ok().and_then(|runnables| {
                                                  runnables.get(&job.class_name)
                                                      .and_then(|r| r.concurrency_limit)
                                              })
                                          })
                                          .unwrap_or(1);
                                      limit
                                  }
                                  #[cfg(not(feature = "python"))]
                                  { 1 }
                              };

                              // Try to acquire semaphore (like Solid Queue's BlockedExecution.release)
                              match acquire_semaphore(txn, &ctx.table_config, concurrency_key.clone(), concurrency_limit, None).await {
                                  Ok(true) => {
                                      info!("Semaphore acquired for key: {}", concurrency_key);

                                      // Use queue_name and priority from blocked_execution (already stored)
                                      query_builder::ready_executions::insert(
                                          txn,
                                          &ctx.table_config,
                                          execution.job_id,
                                          &execution.queue_name,
                                          execution.priority,
                                      )
                                      .await?;

                                      query_builder::blocked_executions::delete_by_id(txn, &ctx.table_config, execution.id)
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
                          }

                          // Dispatch scheduled jobs
                          // Use FOR UPDATE SKIP LOCKED to avoid conflicts between multiple dispatchers
                          // This matches Solid Queue's implementation
                          let scheduled_executions = query_builder::scheduled_executions::find_due(
                              txn,
                              &ctx.table_config,
                              batch_size,
                              ctx.use_skip_locked,
                          ).await;

                          if scheduled_executions.is_err() {
                              warn!("Error fetching scheduled jobs: {:?}", scheduled_executions.err());
                              return Ok(std::collections::HashSet::new());
                          }
                          let scheduled_executions = scheduled_executions?;
                          let size = scheduled_executions.len();

                          // Collect queue names for NOTIFY
                          let mut notified_queues = std::collections::HashSet::new();

                          // Batch fetch all jobs at once (eliminates N+1)
                          let job_ids: Vec<i64> = scheduled_executions.iter().map(|se| se.job_id).collect();
                          let jobs = query_builder::jobs::find_by_ids(txn, &ctx.table_config, job_ids.clone()).await?;
                          let job_map: std::collections::HashMap<i64, _> = jobs.into_iter().map(|j| (j.id, j)).collect();

                          // Prepare batch data for ready_executions insert
                          let mut ready_data: Vec<(i64, &str, i32)> = Vec::with_capacity(scheduled_executions.len());
                          for se in &scheduled_executions {
                              let Some(job) = job_map.get(&se.job_id) else {
                                  warn!("Job {} not found for scheduled execution {}", se.job_id, se.id);
                                  continue;
                              };
                              ready_data.push((se.job_id, &job.queue_name, job.priority));
                              notified_queues.insert(job.queue_name.clone());
                          }

                          // Batch insert ready_executions + batch delete scheduled_executions
                          query_builder::ready_executions::insert_all(txn, &ctx.table_config, &ready_data).await?;
                          query_builder::scheduled_executions::delete_by_job_ids(txn, &ctx.table_config, &job_ids).await?;

                          if size > 0 {
                              info!("Dispatch scheduled jobs size: {}", size);
                          }

                          Ok(notified_queues)
                        })
                    })
                    .instrument(tracing::info_span!("polling", component = "dispatcher"))
                    .await;

                    // Send NOTIFY for each unique queue after transaction commits
                    let Ok(queues) = transaction_result else { continue };
                    if !self.ctx.is_postgres() { continue; }

                    for queue_name in queues {
                        crate::notify::NotifyManager::send_notify(&self.ctx.name, &*polling_db, &queue_name, "new_job")
                            .await
                            .inspect_err(|e| warn!("Failed to send NOTIFY for queue {}: {}", queue_name, e))
                            .ok();
                    }
                }
            }
        }
    }
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
