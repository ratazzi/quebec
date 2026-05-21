use crate::context::*;
use crate::process::{ProcessInfo, ProcessTrait};
use crate::query_builder;
use crate::semaphore::acquire_semaphore;

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
                    self.heartbeat(&heartbeat_db, &process).await?;
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

                          // Collect expired concurrency keys
                          let expired_keys: Vec<String> = expired_keys_result
                              .iter()
                              .filter_map(|row| row.try_get::<String>("", "concurrency_key").ok())
                              .collect();

                          if expired_keys.is_empty() {
                              trace!("No expired concurrency keys found to unblock");
                          } else {
                              info!(
                                  "Found {} expired concurrency keys to unblock: {:?}",
                                  expired_keys.len(),
                                  expired_keys
                              );
                          }

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

                          // Split scheduled-due jobs into two groups so we can
                          // bulk-promote the unrestricted ones but still enforce
                          // concurrency limits on the rest (matches Solid Queue's
                          // `Job.dispatch_all` partition between
                          // `dispatch_all_at_once` and `dispatch_all_one_by_one`).
                          // Without this split, every retry / wait_until / wait
                          // path bypassed the semaphore at promotion time and
                          // could violate the declared concurrency_limit.
                          let mut ready_data: Vec<(i64, &str, i32)> = Vec::new();
                          let mut concurrency_limited: Vec<&_> = Vec::new();
                          for se in &scheduled_executions {
                              let Some(job) = job_map.get(&se.job_id) else {
                                  warn!("Job {} not found for scheduled execution {}", se.job_id, se.id);
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
                                  ready_data.push((se.job_id, &job.queue_name, job.priority));
                                  notified_queues.insert(job.queue_name.clone());
                              }
                          }

                          // Bulk insert the unrestricted ones in a single round-trip.
                          if !ready_data.is_empty() {
                              query_builder::ready_executions::insert_all(
                                  txn,
                                  &ctx.table_config,
                                  &ready_data,
                              )
                              .await?;
                          }

                          // Concurrency-limited ones go one-by-one so each can
                          // acquire the semaphore and route to ready / blocked /
                          // discarded per its on_conflict policy.
                          for job in concurrency_limited {
                              let concurrency_key = job.concurrency_key.as_deref().unwrap_or("");
                              let (limit, duration_opt, on_conflict) = {
                                  #[cfg(feature = "python")]
                                  {
                                      ctx.runnables
                                          .read()
                                          .ok()
                                          .and_then(|runnables| {
                                              runnables.get(&job.class_name).map(|r| {
                                                  (
                                                      r.concurrency_limit.unwrap_or(1),
                                                      r.concurrency_duration
                                                          .map(|s| chrono::Duration::seconds(s as i64)),
                                                      r.concurrency_on_conflict,
                                                  )
                                              })
                                          })
                                          .unwrap_or((1, None, ConcurrencyConflict::Block))
                                  }
                                  #[cfg(not(feature = "python"))]
                                  {
                                      (1, None, ConcurrencyConflict::Block)
                                  }
                              };

                              let acquired = acquire_semaphore(
                                  txn,
                                  &ctx.table_config,
                                  concurrency_key.to_string(),
                                  limit,
                                  duration_opt,
                              )
                              .await?;

                              if acquired {
                                  query_builder::ready_executions::insert(
                                      txn,
                                      &ctx.table_config,
                                      job.id,
                                      &job.queue_name,
                                      job.priority,
                                  )
                                  .await?;
                                  notified_queues.insert(job.queue_name.clone());
                              } else {
                                  let duration = duration_opt.unwrap_or_else(|| {
                                      chrono::Duration::from_std(
                                          ctx.default_concurrency_control_period,
                                      )
                                      .unwrap_or_else(|_| chrono::Duration::seconds(60))
                                  });
                                  match on_conflict {
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
                                              now + duration,
                                          )
                                          .await?;
                                      }
                                  }
                              }
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

                          Ok(notified_queues)
                        })
                    })
                    .instrument(tracing::info_span!("polling", component = "dispatcher"))
                    .await;

                    // Send NOTIFY for each unique queue after transaction commits.
                    // `should_send_notify` enforces backend + use_listen_notify + per-queue throttle.
                    let Ok(queues) = transaction_result else { continue };

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
