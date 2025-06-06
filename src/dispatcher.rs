use crate::context::*;
use crate::entities::{prelude::*, *};
use crate::process::ProcessTrait;
use crate::semaphore::acquire_semaphore;
use anyhow::Result;
use async_trait::async_trait;
use sea_orm::TransactionTrait;
use sea_orm::*;
use std::sync::Arc;

use tracing::Instrument;
use tracing::{debug, error, info, trace, warn};

#[derive(Debug)]
pub struct Dispatcher {
    pub ctx: Arc<AppContext>,
}

impl Dispatcher {
    pub fn new(ctx: Arc<AppContext>) -> Self {
        Self { ctx }
    }

    // pub async fn run(&self) -> Result<()> {
    //     let mut workers = vec![];
    //     let mut handles = vec![];

    //     for i in 0..self.ctx.connect_options.worker_count {
    //         let ctx = self.ctx.clone();
    //         let worker = Worker::new(ctx.clone());
    //         let handle = tokio::spawn(worker.run());
    //         workers.push(worker);
    //         handles.push(handle);
    //     }

    //     for handle in handles {
    //         handle.await?;
    //     }

    //     Ok(())
    // }

    pub async fn run(&self) -> Result<(), anyhow::Error> {
        let db = self.ctx.get_db().await;
        let mut polling_interval = tokio::time::interval(self.ctx.dispatcher_polling_interval);
        let mut heartbeat_interval = tokio::time::interval(self.ctx.process_heartbeat_interval);
        let batch_size = self.ctx.dispatcher_batch_size;

        // let kind = "Dispatcher".to_string();
        // let name = "dispatcher".to_string();
        // let process = self.on_start(&db, kind, name).await?;
        // info!(">> Process started: {:?}", process);

        let quit = self.ctx.graceful_shutdown.clone();

        loop {
            tokio::select! {
              _ = heartbeat_interval.tick() => {
                // self.heartbeat(&db, &process).await?;
              }
              _ = quit.cancelled() => {
                info!("Stopped");
                return Ok(());
              }
              // _ = tokio::signal::ctrl_c() => {
              //   info!("ctrl-c received");
              //   return Ok(());
              // }
              _ = polling_interval.tick() => {
                // let pool = db.get_postgres_connection_pool();
                // debug!("----- Max connections: {}, Idle connections: {}", pool.size(), pool.num_idle());

                let _ = db.transaction::<_, (), DbErr>(|txn| {
                    Box::pin(async move {
                      // delete expired semaphores
                      // let result =
                      //     solid_queue_semaphores::Entity::delete_many()
                      //         .filter(
                      //             solid_queue_semaphores::Column::ExpiresAt.lt(chrono::Utc::now().naive_utc())
                      //         )
                      //         .exec(txn)
                      //         .await?;

                      // debug!("Deleted semaphores: {:?}", result);
                      // if result.rows_affected > 0 {
                      //     debug!("Deleted semaphores: {:?}", result.rows_affected);
                      // }

                      // // select distinct concurrency_key from solid_queue_blocked_executions
                      // let sql = "SELECT DISTINCT concurrency_key FROM solid_queue_blocked_executions WHERE expires_at < $1 LIMIT $2";

                      // let now = chrono::Utc::now().naive_utc();
                      // let result = txn.query_all(Statement::from_sql_and_values(
                      //     txn.get_database_backend(),
                      //     sql,
                      //     vec![now.into(), batch_size.into()],
                      // )).await?;

                      // info!("Unblock jobs size: {:?}", result.len());

                      // for row in result {
                      //     let concurrency_key = row.try_get::<String>("", "concurrency_key").unwrap();
                      //     // println!("Concurrency Key: {:?}", row.try_get::<String>("", "concurrency_key"));
                      //     // println!("Concurrency Key: {:?}", row.try_get_by_index::<String>(0));

                      //     // TRANSACTION (0.2ms)  BEGIN
                      //     //   SolidQueue::BlockedExecution Load (1.4ms)  SELECT "solid_queue_blocked_executions".* FROM "solid_queue_blocked_executions" WHERE "solid_queue_blocked_executions"."concurrency_key" = $1 ORDER BY "solid_queue_blocked_executions"."priority" ASC, "solid_queue_blocked_executions"."job_id" ASC LIMIT $2 FOR UPDATE SKIP LOCKED  [["concurrency_key", "FakeJob/3466"], ["LIMIT", 1]]
                      //     //   SolidQueue::Job Load (4.5ms)  SELECT "solid_queue_jobs".* FROM "solid_queue_jobs" WHERE "solid_queue_jobs"."id" = $1 LIMIT $2  [["id", 422969], ["LIMIT", 1]]
                      //     //   SolidQueue::Semaphore Load (0.3ms)  SELECT "solid_queue_semaphores".* FROM "solid_queue_semaphores" WHERE "solid_queue_semaphores"."key" = $1 LIMIT $2  [["key", "FakeJob/3466"], ["LIMIT", 1]]
                      //     //   SolidQueue::Semaphore Insert (1.7ms)  INSERT INTO "solid_queue_semaphores" ("key","value","expires_at","created_at","updated_at") VALUES ('FakeJob/3466', 0, '2024-11-05 08:54:44.377237', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) ON CONFLICT ("key") DO NOTHING RETURNING "id"
                      //     //   SolidQueue::Job Load (0.3ms)  SELECT "solid_queue_jobs".* FROM "solid_queue_jobs" WHERE "solid_queue_jobs"."id" = $1 LIMIT $2  [["id", 422969], ["LIMIT", 1]]
                      //     //   SolidQueue::ReadyExecution Create (3.4ms)  INSERT INTO "solid_queue_ready_executions" ("job_id", "queue_name", "priority", "created_at") VALUES ($1, $2, $3, $4) RETURNING "id"  [["job_id", 422969], ["queue_name", "high"], ["priority", 0], ["created_at", "2024-11-05 08:53:44.384093"]]
                      //     //   SolidQueue::BlockedExecution Destroy (0.4ms)  DELETE FROM "solid_queue_blocked_executions" WHERE "solid_queue_blocked_executions"."id" = $1  [["id", 21]]
                      //     // D, [2024-11-05T16:53:44.398640 #61704] DEBUG -- : SolidQueue-1.0.0 Release blocked job (22.6ms)  job_id: 422969, concurrency_key: "FakeJob/3466", released: true
                      //     //   TRANSACTION (0.8ms)  COMMIT

                      //     let record = solid_queue_blocked_executions::Entity::find()
                      //          .from_raw_sql(Statement::from_sql_and_values(
                      //             txn.get_database_backend(),
                      //             r#"SELECT * FROM "solid_queue_blocked_executions" WHERE "concurrency_key" = $1 ORDER BY "priority" ASC, "job_id" ASC LIMIT $2 FOR UPDATE SKIP LOCKED"#,
                      //             [concurrency_key.clone().into(), 1.into()],
                      //         ))
                      //         .one(txn)
                      //         .await?;

                      //     if record.is_none() { continue; }

                      //     let execution = record.unwrap();
                      //     // debug!("execution: {:?}", execution);

                      //     // accquire semaphore
                      //     if !acquire_semaphore(txn, concurrency_key.clone()).await.unwrap() {
                      //         continue;
                      //     } else {
                      //       info!("Semaphore acquired: {:?}", concurrency_key.clone());
                      //     }

                      //     // move to ready_executions and then delete from blocked_executions

                      //     let ready_execution = solid_queue_ready_executions::ActiveModel {
                      //         id: ActiveValue::NotSet,
                      //         queue_name: ActiveValue::Set("default".to_string()),
                      //         job_id: ActiveValue::Set(execution.job_id.clone()),
                      //         priority: ActiveValue::Set(0),
                      //         created_at: ActiveValue::Set(chrono::Utc::now().naive_utc()),
                      //     }
                      //     .save(txn)
                      //     .await?;

                      //     solid_queue_blocked_executions::Entity::delete_by_id(execution.id)
                      //         .exec(txn)
                      //         .await?;
                      // }

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
                      let scheduled_executions = scheduled_executions.unwrap();
                      let size = scheduled_executions.len();
                      // info!("Dispatching scheduled jobs size: {}", size);
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

                      info!("Dispatch scheduled jobs size: {}", size);

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
