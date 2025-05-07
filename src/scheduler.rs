use crate::context::{AppContext, ScheduledEntry};
use crate::entities::{prelude::*, *};
use crate::process::ProcessTrait;
use anyhow::Result;
use async_trait::async_trait;
use chrono::NaiveDateTime;
use sea_orm::TransactionTrait;
use sea_orm::*;
use serde_json::json;
use serde_yaml;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

#[cfg(feature = "use-log")]
use log::{debug, error, info, trace, warn};

#[cfg(feature = "use-tracing")]
use tracing::{debug, error, info, trace, warn};

// ```sql
// INSERT INTO "solid_queue_recurring_tasks" (
//     "key",
//     "schedule",
//     "command",
//     "class_name",
//     "arguments",
//     "queue_name",
//     "priority",
//     "static",
//     "description",
//     "created_at",
//     "updated_at"
// ) VALUES (
//     'periodic_cleanup',
//     'every minute',
//     NULL,
//     'FakeJob',
//     '["every_minute"]',
//     NULL,
//     NULL,
//     TRUE,
//     NULL,
//     CURRENT_TIMESTAMP,
//     CURRENT_TIMESTAMP
// ) ON CONFLICT ("key") DO UPDATE SET
//     "updated_at" = (
//         CASE
//             WHEN (
//                 "solid_queue_recurring_tasks"."schedule" IS NOT DISTINCT FROM excluded."schedule"
//                 AND "solid_queue_recurring_tasks"."command" IS NOT DISTINCT FROM excluded."command"
//                 AND "solid_queue_recurring_tasks"."class_name" IS NOT DISTINCT FROM excluded."class_name"
//                 AND "solid_queue_recurring_tasks"."arguments" IS NOT DISTINCT FROM excluded."arguments"
//                 AND "solid_queue_recurring_tasks"."queue_name" IS NOT DISTINCT FROM excluded."queue_name"
//                 AND "solid_queue_recurring_tasks"."priority" IS NOT DISTINCT FROM excluded."priority"
//                 AND "solid_queue_recurring_tasks"."static" IS NOT DISTINCT FROM excluded."static"
//                 AND "solid_queue_recurring_tasks"."description" IS NOT DISTINCT FROM excluded."description"
//             ) THEN "solid_queue_recurring_tasks".updated_at
//             ELSE CURRENT_TIMESTAMP
//         END
//     ),
//     "schedule" = excluded."schedule",
//     "command" = excluded."command",
//     "class_name" = excluded."class_name",
//     "arguments" = excluded."arguments",
//     "queue_name" = excluded."queue_name",
//     "priority" = excluded."priority",
//     "static" = excluded."static",
//     "description" = excluded."description"
// RETURNING "id"
// ```
pub async fn upsert_task<C>(db: &C, entry: ScheduledEntry) -> Result<ExecResult, DbErr>
where
    C: ConnectionTrait,
{
    let sql = r#"INSERT INTO "solid_queue_recurring_tasks" (
        "key",
        "schedule",
        "command",
        "class_name",
        "arguments",
        "queue_name",
        "priority",
        "static",
        "description",
        "created_at",
        "updated_at"
        ) VALUES (
            $1,
            $2,
            $3,
            $4,
            $5,
            $6,
            $7,
            $8,
            $9,
            CURRENT_TIMESTAMP,
            CURRENT_TIMESTAMP
        ) ON CONFLICT ("key") DO UPDATE SET
            "updated_at" = (
                CASE
                    WHEN (
                        "solid_queue_recurring_tasks"."schedule" = excluded."schedule"
                        AND "solid_queue_recurring_tasks"."command" = excluded."command"
                        AND "solid_queue_recurring_tasks"."class_name" = excluded."class_name"
                        AND "solid_queue_recurring_tasks"."arguments" = excluded."arguments"
                        AND "solid_queue_recurring_tasks"."queue_name" = excluded."queue_name"
                        AND "solid_queue_recurring_tasks"."priority" = excluded."priority"
                        AND "solid_queue_recurring_tasks"."static" = excluded."static"
                        AND "solid_queue_recurring_tasks"."description" = excluded."description"
                    ) THEN "solid_queue_recurring_tasks"."updated_at"
                    ELSE CURRENT_TIMESTAMP
                END
            ),
            "schedule" = excluded."schedule",
            "command" = excluded."command",
            "class_name" = excluded."class_name",
            "arguments" = excluded."arguments",
            "queue_name" = excluded."queue_name",
            "priority" = excluded."priority",
            "static" = excluded."static",
            "description" = excluded."description"
        RETURNING "id""#;

    let cleaned_sql = sql.lines().map(str::trim).collect::<Vec<&str>>().join(" ");

    let ret = db
        .execute(Statement::from_sql_and_values(
            db.get_database_backend(),
            cleaned_sql,
            vec![
                Value::from(entry.key),
                Value::from(entry.schedule),
                Value::from(Some("")),
                Value::from(entry.class),
                Value::from(json!(entry.args)),
                Value::from(Some("")),
                Value::from(0),
                Value::from(true),
                Value::from(""),
            ],
        ))
        .await?;

    trace!("upsert_task: {:?}", ret);

    Ok(ret)
}

pub async fn enqueue_job<C>(
    db: &C, entry: ScheduledEntry, scheduled_at: NaiveDateTime,
) -> Result<bool, DbErr>
where
    C: ConnectionTrait,
{
    let queue_name = "default";
    let params = serde_json::json!({
        "job_class": entry.class,
        "job_id": entry.key,
        "provider_job_id": "",
        "queue_name": queue_name,
        "priority": 0,
        "arguments": entry.args,
        "executions": 0,
        "exception_executions": {},
        "locale": "en",
        "timezone": "UTC",
        "scheduled_at": scheduled_at,
        "enqueued_at": chrono::Utc::now().naive_utc(),
    });

    let job = solid_queue_jobs::ActiveModel {
        id: ActiveValue::NotSet,
        queue_name: ActiveValue::Set(queue_name.to_string()),
        class_name: ActiveValue::Set(entry.class),
        arguments: ActiveValue::Set(Some(params.to_string())),
        priority: ActiveValue::Set(0),
        failed_attempts: ActiveValue::Set(0),
        active_job_id: ActiveValue::Set(Some("".to_string())),
        scheduled_at: ActiveValue::Set(Some(scheduled_at)),
        finished_at: ActiveValue::Set(None),
        concurrency_key: ActiveValue::Set(Some("".to_string())),
        created_at: ActiveValue::Set(chrono::Utc::now().naive_utc()),
        updated_at: ActiveValue::Set(chrono::Utc::now().naive_utc()),
    }
    .save(db)
    .await?;

    let job = job.try_into_model()?;

    // let scheduled_execution = solid_queue_scheduled_executions::ActiveModel {
    //     id: ActiveValue::not_set(),
    //     job_id: ActiveValue::Set(job.id.unwrap()),
    //     queue_name: ActiveValue::Set(job.queue_name.clone().unwrap()),
    //     priority: ActiveValue::Set(job.priority.clone().unwrap()),
    //     scheduled_at: ActiveValue::Set(scheduled_at),
    //     created_at: ActiveValue::Set(chrono::Utc::now().naive_utc()),
    // }
    // .save(db)
    // .await?;

    let recurring_execution = solid_queue_recurring_executions::ActiveModel {
        id: ActiveValue::not_set(),
        job_id: ActiveValue::Set(job.id),
        task_key: ActiveValue::Set(entry.key.unwrap()),
        run_at: ActiveValue::Set(scheduled_at),
        created_at: ActiveValue::Set(chrono::Utc::now().naive_utc()),
    }
    .save(db)
    .await?;

    let ready_execution = solid_queue_ready_executions::ActiveModel {
        id: ActiveValue::not_set(),
        job_id: ActiveValue::Set(job.id),
        queue_name: ActiveValue::Set(job.queue_name),
        priority: ActiveValue::Set(job.priority),
        created_at: ActiveValue::Set(chrono::Utc::now().naive_utc()),
    }
    .save(db)
    .await?;

    Ok(true)
}

#[derive(Debug)]
pub struct Scheduler {
    pub ctx: Arc<AppContext>,
    pub schedule: Vec<HashMap<String, ScheduledEntry>>,
}

impl Scheduler {
    pub fn new(ctx: Arc<AppContext>) -> Self {
        Self { ctx, schedule: Vec::new() }
    }

    pub async fn run(&self) -> Result<(), anyhow::Error> {
        let db = self.ctx.get_db().await;
        let mut interval = tokio::time::interval(self.ctx.dispatcher_polling_interval);
        let mut heartbeat_interval = tokio::time::interval(self.ctx.process_heartbeat_interval);

        let delta = chrono::Duration::seconds(
            self.ctx.dispatcher_polling_interval.as_secs().try_into().unwrap(),
        );
        let mut scheduled = Vec::<ScheduledEntry>::new();
        let contents = std::fs::read_to_string("schedule.yml").unwrap();
        let schedule: Vec<HashMap<String, ScheduledEntry>> =
            serde_yaml::from_str(&contents).unwrap();
        debug!("Schedule: {:?}", schedule);

        // let kind = "Scheduler".to_string();
        // let name = "scheduler".to_string();
        // let process = self.on_start(&db, kind, name).await?;
        // info!(">> Process started: {:?}", process);

        for entry in schedule {
            for (key, mut value) in entry {
                value.key = Some(key.clone());
                scheduled.push(value.clone());

                let ret = db
                    .transaction::<_, ExecResult, DbErr>(|txn| {
                        Box::pin(async move { Ok(upsert_task(txn, value.clone()).await?) })
                    })
                    .await?;

                trace!("Upsert task: {:?}", ret);
            }
        }
        debug!("Scheduled: {:?}", scheduled);

        // let num_intervals = 3; // 假设我们想创建 3 个 interval
        let entries = scheduled.clone();
        // debug!("------------ scheduled entries: {:?}", entries);

        let mut task_handles = Vec::new();

        for (i, entry) in entries.into_iter().enumerate() {
            // let opts = self.ctx.connect_options.clone();
            // debug!("------------ entry: {:?}", entry);
            let db = self.ctx.get_db().await;
            let graceful_shutdown = self.ctx.graceful_shutdown.clone();

            let task_key = entry.key.clone().unwrap_or_else(|| format!("task_{}", i));
            let handle = tokio::spawn(async move {
                // let db = Database::connect(opts).await.unwrap();
                // let db = db.await;

                let cron = entry.as_cron();
                let cron = cron.unwrap();
                let time = chrono::Local::now();
                let mut last = cron.find_next_occurrence(&time, false).unwrap();
                debug!("Starting scheduled task: {}", task_key);

                loop {
                    if graceful_shutdown.is_cancelled() {
                        info!("Scheduler task for {} exiting due to shutdown signal", task_key);
                        break;
                    }

                    let now = chrono::Local::now();
                    let next = cron.find_next_occurrence(&now, false).unwrap();
                    let scheduled_at = next.naive_utc();
                    let delta = next - now;

                    if next == last {
                        continue;
                    } else {
                        last = next;
                    }

                    debug!("next: {:?}", next);

                    // 如果 delta 为零，设置一个最小的睡眠时间
                    let sleep_duration = if delta.num_seconds() <= 0 {
                        tokio::time::Duration::from_secs(2)
                    } else {
                        tokio::time::Duration::from_millis(delta.num_milliseconds() as u64)
                    };

                    debug!("Job({:?}) next tick: {:?}", &task_key, next);

                    tokio::select! {
                        _ = tokio::time::sleep(sleep_duration) => {
                        }
                        _ = graceful_shutdown.cancelled() => {
                            info!("Scheduler task for {} cancelled during sleep", task_key);
                            return;
                        }
                    }

                    if graceful_shutdown.is_cancelled() {
                        info!("Scheduler task for {} exiting before transaction", task_key);
                        break;
                    }

                    let start_time = Instant::now();
                    let _ = db
                        .transaction::<_, (), DbErr>(|txn| {
                            let entry = entry.clone();
                            let task_key = task_key.clone();
                            Box::pin(async move {
                                let ret = enqueue_job(txn, entry, scheduled_at).await;
                                debug!("Job({:?}) enqueue_job: {:?}", task_key, ret);
                                Ok(())
                            })
                        })
                        .await;

                    let duration = start_time.elapsed();
                    debug!("Interval({:?}) {} ticked!: {:?}", &task_key, i, duration);
                }

                info!("Scheduler task for {} completed", task_key);
            });

            task_handles.push(handle);
        }

        info!("Started {} scheduled tasks", task_handles.len());

        let graceful_shutdown = self.ctx.graceful_shutdown.clone();
        loop {
            tokio::select! {
              _ = heartbeat_interval.tick() => {
                // self.heartbeat(&db, &process).await?;
                debug!("heartbeat_interval.tick()");
              }
              _ = graceful_shutdown.cancelled() => {
                debug!("scheduler stopping - waiting for {} tasks to complete", task_handles.len());

                let shutdown_timeout = tokio::time::Duration::from_secs(5);
                match tokio::time::timeout(shutdown_timeout, futures::future::join_all(task_handles)).await {
                    Ok(_) => info!("All scheduler tasks completed gracefully"),
                    Err(_) => warn!("Some scheduler tasks did not complete within timeout"),
                }

                debug!("Scheduler stopped");
                return Ok(());
              }
              // _ = tokio::signal::ctrl_c() => {
              //   info!("ctrl-c received");
              //   return Ok(());
              // }
              _ = interval.tick() => {
                // let pool = db.get_postgres_connection_pool();
                // debug!("----- scheduler Max connections: {}, Idle connections: {}", pool.size(), pool.num_idle());
                debug!("interval.tick()");
              }
            }
        }
    }
}

#[async_trait]
impl ProcessTrait for Scheduler {}
