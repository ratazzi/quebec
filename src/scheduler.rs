use crate::context::{AppContext, ScheduledEntry};
use crate::entities::*;
use crate::notify::NotifyManager;
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

use tracing::{error, info, trace, warn};

// ```sql
// INSERT INTO "quebec_recurring_tasks" (
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
//                 "quebec_recurring_tasks"."schedule" IS NOT DISTINCT FROM excluded."schedule"
//                 AND "quebec_recurring_tasks"."command" IS NOT DISTINCT FROM excluded."command"
//                 AND "quebec_recurring_tasks"."class_name" IS NOT DISTINCT FROM excluded."class_name"
//                 AND "quebec_recurring_tasks"."arguments" IS NOT DISTINCT FROM excluded."arguments"
//                 AND "quebec_recurring_tasks"."queue_name" IS NOT DISTINCT FROM excluded."queue_name"
//                 AND "quebec_recurring_tasks"."priority" IS NOT DISTINCT FROM excluded."priority"
//                 AND "quebec_recurring_tasks"."static" IS NOT DISTINCT FROM excluded."static"
//                 AND "quebec_recurring_tasks"."description" IS NOT DISTINCT FROM excluded."description"
//             ) THEN "quebec_recurring_tasks".updated_at
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
pub async fn upsert_task<C>(db: &C, table_config: &crate::context::TableConfig, entry: ScheduledEntry) -> Result<ExecResult, DbErr>
where
    C: ConnectionTrait,
{
    let sql = format!(r#"INSERT INTO "{}" (
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
                        "{}"."schedule" = excluded."schedule"
                        AND "{}"."command" = excluded."command"
                        AND "{}"."class_name" = excluded."class_name"
                        AND "{}"."arguments" = excluded."arguments"
                        AND "{}"."queue_name" = excluded."queue_name"
                        AND "{}"."priority" = excluded."priority"
                        AND "{}"."static" = excluded."static"
                        AND "{}"."description" = excluded."description"
                    ) THEN "{}"."updated_at"
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
        RETURNING "id""#,
        table_config.recurring_tasks,
        table_config.recurring_tasks, table_config.recurring_tasks, table_config.recurring_tasks,
        table_config.recurring_tasks, table_config.recurring_tasks, table_config.recurring_tasks,
        table_config.recurring_tasks, table_config.recurring_tasks, table_config.recurring_tasks);

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
                Value::from(entry.queue.or(Some("".to_string()))),
                Value::from(entry.priority.unwrap_or(0)),
                Value::from(true),
                Value::from(""),
            ],
        ))
        .await?;

    trace!("upsert_task: {:?}", ret);

    Ok(ret)
}

pub async fn enqueue_job<C>(
    ctx: &Arc<AppContext>, db: &C, entry: ScheduledEntry, scheduled_at: NaiveDateTime,
) -> Result<bool, DbErr>
where
    C: ConnectionTrait,
{
    let queue_name = entry.queue.as_deref().unwrap_or("default");
    let priority = entry.priority.unwrap_or(0);

    let params = serde_json::json!({
        "job_class": entry.class,
        "job_id": entry.key,
        "provider_job_id": "",
        "queue_name": queue_name,
        "priority": priority,
        "arguments": entry.args,
        "executions": 0,
        "exception_executions": {},
        "locale": "en",
        "timezone": "UTC",
        "scheduled_at": scheduled_at,
        "enqueued_at": chrono::Utc::now().naive_utc(),
    });

    // Get concurrency constraint using runnable
    let concurrency_constraint = if ctx.has_concurrency_control(&entry.class.to_string()) {
        // Only if concurrency control is enabled, get the runnable and compute constraint

        if let Ok(runnable) = ctx.get_runnable(&entry.class) {
            if let Some(args) = &entry.args {
                // Assume args is a list of arguments, no kwargs for scheduled jobs
                runnable.get_concurrency_constraint(Some(args), None::<&serde_yaml::Value>).unwrap_or(None)
            } else {
                runnable.get_concurrency_constraint(None::<&serde_yaml::Value>, None::<&serde_yaml::Value>).unwrap_or(None)
            }
        } else {
            None
        }
    } else {
        None
    };

    let job = quebec_jobs::ActiveModel {
        id: ActiveValue::NotSet,
        queue_name: ActiveValue::Set(queue_name.to_string()),
        class_name: ActiveValue::Set(entry.class),
        arguments: ActiveValue::Set(Some(params.to_string())),
        priority: ActiveValue::Set(priority),
        failed_attempts: ActiveValue::Set(0),
        active_job_id: ActiveValue::Set(Some("".to_string())),
        scheduled_at: ActiveValue::Set(Some(scheduled_at)),
        finished_at: ActiveValue::Set(None),
        concurrency_key: ActiveValue::Set(concurrency_constraint.as_ref().map(|c| c.key.clone())),
        created_at: ActiveValue::Set(chrono::Utc::now().naive_utc()),
        updated_at: ActiveValue::Set(chrono::Utc::now().naive_utc()),
    }
    .save(db)
    .await?;

    let job = job.try_into_model()?;

    let task_key = entry.key.ok_or_else(|| DbErr::Custom("Task key is missing".to_string()))?;
    let _recurring_execution = quebec_recurring_executions::ActiveModel {
        id: ActiveValue::not_set(),
        job_id: ActiveValue::Set(job.id),
        task_key: ActiveValue::Set(task_key),
        run_at: ActiveValue::Set(scheduled_at),
        created_at: ActiveValue::Set(chrono::Utc::now().naive_utc()),
    }
    .save(db)
    .await?;

    // Apply concurrency control logic
    if let Some(constraint) = &concurrency_constraint {
        use crate::semaphore::acquire_semaphore_with_constraint;

        // Try to acquire the semaphore using the constraint
        if acquire_semaphore_with_constraint(db, &ctx.table_config, constraint).await? {
            info!("Scheduler: Semaphore acquired for key: {}", constraint.key);

            // Create ready execution - job can run immediately
            let _ready_execution = quebec_ready_executions::ActiveModel {
                id: ActiveValue::not_set(),
                job_id: ActiveValue::Set(job.id),
                queue_name: ActiveValue::Set(job.queue_name.clone()),
                priority: ActiveValue::Set(job.priority),
                created_at: ActiveValue::Set(chrono::Utc::now().naive_utc()),
            }
            .save(db)
            .await?;
        } else {
            warn!("Scheduler: Failed to acquire semaphore for key: {}", constraint.key);

            // Create blocked execution - job must wait
            let now = chrono::Utc::now().naive_utc();
            let duration = ctx.default_concurrency_control_period;
            let expires_at = now + duration;

            let _blocked_execution = quebec_blocked_executions::ActiveModel {
                id: ActiveValue::NotSet,
                queue_name: ActiveValue::Set(job.queue_name.clone()),
                job_id: ActiveValue::Set(job.id),
                priority: ActiveValue::Set(job.priority),
                concurrency_key: ActiveValue::Set(constraint.key.clone()),
                expires_at: ActiveValue::Set(expires_at),
                created_at: ActiveValue::Set(chrono::Utc::now().naive_utc()),
            }
            .save(db)
            .await?;
        }
    } else {
        // No concurrency control - create ready execution immediately
        let _ready_execution = quebec_ready_executions::ActiveModel {
            id: ActiveValue::not_set(),
            job_id: ActiveValue::Set(job.id),
            queue_name: ActiveValue::Set(job.queue_name.clone()),
            priority: ActiveValue::Set(job.priority),
            created_at: ActiveValue::Set(chrono::Utc::now().naive_utc()),
        }
        .save(db)
        .await?;
    }

    // Send PostgreSQL NOTIFY after scheduled job is created
    if ctx.is_postgres() {
        if let Err(e) = NotifyManager::send_notify(&ctx.name, db, &job.queue_name, "new_job").await {
            warn!("Failed to send NOTIFY for scheduled job: {}", e);
        }
    }

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

    fn parse_schedule_file(contents: &str) -> Result<Vec<HashMap<String, ScheduledEntry>>, anyhow::Error> {
        // Parse as multi-environment config (Solid Queue format)
        // Format: { development: { task1: {...}, task2: {...} }, production: {...} }
        let env_config = serde_yaml::from_str::<HashMap<String, HashMap<String, ScheduledEntry>>>(contents)
            .map_err(|e| {
                error!("Failed to parse schedule file: {}", e);
                anyhow::anyhow!("Failed to parse schedule file: {}", e)
            })?;

        // Use shared environment config parser
        let tasks = crate::utils::parse_env_config_cloneable(env_config)?;
        info!("Loaded {} scheduled tasks", tasks.len());
        Ok(vec![tasks])
    }

    pub async fn run(&self) -> Result<(), anyhow::Error> {
        let db = self.ctx.get_db().await;
        let mut interval = tokio::time::interval(self.ctx.dispatcher_polling_interval);
        let mut heartbeat_interval = tokio::time::interval(self.ctx.process_heartbeat_interval);

        let _delta = chrono::Duration::seconds(
            self.ctx.dispatcher_polling_interval.as_secs().try_into().unwrap_or(1),
        );
        let mut scheduled = Vec::<ScheduledEntry>::new();

        // Find schedule file with priority:
        // 1. SOLID_QUEUE_RECURRING_SCHEDULE env var (for Solid Queue compatibility)
        // 2. QUEBEC_RECURRING_SCHEDULE env var
        // 3. recurring.yml (current directory)
        // 4. config/recurring.yml (Solid Queue compatible)
        let schedule_path = std::env::var("SOLID_QUEUE_RECURRING_SCHEDULE")
            .or_else(|_| std::env::var("QUEBEC_RECURRING_SCHEDULE"))
            .ok()
            .or_else(|| {
                if std::path::Path::new("recurring.yml").exists() {
                    Some("recurring.yml".to_string())
                } else if std::path::Path::new("config/recurring.yml").exists() {
                    Some("config/recurring.yml".to_string())
                } else {
                    None
                }
            });

        let schedule: Vec<HashMap<String, ScheduledEntry>> = match schedule_path {
            None => {
                info!("No schedule file found, running without scheduled tasks");
                Vec::new()
            }
            Some(path) => {
                info!("Loading schedule from: {}", path);
                let contents = std::fs::read_to_string(&path)
                    .map_err(|e| {
                        error!("Failed to read schedule file {}: {}", path, e);
                        anyhow::anyhow!("Failed to read schedule file: {}", e)
                    })?;

                Self::parse_schedule_file(&contents)?
            }
        };

        trace!("Schedule: {:?}", schedule);

        let kind = "Scheduler".to_string();
        let name = "scheduler".to_string();
        let process = self.on_start(&db, kind, name).await?;
        info!(">> Process started: {:?}", process);

        for entry in schedule {
            for (key, mut value) in entry {
                value.key = Some(key.clone());
                scheduled.push(value.clone());

                let table_config = self.ctx.table_config.clone();
                let ret = db
                    .transaction::<_, ExecResult, DbErr>(|txn| {
                        Box::pin(async move { upsert_task(txn, &table_config, value.clone()).await })
                    })
                    .await?;

                trace!("Upsert task: {:?}", ret);
            }
        }
        trace!("Scheduled: {:?}", scheduled);

        let entries = scheduled.clone();
        let mut task_handles = Vec::new();

        for (i, entry) in entries.into_iter().enumerate() {
            let db = self.ctx.get_db().await;
            let graceful_shutdown = self.ctx.graceful_shutdown.clone();
            let ctx = self.ctx.clone(); // Clone context for the async move block

            let task_key = entry.key.clone().unwrap_or_else(|| format!("task_{}", i));
            let handle = tokio::spawn(async move {


                let cron = match entry.as_cron() {
                    Ok(c) => c,
                    Err(e) => {
                        error!("Failed to parse cron expression for task {}: {}", task_key, e);
                        return;
                    }
                };
                let time = chrono::Local::now();
                let mut last = match cron.find_next_occurrence(&time, false) {
                    Ok(occurrence) => occurrence,
                    Err(e) => {
                        error!("Failed to find next occurrence for task {}: {}", task_key, e);
                        return;
                    }
                };
                info!("Starting scheduled task: {}", task_key);

                loop {
                    if graceful_shutdown.is_cancelled() {
                        info!("Scheduler task for {} exiting due to shutdown signal", task_key);
                        break;
                    }

                    // Capture both time domains: wall clock for cron and monotonic for sleeping
                    let now_monotonic = tokio::time::Instant::now();
                    let now_wall = chrono::Local::now();

                    let next_wall = match cron.find_next_occurrence(&now_wall, false) {
                        Ok(n) => n,
                        Err(e) => {
                            warn!("No next occurrence found for task {}: {}", task_key, e);
                            break;
                        }
                    };
                    let scheduled_at = next_wall.naive_utc();

                    if next_wall == last {
                        continue;
                    } else {
                        last = next_wall;
                    }

                    trace!("next_wall: {:?}", next_wall);

                    // Compute a fixed deadline on the monotonic clock to avoid drift
                    let delay = (next_wall - now_wall).to_std().unwrap_or_else(|_| {
                        warn!(
                            r"Could not convert negative duration to std::time::Duration for task {}. \
                             This can happen due to system time changes (e.g., NTP sync, DST). \
                             Falling back to a 2-second delay.",
                            task_key
                        );
                        std::time::Duration::from_secs(2)
                    });
                    let deadline = now_monotonic + delay;

                    trace!("Job({:?}) next tick at: {:?}", &task_key, next_wall);

                    tokio::select! {
                        _ = tokio::time::sleep_until(deadline) => { }
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
                            let ctx = ctx.clone(); // Clone for each transaction
                            Box::pin(async move {
                                let _ret = enqueue_job(&ctx, txn, entry, scheduled_at).await;
                                trace!("Job({:?}) enqueued", task_key);
                                Ok(())
                            })
                        })
                        .await;

                    let duration = start_time.elapsed();
                    trace!("Interval({:?}) {} ticked: {:?}", &task_key, i, duration);
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
                    self.heartbeat(&db, &process).await?;
                    trace!("Scheduler heartbeat");
                }
                _ = graceful_shutdown.cancelled() => {
                    info!("Scheduler stopping - waiting for {} tasks to complete", task_handles.len());

                    self.on_stop(&db, &process).await?;

                    let shutdown_timeout = tokio::time::Duration::from_secs(5);
                    match tokio::time::timeout(shutdown_timeout, futures::future::join_all(task_handles)).await {
                        Ok(_) => info!("All scheduler tasks completed gracefully"),
                        Err(_) => warn!("Some scheduler tasks did not complete within timeout"),
                    }

                    info!("Scheduler stopped");
                    return Ok(());
                }
                _ = interval.tick() => {
                    trace!("Scheduler interval tick");
                }
            }
        }
    }
}

#[async_trait]
impl ProcessTrait for Scheduler {}
