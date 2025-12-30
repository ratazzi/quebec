use crate::context::{AppContext, ScheduledEntry};
use crate::notify::NotifyManager;
use crate::process::{ProcessInfo, ProcessTrait};
use crate::query_builder;
use anyhow::Result;
use async_trait::async_trait;
use chrono::NaiveDateTime;
use sea_orm::{ConnectionTrait, DbBackend, DbErr, ExecResult, Statement, TransactionTrait, Value};
use serde_json::json;
use serde_yaml;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use tracing::{error, info, trace, warn};

/// Upsert a recurring task into the database.
/// Supports PostgreSQL, SQLite, and MySQL with database-specific syntax.
/// Uses NULL-safe comparison (IS NOT DISTINCT FROM / <=> / IS) to correctly handle nullable columns.
pub async fn upsert_task<C>(
    db: &C,
    table_config: &crate::context::TableConfig,
    entry: ScheduledEntry,
) -> Result<ExecResult, DbErr>
where
    C: ConnectionTrait,
{
    let backend = db.get_database_backend();
    let table = &table_config.recurring_tasks;

    // Prepare values
    // Use NULL for optional fields (command, description) - scheduled tasks use class, not command
    let values = vec![
        Value::from(entry.key.clone()),
        Value::from(entry.schedule.clone()),
        Value::from(None::<String>), // command - NULL for scheduled tasks
        Value::from(entry.class.clone()),
        // Use empty array if args is None, to avoid "arguments is not an array" error
        Value::from(json!(entry.args.as_ref().unwrap_or(&vec![]))),
        Value::from(entry.queue.clone()), // queue - NULL if not specified
        Value::from(entry.priority.unwrap_or(0)),
        Value::from(true),
        Value::from(None::<String>), // description - NULL
    ];

    let sql = match backend {
        DbBackend::Postgres => {
            // PostgreSQL: ON CONFLICT DO UPDATE with IS NOT DISTINCT FROM for NULL-safe comparison
            format!(
                r#"INSERT INTO "{t}" (
                    "key", "schedule", "command", "class_name", "arguments",
                    "queue_name", "priority", "static", "description",
                    "created_at", "updated_at"
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                ON CONFLICT ("key") DO UPDATE SET
                    "updated_at" = CASE
                        WHEN "{t}"."schedule" IS NOT DISTINCT FROM excluded."schedule"
                            AND "{t}"."command" IS NOT DISTINCT FROM excluded."command"
                            AND "{t}"."class_name" IS NOT DISTINCT FROM excluded."class_name"
                            AND "{t}"."arguments" IS NOT DISTINCT FROM excluded."arguments"
                            AND "{t}"."queue_name" IS NOT DISTINCT FROM excluded."queue_name"
                            AND "{t}"."priority" IS NOT DISTINCT FROM excluded."priority"
                            AND "{t}"."static" IS NOT DISTINCT FROM excluded."static"
                            AND "{t}"."description" IS NOT DISTINCT FROM excluded."description"
                        THEN "{t}"."updated_at"
                        ELSE CURRENT_TIMESTAMP
                    END,
                    "schedule" = excluded."schedule",
                    "command" = excluded."command",
                    "class_name" = excluded."class_name",
                    "arguments" = excluded."arguments",
                    "queue_name" = excluded."queue_name",
                    "priority" = excluded."priority",
                    "static" = excluded."static",
                    "description" = excluded."description""#,
                t = table
            )
        }
        DbBackend::Sqlite => {
            // SQLite: ON CONFLICT DO UPDATE with IS for NULL-safe comparison
            format!(
                r#"INSERT INTO "{t}" (
                    "key", "schedule", "command", "class_name", "arguments",
                    "queue_name", "priority", "static", "description",
                    "created_at", "updated_at"
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                ON CONFLICT ("key") DO UPDATE SET
                    "updated_at" = CASE
                        WHEN "{t}"."schedule" IS excluded."schedule"
                            AND "{t}"."command" IS excluded."command"
                            AND "{t}"."class_name" IS excluded."class_name"
                            AND "{t}"."arguments" IS excluded."arguments"
                            AND "{t}"."queue_name" IS excluded."queue_name"
                            AND "{t}"."priority" IS excluded."priority"
                            AND "{t}"."static" IS excluded."static"
                            AND "{t}"."description" IS excluded."description"
                        THEN "{t}"."updated_at"
                        ELSE CURRENT_TIMESTAMP
                    END,
                    "schedule" = excluded."schedule",
                    "command" = excluded."command",
                    "class_name" = excluded."class_name",
                    "arguments" = excluded."arguments",
                    "queue_name" = excluded."queue_name",
                    "priority" = excluded."priority",
                    "static" = excluded."static",
                    "description" = excluded."description""#,
                t = table
            )
        }
        DbBackend::MySql => {
            // MySQL 8.0.20+: Use alias syntax instead of deprecated VALUES()
            // https://dev.mysql.com/doc/refman/8.0/en/insert-on-duplicate.html
            format!(
                r#"INSERT INTO `{t}` (
                    `key`, `schedule`, `command`, `class_name`, `arguments`,
                    `queue_name`, `priority`, `static`, `description`,
                    `created_at`, `updated_at`
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                AS new_vals
                ON DUPLICATE KEY UPDATE
                    `updated_at` = CASE
                        WHEN `schedule` <=> new_vals.`schedule`
                            AND `command` <=> new_vals.`command`
                            AND `class_name` <=> new_vals.`class_name`
                            AND `arguments` <=> new_vals.`arguments`
                            AND `queue_name` <=> new_vals.`queue_name`
                            AND `priority` <=> new_vals.`priority`
                            AND `static` <=> new_vals.`static`
                            AND `description` <=> new_vals.`description`
                        THEN `updated_at`
                        ELSE CURRENT_TIMESTAMP
                    END,
                    `schedule` = new_vals.`schedule`,
                    `command` = new_vals.`command`,
                    `class_name` = new_vals.`class_name`,
                    `arguments` = new_vals.`arguments`,
                    `queue_name` = new_vals.`queue_name`,
                    `priority` = new_vals.`priority`,
                    `static` = new_vals.`static`,
                    `description` = new_vals.`description`"#,
                t = table
            )
        }
    };

    let cleaned_sql = sql.lines().map(str::trim).collect::<Vec<&str>>().join(" ");

    let ret = db
        .execute(Statement::from_sql_and_values(backend, cleaned_sql, values))
        .await?;

    trace!("upsert_task: {:?}", ret);

    Ok(ret)
}

/// Enqueue a job for execution. Returns Some(queue_name) for NOTIFY, or None if skipped.
/// Uses optimistic locking to handle concurrent schedulers:
/// 1. Create the job first
/// 2. Try to insert recurring_execution with ON CONFLICT DO NOTHING
/// 3. If already exists (another scheduler won), delete the job and return None
/// IMPORTANT: Caller should send NOTIFY after transaction commits, not inside.
pub async fn enqueue_job<C>(
    ctx: &Arc<AppContext>,
    db: &C,
    entry: ScheduledEntry,
    scheduled_at: NaiveDateTime,
) -> Result<Option<String>, DbErr>
where
    C: ConnectionTrait,
{
    let task_key = entry
        .key
        .clone()
        .ok_or_else(|| DbErr::Custom("Task key is missing".to_string()))?;

    // Step 1: Create the job first
    let queue_name = entry.queue.as_deref().unwrap_or("default");
    let priority = entry.priority.unwrap_or(0);

    let now = chrono::Utc::now().naive_utc();
    // Use empty array if args is None, to avoid "arguments is not an array" error
    let args = entry.args.as_ref().cloned().unwrap_or_default();
    let params = serde_json::json!({
        "job_class": entry.class,
        "job_id": entry.key,
        "provider_job_id": "",
        "queue_name": queue_name,
        "priority": priority,
        "arguments": args,
        "executions": 0,
        "exception_executions": {},
        "locale": "en",
        "timezone": "UTC",
        "scheduled_at": scheduled_at,
        "enqueued_at": now,
    });

    // Get concurrency constraint using runnable
    // Use normalized args (consistent with what's stored in the job)
    let concurrency_constraint = ctx
        .has_concurrency_control(&entry.class.to_string())
        .then(|| ctx.get_runnable(&entry.class).ok())
        .flatten()
        .and_then(|runnable| {
            let args_ref = if args.is_empty() { None } else { Some(&args) };
            runnable
                .get_concurrency_constraint(args_ref, None::<&serde_yaml::Value>)
                .unwrap_or(None)
        });

    let concurrency_key_str = concurrency_constraint.as_ref().map(|c| c.key.as_str());
    let active_job_id = crate::utils::generate_job_id();
    let job_id = query_builder::jobs::insert(
        db,
        &ctx.table_config,
        queue_name,
        &entry.class,
        Some(params.to_string()).as_deref(),
        priority,
        Some(active_job_id.as_str()),
        Some(scheduled_at),
        concurrency_key_str,
    )
    .await?;

    let job = query_builder::jobs::find_by_id(db, &ctx.table_config, job_id)
        .await?
        .ok_or_else(|| DbErr::Custom("Failed to find inserted job".to_string()))?;

    // Step 2: Try to claim this execution slot using optimistic locking
    // If another scheduler already claimed it, we delete the job and skip
    let claimed = query_builder::recurring_executions::try_insert(
        db,
        &ctx.table_config,
        job.id,
        &task_key,
        scheduled_at,
    )
    .await?;

    if !claimed {
        // Another scheduler instance already claimed this execution
        // Delete the job we just created and return None
        query_builder::jobs::delete_by_id(db, &ctx.table_config, job.id).await?;
        trace!(
            "Skipping job {} at {} - already claimed by another scheduler",
            task_key,
            scheduled_at
        );
        return Ok(None);
    }

    // Apply concurrency control logic
    // Returns Some(constraint) if blocked, None if ready to execute
    let blocked_by = if let Some(constraint) = &concurrency_constraint {
        use crate::semaphore::acquire_semaphore_with_constraint;

        if acquire_semaphore_with_constraint(db, &ctx.table_config, constraint).await? {
            info!("Scheduler: Semaphore acquired for key: {}", constraint.key);
            None
        } else {
            warn!(
                "Scheduler: Failed to acquire semaphore for key: {}",
                constraint.key
            );
            Some(constraint)
        }
    } else {
        None
    };

    if let Some(constraint) = blocked_by {
        // Create blocked execution - job must wait
        let block_now = chrono::Utc::now().naive_utc();
        let duration = constraint.duration.unwrap_or_else(|| {
            chrono::Duration::from_std(ctx.default_concurrency_control_period)
                .unwrap_or_else(|_| chrono::Duration::seconds(60))
        });
        let expires_at = block_now + duration;

        query_builder::blocked_executions::insert(
            db,
            &ctx.table_config,
            job.id,
            &job.queue_name,
            job.priority,
            &constraint.key,
            expires_at,
        )
        .await?;
    } else {
        // Job is ready to execute
        query_builder::ready_executions::insert(
            db,
            &ctx.table_config,
            job.id,
            &job.queue_name,
            job.priority,
        )
        .await?;
    }

    // Return queue name so caller can send NOTIFY after transaction commits
    Ok(Some(job.queue_name))
}

#[derive(Debug)]
pub struct Scheduler {
    pub ctx: Arc<AppContext>,
    pub schedule: Vec<HashMap<String, ScheduledEntry>>,
}

impl Scheduler {
    pub fn new(ctx: Arc<AppContext>) -> Self {
        Self {
            ctx,
            schedule: Vec::new(),
        }
    }

    /// Find schedule file with priority:
    /// 1. SOLID_QUEUE_RECURRING_SCHEDULE env var (for Solid Queue compatibility)
    /// 2. QUEBEC_RECURRING_SCHEDULE env var
    /// 3. recurring.yml (current directory)
    /// 4. config/recurring.yml (Solid Queue compatible)
    fn find_schedule_path() -> Option<String> {
        std::env::var("SOLID_QUEUE_RECURRING_SCHEDULE")
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
            })
    }

    fn parse_schedule_file(
        contents: &str,
    ) -> Result<Vec<HashMap<String, ScheduledEntry>>, anyhow::Error> {
        // Parse as multi-environment config (Solid Queue format)
        // Format: { development: { task1: {...}, task2: {...} }, production: {...} }
        let env_config =
            serde_yaml::from_str::<HashMap<String, HashMap<String, ScheduledEntry>>>(contents)
                .map_err(|e| {
                    error!("Failed to parse schedule file: {}", e);
                    anyhow::anyhow!("Failed to parse schedule file: {}", e)
                })?;

        // Use shared environment config parser
        let tasks = crate::utils::parse_env_config_cloneable(env_config)?;
        info!("Loaded {} scheduled tasks", tasks.len());
        Ok(vec![tasks])
    }

    /// Load schedule from file path, returns empty vec if no path provided
    fn load_schedule(
        path: Option<String>,
    ) -> Result<Vec<HashMap<String, ScheduledEntry>>, anyhow::Error> {
        let Some(path) = path else {
            info!("No schedule file found, running without scheduled tasks");
            return Ok(Vec::new());
        };

        info!("Loading schedule from: {}", path);
        let contents = std::fs::read_to_string(&path).map_err(|e| {
            error!("Failed to read schedule file {}: {}", path, e);
            anyhow::anyhow!("Failed to read schedule file: {}", e)
        })?;

        Self::parse_schedule_file(&contents)
    }

    /// Sync scheduled tasks to database (upsert recurring_tasks table)
    async fn sync_tasks_to_db<C: ConnectionTrait + TransactionTrait>(
        db: &C,
        table_config: &crate::context::TableConfig,
        schedule: Vec<HashMap<String, ScheduledEntry>>,
    ) -> Result<Vec<ScheduledEntry>, anyhow::Error> {
        let mut scheduled = Vec::new();

        for entry in schedule {
            for (key, mut value) in entry {
                value.key = Some(key);

                let ret = db
                    .transaction::<_, ExecResult, DbErr>(|txn| {
                        let tc = table_config.clone();
                        let v = value.clone();
                        Box::pin(async move { upsert_task(txn, &tc, v).await })
                    })
                    .await?;

                trace!("Upsert task: {:?}", ret);
                scheduled.push(value);
            }
        }

        trace!("Scheduled: {:?}", scheduled);
        Ok(scheduled)
    }

    /// Run the cron loop for a single scheduled task
    async fn run_task_loop(
        ctx: Arc<AppContext>,
        db: Arc<sea_orm::DatabaseConnection>,
        entry: ScheduledEntry,
        task_key: String,
        graceful_shutdown: tokio_util::sync::CancellationToken,
    ) {
        let cron = match entry.as_cron() {
            Ok(c) => c,
            Err(e) => {
                error!(
                    "Failed to parse cron expression for task {}: {}",
                    task_key, e
                );
                return;
            }
        };

        info!("Starting scheduled task: {}", task_key);

        // Track consecutive time errors for exponential backoff
        let mut time_error_count: u32 = 0;
        let mut last_planned: Option<chrono::DateTime<chrono::Local>> = None;

        loop {
            if graceful_shutdown.is_cancelled() {
                info!(
                    "Scheduler task for {} exiting due to shutdown signal",
                    task_key
                );
                break;
            }

            // Capture both time domains: wall clock for cron and monotonic for sleeping
            let now_monotonic = tokio::time::Instant::now();
            let now_wall = chrono::Local::now();

            // Ensure we don't repeatedly return the same occurrence (can happen around time jumps).
            // Search from a time strictly after the last planned occurrence to avoid duplicate enqueues.
            let mut search_from = now_wall;
            if let Some(last) = last_planned {
                if last >= search_from {
                    search_from = last + chrono::Duration::milliseconds(1);
                }
            }

            let next_wall = match cron.find_next_occurrence(&search_from, false) {
                Ok(n) => n,
                Err(e) => {
                    warn!("No next occurrence found for task {}: {}", task_key, e);
                    break;
                }
            };
            last_planned = Some(next_wall);

            trace!("next_wall: {:?}", next_wall);

            // Compute a fixed deadline on the monotonic clock to avoid drift
            let delay = match (next_wall - now_wall).to_std() {
                Ok(d) => {
                    // Reset error count on successful time calculation
                    time_error_count = 0;

                    // Detect large time jumps (> 24 hours) and log warning
                    if d.as_secs() > 86400 {
                        warn!(
                            "Large time jump detected for task {}: next execution in {:.1} hours. \
                             This may indicate a system time issue or very sparse schedule.",
                            task_key,
                            d.as_secs() as f64 / 3600.0
                        );
                    }
                    d
                }
                Err(_) => {
                    // Exponential backoff: 2, 4, 8, 16, 32, 60, 60, 60... seconds
                    time_error_count = time_error_count.saturating_add(1);
                    let backoff_secs = std::cmp::min(2u64.saturating_pow(time_error_count), 60);
                    let backoff = std::time::Duration::from_secs(backoff_secs);
                    warn!(
                        "Could not convert negative duration for task {} (attempt {}). \
                         This can happen due to system time changes (e.g., NTP sync, DST). \
                         Using exponential backoff: {} seconds and retrying time calculation.",
                        task_key, time_error_count, backoff_secs
                    );

                    tokio::select! {
                        _ = tokio::time::sleep(backoff) => { }
                        _ = graceful_shutdown.cancelled() => {
                            info!("Scheduler task for {} cancelled during backoff", task_key);
                            return;
                        }
                    }
                    continue;
                }
            };
            let scheduled_at = next_wall.naive_utc();
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
            let result = db
                .transaction::<_, Option<String>, DbErr>(|txn| {
                    let entry = entry.clone();
                    let task_key = task_key.clone();
                    let ctx = ctx.clone();
                    Box::pin(async move {
                        let queue_name = enqueue_job(&ctx, txn, entry, scheduled_at).await?;
                        if queue_name.is_some() {
                            trace!("Job({:?}) enqueued", task_key);
                        }
                        Ok(queue_name)
                    })
                })
                .await
                .inspect_err(|e| error!("Failed to enqueue scheduled job {}: {}", task_key, e));

            // Send NOTIFY after transaction commits (only if job was actually created)
            if let Ok(Some(queue_name)) = result {
                if ctx.is_postgres() {
                    NotifyManager::send_notify(&ctx.name, db.as_ref(), &queue_name, "new_job")
                        .await
                        .inspect_err(|e| warn!("Failed to send NOTIFY: {}", e))
                        .ok();
                }
            }

            trace!("Task({:?}) ticked: {:?}", &task_key, start_time.elapsed());
        }

        info!("Scheduler task for {} completed", task_key);
    }

    /// Main loop: heartbeat + graceful shutdown handling
    async fn run_main_loop(
        &self,
        db: &sea_orm::DatabaseConnection,
        process: &crate::entities::quebec_processes::Model,
        mut heartbeat_interval: tokio::time::Interval,
        mut interval: tokio::time::Interval,
        task_handles: Vec<tokio::task::JoinHandle<()>>,
    ) -> Result<(), anyhow::Error> {
        let graceful_shutdown = self.ctx.graceful_shutdown.clone();

        loop {
            tokio::select! {
                _ = heartbeat_interval.tick() => {
                    self.heartbeat(db, process).await?;
                    trace!("Scheduler heartbeat");
                }
                _ = graceful_shutdown.cancelled() => {
                    info!("Scheduler stopping - waiting for {} tasks to complete", task_handles.len());

                    self.on_stop(db, process).await?;

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

    pub async fn run(&self) -> Result<(), anyhow::Error> {
        let db = self.ctx.get_db().await;
        let interval = tokio::time::interval(self.ctx.dispatcher_polling_interval);
        let heartbeat_interval = tokio::time::interval(self.ctx.process_heartbeat_interval);

        let _delta = chrono::Duration::seconds(
            self.ctx
                .dispatcher_polling_interval
                .as_secs()
                .try_into()
                .unwrap_or(1),
        );

        let schedule = Self::load_schedule(Self::find_schedule_path())?;
        trace!("Schedule: {:?}", schedule);

        let process = self.on_start(&db).await?;
        info!(">> Process started: {:?}", process);

        let scheduled = Self::sync_tasks_to_db(&*db, &self.ctx.table_config, schedule).await?;

        let mut task_handles = Vec::new();

        for (i, entry) in scheduled.into_iter().enumerate() {
            let db = self.ctx.get_db().await;
            let graceful_shutdown = self.ctx.graceful_shutdown.clone();
            let ctx = self.ctx.clone();
            let task_key = entry.key.clone().unwrap_or_else(|| format!("task_{}", i));

            let handle = tokio::spawn(Self::run_task_loop(
                ctx,
                db,
                entry,
                task_key,
                graceful_shutdown,
            ));
            task_handles.push(handle);
        }

        info!("Started {} scheduled tasks", task_handles.len());

        self.run_main_loop(&db, &process, heartbeat_interval, interval, task_handles)
            .await
    }
}

#[async_trait]
impl ProcessTrait for Scheduler {
    fn ctx(&self) -> &Arc<AppContext> {
        &self.ctx
    }

    fn process_info(&self) -> ProcessInfo {
        ProcessInfo::new("Scheduler", "scheduler")
    }
}
