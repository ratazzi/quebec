use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{Html, IntoResponse, Redirect},
};
use sea_orm::{ConnectionTrait, DbErr, Statement, TransactionTrait, Value};
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, error, info, warn};

use crate::context::ScheduledEntry;
use crate::control_plane::{models::RecurringTaskInfo, utils::clean_sql, ControlPlane};
use crate::notify::NotifyManager;
use crate::scheduler::enqueue_job;

impl ControlPlane {
    pub async fn recurring_jobs(
        State(state): State<Arc<ControlPlane>>,
    ) -> Result<Html<String>, (StatusCode, String)> {
        let start = Instant::now();
        let db = state.ctx.get_db().await;
        let db = db.as_ref();

        let table_config = &state.ctx.table_config;
        let backend = db.get_database_backend();

        // Get all recurring tasks
        let sql = clean_sql(&format!(
            "SELECT
            rt.id,
            rt.key,
            rt.class_name,
            rt.schedule,
            rt.queue_name,
            rt.priority,
            rt.description,
            (SELECT MAX(re.run_at) FROM {} re WHERE re.task_key = rt.key) as last_run_at
        FROM {}  rt
        ORDER BY rt.key ASC",
            table_config.recurring_executions, table_config.recurring_tasks
        ));

        let result = db
            .query_all(Statement::from_sql_and_values(backend, &sql, []))
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        let mut tasks = Vec::with_capacity(result.len());
        for row in result {
            let id: i64 = row.try_get("", "id").unwrap_or_default();
            let key: String = row.try_get("", "key").unwrap_or_default();
            let class_name: String = row.try_get("", "class_name").unwrap_or_default();
            let schedule: String = row.try_get("", "schedule").unwrap_or_default();
            let queue_name: String = row
                .try_get("", "queue_name")
                .unwrap_or_else(|_| "default".to_string());
            let priority: i32 = row.try_get("", "priority").unwrap_or(0);
            let description: Option<String> = row.try_get("", "description").ok();

            let last_run_at = row
                .try_get::<chrono::NaiveDateTime>("", "last_run_at")
                .ok()
                .map(|dt| Self::format_naive_datetime(dt));

            // Calculate next run time based on cron schedule
            let next_run_at = Self::calculate_next_run(&schedule);

            tasks.push(RecurringTaskInfo {
                id,
                key,
                class_name,
                schedule,
                queue_name,
                priority,
                description,
                last_run_at,
                next_run_at,
            });
        }

        debug!("Fetched recurring tasks in {:?}", start.elapsed());

        let mut context = tera::Context::new();
        context.insert("recurring_tasks", &tasks);
        context.insert("active_page", "recurring-jobs");

        let html = state
            .render_template("recurring-jobs.html", &mut context)
            .await?;

        Ok(Html(html))
    }

    pub async fn run_recurring_job_now(
        State(state): State<Arc<ControlPlane>>,
        Path(id): Path<i64>,
    ) -> impl IntoResponse {
        if let Err(e) = Self::do_run_recurring_job(&state, id).await {
            error!("Failed to run recurring job {}: {}", id, e);
        }
        Redirect::to("/recurring-jobs")
    }

    async fn do_run_recurring_job(state: &Arc<ControlPlane>, id: i64) -> Result<(), anyhow::Error> {
        let db = state.ctx.get_db().await;
        let db = db.as_ref();
        let table_config = &state.ctx.table_config;
        let backend = db.get_database_backend();

        let sql = clean_sql(&format!(
            "SELECT key, class_name, queue_name, priority, arguments FROM {} WHERE id = $1",
            table_config.recurring_tasks
        ));

        let row = db
            .query_one(Statement::from_sql_and_values(
                backend,
                &sql,
                [Value::from(id)],
            ))
            .await?
            .ok_or_else(|| anyhow::anyhow!("Recurring task {} not found", id))?;

        let key: String = row.try_get("", "key").unwrap_or_default();
        let class_name: String = row.try_get("", "class_name").unwrap_or_default();
        let queue_name: Option<String> = row.try_get("", "queue_name").ok();
        let priority: Option<i32> = row.try_get("", "priority").ok();
        let arguments: Option<String> = row.try_get("", "arguments").ok();

        let args = arguments
            .as_ref()
            .and_then(|s| serde_json::from_str::<Vec<serde_json::Value>>(s).ok())
            .map(|v| {
                v.into_iter()
                    .map(|j| serde_yaml::to_value(&j).unwrap_or(serde_yaml::Value::Null))
                    .collect()
            });

        let entry = ScheduledEntry {
            key: Some(key.clone()),
            class: class_name.clone(),
            queue: queue_name.clone(),
            priority,
            args,
            schedule: String::new(),
        };

        let now = chrono::Utc::now().naive_utc();
        let ctx = state.ctx.clone();

        let enqueued_queue = db
            .transaction::<_, String, DbErr>(|txn| {
                let entry = entry.clone();
                let ctx = ctx.clone();
                Box::pin(async move { enqueue_job(&ctx, txn, entry, now).await })
            })
            .await?;

        // Send NOTIFY after transaction commits
        if state.ctx.is_postgres() {
            NotifyManager::send_notify(&state.ctx.name, db, &enqueued_queue, "new_job")
                .await
                .inspect_err(|e| warn!("Failed to send NOTIFY: {}", e))
                .ok();
        }

        info!(
            "Manually triggered recurring job: {} ({}) -> queue: {}",
            key,
            class_name,
            queue_name.as_deref().unwrap_or("default")
        );

        Ok(())
    }

    fn calculate_next_run(schedule: &str) -> Option<String> {
        use croner::Cron;

        Cron::new(schedule)
            .with_seconds_optional()
            .parse()
            .ok()
            .and_then(|cron| cron.find_next_occurrence(&chrono::Utc::now(), false).ok())
            .map(|dt| Self::format_naive_datetime(dt.naive_utc()))
    }
}
