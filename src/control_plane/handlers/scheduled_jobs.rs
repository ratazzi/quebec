use std::sync::Arc;
use std::time::Instant;
use axum::{
    extract::{State, Query, Path},
    response::{Html, IntoResponse},
    http::StatusCode,
};
use sea_orm::{EntityTrait, Statement, DbBackend, Value, DbErr, TransactionTrait, ActiveModelTrait, ActiveValue, ConnectionTrait};
use tracing::{debug, info, error};

use crate::entities::{solid_queue_jobs, solid_queue_scheduled_executions};
use crate::control_plane::{ControlPlane, models::{Pagination, ScheduledJobInfo}};

impl ControlPlane {
    pub async fn scheduled_jobs(
        State(state): State<Arc<ControlPlane>>,
        Query(pagination): Query<Pagination>,
    ) -> Result<Html<String>, (StatusCode, String)> {
        let start = Instant::now();
        let db = state.ctx.get_db().await;
        let db = db.as_ref();
        debug!("Database connection obtained in {:?}", start.elapsed());

        let start = Instant::now();

        // Calculate page offset
        let page_size = state.page_size;
        let offset = (pagination.page - 1) * page_size;

        // Get scheduled jobs with related information
        let scheduled_jobs_result = db
            .query_all(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "SELECT
                    s.id as execution_id,
                    s.job_id,
                    s.scheduled_at,
                    j.class_name,
                    j.queue_name,
                    j.created_at
                FROM solid_queue_scheduled_executions s
                JOIN solid_queue_jobs j ON s.job_id = j.id
                WHERE j.finished_at IS NULL
                ORDER BY s.scheduled_at ASC
                LIMIT $1 OFFSET $2",
                [
                    Value::from(page_size as i32),
                    Value::from(offset as i32)
                ]
            ))
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        // Get current time, for calculating how long until execution
        let _now = chrono::Utc::now().naive_utc();

        let mut scheduled_jobs = Vec::with_capacity(scheduled_jobs_result.len());
        for row in scheduled_jobs_result {
            let execution_id: i64 = row.try_get("", "execution_id").unwrap_or_default();
            let job_id: i64 = row.try_get("", "job_id").unwrap_or_default();
            let class_name: String = row.try_get("", "class_name").unwrap_or_default();
            let queue_name: String = row.try_get("", "queue_name").unwrap_or_default();

            // Parse creation time
            let created_at_str = Self::format_datetime(row.try_get::<chrono::NaiveDateTime>("", "created_at"));

            // Parse scheduled execution time
            let (scheduled_at_str, scheduled_in) = match row.try_get::<chrono::NaiveDateTime>("", "scheduled_at") {
                Ok(dt) => {
                    let now = chrono::Utc::now().naive_utc();
                    let scheduled_in = if dt > now {
                        let duration = dt.signed_duration_since(now);
                        if duration.num_hours() > 0 {
                            format!("in {}h {}m", duration.num_hours(), duration.num_minutes() % 60)
                        } else if duration.num_minutes() > 0 {
                            format!("in {}m {}s", duration.num_minutes(), duration.num_seconds() % 60)
                        } else {
                            format!("in {}s", duration.num_seconds())
                        }
                    } else {
                        "overdue".to_string()
                    };

                    (Self::format_naive_datetime(dt), scheduled_in)
                },
                Err(_) => ("Unknown time".to_string(), "Unknown".to_string()),
            };

            scheduled_jobs.push(ScheduledJobInfo {
                id: execution_id,
                job_id,
                queue_name,
                class_name,
                created_at: created_at_str,
                scheduled_at: scheduled_at_str,
                scheduled_in,
            });
        }

        debug!("Fetched scheduled jobs in {:?}", start.elapsed());

        // Get total number of scheduled jobs for pagination
        let total_count = db
            .query_one(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "SELECT COUNT(*) AS count
                 FROM solid_queue_scheduled_executions s
                 JOIN solid_queue_jobs j ON s.job_id = j.id
                 WHERE j.finished_at IS NULL",
                []
            ))
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
            .map(|row| row.try_get::<i64>("", "count").unwrap_or(0))
            .unwrap_or(0);

        let total_pages = ((total_count as f64) / (page_size as f64)).ceil() as u64;
        let total_pages = total_pages.max(1);
        
        if total_pages > 0 && pagination.page > total_pages {
            return Err((StatusCode::NOT_FOUND, "Page not found".to_string()));
        }

        // Use abstract method to get all queue names and job classes
        let queue_names = state.get_queue_names()
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        let job_classes = state.get_job_classes()
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        let start = Instant::now();
        let mut context = tera::Context::new();
        context.insert("scheduled_jobs", &scheduled_jobs);
        context.insert("current_page_num", &pagination.page);
        context.insert("total_pages", &total_pages);
        context.insert("queue_names", &queue_names);
        context.insert("job_classes", &job_classes);
        context.insert("active_page", "scheduled-jobs");

        let html = state.render_template("scheduled-jobs.html", &mut context).await?;
        debug!("Template rendering completed in {:?}", start.elapsed());

        Ok(Html(html))
    }

    pub async fn cancel_scheduled_job(
        State(state): State<Arc<ControlPlane>>,
        Path(id): Path<i64>,
    ) -> impl IntoResponse {
        let db = state.ctx.get_db().await;
        let db = db.as_ref();

        // Use transaction to operate
        let txn_result = db.transaction::<_, (), DbErr>(|txn| {
            Box::pin(async move {
                // Find scheduled execution record to cancel
                let scheduled_execution = solid_queue_scheduled_executions::Entity::find_by_id(id)
                    .one(txn)
                    .await?;

                if let Some(execution) = scheduled_execution {
                    // First mark job as completed
                    let job_result = solid_queue_jobs::Entity::find_by_id(execution.job_id)
                        .one(txn)
                        .await?;

                    if let Some(job) = job_result {
                        // Update job status to completed
                        let mut job_model: solid_queue_jobs::ActiveModel = job.into();
                        job_model.finished_at = ActiveValue::Set(Some(chrono::Utc::now().naive_utc()));
                        job_model.updated_at = ActiveValue::Set(chrono::Utc::now().naive_utc());
                        job_model.update(txn).await?;
                    }

                    // Delete scheduled_execution record
                    solid_queue_scheduled_executions::Entity::delete_by_id(id)
                        .exec(txn)
                        .await?;

                    info!("Cancelled scheduled job ID: {}", id);
                } else {
                    return Err(DbErr::Custom(format!("Scheduled job with ID {} not found", id)));
                }

                Ok(())
            })
        }).await;

        match txn_result {
            Ok(_) => (StatusCode::SEE_OTHER, "/scheduled-jobs".to_string()),
            Err(e) => {
                error!("Failed to cancel scheduled job {}: {}", id, e);
                (StatusCode::INTERNAL_SERVER_ERROR, "/scheduled-jobs".to_string())
            }
        }
    }
}