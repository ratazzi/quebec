use std::sync::Arc;
use std::time::{Duration, Instant};

use std::error::Error;
use std::collections::HashMap;
use axum::{
    Router,
    routing::{get, post},
    extract::{State, Query, Path},
    response::{Html, IntoResponse},
    http::StatusCode,
};
use serde::{Deserialize, Serialize};
use tera::Tera;
use crate::context::AppContext;
use sea_orm::{EntityTrait, ActiveModelTrait, ActiveValue};
use sea_orm::{QueryFilter, ColumnTrait, DbErr};
use sea_orm::QuerySelect;

use sea_orm::Order;
use sea_orm::ConnectionTrait;
use sea_orm::TransactionTrait;

use sea_orm::PaginatorTrait;
use sea_orm::Value;
use sea_orm::Statement;
use sea_orm::DbBackend;
use sea_orm::QueryOrder;
use crate::entities::solid_queue_jobs;
use crate::entities::solid_queue_pauses;
use crate::entities::solid_queue_failed_executions;
use crate::entities::solid_queue_claimed_executions;

use crate::entities::solid_queue_scheduled_executions;
use crate::entities::solid_queue_blocked_executions;
use crate::entities::solid_queue_processes;
use tracing::{error, info, debug, warn};
use tokio::time::sleep;
use tower_http::trace::{self, TraceLayer};
use tracing::{Level, Span, instrument};

use chrono::NaiveDateTime;

use std::sync::RwLock;
use crate::web::templates;

pub struct ControlPlane {
    ctx: Arc<AppContext>,
    tera: RwLock<Tera>,  // Use RwLock to provide thread-safe interior mutability
    template_path: String,
    page_size: usize,
}

#[derive(Debug, Serialize)]
struct QueueInfo {
    name: String,
    jobs_count: usize,
    status: String,
}

#[derive(Debug, Serialize)]
struct WorkerInfo {
    id: i64,
    name: String,
    kind: String,
    hostname: Option<String>,
    pid: i32,
    last_heartbeat_at: String,
    seconds_since_heartbeat: i64,
    status: String,
}

#[derive(Debug, Deserialize)]
struct Pagination {
    #[serde(default = "default_page")]
    page: usize,
}

fn default_page() -> usize {
    1
}

// Add data structure for failed tasks
#[derive(Debug, Serialize)]
struct FailedJobInfo {
    id: i64,
    queue_name: String,
    class_name: String,
    error: String,
    failed_at: String,
}

// Add InProgressJobInfo structure
#[derive(Debug, Serialize)]
struct InProgressJobInfo {
    id: i64,
    job_id: i64,
    queue_name: String,
    class_name: String,
    worker_id: String,
    started_at: String,
    runtime: String,
}

// Add data structure for jobs in queue
#[derive(Debug, Serialize)]
struct QueueJobInfo {
    id: i64,
    class_name: String,
    status: String,
    created_at: String,
    execution_id: Option<i64>,
}

// Add ScheduledJobInfo structure at appropriate location
#[derive(Debug, Serialize)]
struct ScheduledJobInfo {
    id: i64,
    job_id: i64,
    queue_name: String,
    class_name: String,
    created_at: String,
    scheduled_at: String,
    scheduled_in: String,
}

// Add BlockedJobInfo structure
#[derive(Debug, Serialize)]
struct BlockedJobInfo {
    id: i64,
    job_id: i64,
    queue_name: String,
    class_name: String,
    concurrency_key: String,
    created_at: String,
    waiting_time: String,
    expires_at: String,
}

// Add FinishedJobInfo structure
#[derive(Debug, Serialize)]
struct FinishedJobInfo {
    id: i64,
    queue_name: String,
    class_name: String,
    created_at: String,
    finished_at: String,
    runtime: String,
}

// Add JobDetailsInfo structure
#[derive(Debug, Serialize)]
struct JobDetailsInfo {
    id: i64,
    queue_name: String,
    class_name: String,
    status: String,
    created_at: String,
    failed_at: Option<String>,
    scheduled_at: Option<String>,
    scheduled_in: Option<String>,
    concurrency_key: Option<String>,
    waiting_time: Option<String>,
    expires_at: Option<String>,
    started_at: Option<String>,
    runtime: Option<String>,
    worker_id: Option<String>,
    finished_at: Option<String>,
    error: Option<String>,
    backtrace: Option<String>,
    arguments: Option<String>,
    context: Option<String>,
    execution_id: Option<i64>,
    execution_history: Option<Vec<ExecutionHistoryItem>>,
}

#[derive(Debug, Serialize)]
struct ExecutionHistoryItem {
    attempt: i32,
    timestamp: String,
    status: String,
    error: Option<String>,
}

impl ControlPlane {
    pub fn new(ctx: Arc<AppContext>) -> Self {
        let start = Instant::now();

        // Initialize Tera
        let mut tera = Tera::default();

        // Add all templates
        for template_name in templates::list_templates() {
            if let Some(content) = templates::get_template_content(&template_name) {
                if let Err(e) = tera.add_raw_template(&template_name, &content) {
                    error!("Failed to add template {}: {}", template_name, e);
                }
            }
        }

        // Set automatic HTML escaping
        tera.autoescape_on(vec!["html"]);

        // Output final template list
        let template_list = tera.get_template_names().collect::<Vec<_>>();
        debug!("Available templates: {:?}", template_list);
        debug!("Tera template engine initialized in {:?}", start.elapsed());

        // Save template path for hot reload
        let template_path = "src/web/templates/**/*".to_string();

        Self {
            ctx,
            tera: RwLock::new(tera),
            template_path,
            page_size: 10
        }
    }

    pub fn router(self) -> Router {
        Router::new()
            .route("/", get(Self::overview))
            .route("/queues", get(Self::queues))
            .route("/queues/:name", get(Self::queue_details))
            .route("/queues/:name/pause", post(Self::pause_queue))
            .route("/queues/:name/resume", post(Self::resume_queue))
            .route("/failed-jobs", get(Self::failed_jobs))
            .route("/failed-jobs/:id/retry", post(Self::retry_failed_job))
            .route("/failed-jobs/:id/delete", post(Self::delete_failed_job))
            .route("/failed-jobs/all/retry", post(Self::retry_all_failed_jobs))
            .route("/failed-jobs/all/delete", post(Self::discard_all_failed_jobs))
            .route("/in-progress-jobs", get(Self::in_progress_jobs))
            .route("/in-progress-jobs/:id/cancel", post(Self::cancel_in_progress_job))
            .route("/in-progress-jobs/all/cancel", post(Self::cancel_all_in_progress_jobs))
            .route("/blocked-jobs", get(Self::blocked_jobs))
            .route("/blocked-jobs/:id/unblock", post(Self::unblock_job))
            .route("/blocked-jobs/:id/cancel", post(Self::cancel_blocked_job))
            .route("/blocked-jobs/all/unblock", post(Self::unblock_all_jobs))
            .route("/scheduled-jobs", get(Self::scheduled_jobs))
            .route("/scheduled-jobs/:id/cancel", post(Self::cancel_scheduled_job))
            .route("/finished-jobs", get(Self::finished_jobs))
            .route("/jobs/:id", get(Self::job_details))
            .route("/stats", get(Self::stats))
            .route("/workers", get(Self::workers))
            .layer(
                TraceLayer::new_for_http()
                    .make_span_with(trace::DefaultMakeSpan::new().level(Level::INFO))
                    .on_request(|request: &axum::http::Request<_>, _span: &Span| {
                        debug!("Started {} {}", request.method(), request.uri());
                    })
                    .on_response(|response: &axum::http::Response<_>, latency: Duration, _span: &Span| {
                        info!(
                            "Finished in {:?} with status {}",
                            latency,
                            response.status()
                        );
                    })
                    .on_failure(trace::DefaultOnFailure::new().level(Level::ERROR))
            )
            .with_state(Arc::new(self))
    }

    async fn index() -> &'static str {
        "Quebec Control Plane"
    }

    #[instrument(skip(state), fields(path = "/"))]
    async fn overview(
        State(state): State<Arc<ControlPlane>>,
        Query(params): Query<std::collections::HashMap<String, String>>,
    ) -> Result<Html<String>, (StatusCode, String)> {
        let start = Instant::now();
        let db = state.ctx.get_db().await;
        let db = db.as_ref();
        debug!("Database connection obtained in {:?}", start.elapsed());

        // Get time range parameter, default to 24 hours
        let hours: i64 = params.get("hours")
            .and_then(|h| h.parse().ok())
            .unwrap_or(24);

        let now = chrono::Utc::now().naive_utc();
        let period_start = now - chrono::Duration::hours(hours);
        let previous_period_start = period_start - chrono::Duration::hours(hours);

        // Get total number of completed jobs in current period
        let total_jobs_processed = solid_queue_jobs::Entity::find()
            .filter(solid_queue_jobs::Column::FinishedAt.is_not_null())
            .filter(solid_queue_jobs::Column::FinishedAt.gt(period_start))
            .count(db)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        // Get total number of completed jobs in previous period for calculating change rate
        let previous_jobs_processed = solid_queue_jobs::Entity::find()
            .filter(solid_queue_jobs::Column::FinishedAt.is_not_null())
            .filter(solid_queue_jobs::Column::FinishedAt.gt(previous_period_start))
            .filter(solid_queue_jobs::Column::FinishedAt.lte(period_start))
            .count(db)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        // Calculate change rate of job processing count
        let jobs_processed_change = if previous_jobs_processed > 0 {
            ((total_jobs_processed as f64 - previous_jobs_processed as f64) / previous_jobs_processed as f64 * 100.0).round() as i32
        } else {
            0
        };

        // Get average processing time of jobs in current period
        let avg_duration_stmt = Statement::from_sql_and_values(
            DbBackend::Postgres,
            r#"SELECT AVG(EXTRACT(EPOCH FROM (finished_at - created_at))) as avg_duration
               FROM solid_queue_jobs
               WHERE finished_at IS NOT NULL
               AND finished_at > $1"#,
            [period_start.into()]
        );

        let avg_duration: Option<f64> = db
            .query_one(avg_duration_stmt)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
            .map(|row| row.try_get("", "avg_duration").unwrap_or(0.0));

        // Get average processing time of jobs in previous period
        let prev_avg_duration_stmt = Statement::from_sql_and_values(
            DbBackend::Postgres,
            r#"SELECT AVG(EXTRACT(EPOCH FROM (finished_at - created_at))) as avg_duration
               FROM solid_queue_jobs
               WHERE finished_at IS NOT NULL
               AND finished_at > $1
               AND finished_at <= $2"#,
            [previous_period_start.into(), period_start.into()]
        );

        let prev_avg_duration: Option<f64> = db
            .query_one(prev_avg_duration_stmt)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
            .map(|row| row.try_get("", "avg_duration").unwrap_or(0.0));

        // Calculate change rate of average processing time
        let avg_duration_change = match (avg_duration, prev_avg_duration) {
            (Some(curr), Some(prev)) if prev > 0.0 => {
                ((curr - prev) / prev * 100.0).round() as i32
            },
            _ => 0
        };

        // Format average processing time
        let avg_job_duration = match avg_duration {
            Some(secs) if secs >= 3600.0 => {
                format!("{:.1}h", secs / 3600.0)
            },
            Some(secs) if secs >= 60.0 => {
                format!("{:.1}m", secs / 60.0)
            },
            Some(secs) => {
                format!("{:.1}s", secs)
            },
            None => "N/A".to_string()
        };

        // Get number of active workers
        let active_workers = solid_queue_processes::Entity::find()
            .filter(solid_queue_processes::Column::LastHeartbeatAt.gt(now - chrono::Duration::seconds(30)))
            .count(db)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        // Get number of active workers in previous period (simplified here, should query historical data)
        let prev_active_workers = active_workers; // Assume no change, should get from historical data

        // Calculate change in number of active workers
        let active_workers_change = active_workers as i32 - prev_active_workers as i32;

        // Calculate failure rate
        let failed_jobs = solid_queue_failed_executions::Entity::find()
            .filter(solid_queue_failed_executions::Column::CreatedAt.gt(period_start))
            .count(db)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        let total_jobs = solid_queue_jobs::Entity::find()
            .filter(solid_queue_jobs::Column::CreatedAt.gt(period_start))
            .count(db)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        let failed_jobs_rate = if total_jobs > 0 {
            ((failed_jobs as f64 / total_jobs as f64) * 100.0).round() as i32
        } else {
            0
        };

        // Get failure rate of previous period
        let prev_failed_jobs = solid_queue_failed_executions::Entity::find()
            .filter(solid_queue_failed_executions::Column::CreatedAt.gt(previous_period_start))
            .filter(solid_queue_failed_executions::Column::CreatedAt.lte(period_start))
            .count(db)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        let prev_total_jobs = solid_queue_jobs::Entity::find()
            .filter(solid_queue_jobs::Column::CreatedAt.gt(previous_period_start))
            .filter(solid_queue_jobs::Column::CreatedAt.lte(period_start))
            .count(db)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        let prev_failed_jobs_rate = if prev_total_jobs > 0 {
            ((prev_failed_jobs as f64 / prev_total_jobs as f64) * 100.0).round() as i32
        } else {
            0
        };

        let failed_rate_change = failed_jobs_rate - prev_failed_jobs_rate;

        // Prepare time labels and job processing data (for charts)
        let mut time_labels = Vec::new();
        let mut jobs_processed_data = Vec::new();

        // Determine time interval based on selected time range
        let (interval_hours, format_string) = if hours <= 24 {
            (1, "%H:%M") // Every hour, display hour:minute
        } else if hours <= 168 { // 7 days
            (6, "%m-%d %H:%M") // Every 6 hours, display month-day hour:minute
        } else {
            (24, "%m-%d") // Every day, display month-day
        };

        // Generate time series data
        for i in 0..(hours / interval_hours) {
            let end_time = now - chrono::Duration::hours(i * interval_hours);
            let start_time = end_time - chrono::Duration::hours(interval_hours);

            time_labels.push(end_time.format(format_string).to_string());

            // Query number of jobs completed in this time period
            let period_jobs = solid_queue_jobs::Entity::find()
                .filter(solid_queue_jobs::Column::FinishedAt.is_not_null())
                .filter(solid_queue_jobs::Column::FinishedAt.gt(start_time))
                .filter(solid_queue_jobs::Column::FinishedAt.lte(end_time))
                .count(db)
                .await
                .unwrap_or(0);

            jobs_processed_data.push(period_jobs);
        }

        // Reverse arrays to display in chronological order
        time_labels.reverse();
        jobs_processed_data.reverse();

        // Get job type distribution
        let job_types_stmt = Statement::from_sql_and_values(
            DbBackend::Postgres,
            r#"SELECT class_name, COUNT(*) as count
               FROM solid_queue_jobs
               WHERE created_at > $1
               GROUP BY class_name
               ORDER BY count DESC
               LIMIT 7"#,
            [period_start.into()]
        );

        let job_types_result = db
            .query_all(job_types_stmt)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        let mut job_types_labels = Vec::new();
        let mut job_types_data = Vec::new();

        for row in job_types_result {
            let class_name: String = row.try_get("", "class_name").unwrap_or_default();
            let count: i64 = row.try_get("", "count").unwrap_or_default();

            job_types_labels.push(class_name);
            job_types_data.push(count);
        }

        // Get queue performance statistics
        let queue_performance_stmt = Statement::from_sql_and_values(
            DbBackend::Postgres,
            r#"SELECT
                 j.queue_name,
                 COUNT(*) FILTER (WHERE j.finished_at IS NOT NULL) as jobs_processed,
                 AVG(EXTRACT(EPOCH FROM (j.finished_at - j.created_at))) FILTER (WHERE j.finished_at IS NOT NULL) as avg_duration,
                 COUNT(*) FILTER (WHERE EXISTS (SELECT 1 FROM solid_queue_failed_executions f WHERE f.job_id = j.id)) as failed_jobs,
                 COUNT(*) as total_jobs
               FROM solid_queue_jobs j
               WHERE j.created_at > $1
               GROUP BY j.queue_name"#,
            [period_start.into()]
        );

        let queue_performance_result = db
            .query_all(queue_performance_stmt)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        // Get paused queues
        let paused_queues = solid_queue_pauses::Entity::find()
            .all(db)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        let paused_queue_names: Vec<String> = paused_queues.iter()
            .map(|p| p.queue_name.clone())
            .collect();

        let mut queue_stats = Vec::new();

        for row in queue_performance_result {
            let queue_name: String = row.try_get("", "queue_name").unwrap_or_default();
            let jobs_processed: i64 = row.try_get("", "jobs_processed").unwrap_or_default();
            let avg_duration: Option<f64> = row.try_get("", "avg_duration").ok();
            let failed_jobs: i64 = row.try_get("", "failed_jobs").unwrap_or_default();
            let total_jobs: i64 = row.try_get("", "total_jobs").unwrap_or_default();

            // Format average processing time
            let avg_duration_str = match avg_duration {
                Some(secs) if secs >= 3600.0 => {
                    format!("{:.1}h", secs / 3600.0)
                },
                Some(secs) if secs >= 60.0 => {
                    format!("{:.1}m", secs / 60.0)
                },
                Some(secs) => {
                    format!("{:.1}s", secs)
                },
                None => "N/A".to_string()
            };

            // Calculate failure rate
            let failed_rate = if total_jobs > 0 {
                ((failed_jobs as f64 / total_jobs as f64) * 100.0).round() as i32
            } else {
                0
            };

            // Check queue status
            let status = if paused_queue_names.contains(&queue_name) {
                "paused"
            } else {
                "active"
            };

            queue_stats.push(serde_json::json!({
                "name": queue_name,
                "jobs_processed": jobs_processed,
                "avg_duration": avg_duration_str,
                "failed_rate": failed_rate,
                "status": status
            }));
        }

        // Prepare template context
        let mut context = tera::Context::new();
        context.insert("total_jobs_processed", &total_jobs_processed);
        context.insert("jobs_processed_change", &jobs_processed_change);
        context.insert("avg_job_duration", &avg_job_duration);
        context.insert("avg_duration_change", &avg_duration_change);
        context.insert("active_workers", &active_workers);
        context.insert("active_workers_change", &active_workers_change);
        context.insert("failed_jobs_rate", &failed_jobs_rate);
        context.insert("failed_rate_change", &failed_rate_change);

        // Get recently failed jobs
        let recent_failed_jobs_stmt = Statement::from_sql_and_values(
            DbBackend::Postgres,
            r#"SELECT
                f.id,
                j.class_name,
                j.queue_name,
                f.created_at as failed_at,
                f.error
              FROM solid_queue_failed_executions f
              JOIN solid_queue_jobs j ON f.job_id = j.id
              WHERE f.created_at > $1
              ORDER BY f.created_at DESC
              LIMIT 10"#,
            [period_start.into()]
        );

        let recent_failed_jobs_result = db
            .query_all(recent_failed_jobs_stmt)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        let mut recent_failed_jobs = Vec::new();

        for row in recent_failed_jobs_result {
            let id: i64 = row.try_get("", "id").unwrap_or_default();
            let class_name: String = row.try_get("", "class_name").unwrap_or_default();
            let queue_name: String = row.try_get("", "queue_name").unwrap_or_default();
            let failed_at: Option<NaiveDateTime> = row.try_get("", "failed_at").ok();
            let error: String = row.try_get("", "error").unwrap_or_default();

            let formatted_failed_at = Self::format_optional_datetime(failed_at)
                .unwrap_or_else(|| "N/A".to_string());

            recent_failed_jobs.push(serde_json::json!({
                "id": id,
                "class_name": class_name,
                "queue_name": queue_name,
                "failed_at": formatted_failed_at,
                "error": error
            }));
        }

        // Serialize arrays to JSON strings
        context.insert("time_labels", &serde_json::to_string(&time_labels).unwrap_or_else(|_| "[]".to_string()));
        context.insert("jobs_processed_data", &serde_json::to_string(&jobs_processed_data).unwrap_or_else(|_| "[]".to_string()));
        context.insert("job_types_labels", &serde_json::to_string(&job_types_labels).unwrap_or_else(|_| "[]".to_string()));
        context.insert("job_types_data", &serde_json::to_string(&job_types_data).unwrap_or_else(|_| "[]".to_string()));
        context.insert("queue_stats", &queue_stats);
        context.insert("recent_failed_jobs", &recent_failed_jobs);
        context.insert("active_page", "overview");

        // Render template
        let html = state.render_template("overview.html", &mut context).await?;
        debug!("Template rendering completed in {:?}", start.elapsed());

        Ok(Html(html))
    }

    #[instrument(skip(state), fields(path = "/queues"))]
    async fn queues(
        State(state): State<Arc<ControlPlane>>,
        Query(pagination): Query<Pagination>,
    ) -> Result<Html<String>, (StatusCode, String)> {
        let start = Instant::now();
        let db = state.ctx.get_db().await;
        let db = db.as_ref();
        debug!("Database connection obtained in {:?}", start.elapsed());

        let start = Instant::now();

        // Use Sea-ORM's native SQL query functionality, only count unfinished jobs
        // Get all queue names
        let all_queue_names = state.get_queue_names(db)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        let stmt = Statement::from_sql_and_values(
            DbBackend::Postgres,
            "SELECT queue_name, COUNT(*) as count
             FROM solid_queue_jobs
             WHERE finished_at IS NULL
             GROUP BY queue_name",
            []
        );

        // Get queue counts with unfinished jobs
        let queue_counts_map: HashMap<String, i64> = db
            .query_all(stmt)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
            .into_iter()
            .map(|row| {
                let queue_name: String = row.try_get("", "queue_name").unwrap_or_default();
                let count: i64 = row.try_get("", "count").unwrap_or_default();
                (queue_name, count)
            })
            .collect();

        // Ensure all queues are included in results, even if they have no unfinished jobs
        let queue_counts: Vec<(String, i64)> = all_queue_names
            .into_iter()
            .map(|queue_name| (queue_name.clone(), *queue_counts_map.get(&queue_name).unwrap_or(&0)))
            .collect();

        debug!("Fetched jobs in {:?}", start.elapsed());

        let start = Instant::now();
        // Get paused queues
        let paused_queues = solid_queue_pauses::Entity::find()
            .all(db)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        debug!("Fetched paused queues in {:?}", start.elapsed());

        let start = Instant::now();
        let paused_queue_names: Vec<String> = paused_queues.iter()
            .map(|p| p.queue_name.clone())
            .collect();

        let queue_infos: Vec<QueueInfo> = queue_counts
            .into_iter()
            .map(|(name, count)| QueueInfo {
                name: name.clone(),
                jobs_count: count as usize,
                status: if paused_queue_names.contains(&name) {
                    "paused".to_string()
                } else {
                    "active".to_string()
                },
            })
            .collect();
        debug!("Processed queue data in {:?}", start.elapsed());

        let start = Instant::now();
        let mut context = tera::Context::new();
        context.insert("current_page_num", &pagination.page);
        context.insert("total_pages", &1);
        context.insert("queues", &queue_infos);
        context.insert("active_page", "queues");

        let html = state.render_template("queues.html", &mut context).await?;
        debug!("Template rendering completed in {:?}", start.elapsed());

        Ok(Html(html))
    }

    async fn pause_queue(
        State(state): State<Arc<ControlPlane>>,
        Path(queue_name): Path<String>,
    ) -> impl IntoResponse {
        let db = state.ctx.get_db().await;
        let db = db.as_ref();

        // Create a new pause record
        let pause = solid_queue_pauses::ActiveModel {
            id: ActiveValue::NotSet,
            queue_name: ActiveValue::Set(queue_name.clone()),
            created_at: ActiveValue::Set(chrono::Utc::now().naive_utc()),
        };

        pause.insert(db).await
            .map(|_| StatusCode::OK)
            .map_err(|e| {
                error!("Failed to pause queue: {}", e);
                StatusCode::INTERNAL_SERVER_ERROR
            })
    }

    async fn resume_queue(
        State(state): State<Arc<ControlPlane>>,
        Path(queue_name): Path<String>,
    ) -> impl IntoResponse {
        let db = state.ctx.get_db().await;
        let db = db.as_ref();

        // Delete the pause record for this queue
        solid_queue_pauses::Entity::delete_many()
            .filter(solid_queue_pauses::Column::QueueName.eq(queue_name.clone()))
            .exec(db)
            .await
            .map(|_| StatusCode::OK)
            .map_err(|e| {
                error!("Failed to resume queue: {}", e);
                StatusCode::INTERNAL_SERVER_ERROR
            })
    }

    #[instrument(skip(state), fields(path = "/failed-jobs"))]
    async fn failed_jobs(
        State(state): State<Arc<ControlPlane>>,
        Query(pagination): Query<Pagination>,
    ) -> Result<Html<String>, (StatusCode, String)> {
        let start = Instant::now();
        let db = state.ctx.get_db().await;
        let db = db.as_ref();
        debug!("Database connection obtained in {:?}", start.elapsed());

        let start = Instant::now();

        // Calculate pagination offset
        let page_size = state.page_size;
        let offset = (pagination.page - 1) * page_size;

        // Get failed execution records
        let failed_executions = solid_queue_failed_executions::Entity::find()
            .offset(offset as u64)
            .limit(page_size as u64)
            .all(db)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        // Create a vector to store failed job information
        let mut failed_jobs: Vec<FailedJobInfo> = Vec::with_capacity(failed_executions.len());

        // Get job information for each failed execution
        for execution in failed_executions {
            if let Ok(Some(job)) = solid_queue_jobs::Entity::find_by_id(execution.job_id).one(db).await {
                failed_jobs.push(FailedJobInfo {
                    id: job.id,
                    queue_name: job.queue_name.clone(),
                    class_name: job.class_name.clone(),
                    error: execution.error.unwrap_or_default(),
                    failed_at: Self::format_naive_datetime(execution.created_at),
                });
            }
        }

        debug!("Fetched failed jobs in {:?}", start.elapsed());
        info!("Found {} failed jobs", failed_jobs.len());

        // Get total count of failed jobs for pagination calculation
        let start = Instant::now();

        // Execute SQL count query directly
        let total_count: i64 = solid_queue_failed_executions::Entity::find()
            .select_only()
            .column_as(sea_orm::sea_query::Expr::expr(sea_orm::sea_query::Func::count(sea_orm::sea_query::Expr::cust("*"))), "count")
            .into_tuple()
            .one(db)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
            .unwrap_or(0);

        info!("Total failed jobs count: {}", total_count);

        let total_pages = ((total_count as f64) / (page_size as f64)).ceil() as usize;
        if total_pages > 0 && pagination.page > total_pages {
            return Err((StatusCode::NOT_FOUND, "Page not found".to_string()));
        }
        debug!("Fetched count in {:?}", start.elapsed());

        let start = Instant::now();
        let mut context = tera::Context::new();
        context.insert("failed_jobs", &failed_jobs);
        context.insert("current_page_num", &pagination.page);
        context.insert("total_pages", &total_pages);
        context.insert("active_page", "failed-jobs");

        // Use helper method to render template
        let html = state.render_template("failed-jobs.html", &mut context).await?;

        debug!("Template rendering completed in {:?}", start.elapsed());

        Ok(Html(html))
    }

    #[instrument(skip(state), fields(path = "/failed-jobs/:id/retry"))]
    async fn retry_failed_job(
        State(state): State<Arc<ControlPlane>>,
        Path(id): Path<i64>,
    ) -> impl IntoResponse {
        use crate::entities::solid_queue_failed_executions::{Entity as FailedExecutionEntity, Retryable};

        let db = state.ctx.get_db().await;
        let db = db.as_ref();

        // Use transaction for operation
        db.transaction::<_, (), DbErr>(|txn| {
            Box::pin(async move {
                // First get failed execution record
                let failed_execution = FailedExecutionEntity::find()
                    .filter(crate::entities::solid_queue_failed_executions::Column::JobId.eq(id))
                    .one(txn)
                    .await?;

                match failed_execution {
                    Some(execution) => {
                        // Use Retryable trait's retry method
                        execution.retry(txn).await?;
                        Ok(())
                    },
                    None => {
                        Err(DbErr::Custom(format!("Failed execution for job {} not found", id)))
                    }
                }
            })
        })
        .await
        .map(|_| {
            info!("Retried failed job {}", id);
            (StatusCode::SEE_OTHER, [("Location", "/failed-jobs")])
        })
        .map_err(|e| {
            error!("Failed to retry job {}: {}", id, e);
            (StatusCode::INTERNAL_SERVER_ERROR, [("Location", "/failed-jobs")])
        })
    }

    #[instrument(skip(state), fields(path = "/failed-jobs/:id/delete"))]
    async fn delete_failed_job(
        State(state): State<Arc<ControlPlane>>,
        Path(id): Path<i64>,
    ) -> impl IntoResponse {
        use crate::entities::solid_queue_failed_executions::{Entity as FailedExecutionEntity, Discardable};

        let db = state.ctx.get_db().await;
        let db = db.as_ref();

        // Use transaction for operation
        db.transaction::<_, (), DbErr>(|txn| {
            Box::pin(async move {
                // First get failed execution record
                let failed_execution = FailedExecutionEntity::find()
                    .filter(crate::entities::solid_queue_failed_executions::Column::JobId.eq(id))
                    .one(txn)
                    .await?;

                match failed_execution {
                    Some(execution) => {
                        // Use Discardable trait's discard method
                        execution.discard(txn).await?;
                        Ok(())
                    },
                    None => {
                        Err(DbErr::Custom(format!("Failed execution for job {} not found", id)))
                    }
                }
            })
        })
        .await
        .map(|_| {
            info!("Deleted failed job {}", id);
            (StatusCode::SEE_OTHER, [("Location", "/failed-jobs")])
        })
        .map_err(|e| {
            error!("Failed to delete job {}: {}", id, e);
            (StatusCode::INTERNAL_SERVER_ERROR, [("Location", "/failed-jobs")])
        })
    }

    #[instrument(skip(state), fields(path = "/failed-jobs/all/retry"))]
    async fn retry_all_failed_jobs(
        State(state): State<Arc<ControlPlane>>,
    ) -> impl IntoResponse {
        use crate::entities::solid_queue_failed_executions::{Entity as FailedExecutionEntity, Retryable};

        let db = state.ctx.get_db().await;
        let db = db.as_ref();

        // Use transaction for operation
        db.transaction::<_, u64, DbErr>(|txn| {
            Box::pin(async move {
                // Use Retryable trait's retry_all method
                let count = FailedExecutionEntity.retry_all(txn).await?;
                Ok(count)
            })
        })
        .await
        .map(|count| {
            info!("Retried all {} failed jobs", count);
            (StatusCode::SEE_OTHER, [("Location", "/failed-jobs")])
        })
        .map_err(|e| {
            error!("Failed to retry all jobs: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, [("Location", "/failed-jobs")])
        })
    }

    #[instrument(skip(state), fields(path = "/failed-jobs/all/delete"))]
    async fn discard_all_failed_jobs(
        State(state): State<Arc<ControlPlane>>,
    ) -> impl IntoResponse {
        use crate::entities::solid_queue_failed_executions::{Entity as FailedExecutionEntity, Discardable};

        let db = state.ctx.get_db().await;
        let db = db.as_ref();

        // Use transaction for operation
        db.transaction::<_, u64, DbErr>(|txn| {
            Box::pin(async move {
                // Use Discardable trait's discard_all method
                let count = FailedExecutionEntity.discard_all(txn).await?;
                Ok(count)
            })
        })
        .await
        .map(|count| {
            info!("Discarded all {} failed jobs", count);
            (StatusCode::SEE_OTHER, [("Location", "/failed-jobs")])
        })
        .map_err(|e| {
            error!("Failed to discard all jobs: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, [("Location", "/failed-jobs")])
        })
    }

    // Add the following helper methods for extracting common functionality

    /// Get all unique queue names
    async fn get_queue_names(&self, db: &sea_orm::DatabaseConnection) -> Result<Vec<String>, DbErr> {
        db.query_all(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "SELECT DISTINCT queue_name FROM solid_queue_jobs",
            []
        ))
        .await
        .map(|rows| {
            rows.into_iter()
                .filter_map(|row| row.try_get::<String>("", "queue_name").ok())
                .collect()
        })
    }

    /// Get all unique job class names
    async fn get_job_classes(&self, db: &sea_orm::DatabaseConnection) -> Result<Vec<String>, DbErr> {
        db.query_all(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "SELECT DISTINCT class_name FROM solid_queue_jobs",
            []
        ))
        .await
        .map(|rows| {
            rows.into_iter()
                .filter_map(|row| row.try_get::<String>("", "class_name").ok())
                .collect()
        })
    }

    /// Render template and handle errors
    async fn render_template(&self, template_name: &str, context: &mut tera::Context) -> Result<String, (StatusCode, String)> {
        // In debug compilation mode, reload templates
        #[cfg(debug_assertions)]
        {
            debug!("Debug mode detected, reloading templates");
            match Tera::new(&self.template_path) {
                Ok(new_tera) => {
                    // Successfully loaded new templates, replace existing Tera instance
                    match self.tera.write() {
                        Ok(mut tera) => {
                            *tera = new_tera;
                            tera.autoescape_on(vec!["html"]);
                            debug!("Templates reloaded successfully");
                        },
                        Err(e) => error!("Failed to acquire write lock: {}", e)
                    }
                },
                Err(e) => error!("Error reloading templates: {}", e)
            }
        }
        // Add database connection information
        let _db = self.ctx.get_db().await;

        // Get database DSN information directly from AppContext
        let dsn = self.ctx.dsn.to_string();
        debug!("Original DSN: {}", dsn);

        // Handle different types of database connections
        let (db_type, connection_info) = if dsn.starts_with("postgres:") {
            // If it's a PostgreSQL database
            let clean_dsn = if dsn.contains("@") {
                // Contains username and password, need to remove
                let parts: Vec<&str> = dsn.split("@").collect();
                if parts.len() > 1 {
                    // Get protocol part
                    let protocol = if parts[0].contains("://") {
                        parts[0].split("://").next().unwrap_or("postgres")
                    } else {
                        "postgres"
                    };

                    // Reconstruct URL, removing username and password
                    let host_part = parts[1];
                    format!("{0}://{1}", protocol, host_part)
                } else {
                    dsn.clone()
                }
            } else {
                // Does not contain username and password
                dsn.clone()
            };

            // If there are query parameters, remove them
            let final_dsn = if clean_dsn.contains("?") {
                clean_dsn.split("?").next().unwrap_or(&clean_dsn).to_string()
            } else {
                clean_dsn
            };

            debug!("Cleaned PostgreSQL DSN: {}", final_dsn);
            ("PostgreSQL", final_dsn)
        } else if dsn.starts_with("sqlite:") {
            // If it's a SQLite database
            let path = dsn.replace("sqlite:", "").replace("//", "");
            debug!("SQLite path: {}", path);
            ("SQLite", format!("sqlite://{}", path))
        } else {
            // Other types of databases
            debug!("Unknown database type: {}", dsn);
            ("Unknown", dsn)
        };

        let db_info = serde_json::json!({
            "database_type": db_type,
            "connection_type": "Pool",
            "status": "Connected",
            "dsn": connection_info
        });
        context.insert("db_info", &db_info);

        // In debug compilation mode, reload templates
        #[cfg(debug_assertions)]
        {
            debug!("Debug mode detected, reloading templates before rendering");

            // Initialize new Tera instance
            let mut new_tera = Tera::default();

            // Use embedded template system to reload all templates
            for template_name in templates::list_templates() {
                if let Some(content) = templates::get_template_content(&template_name) {
                    if let Err(e) = new_tera.add_raw_template(&template_name, &content) {
                        error!("Failed to add template {}: {}", template_name, e);
                    }
                }
            }

            // Set automatic HTML escaping
            new_tera.autoescape_on(vec!["html"]);

            // Update Tera instance
            if let Ok(mut tera) = self.tera.write() {
                *tera = new_tera;
                debug!("Templates reloaded successfully");
            } else {
                error!("Failed to acquire write lock for template reloading");
            }
        }

        // Check if template exists in Tera instance
        let tera_read_result = self.tera.read();
        if let Err(e) = tera_read_result {
            error!("Failed to acquire read lock: {}", e);
            return Err((StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to acquire template lock: {}", e)));
        }

        let tera = tera_read_result.unwrap();
        let templates = tera.get_template_names().collect::<Vec<_>>();
        debug!("Available templates: {:?}", templates);

        // Try to render template
        match tera.render(template_name, context) {
            Ok(html) => Ok(html),
            Err(e) => {
                error!("Failed to render {}: {}", template_name, e);

                // Output complete error chain
                let mut error_detail = format!("Template error: {}", e);
                let mut current_error = e.source();
                let mut level = 1;

                while let Some(source) = current_error {
                    error_detail.push_str(&format!("\n  Caused by ({}): {}", level, source));
                    current_error = source.source();
                    level += 1;
                }

                // Output Tera-specific error details
                error!("Detailed error: {}", error_detail);

                // Output source template content
                let template_path = format!("src/web/templates/{}", template_name);
                match std::fs::read_to_string(&template_path) {
                    Ok(content) => {
                        info!("Template content preview (first 100 chars): {}",
                             content.chars().take(100).collect::<String>());
                    },
                    Err(e) => error!("Could not read template file {}: {}", template_path, e),
                }

                Err((StatusCode::INTERNAL_SERVER_ERROR, format!("Template error: {}", e)))
            }
        }
    }

    /// Populate common navigation statistics into template context
    async fn populate_nav_stats(&self, db: &sea_orm::DatabaseConnection, context: &mut tera::Context) -> Result<(), DbErr> {
        // Get number of failed jobs
        let failed_jobs_count = solid_queue_failed_executions::Entity::find()
            .count(db)
            .await?;

        // Get number of in-progress jobs
        let in_progress_jobs_count = solid_queue_claimed_executions::Entity::find()
            .count(db)
            .await?;

        // Get number of scheduled jobs
        let scheduled_jobs_count = solid_queue_scheduled_executions::Entity::find()
            .count(db)
            .await?;

        // Get number of blocked jobs
        let blocked_jobs_count = solid_queue_blocked_executions::Entity::find()
            .count(db)
            .await?;

        // Get number of finished jobs
        let finished_jobs_count = solid_queue_jobs::Entity::find()
            .filter(solid_queue_jobs::Column::FinishedAt.is_not_null())
            .count(db)
            .await?;

        let failed_count = usize::try_from(failed_jobs_count).unwrap_or(0);
        let in_progress_count = usize::try_from(in_progress_jobs_count).unwrap_or(0);
        let scheduled_count = usize::try_from(scheduled_jobs_count).unwrap_or(0);
        let blocked_count = usize::try_from(blocked_jobs_count).unwrap_or(0);
        let finished_count = usize::try_from(finished_jobs_count).unwrap_or(0);

        context.insert("failed_jobs_count", &failed_count);
        context.insert("in_progress_jobs_count", &in_progress_count);
        context.insert("scheduled_jobs_count", &scheduled_count);
        context.insert("blocked_jobs_count", &blocked_count);
        context.insert("finished_jobs_count", &finished_count);

        Ok(())
    }

    /// Parse timestamp and format it in human-readable form
    fn format_timestamp(datetime: Result<chrono::NaiveDateTime, DbErr>, format: &str) -> String {
        match datetime {
            Ok(dt) => dt.format(format).to_string(),
            Err(_) => "Unknown time".to_string(),
        }
    }

    /// Format timestamp with default datetime format
    fn format_datetime(datetime: Result<chrono::NaiveDateTime, DbErr>) -> String {
        Self::format_timestamp(datetime, "%Y-%m-%d %H:%M:%S")
    }

    /// Format optional timestamp with default datetime format
    fn format_optional_datetime(datetime: Option<chrono::NaiveDateTime>) -> Option<String> {
        datetime.map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
    }

    /// Format chrono::NaiveDateTime directly
    fn format_naive_datetime(datetime: chrono::NaiveDateTime) -> String {
        datetime.format("%Y-%m-%d %H:%M:%S").to_string()
    }

    // Modify scheduled_jobs method to use these helper methods
    #[instrument(skip(state), fields(path = "/scheduled-jobs"))]
    async fn scheduled_jobs(
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

        let total_pages = ((total_count as f64) / (page_size as f64)).ceil() as usize;
        if total_pages > 0 && pagination.page > total_pages {
            return Err((StatusCode::NOT_FOUND, "Page not found".to_string()));
        }

        // Use abstract method to get all queue names and job classes
        let queue_names = state.get_queue_names(db)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        let job_classes = state.get_job_classes(db)
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

    // Modify in_progress_jobs method, use helper method
    #[instrument(skip(state), fields(path = "/in-progress-jobs"))]
    async fn in_progress_jobs(
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

        // Get claimed (in-progress) jobs
        let claimed_executions = crate::entities::solid_queue_claimed_executions::Entity::find()
            .offset(offset as u64)
            .limit(page_size as u64)
            .all(db)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        // Get process information
        let processes = crate::entities::solid_queue_processes::Entity::find()
            .all(db)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        // Create a vector to store in-progress job information
        let mut in_progress_jobs: Vec<InProgressJobInfo> = Vec::with_capacity(claimed_executions.len());

        // Get current time, for calculating runtime
        let now = chrono::Utc::now().naive_utc();

        // Get job information for each in-progress execution
        for execution in claimed_executions {
            if let Ok(Some(job)) = solid_queue_jobs::Entity::find_by_id(execution.job_id).one(db).await {
                // Find process information
                let worker_id = match execution.process_id {
                    Some(pid) => {
                        let process = processes.iter().find(|p| p.id == pid);
                        match process {
                            Some(p) => format!("{} ({})", p.name, p.hostname.clone().unwrap_or_default()),
                            None => format!("Unknown (PID: {})", pid),
                        }
                    },
                    None => "Unknown".to_string(),
                };

                // Calculate runtime
                let runtime = match now.signed_duration_since(execution.created_at) {
                    duration if duration.num_hours() >= 1 => {
                        format!("{}h {}m", duration.num_hours(), duration.num_minutes() % 60)
                    },
                    duration if duration.num_minutes() >= 1 => {
                        format!("{}m {}s", duration.num_minutes(), duration.num_seconds() % 60)
                    },
                    duration => {
                        format!("{}s", duration.num_seconds())
                    },
                };

                in_progress_jobs.push(InProgressJobInfo {
                    id: execution.id,
                    job_id: execution.job_id,
                    queue_name: job.queue_name.clone(),
                    class_name: job.class_name.clone(),
                    worker_id,
                    started_at: Self::format_naive_datetime(execution.created_at),
                    runtime,
                });
            }
        }

        debug!("Fetched in-progress jobs in {:?}", start.elapsed());
        info!("Found {} in-progress jobs", in_progress_jobs.len());

        // Get total number of in-progress jobs for pagination
        let start = Instant::now();

        // Execute SQL count query
        let total_count: i64 = crate::entities::solid_queue_claimed_executions::Entity::find()
            .select_only()
            .column_as(sea_orm::sea_query::Expr::expr(sea_orm::sea_query::Func::count(sea_orm::sea_query::Expr::cust("*"))), "count")
            .into_tuple()
            .one(db)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
            .unwrap_or(0);

        info!("Total in-progress jobs count: {}", total_count);

        let total_pages = ((total_count as f64) / (page_size as f64)).ceil() as usize;
        if total_pages > 0 && pagination.page > total_pages {
            return Err((StatusCode::NOT_FOUND, "Page not found".to_string()));
        }
        debug!("Fetched count in {:?}", start.elapsed());

        // Use abstract method to get all queue names and job classes
        let queue_names = state.get_queue_names(db)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        let job_classes = state.get_job_classes(db)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        let start = Instant::now();
        let mut context = tera::Context::new();
        context.insert("in_progress_jobs", &in_progress_jobs);
        context.insert("current_page_num", &pagination.page);
        context.insert("total_pages", &total_pages);
        context.insert("active_page", "in-progress-jobs");
        context.insert("queue_names", &queue_names);
        context.insert("job_classes", &job_classes);

        let html = state.render_template("in-progress-jobs.html", &mut context).await?;
        debug!("Template rendering completed in {:?}", start.elapsed());

        Ok(Html(html))
    }

    // Implement method to cancel a single in-progress job
    #[instrument(skip(state), fields(path = "/in-progress-jobs/:id/cancel"))]
    async fn cancel_in_progress_job(
        State(state): State<Arc<ControlPlane>>,
        Path(id): Path<i64>,
    ) -> impl IntoResponse {
        let db = state.ctx.get_db().await;
        let db = db.as_ref();

        // Use transaction to operate
        db.transaction::<_, (), DbErr>(|txn| {
            Box::pin(async move {
                // Find execution record to cancel
                let claimed_execution = crate::entities::solid_queue_claimed_executions::Entity::find_by_id(id)
                    .one(txn)
                    .await?;

                if let Some(execution) = claimed_execution {
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

                    // Delete claimed_execution record
                    crate::entities::solid_queue_claimed_executions::Entity::delete_by_id(id)
                        .exec(txn)
                        .await?;

                    info!("Cancelled in-progress job ID: {}", id);
                } else {
                    return Err(DbErr::Custom(format!("In-progress job with ID {} not found", id)));
                }

                Ok(())
            })
        })
        .await
        .map(|_| {
            info!("Cancelled in-progress job ID: {}", id);
            (StatusCode::SEE_OTHER, [("Location", "/in-progress-jobs")])
        })
        .map_err(|e| {
            error!("Failed to cancel in-progress job {}: {}", id, e);
            (StatusCode::INTERNAL_SERVER_ERROR, [("Location", "/in-progress-jobs")])
        })
    }

    // Implement blocked_jobs method, display all blocked jobs
    #[instrument(skip(state), fields(path = "/blocked-jobs"))]
    async fn blocked_jobs(
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

        // Get blocked jobs
        let blocked_executions = crate::entities::solid_queue_blocked_executions::Entity::find()
            .offset(offset as u64)
            .limit(page_size as u64)
            .all(db)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        // Create a vector to store blocked job information
        let mut blocked_jobs: Vec<BlockedJobInfo> = Vec::with_capacity(blocked_executions.len());

        // Get current time, for calculating waiting time
        let now = chrono::Utc::now().naive_utc();

        // Get job information for each blocked execution
        for execution in blocked_executions {
            if let Ok(Some(job)) = solid_queue_jobs::Entity::find_by_id(execution.job_id).one(db).await {
                // Calculate waiting time
                let waiting_time = match now.signed_duration_since(execution.created_at) {
                    duration if duration.num_hours() >= 1 => {
                        format!("{}h {}m", duration.num_hours(), duration.num_minutes() % 60)
                    },
                    duration if duration.num_minutes() >= 1 => {
                        format!("{}m {}s", duration.num_minutes(), duration.num_seconds() % 60)
                    },
                    duration => {
                        format!("{}s", duration.num_seconds())
                    },
                };

                blocked_jobs.push(BlockedJobInfo {
                    id: execution.id,
                    job_id: execution.job_id,
                    queue_name: execution.queue_name.clone(),
                    class_name: job.class_name.clone(),
                    concurrency_key: execution.concurrency_key.clone(),
                    created_at: Self::format_naive_datetime(execution.created_at),
                    waiting_time,
                    expires_at: Self::format_naive_datetime(execution.expires_at),
                });
            }
        }

        debug!("Fetched blocked jobs in {:?}", start.elapsed());
        info!("Found {} blocked jobs", blocked_jobs.len());

        // Get total number of blocked jobs for pagination
        let start = Instant::now();

        // Execute SQL count query
        let total_count: i64 = crate::entities::solid_queue_blocked_executions::Entity::find()
            .select_only()
            .column_as(sea_orm::sea_query::Expr::expr(sea_orm::sea_query::Func::count(sea_orm::sea_query::Expr::cust("*"))), "count")
            .into_tuple()
            .one(db)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
            .unwrap_or(0);

        info!("Total blocked jobs count: {}", total_count);

        let total_pages = ((total_count as f64) / (page_size as f64)).ceil() as usize;
        if total_pages > 0 && pagination.page > total_pages {
            return Err((StatusCode::NOT_FOUND, "Page not found".to_string()));
        }
        debug!("Fetched count in {:?}", start.elapsed());

        // Use abstract method to get all queue names and job classes
        let queue_names = state.get_queue_names(db)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        let job_classes = state.get_job_classes(db)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        let start = Instant::now();
        let mut context = tera::Context::new();
        context.insert("blocked_jobs", &blocked_jobs);
        context.insert("current_page_num", &pagination.page);
        context.insert("total_pages", &total_pages);
        context.insert("active_page", "blocked-jobs");
        context.insert("queue_names", &queue_names);
        context.insert("job_classes", &job_classes);

        let html = state.render_template("blocked-jobs.html", &mut context).await?;
        debug!("Template rendering completed in {:?}", start.elapsed());

        Ok(Html(html))
    }

    // Implement method to unblock a single blocked job
    #[instrument(skip(state), fields(path = "/blocked-jobs/:id/unblock"))]
    async fn unblock_job(
        State(state): State<Arc<ControlPlane>>,
        Path(id): Path<i64>,
    ) -> impl IntoResponse {
        let db = state.ctx.get_db().await;
        let db = db.as_ref();

        // Use transaction to operate
        db.transaction::<_, (), DbErr>(|txn| {
            Box::pin(async move {
                // Find blocked execution
                let _blocked_execution = solid_queue_blocked_executions::Entity::find_by_id(id)
                    .one(txn)
                    .await?
                    .ok_or_else(|| DbErr::Custom(format!("Blocked execution with ID {} not found", id)))?;

                // Delete blocked execution
                solid_queue_blocked_executions::Entity::delete_by_id(id)
                    .exec(txn)
                    .await?;

                info!("Unblocked job ID: {}", id);
                Ok(())
            })
        })
        .await
        .map(|_| {
            (StatusCode::SEE_OTHER, "/blocked-jobs".to_string())
        })
        .map_err(|e| {
            error!("Failed to unblock job {}: {}", id, e);
            match e.to_string() {
                s if s.contains("not found") => (StatusCode::NOT_FOUND, "/blocked-jobs".to_string()),
                _ => (StatusCode::INTERNAL_SERVER_ERROR, "/blocked-jobs".to_string())
            }
        })
    }

    // Implement method to cancel a single blocked job
    #[instrument(skip(state), fields(path = "/blocked-jobs/:id/cancel"))]
    async fn cancel_blocked_job(
        State(state): State<Arc<ControlPlane>>,
        Path(id): Path<i64>,
    ) -> impl IntoResponse {
        let db = state.ctx.get_db().await;
        let db = db.as_ref();

        // Use transaction to operate
        db.transaction::<_, (), DbErr>(|txn| {
            Box::pin(async move {
                // Find blocked execution
                let blocked_execution = solid_queue_blocked_executions::Entity::find_by_id(id)
                    .one(txn)
                    .await?
                    .ok_or_else(|| DbErr::Custom(format!("Blocked execution with ID {} not found", id)))?;

                let job_id = blocked_execution.job_id;

                // Delete blocked execution
                solid_queue_blocked_executions::Entity::delete_by_id(id)
                    .exec(txn)
                    .await?;

                // Delete related job
                solid_queue_jobs::Entity::delete_by_id(job_id)
                    .exec(txn)
                    .await?;

                info!("Cancelled blocked job ID: {}, job ID: {}", id, job_id);
                Ok(())
            })
        })
        .await
        .map(|_| {
            (StatusCode::SEE_OTHER, "/blocked-jobs".to_string())
        })
        .map_err(|e| {
            error!("Failed to cancel blocked job {}: {}", id, e);
            match e.to_string() {
                s if s.contains("not found") => (StatusCode::NOT_FOUND, "/blocked-jobs".to_string()),
                _ => (StatusCode::INTERNAL_SERVER_ERROR, "/blocked-jobs".to_string())
            }
        })
    }

    // Implement method to unblock all blocked jobs
    #[instrument(skip(state), fields(path = "/blocked-jobs/all/unblock"))]
    async fn unblock_all_jobs(
        State(state): State<Arc<ControlPlane>>,
    ) -> impl IntoResponse {
        let db = state.ctx.get_db().await;
        let db = db.as_ref();

        // Use transaction to operate
        db.transaction::<_, u64, DbErr>(|txn| {
            Box::pin(async move {
                // Delete all blocked executions
                let result = solid_queue_blocked_executions::Entity::delete_many()
                    .exec(txn)
                    .await?;

                let count = result.rows_affected;
                info!("Unblocked all {} blocked jobs", count);
                Ok(count)
            })
        })
        .await
        .map(|_count| {
            (StatusCode::SEE_OTHER, "/blocked-jobs".to_string())
        })
        .map_err(|e| {
            error!("Failed to unblock all jobs: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, "/blocked-jobs".to_string())
        })
    }

    // Implement method to cancel all in-progress jobs
    #[instrument(skip(state), fields(path = "/in-progress-jobs/all/cancel"))]
    async fn cancel_all_in_progress_jobs(
        State(state): State<Arc<ControlPlane>>,
    ) -> impl IntoResponse {
        let db = state.ctx.get_db().await;
        let db = db.as_ref();

        // Use transaction to operate
        db.transaction::<_, u64, DbErr>(|txn| {
            Box::pin(async move {
                // Get all in-progress jobs
                let claimed_executions = crate::entities::solid_queue_claimed_executions::Entity::find()
                    .all(txn)
                    .await?;

                if claimed_executions.is_empty() {
                    return Ok(0);
                }

                let job_ids: Vec<i64> = claimed_executions.iter()
                    .map(|execution| execution.job_id)
                    .collect();

                // Update all related jobs to completed status
                let now = chrono::Utc::now().naive_utc();

                // Use batch update
                let update_sql = r#"
                    UPDATE solid_queue_jobs
                    SET finished_at = $1, updated_at = $1
                    WHERE id = ANY($2)
                "#;

                let stmt = sea_orm::Statement::from_sql_and_values(
                    sea_orm::DbBackend::Postgres,
                    update_sql,
                    [now.into(), job_ids.into()]
                );

                txn.execute(stmt).await?;

                // Delete all claimed_execution records
                let delete_result = crate::entities::solid_queue_claimed_executions::Entity::delete_many()
                    .exec(txn)
                    .await?;

                let count = delete_result.rows_affected;
                info!("Cancelled all {} in-progress jobs", count);
                Ok(count)
            })
        })
        .await
        .map(|count| {
            info!("Cancelled all {} in-progress jobs", count);
            (StatusCode::SEE_OTHER, [("Location", "/in-progress-jobs")])
        })
        .map_err(|e| {
            error!("Failed to cancel all in-progress jobs: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, [("Location", "/in-progress-jobs")])
        })
    }

    #[instrument(skip(state), fields(path = "/queues/:name"))]
    async fn queue_details(
        State(state): State<Arc<ControlPlane>>,
        Path(queue_name): Path<String>,
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

        // Get queue status information
        let is_paused = solid_queue_pauses::Entity::find()
            .filter(solid_queue_pauses::Column::QueueName.eq(queue_name.clone()))
            .count(db)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
            > 0;

        let status = if is_paused { "paused" } else { "active" };

        // Get total number of uncompleted jobs in queue
        let total_count = solid_queue_jobs::Entity::find()
            .filter(solid_queue_jobs::Column::QueueName.eq(queue_name.clone()))
            .filter(solid_queue_jobs::Column::FinishedAt.is_null())
            .count(db)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        // Number of waiting jobs (uncompleted and not associated with other tables)
        let waiting_count = db
            .query_one(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "SELECT COUNT(*) AS count
                 FROM solid_queue_jobs j
                 WHERE j.queue_name = $1
                 AND j.finished_at IS NULL
                 AND NOT EXISTS (SELECT 1 FROM solid_queue_claimed_executions c WHERE c.job_id = j.id)
                 AND NOT EXISTS (SELECT 1 FROM solid_queue_failed_executions f WHERE f.job_id = j.id)
                 AND NOT EXISTS (SELECT 1 FROM solid_queue_scheduled_executions s WHERE s.job_id = j.id)",
                [Value::from(queue_name.clone())]
            ))
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
            .map(|row| row.try_get::<i64>("", "count").unwrap_or(0))
            .unwrap_or(0);

        // Number of jobs being processed (using claimed_executions table, and not completed)
        let processing_count = db
            .query_one(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "SELECT COUNT(*) AS count
                 FROM solid_queue_claimed_executions c
                 JOIN solid_queue_jobs j ON c.job_id = j.id
                 WHERE j.queue_name = $1
                 AND j.finished_at IS NULL",
                [Value::from(queue_name.clone())]
            ))
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
            .map(|row| row.try_get::<i64>("", "count").unwrap_or(0))
            .unwrap_or(0);

        // Get jobs in queue, including job status (only display uncompleted jobs)
        let jobs_result = db
            .query_all(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "SELECT
                    j.id,
                    j.class_name,
                    j.created_at,
                    CASE
                        WHEN c.id IS NOT NULL THEN 'processing'
                        WHEN f.id IS NOT NULL THEN 'failed'
                        WHEN s.id IS NOT NULL THEN 'scheduled'
                        ELSE 'pending'
                    END AS status
                FROM solid_queue_jobs j
                LEFT JOIN solid_queue_claimed_executions c ON j.id = c.job_id
                LEFT JOIN solid_queue_failed_executions f ON j.id = f.job_id
                LEFT JOIN solid_queue_scheduled_executions s ON j.id = s.job_id
                WHERE j.queue_name = $1
                AND j.finished_at IS NULL
                ORDER BY j.created_at DESC
                LIMIT $2 OFFSET $3",
                [
                    Value::from(queue_name.clone()),
                    Value::from(page_size as i32),
                    Value::from(offset as i32)
                ]
            ))
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        let mut jobs = Vec::with_capacity(jobs_result.len());
        for row in jobs_result {
            let id: i64 = row.try_get("", "id").unwrap_or_default();
            let class_name: String = row.try_get("", "class_name").unwrap_or_default();
            let status: String = row.try_get("", "status").unwrap_or_default();

            // Get time and record debug information
            // Simplified time parsing code, consistent with other methods
            let created_at_str = Self::format_datetime(row.try_get::<chrono::NaiveDateTime>("", "created_at"));

            jobs.push(QueueJobInfo {
                id,
                class_name,
                status,
                created_at: created_at_str,
                execution_id: None,
            });
        }

        debug!("Fetched queue details in {:?}", start.elapsed());

        // Calculate pagination - use number of uncompleted jobs
        let total_pages = ((total_count as f64) / (page_size as f64)).ceil() as usize;
        if total_pages > 0 && pagination.page > total_pages {
            return Err((StatusCode::NOT_FOUND, "Page not found".to_string()));
        }

        // Use SQL to count failed jobs (uncompleted)
        let failed_count = db
            .query_one(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "SELECT COUNT(*) AS count
                 FROM solid_queue_failed_executions f
                 JOIN solid_queue_jobs j ON f.job_id = j.id
                 WHERE j.queue_name = $1
                 AND j.finished_at IS NULL",
                [Value::from(queue_name.clone())]
            ))
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
            .map(|row| row.try_get::<i64>("", "count").unwrap_or(0))
            .unwrap_or(0);

        // Use SQL to count scheduled jobs (uncompleted)
        let scheduled_count = db
            .query_one(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "SELECT COUNT(*) AS count
                 FROM solid_queue_scheduled_executions s
                 JOIN solid_queue_jobs j ON s.job_id = j.id
                 WHERE j.queue_name = $1
                 AND j.finished_at IS NULL",
                [Value::from(queue_name.clone())]
            ))
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
            .map(|row| row.try_get::<i64>("", "count").unwrap_or(0))
            .unwrap_or(0);

        let start = Instant::now();
        let mut context = tera::Context::new();
        context.insert("queue_name", &queue_name);
        context.insert("status", &status);
        context.insert("jobs", &jobs);
        context.insert("waiting_count", &(waiting_count as usize));
        context.insert("processing_count", &(processing_count as usize));
        context.insert("scheduled_count", &(scheduled_count as usize));
        context.insert("failed_count", &(failed_count as usize));
        context.insert("current_page_num", &pagination.page);
        context.insert("total_pages", &total_pages);
        context.insert("total_count", &(total_count as usize));
        context.insert("active_page", "queues");

        let html = state.render_template("queue-details.html", &mut context).await?;
        debug!("Template rendering completed in {:?}", start.elapsed());

        Ok(Html(html))
    }

    // Implement finished_jobs method, display all completed jobs
    #[instrument(skip(state), fields(path = "/finished-jobs"))]
    async fn finished_jobs(
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

        // Get completed jobs
        let finished_jobs_query = solid_queue_jobs::Entity::find()
            .filter(solid_queue_jobs::Column::FinishedAt.is_not_null())
            .order_by_desc(solid_queue_jobs::Column::FinishedAt)
            .offset(offset as u64)
            .limit(page_size as u64);

        let finished_jobs_models = finished_jobs_query
            .all(db)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        // Create a vector to store completed job information
        let mut finished_jobs: Vec<FinishedJobInfo> = Vec::with_capacity(finished_jobs_models.len());

        // Get information for each completed job
        for job in finished_jobs_models {
            if let Some(finished_at) = job.finished_at {
                // Calculate runtime
                let runtime = match finished_at.signed_duration_since(job.created_at) {
                    duration if duration.num_hours() >= 1 => {
                        format!("{}h {}m", duration.num_hours(), duration.num_minutes() % 60)
                    },
                    duration if duration.num_minutes() >= 1 => {
                        format!("{}m {}s", duration.num_minutes(), duration.num_seconds() % 60)
                    },
                    duration => {
                        format!("{}s", duration.num_seconds())
                    },
                };

                finished_jobs.push(FinishedJobInfo {
                    id: job.id,
                    queue_name: job.queue_name,
                    class_name: job.class_name,
                    created_at: Self::format_naive_datetime(job.created_at),
                    finished_at: Self::format_naive_datetime(finished_at),
                    runtime,
                });
            }
        }

        debug!("Fetched finished jobs in {:?}", start.elapsed());
        info!("Found {} finished jobs", finished_jobs.len());

        // Get total number of completed jobs for pagination
        let start = Instant::now();

        // Execute SQL count query
        let total_count: i64 = solid_queue_jobs::Entity::find()
            .filter(solid_queue_jobs::Column::FinishedAt.is_not_null())
            .select_only()
            .column_as(sea_orm::sea_query::Expr::expr(sea_orm::sea_query::Func::count(sea_orm::sea_query::Expr::cust("*"))), "count")
            .into_tuple()
            .one(db)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
            .unwrap_or(0);

        info!("Total finished jobs count: {}", total_count);

        let total_pages = ((total_count as f64) / (page_size as f64)).ceil() as usize;
        if total_pages > 0 && pagination.page > total_pages {
            return Err((StatusCode::NOT_FOUND, "Page not found".to_string()));
        }
        debug!("Fetched count in {:?}", start.elapsed());

        // Use abstract method to get all queue names and job classes
        let queue_names = state.get_queue_names(db)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        let job_classes = state.get_job_classes(db)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        let start = Instant::now();
        let mut context = tera::Context::new();
        context.insert("finished_jobs", &finished_jobs);
        context.insert("current_page_num", &pagination.page);
        context.insert("total_pages", &total_pages);
        context.insert("active_page", "finished-jobs");
        context.insert("queue_names", &queue_names);
        context.insert("job_classes", &job_classes);

        let html = state.render_template("finished-jobs.html", &mut context).await?;
        debug!("Template rendering completed in {:?}", start.elapsed());

        Ok(Html(html))
    }

    // Implement stats method, specifically for returning statistics
    async fn stats(
        State(state): State<Arc<ControlPlane>>,
        _req: axum::http::Request<axum::body::Body>,
    ) -> Result<impl IntoResponse, (StatusCode, String)> {
        let start = Instant::now();
        let db = state.ctx.get_db().await;
        let db = db.as_ref();
        debug!("Database connection obtained in {:?}", start.elapsed());

        let mut context = tera::Context::new();

        // Use populate_nav_stats method to fill statistics
        state.populate_nav_stats(db, &mut context)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        // Return Turbo Stream format response directly
        let html = state.render_template("stats.html", &mut context).await?;

        // Set correct Content-Type
        let response = axum::response::Response::builder()
            .header("Content-Type", "text/vnd.turbo-stream.html")
            .body(axum::body::Body::from(html))
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        Ok(response)
    }

    // Implement workers method, used to display all worker processes
    #[instrument(skip(state), fields(path = "/workers"))]
    async fn workers(
        State(state): State<Arc<ControlPlane>>,
    ) -> Result<Html<String>, (StatusCode, String)> {
        let start = Instant::now();
        let db = state.ctx.get_db().await;
        let db = db.as_ref();
        debug!("Database connection obtained in {:?}", start.elapsed());

        // Query all worker processes
        let workers = solid_queue_processes::Entity::find()
            .order_by(solid_queue_processes::Column::LastHeartbeatAt, Order::Desc)
            .all(db)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        // Calculate time since last heartbeat for each worker
        let workers_info: Vec<WorkerInfo> = workers.into_iter().map(|worker| {
            let last_heartbeat = worker.last_heartbeat_at;
            let now = chrono::Utc::now().naive_utc();
            // Calculate time difference (seconds)
            let duration = now.signed_duration_since(last_heartbeat);
            let seconds_since_heartbeat = duration.num_seconds();

            // If heartbeat is not received for more than 30 seconds, consider worker dead
            let status = if seconds_since_heartbeat > 30 {
                "dead"
            } else {
                "alive"
            };

            WorkerInfo {
                id: worker.id,
                name: worker.name,
                kind: worker.kind,
                hostname: worker.hostname,
                pid: worker.pid,
                last_heartbeat_at: Self::format_naive_datetime(last_heartbeat),
                seconds_since_heartbeat,
                status: status.to_string(),
            }
        }).collect();

        let mut context = tera::Context::new();
        context.insert("workers", &workers_info);
        context.insert("active_page", "workers");

        let html = state.render_template("workers.html", &mut context).await?;

        Ok(Html(html))
    }

    // Add job details page controller method
    #[instrument(skip(state), fields(path = "/jobs/:id"))]
    async fn job_details(
        State(state): State<Arc<ControlPlane>>,
        Path(id): Path<i64>,
    ) -> Result<Html<String>, (StatusCode, String)> {
        let start = Instant::now();
        let db = state.ctx.get_db().await;
        let db = db.as_ref();
        debug!("Database connection obtained in {:?}", start.elapsed());

        let start = Instant::now();

        // Get job basic information
        let job_result = db
            .query_one(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "SELECT
                    j.id,
                    j.queue_name,
                    j.class_name,
                    j.created_at,
                    j.finished_at,
                    j.arguments,
                    CASE
                        WHEN j.finished_at IS NOT NULL THEN 'finished'
                        WHEN c.id IS NOT NULL THEN 'processing'
                        WHEN f.id IS NOT NULL THEN 'failed'
                        WHEN s.id IS NOT NULL THEN 'scheduled'
                        WHEN b.id IS NOT NULL THEN 'blocked'
                        ELSE 'pending'
                    END AS status,
                    c.id as claimed_id,
                    f.id as failed_id,
                    s.id as scheduled_id,
                    b.id as blocked_id,
                    f.error as error_message,
                    s.scheduled_at,
                    b.concurrency_key,
                    b.expires_at,
                    c.process_id
                FROM solid_queue_jobs j
                LEFT JOIN solid_queue_claimed_executions c ON j.id = c.job_id
                LEFT JOIN solid_queue_failed_executions f ON j.id = f.job_id
                LEFT JOIN solid_queue_scheduled_executions s ON j.id = s.job_id
                LEFT JOIN solid_queue_blocked_executions b ON j.id = b.job_id
                WHERE j.id = $1",
                [Value::from(id)]
            ))
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        if let Some(row) = job_result {
            // Parse basic information
            let job_id: i64 = row.try_get("", "id").unwrap_or_default();
            let queue_name: String = row.try_get("", "queue_name").unwrap_or_default();
            let class_name: String = row.try_get("", "class_name").unwrap_or_default();
            let status: String = row.try_get("", "status").unwrap_or_default();
            let arguments: Option<String> = row.try_get("", "arguments").ok();
            let error: Option<String> = row.try_get("", "error_message").ok();

            // Parse creation time
            let created_at = Self::format_datetime(row.try_get::<chrono::NaiveDateTime>("", "created_at"));

            // Get completion time (if any)
            let finished_at = match row.try_get::<Option<chrono::NaiveDateTime>>("", "finished_at") {
                Ok(dt_opt) => Self::format_optional_datetime(dt_opt),
                _ => None,
            };

            // Calculate runtime (if completed)
            let runtime = if let Some(finished) = row.try_get::<Option<chrono::NaiveDateTime>>("", "finished_at").ok().flatten() {
                if let Ok(created) = row.try_get::<chrono::NaiveDateTime>("", "created_at") {
                    let duration = finished.signed_duration_since(created);
                    Some(format!("{} seconds", duration.num_seconds()))
                } else {
                    None
                }
            } else {
                None
            };

            // Get specific information based on job status
            let mut job_details = JobDetailsInfo {
                id: job_id,
                queue_name,
                class_name,
                status: status.clone(),
                created_at,
                failed_at: None,
                scheduled_at: None,
                scheduled_in: None,
                concurrency_key: None,
                waiting_time: None,
                expires_at: None,
                started_at: None,
                runtime,
                worker_id: None,
                finished_at,
                error,
                backtrace: None,
                arguments,
                context: None,
                execution_id: None,
                execution_history: None,
            };

            // Get specific detailed information based on status
            match status.as_str() {
                "failed" => {
                    let failed_id: i64 = row.try_get("", "failed_id").unwrap_or_default();
                    job_details.execution_id = Some(failed_id);

                    // Get failure information
                    if let Ok(failed_info) = db
                        .query_one(Statement::from_sql_and_values(
                            DbBackend::Postgres,
                            "SELECT error, failed_at FROM solid_queue_failed_executions WHERE id = $1",
                            [Value::from(failed_id)]
                        ))
                        .await
                    {
                        if let Some(row) = failed_info {
                            job_details.error = row.try_get("", "error").ok();

                            // Parse failure time
                            if let Ok(dt) = row.try_get::<chrono::NaiveDateTime>("", "failed_at") {
                                job_details.failed_at = Some(Self::format_naive_datetime(dt));
                            }
                        }
                    }
                },
                "scheduled" => {
                    let scheduled_id: i64 = row.try_get("", "scheduled_id").unwrap_or_default();
                    job_details.execution_id = Some(scheduled_id);

                    // Parse scheduled execution time
                    if let Ok(dt) = row.try_get::<chrono::NaiveDateTime>("", "scheduled_at") {
                        job_details.scheduled_at = Some(Self::format_naive_datetime(dt));

                        // Calculate how much time is left to execute
                        let now = chrono::Utc::now().naive_utc();
                        if dt > now {
                            let duration = dt.signed_duration_since(now);
                            let scheduled_in = if duration.num_hours() > 0 {
                                format!("in {}h {}m", duration.num_hours(), duration.num_minutes() % 60)
                            } else if duration.num_minutes() > 0 {
                                format!("in {}m {}s", duration.num_minutes(), duration.num_seconds() % 60)
                            } else {
                                format!("in {}s", duration.num_seconds())
                            };
                            job_details.scheduled_in = Some(scheduled_in);
                        } else {
                            job_details.scheduled_in = Some("overdue".to_string());
                        }
                    }
                },
                "blocked" => {
                    let blocked_id: i64 = row.try_get("", "blocked_id").unwrap_or_default();
                    job_details.execution_id = Some(blocked_id);

                    // Get blocked information
                    job_details.concurrency_key = row.try_get("", "concurrency_key").ok();

                    // Parse expiration time
                    if let Ok(dt) = row.try_get::<chrono::NaiveDateTime>("", "expires_at") {
                        job_details.expires_at = Some(Self::format_naive_datetime(dt));

                        // Calculate waiting time
                        if let Ok(created) = row.try_get::<chrono::NaiveDateTime>("", "created_at") {
                            let duration = chrono::Utc::now().naive_utc().signed_duration_since(created);
                            job_details.waiting_time = Some(format!("{} seconds", duration.num_seconds()));
                        }
                    }
                },
                "processing" => {
                    let claimed_id: i64 = row.try_get("", "claimed_id").unwrap_or_default();
                    job_details.execution_id = Some(claimed_id);

                    // Get processing information
                    if let Ok(process_id) = row.try_get::<i64>("", "process_id") {
                        // Get worker process information
                        if let Ok(worker_info) = db
                            .query_one(Statement::from_sql_and_values(
                                DbBackend::Postgres,
                                "SELECT name FROM solid_queue_processes WHERE id = $1",
                                [Value::from(process_id)]
                            ))
                            .await
                        {
                            if let Some(row) = worker_info {
                                job_details.worker_id = row.try_get("", "name").ok();
                            }
                        }
                    }

                    // Get start time and runtime
                    if let Ok(claimed_info) = db
                        .query_one(Statement::from_sql_and_values(
                            DbBackend::Postgres,
                            "SELECT created_at FROM solid_queue_claimed_executions WHERE id = $1",
                            [Value::from(claimed_id)]
                        ))
                        .await
                    {
                        if let Some(row) = claimed_info {
                            if let Ok(dt) = row.try_get::<chrono::NaiveDateTime>("", "created_at") {
                                job_details.started_at = Some(Self::format_naive_datetime(dt));

                                // Calculate current runtime
                                let now = chrono::Utc::now().naive_utc();
                                let duration = now.signed_duration_since(dt);
                                job_details.runtime = Some(format!("{} seconds", duration.num_seconds()));
                            }
                        }
                    }
                },
                _ => {}
            }

            let mut context = tera::Context::new();
            context.insert("job", &job_details);
            context.insert("active_page", match status.as_str() {
                "failed" => "failed_jobs",
                "scheduled" => "scheduled_jobs",
                "blocked" => "blocked_jobs",
                "processing" => "in_progress_jobs",
                "finished" => "finished_jobs",
                _ => "queues",
            });

            let html = state.render_template("job-details.html", &mut context).await?;
            debug!("Template rendering completed in {:?}", start.elapsed());

            Ok(Html(html))
        } else {
            Err((StatusCode::NOT_FOUND, format!("Job with ID {} not found", id)))
        }
    }

    // Add controller method to cancel scheduled job
    #[instrument(skip(state), fields(path = "/scheduled-jobs/:id/cancel"))]
    async fn cancel_scheduled_job(
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

pub trait ControlPlaneExt {
    fn start_control_plane(&self, addr: String) -> tokio::task::JoinHandle<()>;
}

impl ControlPlaneExt for Arc<AppContext> {
    fn start_control_plane(&self, addr: String) -> tokio::task::JoinHandle<()> {
        let ctx = self.clone();
        let addr = addr.clone();
        let app = ControlPlane::new(ctx.clone()).router();
        let graceful_shutdown = ctx.graceful_shutdown.clone();
        let force_quit = ctx.force_quit.clone();

        tokio::spawn(async move {
            info!("Starting control plane on http://{}", addr);

            let listener = match tokio::net::TcpListener::bind(&addr).await {
                Ok(l) => {
                    debug!("Control plane successfully bound to {}", addr);
                    l
                },
                Err(e) => {
                    error!("Failed to bind control plane to {}: {}", addr, e);
                    return;
                }
            };

            let server = axum::serve(listener, app);

            let shutdown_future = async move {
                tokio::select! {
                    _ = graceful_shutdown.cancelled() => {
                        debug!("Control plane received graceful shutdown signal");
                        sleep(Duration::from_secs(1)).await;
                        debug!("Control plane graceful shutdown completed");
                    }
                    _ = force_quit.cancelled() => {
                        warn!("Control plane received force quit signal");
                    }
                }
            };

            match server.with_graceful_shutdown(shutdown_future).await {
                Ok(_) => debug!("Control plane server shut down successfully"),
                Err(e) => error!("Control plane server error during shutdown: {}", e),
            }
        })
    }
}
