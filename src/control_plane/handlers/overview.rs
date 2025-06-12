use std::sync::Arc;
use std::time::Instant;
use std::collections::HashMap;
use axum::{
    extract::{State, Query},
    response::Html,
    http::StatusCode,
};
use sea_orm::{EntityTrait, QueryFilter, ColumnTrait, ConnectionTrait, Statement, DbBackend, PaginatorTrait};
use chrono::NaiveDateTime;
use tracing::{debug, instrument};

use crate::entities::{solid_queue_jobs, solid_queue_processes, solid_queue_failed_executions, solid_queue_pauses};
use crate::control_plane::ControlPlane;

impl ControlPlane {
    #[instrument(skip(state), fields(path = "/"))]
    pub async fn overview(
        State(state): State<Arc<ControlPlane>>,
        Query(params): Query<HashMap<String, String>>,
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
}
