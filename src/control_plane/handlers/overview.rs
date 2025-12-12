use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::Html,
};
use chrono::NaiveDateTime;
use sea_orm::sea_query::{
    Alias, Expr, Func, MysqlQueryBuilder, PostgresQueryBuilder, Query as SeaQuery,
    SqliteQueryBuilder,
};
use sea_orm::Order;
use sea_orm::{
    ColumnTrait, ConnectionTrait, DbBackend, EntityTrait, PaginatorTrait, QueryFilter, Statement,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, instrument};

use crate::control_plane::ControlPlane;
use crate::entities::{quebec_failed_executions, quebec_jobs, quebec_pauses, quebec_processes};

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
        let hours: i64 = params
            .get("hours")
            .and_then(|h| h.parse().ok())
            .unwrap_or(24);

        let now = chrono::Utc::now().naive_utc();
        let period_start = now - chrono::Duration::hours(hours);
        let previous_period_start = period_start - chrono::Duration::hours(hours);

        // Get total number of completed jobs in current period
        let total_jobs_processed = quebec_jobs::Entity::find()
            .filter(quebec_jobs::Column::FinishedAt.is_not_null())
            .filter(quebec_jobs::Column::FinishedAt.gt(period_start))
            .count(db)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        // Get total number of completed jobs in previous period for calculating change rate
        let previous_jobs_processed = quebec_jobs::Entity::find()
            .filter(quebec_jobs::Column::FinishedAt.is_not_null())
            .filter(quebec_jobs::Column::FinishedAt.gt(previous_period_start))
            .filter(quebec_jobs::Column::FinishedAt.lte(period_start))
            .count(db)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        // Calculate change rate of job processing count
        let jobs_processed_change = if previous_jobs_processed > 0 {
            ((total_jobs_processed as f64 - previous_jobs_processed as f64)
                / previous_jobs_processed as f64
                * 100.0)
                .round() as i32
        } else {
            0
        };

        // Get average processing time of jobs in current period
        let table_config = &state.ctx.table_config;
        let jobs_table = Alias::new(&table_config.jobs);

        let avg_duration_query = SeaQuery::select()
            .expr_as(
                Func::avg(Expr::cust("EXTRACT(EPOCH FROM (finished_at - created_at))")),
                Alias::new("avg_duration"),
            )
            .from(jobs_table)
            .and_where(Expr::col(Alias::new("finished_at")).is_not_null())
            .and_where(Expr::col(Alias::new("finished_at")).gt(period_start))
            .to_owned();

        let (avg_duration_sql, avg_duration_values) = match db.get_database_backend() {
            DbBackend::Postgres => avg_duration_query.build(PostgresQueryBuilder),
            DbBackend::Sqlite => avg_duration_query.build(SqliteQueryBuilder),
            DbBackend::MySql => avg_duration_query.build(MysqlQueryBuilder),
        };

        let avg_duration_stmt = Statement::from_sql_and_values(
            db.get_database_backend(),
            &avg_duration_sql,
            avg_duration_values,
        );

        let avg_duration: Option<f64> = db
            .query_one(avg_duration_stmt)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
            .map(|row| row.try_get("", "avg_duration").unwrap_or(0.0));

        // Get average processing time of jobs in previous period
        let prev_avg_duration_query = SeaQuery::select()
            .expr_as(
                Func::avg(Expr::cust("EXTRACT(EPOCH FROM (finished_at - created_at))")),
                Alias::new("avg_duration"),
            )
            .from(Alias::new(&table_config.jobs))
            .and_where(Expr::col(Alias::new("finished_at")).is_not_null())
            .and_where(Expr::col(Alias::new("finished_at")).gt(previous_period_start))
            .and_where(Expr::col(Alias::new("finished_at")).lte(period_start))
            .to_owned();

        let (prev_avg_duration_sql, prev_avg_duration_values) = match db.get_database_backend() {
            DbBackend::Postgres => prev_avg_duration_query.build(PostgresQueryBuilder),
            DbBackend::Sqlite => prev_avg_duration_query.build(SqliteQueryBuilder),
            DbBackend::MySql => prev_avg_duration_query.build(MysqlQueryBuilder),
        };

        let prev_avg_duration_stmt = Statement::from_sql_and_values(
            db.get_database_backend(),
            &prev_avg_duration_sql,
            prev_avg_duration_values,
        );

        let prev_avg_duration: Option<f64> = db
            .query_one(prev_avg_duration_stmt)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
            .map(|row| row.try_get("", "avg_duration").unwrap_or(0.0));

        // Calculate change rate of average processing time
        let avg_duration_change = match (avg_duration, prev_avg_duration) {
            (Some(curr), Some(prev)) if prev > 0.0 => ((curr - prev) / prev * 100.0).round() as i32,
            _ => 0,
        };

        // Format average processing time
        let avg_job_duration = match avg_duration {
            Some(secs) if secs >= 3600.0 => {
                format!("{:.1}h", secs / 3600.0)
            }
            Some(secs) if secs >= 60.0 => {
                format!("{:.1}m", secs / 60.0)
            }
            Some(secs) => {
                format!("{:.1}s", secs)
            }
            None => "N/A".to_string(),
        };

        // Get number of active workers
        let active_workers = quebec_processes::Entity::find()
            .filter(
                quebec_processes::Column::LastHeartbeatAt.gt(now - chrono::Duration::seconds(30)),
            )
            .count(db)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        // Get number of active workers in previous period (simplified here, should query historical data)
        let prev_active_workers = active_workers; // Assume no change, should get from historical data

        // Calculate change in number of active workers
        let active_workers_change = active_workers as i32 - prev_active_workers as i32;

        // Calculate failure rate
        let failed_jobs = quebec_failed_executions::Entity::find()
            .filter(quebec_failed_executions::Column::CreatedAt.gt(period_start))
            .count(db)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        let total_jobs = quebec_jobs::Entity::find()
            .filter(quebec_jobs::Column::CreatedAt.gt(period_start))
            .count(db)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        let failed_jobs_rate = if total_jobs > 0 {
            ((failed_jobs as f64 / total_jobs as f64) * 100.0).round() as i32
        } else {
            0
        };

        // Get failure rate of previous period
        let prev_failed_jobs = quebec_failed_executions::Entity::find()
            .filter(quebec_failed_executions::Column::CreatedAt.gt(previous_period_start))
            .filter(quebec_failed_executions::Column::CreatedAt.lte(period_start))
            .count(db)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        let prev_total_jobs = quebec_jobs::Entity::find()
            .filter(quebec_jobs::Column::CreatedAt.gt(previous_period_start))
            .filter(quebec_jobs::Column::CreatedAt.lte(period_start))
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
        } else if hours <= 168 {
            // 7 days
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
            let period_jobs = quebec_jobs::Entity::find()
                .filter(quebec_jobs::Column::FinishedAt.is_not_null())
                .filter(quebec_jobs::Column::FinishedAt.gt(start_time))
                .filter(quebec_jobs::Column::FinishedAt.lte(end_time))
                .count(db)
                .await
                .unwrap_or(0);

            jobs_processed_data.push(period_jobs);
        }

        // Reverse arrays to display in chronological order
        time_labels.reverse();
        jobs_processed_data.reverse();

        // Get job type distribution
        let job_types_query = SeaQuery::select()
            .column(Alias::new("class_name"))
            .expr_as(
                Expr::col(Alias::new("class_name")).count(),
                Alias::new("count"),
            )
            .from(Alias::new(&table_config.jobs))
            .and_where(Expr::col(Alias::new("created_at")).gt(period_start))
            .group_by_col(Alias::new("class_name"))
            .order_by(Alias::new("count"), Order::Desc)
            .limit(7)
            .to_owned();

        let (job_types_sql, job_types_values) = match db.get_database_backend() {
            DbBackend::Postgres => job_types_query.build(PostgresQueryBuilder),
            DbBackend::Sqlite => job_types_query.build(SqliteQueryBuilder),
            DbBackend::MySql => job_types_query.build(MysqlQueryBuilder),
        };

        let job_types_stmt = Statement::from_sql_and_values(
            db.get_database_backend(),
            &job_types_sql,
            job_types_values,
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

        // Get queue performance statistics (using raw SQL for complex aggregations)
        let failed_table = &table_config.failed_executions;
        let queue_performance_sql = format!(
            r#"SELECT
                 j.queue_name,
                 COUNT(*) FILTER (WHERE j.finished_at IS NOT NULL) as jobs_processed,
                 AVG(EXTRACT(EPOCH FROM (j.finished_at - j.created_at))) FILTER (WHERE j.finished_at IS NOT NULL) as avg_duration,
                 COUNT(*) FILTER (WHERE EXISTS (SELECT 1 FROM {} f WHERE f.job_id = j.id)) as failed_jobs,
                 COUNT(*) as total_jobs
               FROM {} j
               WHERE j.created_at > $1
               GROUP BY j.queue_name"#,
            failed_table, table_config.jobs
        );

        let queue_performance_stmt = Statement::from_sql_and_values(
            db.get_database_backend(),
            &queue_performance_sql,
            [period_start.into()],
        );

        let queue_performance_result = db
            .query_all(queue_performance_stmt)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        // Get paused queues
        let paused_queues = quebec_pauses::Entity::find()
            .all(db)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        let paused_queue_names: Vec<String> =
            paused_queues.iter().map(|p| p.queue_name.clone()).collect();

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
                }
                Some(secs) if secs >= 60.0 => {
                    format!("{:.1}m", secs / 60.0)
                }
                Some(secs) => {
                    format!("{:.1}s", secs)
                }
                None => "N/A".to_string(),
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

        // Get recently failed jobs (using raw SQL for JOIN operations)
        let recent_failed_jobs_sql = format!(
            r#"SELECT
                f.id,
                j.class_name,
                j.queue_name,
                f.created_at as failed_at,
                f.error
              FROM {} f
              JOIN {} j ON f.job_id = j.id
              WHERE f.created_at > $1
              ORDER BY f.created_at DESC
              LIMIT 10"#,
            table_config.failed_executions, table_config.jobs
        );

        let recent_failed_jobs_stmt = Statement::from_sql_and_values(
            db.get_database_backend(),
            &recent_failed_jobs_sql,
            [period_start.into()],
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

            let formatted_failed_at =
                Self::format_optional_datetime(failed_at).unwrap_or_else(|| "N/A".to_string());

            recent_failed_jobs.push(serde_json::json!({
                "id": id,
                "class_name": class_name,
                "queue_name": queue_name,
                "failed_at": formatted_failed_at,
                "error": error
            }));
        }

        // Serialize arrays to JSON strings
        context.insert(
            "time_labels",
            &serde_json::to_string(&time_labels).unwrap_or_else(|_| "[]".to_string()),
        );
        context.insert(
            "jobs_processed_data",
            &serde_json::to_string(&jobs_processed_data).unwrap_or_else(|_| "[]".to_string()),
        );
        context.insert(
            "job_types_labels",
            &serde_json::to_string(&job_types_labels).unwrap_or_else(|_| "[]".to_string()),
        );
        context.insert(
            "job_types_data",
            &serde_json::to_string(&job_types_data).unwrap_or_else(|_| "[]".to_string()),
        );
        context.insert("queue_stats", &queue_stats);
        context.insert("recent_failed_jobs", &recent_failed_jobs);
        context.insert("active_page", "overview");

        // Render template
        let html = state.render_template("overview.html", &mut context).await?;
        debug!("Template rendering completed in {:?}", start.elapsed());

        Ok(Html(html))
    }
}
