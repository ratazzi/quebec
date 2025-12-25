use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::Html,
};
use chrono::NaiveDateTime;
use sea_orm::sea_query::{
    Alias, Expr, MysqlQueryBuilder, PostgresQueryBuilder, Query as SeaQuery, SqliteQueryBuilder,
};
use sea_orm::Order;
use sea_orm::{ConnectionTrait, DbBackend, Statement};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, instrument};

use crate::control_plane::{utils::clean_sql, ControlPlane};
use crate::query_builder;

impl ControlPlane {
    #[instrument(skip(state), fields(path = "/"))]
    pub async fn overview(
        State(state): State<Arc<ControlPlane>>,
        Query(params): Query<HashMap<String, String>>,
    ) -> Result<Html<String>, (StatusCode, String)> {
        let start = Instant::now();
        let db = state.ctx.get_db().await;
        let db = db.as_ref();
        let table_config = &state.ctx.table_config;
        debug!("Database connection obtained in {:?}", start.elapsed());

        // Get time range parameter, default to 24 hours
        let hours: i64 = params
            .get("hours")
            .and_then(|h| h.parse().ok())
            .unwrap_or(24);

        let now = chrono::Utc::now().naive_utc();
        let period_start = now - chrono::Duration::hours(hours);
        let previous_period_start = period_start - chrono::Duration::hours(hours);

        // Get total number of completed jobs in current period using query_builder
        let total_jobs_processed =
            query_builder::jobs::count_finished_in_range(db, table_config, period_start, Some(now))
                .await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        // Get total number of completed jobs in previous period for calculating change rate
        let previous_jobs_processed = query_builder::jobs::count_finished_in_range(
            db,
            table_config,
            previous_period_start,
            Some(period_start),
        )
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
        // Use database-specific SQL for duration calculation
        let avg_duration_sql = clean_sql(&match db.get_database_backend() {
            DbBackend::Postgres => format!(
                r#"SELECT AVG(EXTRACT(EPOCH FROM (finished_at - created_at))) as avg_duration
                   FROM "{}" WHERE finished_at IS NOT NULL AND finished_at > $1"#,
                table_config.jobs
            ),
            DbBackend::Sqlite => format!(
                r#"SELECT AVG((julianday(finished_at) - julianday(created_at)) * 86400) as avg_duration
                   FROM "{}" WHERE finished_at IS NOT NULL AND finished_at > ?"#,
                table_config.jobs
            ),
            DbBackend::MySql => format!(
                r#"SELECT AVG(TIMESTAMPDIFF(SECOND, created_at, finished_at)) as avg_duration
                   FROM `{}` WHERE finished_at IS NOT NULL AND finished_at > ?"#,
                table_config.jobs
            ),
        });

        let avg_duration_stmt = Statement::from_sql_and_values(
            db.get_database_backend(),
            &avg_duration_sql,
            [period_start.into()],
        );

        let avg_duration: Option<f64> = db
            .query_one(avg_duration_stmt)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
            .and_then(|row| row.try_get("", "avg_duration").ok());

        // Get average processing time of jobs in previous period
        let prev_avg_duration_sql = clean_sql(&match db.get_database_backend() {
            DbBackend::Postgres => format!(
                r#"SELECT AVG(EXTRACT(EPOCH FROM (finished_at - created_at))) as avg_duration
                   FROM "{}" WHERE finished_at IS NOT NULL AND finished_at > $1 AND finished_at <= $2"#,
                table_config.jobs
            ),
            DbBackend::Sqlite => format!(
                r#"SELECT AVG((julianday(finished_at) - julianday(created_at)) * 86400) as avg_duration
                   FROM "{}" WHERE finished_at IS NOT NULL AND finished_at > ? AND finished_at <= ?"#,
                table_config.jobs
            ),
            DbBackend::MySql => format!(
                r#"SELECT AVG(TIMESTAMPDIFF(SECOND, created_at, finished_at)) as avg_duration
                   FROM `{}` WHERE finished_at IS NOT NULL AND finished_at > ? AND finished_at <= ?"#,
                table_config.jobs
            ),
        });

        let prev_avg_duration_stmt = Statement::from_sql_and_values(
            db.get_database_backend(),
            &prev_avg_duration_sql,
            [previous_period_start.into(), period_start.into()],
        );

        let prev_avg_duration: Option<f64> = db
            .query_one(prev_avg_duration_stmt)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
            .and_then(|row| row.try_get("", "avg_duration").ok());

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

        // Get number of active workers using query_builder
        let heartbeat_threshold = now - chrono::Duration::seconds(30);
        let active_workers =
            query_builder::processes::count_active(db, table_config, heartbeat_threshold)
                .await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        // Get number of active workers in previous period (simplified here, should query historical data)
        let prev_active_workers = active_workers; // Assume no change, should get from historical data

        // Calculate change in number of active workers
        let active_workers_change = active_workers as i32 - prev_active_workers as i32;

        // Calculate failure rate using query_builder
        let failed_jobs = query_builder::failed_executions::count_created_in_range(
            db,
            table_config,
            period_start,
            Some(now),
        )
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        let total_jobs =
            query_builder::jobs::count_created_in_range(db, table_config, period_start, Some(now))
                .await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        let failed_jobs_rate = if total_jobs > 0 {
            ((failed_jobs as f64 / total_jobs as f64) * 100.0).round() as i32
        } else {
            0
        };

        // Get failure rate of previous period using query_builder
        let prev_failed_jobs = query_builder::failed_executions::count_created_in_range(
            db,
            table_config,
            previous_period_start,
            Some(period_start),
        )
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        let prev_total_jobs = query_builder::jobs::count_created_in_range(
            db,
            table_config,
            previous_period_start,
            Some(period_start),
        )
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

            // Query number of jobs completed in this time period using query_builder
            let period_jobs = query_builder::jobs::count_finished_in_range(
                db,
                table_config,
                start_time,
                Some(end_time),
            )
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

        let (job_types_labels, job_types_data): (Vec<String>, Vec<i64>) = job_types_result
            .into_iter()
            .map(|row| {
                let class_name: String = row.try_get("", "class_name").unwrap_or_default();
                let count: i64 = row.try_get("", "count").unwrap_or_default();
                (class_name, count)
            })
            .unzip();

        // Get queue performance statistics (using raw SQL for complex aggregations)
        // Use database-specific SQL for compatibility
        let failed_table = &table_config.failed_executions;
        let queue_performance_sql = clean_sql(&match db.get_database_backend() {
            DbBackend::Postgres => format!(
                r#"SELECT j.queue_name,
                     COUNT(CASE WHEN j.finished_at IS NOT NULL THEN 1 END) as jobs_processed,
                     AVG(EXTRACT(EPOCH FROM (j.finished_at - j.created_at))) as avg_duration,
                     COUNT(CASE WHEN EXISTS (SELECT 1 FROM "{}" f WHERE f.job_id = j.id) THEN 1 END) as failed_jobs,
                     COUNT(*) as total_jobs
                   FROM "{}" j WHERE j.created_at > $1 GROUP BY j.queue_name"#,
                failed_table, table_config.jobs
            ),
            DbBackend::Sqlite => format!(
                r#"SELECT j.queue_name,
                     COUNT(CASE WHEN j.finished_at IS NOT NULL THEN 1 END) as jobs_processed,
                     AVG(CASE WHEN j.finished_at IS NOT NULL THEN (julianday(j.finished_at) - julianday(j.created_at)) * 86400 END) as avg_duration,
                     COUNT(CASE WHEN EXISTS (SELECT 1 FROM "{}" f WHERE f.job_id = j.id) THEN 1 END) as failed_jobs,
                     COUNT(*) as total_jobs
                   FROM "{}" j WHERE j.created_at > ? GROUP BY j.queue_name"#,
                failed_table, table_config.jobs
            ),
            DbBackend::MySql => format!(
                r#"SELECT j.queue_name,
                     COUNT(CASE WHEN j.finished_at IS NOT NULL THEN 1 END) as jobs_processed,
                     AVG(CASE WHEN j.finished_at IS NOT NULL THEN TIMESTAMPDIFF(SECOND, j.created_at, j.finished_at) END) as avg_duration,
                     COUNT(CASE WHEN EXISTS (SELECT 1 FROM `{}` f WHERE f.job_id = j.id) THEN 1 END) as failed_jobs,
                     COUNT(*) as total_jobs
                   FROM `{}` j WHERE j.created_at > ? GROUP BY j.queue_name"#,
                failed_table, table_config.jobs
            ),
        });

        let queue_performance_stmt = Statement::from_sql_and_values(
            db.get_database_backend(),
            &queue_performance_sql,
            [period_start.into()],
        );

        let queue_performance_result = db
            .query_all(queue_performance_stmt)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        // Get paused queues using query_builder
        let paused_queue_names: Vec<String> =
            query_builder::pauses::find_all_queue_names(db, table_config)
                .await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

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
        let p1 = match db.get_database_backend() {
            DbBackend::Postgres => "$1",
            DbBackend::MySql | DbBackend::Sqlite => "?",
        };
        let recent_failed_jobs_sql = clean_sql(&format!(
            r#"SELECT
            f.id,
            j.class_name,
            j.queue_name,
            f.created_at as failed_at,
            f.error
          FROM {} f
          JOIN {} j ON f.job_id = j.id
          WHERE f.created_at > {}
          ORDER BY f.created_at DESC
          LIMIT 10"#,
            table_config.failed_executions, table_config.jobs, p1
        ));

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
