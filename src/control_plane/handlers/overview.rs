use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::Html,
};
use chrono::NaiveDateTime;
use sea_orm::sea_query::{
    Alias, Expr, PostgresQueryBuilder, Query as SeaQuery, SqliteQueryBuilder,
};
use sea_orm::Order;
use sea_orm::{ConnectionTrait, DbBackend, Statement, Value};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, instrument};

use crate::control_plane::{utils::clean_sql, ControlPlane};
use crate::query_builder;

const DEFAULT_OVERVIEW_HOURS: i64 = 24;
const MAX_OVERVIEW_HOURS: i64 = 24 * 30;

fn normalize_overview_hours(raw: Option<&str>) -> i64 {
    raw.and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(DEFAULT_OVERVIEW_HOURS)
        .clamp(1, MAX_OVERVIEW_HOURS)
}

fn overview_chart_interval(hours: i64) -> (i64, &'static str) {
    if hours <= 24 {
        (1, "%H:%M")
    } else if hours <= 168 {
        (6, "%m-%d %H:%M")
    } else {
        (24, "%m-%d")
    }
}

impl ControlPlane {
    #[instrument(skip(state), fields(path = "/"))]
    pub async fn overview(
        State(state): State<Arc<ControlPlane>>,
        Query(params): Query<HashMap<String, String>>,
    ) -> Result<Html<String>, (StatusCode, String)> {
        let start = Instant::now();
        let db = state
            .ctx
            .get_db()
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        let db = db.as_ref();
        let table_config = &state.ctx.table_config;
        debug!("Database connection obtained in {:?}", start.elapsed());

        // Get time range parameter, default to 24 hours. Clamp to the largest
        // range exposed by the UI (30 days), which also caps the chart at 30
        // serial bucket COUNTs.
        // The value drives both `chrono::Duration::hours` (which panics on
        // overflow, e.g. `?hours=9999999999999999`) and the per-bucket COUNT
        // loop below (an unbounded value would fire tens of thousands of serial
        // COUNTs — a page-load query storm).
        let hours = normalize_overview_hours(params.get("hours").map(String::as_str));

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
                format!("{secs:.1}s")
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
        let (interval_hours, format_string) = overview_chart_interval(hours);

        let bucket_count = hours / interval_hours;
        // Keep the labels and half-open bucket boundaries identical across
        // backends. PostgreSQL uses date_bin; SQLite and MySQL count all
        // buckets in one CASE aggregate.
        let finished_col =
            query_builder::quote_identifier(db.get_database_backend(), "finished_at");
        let mut chart_values: Vec<Value> = Vec::with_capacity((bucket_count * 2 + 2) as usize);
        let mut chart_counts = Vec::with_capacity(bucket_count as usize);
        for i in 0..bucket_count {
            let end_time = now - chrono::Duration::hours(i * interval_hours);
            let start_time = end_time - chrono::Duration::hours(interval_hours);
            time_labels.push(end_time.format(format_string).to_string());
            let first = chart_values.len() + 1;
            chart_values.push(start_time.into());
            chart_values.push(end_time.into());
            let (start_param, end_param) = match db.get_database_backend() {
                DbBackend::Postgres => (format!("${first}"), format!("${}", first + 1)),
                DbBackend::Sqlite | DbBackend::MySql => ("?".to_string(), "?".to_string()),
            };
            let bucket_col =
                query_builder::quote_identifier(db.get_database_backend(), &format!("bucket_{i}"));
            chart_counts.push(format!(
                "COUNT(CASE WHEN {finished_col} >= {start_param} AND {finished_col} < {end_param} THEN 1 END) AS {bucket_col}"
            ));
        }

        // date_bin and TIMESTAMPDIFF align bins with the exact oldest
        // boundary. PostgreSQL before 14 falls through to CASE below.
        let binned_counts = if db.get_database_backend() == DbBackend::Postgres {
            let oldest = now - chrono::Duration::hours(bucket_count * interval_hours);
            let table_name =
                query_builder::quote_identifier(DbBackend::Postgres, &table_config.jobs);
            let sql = format!(
                "SELECT date_bin($1::interval, \"finished_at\", $2::timestamp) AS bucket, COUNT(*) AS count FROM {table_name} WHERE \"finished_at\" >= $3 AND \"finished_at\" < $4 GROUP BY bucket",
            );
            let stmt = Statement::from_sql_and_values(
                DbBackend::Postgres,
                sql,
                [
                    format!("{interval_hours} hours").into(),
                    oldest.into(),
                    oldest.into(),
                    now.into(),
                ],
            );
            db.query_all(stmt).await.ok().and_then(|rows| {
                let mut counts = vec![0u64; bucket_count as usize];
                for row in rows {
                    let start: NaiveDateTime = row.try_get("", "bucket").ok()?;
                    let count: i64 = row.try_get("", "count").ok()?;
                    // PostgreSQL timestamps have microsecond precision while
                    // `oldest` may have sub-microsecond nanoseconds. Round to
                    // the nearest stride, then verify the residual is tiny.
                    let offset_us = (start - oldest).num_microseconds()?;
                    let stride_us = interval_hours * 3_600_000_000;
                    let index = (offset_us + stride_us / 2).div_euclid(stride_us);
                    if index < 0
                        || index >= bucket_count
                        || (offset_us - index * stride_us).abs() > 2
                    {
                        return None;
                    }
                    counts[(bucket_count - 1 - index) as usize] = count as u64;
                }
                Some(counts)
            })
        } else if db.get_database_backend() == DbBackend::MySql {
            let oldest = now - chrono::Duration::hours(bucket_count * interval_hours);
            let stride_us = interval_hours * 3_600_000_000;
            let table_name = query_builder::quote_identifier(DbBackend::MySql, &table_config.jobs);
            let sql = format!(
                "SELECT TIMESTAMPDIFF(MICROSECOND, ?, {finished_col}) DIV ? AS bucket, COUNT(*) AS count \
                 FROM {table_name} WHERE {finished_col} >= ? AND {finished_col} < ? GROUP BY bucket"
            );
            let stmt = Statement::from_sql_and_values(
                DbBackend::MySql,
                sql,
                [oldest.into(), stride_us.into(), oldest.into(), now.into()],
            );
            db.query_all(stmt).await.ok().and_then(|rows| {
                let mut counts = vec![0u64; bucket_count as usize];
                for row in rows {
                    let index: i64 = row.try_get("", "bucket").ok()?;
                    let count: i64 = row.try_get("", "count").ok()?;
                    if index < 0 || index >= bucket_count {
                        return None;
                    }
                    counts[(bucket_count - 1 - index) as usize] = count as u64;
                }
                Some(counts)
            })
        } else {
            None
        };

        let grouped_counts = if binned_counts.is_some() {
            binned_counts
        } else if matches!(
            db.get_database_backend(),
            DbBackend::Postgres | DbBackend::Sqlite | DbBackend::MySql
        ) {
            let oldest = now - chrono::Duration::hours(bucket_count * interval_hours);
            let first = chart_values.len() + 1;
            chart_values.push(oldest.into());
            chart_values.push(now.into());
            let (start_param, end_param) = match db.get_database_backend() {
                DbBackend::Postgres => (format!("${first}"), format!("${}", first + 1)),
                DbBackend::Sqlite | DbBackend::MySql => ("?".to_string(), "?".to_string()),
            };
            let table_name =
                query_builder::quote_identifier(db.get_database_backend(), &table_config.jobs);
            let sql = format!(
                "SELECT {} FROM {table_name} WHERE {finished_col} >= {start_param} AND {finished_col} < {end_param}",
                chart_counts.join(", "),
            );
            let stmt = Statement::from_sql_and_values(db.get_database_backend(), sql, chart_values);
            db.query_one(stmt).await.ok().flatten().and_then(|row| {
                (0..bucket_count)
                    .map(|i| row.try_get::<i64>("", &format!("bucket_{i}")))
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .ok()
                    .map(|counts| counts.into_iter().map(|count| count as u64).collect())
            })
        } else {
            None
        };

        if let Some(counts) = grouped_counts {
            jobs_processed_data = counts;
        } else {
            for i in 0..bucket_count {
                let end_time = now - chrono::Duration::hours(i * interval_hours);
                let start_time = end_time - chrono::Duration::hours(interval_hours);
                jobs_processed_data.push(
                    query_builder::jobs::count_finished_in_range(
                        db,
                        table_config,
                        start_time,
                        Some(end_time),
                    )
                    .await
                    .unwrap_or(0),
                );
            }
        }

        // Reverse arrays to display in chronological order
        time_labels.reverse();
        jobs_processed_data.reverse();

        // MySQL's class_name index requires a row lookup for created_at on
        // every job. Scan the clustered primary key instead; stable tie
        // ordering keeps the seven displayed classes independent of the plan.
        let job_types_stmt = if db.get_database_backend() == DbBackend::MySql {
            let jobs_table =
                query_builder::quote_identifier(db.get_database_backend(), &table_config.jobs);
            let sql = format!(
                "SELECT `class_name`, COUNT(`class_name`) AS `count` FROM {jobs_table} \
                 FORCE INDEX(PRIMARY) WHERE `created_at` > ? GROUP BY `class_name` \
                 ORDER BY `count` DESC, `class_name` ASC LIMIT 7"
            );
            Statement::from_sql_and_values(db.get_database_backend(), sql, [period_start.into()])
        } else {
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
            let (sql, values) = match db.get_database_backend() {
                DbBackend::Postgres => job_types_query.build(PostgresQueryBuilder),
                DbBackend::Sqlite => job_types_query.build(SqliteQueryBuilder),
                DbBackend::MySql => unreachable!(),
            };
            Statement::from_sql_and_values(db.get_database_backend(), sql, values)
        };

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
                     COUNT(f.job_id) as failed_jobs,
                     COUNT(*) as total_jobs
                   FROM "{}" j LEFT JOIN "{}" f ON f.job_id = j.id
                   WHERE j.created_at > $1 GROUP BY j.queue_name"#,
                table_config.jobs, failed_table
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
            // failed_executions.job_id is unique, so this join keeps one row
            // per job while avoiding a dependent EXISTS lookup per job.
            DbBackend::MySql => format!(
                r#"SELECT j.queue_name,
                     COUNT(CASE WHEN j.finished_at IS NOT NULL THEN 1 END) as jobs_processed,
                     AVG(CASE WHEN j.finished_at IS NOT NULL THEN TIMESTAMPDIFF(SECOND, j.created_at, j.finished_at) END) as avg_duration,
                     COUNT(f.job_id) as failed_jobs,
                     COUNT(*) as total_jobs
                   FROM `{}` j LEFT JOIN `{}` f ON f.job_id = j.id
                   WHERE j.created_at > ? GROUP BY j.queue_name"#,
                table_config.jobs, failed_table
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
                    format!("{secs:.1}s")
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overview_hours_defaults_and_clamps_to_supported_range() {
        for (raw, expected) in [
            (None, DEFAULT_OVERVIEW_HOURS),
            (Some(""), DEFAULT_OVERVIEW_HOURS),
            (Some("invalid"), DEFAULT_OVERVIEW_HOURS),
            (Some("999999999999999999999999999"), DEFAULT_OVERVIEW_HOURS),
            (Some("-1"), 1),
            (Some("0"), 1),
            (Some("1"), 1),
            (Some("24"), 24),
            (Some("168"), 168),
            (Some("720"), MAX_OVERVIEW_HOURS),
            (Some("721"), MAX_OVERVIEW_HOURS),
        ] {
            assert_eq!(normalize_overview_hours(raw), expected);
        }

        let huge = i64::MAX.to_string();
        assert_eq!(
            normalize_overview_hours(Some(huge.as_str())),
            MAX_OVERVIEW_HOURS
        );
    }

    #[test]
    fn supported_overview_ranges_never_exceed_thirty_chart_buckets() {
        for hours in 1..=MAX_OVERVIEW_HOURS {
            let (interval_hours, _) = overview_chart_interval(hours);
            assert!(hours / interval_hours <= 30);
        }
    }
}
