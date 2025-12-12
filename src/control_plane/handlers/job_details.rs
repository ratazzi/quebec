use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::Html,
};
use sea_orm::{ConnectionTrait, Statement, Value};
use std::sync::Arc;
use std::time::Instant;
use tracing::debug;

use crate::control_plane::{models::JobDetailsInfo, ControlPlane};

impl ControlPlane {
    pub async fn job_details(
        State(state): State<Arc<ControlPlane>>, Path(id): Path<i64>,
    ) -> Result<Html<String>, (StatusCode, String)> {
        let start = Instant::now();
        let db = state.ctx.get_db().await;
        let db = db.as_ref();
        debug!("Database connection obtained in {:?}", start.elapsed());

        let start = Instant::now();

        // Get job basic information using dynamic table names
        let table_config = &state.ctx.table_config;
        let job_details_sql = format!(
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
            FROM {} j
            LEFT JOIN {} c ON j.id = c.job_id
            LEFT JOIN {} f ON j.id = f.job_id
            LEFT JOIN {} s ON j.id = s.job_id
            LEFT JOIN {} b ON j.id = b.job_id
            WHERE j.id = $1",
            table_config.jobs,
            table_config.claimed_executions,
            table_config.failed_executions,
            table_config.scheduled_executions,
            table_config.blocked_executions
        );

        let job_result = db
            .query_one(Statement::from_sql_and_values(
                db.get_database_backend(),
                &job_details_sql,
                [Value::from(id)],
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
            let context: Option<String> = None; // No metadata field in current schema

            // Parse creation time
            let created_at =
                Self::format_datetime(row.try_get::<chrono::NaiveDateTime>("", "created_at"));

            // Get completion time (if any)
            let finished_at = match row.try_get::<Option<chrono::NaiveDateTime>>("", "finished_at")
            {
                Ok(dt_opt) => Self::format_optional_datetime(dt_opt),
                _ => None,
            };

            // Calculate runtime (if completed)
            let runtime = if let Some(finished) =
                row.try_get::<Option<chrono::NaiveDateTime>>("", "finished_at").ok().flatten()
            {
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
                arguments: arguments.unwrap_or_else(|| "{}".to_string()),
                context,
                execution_id: None,
                execution_history: Vec::new(),
            };

            // Get specific detailed information based on status
            match status.as_str() {
                "failed" => {
                    let failed_id: i64 = row.try_get("", "failed_id").unwrap_or_default();
                    job_details.execution_id = Some(failed_id);

                    // Get failure information
                    let failed_details_sql = format!(
                        "SELECT error, created_at as failed_at FROM {} WHERE id = $1",
                        table_config.failed_executions
                    );

                    if let Ok(failed_info) = db
                        .query_one(Statement::from_sql_and_values(
                            db.get_database_backend(),
                            &failed_details_sql,
                            [Value::from(failed_id)],
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
                }
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
                                format!(
                                    "in {}h {}m",
                                    duration.num_hours(),
                                    duration.num_minutes() % 60
                                )
                            } else if duration.num_minutes() > 0 {
                                format!(
                                    "in {}m {}s",
                                    duration.num_minutes(),
                                    duration.num_seconds() % 60
                                )
                            } else {
                                format!("in {}s", duration.num_seconds())
                            };
                            job_details.scheduled_in = Some(scheduled_in);
                        } else {
                            job_details.scheduled_in = Some("overdue".to_string());
                        }
                    }
                }
                "blocked" => {
                    let blocked_id: i64 = row.try_get("", "blocked_id").unwrap_or_default();
                    job_details.execution_id = Some(blocked_id);

                    // Get blocked information
                    job_details.concurrency_key = row.try_get("", "concurrency_key").ok();

                    // Parse expiration time
                    if let Ok(dt) = row.try_get::<chrono::NaiveDateTime>("", "expires_at") {
                        job_details.expires_at = Some(Self::format_naive_datetime(dt));

                        // Calculate waiting time
                        if let Ok(created) = row.try_get::<chrono::NaiveDateTime>("", "created_at")
                        {
                            let duration =
                                chrono::Utc::now().naive_utc().signed_duration_since(created);
                            job_details.waiting_time =
                                Some(format!("{} seconds", duration.num_seconds()));
                        }
                    }
                }
                "processing" => {
                    let claimed_id: i64 = row.try_get("", "claimed_id").unwrap_or_default();
                    job_details.execution_id = Some(claimed_id);

                    // Get processing information
                    if let Ok(process_id) = row.try_get::<i64>("", "process_id") {
                        // Get worker process information
                        let worker_details_sql = format!(
                            "SELECT name, hostname FROM {} WHERE id = $1",
                            table_config.processes
                        );

                        if let Ok(worker_info) = db
                            .query_one(Statement::from_sql_and_values(
                                db.get_database_backend(),
                                &worker_details_sql,
                                [Value::from(process_id)],
                            ))
                            .await
                        {
                            if let Some(row) = worker_info {
                                if let Ok(name) = row.try_get::<String>("", "name") {
                                    if let Ok(hostname) =
                                        row.try_get::<Option<String>>("", "hostname")
                                    {
                                        job_details.worker_id = Some(format!(
                                            "{} ({})",
                                            name,
                                            hostname.unwrap_or_else(|| "unknown".to_string())
                                        ));
                                    } else {
                                        job_details.worker_id = Some(name);
                                    }
                                }
                            }
                        } else {
                            job_details.worker_id = Some(process_id.to_string());
                        }
                    }

                    // Get start time and runtime
                    let claimed_details_sql = format!(
                        "SELECT created_at FROM {} WHERE id = $1",
                        table_config.claimed_executions
                    );

                    if let Ok(claimed_info) = db
                        .query_one(Statement::from_sql_and_values(
                            db.get_database_backend(),
                            &claimed_details_sql,
                            [Value::from(claimed_id)],
                        ))
                        .await
                    {
                        if let Some(row) = claimed_info {
                            if let Ok(dt) = row.try_get::<chrono::NaiveDateTime>("", "created_at") {
                                job_details.started_at = Some(Self::format_naive_datetime(dt));

                                // Calculate current runtime
                                let now = chrono::Utc::now().naive_utc();
                                let duration = now.signed_duration_since(dt);
                                job_details.runtime =
                                    Some(format!("{} seconds", duration.num_seconds()));
                            }
                        }
                    }
                }
                _ => {}
            }

            // Get execution history using dynamic table names
            let history_sql = format!(
                "SELECT 
                    'failed' as event_type,
                    created_at as timestamp,
                    error
                 FROM {}
                 WHERE job_id = $1
                 UNION ALL
                 SELECT 
                    'scheduled' as event_type,
                    created_at as timestamp,
                    NULL as error
                 FROM {}
                 WHERE job_id = $1
                 UNION ALL
                 SELECT 
                    'claimed' as event_type,
                    created_at as timestamp,
                    NULL as error
                 FROM {}
                 WHERE job_id = $1
                 ORDER BY timestamp DESC",
                table_config.failed_executions,
                table_config.scheduled_executions,
                table_config.claimed_executions
            );

            let history_result = db
                .query_all(Statement::from_sql_and_values(
                    db.get_database_backend(),
                    &history_sql,
                    [Value::from(id)],
                ))
                .await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

            let mut attempt = 1;
            for row in history_result {
                let event_type: String = row.try_get("", "event_type").unwrap_or_default();
                let timestamp = match row.try_get::<chrono::NaiveDateTime>("", "timestamp") {
                    Ok(dt) => Self::format_naive_datetime(dt),
                    Err(_) => "Unknown".to_string(),
                };
                let error: Option<String> = row.try_get("", "error").ok();

                job_details.execution_history.push(
                    crate::control_plane::models::ExecutionHistoryItem {
                        attempt,
                        timestamp,
                        status: event_type,
                        error,
                    },
                );
                attempt += 1;
            }

            // Format arguments for display
            job_details.arguments = Self::parse_arguments(&job_details.arguments);

            let mut context = tera::Context::new();
            context.insert("job", &job_details);
            context.insert(
                "active_page",
                match status.as_str() {
                    "failed" => "failed-jobs",
                    "scheduled" => "scheduled-jobs",
                    "blocked" => "blocked-jobs",
                    "processing" => "in-progress-jobs",
                    "finished" => "finished-jobs",
                    _ => "queues",
                },
            );

            let html = state.render_template("job-details.html", &mut context).await?;
            debug!("Template rendering completed in {:?}", start.elapsed());

            Ok(Html(html))
        } else {
            Err((StatusCode::NOT_FOUND, format!("Job with ID {} not found", id)))
        }
    }
}
