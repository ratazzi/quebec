use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{Html, IntoResponse},
};
use sea_orm::{ConnectionTrait, DbErr, Statement, TransactionTrait, Value};
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, error, info};

use crate::control_plane::{
    models::{FilterOptions, Pagination, ScheduledJobInfo},
    utils::clean_sql,
    ControlPlane,
};
use crate::query_builder;

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

        // Get scheduled jobs with related information using dynamic table names
        let table_config = &state.ctx.table_config;
        let backend = db.get_database_backend();

        // Use database-specific placeholders
        let (p1, p2) = match backend {
            sea_orm::DbBackend::Postgres => ("$1".to_string(), "$2".to_string()),
            sea_orm::DbBackend::MySql | sea_orm::DbBackend::Sqlite => {
                ("?".to_string(), "?".to_string())
            }
        };

        let scheduled_jobs_sql = clean_sql(&format!(
            "SELECT
            s.id as execution_id,
            s.job_id,
            s.scheduled_at,
            j.class_name,
            j.queue_name,
            j.created_at
        FROM {} s
        JOIN {} j ON s.job_id = j.id
        WHERE j.finished_at IS NULL
        ORDER BY s.scheduled_at ASC
        LIMIT {} OFFSET {}",
            table_config.scheduled_executions, table_config.jobs, p1, p2
        ));

        let scheduled_jobs_result = db
            .query_all(Statement::from_sql_and_values(
                backend,
                &scheduled_jobs_sql,
                [Value::from(page_size as i32), Value::from(offset as i32)],
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

            // Apply filters
            if let Some(ref filter_class) = pagination.class_name {
                if &class_name != filter_class {
                    continue;
                }
            }
            if let Some(ref filter_queue) = pagination.queue_name {
                if &queue_name != filter_queue {
                    continue;
                }
            }

            // Parse creation time
            let created_at_str =
                Self::format_datetime(row.try_get::<chrono::NaiveDateTime>("", "created_at"));

            // Parse scheduled execution time
            let (scheduled_at_str, scheduled_in) =
                match row.try_get::<chrono::NaiveDateTime>("", "scheduled_at") {
                    Ok(dt) => {
                        let now = chrono::Utc::now().naive_utc();
                        let scheduled_in = if dt > now {
                            let duration = dt.signed_duration_since(now);
                            if duration.num_hours() > 0 {
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
                            }
                        } else {
                            "overdue".to_string()
                        };

                        (Self::format_naive_datetime(dt), scheduled_in)
                    }
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

        // Get global filter options
        let class_names = state
            .get_job_classes()
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        let queue_names = state
            .get_queue_names()
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        let filter_options = FilterOptions {
            class_names,
            queue_names,
        };

        debug!("Fetched scheduled jobs in {:?}", start.elapsed());

        // Get total number of scheduled jobs for pagination
        let count_sql = clean_sql(&format!(
            "SELECT COUNT(*) AS count
         FROM {} s
         JOIN {} j ON s.job_id = j.id
         WHERE j.finished_at IS NULL",
            table_config.scheduled_executions, table_config.jobs
        ));

        let total_count = db
            .query_one(Statement::from_sql_and_values(backend, &count_sql, []))
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
            .map(|row| row.try_get::<i64>("", "count").unwrap_or(0))
            .unwrap_or(0);

        let total_pages = ((total_count as f64) / (page_size as f64)).ceil() as u64;
        let total_pages = total_pages.max(1);

        if total_pages > 0 && pagination.page > total_pages {
            return Err((StatusCode::NOT_FOUND, "Page not found".to_string()));
        }

        let start = Instant::now();
        let mut context = tera::Context::new();
        context.insert("scheduled_jobs", &scheduled_jobs);
        context.insert("filter_options", &filter_options);
        context.insert("current_page_num", &pagination.page);
        context.insert("total_pages", &total_pages);
        context.insert("active_page", "scheduled-jobs");

        let html = state
            .render_template("scheduled-jobs.html", &mut context)
            .await?;
        debug!("Template rendering completed in {:?}", start.elapsed());

        Ok(Html(html))
    }

    pub async fn cancel_scheduled_job(
        State(state): State<Arc<ControlPlane>>,
        Path(id): Path<i64>,
    ) -> impl IntoResponse {
        let db = state.ctx.get_db().await;
        let db = db.as_ref();
        let table_config = state.ctx.table_config.clone();

        // Use transaction to operate
        let txn_result = db
            .transaction::<_, (), DbErr>(|txn| {
                let table_config = table_config.clone();
                Box::pin(async move {
                    // Find scheduled execution record to cancel using query_builder
                    let scheduled_execution =
                        query_builder::scheduled_executions::find_by_id(txn, &table_config, id)
                            .await?;

                    if let Some(execution) = scheduled_execution {
                        // Mark job as finished using query_builder
                        query_builder::jobs::mark_finished(txn, &table_config, execution.job_id)
                            .await?;

                        // Delete scheduled_execution record using query_builder
                        query_builder::scheduled_executions::delete_by_id(txn, &table_config, id)
                            .await?;

                        info!("Cancelled scheduled job ID: {}", id);
                    } else {
                        return Err(DbErr::Custom(format!(
                            "Scheduled job with ID {} not found",
                            id
                        )));
                    }

                    Ok(())
                })
            })
            .await;

        match txn_result {
            Ok(_) => (StatusCode::SEE_OTHER, "/scheduled-jobs".to_string()),
            Err(e) => {
                error!("Failed to cancel scheduled job {}: {}", id, e);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "/scheduled-jobs".to_string(),
                )
            }
        }
    }
}
