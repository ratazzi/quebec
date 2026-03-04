use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{Html, Response},
};
use sea_orm::{DbErr, TransactionTrait};
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, error, info, instrument};

use crate::control_plane::{
    models::{FilterOptions, InProgressJobInfo, Pagination},
    ControlPlane,
};
use crate::query_builder;

impl ControlPlane {
    pub async fn in_progress_jobs(
        State(state): State<Arc<ControlPlane>>,
        Query(pagination): Query<Pagination>,
    ) -> Result<Html<String>, (StatusCode, String)> {
        let start = Instant::now();
        let db = state.ctx.get_db().await;
        let db = db.as_ref();
        let table_config = &state.ctx.table_config;
        debug!("Database connection obtained in {:?}", start.elapsed());

        let start = Instant::now();

        // Calculate page offset
        let page_size = state.page_size;
        let offset = (pagination.page - 1) * page_size;

        // Get claimed (in-progress) jobs using query_builder (with SQL-level filtering)
        let claimed_executions = query_builder::claimed_executions::find_paginated(
            db,
            table_config,
            offset,
            page_size,
            pagination.class_name.as_deref(),
            pagination.queue_name.as_deref(),
        )
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        // Get process information using query_builder
        let processes = query_builder::processes::find_all(db, table_config)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        // Create a vector to store in-progress job information
        let mut in_progress_jobs: Vec<InProgressJobInfo> =
            Vec::with_capacity(claimed_executions.len());

        // Get current time, for calculating runtime
        let now = chrono::Utc::now().naive_utc();

        // Get job information for each in-progress execution
        for execution in claimed_executions {
            if let Ok(Some(job)) =
                query_builder::jobs::find_by_id(db, table_config, execution.job_id).await
            {
                // Find process information
                let worker_id = match execution.process_id {
                    Some(pid) => {
                        let process = processes.iter().find(|p| p.id == pid);
                        process.map_or(pid, |p| p.id)
                    }
                    None => 0,
                };

                // Calculate runtime
                let runtime = match now.signed_duration_since(execution.created_at) {
                    duration if duration.num_hours() >= 1 => {
                        format!("{}h {}m", duration.num_hours(), duration.num_minutes() % 60)
                    }
                    duration if duration.num_minutes() >= 1 => {
                        format!(
                            "{}m {}s",
                            duration.num_minutes(),
                            duration.num_seconds() % 60
                        )
                    }
                    duration => {
                        format!("{}s", duration.num_seconds())
                    }
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

        debug!("Fetched in-progress jobs in {:?}", start.elapsed());
        info!("Found {} in-progress jobs", in_progress_jobs.len());

        // Get total number of in-progress jobs for pagination
        let start = Instant::now();

        let total_count: u64 = query_builder::claimed_executions::count_all(
            db,
            table_config,
            pagination.class_name.as_deref(),
            pagination.queue_name.as_deref(),
        )
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        info!("Total in-progress jobs count: {}", total_count);

        let total_pages = ((total_count as f64) / (page_size as f64)).ceil() as u64;
        let total_pages = total_pages.max(1);

        if total_pages > 0 && pagination.page > total_pages {
            return Err((StatusCode::NOT_FOUND, "Page not found".to_string()));
        }
        debug!("Fetched count in {:?}", start.elapsed());

        let start = Instant::now();
        let mut context = tera::Context::new();
        context.insert("in_progress_jobs", &in_progress_jobs);
        context.insert("filter_options", &filter_options);
        context.insert("current_page_num", &pagination.page);
        context.insert("total_pages", &total_pages);
        context.insert("active_page", "in-progress-jobs");

        let html = state
            .render_template("in-progress-jobs.html", &mut context)
            .await?;
        debug!("Template rendering completed in {:?}", start.elapsed());

        Ok(Html(html))
    }

    // Implement method to cancel a single in-progress job
    #[instrument(skip(state), fields(path = "/in-progress-jobs/:id/cancel"))]
    pub async fn cancel_in_progress_job(
        State(state): State<Arc<ControlPlane>>,
        headers: HeaderMap,
        Path(id): Path<i64>,
    ) -> Response {
        let db = state.ctx.get_db().await;
        let db = db.as_ref();
        let table_config = state.ctx.table_config.clone();
        let redirect = Self::referer_or(&headers, "/in-progress-jobs");

        match db
            .transaction::<_, (), DbErr>(|txn| {
                let table_config = table_config.clone();
                Box::pin(async move {
                    let claimed_execution =
                        query_builder::claimed_executions::find_by_id(txn, &table_config, id)
                            .await?;

                    if let Some(execution) = claimed_execution {
                        query_builder::jobs::mark_finished(txn, &table_config, execution.job_id)
                            .await?;
                        query_builder::claimed_executions::delete_by_id(txn, &table_config, id)
                            .await?;
                    } else {
                        return Err(DbErr::Custom(format!(
                            "In-progress job with ID {} not found",
                            id
                        )));
                    }

                    Ok(())
                })
            })
            .await
        {
            Ok(_) => {
                info!("Cancelled in-progress job ID: {}", id);
                Self::redirect_back(&redirect)
            }
            Err(e) => {
                error!("Failed to cancel in-progress job {}: {}", id, e);
                Self::error_response()
            }
        }
    }

    pub async fn cancel_all_in_progress_jobs(
        State(state): State<Arc<ControlPlane>>,
        headers: HeaderMap,
    ) -> Response {
        let db = state.ctx.get_db().await;
        let db = db.as_ref();
        let table_config = state.ctx.table_config.clone();
        let redirect = Self::referer_or(&headers, "/in-progress-jobs");

        match db
            .transaction::<_, u64, DbErr>(|txn| {
                let table_config = table_config.clone();
                Box::pin(async move {
                    let claimed_executions =
                        query_builder::claimed_executions::find_all(txn, &table_config).await?;

                    if claimed_executions.is_empty() {
                        return Ok(0);
                    }

                    let job_ids: Vec<i64> = claimed_executions
                        .iter()
                        .map(|execution| execution.job_id)
                        .collect();

                    query_builder::jobs::mark_finished_by_ids(txn, &table_config, job_ids).await?;

                    let count =
                        query_builder::claimed_executions::delete_all(txn, &table_config).await?;

                    Ok(count)
                })
            })
            .await
        {
            Ok(count) => {
                info!("Cancelled all {} in-progress jobs", count);
                Self::redirect_back(&redirect)
            }
            Err(e) => {
                error!("Failed to cancel all in-progress jobs: {}", e);
                Self::error_response()
            }
        }
    }
}
