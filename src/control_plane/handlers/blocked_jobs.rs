use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{Html, IntoResponse},
};
use sea_orm::{DbErr, TransactionTrait};
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, error, info, instrument};

use crate::control_plane::{
    models::{BlockedJobInfo, FilterOptions, Pagination},
    ControlPlane,
};
use crate::query_builder;

impl ControlPlane {
    pub async fn blocked_jobs(
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

        // Get blocked jobs using query_builder
        let blocked_executions =
            query_builder::blocked_executions::find_paginated(db, table_config, offset, page_size)
                .await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        // Create a vector to store blocked job information
        let mut blocked_jobs: Vec<BlockedJobInfo> = Vec::with_capacity(blocked_executions.len());

        // Get current time, for calculating waiting time
        let now = chrono::Utc::now().naive_utc();

        // Get job information for each blocked execution
        for execution in blocked_executions {
            if let Ok(Some(job)) =
                query_builder::jobs::find_by_id(db, table_config, execution.job_id).await
            {
                // Apply filters
                if let Some(ref filter_class) = pagination.class_name {
                    if &job.class_name != filter_class {
                        continue;
                    }
                }
                if let Some(ref filter_queue) = pagination.queue_name {
                    if &execution.queue_name != filter_queue {
                        continue;
                    }
                }

                // Calculate waiting time
                let waiting_time = match now.signed_duration_since(execution.created_at) {
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

                blocked_jobs.push(BlockedJobInfo {
                    id: execution.id,
                    job_id: execution.job_id,
                    queue_name: execution.queue_name.clone(),
                    class_name: job.class_name.clone(),
                    concurrency_key: execution.concurrency_key.clone(),
                    created_at: Self::format_naive_datetime(execution.created_at),
                    waiting_time,
                    expires_at: Self::format_optional_datetime(Some(execution.expires_at)),
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

        debug!("Fetched blocked jobs in {:?}", start.elapsed());
        info!("Found {} blocked jobs", blocked_jobs.len());

        // Get total number of blocked jobs for pagination
        let start = Instant::now();

        let total_count: u64 = query_builder::blocked_executions::count_all(db, table_config)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        info!("Total blocked jobs count: {}", total_count);

        let total_pages = ((total_count as f64) / (page_size as f64)).ceil() as u64;
        let total_pages = total_pages.max(1);

        if total_pages > 0 && pagination.page > total_pages {
            return Err((StatusCode::NOT_FOUND, "Page not found".to_string()));
        }
        debug!("Fetched count in {:?}", start.elapsed());

        let start = Instant::now();
        let mut context = tera::Context::new();
        context.insert("blocked_jobs", &blocked_jobs);
        context.insert("filter_options", &filter_options);
        context.insert("current_page_num", &pagination.page);
        context.insert("total_pages", &total_pages);
        context.insert("active_page", "blocked-jobs");

        let html = state
            .render_template("blocked-jobs.html", &mut context)
            .await?;
        debug!("Template rendering completed in {:?}", start.elapsed());

        Ok(Html(html))
    }

    // Implement method to unblock a single blocked job
    #[instrument(skip(state), fields(path = "/blocked-jobs/:id/unblock"))]
    pub async fn unblock_job(
        State(state): State<Arc<ControlPlane>>,
        Path(id): Path<i64>,
    ) -> impl IntoResponse {
        let db = state.ctx.get_db().await;
        let db = db.as_ref();
        let table_config = state.ctx.table_config.clone();

        // Use transaction to operate
        db.transaction::<_, (), DbErr>(|txn| {
            let table_config = table_config.clone();
            Box::pin(async move {
                // Find blocked execution using query_builder
                let _blocked_execution =
                    query_builder::blocked_executions::find_by_id(txn, &table_config, id)
                        .await?
                        .ok_or_else(|| {
                            DbErr::Custom(format!("Blocked execution with ID {} not found", id))
                        })?;

                // Delete blocked execution using query_builder
                query_builder::blocked_executions::delete_by_id(txn, &table_config, id).await?;

                info!("Unblocked job ID: {}", id);
                Ok(())
            })
        })
        .await
        .map(|_| (StatusCode::SEE_OTHER, "/blocked-jobs".to_string()))
        .map_err(|e| {
            error!("Failed to unblock job {}: {}", id, e);
            match e.to_string() {
                s if s.contains("not found") => {
                    (StatusCode::NOT_FOUND, "/blocked-jobs".to_string())
                }
                _ => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "/blocked-jobs".to_string(),
                ),
            }
        })
    }

    // Implement method to cancel a single blocked job
    #[instrument(skip(state), fields(path = "/blocked-jobs/:id/cancel"))]
    pub async fn cancel_blocked_job(
        State(state): State<Arc<ControlPlane>>,
        Path(id): Path<i64>,
    ) -> impl IntoResponse {
        let db = state.ctx.get_db().await;
        let db = db.as_ref();
        let table_config = state.ctx.table_config.clone();

        // Use transaction to operate
        db.transaction::<_, (), DbErr>(|txn| {
            let table_config = table_config.clone();
            Box::pin(async move {
                // Find blocked execution using query_builder
                let blocked_execution =
                    query_builder::blocked_executions::find_by_id(txn, &table_config, id)
                        .await?
                        .ok_or_else(|| {
                            DbErr::Custom(format!("Blocked execution with ID {} not found", id))
                        })?;

                let job_id = blocked_execution.job_id;

                // Delete blocked execution using query_builder
                query_builder::blocked_executions::delete_by_id(txn, &table_config, id).await?;

                // Delete related job using query_builder
                query_builder::jobs::delete_by_id(txn, &table_config, job_id).await?;

                info!("Cancelled blocked job ID: {}, job ID: {}", id, job_id);
                Ok(())
            })
        })
        .await
        .map(|_| (StatusCode::SEE_OTHER, "/blocked-jobs".to_string()))
        .map_err(|e| {
            error!("Failed to cancel blocked job {}: {}", id, e);
            match e.to_string() {
                s if s.contains("not found") => {
                    (StatusCode::NOT_FOUND, "/blocked-jobs".to_string())
                }
                _ => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "/blocked-jobs".to_string(),
                ),
            }
        })
    }

    pub async fn unblock_all_jobs(State(state): State<Arc<ControlPlane>>) -> impl IntoResponse {
        let db = state.ctx.get_db().await;
        let db = db.as_ref();
        let table_config = state.ctx.table_config.clone();

        // Use transaction to operate
        db.transaction::<_, u64, DbErr>(|txn| {
            let table_config = table_config.clone();
            Box::pin(async move {
                // Delete all blocked executions using query_builder
                let count =
                    query_builder::blocked_executions::delete_all(txn, &table_config).await?;

                info!("Unblocked all {} blocked jobs", count);
                Ok(count)
            })
        })
        .await
        .map(|_count| (StatusCode::SEE_OTHER, "/blocked-jobs".to_string()))
        .map_err(|e| {
            error!("Failed to unblock all jobs: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "/blocked-jobs".to_string(),
            )
        })
    }
}
