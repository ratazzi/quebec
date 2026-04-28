use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{Html, IntoResponse, Response},
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
    ) -> Result<Response, (StatusCode, String)> {
        let start = Instant::now();
        let db = state
            .ctx
            .get_db()
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        let db = db.as_ref();
        let table_config = &state.ctx.table_config;
        debug!("Database connection obtained in {:?}", start.elapsed());

        let start = Instant::now();

        // Calculate page offset
        let page_size = state.page_size;
        let offset = (pagination.page - 1) * page_size;

        // Get blocked jobs using query_builder (filters applied at SQL level)
        let blocked_executions = query_builder::blocked_executions::find_paginated(
            db,
            table_config,
            offset,
            page_size,
            pagination.class_name.as_deref(),
            pagination.queue_name.as_deref(),
        )
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

        let total_count: u64 = query_builder::blocked_executions::count_all(
            db,
            table_config,
            pagination.class_name.as_deref(),
            pagination.queue_name.as_deref(),
        )
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        info!("Total blocked jobs count: {}", total_count);

        let total_pages = ((total_count as f64) / (page_size as f64)).ceil() as u64;
        let total_pages = total_pages.max(1);

        // Clamp out-of-range page to the last valid page (preserving filters) so
        // actions taken on the final row don't bounce users onto a dead page.
        if pagination.page > total_pages {
            let mut ser = url::form_urlencoded::Serializer::new(String::new());
            ser.append_pair("page", &total_pages.to_string());
            if let Some(cn) = pagination.class_name.as_deref() {
                ser.append_pair("class_name", cn);
            }
            if let Some(qn) = pagination.queue_name.as_deref() {
                ser.append_pair("queue_name", qn);
            }
            let target = format!("{}/blocked-jobs?{}", state.base_path, ser.finish());
            return Ok(Self::redirect_back(&target));
        }
        debug!("Fetched count in {:?}", start.elapsed());

        let start = Instant::now();
        let mut context = tera::Context::new();
        context.insert("blocked_jobs", &blocked_jobs);
        context.insert("filter_options", &filter_options);
        context.insert("current_page_num", &pagination.page);
        context.insert("total_pages", &total_pages);
        context.insert("active_page", "blocked-jobs");
        context.insert("filter_class_name", &pagination.class_name);
        context.insert("filter_queue_name", &pagination.queue_name);

        let html = state
            .render_template("blocked-jobs.html", &mut context)
            .await?;
        debug!("Template rendering completed in {:?}", start.elapsed());

        Ok(Html(html).into_response())
    }

    #[instrument(skip(state), fields(path = "/blocked-jobs/:id/unblock"))]
    pub async fn unblock_job(
        State(state): State<Arc<ControlPlane>>,
        headers: HeaderMap,
        Path(id): Path<i64>,
    ) -> Response {
        let db = match state.ctx.get_db().await {
            Ok(db) => db,
            Err(e) => {
                error!("Failed to get db connection: {}", e);
                return Self::error_response();
            }
        };
        let db = db.as_ref();
        let table_config = state.ctx.table_config.clone();
        let redirect = Self::referer_or(&headers, "/blocked-jobs");

        match db
            .transaction::<_, (), DbErr>(|txn| {
                let table_config = table_config.clone();
                Box::pin(async move {
                    let _blocked_execution =
                        query_builder::blocked_executions::find_by_id(txn, &table_config, id)
                            .await?
                            .ok_or_else(|| {
                                DbErr::Custom(format!("Blocked execution with ID {id} not found"))
                            })?;

                    query_builder::blocked_executions::delete_by_id(txn, &table_config, id).await?;
                    Ok(())
                })
            })
            .await
        {
            Ok(_) => {
                info!("Unblocked job ID: {}", id);
                Self::redirect_back(&redirect)
            }
            Err(e) if e.to_string().contains("not found") => {
                info!("Blocked job {} already gone: {}", id, e);
                Self::not_found_response()
            }
            Err(e) => {
                error!("Failed to unblock job {}: {}", id, e);
                Self::error_response()
            }
        }
    }

    #[instrument(skip(state), fields(path = "/blocked-jobs/:id/cancel"))]
    pub async fn cancel_blocked_job(
        State(state): State<Arc<ControlPlane>>,
        headers: HeaderMap,
        Path(id): Path<i64>,
    ) -> Response {
        let db = match state.ctx.get_db().await {
            Ok(db) => db,
            Err(e) => {
                error!("Failed to get db connection: {}", e);
                return Self::error_response();
            }
        };
        let db = db.as_ref();
        let table_config = state.ctx.table_config.clone();
        let redirect = Self::referer_or(&headers, "/blocked-jobs");

        match db
            .transaction::<_, i64, DbErr>(|txn| {
                let table_config = table_config.clone();
                Box::pin(async move {
                    let blocked_execution =
                        query_builder::blocked_executions::find_by_id(txn, &table_config, id)
                            .await?
                            .ok_or_else(|| {
                                DbErr::Custom(format!("Blocked execution with ID {id} not found"))
                            })?;

                    let job_id = blocked_execution.job_id;
                    query_builder::blocked_executions::delete_by_id(txn, &table_config, id).await?;
                    query_builder::jobs::delete_by_id(txn, &table_config, job_id).await?;
                    Ok(job_id)
                })
            })
            .await
        {
            Ok(job_id) => {
                info!("Cancelled blocked job ID: {}, job ID: {}", id, job_id);
                Self::redirect_back(&redirect)
            }
            Err(e) if e.to_string().contains("not found") => {
                info!("Blocked job {} already gone: {}", id, e);
                Self::not_found_response()
            }
            Err(e) => {
                error!("Failed to cancel blocked job {}: {}", id, e);
                Self::error_response()
            }
        }
    }

    #[instrument(skip(state), fields(path = "/blocked-jobs/all/unblock"))]
    pub async fn unblock_all_jobs(
        State(state): State<Arc<ControlPlane>>,
        Query(pagination): Query<Pagination>,
        headers: HeaderMap,
    ) -> Response {
        let db = match state.ctx.get_db().await {
            Ok(db) => db,
            Err(e) => {
                error!("Failed to get db connection: {}", e);
                return Self::error_response();
            }
        };
        let db = db.as_ref();
        let table_config = state.ctx.table_config.clone();
        let redirect = Self::referer_without_page_or(&headers, "/blocked-jobs");
        let class_name = pagination.class_name.clone();
        let queue_name = pagination.queue_name.clone();

        match db
            .transaction::<_, u64, DbErr>(|txn| {
                let table_config = table_config.clone();
                let class_name = class_name.clone();
                let queue_name = queue_name.clone();
                Box::pin(async move {
                    let count = query_builder::blocked_executions::delete_all(
                        txn,
                        &table_config,
                        class_name.as_deref(),
                        queue_name.as_deref(),
                    )
                    .await?;
                    Ok(count)
                })
            })
            .await
        {
            Ok(count) => {
                info!("Unblocked all {} blocked jobs", count);
                Self::redirect_back(&redirect)
            }
            Err(e) => {
                error!("Failed to unblock all jobs: {}", e);
                Self::error_response()
            }
        }
    }
}
