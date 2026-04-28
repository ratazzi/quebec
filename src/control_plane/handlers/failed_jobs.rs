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
    models::{FailedJobInfo, FilterOptions, Pagination},
    ControlPlane,
};
use crate::entities::quebec_failed_executions::{
    Discardable, Entity as FailedExecutionEntity, Retryable,
};
use crate::query_builder;

impl ControlPlane {
    pub async fn failed_jobs(
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

        // Calculate pagination offset
        let page_size = state.page_size;
        let offset = (pagination.page - 1) * page_size;

        // Get failed execution records using query_builder (with SQL-level filtering)
        let failed_executions = query_builder::failed_executions::find_paginated(
            db,
            table_config,
            offset,
            page_size,
            pagination.class_name.as_deref(),
            pagination.queue_name.as_deref(),
        )
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        // Create a vector to store failed job information
        let mut failed_jobs: Vec<FailedJobInfo> = Vec::with_capacity(failed_executions.len());

        // Get job information for each failed execution
        for execution in failed_executions {
            if let Ok(Some(job)) =
                query_builder::jobs::find_by_id(db, table_config, execution.job_id).await
            {
                failed_jobs.push(FailedJobInfo {
                    id: job.id,
                    queue_name: job.queue_name.clone(),
                    class_name: job.class_name.clone(),
                    error: execution.error.unwrap_or_default(),
                    failed_at: Self::format_naive_datetime(execution.created_at),
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

        debug!("Fetched failed jobs in {:?}", start.elapsed());
        info!("Found {} failed jobs", failed_jobs.len());

        // Get total count of failed jobs for pagination calculation
        let start = Instant::now();

        // Execute count query using query_builder (with same filters)
        let total_count: u64 = query_builder::failed_executions::count_all(
            db,
            table_config,
            pagination.class_name.as_deref(),
            pagination.queue_name.as_deref(),
        )
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        info!("Total failed jobs count: {}", total_count);

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
            let target = format!("{}/failed-jobs?{}", state.base_path, ser.finish());
            return Ok(Self::redirect_back(&target));
        }
        debug!("Fetched count in {:?}", start.elapsed());

        let start = Instant::now();
        let mut context = tera::Context::new();
        context.insert("failed_jobs", &failed_jobs);
        context.insert("filter_options", &filter_options);
        context.insert("current_page_num", &pagination.page);
        context.insert("total_pages", &total_pages);
        context.insert("active_page", "failed-jobs");
        context.insert("filter_class_name", &pagination.class_name);
        context.insert("filter_queue_name", &pagination.queue_name);

        // Use helper method to render template
        let html = state
            .render_template("failed-jobs.html", &mut context)
            .await?;

        debug!("Template rendering completed in {:?}", start.elapsed());

        Ok(Html(html).into_response())
    }

    #[instrument(skip(state), fields(path = "/failed-jobs/:id/retry"))]
    pub async fn retry_failed_job(
        State(state): State<Arc<ControlPlane>>,
        headers: HeaderMap,
        Path(id): Path<i64>,
    ) -> Response {
        let db = match state.ctx.get_db().await {
            Ok(db) => db,
            Err(_) => return Self::error_response(),
        };
        let db = db.as_ref();
        let table_config = state.ctx.table_config.clone();
        let redirect = Self::referer_or(&headers, "/failed-jobs");

        match db
            .transaction::<_, (), DbErr>(|txn| {
                let table_config = table_config.clone();
                Box::pin(async move {
                    let failed_execution =
                        query_builder::failed_executions::find_by_job_id(txn, &table_config, id)
                            .await?;

                    match failed_execution {
                        Some(execution) => {
                            execution.retry(txn, &table_config).await?;
                            Ok(())
                        }
                        None => Err(DbErr::Custom(format!(
                            "Failed execution for job {id} not found"
                        ))),
                    }
                })
            })
            .await
        {
            Ok(_) => {
                info!("Retried failed job {}", id);
                Self::redirect_back(&redirect)
            }
            Err(e) if e.to_string().contains("not found") => {
                info!("Failed job {} already gone: {}", id, e);
                Self::not_found_response()
            }
            Err(e) => {
                error!("Failed to retry job {}: {}", id, e);
                Self::error_response()
            }
        }
    }

    #[instrument(skip(state), fields(path = "/failed-jobs/:id/delete"))]
    pub async fn delete_failed_job(
        State(state): State<Arc<ControlPlane>>,
        headers: HeaderMap,
        Path(id): Path<i64>,
    ) -> Response {
        let db = match state.ctx.get_db().await {
            Ok(db) => db,
            Err(_) => return Self::error_response(),
        };
        let db = db.as_ref();
        let table_config = state.ctx.table_config.clone();
        let redirect = Self::referer_or(&headers, "/failed-jobs");

        match db
            .transaction::<_, (), DbErr>(|txn| {
                let table_config = table_config.clone();
                Box::pin(async move {
                    let failed_execution =
                        query_builder::failed_executions::find_by_job_id(txn, &table_config, id)
                            .await?;

                    match failed_execution {
                        Some(execution) => {
                            execution.discard(txn, &table_config).await?;
                            Ok(())
                        }
                        None => Err(DbErr::Custom(format!(
                            "Failed execution for job {id} not found"
                        ))),
                    }
                })
            })
            .await
        {
            Ok(_) => {
                info!("Deleted failed job {}", id);
                Self::redirect_back(&redirect)
            }
            Err(e) if e.to_string().contains("not found") => {
                info!("Failed job {} already gone: {}", id, e);
                Self::not_found_response()
            }
            Err(e) => {
                error!("Failed to delete job {}: {}", id, e);
                Self::error_response()
            }
        }
    }

    #[instrument(skip(state), fields(path = "/failed-jobs/all/retry"))]
    pub async fn retry_all_failed_jobs(
        State(state): State<Arc<ControlPlane>>,
        Query(pagination): Query<Pagination>,
        headers: HeaderMap,
    ) -> Response {
        let db = match state.ctx.get_db().await {
            Ok(db) => db,
            Err(_) => return Self::error_response(),
        };
        let db = db.as_ref();
        let table_config = state.ctx.table_config.clone();
        let redirect = Self::referer_without_page_or(&headers, "/failed-jobs");
        let class_name = pagination.class_name.clone();
        let queue_name = pagination.queue_name.clone();

        match db
            .transaction::<_, u64, DbErr>(|txn| {
                Box::pin(async move {
                    let count = FailedExecutionEntity
                        .retry_all(
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
                info!("Retried all {} failed jobs", count);
                Self::redirect_back(&redirect)
            }
            Err(e) => {
                error!("Failed to retry all jobs: {}", e);
                Self::error_response()
            }
        }
    }

    #[instrument(skip(state), fields(path = "/failed-jobs/all/delete"))]
    pub async fn discard_all_failed_jobs(
        State(state): State<Arc<ControlPlane>>,
        Query(pagination): Query<Pagination>,
        headers: HeaderMap,
    ) -> Response {
        let db = match state.ctx.get_db().await {
            Ok(db) => db,
            Err(_) => return Self::error_response(),
        };
        let db = db.as_ref();
        let table_config = state.ctx.table_config.clone();
        let redirect = Self::referer_without_page_or(&headers, "/failed-jobs");
        let class_name = pagination.class_name.clone();
        let queue_name = pagination.queue_name.clone();

        match db
            .transaction::<_, u64, DbErr>(|txn| {
                Box::pin(async move {
                    let count = FailedExecutionEntity
                        .discard_all(
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
                info!("Discarded all {} failed jobs", count);
                Self::redirect_back(&redirect)
            }
            Err(e) => {
                error!("Failed to discard all jobs: {}", e);
                Self::error_response()
            }
        }
    }
}
