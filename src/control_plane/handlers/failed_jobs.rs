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
    ) -> Result<Html<String>, (StatusCode, String)> {
        let start = Instant::now();
        let db = state.ctx.get_db().await;
        let db = db.as_ref();
        let table_config = &state.ctx.table_config;
        debug!("Database connection obtained in {:?}", start.elapsed());

        let start = Instant::now();

        // Calculate pagination offset
        let page_size = state.page_size;
        let offset = (pagination.page - 1) * page_size;

        // Get failed execution records using query_builder
        let failed_executions =
            query_builder::failed_executions::find_paginated(db, table_config, offset, page_size)
                .await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        // Create a vector to store failed job information
        let mut failed_jobs: Vec<FailedJobInfo> = Vec::with_capacity(failed_executions.len());

        // Get job information for each failed execution
        for execution in failed_executions {
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
                    if &job.queue_name != filter_queue {
                        continue;
                    }
                }

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

        // Execute count query using query_builder
        let total_count: u64 = query_builder::failed_executions::count_all(db, table_config)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        info!("Total failed jobs count: {}", total_count);

        let total_pages = ((total_count as f64) / (page_size as f64)).ceil() as u64;
        let total_pages = total_pages.max(1);

        if total_pages > 0 && pagination.page > total_pages {
            return Err((StatusCode::NOT_FOUND, "Page not found".to_string()));
        }
        debug!("Fetched count in {:?}", start.elapsed());

        let start = Instant::now();
        let mut context = tera::Context::new();
        context.insert("failed_jobs", &failed_jobs);
        context.insert("filter_options", &filter_options);
        context.insert("current_page_num", &pagination.page);
        context.insert("total_pages", &total_pages);
        context.insert("active_page", "failed-jobs");

        // Use helper method to render template
        let html = state
            .render_template("failed-jobs.html", &mut context)
            .await?;

        debug!("Template rendering completed in {:?}", start.elapsed());

        Ok(Html(html))
    }

    #[instrument(skip(state), fields(path = "/failed-jobs/:id/retry"))]
    pub async fn retry_failed_job(
        State(state): State<Arc<ControlPlane>>,
        Path(id): Path<i64>,
    ) -> impl IntoResponse {
        let db = state.ctx.get_db().await;
        let db = db.as_ref();
        let table_config = state.ctx.table_config.clone();

        // Use transaction for operation
        db.transaction::<_, (), DbErr>(|txn| {
            let table_config = table_config.clone();
            Box::pin(async move {
                // First get failed execution record using query_builder
                let failed_execution =
                    query_builder::failed_executions::find_by_job_id(txn, &table_config, id)
                        .await?;

                match failed_execution {
                    Some(execution) => {
                        // Use Retryable trait's retry method
                        execution.retry(txn, &table_config).await?;
                        Ok(())
                    }
                    None => Err(DbErr::Custom(format!(
                        "Failed execution for job {} not found",
                        id
                    ))),
                }
            })
        })
        .await
        .map(|_| {
            info!("Retried failed job {}", id);
            (StatusCode::SEE_OTHER, [("Location", "/failed-jobs")])
        })
        .map_err(|e| {
            error!("Failed to retry job {}: {}", id, e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                [("Location", "/failed-jobs")],
            )
        })
    }

    #[instrument(skip(state), fields(path = "/failed-jobs/:id/delete"))]
    pub async fn delete_failed_job(
        State(state): State<Arc<ControlPlane>>,
        Path(id): Path<i64>,
    ) -> impl IntoResponse {
        let db = state.ctx.get_db().await;
        let db = db.as_ref();
        let table_config = state.ctx.table_config.clone();

        // Use transaction for operation
        db.transaction::<_, (), DbErr>(|txn| {
            let table_config = table_config.clone();
            Box::pin(async move {
                // First get failed execution record using query_builder
                let failed_execution =
                    query_builder::failed_executions::find_by_job_id(txn, &table_config, id)
                        .await?;

                match failed_execution {
                    Some(execution) => {
                        // Use Discardable trait's discard method
                        execution.discard(txn, &table_config).await?;
                        Ok(())
                    }
                    None => Err(DbErr::Custom(format!(
                        "Failed execution for job {} not found",
                        id
                    ))),
                }
            })
        })
        .await
        .map(|_| {
            info!("Deleted failed job {}", id);
            (StatusCode::SEE_OTHER, [("Location", "/failed-jobs")])
        })
        .map_err(|e| {
            error!("Failed to delete job {}: {}", id, e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                [("Location", "/failed-jobs")],
            )
        })
    }

    #[instrument(skip(state), fields(path = "/failed-jobs/all/retry"))]
    pub async fn retry_all_failed_jobs(
        State(state): State<Arc<ControlPlane>>,
    ) -> impl IntoResponse {
        let db = state.ctx.get_db().await;
        let db = db.as_ref();
        let table_config = state.ctx.table_config.clone();

        // Use transaction for operation
        db.transaction::<_, u64, DbErr>(|txn| {
            Box::pin(async move {
                // Use Retryable trait's retry_all method
                let count = FailedExecutionEntity.retry_all(txn, &table_config).await?;
                Ok(count)
            })
        })
        .await
        .map(|count| {
            info!("Retried all {} failed jobs", count);
            (StatusCode::SEE_OTHER, [("Location", "/failed-jobs")])
        })
        .map_err(|e| {
            error!("Failed to retry all jobs: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                [("Location", "/failed-jobs")],
            )
        })
    }

    #[instrument(skip(state), fields(path = "/failed-jobs/all/delete"))]
    pub async fn discard_all_failed_jobs(
        State(state): State<Arc<ControlPlane>>,
    ) -> impl IntoResponse {
        let db = state.ctx.get_db().await;
        let db = db.as_ref();
        let table_config = state.ctx.table_config.clone();

        // Use transaction for operation
        db.transaction::<_, u64, DbErr>(|txn| {
            Box::pin(async move {
                // Use Discardable trait's discard_all method
                let count = FailedExecutionEntity
                    .discard_all(txn, &table_config)
                    .await?;
                Ok(count)
            })
        })
        .await
        .map(|count| {
            info!("Discarded all {} failed jobs", count);
            (StatusCode::SEE_OTHER, [("Location", "/failed-jobs")])
        })
        .map_err(|e| {
            error!("Failed to discard all jobs: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                [("Location", "/failed-jobs")],
            )
        })
    }
}
