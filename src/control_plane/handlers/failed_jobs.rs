use std::sync::Arc;
use std::time::Instant;
use axum::{
    extract::{State, Query, Path},
    response::{Html, IntoResponse},
    http::StatusCode,
};
use sea_orm::{EntityTrait, QueryFilter, ColumnTrait, DbErr, TransactionTrait, PaginatorTrait, QuerySelect};
use tracing::{debug, info, error, instrument};

use crate::entities::{solid_queue_jobs, solid_queue_failed_executions};
use crate::control_plane::{ControlPlane, models::{Pagination, FailedJobInfo}};

impl ControlPlane {
    pub async fn failed_jobs(
        State(state): State<Arc<ControlPlane>>,
        Query(pagination): Query<Pagination>,
    ) -> Result<Html<String>, (StatusCode, String)> {
        let start = Instant::now();
        let db = state.ctx.get_db().await;
        let db = db.as_ref();
        debug!("Database connection obtained in {:?}", start.elapsed());

        let start = Instant::now();

        // Calculate pagination offset
        let page_size = state.page_size;
        let offset = (pagination.page - 1) * page_size;

        // Get failed execution records
        let failed_executions = solid_queue_failed_executions::Entity::find()
            .offset(offset)
            .limit(page_size)
            .all(db)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        // Create a vector to store failed job information
        let mut failed_jobs: Vec<FailedJobInfo> = Vec::with_capacity(failed_executions.len());

        // Get job information for each failed execution
        for execution in failed_executions {
            if let Ok(Some(job)) = solid_queue_jobs::Entity::find_by_id(execution.job_id).one(db).await {
                failed_jobs.push(FailedJobInfo {
                    id: job.id,
                    queue_name: job.queue_name.clone(),
                    class_name: job.class_name.clone(),
                    error: execution.error.unwrap_or_default(),
                    failed_at: Self::format_naive_datetime(execution.created_at),
                });
            }
        }

        debug!("Fetched failed jobs in {:?}", start.elapsed());
        info!("Found {} failed jobs", failed_jobs.len());

        // Get total count of failed jobs for pagination calculation
        let start = Instant::now();

        // Execute SQL count query directly
        let total_count: u64 = solid_queue_failed_executions::Entity::find()
            .count(db)
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
        context.insert("current_page_num", &pagination.page);
        context.insert("total_pages", &total_pages);
        context.insert("active_page", "failed-jobs");

        // Use helper method to render template
        let html = state.render_template("failed-jobs.html", &mut context).await?;

        debug!("Template rendering completed in {:?}", start.elapsed());

        Ok(Html(html))
    }

    #[instrument(skip(state), fields(path = "/failed-jobs/:id/retry"))]
    pub async fn retry_failed_job(
        State(state): State<Arc<ControlPlane>>,
        Path(id): Path<i64>,
    ) -> impl IntoResponse {
        use crate::entities::solid_queue_failed_executions::{Entity as FailedExecutionEntity, Retryable};

        let db = state.ctx.get_db().await;
        let db = db.as_ref();

        // Use transaction for operation
        db.transaction::<_, (), DbErr>(|txn| {
            Box::pin(async move {
                // First get failed execution record
                let failed_execution = FailedExecutionEntity::find()
                    .filter(solid_queue_failed_executions::Column::JobId.eq(id))
                    .one(txn)
                    .await?;

                match failed_execution {
                    Some(execution) => {
                        // Use Retryable trait's retry method
                        execution.retry(txn).await?;
                        Ok(())
                    },
                    None => {
                        Err(DbErr::Custom(format!("Failed execution for job {} not found", id)))
                    }
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
            (StatusCode::INTERNAL_SERVER_ERROR, [("Location", "/failed-jobs")])
        })
    }

    #[instrument(skip(state), fields(path = "/failed-jobs/:id/delete"))]
    pub async fn delete_failed_job(
        State(state): State<Arc<ControlPlane>>,
        Path(id): Path<i64>,
    ) -> impl IntoResponse {
        use crate::entities::solid_queue_failed_executions::{Entity as FailedExecutionEntity, Discardable};

        let db = state.ctx.get_db().await;
        let db = db.as_ref();

        // Use transaction for operation
        db.transaction::<_, (), DbErr>(|txn| {
            Box::pin(async move {
                // First get failed execution record
                let failed_execution = FailedExecutionEntity::find()
                    .filter(solid_queue_failed_executions::Column::JobId.eq(id))
                    .one(txn)
                    .await?;

                match failed_execution {
                    Some(execution) => {
                        // Use Discardable trait's discard method
                        execution.discard(txn).await?;
                        Ok(())
                    },
                    None => {
                        Err(DbErr::Custom(format!("Failed execution for job {} not found", id)))
                    }
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
            (StatusCode::INTERNAL_SERVER_ERROR, [("Location", "/failed-jobs")])
        })
    }

    #[instrument(skip(state), fields(path = "/failed-jobs/all/retry"))]
    pub async fn retry_all_failed_jobs(
        State(state): State<Arc<ControlPlane>>,
    ) -> impl IntoResponse {
        use crate::entities::solid_queue_failed_executions::{Entity as FailedExecutionEntity, Retryable};

        let db = state.ctx.get_db().await;
        let db = db.as_ref();

        // Use transaction for operation
        db.transaction::<_, u64, DbErr>(|txn| {
            Box::pin(async move {
                // Use Retryable trait's retry_all method
                let count = FailedExecutionEntity.retry_all(txn).await?;
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
            (StatusCode::INTERNAL_SERVER_ERROR, [("Location", "/failed-jobs")])
        })
    }

    #[instrument(skip(state), fields(path = "/failed-jobs/all/delete"))]
    pub async fn discard_all_failed_jobs(
        State(state): State<Arc<ControlPlane>>,
    ) -> impl IntoResponse {
        use crate::entities::solid_queue_failed_executions::{Entity as FailedExecutionEntity, Discardable};

        let db = state.ctx.get_db().await;
        let db = db.as_ref();

        // Use transaction for operation
        db.transaction::<_, u64, DbErr>(|txn| {
            Box::pin(async move {
                // Use Discardable trait's discard_all method
                let count = FailedExecutionEntity.discard_all(txn).await?;
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
            (StatusCode::INTERNAL_SERVER_ERROR, [("Location", "/failed-jobs")])
        })
    }
}