use std::sync::Arc;
use std::time::Instant;
use axum::{
    extract::{State, Query, Path},
    response::{Html, IntoResponse},
    http::StatusCode,
};
use sea_orm::{EntityTrait, DbErr, TransactionTrait, ActiveModelTrait, ActiveValue, PaginatorTrait, QuerySelect, ConnectionTrait};
use tracing::{debug, info, error, instrument};

use crate::entities::{solid_queue_jobs, solid_queue_claimed_executions, solid_queue_processes};
use crate::control_plane::{ControlPlane, models::{Pagination, InProgressJobInfo}};

impl ControlPlane {
    pub async fn in_progress_jobs(
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

        // Get claimed (in-progress) jobs
        let claimed_executions = solid_queue_claimed_executions::Entity::find()
            .offset(offset)
            .limit(page_size)
            .all(db)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        // Get process information
        let processes = solid_queue_processes::Entity::find()
            .all(db)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        // Create a vector to store in-progress job information
        let mut in_progress_jobs: Vec<InProgressJobInfo> = Vec::with_capacity(claimed_executions.len());

        // Get current time, for calculating runtime
        let now = chrono::Utc::now().naive_utc();

        // Get job information for each in-progress execution
        for execution in claimed_executions {
            if let Ok(Some(job)) = solid_queue_jobs::Entity::find_by_id(execution.job_id).one(db).await {
                // Find process information
                let worker_id = match execution.process_id {
                    Some(pid) => {
                        let process = processes.iter().find(|p| p.id == pid);
                        process.map_or(pid, |p| p.id)
                    },
                    None => 0,
                };

                // Calculate runtime
                let runtime = match now.signed_duration_since(execution.created_at) {
                    duration if duration.num_hours() >= 1 => {
                        format!("{}h {}m", duration.num_hours(), duration.num_minutes() % 60)
                    },
                    duration if duration.num_minutes() >= 1 => {
                        format!("{}m {}s", duration.num_minutes(), duration.num_seconds() % 60)
                    },
                    duration => {
                        format!("{}s", duration.num_seconds())
                    },
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

        debug!("Fetched in-progress jobs in {:?}", start.elapsed());
        info!("Found {} in-progress jobs", in_progress_jobs.len());

        // Get total number of in-progress jobs for pagination
        let start = Instant::now();

        let total_count: u64 = solid_queue_claimed_executions::Entity::find()
            .count(db)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        info!("Total in-progress jobs count: {}", total_count);

        let total_pages = ((total_count as f64) / (page_size as f64)).ceil() as u64;
        let total_pages = total_pages.max(1);

        if total_pages > 0 && pagination.page > total_pages {
            return Err((StatusCode::NOT_FOUND, "Page not found".to_string()));
        }
        debug!("Fetched count in {:?}", start.elapsed());

        // Use abstract method to get all queue names and job classes
        let queue_names = state.get_queue_names()
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        let job_classes = state.get_job_classes()
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        let start = Instant::now();
        let mut context = tera::Context::new();
        context.insert("in_progress_jobs", &in_progress_jobs);
        context.insert("current_page_num", &pagination.page);
        context.insert("total_pages", &total_pages);
        context.insert("active_page", "in-progress-jobs");
        context.insert("queue_names", &queue_names);
        context.insert("job_classes", &job_classes);

        let html = state.render_template("in-progress-jobs.html", &mut context).await?;
        debug!("Template rendering completed in {:?}", start.elapsed());

        Ok(Html(html))
    }

    // Implement method to cancel a single in-progress job
    #[instrument(skip(state), fields(path = "/in-progress-jobs/:id/cancel"))]
    pub async fn cancel_in_progress_job(
        State(state): State<Arc<ControlPlane>>,
        Path(id): Path<i64>,
    ) -> impl IntoResponse {
        let db = state.ctx.get_db().await;
        let db = db.as_ref();

        // Use transaction to operate
        db.transaction::<_, (), DbErr>(|txn| {
            Box::pin(async move {
                // Find execution record to cancel
                let claimed_execution = solid_queue_claimed_executions::Entity::find_by_id(id)
                    .one(txn)
                    .await?;

                if let Some(execution) = claimed_execution {
                    // First mark job as completed
                    let job_result = solid_queue_jobs::Entity::find_by_id(execution.job_id)
                        .one(txn)
                        .await?;

                    if let Some(job) = job_result {
                        // Update job status to completed
                        let mut job_model: solid_queue_jobs::ActiveModel = job.into();
                        job_model.finished_at = ActiveValue::Set(Some(chrono::Utc::now().naive_utc()));
                        job_model.updated_at = ActiveValue::Set(chrono::Utc::now().naive_utc());
                        job_model.update(txn).await?;
                    }

                    // Delete claimed_execution record
                    solid_queue_claimed_executions::Entity::delete_by_id(id)
                        .exec(txn)
                        .await?;

                    info!("Cancelled in-progress job ID: {}", id);
                } else {
                    return Err(DbErr::Custom(format!("In-progress job with ID {} not found", id)));
                }

                Ok(())
            })
        })
        .await
        .map(|_| {
            info!("Cancelled in-progress job ID: {}", id);
            (StatusCode::SEE_OTHER, [("Location", "/in-progress-jobs")])
        })
        .map_err(|e| {
            error!("Failed to cancel in-progress job {}: {}", id, e);
            (StatusCode::INTERNAL_SERVER_ERROR, [("Location", "/in-progress-jobs")])
        })
    }

    pub async fn cancel_all_in_progress_jobs(
        State(state): State<Arc<ControlPlane>>,
    ) -> impl IntoResponse {
        let db = state.ctx.get_db().await;
        let db = db.as_ref();

        // Use transaction to operate
        db.transaction::<_, u64, DbErr>(|txn| {
            Box::pin(async move {
                // Get all in-progress jobs
                let claimed_executions = solid_queue_claimed_executions::Entity::find()
                    .all(txn)
                    .await?;

                if claimed_executions.is_empty() {
                    return Ok(0);
                }

                let job_ids: Vec<i64> = claimed_executions.iter()
                    .map(|execution| execution.job_id)
                    .collect();

                // Update all related jobs to completed status
                let now = chrono::Utc::now().naive_utc();

                // Use batch update
                let update_sql = r#"
                    UPDATE solid_queue_jobs
                    SET finished_at = $1, updated_at = $1
                    WHERE id = ANY($2)
                "#;

                let stmt = sea_orm::Statement::from_sql_and_values(
                    sea_orm::DbBackend::Postgres,
                    update_sql,
                    [now.into(), job_ids.into()]
                );

                txn.execute(stmt).await?;

                // Delete all claimed_execution records
                let delete_result = solid_queue_claimed_executions::Entity::delete_many()
                    .exec(txn)
                    .await?;

                let count = delete_result.rows_affected;
                info!("Cancelled all {} in-progress jobs", count);
                Ok(count)
            })
        })
        .await
        .map(|count| {
            info!("Cancelled all {} in-progress jobs", count);
            (StatusCode::SEE_OTHER, [("Location", "/in-progress-jobs")])
        })
        .map_err(|e| {
            error!("Failed to cancel all in-progress jobs: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, [("Location", "/in-progress-jobs")])
        })
    }
}
