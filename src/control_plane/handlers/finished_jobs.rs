use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::Html,
};
use sea_orm::{ColumnTrait, EntityTrait, PaginatorTrait, QueryFilter, QueryOrder, QuerySelect};
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, info};

use crate::control_plane::{
    models::{FinishedJobInfo, Pagination},
    ControlPlane,
};
use crate::entities::quebec_jobs;

impl ControlPlane {
    pub async fn finished_jobs(
        State(state): State<Arc<ControlPlane>>, Query(pagination): Query<Pagination>,
    ) -> Result<Html<String>, (StatusCode, String)> {
        let start = Instant::now();
        let db = state.ctx.get_db().await;
        let db = db.as_ref();
        debug!("Database connection obtained in {:?}", start.elapsed());

        let start = Instant::now();

        // Calculate page offset
        let page_size = state.page_size;
        let offset = (pagination.page - 1) * page_size;

        // Get completed jobs
        let finished_jobs_query = quebec_jobs::Entity::find()
            .filter(quebec_jobs::Column::FinishedAt.is_not_null())
            .order_by_desc(quebec_jobs::Column::FinishedAt)
            .offset(offset)
            .limit(page_size);

        let finished_jobs_models = finished_jobs_query
            .all(db)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        // Create a vector to store completed job information
        let mut finished_jobs: Vec<FinishedJobInfo> =
            Vec::with_capacity(finished_jobs_models.len());

        // Get information for each completed job
        for job in finished_jobs_models {
            if let Some(finished_at) = job.finished_at {
                // Calculate runtime
                let runtime = match finished_at.signed_duration_since(job.created_at) {
                    duration if duration.num_hours() >= 1 => {
                        format!("{}h {}m", duration.num_hours(), duration.num_minutes() % 60)
                    }
                    duration if duration.num_minutes() >= 1 => {
                        format!("{}m {}s", duration.num_minutes(), duration.num_seconds() % 60)
                    }
                    duration => {
                        format!("{}s", duration.num_seconds())
                    }
                };

                finished_jobs.push(FinishedJobInfo {
                    id: job.id,
                    queue_name: job.queue_name,
                    class_name: job.class_name,
                    created_at: Self::format_naive_datetime(job.created_at),
                    finished_at: Self::format_naive_datetime(finished_at),
                    runtime,
                });
            }
        }

        debug!("Fetched finished jobs in {:?}", start.elapsed());
        info!("Found {} finished jobs", finished_jobs.len());

        // Get total number of completed jobs for pagination
        let start = Instant::now();

        let total_count: u64 = quebec_jobs::Entity::find()
            .filter(quebec_jobs::Column::FinishedAt.is_not_null())
            .count(db)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        info!("Total finished jobs count: {}", total_count);

        let total_pages = ((total_count as f64) / (page_size as f64)).ceil() as u64;
        let total_pages = total_pages.max(1);

        if total_pages > 0 && pagination.page > total_pages {
            return Err((StatusCode::NOT_FOUND, "Page not found".to_string()));
        }
        debug!("Fetched count in {:?}", start.elapsed());

        // Use abstract method to get all queue names and job classes
        let queue_names = state
            .get_queue_names()
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        let job_classes = state
            .get_job_classes()
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        let start = Instant::now();
        let mut context = tera::Context::new();
        context.insert("finished_jobs", &finished_jobs);
        context.insert("current_page_num", &pagination.page);
        context.insert("total_pages", &total_pages);
        context.insert("active_page", "finished-jobs");
        context.insert("queue_names", &queue_names);
        context.insert("job_classes", &job_classes);

        let html = state.render_template("finished-jobs.html", &mut context).await?;
        debug!("Template rendering completed in {:?}", start.elapsed());

        Ok(Html(html))
    }
}
