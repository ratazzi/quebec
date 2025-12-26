use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::Html,
};
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, info};

use crate::control_plane::{
    models::{FilterOptions, FinishedJobInfo, Pagination},
    ControlPlane,
};
use crate::query_builder;

impl ControlPlane {
    pub async fn finished_jobs(
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

        // Get completed jobs using query_builder
        let finished_jobs_models =
            query_builder::jobs::find_finished_paginated(db, table_config, offset, page_size)
                .await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        // Create a vector to store completed job information
        let mut finished_jobs: Vec<FinishedJobInfo> =
            Vec::with_capacity(finished_jobs_models.len());

        // Get information for each completed job
        for job in finished_jobs_models {
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
            if let Some(finished_at) = job.finished_at {
                // Calculate runtime
                let runtime = match finished_at.signed_duration_since(job.created_at) {
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

        debug!("Fetched finished jobs in {:?}", start.elapsed());
        info!("Found {} finished jobs", finished_jobs.len());

        // Get total number of completed jobs for pagination using query_builder
        let start = Instant::now();

        let total_count: u64 = query_builder::jobs::count_finished(db, table_config)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        info!("Total finished jobs count: {}", total_count);

        let total_pages = ((total_count as f64) / (page_size as f64)).ceil() as u64;
        let total_pages = total_pages.max(1);

        if total_pages > 0 && pagination.page > total_pages {
            return Err((StatusCode::NOT_FOUND, "Page not found".to_string()));
        }
        debug!("Fetched count in {:?}", start.elapsed());

        let start = Instant::now();
        let mut context = tera::Context::new();
        context.insert("finished_jobs", &finished_jobs);
        context.insert("filter_options", &filter_options);
        context.insert("current_page_num", &pagination.page);
        context.insert("total_pages", &total_pages);
        context.insert("active_page", "finished-jobs");

        let html = state
            .render_template("finished-jobs.html", &mut context)
            .await?;
        debug!("Template rendering completed in {:?}", start.elapsed());

        Ok(Html(html))
    }
}
