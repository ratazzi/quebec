use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::Html,
};
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, info};

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
                // Find process information (hostname:pid)
                let worker_info = match execution.process_id {
                    Some(pid) => {
                        let process = processes.iter().find(|p| p.id == pid);
                        match process {
                            Some(p) => {
                                format!("{}:{}", p.hostname.as_deref().unwrap_or("unknown"), p.pid)
                            }
                            None => format!("pid:{}", pid),
                        }
                    }
                    None => "unknown".to_string(),
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
                    worker_info,
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
}
