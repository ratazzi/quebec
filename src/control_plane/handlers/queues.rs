use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{Html, IntoResponse, Response},
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, error};

use crate::control_plane::{
    models::{queue_slug, Pagination, QueueInfo},
    ControlPlane,
};
use crate::query_builder;

impl ControlPlane {
    pub async fn queues(
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
        debug!("Database connection obtained in {:?}", start.elapsed());

        let start = Instant::now();

        // Use Sea-ORM's native SQL query functionality, only count unfinished jobs
        // Get all queue names
        let all_queue_names = state
            .get_queue_names()
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        let table_config = &state.ctx.table_config;

        // Count only ready executions per queue (matching Solid Queue's Queue#size)
        let queue_counts_map = query_builder::ready_executions::count_by_queue(db, table_config)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        // Ensure all queues are included in results, even if they have no unfinished jobs
        let queue_counts: Vec<(String, i64)> = all_queue_names
            .into_iter()
            .map(|queue_name| {
                (
                    queue_name.clone(),
                    *queue_counts_map.get(&queue_name).unwrap_or(&0),
                )
            })
            .collect();

        debug!("Fetched jobs in {:?}", start.elapsed());

        let start = Instant::now();
        // Get paused queues using query_builder
        let paused_queue_names: Vec<String> =
            query_builder::pauses::find_all_queue_names(db, table_config)
                .await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        debug!("Fetched paused queues in {:?}", start.elapsed());

        let start = Instant::now();

        let queue_infos: Vec<QueueInfo> = queue_counts
            .into_iter()
            .map(|(name, count)| QueueInfo {
                slug: queue_slug(&name),
                status: if paused_queue_names.contains(&name) {
                    "paused".to_string()
                } else {
                    "active".to_string()
                },
                concurrency_limit: state.ctx.experimental_queue_concurrency.get(&name).copied(),
                name,
                jobs_count: count,
            })
            .filter(|queue| {
                // Apply status filter if provided
                if let Some(ref filter_status) = pagination.status {
                    &queue.status == filter_status
                } else {
                    true
                }
            })
            .collect();
        debug!("Processed queue data in {:?}", start.elapsed());

        let start = Instant::now();
        let mut context = tera::Context::new();
        context.insert("current_page_num", &pagination.page);
        context.insert("total_pages", &1);
        context.insert("queues", &queue_infos);
        context.insert("active_page", "queues");

        let html = state.render_template("queues.html", &mut context).await?;
        debug!("Template rendering completed in {:?}", start.elapsed());

        Ok(Html(html))
    }

    pub async fn pause_queue(
        State(state): State<Arc<ControlPlane>>,
        Path(queue_name): Path<String>,
    ) -> Response {
        let db = match state.ctx.get_db().await {
            Ok(db) => db,
            Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        };
        let db = db.as_ref();
        let table_config = &state.ctx.table_config;

        // Insert is idempotent (ON CONFLICT DO NOTHING / INSERT OR IGNORE)
        if let Err(e) = query_builder::pauses::insert(
            db,
            table_config,
            &queue_name,
            chrono::Utc::now().naive_utc(),
        )
        .await
        {
            error!("Failed to pause queue: {}", e);
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }

        Self::redirect_back(&format!("/queues/{queue_name}"))
    }

    pub async fn resume_queue(
        State(state): State<Arc<ControlPlane>>,
        Path(queue_name): Path<String>,
    ) -> Response {
        let db = match state.ctx.get_db().await {
            Ok(db) => db,
            Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        };
        let db = db.as_ref();
        let table_config = &state.ctx.table_config;

        // Delete the pause record for this queue using query_builder
        if let Err(e) =
            query_builder::pauses::delete_by_queue_name(db, table_config, &queue_name).await
        {
            error!("Failed to resume queue: {}", e);
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }

        Self::redirect_back(&format!("/queues/{queue_name}"))
    }

    pub async fn pause_all_queues(State(state): State<Arc<ControlPlane>>) -> Response {
        let db = match state.ctx.get_db().await {
            Ok(db) => db,
            Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        };
        let db = db.as_ref();
        let table_config = &state.ctx.table_config;

        // Pause-all is a mutating action that must see the current queue set,
        // so bypass the stale-tolerant cached helper and query jobs directly —
        // otherwise a queue created within the cache TTL would be missed.
        let queue_names = match query_builder::jobs::get_queue_names(db, table_config).await {
            Ok(names) => names,
            Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        };

        let paused = match query_builder::pauses::find_all_queue_names(db, table_config).await {
            Ok(names) => names,
            Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        };

        let now = chrono::Utc::now().naive_utc();
        for name in &queue_names {
            if paused.contains(name) {
                continue;
            }
            if let Err(e) = query_builder::pauses::insert(db, table_config, name, now).await {
                error!("Failed to pause queue {}: {}", name, e);
                return StatusCode::INTERNAL_SERVER_ERROR.into_response();
            }
        }

        Self::redirect_back("/queues")
    }

    pub async fn resume_all_queues(State(state): State<Arc<ControlPlane>>) -> Response {
        let db = match state.ctx.get_db().await {
            Ok(db) => db,
            Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        };
        let db = db.as_ref();
        let table_config = &state.ctx.table_config;

        if let Err(e) = query_builder::pauses::delete_all(db, table_config).await {
            error!("Failed to resume all queues: {}", e);
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }

        Self::redirect_back("/queues")
    }

    pub async fn queue_details(
        State(state): State<Arc<ControlPlane>>,
        Path(queue_name): Path<String>,
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

        let page = pagination.page;
        let offset = (page - 1) * state.page_size;

        // Get queue status
        let is_paused = state
            .is_queue_paused(&queue_name)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        // Count and fetch ready executions for this queue. This matches the
        // /queues list size (and Mission Control's Queue#size + Queue#jobs):
        // only ready/pending jobs belong to a queue's listing. Blocked /
        // scheduled / claimed / failed jobs are surfaced through their own
        // top-nav tabs to avoid the "list shows 0 but detail shows blocked"
        // double-counting confusion.
        let total_jobs =
            query_builder::ready_executions::count_by_queue_name(db, table_config, &queue_name)
                .await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        let ready_rows = query_builder::ready_executions::find_by_queue_paginated(
            db,
            table_config,
            &queue_name,
            offset,
            state.page_size,
        )
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        let job_ids: Vec<i64> = ready_rows.iter().map(|r| r.job_id).collect();
        let jobs = query_builder::jobs::find_by_ids(db, table_config, job_ids)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        let jobs_by_id: HashMap<i64, _> = jobs.into_iter().map(|j| (j.id, j)).collect();

        let queue_jobs: Vec<crate::control_plane::models::QueueJobInfo> = ready_rows
            .iter()
            .filter_map(|ready| {
                jobs_by_id.get(&ready.job_id).map(|job| {
                    crate::control_plane::models::QueueJobInfo {
                        id: job.id,
                        class_name: job.class_name.clone(),
                        priority: job.priority,
                        created_at: Self::format_naive_datetime(job.created_at),
                    }
                })
            })
            .collect();

        // Calculate pagination
        let total_pages = (total_jobs as f64 / state.page_size as f64).ceil() as u64;
        let total_pages = total_pages.max(1);

        let mut context = tera::Context::new();
        context.insert("queue_name", &queue_name);
        context.insert("queue_status", if is_paused { "paused" } else { "active" });
        context.insert("jobs", &queue_jobs);
        context.insert("current_page", &page);
        context.insert("total_pages", &total_pages);
        context.insert("total_jobs", &total_jobs);
        context.insert("has_prev", &(page > 1));
        context.insert("has_next", &(page < total_pages));
        context.insert("prev_page", &(if page > 1 { page - 1 } else { 1 }));
        context.insert(
            "next_page",
            &(if page < total_pages {
                page + 1
            } else {
                total_pages
            }),
        );
        context.insert("active_page", "queues");

        let html = state
            .render_template("queue-details.html", &mut context)
            .await?;
        debug!("Template rendering completed in {:?}", start.elapsed());

        Ok(Html(html))
    }
}
