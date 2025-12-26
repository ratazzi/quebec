use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{Html, IntoResponse, Response},
};
use sea_orm::sea_query::{
    Alias, Expr, MysqlQueryBuilder, PostgresQueryBuilder, Query as SeaQuery, SqliteQueryBuilder,
};
use sea_orm::{ConnectionTrait, DbBackend, Statement};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, error};

use crate::control_plane::{
    models::{Pagination, QueueInfo},
    ControlPlane,
};
use crate::query_builder;

impl ControlPlane {
    pub async fn queues(
        State(state): State<Arc<ControlPlane>>,
        Query(pagination): Query<Pagination>,
    ) -> Result<Html<String>, (StatusCode, String)> {
        let start = Instant::now();
        let db = state.ctx.get_db().await;
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
        let jobs_table = Alias::new(&table_config.jobs);

        let query = SeaQuery::select()
            .column(Alias::new("queue_name"))
            .expr_as(
                Expr::col(Alias::new("queue_name")).count(),
                Alias::new("count"),
            )
            .from(jobs_table)
            .and_where(Expr::col(Alias::new("finished_at")).is_null())
            .group_by_col(Alias::new("queue_name"))
            .to_owned();

        let (sql, values) = match db.get_database_backend() {
            DbBackend::Postgres => query.build(PostgresQueryBuilder),
            DbBackend::Sqlite => query.build(SqliteQueryBuilder),
            DbBackend::MySql => query.build(MysqlQueryBuilder),
        };

        let stmt = Statement::from_sql_and_values(db.get_database_backend(), &sql, values);

        // Get queue counts with unfinished jobs
        let queue_counts_map: HashMap<String, i64> = db
            .query_all(stmt)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
            .into_iter()
            .map(|row| {
                let queue_name: String = row.try_get("", "queue_name").unwrap_or_default();
                let count: i64 = row.try_get("", "count").unwrap_or_default();
                (queue_name, count)
            })
            .collect();

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
                name: name.clone(),
                jobs_count: count,
                status: if paused_queue_names.contains(&name) {
                    "paused".to_string()
                } else {
                    "active".to_string()
                },
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
        let db = state.ctx.get_db().await;
        let db = db.as_ref();
        let table_config = &state.ctx.table_config;

        // Create a new pause record using query_builder
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

        Self::redirect_back(&format!("/queues/{}", queue_name))
    }

    pub async fn resume_queue(
        State(state): State<Arc<ControlPlane>>,
        Path(queue_name): Path<String>,
    ) -> Response {
        let db = state.ctx.get_db().await;
        let db = db.as_ref();
        let table_config = &state.ctx.table_config;

        // Delete the pause record for this queue using query_builder
        if let Err(e) =
            query_builder::pauses::delete_by_queue_name(db, table_config, &queue_name).await
        {
            error!("Failed to resume queue: {}", e);
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }

        Self::redirect_back(&format!("/queues/{}", queue_name))
    }

    pub async fn queue_details(
        State(state): State<Arc<ControlPlane>>,
        Path(queue_name): Path<String>,
        Query(pagination): Query<Pagination>,
    ) -> Result<Html<String>, (StatusCode, String)> {
        let start = Instant::now();
        let db = state.ctx.get_db().await;
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

        // Get total job count for this queue using query_builder
        let total_jobs =
            query_builder::jobs::count_by_queue_unfinished(db, table_config, &queue_name)
                .await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        // Get jobs for this queue with pagination using query_builder
        let jobs = query_builder::jobs::find_by_queue_unfinished_paginated(
            db,
            table_config,
            &queue_name,
            offset,
            state.page_size,
        )
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        // Get execution status for each job
        let job_ids: Vec<i64> = jobs.iter().map(|j| j.id).collect();

        // Check for failed executions
        let failed_table = Alias::new(&state.ctx.table_config.failed_executions);

        let failed_query = SeaQuery::select()
            .column(Alias::new("job_id"))
            .from(failed_table)
            .and_where(Expr::col(Alias::new("job_id")).is_in(job_ids.clone()))
            .to_owned();

        let (failed_sql, failed_values) = match db.get_database_backend() {
            DbBackend::Postgres => failed_query.build(PostgresQueryBuilder),
            DbBackend::Sqlite => failed_query.build(SqliteQueryBuilder),
            DbBackend::MySql => failed_query.build(MysqlQueryBuilder),
        };

        let failed_executions_stmt =
            Statement::from_sql_and_values(db.get_database_backend(), &failed_sql, failed_values);

        let failed_job_ids: Vec<i64> = db
            .query_all(failed_executions_stmt)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
            .iter()
            .map(|row| row.try_get::<i64>("", "job_id").unwrap_or(0))
            .collect();

        // Check for claimed executions (in progress)
        let claimed_table = Alias::new(&state.ctx.table_config.claimed_executions);

        let claimed_query = SeaQuery::select()
            .columns([Alias::new("id"), Alias::new("job_id")])
            .from(claimed_table)
            .and_where(Expr::col(Alias::new("job_id")).is_in(job_ids.clone()))
            .to_owned();

        let (claimed_sql, claimed_values) = match db.get_database_backend() {
            DbBackend::Postgres => claimed_query.build(PostgresQueryBuilder),
            DbBackend::Sqlite => claimed_query.build(SqliteQueryBuilder),
            DbBackend::MySql => claimed_query.build(MysqlQueryBuilder),
        };

        let claimed_executions_stmt =
            Statement::from_sql_and_values(db.get_database_backend(), &claimed_sql, claimed_values);

        let claimed_map: HashMap<i64, i64> = db
            .query_all(claimed_executions_stmt)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
            .iter()
            .map(|row| {
                let job_id: i64 = row.try_get("", "job_id").unwrap_or(0);
                let execution_id: i64 = row.try_get("", "id").unwrap_or(0);
                (job_id, execution_id)
            })
            .collect();

        // Check for scheduled executions
        let scheduled_table = Alias::new(&state.ctx.table_config.scheduled_executions);

        let scheduled_query = SeaQuery::select()
            .columns([Alias::new("id"), Alias::new("job_id")])
            .from(scheduled_table)
            .and_where(Expr::col(Alias::new("job_id")).is_in(job_ids.clone()))
            .to_owned();

        let (scheduled_sql, scheduled_values) = match db.get_database_backend() {
            DbBackend::Postgres => scheduled_query.build(PostgresQueryBuilder),
            DbBackend::Sqlite => scheduled_query.build(SqliteQueryBuilder),
            DbBackend::MySql => scheduled_query.build(MysqlQueryBuilder),
        };

        let scheduled_executions_stmt = Statement::from_sql_and_values(
            db.get_database_backend(),
            &scheduled_sql,
            scheduled_values,
        );

        let scheduled_map: HashMap<i64, i64> = db
            .query_all(scheduled_executions_stmt)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
            .iter()
            .map(|row| {
                let job_id: i64 = row.try_get("", "job_id").unwrap_or(0);
                let execution_id: i64 = row.try_get("", "id").unwrap_or(0);
                (job_id, execution_id)
            })
            .collect();

        // Check for blocked executions
        let blocked_table = Alias::new(&state.ctx.table_config.blocked_executions);

        let blocked_query = SeaQuery::select()
            .columns([Alias::new("id"), Alias::new("job_id")])
            .from(blocked_table)
            .and_where(Expr::col(Alias::new("job_id")).is_in(job_ids))
            .to_owned();

        let (blocked_sql, blocked_values) = match db.get_database_backend() {
            DbBackend::Postgres => blocked_query.build(PostgresQueryBuilder),
            DbBackend::Sqlite => blocked_query.build(SqliteQueryBuilder),
            DbBackend::MySql => blocked_query.build(MysqlQueryBuilder),
        };

        let blocked_executions_stmt =
            Statement::from_sql_and_values(db.get_database_backend(), &blocked_sql, blocked_values);

        let blocked_map: HashMap<i64, i64> = db
            .query_all(blocked_executions_stmt)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
            .iter()
            .map(|row| {
                let job_id: i64 = row.try_get("", "job_id").unwrap_or(0);
                let execution_id: i64 = row.try_get("", "id").unwrap_or(0);
                (job_id, execution_id)
            })
            .collect();

        let queue_jobs: Vec<crate::control_plane::models::QueueJobInfo> = jobs
            .into_iter()
            .map(|job| {
                let status = if failed_job_ids.contains(&job.id) {
                    "failed"
                } else if claimed_map.contains_key(&job.id) {
                    "in_progress"
                } else if scheduled_map.contains_key(&job.id) {
                    "scheduled"
                } else if blocked_map.contains_key(&job.id) {
                    "blocked"
                } else {
                    "ready"
                };

                let execution_id = claimed_map
                    .get(&job.id)
                    .or_else(|| scheduled_map.get(&job.id))
                    .or_else(|| blocked_map.get(&job.id))
                    .copied();

                crate::control_plane::models::QueueJobInfo {
                    id: job.id,
                    class_name: job.class_name.clone(),
                    status: status.to_string(),
                    created_at: Self::format_naive_datetime(job.created_at),
                    execution_id,
                }
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
