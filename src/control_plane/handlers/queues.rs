use std::sync::Arc;
use std::time::Instant;
use std::collections::HashMap;
use axum::{
    extract::{State, Query, Path},
    response::{Html, IntoResponse},
    http::StatusCode,
};
use sea_orm::{EntityTrait, QueryFilter, ColumnTrait, ActiveModelTrait, ActiveValue, Statement, DbBackend, ConnectionTrait, QueryOrder, Order, PaginatorTrait, QuerySelect};
use sea_orm::sea_query::{Query as SeaQuery, Expr, Alias, PostgresQueryBuilder, SqliteQueryBuilder, MysqlQueryBuilder};
use tracing::{debug, error};

use crate::entities::{quebec_jobs, quebec_pauses};
use crate::control_plane::{ControlPlane, models::{Pagination, QueueInfo}};

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
        let all_queue_names = state.get_queue_names()
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        let table_config = &state.ctx.table_config;
        let jobs_table = Alias::new(&table_config.jobs);
        
        let query = SeaQuery::select()
            .column(Alias::new("queue_name"))
            .expr_as(Expr::col(Alias::new("queue_name")).count(), Alias::new("count"))
            .from(jobs_table)
            .and_where(Expr::col(Alias::new("finished_at")).is_null())
            .group_by_col(Alias::new("queue_name"))
            .to_owned();

        let (sql, values) = match db.get_database_backend() {
            DbBackend::Postgres => query.build(PostgresQueryBuilder),
            DbBackend::Sqlite => query.build(SqliteQueryBuilder),
            DbBackend::MySql => query.build(MysqlQueryBuilder),
        };

        let stmt = Statement::from_sql_and_values(
            db.get_database_backend(),
            &sql,
            values
        );

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
            .map(|queue_name| (queue_name.clone(), *queue_counts_map.get(&queue_name).unwrap_or(&0)))
            .collect();

        debug!("Fetched jobs in {:?}", start.elapsed());

        let start = Instant::now();
        // Get paused queues
        let paused_queues = quebec_pauses::Entity::find()
            .all(db)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        debug!("Fetched paused queues in {:?}", start.elapsed());

        let start = Instant::now();
        let paused_queue_names: Vec<String> = paused_queues.iter()
            .map(|p| p.queue_name.clone())
            .collect();

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
    ) -> impl IntoResponse {
        let db = state.ctx.get_db().await;
        let db = db.as_ref();

        // Create a new pause record
        let pause = quebec_pauses::ActiveModel {
            id: ActiveValue::NotSet,
            queue_name: ActiveValue::Set(queue_name.clone()),
            created_at: ActiveValue::Set(chrono::Utc::now().naive_utc()),
        };

        pause.insert(db).await
            .map(|_| StatusCode::OK)
            .map_err(|e| {
                error!("Failed to pause queue: {}", e);
                StatusCode::INTERNAL_SERVER_ERROR
            })
    }

    pub async fn resume_queue(
        State(state): State<Arc<ControlPlane>>,
        Path(queue_name): Path<String>,
    ) -> impl IntoResponse {
        let db = state.ctx.get_db().await;
        let db = db.as_ref();

        // Delete the pause record for this queue
        quebec_pauses::Entity::delete_many()
            .filter(quebec_pauses::Column::QueueName.eq(queue_name.clone()))
            .exec(db)
            .await
            .map(|_| StatusCode::OK)
            .map_err(|e| {
                error!("Failed to resume queue: {}", e);
                StatusCode::INTERNAL_SERVER_ERROR
            })
    }

    pub async fn queue_details(
        State(state): State<Arc<ControlPlane>>,
        Path(queue_name): Path<String>,
        Query(pagination): Query<Pagination>,
    ) -> Result<Html<String>, (StatusCode, String)> {
        let start = Instant::now();
        let db = state.ctx.get_db().await;
        let db = db.as_ref();
        debug!("Database connection obtained in {:?}", start.elapsed());
        
        let page = pagination.page;
        let offset = (page - 1) * state.page_size;
        
        // Get queue status
        let is_paused = state.is_queue_paused(&queue_name).await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        
        // Get total job count for this queue
        let total_jobs = quebec_jobs::Entity::find()
            .filter(quebec_jobs::Column::QueueName.eq(&queue_name))
            .filter(quebec_jobs::Column::FinishedAt.is_null())
            .count(db)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        
        // Get jobs for this queue with pagination
        let jobs = quebec_jobs::Entity::find()
            .filter(quebec_jobs::Column::QueueName.eq(&queue_name))
            .filter(quebec_jobs::Column::FinishedAt.is_null())
            .order_by(quebec_jobs::Column::CreatedAt, Order::Desc)
            .limit(state.page_size)
            .offset(offset)
            .all(db)
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

        let failed_executions_stmt = Statement::from_sql_and_values(
            db.get_database_backend(),
            &failed_sql,
            failed_values
        );
        
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

        let claimed_executions_stmt = Statement::from_sql_and_values(
            db.get_database_backend(),
            &claimed_sql,
            claimed_values
        );
        
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
            scheduled_values
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

        let blocked_executions_stmt = Statement::from_sql_and_values(
            db.get_database_backend(),
            &blocked_sql,
            blocked_values
        );
        
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
        
        let queue_jobs: Vec<crate::control_plane::models::QueueJobInfo> = jobs.into_iter().map(|job| {
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
            
            let execution_id = claimed_map.get(&job.id)
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
        }).collect();
        
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
        context.insert("next_page", &(if page < total_pages { page + 1 } else { total_pages }));
        context.insert("active_page", "queues");
        
        let html = state.render_template("queue_details.html", &mut context).await?;
        debug!("Template rendering completed in {:?}", start.elapsed());
        
        Ok(Html(html))
    }
}