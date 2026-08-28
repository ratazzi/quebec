use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{Html, IntoResponse, Response},
};
use sea_orm::{ConnectionTrait, DatabaseConnection, DbBackend, Statement, Value};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::debug;

use crate::control_plane::{
    models::{BatchCallbackInfo, BatchInfo, BatchJobInfo, Pagination},
    utils::clean_sql,
    ControlPlane,
};
use crate::entities::quebec_batches;
use crate::query_builder;

const STATUSES: [&str; 4] = ["pending", "enqueued", "completed", "failed"];

/// Members shown on a batch page; beyond this the page links to the jobs list.
const MEMBER_LIMIT: u64 = 200;

impl ControlPlane {
    pub async fn batches(
        State(state): State<Arc<ControlPlane>>,
        Query(pagination): Query<Pagination>,
    ) -> Result<Response, (StatusCode, String)> {
        let db = state
            .ctx
            .get_db()
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        let db = db.as_ref();
        let table_config = &state.ctx.table_config;

        let mut context = tera::Context::new();
        context.insert("active_page", "batches");
        context.insert("statuses", &STATUSES);

        if !state.ctx.ensure_batches(db).await {
            context.insert("schema_missing", &true);
            context.insert("batches", &Vec::<BatchInfo>::new());
            context.insert("current_page_num", &1u64);
            context.insert("total_pages", &1u64);
            context.insert("filter_status", &Option::<String>::None);
            let html = state.render_template("batches.html", &mut context).await?;
            return Ok(Html(html).into_response());
        }

        let status = pagination
            .status
            .as_deref()
            .filter(|s| STATUSES.contains(s));
        let page_size = state.page_size;
        let offset = (pagination.page - 1) * page_size;

        let rows =
            query_builder::batches::find_paginated(db, table_config, offset, page_size, status)
                .await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        let total_count = query_builder::batches::count_all(db, table_config, status)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        let total_pages = ((total_count as f64) / (page_size as f64)).ceil() as u64;
        let total_pages = total_pages.max(1);

        if pagination.page > total_pages {
            let mut ser = url::form_urlencoded::Serializer::new(String::new());
            ser.append_pair("page", &total_pages.to_string());
            if let Some(s) = status {
                ser.append_pair("status", s);
            }
            let target = format!("{}/batches?{}", state.base_path, ser.finish());
            return Ok(Self::redirect_back(&target));
        }

        // Live counters for the unfinished batches on this page, two grouped
        // queries instead of two per row.
        let unfinished: Vec<i64> = rows
            .iter()
            .filter(|b| b.finished_at.is_none())
            .map(|b| b.id)
            .collect();
        let pending =
            query_builder::batch_executions::count_by_batch_ids(db, table_config, &unfinished)
                .await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        let failed =
            query_builder::batches::count_failed_jobs_by_batch_ids(db, table_config, &unfinished)
                .await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        let batches: Vec<BatchInfo> = rows
            .iter()
            .map(|b| {
                Self::batch_info(
                    b,
                    pending.get(&b.id).copied().unwrap_or(0),
                    failed.get(&b.id).copied().unwrap_or(0),
                )
            })
            .collect();
        debug!(
            "Listed {} batches (page {})",
            batches.len(),
            pagination.page
        );

        context.insert("schema_missing", &false);
        context.insert("batches", &batches);
        context.insert("current_page_num", &pagination.page);
        context.insert("total_pages", &total_pages);
        context.insert("filter_status", &status);
        let html = state.render_template("batches.html", &mut context).await?;
        Ok(Html(html).into_response())
    }

    pub async fn batch_details(
        State(state): State<Arc<ControlPlane>>,
        Path(id): Path<i64>,
    ) -> Result<Response, (StatusCode, String)> {
        let db = state
            .ctx
            .get_db()
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        let db = db.as_ref();
        let table_config = &state.ctx.table_config;

        if !state.ctx.ensure_batches(db).await {
            return Ok(Self::not_found_response());
        }
        let Some(row) = query_builder::batches::find_by_id(db, table_config, id)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        else {
            return Ok(Self::not_found_response());
        };

        let (pending, failed) = if row.finished_at.is_some() {
            (0, 0)
        } else {
            let pending = query_builder::batch_executions::count_for_batch(db, table_config, id)
                .await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
            let failed = query_builder::batches::count_failed_jobs(db, table_config, id)
                .await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
            (pending, failed)
        };
        let batch = Self::batch_info(&row, pending, failed);

        let jobs = Self::batch_member_jobs(db, &state, id)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        let callbacks = Self::batch_callbacks(db, &state, &row)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        let metadata_pretty = row
            .metadata
            .as_deref()
            .and_then(|m| serde_json::from_str::<serde_json::Value>(m).ok())
            .filter(|v| v.as_object().is_some_and(|o| !o.is_empty()))
            .and_then(|v| serde_json::to_string_pretty(&v).ok());

        let mut context = tera::Context::new();
        context.insert("active_page", "batches");
        context.insert("batch", &batch);
        context.insert("metadata_pretty", &metadata_pretty);
        context.insert("jobs", &jobs);
        context.insert("jobs_truncated", &(jobs.len() as u64 >= MEMBER_LIMIT));
        context.insert("callbacks", &callbacks);
        let html = state
            .render_template("batch-details.html", &mut context)
            .await?;
        Ok(Html(html).into_response())
    }

    /// Row -> view model, deriving the live counters Solid Queue's `Batch`
    /// exposes while a batch is unfinished.
    fn batch_info(b: &quebec_batches::Model, pending: i64, live_failed: i64) -> BatchInfo {
        let finished = b.finished_at.is_some();
        let total = i64::from(b.total_jobs);
        let (completed, failed, pending) = if finished {
            (i64::from(b.completed_jobs), i64::from(b.failed_jobs), 0)
        } else {
            ((total - pending - live_failed).max(0), live_failed, pending)
        };
        let status = if finished {
            if b.failed_at.is_some() {
                "failed"
            } else {
                "completed"
            }
        } else if b.enqueued_at.is_some() {
            "enqueued"
        } else {
            "pending"
        };
        let progress = if total == 0 {
            0.0
        } else {
            // Percent with two decimals, like Solid Queue's `progress_percentage`.
            ((total - pending).max(0) as f64 * 10_000.0 / total as f64).round() / 100.0
        };
        BatchInfo {
            id: b.id,
            active_job_batch_id: b.active_job_batch_id.clone(),
            description: b.description.clone(),
            status: status.to_string(),
            total_jobs: total,
            completed_jobs: completed,
            failed_jobs: failed,
            pending_jobs: pending,
            progress_percentage: progress,
            metadata: b.metadata.clone(),
            created_at: Self::format_naive_datetime(b.created_at),
            enqueued_at: Self::format_optional_datetime(b.enqueued_at),
            finished_at: Self::format_optional_datetime(b.finished_at),
            failed_at: Self::format_optional_datetime(b.failed_at),
        }
    }

    /// Member jobs (newest first) with the same status derivation as the job
    /// details page.
    async fn batch_member_jobs(
        db: &DatabaseConnection,
        state: &ControlPlane,
        batch_id: i64,
    ) -> Result<Vec<BatchJobInfo>, sea_orm::DbErr> {
        let tc = &state.ctx.table_config;
        let backend = db.get_database_backend();
        let (p1, p2) = match backend {
            DbBackend::Postgres => ("$1", "$2"),
            DbBackend::MySql | DbBackend::Sqlite => ("?", "?"),
        };
        let sql = clean_sql(&format!(
            "SELECT j.id, j.class_name, j.queue_name, j.created_at, j.finished_at,
                CASE
                    WHEN EXISTS (SELECT 1 FROM {fe} WHERE job_id = j.id) THEN 'failed'
                    WHEN EXISTS (SELECT 1 FROM {ce} WHERE job_id = j.id) THEN 'processing'
                    WHEN EXISTS (SELECT 1 FROM {se} WHERE job_id = j.id) THEN 'scheduled'
                    WHEN EXISTS (SELECT 1 FROM {be} WHERE job_id = j.id) THEN 'blocked'
                    WHEN j.finished_at IS NOT NULL THEN 'finished'
                    ELSE 'pending'
                END AS status
             FROM {jobs} j
             WHERE j.batch_id = {p1}
             ORDER BY j.id DESC
             LIMIT {p2}",
            jobs = tc.jobs,
            fe = tc.failed_executions,
            ce = tc.claimed_executions,
            se = tc.scheduled_executions,
            be = tc.blocked_executions,
        ));
        let rows = db
            .query_all(Statement::from_sql_and_values(
                backend,
                &sql,
                [Value::from(batch_id), Value::from(MEMBER_LIMIT as i64)],
            ))
            .await?;
        rows.iter()
            .map(|row| {
                Ok(BatchJobInfo {
                    id: row.try_get("", "id")?,
                    class_name: row.try_get("", "class_name")?,
                    queue_name: row.try_get("", "queue_name")?,
                    status: row.try_get("", "status")?,
                    created_at: Self::format_naive_datetime(
                        row.try_get::<chrono::NaiveDateTime>("", "created_at")?,
                    ),
                    finished_at: Self::format_optional_datetime(row.try_get::<Option<
                        chrono::NaiveDateTime,
                    >>(
                        "", "finished_at"
                    )?),
                })
            })
            .collect()
    }

    /// The configured callbacks, each linked to its enqueued job (by the
    /// `job_id` stored in the serialized callback) once the batch finished.
    async fn batch_callbacks(
        db: &DatabaseConnection,
        state: &ControlPlane,
        batch: &quebec_batches::Model,
    ) -> Result<Vec<BatchCallbackInfo>, sea_orm::DbErr> {
        let tc = &state.ctx.table_config;
        let configured: Vec<(&str, serde_json::Value)> = [
            ("on_success", batch.on_success.as_deref()),
            ("on_failure", batch.on_failure.as_deref()),
            ("on_finish", batch.on_finish.as_deref()),
        ]
        .into_iter()
        .filter_map(|(kind, json)| {
            let value = serde_json::from_str::<serde_json::Value>(json?).ok()?;
            Some((kind, value))
        })
        .collect();
        if configured.is_empty() {
            return Ok(Vec::new());
        }

        let active_job_ids: Vec<String> = configured
            .iter()
            .filter_map(|(_, v)| v.get("job_id")?.as_str().map(str::to_string))
            .collect();
        let mut enqueued: HashMap<String, (i64, String)> = HashMap::new();
        if !active_job_ids.is_empty() {
            let backend = db.get_database_backend();
            let placeholders: Vec<String> = (1..=active_job_ids.len())
                .map(|i| match backend {
                    DbBackend::Postgres => format!("${i}"),
                    _ => "?".to_string(),
                })
                .collect();
            let sql = clean_sql(&format!(
                "SELECT j.id, j.active_job_id,
                    CASE
                        WHEN EXISTS (SELECT 1 FROM {fe} WHERE job_id = j.id) THEN 'failed'
                        WHEN EXISTS (SELECT 1 FROM {ce} WHERE job_id = j.id) THEN 'processing'
                        WHEN EXISTS (SELECT 1 FROM {se} WHERE job_id = j.id) THEN 'scheduled'
                        WHEN j.finished_at IS NOT NULL THEN 'finished'
                        ELSE 'pending'
                    END AS status
                 FROM {jobs} j
                 WHERE j.active_job_id IN ({ids})",
                jobs = tc.jobs,
                fe = tc.failed_executions,
                ce = tc.claimed_executions,
                se = tc.scheduled_executions,
                ids = placeholders.join(", "),
            ));
            let values: Vec<Value> = active_job_ids
                .iter()
                .map(|s| Value::from(s.clone()))
                .collect();
            for row in db
                .query_all(Statement::from_sql_and_values(backend, &sql, values))
                .await?
            {
                let active_job_id: Option<String> = row.try_get("", "active_job_id")?;
                if let Some(active_job_id) = active_job_id {
                    enqueued.insert(
                        active_job_id,
                        (row.try_get("", "id")?, row.try_get("", "status")?),
                    );
                }
            }
        }

        Ok(configured
            .into_iter()
            .map(|(kind, v)| {
                let text = |key: &str| v.get(key).and_then(|x| x.as_str()).map(str::to_string);
                let found = text("job_id").and_then(|id| enqueued.get(&id).cloned());
                BatchCallbackInfo {
                    kind: kind.to_string(),
                    job_class: text("job_class").unwrap_or_default(),
                    queue_name: text("queue_name").unwrap_or_else(|| "default".to_string()),
                    job_id: found.as_ref().map(|(id, _)| *id),
                    status: found.map(|(_, status)| status),
                }
            })
            .collect())
    }
}
