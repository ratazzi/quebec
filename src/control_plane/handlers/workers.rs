use axum::{
    extract::State,
    http::StatusCode,
    response::{Html, IntoResponse},
};
use std::sync::Arc;
use std::time::Instant;
use tracing::debug;

use crate::control_plane::{models::WorkerInfo, ControlPlane};
use crate::query_builder;

impl ControlPlane {
    /// Query and assemble the live worker list. Shared between the full
    /// `/workers` page render and the `/stats` turbo-stream that drives
    /// live refresh (SSE in standalone mode, polling fallback under ASGI).
    pub(crate) async fn fetch_workers_info(&self) -> Result<Vec<WorkerInfo>, sea_orm::DbErr> {
        let start = Instant::now();
        let db = self.ctx.get_db().await?;
        let db = db.as_ref();
        let table_config = &self.ctx.table_config;
        debug!("Database connection obtained in {:?}", start.elapsed());

        let workers = query_builder::processes::find_all_by_heartbeat(db, table_config).await?;

        let threshold_seconds = self.ctx.process_alive_threshold.as_secs() as i64;
        let now = chrono::Utc::now().naive_utc();

        Ok(workers
            .into_iter()
            .map(|worker| {
                let last_heartbeat = worker.last_heartbeat_at;
                let seconds_since_heartbeat =
                    now.signed_duration_since(last_heartbeat).num_seconds();
                let status = if seconds_since_heartbeat > threshold_seconds {
                    "dead"
                } else {
                    "alive"
                };
                let quiet = worker
                    .metadata
                    .as_deref()
                    .and_then(|m| serde_json::from_str::<serde_json::Value>(m).ok())
                    .and_then(|v| v.get("quiet").and_then(|q| q.as_bool()))
                    .unwrap_or(false);

                WorkerInfo {
                    id: worker.id,
                    name: worker.name,
                    kind: worker.kind,
                    hostname: worker.hostname.unwrap_or_else(|| "unknown".to_string()),
                    pid: worker.pid,
                    last_heartbeat_at: Self::format_naive_datetime(last_heartbeat),
                    seconds_since_heartbeat,
                    status: status.to_string(),
                    quiet,
                }
            })
            .collect())
    }

    pub async fn workers(
        State(state): State<Arc<ControlPlane>>,
    ) -> Result<Html<String>, (StatusCode, String)> {
        // Page-context: /workers owns its core data. Querying it here (rather
        // than via populate_nav_stats) keeps the page robust against nav-stats
        // failures and surfaces a 5xx from this handler — not from deep inside
        // render_template — if the processes query itself fails.
        let workers_info = state
            .fetch_workers_info()
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        let mut context = tera::Context::new();
        context.insert("workers", &workers_info);
        context.insert("active_page", "workers");

        let html = state.render_template("workers.html", &mut context).await?;

        Ok(Html(html))
    }

    pub async fn stats(
        State(state): State<Arc<ControlPlane>>,
        _req: axum::http::Request<axum::body::Body>,
    ) -> Result<impl IntoResponse, (StatusCode, String)> {
        let mut context = tera::Context::new();

        // Stream-context: extras beyond nav stats (workers, ...). Nav stats
        // are populated implicitly by render_template; here we add the
        // resource-level live data that stats.html broadcasts as a
        // turbo-stream.
        state
            .populate_stream_extras(&mut context)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        // Return Turbo Stream format response directly
        let html = state.render_template("stats.html", &mut context).await?;

        // Set correct Content-Type
        let response = axum::response::Response::builder()
            .header("Content-Type", "text/vnd.turbo-stream.html")
            .body(axum::body::Body::from(html))
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        Ok(response)
    }
}
