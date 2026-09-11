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
    pub async fn workers(
        State(state): State<Arc<ControlPlane>>,
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

        // Query all worker processes using query_builder
        let workers = query_builder::processes::find_all_by_heartbeat(db, table_config)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        // Calculate time since last heartbeat for each worker
        let workers_info: Vec<WorkerInfo> = workers
            .into_iter()
            .map(|worker| {
                let last_heartbeat = worker.last_heartbeat_at;
                let now = chrono::Utc::now().naive_utc();
                // Calculate time difference (seconds)
                let duration = now.signed_duration_since(last_heartbeat);
                let seconds_since_heartbeat = duration.num_seconds();

                // Use process_alive_threshold from context to determine worker status
                let threshold_seconds = state.ctx.process_alive_threshold.as_secs() as i64;
                let status = if seconds_since_heartbeat > threshold_seconds {
                    "dead"
                } else {
                    "alive"
                };

                let metadata = worker
                    .metadata
                    .as_deref()
                    .and_then(|m| serde_json::from_str::<serde_json::Value>(m).ok());
                let quiet = metadata
                    .as_ref()
                    .and_then(|v| v.get("quiet").and_then(|q| q.as_bool()))
                    .unwrap_or(false);
                let revision = metadata
                    .as_ref()
                    .and_then(|v| v.get("revision").and_then(|s| s.as_str()))
                    .map(|s| s.to_string());
                let memory = metadata
                    .as_ref()
                    .and_then(|v| v.get("rss_bytes").and_then(|b| b.as_u64()))
                    .map(Self::format_rss_bytes);

                let num = |key: &str| {
                    metadata
                        .as_ref()
                        .and_then(|v| v.get(key).and_then(|b| b.as_u64()))
                };
                let cgroup_current = num("cgroup_current_bytes");
                let cgroup_limit = num("cgroup_memory_max");
                let cgroup_memory = cgroup_current.map(Self::format_rss_bytes);
                let cgroup_memory_max = cgroup_limit.map(Self::format_rss_bytes);
                // Only meaningful with a limit to divide by; an unlimited
                // cgroup shows the raw number and no bar.
                let cgroup_memory_pct = match (cgroup_current, cgroup_limit) {
                    (Some(current), Some(limit)) if limit > 0 => {
                        Some((current.saturating_mul(100) / limit).min(100))
                    }
                    _ => None,
                };
                let nr_throttled = num("cpu_nr_throttled").unwrap_or(0);
                let high_events = num("cgroup_high_events").unwrap_or(0);
                let cgroup_throttled = nr_throttled > 0 || high_events > 0;
                let cgroup_throttle_hint = cgroup_throttled.then(|| {
                    format!("Kernel throttling: nr_throttled={nr_throttled}, memory.events high={high_events}")
                });

                WorkerInfo {
                    id: worker.id,
                    name: worker.name,
                    kind: worker.kind,
                    hostname: worker.hostname.unwrap_or_else(|| "unknown".to_string()),
                    pid: worker.pid,
                    created_at: Self::format_naive_datetime(worker.created_at),
                    last_heartbeat_at: Self::format_naive_datetime(last_heartbeat),
                    seconds_since_heartbeat,
                    status: status.to_string(),
                    quiet,
                    revision,
                    memory,
                    cgroup_memory,
                    cgroup_memory_max,
                    cgroup_memory_pct,
                    cgroup_throttled,
                    cgroup_throttle_hint,
                }
            })
            .collect();

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

        // Use populate_nav_stats method to fill statistics
        state
            .populate_nav_stats(&mut context)
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
