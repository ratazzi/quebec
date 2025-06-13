use std::sync::Arc;
use std::time::Instant;
use axum::{
    extract::State,
    response::{Html, IntoResponse},
    http::StatusCode,
};
use sea_orm::{EntityTrait, QueryOrder, Order};
use tracing::debug;

use crate::entities::solid_queue_processes;
use crate::control_plane::{ControlPlane, models::WorkerInfo};

impl ControlPlane {
    pub async fn workers(
        State(state): State<Arc<ControlPlane>>,
    ) -> Result<Html<String>, (StatusCode, String)> {
        let start = Instant::now();
        let db = state.ctx.get_db().await;
        let db = db.as_ref();
        debug!("Database connection obtained in {:?}", start.elapsed());

        // Query all worker processes
        let workers = solid_queue_processes::Entity::find()
            .order_by(solid_queue_processes::Column::LastHeartbeatAt, Order::Desc)
            .all(db)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        // Calculate time since last heartbeat for each worker
        let workers_info: Vec<WorkerInfo> = workers.into_iter().map(|worker| {
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

            WorkerInfo {
                id: worker.id,
                name: worker.name,
                kind: worker.kind,
                hostname: worker.hostname.unwrap_or_else(|| "unknown".to_string()),
                pid: worker.pid,
                last_heartbeat_at: Self::format_naive_datetime(last_heartbeat),
                seconds_since_heartbeat,
                status: status.to_string(),
            }
        }).collect();

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
        state.populate_nav_stats(&mut context)
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
