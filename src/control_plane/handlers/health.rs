use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use std::sync::Arc;

use sea_orm::ConnectionTrait;

use crate::control_plane::ControlPlane;
use crate::query_builder;

impl ControlPlane {
    pub async fn health(
        State(state): State<Arc<ControlPlane>>,
    ) -> Result<impl IntoResponse, (StatusCode, String)> {
        let db = state
            .ctx
            .get_db()
            .await
            .map_err(|e| (StatusCode::SERVICE_UNAVAILABLE, e.to_string()))?;
        let db = db.as_ref();
        let table_config = &state.ctx.table_config;

        // Ping the database
        db.execute_unprepared("SELECT 1")
            .await
            .map_err(|e| (StatusCode::SERVICE_UNAVAILABLE, e.to_string()))?;

        // Process status
        let processes = query_builder::processes::find_all_by_heartbeat(db, table_config)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        let now = chrono::Utc::now().naive_utc();
        let threshold_seconds = state.ctx.process_alive_threshold.as_secs() as i64;

        // Group by kind, keep only the most recent heartbeat per kind
        let mut best_per_kind: std::collections::HashMap<String, (bool, i64)> =
            std::collections::HashMap::new();

        for process in &processes {
            let seconds_ago = now
                .signed_duration_since(process.last_heartbeat_at)
                .num_seconds();
            let alive = seconds_ago <= threshold_seconds;
            let kind_key = process.kind.to_lowercase();

            let is_better = best_per_kind
                .get(&kind_key)
                .map_or(true, |(_, prev)| seconds_ago < *prev);
            if is_better {
                best_per_kind.insert(kind_key, (alive, seconds_ago));
            }
        }

        let mut processes_json = serde_json::Map::new();
        let mut any_worker_alive = false;
        let mut all_alive = true;

        for (kind_key, (alive, seconds_ago)) in &best_per_kind {
            if kind_key == "worker" && *alive {
                any_worker_alive = true;
            }
            if !alive {
                all_alive = false;
            }
            processes_json.insert(
                kind_key.clone(),
                serde_json::json!({
                    "alive": alive,
                    "last_heartbeat_seconds_ago": seconds_ago,
                }),
            );
        }

        // Latency
        let pending_seconds = query_builder::ready_executions::oldest_created_at(db, table_config)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
            .map_or(0.0, |created_at| {
                now.signed_duration_since(created_at).num_milliseconds() as f64 / 1000.0
            });

        let processing_seconds =
            query_builder::claimed_executions::oldest_created_at(db, table_config)
                .await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
                .map_or(0.0, |created_at| {
                    now.signed_duration_since(created_at).num_milliseconds() as f64 / 1000.0
                });

        // Job counts — propagate errors so the health endpoint surfaces DB failures
        let map_err = |e: sea_orm::DbErr| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string());
        let ready = query_builder::ready_executions::count_all(db, table_config)
            .await
            .map_err(map_err)?;
        let in_progress =
            query_builder::claimed_executions::count_all(db, table_config, None, None)
                .await
                .map_err(map_err)?;
        let scheduled = query_builder::scheduled_executions::count_all(db, table_config)
            .await
            .map_err(map_err)?;
        let blocked = query_builder::blocked_executions::count_all(db, table_config, None, None)
            .await
            .map_err(map_err)?;
        let failed = query_builder::failed_executions::count_all(db, table_config, None, None)
            .await
            .map_err(map_err)?;

        // Status determination
        // "unavailable" only when workers exist but all are dead, or no processes at all
        let has_workers = best_per_kind.contains_key("worker");
        let status = if processes.is_empty() {
            "unavailable"
        } else if has_workers && !any_worker_alive {
            "unavailable"
        } else if all_alive {
            "healthy"
        } else {
            "degraded"
        };

        Ok(Json(serde_json::json!({
            "status": status,
            "version": env!("CARGO_PKG_VERSION"),
            "processes": processes_json,
            "latency": {
                "pending_seconds": pending_seconds,
                "processing_seconds": processing_seconds,
            },
            "jobs": {
                "ready": ready,
                "in_progress": in_progress,
                "scheduled": scheduled,
                "blocked": blocked,
                "failed": failed,
            },
        })))
    }
}
