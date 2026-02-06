use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use std::sync::Arc;

use sea_orm::ConnectionTrait;

use crate::control_plane::ControlPlane;

impl ControlPlane {
    pub async fn health(
        State(state): State<Arc<ControlPlane>>,
    ) -> Result<impl IntoResponse, (StatusCode, String)> {
        let db = state
            .ctx
            .get_db_result()
            .await
            .map_err(|e| (StatusCode::SERVICE_UNAVAILABLE, e.to_string()))?;

        // Ping the database
        db.execute_unprepared("SELECT 1")
            .await
            .map_err(|e| (StatusCode::SERVICE_UNAVAILABLE, e.to_string()))?;

        Ok(Json(serde_json::json!({
            "status": "ok",
            "version": env!("CARGO_PKG_VERSION"),
        })))
    }
}
