//! Observability endpoints for the cluster (handy for demos and for the report).
//!   GET /api/health          -> is this API node healthy (DB reachable)?
//!   GET /api/cluster/status  -> storage node liveness, leader, replica statistics

use std::sync::{atomic::Ordering, Arc};

use axum::{http::StatusCode, response::IntoResponse, routing::get, Extension, Json, Router};
use serde_json::json;

use crate::{error::HttpError, AppState};

pub fn status_handler() -> Router {
    Router::new().route("/status", get(cluster_status))
}

pub async fn health(Extension(app_state): Extension<Arc<AppState>>) -> impl IntoResponse {
    match app_state.db_client.ping().await {
        Ok(()) => (
            StatusCode::OK,
            Json(json!({ "status": "ok", "node": app_state.env.node_id })),
        ),
        Err(e) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "status": "db_unreachable", "error": e.to_string() })),
        ),
    }
}

pub async fn cluster_status(
    Extension(app_state): Extension<Arc<AppState>>,
) -> Result<impl IntoResponse, HttpError> {
    let cluster = &app_state.cluster;
    let nodes = cluster.snapshot();
    let alive = nodes.iter().filter(|n| n.alive).count();
    let down: Vec<String> = nodes.iter().filter(|n| !n.alive).map(|n| n.id.clone()).collect();
    let want = cluster.replication_factor.min(alive.max(1)) as i64;

    let stats = app_state
        .db_client
        .stats(&down, want)
        .await
        .map_err(|e| HttpError::server_error(e.to_string()))?;
    let leader = app_state.db_client.leader().await;

    Ok(Json(json!({
        "status": "success",
        "served_by": app_state.env.node_id,
        "this_node_is_leader": app_state.is_leader.load(Ordering::SeqCst),
        "leader": leader.map(|(id, _)| id),
        "replication_factor": cluster.replication_factor,
        "write_quorum": cluster.write_quorum,
        "chunk_size": app_state.env.chunk_size,
        "parallelism": app_state.env.parallelism,
        "alive_storage_nodes": alive,
        "total_storage_nodes": nodes.len(),
        "storage_nodes": nodes,
        "metadata": stats,
    })))
}
