use std::sync::Arc;

use axum::{
    extract::DefaultBodyLimit,
    http::{HeaderName, HeaderValue},
    middleware,
    routing::get,
    Extension, Router,
};
use tower_http::{set_header::SetResponseHeaderLayer, trace::TraceLayer};

use crate::{
    handler::{
        auth::auth_handler,
        file::file_handle,
        file_query::get_file_list_handler,
        status::{health, status_handler},
        user::users_handler,
    },
    middleware::auth,
    AppState,
};

pub fn create_router(app_state: Arc<AppState>) -> Router {
    let served_by = HeaderValue::from_str(&app_state.env.node_id)
        .unwrap_or_else(|_| HeaderValue::from_static("unknown"));
    let max_upload = app_state.env.max_upload_bytes;

    let api_route = Router::new()
        .route("/health", get(health))
        .nest("/cluster", status_handler())
        .nest("/auth", auth_handler())
        .nest("/users", users_handler().layer(middleware::from_fn(auth)))
        .nest("/file", file_handle().layer(middleware::from_fn(auth)))
        .nest("/list", get_file_list_handler().layer(middleware::from_fn(auth)))
        .layer(DefaultBodyLimit::max(max_upload))
        .layer(TraceLayer::new_for_http())
        // Tell the client which API node answered => makes load balancing visible.
        .layer(SetResponseHeaderLayer::overriding(HeaderName::from_static("x-served-by"), served_by))
        .layer(Extension(app_state));

    Router::new().nest("/api", api_route)
}
