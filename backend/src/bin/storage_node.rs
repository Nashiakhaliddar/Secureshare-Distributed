//! Storage node: a tiny, dumb, horizontally-replicated blob server.
//!
//! It only ever sees ENCRYPTED chunks. A chunk's id is the SHA-256 of its bytes
//! (content addressing), so the node can verify what it is given and the readers can
//! verify what they get back.
//!
//!   PUT    /chunks/:id   store chunk (verifies sha256(body) == id, atomic write)
//!   GET    /chunks/:id   read chunk
//!   DELETE /chunks/:id   delete chunk
//!   GET    /health       liveness + usage numbers (used as the heartbeat)
//!
//! Environment: NODE_ID, PORT (9001), DATA_DIR (./storage-data/<NODE_ID>), CLUSTER_TOKEN

use std::{
    path::{Path as FsPath, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use axum::{
    body::Bytes,
    extract::{DefaultBodyLimit, Path, Request, State},
    http::StatusCode,
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, put},
    Json, Router,
};
use sha2::{Digest, Sha256};
use tokio::{fs, io::AsyncWriteExt};
use tracing_subscriber::EnvFilter;

#[derive(Clone)]
struct Node {
    id: String,
    dir: PathBuf,
    token: String,
    chunks: Arc<AtomicU64>,
    bytes: Arc<AtomicU64>,
}

fn valid_id(id: &str) -> bool {
    id.len() == 64 && id.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn chunk_path(dir: &FsPath, id: &str) -> PathBuf {
    dir.join(&id[..2]).join(id)
}

/// Count existing chunks at start-up and remove half-written temp files left by a crash.
fn scan(dir: &FsPath) -> (u64, u64) {
    let (mut chunks, mut bytes) = (0u64, 0u64);
    if let Ok(top) = std::fs::read_dir(dir) {
        for shard in top.flatten() {
            if let Ok(files) = std::fs::read_dir(shard.path()) {
                for f in files.flatten() {
                    let name = f.file_name().to_string_lossy().to_string();
                    if name.contains(".tmp-") {
                        let _ = std::fs::remove_file(f.path());
                    } else if let Ok(meta) = f.metadata() {
                        chunks += 1;
                        bytes += meta.len();
                    }
                }
            }
        }
    }
    (chunks, bytes)
}

async fn require_token(State(node): State<Node>, req: Request, next: Next) -> Response {
    let ok = req
        .headers()
        .get("x-cluster-token")
        .and_then(|v| v.to_str().ok())
        .map(|v| v == node.token)
        .unwrap_or(false);
    if !ok {
        return (StatusCode::UNAUTHORIZED, "bad cluster token").into_response();
    }
    next.run(req).await
}

async fn health(State(node): State<Node>) -> impl IntoResponse {
    Json(serde_json::json!({
        "node_id": node.id,
        "status": "ok",
        "chunks": node.chunks.load(Ordering::Relaxed),
        "bytes": node.bytes.load(Ordering::Relaxed),
    }))
}

async fn put_chunk(
    State(node): State<Node>,
    Path(id): Path<String>,
    body: Bytes,
) -> Result<StatusCode, (StatusCode, String)> {
    if !valid_id(&id) {
        return Err((StatusCode::BAD_REQUEST, "chunk id must be 64 lowercase hex chars".into()));
    }
    let actual = hex::encode(Sha256::digest(&body));
    if actual != id {
        return Err((StatusCode::BAD_REQUEST, "body does not match chunk id (sha256)".into()));
    }

    let path = chunk_path(&node.dir, &id);
    let parent = path.parent().unwrap().to_path_buf();
    let io = |e: std::io::Error| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string());
    fs::create_dir_all(&parent).await.map_err(io)?;

    let previous = fs::metadata(&path).await.ok().map(|m| m.len());

    // write to a temp file, fsync, then atomically rename => readers never see partial data
    let tmp = parent.join(format!("{}.tmp-{}", id, uuid::Uuid::new_v4()));
    let mut file = fs::File::create(&tmp).await.map_err(io)?;
    file.write_all(&body).await.map_err(io)?;
    file.sync_all().await.map_err(io)?;
    drop(file);
    fs::rename(&tmp, &path).await.map_err(io)?;

    match previous {
        None => {
            node.chunks.fetch_add(1, Ordering::Relaxed);
            node.bytes.fetch_add(body.len() as u64, Ordering::Relaxed);
        }
        Some(old) => {
            // overwrite (e.g. read-repair of a corrupted copy)
            node.bytes.fetch_sub(old, Ordering::Relaxed);
            node.bytes.fetch_add(body.len() as u64, Ordering::Relaxed);
        }
    }
    Ok(StatusCode::OK)
}

async fn get_chunk(State(node): State<Node>, Path(id): Path<String>) -> Result<Vec<u8>, StatusCode> {
    if !valid_id(&id) {
        return Err(StatusCode::BAD_REQUEST);
    }
    match fs::read(chunk_path(&node.dir, &id)).await {
        Ok(data) => Ok(data),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Err(StatusCode::NOT_FOUND),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

async fn delete_chunk(State(node): State<Node>, Path(id): Path<String>) -> StatusCode {
    if !valid_id(&id) {
        return StatusCode::BAD_REQUEST;
    }
    let path = chunk_path(&node.dir, &id);
    if let Ok(meta) = fs::metadata(&path).await {
        if fs::remove_file(&path).await.is_ok() {
            node.chunks.fetch_sub(1, Ordering::Relaxed);
            node.bytes.fetch_sub(meta.len(), Ordering::Relaxed);
        }
    }
    StatusCode::OK
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();
    dotenv::dotenv().ok();

    let id = std::env::var("NODE_ID").unwrap_or_else(|_| "storage-1".into());
    let port: u16 = std::env::var("PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(9001);
    let dir = PathBuf::from(
        std::env::var("DATA_DIR").unwrap_or_else(|_| format!("./storage-data/{}", id)),
    );
    let token = std::env::var("CLUSTER_TOKEN").unwrap_or_else(|_| "dev-cluster-token".into());

    std::fs::create_dir_all(&dir).expect("cannot create DATA_DIR");
    let (chunks, bytes) = scan(&dir);

    let node = Node {
        id: id.clone(),
        dir: dir.clone(),
        token,
        chunks: Arc::new(AtomicU64::new(chunks)),
        bytes: Arc::new(AtomicU64::new(bytes)),
    };

    let protected = Router::new()
        .route("/chunks/:id", put(put_chunk).get(get_chunk).delete(delete_chunk))
        .layer(DefaultBodyLimit::max(64 * 1024 * 1024))
        .layer(middleware::from_fn_with_state(node.clone(), require_token));

    let app = Router::new()
        .route("/health", get(health))
        .merge(protected)
        .with_state(node);

    tracing::info!(
        "storage node '{}' listening on 0.0.0.0:{} (data: {}, {} chunks, {} bytes)",
        id, port, dir.display(), chunks, bytes
    );
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port)).await.expect("cannot bind port");
    axum::serve(listener, app).await.unwrap();
}
