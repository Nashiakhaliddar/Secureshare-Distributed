use std::{sync::Arc, time::Instant};

use axum::{
    body::{Body, Bytes},
    extract::Multipart,
    http::{header, StatusCode},
    response::{IntoResponse, Response as HttpResponse},
    routing::post,
    Extension, Json, Router,
};
use base64::{engine::general_purpose::STANDARD, Engine};
use chrono::{DateTime, Utc};
use futures::{future::join_all, stream, StreamExt};
use rsa::{
    pkcs1::{DecodeRsaPrivateKey, DecodeRsaPublicKey},
    RsaPrivateKey, RsaPublicKey,
};
use uuid::Uuid;
use validator::Validate;

use crate::{
    dtos::{FileUploadDtos, Response as ResponseDto, RetrieveFileDto},
    error::HttpError,
    middleware::JWTAuthMiddeware,
    models::{NewFile, StoredChunk},
    utils::{crypto, password},
    AppState,
};

pub fn file_handle() -> Router {
    Router::new()
        .route("/upload", post(upload_file))
        .route("/retrieve", post(retrieve_file))
}

fn join_err(e: tokio::task::JoinError) -> HttpError {
    HttpError::server_error(format!("worker task failed: {}", e))
}

// ===================================================================================
//  UPLOAD
//
//   file ──split──▶ chunk 0 ─▶ [encrypt]─▶ [sha256]─▶ replicate to R storage nodes ─┐
//                   chunk 1 ─▶ [encrypt]─▶ [sha256]─▶ replicate to R storage nodes ─┼▶ commit
//                   chunk … (up to PARALLELISM chunks in flight at the same time)  ─┘  metadata
//
//  * CPU work (AES-GCM, SHA-256) runs on the blocking thread pool => uses all cores.
//  * Network work (replication) is async, all R copies of a chunk and many chunks at
//    once are in flight simultaneously => hides network latency.
// ===================================================================================
pub async fn upload_file(
    Extension(app_state): Extension<Arc<AppState>>,
    Extension(user): Extension<JWTAuthMiddeware>,
    mut multipart: Multipart,
) -> Result<impl IntoResponse, HttpError> {
    let started = Instant::now();

    let mut file_data = Bytes::new();
    let mut file_name = String::from("unknown_file");
    let mut got_file = false;
    let mut form = FileUploadDtos {
        recipient_email: String::new(),
        password: String::new(),
        expiration_date: String::new(),
    };

    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|e| HttpError::bad_request(e.to_string()))?
    {
        let name = field.name().unwrap_or_default().to_string();
        match name.as_str() {
            "fileUpload" => {
                file_name = field.file_name().unwrap_or("unknown_file").to_string();
                file_data = field.bytes().await.map_err(|e| HttpError::bad_request(e.to_string()))?;
                got_file = true;
            }
            "recipient_email" => {
                form.recipient_email = field.text().await.map_err(|e| HttpError::bad_request(e.to_string()))?
            }
            "password" => {
                form.password = field.text().await.map_err(|e| HttpError::bad_request(e.to_string()))?
            }
            "expiration_date" => {
                form.expiration_date = field.text().await.map_err(|e| HttpError::bad_request(e.to_string()))?
            }
            _ => {}
        }
    }

    if !got_file {
        return Err(HttpError::bad_request("No file was uploaded (field 'fileUpload')"));
    }
    form.validate().map_err(|e| HttpError::bad_request(e.to_string()))?;

    // ---- recipient + their public key
    let recipient = app_state
        .db_client
        .get_user(None, None, Some(&form.recipient_email))
        .await
        .map_err(|e| HttpError::server_error(e.to_string()))?
        .ok_or(HttpError::bad_request("Recipient user not found"))?;

    let public_key_b64 = recipient
        .public_key
        .as_ref()
        .ok_or(HttpError::bad_request("Recipient user has no public key"))?;
    let public_key_pem = String::from_utf8(
        STANDARD.decode(public_key_b64).map_err(|e| HttpError::server_error(e.to_string()))?,
    )
    .map_err(|e| HttpError::server_error(e.to_string()))?;
    let public_key = RsaPublicKey::from_pkcs1_pem(&public_key_pem)
        .map_err(|e| HttpError::server_error(e.to_string()))?;

    let expiration_date = DateTime::parse_from_rfc3339(&form.expiration_date)
        .map_err(|e| HttpError::bad_request(e.to_string()))?
        .with_timezone(&Utc);

    // ---- per-file key, wrapped for the recipient (fail early, before touching storage)
    let file_id = Uuid::new_v4();
    let file_key = crypto::random_key();
    let wrapped_key = tokio::task::spawn_blocking(move || crypto::wrap_file_key(&public_key, &file_key))
        .await
        .map_err(join_err)??;

    let link_password_hash = password::hash_async(form.password.clone())
        .await
        .map_err(|e| HttpError::server_error(e.to_string()))?;

    // ---- PARALLEL PIPELINE over chunks
    let cluster = app_state.cluster.clone();
    let chunk_size = app_state.env.chunk_size;
    let total = file_data.len();
    let n_chunks = total.div_ceil(chunk_size);

    let results: Vec<Result<StoredChunk, HttpError>> = stream::iter(0..n_chunks)
        .map(|idx| {
            let cluster = cluster.clone();
            let plain = file_data.slice(idx * chunk_size..((idx + 1) * chunk_size).min(total)); // O(1) view
            async move {
                // 1) CPU-bound: encrypt + hash on the blocking pool
                let (ciphertext, nonce, chunk_id) = tokio::task::spawn_blocking(move || {
                    let (ct, nonce) = crypto::encrypt_chunk(&file_key, &file_id, idx as u32, &plain)?;
                    let id = crypto::sha256_hex(&ct);
                    Ok::<_, HttpError>((ct, nonce, id))
                })
                .await
                .map_err(join_err)??;

                let size = ciphertext.len() as i32;

                // 2) network-bound: replicate to R nodes in parallel, wait for quorum
                let nodes = cluster.put_chunk(&chunk_id, Bytes::from(ciphertext)).await?;

                Ok(StoredChunk { idx: idx as i32, chunk_id, nonce: nonce.to_vec(), size, nodes })
            }
        })
        .buffer_unordered(app_state.env.parallelism)
        .collect()
        .await;

    let mut stored: Vec<StoredChunk> = Vec::with_capacity(n_chunks);
    let mut first_error: Option<HttpError> = None;
    for r in results {
        match r {
            Ok(c) => stored.push(c),
            Err(e) => {
                if first_error.is_none() {
                    first_error = Some(e);
                }
            }
        }
    }
    if let Some(e) = first_error {
        discard_chunks(&app_state, &stored).await; // do not leak partial uploads
        return Err(e);
    }
    stored.sort_by_key(|c| c.idx);

    // ---- commit metadata atomically (data first, metadata last)
    let replica_total: usize = stored.iter().map(|c| c.nodes.len()).sum();
    let new_file = NewFile {
        file_id,
        user_id: user.user.id,
        recipient_user_id: recipient.id,
        file_name,
        file_size: total as i64,
        chunk_size: chunk_size as i32,
        encrypted_aes_key: wrapped_key,
        link_password_hash,
        expiration_date,
        chunks: stored.clone(),
    };
    if let Err(e) = app_state.db_client.save_file(new_file).await {
        discard_chunks(&app_state, &stored).await;
        return Err(HttpError::server_error(e.to_string()));
    }

    tracing::info!(
        "[{}] uploaded {} bytes as {} chunks ({} replicas) in {} ms",
        app_state.env.node_id,
        total,
        n_chunks,
        replica_total,
        started.elapsed().as_millis()
    );

    Ok(Json(ResponseDto {
        message: "File uploaded and encrypted successfully".to_string(),
        status: "success",
    }))
}

async fn discard_chunks(app_state: &Arc<AppState>, chunks: &[StoredChunk]) {
    let cluster = &app_state.cluster;
    join_all(
        chunks
            .iter()
            .flat_map(|c| c.nodes.iter().map(move |n| (c.chunk_id.clone(), n.clone())))
            .map(|(chunk, node)| {
                let cluster = cluster.clone();
                async move { cluster.delete_chunk(&node, &chunk).await }
            }),
    )
    .await;
}

// ===================================================================================
//  RETRIEVE
//
//  chunks are fetched from the storage nodes in parallel (failing over between
//  replicas), decrypted in parallel, and STREAMED to the client in the right order.
// ===================================================================================
pub async fn retrieve_file(
    Extension(app_state): Extension<Arc<AppState>>,
    Extension(user): Extension<JWTAuthMiddeware>,
    Json(body): Json<RetrieveFileDto>,
) -> Result<impl IntoResponse, HttpError> {
    body.validate().map_err(|e| HttpError::bad_request(e.to_string()))?;

    let user_id = user.user.id;
    let shared_id = Uuid::parse_str(&body.shared_id)
        .map_err(|_| HttpError::bad_request("shared_id is not a valid id"))?;

    let shared = app_state
        .db_client
        .get_shared(shared_id, user_id)
        .await
        .map_err(|e| HttpError::server_error(e.to_string()))?
        .ok_or_else(|| HttpError::bad_request("The requested shared link either does not exist or has expired."))?;

    let password_ok = password::compare_async(body.password.clone(), shared.password.clone())
        .await
        .map_err(|e| HttpError::server_error(e.to_string()))?;
    if !password_ok {
        return Err(HttpError::bad_request("The provided password is incorrect."));
    }

    let file_id = shared.file_id.ok_or_else(|| HttpError::bad_request("File ID is missing"))?;

    let file = app_state
        .db_client
        .get_file(file_id)
        .await
        .map_err(|e| HttpError::server_error(e.to_string()))?
        .ok_or_else(|| HttpError::bad_request("The requested file either does not exist or has expired."))?;
    let chunks = app_state
        .db_client
        .get_file_chunks(file_id)
        .await
        .map_err(|e| HttpError::server_error(e.to_string()))?;
    let replicas = app_state
        .db_client
        .get_file_replicas(file_id)
        .await
        .map_err(|e| HttpError::server_error(e.to_string()))?;

    // ---- recover the file key: open the user's sealed private key, unwrap the file key
    let sealed = user
        .user
        .private_key_enc
        .clone()
        .ok_or_else(|| HttpError::server_error("user has no private key"))?;
    let master_key = app_state.env.master_key;
    let wrapped = file.encrypted_aes_key.clone();
    let file_key = tokio::task::spawn_blocking(move || -> Result<[u8; 32], HttpError> {
        let pem = crypto::open_secret(&master_key, user_id.as_bytes(), &sealed)?;
        let pem = String::from_utf8(pem).map_err(|e| HttpError::server_error(e.to_string()))?;
        let private_key =
            RsaPrivateKey::from_pkcs1_pem(&pem).map_err(|e| HttpError::server_error(e.to_string()))?;
        crypto::unwrap_file_key(&private_key, &wrapped)
    })
    .await
    .map_err(join_err)??;

    // ---- PARALLEL fetch + decrypt, ORDER-PRESERVING stream (`buffered` keeps the order)
    let cluster = app_state.cluster.clone();
    let parallelism = app_state.env.parallelism;
    let body_stream = stream::iter(chunks)
        .map(move |meta| {
            let cluster = cluster.clone();
            let holders = replicas.get(&meta.chunk_id).cloned().unwrap_or_default();
            async move {
                let to_io = |e: HttpError| std::io::Error::new(std::io::ErrorKind::Other, e.message);
                let ciphertext = cluster.get_chunk(&meta.chunk_id, &holders).await.map_err(to_io)?;
                let plain = tokio::task::spawn_blocking(move || {
                    crypto::decrypt_chunk(&file_key, &file_id, meta.idx as u32, &meta.nonce, &ciphertext)
                })
                .await
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?
                .map_err(to_io)?;
                Ok::<Bytes, std::io::Error>(Bytes::from(plain))
            }
        })
        .buffered(parallelism);

    let response = HttpResponse::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .header(header::CONTENT_LENGTH, file.file_size.to_string())
        .header(header::CONTENT_DISPOSITION, content_disposition(&file.file_name))
        .body(Body::from_stream(body_stream))
        .map_err(|e| HttpError::server_error(e.to_string()))?;

    Ok(response)
}

/// `attachment; filename="ascii-fallback"; filename*=UTF-8''percent-encoded`
fn content_disposition(name: &str) -> String {
    let ascii: String = name
        .chars()
        .map(|c| if c.is_ascii_graphic() && c != '"' && c != '\\' || c == ' ' { c } else { '_' })
        .collect();
    let mut encoded = String::new();
    for b in name.bytes() {
        if b.is_ascii_alphanumeric() || b"-._~".contains(&b) {
            encoded.push(b as char);
        } else {
            encoded.push_str(&format!("%{:02X}", b));
        }
    }
    format!("attachment; filename=\"{}\"; filename*=UTF-8''{}", ascii, encoded)
}
