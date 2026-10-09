use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize, sqlx::FromRow)]
pub struct User {
    pub id: uuid::Uuid,
    pub name: String,
    pub email: String,
    pub password: String,
    pub public_key: Option<String>,
    /// RSA private key (PEM) sealed with AES-256-GCM under the cluster MASTER_KEY.
    #[serde(skip)]
    pub private_key_enc: Option<Vec<u8>>,
    pub created_at: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
}

/// File metadata. The bytes themselves live as encrypted chunks on storage nodes.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct File {
    pub id: uuid::Uuid,
    pub user_id: Option<uuid::Uuid>,
    pub file_name: String,
    pub file_size: i64,
    /// Per-file AES-256 key, wrapped (RSA-OAEP) with the recipient's public key.
    pub encrypted_aes_key: Vec<u8>,
    pub chunk_size: i32,
    pub chunk_count: i32,
    pub created_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Deserialize, Serialize, sqlx::FromRow)]
pub struct SharedLink {
    pub id: uuid::Uuid,
    pub file_id: Option<uuid::Uuid>,
    pub recipient_user_id: Option<uuid::Uuid>,
    pub password: String,
    pub expiration_date: Option<DateTime<Utc>>,
    pub created_at: Option<DateTime<Utc>>,
}

#[derive(sqlx::FromRow)]
pub struct SentFileDetails {
    pub file_id: uuid::Uuid,
    pub file_name: String,
    pub recipient_email: String,
    pub expiration_date: Option<DateTime<Utc>>,
    pub created_at: Option<DateTime<Utc>>,
}

#[derive(sqlx::FromRow)]
pub struct ReceiveFileDetails {
    pub file_id: uuid::Uuid,
    pub file_name: String,
    pub sender_email: String,
    pub expiration_date: Option<DateTime<Utc>>,
    pub created_at: Option<DateTime<Utc>>,
}

/// One row of `file_chunks`.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct ChunkMeta {
    pub idx: i32,
    pub chunk_id: String,
    pub nonce: Vec<u8>,
    pub size: i32,
}

/// A chunk that has been encrypted and stored on `nodes` (used while uploading).
#[derive(Debug, Clone)]
pub struct StoredChunk {
    pub idx: i32,
    pub chunk_id: String,
    pub nonce: Vec<u8>,
    pub size: i32,
    pub nodes: Vec<String>,
}

/// Everything needed to commit a finished upload in a single DB transaction.
pub struct NewFile {
    pub file_id: uuid::Uuid,
    pub user_id: uuid::Uuid,
    pub recipient_user_id: uuid::Uuid,
    pub file_name: String,
    pub file_size: i64,
    pub chunk_size: i32,
    pub encrypted_aes_key: Vec<u8>,
    pub link_password_hash: String,
    pub expiration_date: DateTime<Utc>,
    pub chunks: Vec<StoredChunk>,
}
