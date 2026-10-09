use std::collections::HashMap;

use chrono::{DateTime, Utc};
use sqlx::{PgPool};
use uuid::Uuid;

use crate::models::{
    ChunkMeta, File, NewFile, ReceiveFileDetails, SentFileDetails, SharedLink, User,
};

/// Thin wrapper around the PostgreSQL pool.
///
/// PostgreSQL is the *metadata* store of the system (users, files, links, chunk map,
/// replica map). File bytes are NOT stored here any more.
///
/// Plain runtime queries are used (instead of the compile-time `query!` macros of the
/// original code) so the project builds without a running database.
#[derive(Debug, Clone)]
pub struct DBClient {
    pool: PgPool,
}

#[derive(Debug, Default, serde::Serialize)]
pub struct DbStats {
    pub files: i64,
    pub chunks: i64,
    pub replicas: i64,
    pub under_replicated: i64,
}

impl DBClient {
    pub fn new(pool: PgPool) -> Self {
        DBClient { pool }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub async fn ping(&self) -> Result<(), sqlx::Error> {
        sqlx::query("SELECT 1").execute(&self.pool).await.map(|_| ())
    }

    // ------------------------------------------------------------------ users

    pub async fn get_user(
        &self,
        user_id: Option<Uuid>,
        name: Option<&str>,
        email: Option<&str>,
    ) -> Result<Option<User>, sqlx::Error> {
        if let Some(id) = user_id {
            sqlx::query_as::<_, User>("SELECT * FROM users WHERE id = $1")
                .bind(id)
                .fetch_optional(&self.pool)
                .await
        } else if let Some(name) = name {
            sqlx::query_as::<_, User>("SELECT * FROM users WHERE name = $1")
                .bind(name)
                .fetch_optional(&self.pool)
                .await
        } else if let Some(email) = email {
            sqlx::query_as::<_, User>("SELECT * FROM users WHERE email = $1")
                .bind(email)
                .fetch_optional(&self.pool)
                .await
        } else {
            Ok(None)
        }
    }

    pub async fn save_user(&self, name: &str, email: &str, password: &str) -> Result<User, sqlx::Error> {
        sqlx::query_as::<_, User>(
            "INSERT INTO users (name, email, password) VALUES ($1, $2, $3) RETURNING *",
        )
        .bind(name)
        .bind(email)
        .bind(password)
        .fetch_one(&self.pool)
        .await
    }

    pub async fn update_user_name(&self, user_id: Uuid, name: &str) -> Result<User, sqlx::Error> {
        sqlx::query_as::<_, User>(
            "UPDATE users SET name = $1, updated_at = NOW() WHERE id = $2 RETURNING *",
        )
        .bind(name)
        .bind(user_id)
        .fetch_one(&self.pool)
        .await
    }

    pub async fn update_user_password(&self, user_id: Uuid, password: String) -> Result<User, sqlx::Error> {
        sqlx::query_as::<_, User>(
            "UPDATE users SET password = $1, updated_at = NOW() WHERE id = $2 RETURNING *",
        )
        .bind(password)
        .bind(user_id)
        .fetch_one(&self.pool)
        .await
    }

    pub async fn save_user_key(
        &self,
        user_id: Uuid,
        public_key: String,
        private_key_enc: Vec<u8>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            "UPDATE users SET public_key = $1, private_key_enc = $2, updated_at = NOW() WHERE id = $3",
        )
        .bind(public_key)
        .bind(private_key_enc)
        .bind(user_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn search_by_email(&self, user_id: Uuid, query: String) -> Result<Vec<User>, sqlx::Error> {
        sqlx::query_as::<_, User>(
            "SELECT * FROM users WHERE email LIKE $1 AND public_key IS NOT NULL AND id != $2",
        )
        .bind(query)
        .bind(user_id)
        .fetch_all(&self.pool)
        .await
    }

    // ------------------------------------------------------------------ files (metadata)

    /// Commit a finished upload ATOMICALLY: file row + chunk list + replica map + share link.
    /// The chunk bytes were already written to the storage nodes ("data first, metadata
    /// last"), so a crash before this point leaves no visible, half-written file.
    pub async fn save_file(&self, f: NewFile) -> Result<(), sqlx::Error> {
        let mut tx = self.pool.begin().await?;

        sqlx::query(
            "INSERT INTO files (id, user_id, file_name, file_size, encrypted_aes_key, chunk_size, chunk_count, created_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())",
        )
        .bind(f.file_id)
        .bind(f.user_id)
        .bind(&f.file_name)
        .bind(f.file_size)
        .bind(&f.encrypted_aes_key)
        .bind(f.chunk_size)
        .bind(f.chunks.len() as i32)
        .execute(&mut *tx)
        .await?;

        if !f.chunks.is_empty() {
            let idxs: Vec<i32> = f.chunks.iter().map(|c| c.idx).collect();
            let ids: Vec<String> = f.chunks.iter().map(|c| c.chunk_id.clone()).collect();
            let nonces: Vec<Vec<u8>> = f.chunks.iter().map(|c| c.nonce.clone()).collect();
            let sizes: Vec<i32> = f.chunks.iter().map(|c| c.size).collect();

            sqlx::query(
                "INSERT INTO file_chunks (file_id, idx, chunk_id, nonce, size)
                 SELECT $1::uuid, t.idx, t.chunk_id, t.nonce, t.size
                 FROM UNNEST($2::int[], $3::text[], $4::bytea[], $5::int[]) AS t(idx, chunk_id, nonce, size)",
            )
            .bind(f.file_id)
            .bind(idxs)
            .bind(ids)
            .bind(nonces)
            .bind(sizes)
            .execute(&mut *tx)
            .await?;

            let mut rep_chunks: Vec<String> = Vec::new();
            let mut rep_nodes: Vec<String> = Vec::new();
            for c in &f.chunks {
                for n in &c.nodes {
                    rep_chunks.push(c.chunk_id.clone());
                    rep_nodes.push(n.clone());
                }
            }
            sqlx::query(
                "INSERT INTO chunk_replicas (chunk_id, node_id)
                 SELECT * FROM UNNEST($1::text[], $2::text[])
                 ON CONFLICT DO NOTHING",
            )
            .bind(rep_chunks)
            .bind(rep_nodes)
            .execute(&mut *tx)
            .await?;
        }

        sqlx::query(
            "INSERT INTO shared_links (file_id, recipient_user_id, password, expiration_date, created_at)
             VALUES ($1, $2, $3, $4, NOW())",
        )
        .bind(f.file_id)
        .bind(f.recipient_user_id)
        .bind(&f.link_password_hash)
        .bind(f.expiration_date)
        .execute(&mut *tx)
        .await?;

        tx.commit().await
    }

    pub async fn get_shared(&self, shared_id: Uuid, user_id: Uuid) -> Result<Option<SharedLink>, sqlx::Error> {
        sqlx::query_as::<_, SharedLink>(
            "SELECT id, file_id, recipient_user_id, password, expiration_date, created_at
             FROM shared_links
             WHERE id = $1 AND recipient_user_id = $2 AND expiration_date > NOW()",
        )
        .bind(shared_id)
        .bind(user_id)
        .fetch_optional(&self.pool)
        .await
    }

    pub async fn get_file(&self, file_id: Uuid) -> Result<Option<File>, sqlx::Error> {
        sqlx::query_as::<_, File>(
            "SELECT id, user_id, file_name, file_size, encrypted_aes_key, chunk_size, chunk_count, created_at
             FROM files WHERE id = $1",
        )
        .bind(file_id)
        .fetch_optional(&self.pool)
        .await
    }

    pub async fn get_file_chunks(&self, file_id: Uuid) -> Result<Vec<ChunkMeta>, sqlx::Error> {
        sqlx::query_as::<_, ChunkMeta>(
            "SELECT idx, chunk_id, nonce, size FROM file_chunks WHERE file_id = $1 ORDER BY idx",
        )
        .bind(file_id)
        .fetch_all(&self.pool)
        .await
    }

    /// chunk_id -> nodes that hold a copy
    pub async fn get_file_replicas(&self, file_id: Uuid) -> Result<HashMap<String, Vec<String>>, sqlx::Error> {
        let rows = sqlx::query_as::<_, (String, String)>(
            "SELECT chunk_id, node_id FROM chunk_replicas
             WHERE chunk_id IN (SELECT chunk_id FROM file_chunks WHERE file_id = $1)",
        )
        .bind(file_id)
        .fetch_all(&self.pool)
        .await?;
        let mut map: HashMap<String, Vec<String>> = HashMap::new();
        for (chunk, node) in rows {
            map.entry(chunk).or_default().push(node);
        }
        Ok(map)
    }

    pub async fn get_sent_files(
        &self,
        user_id: Uuid,
        page: u32,
        limit: usize,
    ) -> Result<(Vec<SentFileDetails>, i64), sqlx::Error> {
        let offset = (page - 1) * limit as u32;

        let files = sqlx::query_as::<_, SentFileDetails>(
            "SELECT f.id AS file_id, f.file_name, u.email AS recipient_email, sl.expiration_date, sl.created_at
             FROM shared_links sl
             JOIN files f ON sl.file_id = f.id
             JOIN users u ON sl.recipient_user_id = u.id
             WHERE f.user_id = $1
             ORDER BY sl.created_at DESC
             LIMIT $2 OFFSET $3",
        )
        .bind(user_id)
        .bind(limit as i64)
        .bind(offset as i64)
        .fetch_all(&self.pool)
        .await?;

        let total: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM shared_links sl JOIN files f ON sl.file_id = f.id WHERE f.user_id = $1",
        )
        .bind(user_id)
        .fetch_one(&self.pool)
        .await?;

        Ok((files, total))
    }

    pub async fn get_receive_files(
        &self,
        user_id: Uuid,
        page: u32,
        limit: usize,
    ) -> Result<(Vec<ReceiveFileDetails>, i64), sqlx::Error> {
        let offset = (page - 1) * limit as u32;

        let files = sqlx::query_as::<_, ReceiveFileDetails>(
            "SELECT sl.id AS file_id, f.file_name, u.email AS sender_email, sl.expiration_date, sl.created_at
             FROM shared_links sl
             JOIN files f ON sl.file_id = f.id
             JOIN users u ON f.user_id = u.id
             WHERE sl.recipient_user_id = $1
             ORDER BY sl.created_at DESC
             LIMIT $2 OFFSET $3",
        )
        .bind(user_id)
        .bind(limit as i64)
        .bind(offset as i64)
        .fetch_all(&self.pool)
        .await?;

        let total: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM shared_links sl JOIN files f ON sl.file_id = f.id WHERE sl.recipient_user_id = $1",
        )
        .bind(user_id)
        .fetch_one(&self.pool)
        .await?;

        Ok((files, total))
    }

    // ------------------------------------------------------------------ cluster bookkeeping

    pub async fn expired_file_ids(&self, limit: i64) -> Result<Vec<Uuid>, sqlx::Error> {
        sqlx::query_scalar::<_, Uuid>(
            "SELECT DISTINCT file_id FROM shared_links
             WHERE expiration_date < NOW() AND file_id IS NOT NULL
             LIMIT $1",
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await
    }

    /// Delete a file's metadata and return the (chunk_id, node_id) copies that are now
    /// unreferenced and must be removed from the storage nodes.
    pub async fn delete_file_returning_replicas(
        &self,
        file_id: Uuid,
    ) -> Result<Vec<(String, String)>, sqlx::Error> {
        let mut tx = self.pool.begin().await?;

        let chunk_ids: Vec<String> =
            sqlx::query_scalar("SELECT chunk_id FROM file_chunks WHERE file_id = $1")
                .bind(file_id)
                .fetch_all(&mut *tx)
                .await?;

        // cascades to file_chunks and shared_links
        sqlx::query("DELETE FROM files WHERE id = $1")
            .bind(file_id)
            .execute(&mut *tx)
            .await?;

        let copies = sqlx::query_as::<_, (String, String)>(
            "DELETE FROM chunk_replicas cr
             WHERE cr.chunk_id = ANY($1::text[])
               AND NOT EXISTS (SELECT 1 FROM file_chunks fc WHERE fc.chunk_id = cr.chunk_id)
             RETURNING cr.chunk_id, cr.node_id",
        )
        .bind(chunk_ids)
        .fetch_all(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(copies)
    }

    /// Chunks that have fewer than `want` copies on nodes that are not in `excluded_nodes`.
    pub async fn find_under_replicated(
        &self,
        excluded_nodes: &[String],
        want: i64,
        limit: i64,
    ) -> Result<Vec<String>, sqlx::Error> {
        sqlx::query_scalar::<_, String>(
            "SELECT chunk_id FROM chunk_replicas
             GROUP BY chunk_id
             HAVING COUNT(*) FILTER (WHERE NOT (node_id = ANY($1::text[]))) < $2
             LIMIT $3",
        )
        .bind(excluded_nodes.to_vec())
        .bind(want)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
    }

    pub async fn get_replica_nodes(&self, chunk_id: &str) -> Result<Vec<String>, sqlx::Error> {
        sqlx::query_scalar::<_, String>("SELECT node_id FROM chunk_replicas WHERE chunk_id = $1")
            .bind(chunk_id)
            .fetch_all(&self.pool)
            .await
    }

    /// Record a new replica. Returns false (and records nothing) if the chunk is no
    /// longer referenced by any file, i.e. the file was deleted in the meantime.
    pub async fn add_replica(&self, chunk_id: &str, node_id: &str) -> Result<bool, sqlx::Error> {
        let res = sqlx::query(
            "INSERT INTO chunk_replicas (chunk_id, node_id)
             SELECT $1::text, $2::text
             WHERE EXISTS (SELECT 1 FROM file_chunks WHERE chunk_id = $1::text)
             ON CONFLICT DO NOTHING",
        )
        .bind(chunk_id)
        .bind(node_id)
        .execute(&self.pool)
        .await?;
        Ok(res.rows_affected() > 0)
    }

    pub async fn stats(&self, down_nodes: &[String], want: i64) -> Result<DbStats, sqlx::Error> {
        let (files, chunks, replicas): (i64, i64, i64) = sqlx::query_as(
            "SELECT (SELECT COUNT(*) FROM files),
                    (SELECT COUNT(DISTINCT chunk_id) FROM file_chunks),
                    (SELECT COUNT(*) FROM chunk_replicas)",
        )
        .fetch_one(&self.pool)
        .await?;

        let under_replicated: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM (
                 SELECT 1 FROM chunk_replicas
                 GROUP BY chunk_id
                 HAVING COUNT(*) FILTER (WHERE NOT (node_id = ANY($1::text[]))) < $2
             ) t",
        )
        .bind(down_nodes.to_vec())
        .bind(want)
        .fetch_one(&self.pool)
        .await?;

        Ok(DbStats { files, chunks, replicas, under_replicated })
    }

    pub async fn leader(&self) -> Option<(String, DateTime<Utc>)> {
        sqlx::query_as::<_, (String, DateTime<Utc>)>(
            "SELECT value, updated_at FROM cluster_meta WHERE key = 'leader'",
        )
        .fetch_optional(&self.pool)
        .await
        .ok()
        .flatten()
    }
}
