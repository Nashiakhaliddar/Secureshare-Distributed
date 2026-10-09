//! Leader election with a PostgreSQL session-level advisory lock.
//!
//! All API nodes try to take the same lock on a dedicated, long-lived DB connection.
//! Exactly one succeeds => that node is the leader and runs cluster-wide background
//! work (expired-file cleanup, re-replication) so it is not executed N times.
//! If the leader crashes or loses its DB connection, PostgreSQL releases the lock and
//! another node takes over on its next attempt (automatic fail-over).

use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use sqlx::{Connection, PgConnection, PgPool};

const LOCK_KEY: i64 = 0x5EC0_5A4E; // arbitrary application-wide constant

pub fn spawn(pool: PgPool, node_id: String, is_leader: Arc<AtomicBool>) {
    tokio::spawn(async move {
        let mut conn: Option<PgConnection> = None;
        loop {
            if conn.is_none() {
                // detach(): the connection leaves the pool, so dropping it really closes
                // the session and releases the advisory lock.
                match pool.acquire().await {
                    Ok(c) => conn = Some(c.detach()),
                    Err(e) => {
                        tracing::warn!("leader election: cannot get DB connection: {}", e);
                        is_leader.store(false, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_secs(2)).await;
                        continue;
                    }
                }
            }
            let c = conn.as_mut().unwrap();

            if !is_leader.load(Ordering::SeqCst) {
                match sqlx::query_scalar::<_, bool>("SELECT pg_try_advisory_lock($1)")
                    .bind(LOCK_KEY)
                    .fetch_one(&mut *c)
                    .await
                {
                    Ok(true) => {
                        is_leader.store(true, Ordering::SeqCst);
                        tracing::info!("*** {} is now the LEADER ***", node_id);
                        announce(&pool, &node_id).await;
                    }
                    Ok(false) => {}
                    Err(e) => {
                        tracing::warn!("leader election: lock attempt failed: {}", e);
                        conn = None;
                    }
                }
            } else if c.ping().await.is_err() {
                tracing::error!("{} lost its DB session -> stepping down as leader", node_id);
                is_leader.store(false, Ordering::SeqCst);
                conn = None;
            } else {
                announce(&pool, &node_id).await;
            }

            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    });
}

async fn announce(pool: &PgPool, node_id: &str) {
    let _ = sqlx::query(
        "INSERT INTO cluster_meta (key, value, updated_at) VALUES ('leader', $1, NOW())
         ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()",
    )
    .bind(node_id)
    .execute(pool)
    .await;
}
