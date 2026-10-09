//! Background jobs. They only do work on the elected leader (see leader.rs).
//!
//!  * cleanup_round – delete expired files: metadata first, then the chunk copies.
//!  * repair_round  – SELF-HEALING: find chunks with fewer than R copies on healthy
//!                    nodes (because a node died) and re-replicate them elsewhere.

use std::{
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

use futures::{future::join_all, stream, StreamExt};

use crate::AppState;

pub fn spawn(state: Arc<AppState>) {
    // cleanup
    {
        let state = state.clone();
        tokio::spawn(async move {
            let every = Duration::from_secs(state.env.cleanup_interval_secs);
            loop {
                tokio::time::sleep(every).await;
                if state.is_leader.load(Ordering::SeqCst) {
                    if let Err(e) = cleanup_round(&state).await {
                        tracing::error!("cleanup failed: {}", e);
                    }
                }
            }
        });
    }
    // repair
    {
        let state = state.clone();
        tokio::spawn(async move {
            let every = Duration::from_secs(state.env.repair_interval_secs);
            loop {
                tokio::time::sleep(every).await;
                if state.is_leader.load(Ordering::SeqCst) {
                    if let Err(e) = repair_round(&state).await {
                        tracing::error!("repair failed: {}", e);
                    }
                }
            }
        });
    }
}

pub async fn cleanup_round(state: &Arc<AppState>) -> Result<(), String> {
    let ids = state.db_client.expired_file_ids(50).await.map_err(|e| e.to_string())?;
    if ids.is_empty() {
        return Ok(());
    }
    tracing::info!("cleanup: {} expired file(s)", ids.len());
    for file_id in ids {
        let copies = state
            .db_client
            .delete_file_returning_replicas(file_id)
            .await
            .map_err(|e| e.to_string())?;
        join_all(copies.iter().map(|(chunk, node)| state.cluster.delete_chunk(node, chunk))).await;
    }
    Ok(())
}

pub async fn repair_round(state: &Arc<AppState>) -> Result<(), String> {
    let cluster = &state.cluster;
    let alive = cluster.alive_ids();
    if alive.is_empty() {
        return Ok(());
    }
    // Can't keep more copies than there are live nodes.
    let want = cluster.replication_factor.min(alive.len());
    let lost = cluster.long_dead(Duration::from_secs(state.env.repair_after_secs));

    let chunks = state
        .db_client
        .find_under_replicated(&lost, want as i64, 100)
        .await
        .map_err(|e| e.to_string())?;
    if chunks.is_empty() {
        return Ok(());
    }
    tracing::info!("repair: {} under-replicated chunk(s), target {} copies", chunks.len(), want);

    let results: Vec<Result<usize, String>> = stream::iter(chunks)
        .map(|chunk_id| {
            let state = state.clone();
            let lost = lost.clone();
            async move { repair_one(&state, &chunk_id, &lost, want).await }
        })
        .buffer_unordered(state.env.parallelism)
        .collect()
        .await;

    let copied: usize = results.iter().filter_map(|r| r.as_ref().ok()).sum();
    for e in results.iter().filter_map(|r| r.as_ref().err()) {
        tracing::warn!("repair: {}", e);
    }
    if copied > 0 {
        tracing::info!("repair: created {} new replica(s)", copied);
    }
    Ok(())
}

async fn repair_one(state: &Arc<AppState>, chunk_id: &str, lost: &[String], want: usize) -> Result<usize, String> {
    let cluster = &state.cluster;
    let holders = state.db_client.get_replica_nodes(chunk_id).await.map_err(|e| e.to_string())?;
    if holders.is_empty() {
        return Ok(0); // chunk was deleted meanwhile
    }
    let healthy_holders = holders.iter().filter(|n| !lost.contains(n)).count();
    let need = want.saturating_sub(healthy_holders);
    if need == 0 {
        return Ok(0);
    }

    let sources: Vec<String> = holders.iter().filter(|n| cluster.is_alive(n)).cloned().collect();
    if sources.is_empty() {
        return Err(format!("chunk {} has no live source replica", &chunk_id[..8]));
    }
    let targets: Vec<String> = cluster
        .placement(chunk_id)
        .into_iter()
        .filter(|n| !holders.contains(n))
        .take(need)
        .collect();
    if targets.is_empty() {
        return Ok(0);
    }

    let data = cluster.get_chunk(chunk_id, &sources).await.map_err(|e| e.message)?;
    let mut created = 0;
    for target in targets {
        if cluster.put_one(&target, chunk_id, data.clone()).await.is_ok() {
            let tracked = state
                .db_client
                .add_replica(chunk_id, &target)
                .await
                .map_err(|e| e.to_string())?;
            if tracked {
                tracing::info!("repair: chunk {} copied to {}", &chunk_id[..8], target);
                created += 1;
            } else {
                // file was deleted while we were copying -> don't leave garbage
                cluster.delete_chunk(&target, chunk_id).await;
            }
        }
    }
    Ok(created)
}
