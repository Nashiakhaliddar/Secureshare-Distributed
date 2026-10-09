//! Cluster manager: this is the "distributed systems" heart of the backend.
//!
//! Responsibilities
//!  * membership + FAILURE DETECTION  – heartbeat probing of every storage node, plus
//!    passive detection from failed requests;
//!  * DATA PLACEMENT                  – consistent hashing picks which nodes hold a chunk;
//!  * REPLICATION with QUORUM WRITES  – a chunk is sent to R nodes in parallel and the
//!    upload only succeeds if at least W nodes acknowledge;
//!  * FAILOVER READS + READ REPAIR    – reads try replicas until one returns data whose
//!    SHA-256 matches the chunk id; replicas found missing are healed in the background.

pub mod jobs;
pub mod leader;
pub mod ring;

use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

use axum::body::Bytes;
use futures::future::join_all;
use rand::seq::SliceRandom;
use serde::{Deserialize, Serialize};

use crate::{
    config::{Config, StorageNodeCfg},
    error::HttpError,
    utils::crypto::sha256_hex,
};
use ring::HashRing;

#[derive(Debug, Clone)]
struct NodeStatus {
    alive: bool,
    failures: u32,
    down_since: Option<Instant>,
    latency_ms: Option<u64>,
    chunks: u64,
    bytes: u64,
}

/// JSON-friendly view used by /api/cluster/status.
#[derive(Debug, Clone, Serialize)]
pub struct NodeSnapshot {
    pub id: String,
    pub url: String,
    pub alive: bool,
    pub consecutive_failures: u32,
    pub down_for_secs: Option<u64>,
    pub latency_ms: Option<u64>,
    pub chunks: u64,
    pub bytes: u64,
}

#[derive(Deserialize)]
struct HealthReply {
    node_id: String,
    chunks: u64,
    bytes: u64,
}

pub struct Cluster {
    http: reqwest::Client,
    token: String,
    nodes: Vec<StorageNodeCfg>,
    urls: HashMap<String, String>,
    ring: HashRing,
    status: RwLock<HashMap<String, NodeStatus>>,
    pub replication_factor: usize,
    pub write_quorum: usize,
    fail_threshold: u32,
    health_interval: Duration,
}

impl Cluster {
    pub fn new(cfg: &Config) -> Arc<Cluster> {
        let http = reqwest::Client::builder()
            .connect_timeout(Duration::from_millis(1500))
            .timeout(Duration::from_secs(30))
            .pool_max_idle_per_host(16)
            .build()
            .expect("failed to build HTTP client");

        let ids: Vec<String> = cfg.storage_nodes.iter().map(|n| n.id.clone()).collect();
        let status = cfg
            .storage_nodes
            .iter()
            .map(|n| {
                (
                    n.id.clone(),
                    NodeStatus { alive: true, failures: 0, down_since: None, latency_ms: None, chunks: 0, bytes: 0 },
                )
            })
            .collect();

        Arc::new(Cluster {
            http,
            token: cfg.cluster_token.clone(),
            urls: cfg.storage_nodes.iter().map(|n| (n.id.clone(), n.url.clone())).collect(),
            nodes: cfg.storage_nodes.clone(),
            ring: HashRing::new(&ids, 128),
            status: RwLock::new(status),
            replication_factor: cfg.replication_factor,
            write_quorum: cfg.write_quorum,
            fail_threshold: cfg.fail_threshold,
            health_interval: Duration::from_millis(cfg.health_interval_ms),
        })
    }

    // ------------------------------------------------------------------ membership / liveness

    fn with_status<R>(&self, f: impl FnOnce(&mut HashMap<String, NodeStatus>) -> R) -> R {
        let mut guard = self.status.write().unwrap_or_else(|e| e.into_inner());
        f(&mut guard)
    }

    pub fn is_alive(&self, node: &str) -> bool {
        let guard = self.status.read().unwrap_or_else(|e| e.into_inner());
        guard.get(node).map(|s| s.alive).unwrap_or(false)
    }

    pub fn alive_ids(&self) -> Vec<String> {
        let guard = self.status.read().unwrap_or_else(|e| e.into_inner());
        self.nodes
            .iter()
            .filter(|n| guard.get(&n.id).map(|s| s.alive).unwrap_or(false))
            .map(|n| n.id.clone())
            .collect()
    }

    /// Nodes that have been continuously down for at least `after` (=> treated as lost).
    pub fn long_dead(&self, after: Duration) -> Vec<String> {
        let guard = self.status.read().unwrap_or_else(|e| e.into_inner());
        self.nodes
            .iter()
            .filter(|n| {
                guard
                    .get(&n.id)
                    .map(|s| !s.alive && s.down_since.map(|t| t.elapsed() >= after).unwrap_or(false))
                    .unwrap_or(false)
            })
            .map(|n| n.id.clone())
            .collect()
    }

    pub fn snapshot(&self) -> Vec<NodeSnapshot> {
        let guard = self.status.read().unwrap_or_else(|e| e.into_inner());
        self.nodes
            .iter()
            .map(|n| {
                let s = guard.get(&n.id).cloned().unwrap();
                NodeSnapshot {
                    id: n.id.clone(),
                    url: n.url.clone(),
                    alive: s.alive,
                    consecutive_failures: s.failures,
                    down_for_secs: s.down_since.map(|t| t.elapsed().as_secs()),
                    latency_ms: s.latency_ms,
                    chunks: s.chunks,
                    bytes: s.bytes,
                }
            })
            .collect()
    }

    fn record_success(&self, node: &str, latency_ms: Option<u64>, stats: Option<(u64, u64)>) {
        let recovered = self.with_status(|m| {
            if let Some(s) = m.get_mut(node) {
                let was_down = !s.alive;
                s.alive = true;
                s.failures = 0;
                s.down_since = None;
                if latency_ms.is_some() {
                    s.latency_ms = latency_ms;
                }
                if let Some((c, b)) = stats {
                    s.chunks = c;
                    s.bytes = b;
                }
                was_down
            } else {
                false
            }
        });
        if recovered {
            tracing::info!("storage node '{}' is back UP", node);
        }
    }

    fn record_failure(&self, node: &str) {
        let threshold = self.fail_threshold;
        let went_down = self.with_status(|m| {
            if let Some(s) = m.get_mut(node) {
                s.failures += 1;
                if s.alive && s.failures >= threshold {
                    s.alive = false;
                    s.down_since = Some(Instant::now());
                    return true;
                }
            }
            false
        });
        if went_down {
            tracing::warn!("storage node '{}' marked DOWN", node);
        }
    }

    async fn probe(&self, node: &StorageNodeCfg) {
        let started = Instant::now();
        let result = self
            .http
            .get(format!("{}/health", node.url))
            .timeout(Duration::from_millis(1500))
            .send()
            .await;
        match result {
            Ok(resp) if resp.status().is_success() => match resp.json::<HealthReply>().await {
                Ok(h) => {
                    if h.node_id != node.id {
                        tracing::error!(
                            "node at {} identifies as '{}' but is configured as '{}'",
                            node.url, h.node_id, node.id
                        );
                    }
                    self.record_success(&node.id, Some(started.elapsed().as_millis() as u64), Some((h.chunks, h.bytes)));
                }
                Err(_) => self.record_failure(&node.id),
            },
            _ => self.record_failure(&node.id),
        }
    }

    /// One heartbeat round against every node, in parallel.
    pub async fn probe_all(self: &Arc<Self>) {
        join_all(self.nodes.iter().map(|n| self.probe(n))).await;
    }

    /// Background heartbeat loop (failure detector).
    pub fn spawn_health_checker(self: &Arc<Self>) {
        let this = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(this.health_interval).await;
                this.probe_all().await;
            }
        });
    }

    // ------------------------------------------------------------------ placement

    /// Live nodes in ring order for this chunk. The first R are the "primary" replicas;
    /// the rest are fallbacks used when a primary is down.
    pub fn placement(&self, chunk_id: &str) -> Vec<String> {
        self.ring
            .preference_list(chunk_id)
            .into_iter()
            .filter(|n| self.is_alive(n))
            .collect()
    }

    // ------------------------------------------------------------------ low-level node RPCs

    pub(crate) async fn put_one(&self, node: &str, chunk_id: &str, data: Bytes) -> Result<(), String> {
        let base = self.urls.get(node).ok_or_else(|| format!("unknown node {}", node))?;
        match self
            .http
            .put(format!("{}/chunks/{}", base, chunk_id))
            .header("x-cluster-token", &self.token)
            .body(data)
            .send()
            .await
        {
            Ok(r) if r.status().is_success() => Ok(()),
            Ok(r) => {
                if r.status().is_server_error() {
                    self.record_failure(node);
                }
                Err(format!("{} answered {}", node, r.status()))
            }
            Err(e) => {
                self.record_failure(node);
                Err(format!("{}: {}", node, e))
            }
        }
    }

    /// Ok(Some(bytes)) = found, Ok(None) = node is up but does not have it.
    async fn get_one(&self, node: &str, chunk_id: &str) -> Result<Option<Bytes>, String> {
        let base = self.urls.get(node).ok_or_else(|| format!("unknown node {}", node))?;
        match self
            .http
            .get(format!("{}/chunks/{}", base, chunk_id))
            .header("x-cluster-token", &self.token)
            .send()
            .await
        {
            Ok(r) if r.status().is_success() => r.bytes().await.map(Some).map_err(|e| {
                self.record_failure(node);
                format!("{}: {}", node, e)
            }),
            Ok(r) if r.status() == reqwest::StatusCode::NOT_FOUND => Ok(None),
            Ok(r) => {
                if r.status().is_server_error() {
                    self.record_failure(node);
                }
                Err(format!("{} answered {}", node, r.status()))
            }
            Err(e) => {
                self.record_failure(node);
                Err(format!("{}: {}", node, e))
            }
        }
    }

    pub async fn delete_chunk(&self, node: &str, chunk_id: &str) {
        if let Some(base) = self.urls.get(node) {
            let res = self
                .http
                .delete(format!("{}/chunks/{}", base, chunk_id))
                .header("x-cluster-token", &self.token)
                .send()
                .await;
            if res.is_err() {
                self.record_failure(node);
                tracing::warn!("could not delete chunk {} on {} (will be orphaned)", &chunk_id[..8], node);
            }
        }
    }

    // ------------------------------------------------------------------ replicated write

    /// Store a chunk on up to R live nodes IN PARALLEL.
    /// Succeeds when at least W nodes acknowledged. Returns the nodes that hold a copy.
    /// If a primary replica fails we fall back to the next live node on the ring
    /// ("sloppy quorum"); the repair job later brings the chunk up to R copies.
    pub async fn put_chunk(self: &Arc<Self>, chunk_id: &str, data: Bytes) -> Result<Vec<String>, HttpError> {
        let candidates = self.placement(chunk_id);
        if candidates.len() < self.write_quorum {
            return Err(HttpError::new(
                format!(
                    "only {} live storage node(s), need at least {} to write",
                    candidates.len(),
                    self.write_quorum
                ),
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
            ));
        }

        let r = self.replication_factor.min(candidates.len());
        let (primary, backups) = candidates.split_at(r);

        let results = join_all(primary.iter().map(|n| {
            let data = data.clone();
            async move { (n.clone(), self.put_one(n, chunk_id, data).await) }
        }))
        .await;

        let mut acked: Vec<String> = Vec::new();
        for (node, res) in results {
            match res {
                Ok(()) => acked.push(node),
                Err(e) => tracing::warn!("replica write failed: {}", e),
            }
        }

        for node in backups {
            if acked.len() >= r {
                break;
            }
            match self.put_one(node, chunk_id, data.clone()).await {
                Ok(()) => acked.push(node.clone()),
                Err(e) => tracing::warn!("fallback replica write failed: {}", e),
            }
        }

        if acked.len() < self.write_quorum {
            // roll back what we did write so we do not leak chunks
            for node in &acked {
                self.delete_chunk(node, chunk_id).await;
            }
            return Err(HttpError::new(
                format!("write quorum not reached ({}/{} acks)", acked.len(), self.write_quorum),
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
            ));
        }
        Ok(acked)
    }

    // ------------------------------------------------------------------ failover read

    /// Fetch a chunk from any replica. Live replicas first (shuffled to spread load),
    /// then replicas believed down as a last resort. The bytes are accepted only if
    /// their SHA-256 equals the chunk id, so a corrupt/malicious node cannot poison reads.
    pub async fn get_chunk(self: &Arc<Self>, chunk_id: &str, replicas: &[String]) -> Result<Bytes, HttpError> {
        let mut live: Vec<String> = replicas.iter().filter(|n| self.is_alive(n)).cloned().collect();
        let dead: Vec<String> = replicas.iter().filter(|n| !self.is_alive(n)).cloned().collect();
        live.shuffle(&mut rand::thread_rng());
        let order: Vec<String> = live.into_iter().chain(dead).collect();

        let mut needs_repair: Vec<String> = Vec::new();
        for node in &order {
            match self.get_one(node, chunk_id).await {
                Ok(Some(bytes)) if sha256_hex(&bytes) == chunk_id => {
                    // READ REPAIR: heal replicas that answered "not found"/corrupt.
                    for target in needs_repair.into_iter().filter(|n| self.is_alive(n)) {
                        let this = self.clone();
                        let data = bytes.clone();
                        let id = chunk_id.to_string();
                        tokio::spawn(async move {
                            if this.put_one(&target, &id, data).await.is_ok() {
                                tracing::info!("read-repair: restored chunk {} on {}", &id[..8], target);
                            }
                        });
                    }
                    return Ok(bytes);
                }
                Ok(Some(_)) => {
                    tracing::error!("replica {} returned CORRUPT data for chunk {}", node, &chunk_id[..8]);
                    needs_repair.push(node.clone());
                }
                Ok(None) => {
                    tracing::warn!("replica {} is missing chunk {}", node, &chunk_id[..8]);
                    needs_repair.push(node.clone());
                }
                Err(e) => tracing::warn!("read from replica failed: {}", e),
            }
        }
        Err(HttpError::new(
            format!("chunk {} unavailable: no replica could serve it", &chunk_id[..8.min(chunk_id.len())]),
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
        ))
    }
}
