use std::str::FromStr;

/// One configured storage node (static membership; liveness is detected at runtime).
#[derive(Debug, Clone)]
pub struct StorageNodeCfg {
    pub id: String,
    pub url: String,
}

#[derive(Debug, Clone)]
pub struct Config {
    // ---- identity / basic ----
    pub node_id: String,
    pub database_url: String,
    pub jwt_secret: String,
    pub jwt_maxage: i64,
    pub port: u16,
    pub cors_origin: String,
    pub master_key: [u8; 32],

    // ---- distributed storage ----
    pub storage_nodes: Vec<StorageNodeCfg>,
    pub cluster_token: String,
    /// R: how many copies of every chunk we try to keep.
    pub replication_factor: usize,
    /// W: how many copies must be acknowledged before an upload succeeds.
    pub write_quorum: usize,
    pub chunk_size: usize,
    /// Max number of chunks processed concurrently (encrypt + replicate / fetch + decrypt).
    pub parallelism: usize,
    pub max_upload_bytes: usize,

    // ---- failure detection / background jobs ----
    pub health_interval_ms: u64,
    pub fail_threshold: u32,
    pub repair_after_secs: u64,
    pub repair_interval_secs: u64,
    pub cleanup_interval_secs: u64,
}

fn env_or<T: FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.trim().parse::<T>().ok())
        .unwrap_or(default)
}

fn env_req(key: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| panic!("{} must be set", key))
}

/// STORAGE_NODES="s1=http://localhost:9001,s2=http://localhost:9002"
fn parse_storage_nodes(raw: &str) -> Vec<StorageNodeCfg> {
    raw.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|entry| {
            let (id, url) = entry
                .split_once('=')
                .unwrap_or_else(|| panic!("bad STORAGE_NODES entry '{}', expected id=url", entry));
            StorageNodeCfg {
                id: id.trim().to_string(),
                url: url.trim().trim_end_matches('/').to_string(),
            }
        })
        .collect()
}

impl Config {
    pub fn init() -> Config {
        let master_hex = env_req("MASTER_KEY");
        let master_bytes = hex::decode(master_hex.trim())
            .expect("MASTER_KEY must be 64 hex characters (generate with: openssl rand -hex 32)");
        let master_key: [u8; 32] = master_bytes
            .try_into()
            .expect("MASTER_KEY must decode to exactly 32 bytes (64 hex characters)");

        let storage_nodes = parse_storage_nodes(&env_req("STORAGE_NODES"));
        assert!(!storage_nodes.is_empty(), "STORAGE_NODES must list at least one node");

        let replication_factor = env_or("REPLICATION_FACTOR", 3usize).max(1);
        let write_quorum = env_or("WRITE_QUORUM", 2usize).clamp(1, replication_factor);

        Config {
            node_id: env_or("NODE_ID", "api-1".to_string()),
            database_url: env_req("DATABASE_URL"),
            jwt_secret: env_req("JWT_SECRET_KEY"),
            jwt_maxage: env_req("JWT_MAXAGE").parse::<i64>().expect("JWT_MAXAGE must be a number"),
            port: env_or("PORT", 8000u16),
            cors_origin: env_or("CORS_ORIGIN", "http://localhost:3000".to_string()),
            master_key,

            storage_nodes,
            cluster_token: env_or("CLUSTER_TOKEN", "dev-cluster-token".to_string()),
            replication_factor,
            write_quorum,
            chunk_size: env_or("CHUNK_SIZE", 1024 * 1024usize).max(4 * 1024),
            parallelism: env_or("PARALLELISM", 8usize).max(1),
            max_upload_bytes: env_or("MAX_UPLOAD_BYTES", 256 * 1024 * 1024usize),

            health_interval_ms: env_or("HEALTH_INTERVAL_MS", 2000u64),
            fail_threshold: env_or("FAIL_THRESHOLD", 2u32).max(1),
            repair_after_secs: env_or("REPAIR_AFTER_SECS", 20u64),
            repair_interval_secs: env_or("REPAIR_INTERVAL_SECS", 10u64),
            cleanup_interval_secs: env_or("CLEANUP_INTERVAL_SECS", 60u64),
        }
    }
}
