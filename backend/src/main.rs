mod cluster;
mod config;
mod db;
mod dtos;
mod error;
mod handler;
mod middleware;
mod models;
mod router;
mod utils;

use std::{
    sync::{atomic::AtomicBool, Arc},
    time::Duration,
};

use axum::http::{
    header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE},
    HeaderName, HeaderValue, Method,
};
use cluster::Cluster;
use config::Config;
use db::DBClient;
use dotenv::dotenv;
use router::create_router;
use sqlx::postgres::PgPoolOptions;
use tower_http::cors::CorsLayer;
use tracing_subscriber::EnvFilter;

/// Shared by every request handler. Note that it holds NO per-user session state:
/// authentication is a signed JWT, so any API node can serve any request. That is
/// what makes it possible to put N identical API nodes behind a load balancer.
pub struct AppState {
    pub env: Config,
    pub db_client: DBClient,
    pub cluster: Arc<Cluster>,
    pub is_leader: Arc<AtomicBool>,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,sqlx=warn,tower_http=warn")),
        )
        .init();

    dotenv().ok();
    let config = Config::init();

    // ---- metadata database (retry: in Docker the DB may still be starting)
    let mut attempt = 0;
    let pool = loop {
        attempt += 1;
        match PgPoolOptions::new()
            .max_connections(20)
            .acquire_timeout(Duration::from_secs(10))
            .connect(&config.database_url)
            .await
        {
            Ok(pool) => break pool,
            Err(err) if attempt < 15 => {
                tracing::warn!("database not ready ({}), retrying in 2s [{}/15]", err, attempt);
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            Err(err) => {
                eprintln!("🔥 Failed to connect to the database: {:?}", err);
                std::process::exit(1);
            }
        }
    };
    println!("✅ [{}] connected to the database", config.node_id);

    // Safe to run on several nodes at once: sqlx serialises migrations with an advisory lock.
    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .expect("database migration failed");

    // ---- storage cluster: first heartbeat round, then keep probing in the background
    let storage = Cluster::new(&config);
    storage.probe_all().await;
    for n in storage.snapshot() {
        println!(
            "   storage node {:<10} {:<28} {}",
            n.id,
            n.url,
            if n.alive { "UP" } else { "DOWN" }
        );
    }
    storage.spawn_health_checker();

    // ---- leader election + leader-only background jobs
    let db_client = DBClient::new(pool.clone());
    let is_leader = Arc::new(AtomicBool::new(false));
    cluster::leader::spawn(pool, config.node_id.clone(), is_leader.clone());

    let app_state = Arc::new(AppState {
        env: config.clone(),
        db_client,
        cluster: storage,
        is_leader,
    });
    cluster::jobs::spawn(app_state.clone());

    // ---- HTTP
    let cors = CorsLayer::new()
        .allow_origin(config.cors_origin.parse::<HeaderValue>().expect("invalid CORS_ORIGIN"))
        .allow_headers([AUTHORIZATION, ACCEPT, CONTENT_TYPE])
        .expose_headers([HeaderName::from_static("x-served-by")])
        .allow_credentials(true)
        .allow_methods([Method::GET, Method::POST, Method::PUT, Method::DELETE]);

    let app = create_router(app_state).layer(cors);

    println!(
        "🚀 API node '{}' running on http://0.0.0.0:{} (R={}, W={}, chunk={} KiB, parallelism={})",
        config.node_id,
        config.port,
        config.replication_factor,
        config.write_quorum,
        config.chunk_size / 1024,
        config.parallelism
    );

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", config.port))
        .await
        .expect("cannot bind port");
    axum::serve(listener, app).await.unwrap();
}
