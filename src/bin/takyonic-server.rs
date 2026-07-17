//! Takyonic PostgreSQL wire-protocol server.
//!
//! Spins up a single-node Raft cluster, registers the demo `users` table, and
//! masquerades as Postgres on `127.0.0.1:5433`.
//!
//! ```text
//! cargo run --release --bin takyonic-server
//! # then, in another terminal:
//! PGPASSWORD=any psql -h 127.0.0.1 -p 5433 -U admin -d postgres
//! ```

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::net::TcpListener;
use tracing::info;

use pgwire::tokio::process_socket;
use takyonic::pg::TakyonicPgFactory;
use takyonic::{Config, IndexDef, TableSchema, TakyonicClient, TakyonicNode, wait_for_leader};

const PG_ADDR: &str = "127.0.0.1:5433";
const RAFT_ADDR: &str = "127.0.0.1:15433";

fn node_config(root: &std::path::Path) -> Config {
    Config::default()
        .data_dir(root.join("data"))
        .wal_dir(root.join("wal"))
        .memtable_size_bytes(8 * 1024 * 1024)
        .l0_soft_limit(16)
        .l0_hard_limit(48)
        .l0_rapid_pool_threads(2)
        .ln_haul_pool_threads(2)
        .compaction_write_bytes_per_sec(256 * 1024 * 1024)
        .write_admission_ops_per_sec(1_000_000)
        .write_admission_min_ops_per_sec(50_000)
        .write_admission_burst(100_000)
}

#[tokio::main]
async fn main() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "takyonic=info,takyonic_server=info".into()),
        )
        .try_init();

    let root: PathBuf = std::env::var_os("TAKYONIC_DATA")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            std::env::temp_dir().join(format!("takyonic-pg-{}", std::process::id()))
        });
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).context("create data root")?;

    let mut endpoints = HashMap::new();
    endpoints.insert(1u64, RAFT_ADDR.to_string());

    info!(?root, raft = RAFT_ADDR, "opening Takyonic node");
    let node =
        Arc::new(TakyonicNode::open(1, &root, endpoints, node_config(&root)).context("open node")?);
    let (_server, _ticker) = node.start_background();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let leader = wait_for_leader(&[Arc::clone(&node)], Duration::from_secs(10))
        .await
        .context("await leader")?;
    info!(leader, "Raft leader ready");

    let client = TakyonicClient::new([RAFT_ADDR]);
    client.connect().await.context("smart client connect")?;
    client
        .register_table(TableSchema::new(
            "users",
            "id",
            vec![
                IndexDef::new("status", "status"),
                IndexDef::new("city", "city"),
            ],
        ))
        .await
        .context("register users")?;
    info!("registered table users(id PK, indexes status/city)");

    let factory = Arc::new(TakyonicPgFactory::new(client));
    let listener = TcpListener::bind(PG_ADDR)
        .await
        .with_context(|| format!("bind {PG_ADDR}"))?;

    println!("== Takyonic PostgreSQL wire server ==");
    println!("  Raft/gRPC : {RAFT_ADDR}");
    println!("  pgwire    : {PG_ADDR}");
    println!("  connect   : PGPASSWORD=any psql -h 127.0.0.1 -p 5433 -U admin -d postgres");
    println!(
        "  try       : INSERT INTO users (id, name, city, status) VALUES (1, 'Anil', 'Istanbul', 'active');"
    );
    println!("              SELECT * FROM users WHERE status = 'active';");

    loop {
        let (socket, addr) = listener.accept().await.context("accept")?;
        info!(%addr, "pgwire client connected");
        let factory = Arc::clone(&factory);
        tokio::spawn(async move {
            if let Err(e) = process_socket(socket, None, factory).await {
                tracing::warn!(%e, "pgwire session ended with error");
            }
        });
    }
}
