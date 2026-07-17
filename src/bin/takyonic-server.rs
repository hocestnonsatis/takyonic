//! Takyonic PostgreSQL wire-protocol server.
//!
//! Runs one node of a Raft cluster and exposes a Postgres wire endpoint in front
//! of the MVCC / CBO engine. It is configurable via CLI flags or environment
//! variables so the same binary powers both a single-node dev server and a
//! multi-node Docker cluster.
//!
//! ```text
//! # Single node (defaults: node-id 1, no peers, pgwire :5433, raft :5001):
//! takyonic-server
//!
//! # One member of a 3-node cluster:
//! takyonic-server --node-id 1 --peers 2:node-2:5001,3:node-3:5001
//!
//! # Then, from any host that can reach the pgwire port:
//! PGPASSWORD=any psql -h 127.0.0.1 -p 5433 -U admin -d postgres
//! ```
//!
//! Every flag has an environment-variable equivalent (handy for Docker Compose):
//! `TAKYONIC_NODE_ID`, `TAKYONIC_PEERS`, `TAKYONIC_RAFT_PORT`,
//! `TAKYONIC_PG_PORT`, `TAKYONIC_BIND_HOST`, `TAKYONIC_DATA`.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use tokio::net::TcpListener;
use tracing::{info, warn};

use pgwire::tokio::process_socket;
use takyonic::pg::TakyonicPgFactory;
use takyonic::{Config, IndexDef, TableSchema, TakyonicClient, TakyonicNode};

/// Resolved runtime configuration for one server process.
struct ServerArgs {
    node_id: u64,
    /// Peer id -> `host:port` (advertised Raft address), excluding self.
    peers: HashMap<u64, String>,
    raft_port: u16,
    pg_port: u16,
    bind_host: String,
    data_dir: PathBuf,
}

fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.is_empty())
}

/// Parse `id:host:port,id:host:port,...` into a peer map (self is excluded by
/// the caller, but a self-referential entry is tolerated and dropped later).
fn parse_peers(raw: &str) -> Result<HashMap<u64, String>> {
    let mut peers = HashMap::new();
    for spec in raw.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        let (id, addr) = spec
            .split_once(':')
            .ok_or_else(|| anyhow!("bad peer spec `{spec}` (expected id:host:port)"))?;
        let id: u64 = id
            .parse()
            .with_context(|| format!("bad peer id in `{spec}`"))?;
        if addr.is_empty() {
            bail!("bad peer spec `{spec}` (missing host:port)");
        }
        peers.insert(id, addr.to_string());
    }
    Ok(peers)
}

fn parse_args() -> Result<ServerArgs> {
    let mut node_id: Option<u64> = None;
    let mut peers_raw: Option<String> = None;
    let mut raft_port: Option<u16> = None;
    let mut pg_port: Option<u16> = None;
    let mut bind_host: Option<String> = None;
    let mut data_dir: Option<PathBuf> = None;

    let mut args = std::env::args().skip(1);
    while let Some(flag) = args.next() {
        let mut take = |name: &str| -> Result<String> {
            args.next()
                .ok_or_else(|| anyhow!("flag `{name}` requires a value"))
        };
        match flag.as_str() {
            "--node-id" => node_id = Some(take("--node-id")?.parse().context("--node-id")?),
            "--peers" => peers_raw = Some(take("--peers")?),
            "--raft-port" => raft_port = Some(take("--raft-port")?.parse().context("--raft-port")?),
            "--pg-port" => pg_port = Some(take("--pg-port")?.parse().context("--pg-port")?),
            "--bind-host" => bind_host = Some(take("--bind-host")?),
            "--data-dir" => data_dir = Some(PathBuf::from(take("--data-dir")?)),
            "-h" | "--help" => {
                println!(
                    "takyonic-server [--node-id N] [--peers id:host:port,...] \
[--raft-port P] [--pg-port P] [--bind-host H] [--data-dir PATH]"
                );
                std::process::exit(0);
            }
            other => bail!("unknown argument `{other}` (try --help)"),
        }
    }

    let node_id = node_id
        .or_else(|| env("TAKYONIC_NODE_ID").and_then(|v| v.parse().ok()))
        .unwrap_or(1);
    let peers = match peers_raw.or_else(|| env("TAKYONIC_PEERS")) {
        Some(raw) => parse_peers(&raw)?,
        None => HashMap::new(),
    };
    let raft_port = raft_port
        .or_else(|| env("TAKYONIC_RAFT_PORT").and_then(|v| v.parse().ok()))
        .unwrap_or(5001);
    let pg_port = pg_port
        .or_else(|| env("TAKYONIC_PG_PORT").and_then(|v| v.parse().ok()))
        .unwrap_or(5433);
    let bind_host = bind_host
        .or_else(|| env("TAKYONIC_BIND_HOST"))
        .unwrap_or_else(|| "0.0.0.0".to_string());
    let data_dir = data_dir
        .or_else(|| env("TAKYONIC_DATA").map(PathBuf::from))
        .unwrap_or_else(|| PathBuf::from("/data"));

    Ok(ServerArgs {
        node_id,
        peers,
        raft_port,
        pg_port,
        bind_host,
        data_dir,
    })
}

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

    let args = parse_args().context("parse server arguments")?;

    // Self binds to the wildcard host so it is reachable across a container
    // network; peers keep their advertised host:port addresses. `endpoints` is
    // this node's local view of the cluster (self endpoint is only used to bind).
    let raft_bind = format!("{}:{}", args.bind_host, args.raft_port);
    let pg_bind = format!("{}:{}", args.bind_host, args.pg_port);

    let mut endpoints: HashMap<u64, String> = args
        .peers
        .iter()
        .filter(|&(&id, _)| id != args.node_id)
        .map(|(&id, addr)| (id, addr.clone()))
        .collect();
    endpoints.insert(args.node_id, format!("0.0.0.0:{}", args.raft_port));

    let root = args.data_dir.clone();
    std::fs::create_dir_all(&root).with_context(|| format!("create data root {root:?}"))?;

    let peer_ids: Vec<u64> = endpoints
        .keys()
        .copied()
        .filter(|&id| id != args.node_id)
        .collect();
    info!(
        node_id = args.node_id,
        raft = %raft_bind,
        pgwire = %pg_bind,
        ?peer_ids,
        ?root,
        "opening Takyonic node"
    );

    let node = Arc::new(
        TakyonicNode::open(args.node_id, &root, endpoints.clone(), node_config(&root))
            .context("open node")?,
    );
    let (_server, _ticker) = node.start_background();

    // Discover a leader through any reachable member: the local node (via
    // loopback) plus every advertised peer. The smart client transparently
    // follows NotLeader redirects, so pgwire works no matter which node wins.
    let mut seeds = vec![format!("127.0.0.1:{}", args.raft_port)];
    for (&id, addr) in &endpoints {
        if id != args.node_id {
            seeds.push(addr.clone());
        }
    }

    let client = TakyonicClient::new(seeds);
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        match client.connect().await {
            Ok(()) => break,
            Err(e) => {
                if Instant::now() >= deadline {
                    return Err(anyhow!("no Raft leader reachable within 60s: {e}"));
                }
                warn!(%e, "waiting for Raft leader…");
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }
    info!("Raft leader reachable; cluster is ready");

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
    let listener = TcpListener::bind(&pg_bind)
        .await
        .with_context(|| format!("bind {pg_bind}"))?;

    println!("== Takyonic PostgreSQL wire server ==");
    println!("  node id   : {}", args.node_id);
    println!("  Raft/gRPC : {raft_bind}");
    println!("  pgwire    : {pg_bind}");
    println!("  peers     : {peer_ids:?}");
    println!(
        "  connect   : PGPASSWORD=any psql -h 127.0.0.1 -p {} -U admin -d postgres",
        args.pg_port
    );

    let mut shutdown = shutdown_signal();
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (socket, addr) = accepted.context("accept")?;
                info!(%addr, "pgwire client connected");
                let factory = Arc::clone(&factory);
                tokio::spawn(async move {
                    if let Err(e) = process_socket(socket, None, factory).await {
                        tracing::warn!(%e, "pgwire session ended with error");
                    }
                });
            }
            _ = &mut shutdown => {
                info!("shutdown signal received; closing node");
                break;
            }
        }
    }

    if let Err(e) = node.close() {
        warn!(%e, "error during node close");
    }
    Ok(())
}

/// Resolve on Ctrl-C (SIGINT) or SIGTERM (`docker stop`) so the engine can flush.
fn shutdown_signal() -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
    Box::pin(async {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};
            let mut term = match signal(SignalKind::terminate()) {
                Ok(s) => s,
                Err(_) => {
                    let _ = tokio::signal::ctrl_c().await;
                    return;
                }
            };
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = term.recv() => {}
            }
        }
        #[cfg(not(unix))]
        {
            let _ = tokio::signal::ctrl_c().await;
        }
    })
}
