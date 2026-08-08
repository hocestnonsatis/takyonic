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
//! # Local Tier-2 object store (POSIX mirror):
//! takyonic-server --object-store /data/objects
//!
//! # MinIO / S3 (requires `--features s3` build):
//! takyonic-server --s3-endpoint http://127.0.0.1:9000 --s3-bucket takyonic \
//!   --s3-access-key minioadmin --s3-secret-key minioadmin
//!
//! # One member of a 3-node cluster:
//! takyonic-server --node-id 1 --peers 2:node-2:5001,3:node-3:5001
//!
//! # Then, from any host that can reach the pgwire port:
//! PGPASSWORD=password psql -h 127.0.0.1 -p 5433 -U postgres -d postgres
//! ```
//!
//! Authentication is SCRAM-SHA-256 (SASL). The demo seeds role `postgres` /
//! password `password`.
//!
//! By default the server also registers a demo `users` table when missing
//! (`--demo-bootstrap`, env `TAKYONIC_DEMO_BOOTSTRAP=1`). Disable with
//! `--no-demo-bootstrap` / `TAKYONIC_DEMO_BOOTSTRAP=0` for an empty catalog
//! (create tables via SQL DDL).
//!
//! Every flag has an environment-variable equivalent (handy for Docker Compose):
//! `TAKYONIC_NODE_ID`, `TAKYONIC_PEERS`, `TAKYONIC_RAFT_PORT`,
//! `TAKYONIC_PG_PORT`, `TAKYONIC_BIND_HOST`, `TAKYONIC_DATA`,
//! `TAKYONIC_DEMO_BOOTSTRAP`, `TAKYONIC_OBJECT_STORE`, `TAKYONIC_S3_ENDPOINT`,
//! `TAKYONIC_S3_BUCKET`, `TAKYONIC_S3_REGION`, `TAKYONIC_S3_ACCESS_KEY`,
//! `TAKYONIC_S3_SECRET_KEY`.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use tokio::net::TcpListener;
use tracing::{info, warn};

use pgwire::tokio::process_socket;
use takyonic::pg::TakyonicPgFactory;
use takyonic::{
    Config, TakyonicClient, TakyonicNode, demo_users_schema, should_seed_demo_users,
};

/// Resolved runtime configuration for one server process.
struct ServerArgs {
    node_id: u64,
    /// Peer id -> `host:port` (advertised Raft address), excluding self.
    peers: HashMap<u64, String>,
    raft_port: u16,
    pg_port: u16,
    bind_host: String,
    data_dir: PathBuf,
    /// Local POSIX Tier-2 root (`Config::object_store_root`).
    object_store_root: Option<PathBuf>,
    /// MinIO / S3 endpoint (e.g. `http://minio:9000`).
    s3_endpoint: Option<String>,
    s3_bucket: Option<String>,
    s3_region: String,
    s3_access_key: Option<String>,
    s3_secret_key: Option<String>,
    /// Seed classic demo `users` table when absent (default true).
    demo_bootstrap: bool,
    /// Enable MPP Session→Coordinator path (`Config::mpp_enabled`).
    mpp_enabled: bool,
}

fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.is_empty())
}

/// Parse truthy/falsey env (`1`/`true`/`yes`/`on` vs `0`/`false`/`no`/`off`).
fn parse_env_bool(raw: &str) -> Option<bool> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
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
    let mut object_store_root: Option<PathBuf> = None;
    let mut s3_endpoint: Option<String> = None;
    let mut s3_bucket: Option<String> = None;
    let mut s3_region: Option<String> = None;
    let mut s3_access_key: Option<String> = None;
    let mut s3_secret_key: Option<String> = None;
    let mut demo_bootstrap: Option<bool> = None;
    let mut mpp_enabled: Option<bool> = None;

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
            "--object-store" => object_store_root = Some(PathBuf::from(take("--object-store")?)),
            "--s3-endpoint" => s3_endpoint = Some(take("--s3-endpoint")?),
            "--s3-bucket" => s3_bucket = Some(take("--s3-bucket")?),
            "--s3-region" => s3_region = Some(take("--s3-region")?),
            "--s3-access-key" => s3_access_key = Some(take("--s3-access-key")?),
            "--s3-secret-key" => s3_secret_key = Some(take("--s3-secret-key")?),
            "--demo-bootstrap" => demo_bootstrap = Some(true),
            "--no-demo-bootstrap" => demo_bootstrap = Some(false),
            "--mpp" => mpp_enabled = Some(true),
            "--no-mpp" => mpp_enabled = Some(false),
            "-h" | "--help" => {
                println!(
                    "takyonic-server [--node-id N] [--peers id:host:port,...] \
[--raft-port P] [--pg-port P] [--bind-host H] [--data-dir PATH] \
[--demo-bootstrap|--no-demo-bootstrap] [--mpp|--no-mpp] [--object-store PATH] \
[--s3-endpoint URL] [--s3-bucket NAME] [--s3-region R] \
[--s3-access-key K] [--s3-secret-key K]"
                );
                println!(
                    "\nDemo catalog: --demo-bootstrap (default) registers `users` when \
missing; --no-demo-bootstrap leaves an empty catalog (use CREATE TABLE)."
                );
                println!(
                    "\nMPP: --mpp enables Session Coordinator / shuffle (env TAKYONIC_MPP); \
default off. Distributed agg supports GROUP BY + SUM/COUNT."
                );
                println!(
                    "\nTier-2 storage: --object-store (local POSIX) or --s3-* (MinIO/AWS; \
build with --features s3)."
                );
                println!(
                    "\nExamples:\n  takyonic-server\n  \
takyonic-server --no-demo-bootstrap --data-dir ./data\n  \
TAKYONIC_DEMO_BOOTSTRAP=0 takyonic-server"
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
    let object_store_root = object_store_root.or_else(|| {
        env("TAKYONIC_OBJECT_STORE").map(PathBuf::from)
    });
    let s3_endpoint = s3_endpoint.or_else(|| env("TAKYONIC_S3_ENDPOINT"));
    let s3_bucket = s3_bucket.or_else(|| env("TAKYONIC_S3_BUCKET"));
    let s3_region = s3_region
        .or_else(|| env("TAKYONIC_S3_REGION"))
        .unwrap_or_else(|| "us-east-1".into());
    let s3_access_key = s3_access_key.or_else(|| env("TAKYONIC_S3_ACCESS_KEY"));
    let s3_secret_key = s3_secret_key.or_else(|| env("TAKYONIC_S3_SECRET_KEY"));
    let demo_bootstrap = demo_bootstrap
        .or_else(|| env("TAKYONIC_DEMO_BOOTSTRAP").and_then(|v| parse_env_bool(&v)))
        .unwrap_or(true);
    let mpp_enabled = mpp_enabled
        .or_else(|| env("TAKYONIC_MPP").and_then(|v| parse_env_bool(&v)))
        .unwrap_or(false);

    if s3_endpoint.is_some() ^ s3_bucket.is_some() {
        bail!("--s3-endpoint and --s3-bucket must be set together (or via TAKYONIC_S3_*)");
    }
    if s3_endpoint.is_some() && object_store_root.is_some() {
        bail!("choose either --object-store or --s3-endpoint, not both");
    }

    Ok(ServerArgs {
        node_id,
        peers,
        raft_port,
        pg_port,
        bind_host,
        data_dir,
        object_store_root,
        s3_endpoint,
        s3_bucket,
        s3_region,
        s3_access_key,
        s3_secret_key,
        demo_bootstrap,
        mpp_enabled,
    })
}

fn node_config(root: &std::path::Path, args: &ServerArgs) -> Config {
    let mut cfg = Config::default()
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
        .mpp_enabled(args.mpp_enabled);
    if let Some(ref path) = args.object_store_root {
        cfg = cfg.object_store_root(path);
    }
    if let Some(ref ep) = args.s3_endpoint {
        cfg = cfg.s3_endpoint(ep);
    }
    if let Some(ref b) = args.s3_bucket {
        cfg = cfg.s3_bucket(b);
    }
    cfg = cfg.s3_region(&args.s3_region);
    if let Some(ref k) = args.s3_access_key {
        cfg = cfg.s3_access_key(k);
    }
    if let Some(ref k) = args.s3_secret_key {
        cfg = cfg.s3_secret_key(k);
    }
    cfg
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
    let cfg = node_config(&root, &args);
    info!(
        node_id = args.node_id,
        raft = %raft_bind,
        pgwire = %pg_bind,
        ?peer_ids,
        ?root,
        object_store = ?args.object_store_root,
        s3_endpoint = ?args.s3_endpoint,
        s3_bucket = ?args.s3_bucket,
        "opening Takyonic node"
    );

    let node = if cfg.s3_configured() {
        #[cfg(feature = "s3")]
        {
            use takyonic::object_store::S3Backend;
            let endpoint = cfg.s3_endpoint.as_deref().unwrap();
            let bucket = cfg.s3_bucket.as_deref().unwrap();
            let access = cfg
                .s3_access_key
                .as_deref()
                .unwrap_or("minioadmin");
            let secret = cfg
                .s3_secret_key
                .as_deref()
                .unwrap_or("minioadmin");
            info!(%endpoint, %bucket, "connecting S3 / MinIO object store");
            let store = S3Backend::connect_minio(bucket, endpoint, access, secret)
                .await
                .context("connect S3/MinIO")?;
            Arc::new(
                TakyonicNode::open_with_object_storage(
                    args.node_id,
                    &root,
                    endpoints.clone(),
                    cfg,
                    Arc::new(store),
                )
                .context("open node with S3")?,
            )
        }
        #[cfg(not(feature = "s3"))]
        {
            bail!(
                "S3 endpoint configured but this binary was built without `--features s3`. \
Rebuild: cargo build --release --bin takyonic-server --features s3"
            );
        }
    } else {
        Arc::new(
            TakyonicNode::open(args.node_id, &root, endpoints.clone(), cfg)
                .context("open node")?,
        )
    };
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

    // Demo `users` is optional. Prefer the smart client so CatalogUpsert goes
    // through the Raft leader (local engine.register would NotLeader on followers).
    let users_present = node.engine().table_schema("users").is_ok();
    if !args.demo_bootstrap {
        info!("demo bootstrap disabled; catalog left empty (CREATE TABLE via psql)");
    } else if !should_seed_demo_users(true, users_present) {
        info!("demo bootstrap: users table already present");
    } else {
        client
            .register_table(demo_users_schema())
            .await
            .context("demo bootstrap: register users")?;
        info!("demo bootstrap: registered table users(id PK, indexes status/city)");
    }

    let factory = Arc::new(TakyonicPgFactory::new(client, Arc::clone(node.engine())));
    let listener = TcpListener::bind(&pg_bind)
        .await
        .with_context(|| format!("bind {pg_bind}"))?;
    if let Ok(local) = listener.local_addr() {
        factory.set_listen_addr(local);
    }

    println!("== Takyonic PostgreSQL wire server ==");
    println!("  node id   : {}", args.node_id);
    println!("  Raft/gRPC : {raft_bind}");
    println!("  pgwire    : {pg_bind}");
    println!("  peers     : {peer_ids:?}");
    if let Some(ref path) = args.object_store_root {
        println!("  objects   : local {path:?}");
    }
    if let Some(ref ep) = args.s3_endpoint {
        println!(
            "  objects   : s3 {} / {}",
            ep,
            args.s3_bucket.as_deref().unwrap_or("?")
        );
    }
    println!(
        "  demo seed : {}",
        if args.demo_bootstrap {
            "on (users if missing)"
        } else {
            "off (empty catalog)"
        }
    );
    println!(
        "  connect   : PGPASSWORD=password psql -h 127.0.0.1 -p {} -U postgres -d postgres",
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
