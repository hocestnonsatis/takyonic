//! Step 18: SQL parser + logical planner crucible.
//!
//! 1. Spin up a 3-node Takyonic cluster and connect the Smart Client.
//! 2. Register `users(status, city)` indexes.
//! 3. Populate 1_000 users via raw SQL `INSERT` (skewed: only 10 in city Ankara).
//! 4. Run `SELECT * FROM users WHERE status = 'active' AND city = 'Ankara'`.
//!
//! Invariant: the SQL string is parsed, the CBO drives on the `city` index
//! (not a full table scan / status scan), and exactly 10 rows are returned.
//!
//! Usage:
//!   cargo run --release --example sql_interface

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use takyonic::{Config, IndexDef, TableSchema, TakyonicClient, TakyonicNode, wait_for_leader};

const N_USERS: u64 = 1_000;
const ANKARA_COUNT: u64 = 10;
const ACTIVE_COUNT: u64 = 900;
const N_CITIES: u64 = 100;
const INSERT_BATCH: u64 = 50;

fn node_config(root: &std::path::Path, id: u64) -> Config {
    Config::default()
        .data_dir(root.join(format!("node-{id}")).join("data"))
        .wal_dir(root.join(format!("node-{id}")).join("wal"))
        .memtable_size_bytes(4 * 1024 * 1024)
        .l0_soft_limit(16)
        .l0_hard_limit(48)
        .l0_rapid_pool_threads(1)
        .ln_haul_pool_threads(1)
        .compaction_write_bytes_per_sec(64 * 1024 * 1024)
        .write_admission_ops_per_sec(500_000)
        .write_admission_min_ops_per_sec(10_000)
        .write_admission_burst(50_000)
}

fn city_for(id: u64) -> String {
    if id < ANKARA_COUNT {
        "Ankara".into()
    } else {
        let slot = 1 + ((id - ANKARA_COUNT) % (N_CITIES - 1));
        format!("C{slot:03}")
    }
}

fn status_for(id: u64) -> &'static str {
    if id < ACTIVE_COUNT {
        "active"
    } else {
        "inactive"
    }
}

fn sql_escape(s: &str) -> String {
    s.replace('\'', "''")
}

fn insert_sql(start: u64, end: u64) -> String {
    let mut sql = String::from("INSERT INTO users (id, name, status, city) VALUES ");
    for id in start..end {
        if id > start {
            sql.push_str(", ");
        }
        let name = format!("user-{id}");
        sql.push_str(&format!(
            "({}, '{}', '{}', '{}')",
            id,
            sql_escape(&name),
            status_for(id),
            sql_escape(&city_for(id)),
        ));
    }
    sql
}

struct LiveNode {
    node: Arc<TakyonicNode>,
    handles: Vec<tokio::task::JoinHandle<()>>,
}

#[tokio::main]
async fn main() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("takyonic=warn")
        .try_init();

    let root = std::env::temp_dir().join(format!("takyonic-sql-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();

    let mut endpoints = HashMap::new();
    endpoints.insert(1u64, "127.0.0.1:18101".into());
    endpoints.insert(2u64, "127.0.0.1:18102".into());
    endpoints.insert(3u64, "127.0.0.1:18103".into());
    let seeds = vec![
        "127.0.0.1:18101".to_string(),
        "127.0.0.1:18102".to_string(),
        "127.0.0.1:18103".to_string(),
    ];

    println!("== Takyonic SQL interface crucible ==");
    println!("users={N_USERS} ankara={ANKARA_COUNT} active≈{ACTIVE_COUNT} cities≈{N_CITIES}");

    let mut live: HashMap<u64, LiveNode> = HashMap::new();
    for id in 1u64..=3 {
        let node = Arc::new(
            TakyonicNode::open(
                id,
                root.join(format!("node-{id}")),
                endpoints.clone(),
                node_config(&root, id),
            )
            .expect("open"),
        );
        let (s, t) = node.start_background();
        live.insert(
            id,
            LiveNode {
                node,
                handles: vec![s, t],
            },
        );
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    let nodes: Vec<Arc<TakyonicNode>> = live.values().map(|l| Arc::clone(&l.node)).collect();
    let leader_id = wait_for_leader(&nodes, Duration::from_secs(10))
        .await
        .expect("leader");
    println!("phase0: leader=node-{leader_id}");

    let client = TakyonicClient::new(seeds.clone());
    client.connect().await.expect("client connect");

    let schema = TableSchema::new(
        "users",
        "id",
        vec![
            IndexDef::new("status", "status"),
            IndexDef::new("city", "city"),
        ],
    );
    // Register on every node via seeds so any leader can plan/put_record.
    client.register_table(schema).await.expect("register users");
    println!("phase0: registered table users(status, city)");

    let t0 = Instant::now();
    for start in (0..N_USERS).step_by(INSERT_BATCH as usize) {
        let end = (start + INSERT_BATCH).min(N_USERS);
        let sql = insert_sql(start, end);
        client.execute_sql(&sql).await.expect("sql insert");
    }
    println!(
        "phase1: inserted {N_USERS} users via SQL in {:.2?}",
        t0.elapsed()
    );

    // Give apply/stats a moment to settle on the leader.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let select_sql = "SELECT * FROM users WHERE status = 'active' AND city = 'Ankara'";
    println!("phase2: {select_sql}");

    let (rows, explain) = client.explain_sql(select_sql).await.expect("sql select");
    print!("{explain}");

    assert!(
        explain.contains("chosen: IndexScan(city)"),
        "CBO must drive on city index; got:\n{explain}"
    );
    assert!(
        !explain.contains("chosen: IndexScan(status)"),
        "CBO must not drive on status; got:\n{explain}"
    );
    assert!(
        !explain.contains("chosen: TableScan"),
        "CBO must not full-scan; got:\n{explain}"
    );

    println!("phase3: result rows={}", rows.len());
    assert_eq!(
        rows.len() as u64,
        ANKARA_COUNT,
        "expected {ANKARA_COUNT} Ankara users"
    );
    for r in &rows {
        assert_eq!(r.get("city"), Some("Ankara"));
        assert_eq!(r.get("status"), Some("active"));
    }

    for l in live.values() {
        for h in &l.handles {
            h.abort();
        }
        let _ = l.node.close();
    }
    let _ = std::fs::remove_dir_all(&root);

    println!("VERDICT: PASS — SQL parsed, CBO chose city index, returned {ANKARA_COUNT} rows");
}
