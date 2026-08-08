//! Step 16: Secondary indexes + cost-based optimizer crucible.
//!
//! Inserts 10_000 skewed User rows, then runs:
//!   status == "active" AND city == "X"
//!
//! The planner MUST choose the `city` index (high NDV / low est_rows) over
//! `status` (low NDV / ~9k-row scan), and return exactly the 50 city=X users.
//!
//! Usage:
//!   cargo run --release --example cbo_planner

use std::sync::Arc;
use std::time::Instant;

use takyonic::{Config, IndexDef, Record, TableSchema, TakyonicEngine};

const N_USERS: u64 = 10_000;
const CITY_X_COUNT: u64 = 50;
const ACTIVE_COUNT: u64 = 9_000;
/// Distinct cities so NDV(city) ≫ NDV(status) under the uniform CBO model.
const N_CITIES: u64 = 200;

fn city_for(id: u64) -> String {
    if id < CITY_X_COUNT {
        "X".into()
    } else {
        // Spread remaining users across cities C001..C199 (never "X").
        let slot = 1 + ((id - CITY_X_COUNT) % (N_CITIES - 1));
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

fn main() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("takyonic=warn")
        .try_init();

    let root = std::env::temp_dir().join(format!("takyonic-cbo-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();

    let engine = Arc::new(
        TakyonicEngine::open(
            Config::default()
                .data_dir(root.join("data"))
                .wal_dir(root.join("wal"))
                .memtable_size_bytes(4 * 1024 * 1024)
                .l0_soft_limit(32)
                .l0_hard_limit(64)
                .l0_rapid_pool_threads(1)
                .ln_haul_pool_threads(1)
                .compaction_write_bytes_per_sec(128 * 1024 * 1024)
                .write_admission_ops_per_sec(500_000)
                .write_admission_min_ops_per_sec(10_000)
                .write_admission_burst(50_000),
        )
        .expect("open engine"),
    );

    engine
        .register_table(TableSchema::new(
            "users",
            "id",
            vec![
                IndexDef::new("status", "status"),
                IndexDef::new("city", "city"),
            ],
        ))
        .expect("register users");

    println!("== Takyonic CBO planner crucible ==");
    println!(
        "inserting {N_USERS} users (active≈{ACTIVE_COUNT}, city=X={CITY_X_COUNT}, cities≈{N_CITIES})"
    );

    let t0 = Instant::now();
    // Batch inserts in moderate-sized txns to keep OCC/write-set manageable.
    const BATCH: u64 = 100;
    for start in (0..N_USERS).step_by(BATCH as usize) {
        let end = (start + BATCH).min(N_USERS);
        let mut txn = engine.begin().expect("begin");
        for id in start..end {
            let record = Record::new()
                .set("id", format!("{id}"))
                .set("status", status_for(id))
                .set("city", city_for(id))
                .set("age", format!("{}", 20 + (id % 40)));
            txn.put_record("users", record).expect("put_record");
        }
        txn.commit().expect("commit batch");
    }
    println!("insert done in {:.2?}", t0.elapsed());

    let stats = engine.table_stats("users");
    println!(
        "stats: row_count={} distinct_status={:?} distinct_city={:?}",
        stats.row_count,
        stats.distinct.get("status"),
        stats.distinct.get("city")
    );
    println!(
        "eq_cost(status)={} eq_cost(city)={}",
        stats.eq_cost("status"),
        stats.eq_cost("city")
    );

    let mut q = engine
        .query("users")
        .filter("status", "==", "active")
        .expect("filter status")
        .filter("city", "==", "X")
        .expect("filter city");

    let explain = q.explain().expect("explain");
    print!("{explain}");

    assert!(
        explain.contains("chosen: IndexScan(city)"),
        "CBO must drive on city index; got:\n{explain}"
    );
    assert!(
        !explain.contains("chosen: IndexScan(status)"),
        "CBO must not drive on status; got:\n{explain}"
    );

    let rows = q.execute().expect("execute");
    println!("result rows={}", rows.len());
    assert_eq!(
        rows.len() as u64,
        CITY_X_COUNT,
        "expected {CITY_X_COUNT} city=X users"
    );
    for r in &rows {
        assert_eq!(r.get("city"), Some("X"));
        // city=X users are ids 0..49, all within the active prefix.
        assert_eq!(r.get("status"), Some("active"));
    }

    engine.close().expect("close");
    let _ = std::fs::remove_dir_all(&root);

    println!("VERDICT: PASS — CBO chose city index, returned {CITY_X_COUNT} rows");
}
