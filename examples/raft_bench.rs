//! Step 12 Phase 1: concurrent write-storm throughput after network batching.
//!
//! Spins a 3-node cluster, elects a leader, then hammers `put` from N concurrent
//! async tasks. Reports ops/sec vs the Step 11 sequential baseline (~300 ops/s).
//!
//! Usage:
//!   cargo run --release --example raft_bench -- [writers] [total_ops]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use takyonic::{Config, TakyonicNode, wait_for_leader};

fn node_config(root: &std::path::Path, id: u64) -> Config {
    Config::default()
        .data_dir(root.join(format!("node-{id}")).join("data"))
        .wal_dir(root.join(format!("node-{id}")).join("wal"))
        .memtable_size_bytes(32 * 1024 * 1024)
        .l0_soft_limit(32)
        .l0_hard_limit(64)
        .l0_rapid_pool_threads(1)
        .ln_haul_pool_threads(1)
        .compaction_write_bytes_per_sec(64 * 1024 * 1024)
        .write_admission_ops_per_sec(1_000_000)
        .write_admission_min_ops_per_sec(50_000)
        .write_admission_burst(100_000)
}

fn arg<T: std::str::FromStr>(n: usize, default: T) -> T {
    std::env::args()
        .nth(n)
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

#[tokio::main]
async fn main() {
    let writers: usize = arg(1, 32);
    let total_ops: u64 = arg(2, 5_000);

    let root = std::env::temp_dir().join(format!("takyonic-raft-bench-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();

    let mut endpoints = HashMap::new();
    // High ports avoid clashes with raft_cluster defaults.
    endpoints.insert(1u64, "127.0.0.1:16001".into());
    endpoints.insert(2u64, "127.0.0.1:16002".into());
    endpoints.insert(3u64, "127.0.0.1:16003".into());

    println!("== Takyonic Raft network-batch bench ==");
    println!("writers={writers} total_ops={total_ops}");
    println!("baseline (Step 11 sequential): ~300 ops/s");

    let mut nodes = Vec::new();
    let mut handles = Vec::new();
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
        handles.push(s);
        handles.push(t);
        nodes.push(node);
    }
    tokio::time::sleep(Duration::from_millis(200)).await;

    let leader_id = wait_for_leader(&nodes, Duration::from_secs(10))
        .await
        .expect("leader");
    let leader = nodes.iter().find(|n| n.id() == leader_id).unwrap().clone();
    println!("leader=node-{leader_id} term={}", leader.raft().term());

    let next = Arc::new(AtomicU64::new(0));
    let ok = Arc::new(AtomicU64::new(0));
    let err = Arc::new(AtomicU64::new(0));
    let started = Instant::now();

    let mut tasks = Vec::new();
    for w in 0..writers {
        let leader = Arc::clone(&leader);
        let next = Arc::clone(&next);
        let ok = Arc::clone(&ok);
        let err = Arc::clone(&err);
        tasks.push(tokio::spawn(async move {
            loop {
                let i = next.fetch_add(1, Ordering::Relaxed);
                if i >= total_ops {
                    break;
                }
                let key = format!("w{w}-k{i}");
                let val = format!("v{i}");
                match leader.put(key.into_bytes(), val.into_bytes()).await {
                    Ok(_) => {
                        ok.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => {
                        err.fetch_add(1, Ordering::Relaxed);
                        if err.load(Ordering::Relaxed) < 5 {
                            eprintln!("writer {w}: {e}");
                        }
                    }
                }
            }
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    let elapsed = started.elapsed();
    let applied = ok.load(Ordering::Relaxed);
    let errors = err.load(Ordering::Relaxed);
    let ops = applied as f64 / elapsed.as_secs_f64();

    // Catch-up check.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let min_applied = nodes
            .iter()
            .map(|n| n.engine().last_applied())
            .min()
            .unwrap_or(0);
        if min_applied >= applied {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            eprintln!("warn: followers lagging, min_applied={min_applied} leader_ok={applied}");
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    println!("== SUMMARY ==");
    println!("wall time         : {:.2}s", elapsed.as_secs_f64());
    println!("puts applied      : {applied}");
    println!("errors            : {errors}");
    println!("sustained ops/sec : {ops:.0}");
    println!(
        "vs Step 11        : {:.1}x (300 ops/s baseline)",
        ops / 300.0
    );
    for n in &nodes {
        println!(
            "node-{}           : applied={} commit={}",
            n.id(),
            n.engine().last_applied(),
            n.raft().commit_index()
        );
    }
    let verdict = errors == 0 && ops > 300.0;
    println!(
        "verdict           : {}",
        if verdict {
            "SHATTERED the network RTT ceiling"
        } else if errors == 0 {
            "COMPLETED (did not beat 300 ops/s)"
        } else {
            "FAILED"
        }
    );

    for h in &handles {
        h.abort();
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    for n in &nodes {
        let _ = n.close();
    }
    let _ = std::fs::remove_dir_all(&root);
    if errors > 0 {
        std::process::exit(1);
    }
}
