//! Step 11: 3-node Raft cluster on loopback.
//!
//! Spins up nodes on ports 15001–15003, waits for leader election, drives a
//! write storm through the leader, then verifies every node has applied the
//! same key/value state.
//!
//! Usage:
//!   cargo run --release --example raft_cluster

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use takyonic::{Config, Key, Role, TakyonicNode, wait_for_leader};

fn node_config(root: &std::path::Path, id: u64) -> Config {
    Config::default()
        .data_dir(root.join(format!("node-{id}")).join("data"))
        .wal_dir(root.join(format!("node-{id}")).join("wal"))
        .memtable_size_bytes(8 * 1024 * 1024)
        .block_size_bytes(4 * 1024)
        .l0_soft_limit(16)
        .l0_hard_limit(48)
        .l0_rapid_pool_threads(1)
        .ln_haul_pool_threads(1)
        .compaction_write_bytes_per_sec(32 * 1024 * 1024)
        .write_admission_ops_per_sec(200_000)
        .write_admission_min_ops_per_sec(10_000)
        .write_admission_burst(20_000)
}

#[tokio::main]
async fn main() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("takyonic=info")
        .try_init();

    let root = std::env::temp_dir().join(format!("takyonic-raft-cluster-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();

    let mut endpoints = HashMap::new();
    endpoints.insert(1u64, "127.0.0.1:15001".into());
    endpoints.insert(2u64, "127.0.0.1:15002".into());
    endpoints.insert(3u64, "127.0.0.1:15003".into());

    println!("== Takyonic 3-node Raft cluster ==");
    println!("dirs under {}", root.display());

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
            .expect("open node"),
        );
        let (server, ticker) = node.start_background();
        handles.push(server);
        handles.push(ticker);
        nodes.push(node);
    }

    // Give gRPC servers a moment to bind.
    tokio::time::sleep(Duration::from_millis(200)).await;

    println!("-- waiting for leader election --");
    let leader_id = wait_for_leader(&nodes, Duration::from_secs(10))
        .await
        .expect("leader elected");
    let leader = nodes
        .iter()
        .find(|n| n.id() == leader_id)
        .expect("leader node")
        .clone();
    println!(
        "leader=node-{leader_id} term={} role={:?}",
        leader.raft().term(),
        leader.role()
    );
    for n in &nodes {
        println!(
            "  node-{} role={:?} leader_hint={:?}",
            n.id(),
            n.role(),
            n.leader_id()
        );
    }

    let total = 500u64;
    println!("-- write storm: {total} puts via leader --");
    let started = std::time::Instant::now();
    for i in 0..total {
        let key = format!("k-{i:04}");
        let val = format!("v-{i}-term-{}", leader.raft().term());
        leader
            .put(key.into_bytes(), val.into_bytes())
            .await
            .unwrap_or_else(|e| panic!("put {i} failed: {e}"));
        if i > 0 && i % 100 == 0 {
            println!("  ... {i}/{total}");
        }
    }
    let elapsed = started.elapsed();
    println!(
        "wrote {total} entries in {:.2}s ({:.0} ops/s)",
        elapsed.as_secs_f64(),
        total as f64 / elapsed.as_secs_f64()
    );

    // Allow followers to catch commit_index / apply.
    println!("-- waiting for followers to apply --");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let applied: Vec<_> = nodes
            .iter()
            .map(|n| (n.id(), n.engine().last_applied(), n.raft().commit_index()))
            .collect();
        let all_caught = applied.iter().all(|(_, a, _)| *a >= total);
        if all_caught {
            println!("all nodes applied through {total}: {applied:?}");
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("followers did not catch up: {applied:?}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    println!("-- verifying identical state on all nodes --");
    let mut mismatches = 0u64;
    for i in 0..total {
        let key = Key::new(format!("k-{i:04}").into_bytes());
        let expected = format!("v-{i}-term-{}", leader.raft().term());
        for n in &nodes {
            let got = n.get(&key).expect("get").unwrap_or_else(|| {
                panic!(
                    "node-{} missing key {}",
                    n.id(),
                    key.as_bytes().escape_ascii()
                )
            });
            if got.as_bytes() != expected.as_bytes() {
                eprintln!(
                    "MISMATCH node-{} key={} got={:?} expected={}",
                    n.id(),
                    String::from_utf8_lossy(key.as_bytes()),
                    String::from_utf8_lossy(got.as_bytes()),
                    expected
                );
                mismatches += 1;
            }
        }
    }

    println!("== SUMMARY ==");
    println!("leader            : node-{leader_id}");
    for n in &nodes {
        println!(
            "node-{}            : role={:?} applied={} commit={} memtable={}",
            n.id(),
            n.role(),
            n.engine().last_applied(),
            n.raft().commit_index(),
            n.engine().memtable().len(),
        );
    }
    println!("mismatches        : {mismatches}");
    let leaders: Vec<_> = nodes.iter().filter(|n| n.role() == Role::Leader).collect();
    assert_eq!(leaders.len(), 1, "expected exactly one leader");
    assert_eq!(mismatches, 0, "state divergence across replicas");
    println!("verdict           : CLUSTER REPLICATED — 3/3 nodes agree");

    for h in &handles {
        h.abort();
    }
    // Allow aborted tasks to unwind before closing engines.
    tokio::time::sleep(Duration::from_millis(50)).await;
    for n in &nodes {
        let _ = n.close();
    }
    let _ = std::fs::remove_dir_all(&root);
}
