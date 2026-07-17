//! Step 13: Raft log compaction + InstallSnapshot catch-up (comatose follower).
//!
//! 1. Form a 3-node cluster with an aggressive snapshot threshold (5k).
//! 2. Kill Node 3 (comatose).
//! 3. Drive a heavy write storm on the remaining quorum until logs Node 3
//!    missed are compacted away.
//! 4. Resurrect Node 3; verify the leader pushes InstallSnapshot and all three
//!    nodes converge on identical applied state.
//!
//! Usage:
//!   cargo run --release --example raft_snapshot

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use takyonic::{Config, Key, Role, TakyonicNode, wait_for_leader};

fn node_config(root: &std::path::Path, id: u64) -> Config {
    Config::default()
        .data_dir(root.join(format!("node-{id}")).join("data"))
        .wal_dir(root.join(format!("node-{id}")).join("wal"))
        // Small memtable so flushes (and thus SST snapshots) happen often.
        .memtable_size_bytes(256 * 1024)
        .l0_soft_limit(32)
        .l0_hard_limit(64)
        .l0_rapid_pool_threads(1)
        .ln_haul_pool_threads(1)
        .compaction_write_bytes_per_sec(64 * 1024 * 1024)
        .write_admission_ops_per_sec(500_000)
        .write_admission_min_ops_per_sec(10_000)
        .write_admission_burst(50_000)
        .raft_snapshot_threshold(5_000)
}

struct LiveNode {
    node: Arc<TakyonicNode>,
    handles: Vec<tokio::task::JoinHandle<()>>,
}

#[tokio::main]
async fn main() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("takyonic=info")
        .try_init();

    let root = std::env::temp_dir().join(format!("takyonic-raft-snapshot-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();

    let mut endpoints = HashMap::new();
    endpoints.insert(1u64, "127.0.0.1:18001".into());
    endpoints.insert(2u64, "127.0.0.1:18002".into());
    endpoints.insert(3u64, "127.0.0.1:18003".into());

    println!("== Takyonic Raft snapshot / comatose-follower crucible ==");
    println!("snapshot threshold = 5000 entries");

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

    let initial: Vec<Arc<TakyonicNode>> = live.values().map(|l| Arc::clone(&l.node)).collect();
    let leader_id = wait_for_leader(&initial, Duration::from_secs(10))
        .await
        .expect("initial leader");
    println!("phase0: initial leader=node-{leader_id}");

    // --- Phase 1: put Node 3 into a coma ---
    println!("phase1: killing node-3 (comatose)");
    if let Some(dead) = live.remove(&3) {
        for h in dead.handles {
            h.abort();
        }
        drop(dead.node);
    }
    tokio::time::sleep(Duration::from_millis(200)).await;

    let survivors: Vec<Arc<TakyonicNode>> = live.values().map(|l| Arc::clone(&l.node)).collect();
    let leader_id = wait_for_leader(&survivors, Duration::from_secs(10))
        .await
        .expect("leader after coma");
    let leader = Arc::clone(&live.get(&leader_id).expect("leader live").node);
    println!("phase1: surviving leader=node-{leader_id}");

    // --- Phase 2: write storm until Raft log is compacted past what node-3 had ---
    let target_ops = 50_000u64;
    println!("phase2: write storm {target_ops} ops (force compaction past comatose tip)");

    let stop = Arc::new(AtomicBool::new(false));
    let ok = Arc::new(AtomicU64::new(0));
    let err = Arc::new(AtomicU64::new(0));
    let next = Arc::new(AtomicU64::new(0));
    let leader_slot = Arc::new(tokio::sync::RwLock::new(Some(Arc::clone(&leader))));

    let storm: Vec<_> = (0..16)
        .map(|_| {
            let stop = Arc::clone(&stop);
            let ok = Arc::clone(&ok);
            let err = Arc::clone(&err);
            let next = Arc::clone(&next);
            let leader_slot = Arc::clone(&leader_slot);
            tokio::spawn(async move {
                while !stop.load(Ordering::Relaxed) {
                    let Some(n) = leader_slot.read().await.clone() else {
                        tokio::time::sleep(Duration::from_millis(20)).await;
                        continue;
                    };
                    if n.role() != Role::Leader {
                        tokio::time::sleep(Duration::from_millis(20)).await;
                        continue;
                    }
                    let i = next.fetch_add(1, Ordering::Relaxed);
                    if i >= target_ops {
                        break;
                    }
                    let key = Key::new(format!("k{i:08}").into_bytes());
                    let val = format!("v{i}").into_bytes();
                    match tokio::time::timeout(Duration::from_millis(2000), n.put(key, val)).await {
                        Ok(Ok(_)) => {
                            ok.fetch_add(1, Ordering::Relaxed);
                        }
                        _ => {
                            err.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            })
        })
        .collect();

    // Wait until applied progresses far enough AND log has been compacted.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
    let mut compacted = false;
    while tokio::time::Instant::now() < deadline {
        let applied = leader.engine().last_applied();
        let snap = leader.raft().log().snapshot_meta().last_included_index;
        let log_len = leader.raft().log().len();
        if snap >= 5_000 && applied >= 20_000 {
            compacted = true;
            println!(
                "phase2: compaction observed — snapshot_index={snap} applied={applied} log_len={log_len}"
            );
            break;
        }
        if ok.load(Ordering::Relaxed) >= target_ops && snap >= 5_000 {
            compacted = true;
            println!(
                "phase2: target ops done — snapshot_index={snap} applied={applied} log_len={log_len}"
            );
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    stop.store(true, Ordering::Relaxed);
    for t in storm {
        let _ = tokio::time::timeout(Duration::from_secs(2), t).await;
    }
    let acked = ok.load(Ordering::Relaxed);
    println!(
        "phase2: storm done acked={acked} errors={} snapshot_index={}",
        err.load(Ordering::Relaxed),
        leader.raft().log().snapshot_meta().last_included_index
    );
    assert!(
        compacted,
        "expected Raft log compaction before resurrecting node-3"
    );
    let snap_before = leader.raft().log().snapshot_meta().last_included_index;
    assert!(
        snap_before >= 5_000,
        "snapshot index {snap_before} should be >= threshold"
    );

    println!("phase2: leader compacted through {snap_before}; node-3 must InstallSnapshot");

    // --- Phase 3: resurrect comatose follower (same data dir — stale logs) ---
    println!("phase3: resurrecting node-3");
    let phoenix = Arc::new(
        TakyonicNode::open(
            3,
            root.join("node-3"),
            endpoints.clone(),
            node_config(&root, 3),
        )
        .expect("resurrect open"),
    );
    let (s, t) = phoenix.start_background();
    live.insert(
        3,
        LiveNode {
            node: Arc::clone(&phoenix),
            handles: vec![s, t],
        },
    );

    // Wait for InstallSnapshot catch-up: phoenix applied reaches leader applied.
    let catchup_deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    let mut caught_up = false;
    let mut last_report = 0u64;
    while tokio::time::Instant::now() < catchup_deadline {
        let leader_applied = live
            .values()
            .filter(|l| l.node.role() == Role::Leader)
            .map(|l| l.node.engine().last_applied())
            .max()
            .unwrap_or(0);
        let phoenix_applied = phoenix.engine().last_applied();
        let phoenix_snap = phoenix.raft().log().snapshot_meta().last_included_index;
        if phoenix_applied != last_report {
            println!(
                "phase3: phoenix applied={phoenix_applied} snap={phoenix_snap} leader_applied={leader_applied} role={:?}",
                phoenix.role()
            );
            last_report = phoenix_applied;
        }
        if phoenix_snap > 0
            && phoenix_applied + 200 >= leader_applied
            && phoenix.role() == Role::Follower
        {
            caught_up = true;
            break;
        }
        // Keep the leader busy so replication ticks fire.
        if let Some(l) = live.values().find(|n| n.node.role() == Role::Leader) {
            let i = next.fetch_add(1, Ordering::Relaxed);
            let _ = tokio::time::timeout(
                Duration::from_millis(500),
                l.node.put(
                    Key::new(format!("post{i:08}").into_bytes()),
                    b"x".as_slice(),
                ),
            )
            .await;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    assert!(caught_up, "node-3 failed to catch up via InstallSnapshot");

    // Settle: wait for exact applied match.
    let settle_deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while tokio::time::Instant::now() < settle_deadline {
        let apps: Vec<_> = live
            .values()
            .map(|l| (l.node.id(), l.node.engine().last_applied()))
            .collect();
        let min = apps.iter().map(|(_, a)| *a).min().unwrap_or(0);
        let max = apps.iter().map(|(_, a)| *a).max().unwrap_or(0);
        if min == max && max > 0 {
            println!("phase3: converged applied={max} nodes={apps:?}");
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let leader_now = live
        .values()
        .find(|l| l.node.role() == Role::Leader)
        .map(|l| Arc::clone(&l.node))
        .expect("leader");
    let target_applied = leader_now.engine().last_applied();

    // Spot-check keys across all nodes.
    let mut mismatches = 0u64;
    let check_n = 200u64;
    let step = (acked / check_n).max(1);
    for i in (0..acked).step_by(step as usize).take(check_n as usize) {
        let key = Key::new(format!("k{i:08}").into_bytes());
        let expected = format!("v{i}");
        for n in live.values() {
            match n.node.get(&key) {
                Ok(Some(v)) if v.as_bytes() == expected.as_bytes() => {}
                _ => {
                    mismatches += 1;
                }
            }
        }
    }
    let _ = target_applied;

    let leaders: Vec<_> = live
        .values()
        .filter(|l| l.node.role() == Role::Leader)
        .map(|l| l.node.id())
        .collect();
    let states: Vec<_> = live
        .values()
        .map(|l| {
            (
                l.node.id(),
                l.node.engine().last_applied(),
                l.node.raft().log().snapshot_meta().last_included_index,
                l.node.role(),
            )
        })
        .collect();

    println!("== VERDICT ==");
    println!("compaction         : PASS (snapshot_index={snap_before})");
    println!(
        "InstallSnapshot    : {}",
        if caught_up {
            "PASS (comatose node-3 revived)"
        } else {
            "FAIL"
        }
    );
    println!("state convergence  : {states:?}");
    println!(
        "spot-check         : {} (mismatches={mismatches})",
        if mismatches == 0 { "PASS" } else { "FAIL" }
    );
    println!(
        "split-brain        : {}",
        if leaders.len() == 1 {
            format!("PASS (leader={})", leaders[0])
        } else {
            format!("FAIL (leaders={leaders:?})")
        }
    );

    assert!(caught_up);
    assert_eq!(mismatches, 0, "key mismatches after snapshot catch-up");
    assert_eq!(leaders.len(), 1);

    for (_, ln) in live.drain() {
        for h in ln.handles {
            h.abort();
        }
        let _ = ln.node.close();
    }
    let _ = std::fs::remove_dir_all(&root);
    println!("DONE");
}
