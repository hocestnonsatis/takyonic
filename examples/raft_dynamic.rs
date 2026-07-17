//! Step 14: Dynamic Raft membership (single-server Add/Remove).
//!
//! Phase 1 — Expansion: 3-node cluster under write storm → AddNode(4) →
//!   Node 4 receives InstallSnapshot, quorum becomes 3/4.
//! Phase 2 — Eviction: kill Node 1 → RemoveNode(1) → quorum shrinks to 2/3
//!   among survivors {2,3,4}; write storm completes with consistent state.
//!
//! Usage:
//!   cargo run --release --example raft_dynamic

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use takyonic::{Config, Key, Role, TakyonicNode, wait_for_leader};

fn node_config(root: &std::path::Path, id: u64) -> Config {
    Config::default()
        .data_dir(root.join(format!("node-{id}")).join("data"))
        .wal_dir(root.join(format!("node-{id}")).join("wal"))
        .memtable_size_bytes(512 * 1024)
        .l0_soft_limit(16)
        .l0_hard_limit(48)
        .l0_rapid_pool_threads(1)
        .ln_haul_pool_threads(1)
        .compaction_write_bytes_per_sec(32 * 1024 * 1024)
        .write_admission_ops_per_sec(500_000)
        .write_admission_min_ops_per_sec(10_000)
        .write_admission_burst(50_000)
        .raft_snapshot_threshold(2_000)
}

struct LiveNode {
    node: Arc<TakyonicNode>,
    handles: Vec<tokio::task::JoinHandle<()>>,
}

fn membership_ids(node: &TakyonicNode) -> Vec<u64> {
    node.membership().members().collect()
}

#[tokio::main]
async fn main() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("takyonic=info")
        .try_init();

    let root = std::env::temp_dir().join(format!("takyonic-raft-dynamic-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();

    let mut endpoints = HashMap::new();
    endpoints.insert(1u64, "127.0.0.1:19001".into());
    endpoints.insert(2u64, "127.0.0.1:19002".into());
    endpoints.insert(3u64, "127.0.0.1:19003".into());
    let node4_addr = "127.0.0.1:19004";

    println!("== Takyonic Raft dynamic membership crucible ==");

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
    println!(
        "phase0: leader=node-{leader_id} members={:?}",
        membership_ids(live.get(&leader_id).unwrap().node.as_ref())
    );

    let target_ops = 8_000u64;
    let stop = Arc::new(AtomicBool::new(false));
    let ok = Arc::new(AtomicU64::new(0));
    let err = Arc::new(AtomicU64::new(0));
    let next = Arc::new(AtomicU64::new(0));
    let leader_slot = Arc::new(tokio::sync::RwLock::new(
        live.get(&leader_id).map(|l| Arc::clone(&l.node)),
    ));

    let storm: Vec<_> = (0..12)
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
                    if n.role() != Role::Leader || n.raft().is_removed() {
                        tokio::time::sleep(Duration::from_millis(20)).await;
                        continue;
                    }
                    let i = next.fetch_add(1, Ordering::Relaxed);
                    if i >= target_ops {
                        break;
                    }
                    let key = Key::new(format!("k{i:08}").into_bytes());
                    let val = format!("v{i}").into_bytes();
                    match tokio::time::timeout(Duration::from_millis(3000), n.put(key, val)).await {
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

    // Warm up so AddNode triggers a snapshot for the joiner.
    while ok.load(Ordering::Relaxed) < 500 {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    println!(
        "phase1: warm writes ok={} — proposing AddNode(4)",
        ok.load(Ordering::Relaxed)
    );

    // Boot joining node 4 (empty membership — awaits snapshot / AddNode).
    {
        let node4 = Arc::new(
            TakyonicNode::open_joining(4, root.join("node-4"), node4_addr, node_config(&root, 4))
                .expect("open joining node-4"),
        );
        let (s, t) = node4.start_background();
        live.insert(
            4,
            LiveNode {
                node: node4,
                handles: vec![s, t],
            },
        );
    }
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Propose AddNode on current leader.
    let add_idx = {
        let guard = leader_slot.read().await;
        let leader = guard.as_ref().expect("leader");
        tokio::time::timeout(Duration::from_secs(15), leader.add_node(4, node4_addr))
            .await
            .expect("add_node timeout")
            .expect("add_node")
    };
    println!("phase1: AddNode committed at index={add_idx}");

    // Wait until all 4 nodes share membership {1,2,3,4} and quorum 3.
    let expand_deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let nodes: Vec<_> = live.values().map(|l| Arc::clone(&l.node)).collect();
        let all_four = nodes.iter().all(|n| {
            let m = n.membership();
            m.len() == 4 && m.contains(4) && m.quorum() == 3
        });
        let applied = nodes
            .iter()
            .map(|n| n.engine().last_applied())
            .collect::<Vec<_>>();
        if all_four {
            let min_a = *applied.iter().min().unwrap_or(&0);
            let max_a = *applied.iter().max().unwrap_or(&0);
            if max_a.saturating_sub(min_a) <= 64 {
                println!(
                    "phase1: expanded members={:?} quorum=3 applied={applied:?}",
                    membership_ids(nodes[0].as_ref())
                );
                break;
            }
        }
        // Refresh leader slot if leadership moved.
        if let Ok(lid) = wait_for_leader(&nodes, Duration::from_millis(200)).await {
            *leader_slot.write().await = live.get(&lid).map(|l| Arc::clone(&l.node));
        }
        if tokio::time::Instant::now() >= expand_deadline {
            panic!(
                "phase1 timeout waiting for 4-node convergence; applied={applied:?} members={:?}",
                nodes
                    .iter()
                    .map(|n| (n.id(), membership_ids(n)))
                    .collect::<Vec<_>>()
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // --- Phase 2: kill node 1, remove it ---
    println!("phase2: killing node-1");
    if let Some(dead) = live.remove(&1) {
        for h in dead.handles {
            h.abort();
        }
        drop(dead.node);
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Cluster is 3 live + 1 dead with quorum 3 — may struggle. Remove node 1.
    let survivors: Vec<Arc<TakyonicNode>> = live.values().map(|l| Arc::clone(&l.node)).collect();
    // Find a leader among survivors (may take a moment if elections struggle).
    let mut remove_leader = None;
    let evict_deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while tokio::time::Instant::now() < evict_deadline {
        if let Ok(lid) = wait_for_leader(&survivors, Duration::from_millis(500)).await {
            remove_leader = Some(lid);
            break;
        }
        // Nudge: if someone thinks they are leader, try them.
        for n in &survivors {
            if n.role() == Role::Leader {
                remove_leader = Some(n.id());
                break;
            }
        }
        if remove_leader.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let remove_leader = remove_leader.expect("no leader to propose RemoveNode");
    *leader_slot.write().await = live.get(&remove_leader).map(|l| Arc::clone(&l.node));
    println!("phase2: proposing RemoveNode(1) via leader=node-{remove_leader}");

    let rem_idx = {
        let guard = leader_slot.read().await;
        let leader = guard.as_ref().expect("leader");
        // Retry a few times — under quorum pressure the first propose may fail.
        let mut last_err = None;
        let mut idx = None;
        for attempt in 0..20 {
            match tokio::time::timeout(Duration::from_secs(5), leader.remove_node(1)).await {
                Ok(Ok(i)) => {
                    idx = Some(i);
                    break;
                }
                Ok(Err(e)) => {
                    last_err = Some(e.to_string());
                    // Re-resolve leader.
                    let nodes: Vec<_> = live.values().map(|l| Arc::clone(&l.node)).collect();
                    if let Ok(lid) = wait_for_leader(&nodes, Duration::from_secs(3)).await {
                        *leader_slot.write().await = live.get(&lid).map(|l| Arc::clone(&l.node));
                    }
                    eprintln!("  remove_node attempt {attempt} failed: {last_err:?}");
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
                Err(_) => {
                    last_err = Some("timeout".into());
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        }
        idx.unwrap_or_else(|| panic!("RemoveNode failed: {last_err:?}"))
    };
    println!("phase2: RemoveNode committed at index={rem_idx}");

    // Wait for survivors {2,3,4} with quorum 2.
    let shrink_deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let nodes: Vec<_> = live.values().map(|l| Arc::clone(&l.node)).collect();
        let ok_topo = nodes.iter().all(|n| {
            let m = n.membership();
            m.len() == 3 && !m.contains(1) && m.contains(4) && m.quorum() == 2
        });
        if ok_topo {
            println!(
                "phase2: shrunk members={:?} quorum=2",
                membership_ids(nodes[0].as_ref())
            );
            break;
        }
        if tokio::time::Instant::now() >= shrink_deadline {
            panic!(
                "phase2 timeout; members={:?}",
                nodes
                    .iter()
                    .map(|n| (n.id(), membership_ids(n)))
                    .collect::<Vec<_>>()
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Refresh leader and let the write storm finish.
    {
        let nodes: Vec<_> = live.values().map(|l| Arc::clone(&l.node)).collect();
        let lid = wait_for_leader(&nodes, Duration::from_secs(10))
            .await
            .expect("leader after shrink");
        *leader_slot.write().await = live.get(&lid).map(|l| Arc::clone(&l.node));
        println!("phase2: post-eviction leader=node-{lid}");
    }

    // Wait for storm to complete.
    let storm_deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    while next.load(Ordering::Relaxed) < target_ops && tokio::time::Instant::now() < storm_deadline
    {
        let nodes: Vec<_> = live.values().map(|l| Arc::clone(&l.node)).collect();
        if let Ok(lid) = wait_for_leader(&nodes, Duration::from_millis(200)).await {
            *leader_slot.write().await = live.get(&lid).map(|l| Arc::clone(&l.node));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    stop.store(true, Ordering::Relaxed);
    for h in storm {
        let _ = h.await;
    }

    let acked = ok.load(Ordering::Relaxed);
    let failed = err.load(Ordering::Relaxed);
    println!("phase3: write storm done acked={acked} failed={failed} target={target_ops}");

    // Convergence among {2,3,4}.
    let nodes: Vec<_> = live.values().map(|l| Arc::clone(&l.node)).collect();
    let conv_deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let applied: Vec<_> = nodes.iter().map(|n| n.engine().last_applied()).collect();
        let min_a = *applied.iter().min().unwrap();
        let max_a = *applied.iter().max().unwrap();
        if max_a.saturating_sub(min_a) <= 32 {
            println!("phase3: applied converged {applied:?}");
            break;
        }
        if tokio::time::Instant::now() >= conv_deadline {
            panic!("applied divergence: {applied:?}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Spot-check keys on all surviving nodes.
    let mut mismatches = 0u64;
    let sample: Vec<u64> = (0..acked).step_by((acked / 200).max(1) as usize).collect();
    for i in sample {
        let key = Key::new(format!("k{i:08}").into_bytes());
        let expect = format!("v{i}").into_bytes();
        for n in &nodes {
            match n.get(&key) {
                Ok(Some(v)) if v.as_bytes() == expect.as_slice() => {}
                Ok(Some(v)) => {
                    mismatches += 1;
                    eprintln!(
                        "mismatch node-{} key k{i:08}: got {:?}",
                        n.id(),
                        String::from_utf8_lossy(v.as_bytes())
                    );
                }
                Ok(None) => {
                    mismatches += 1;
                    eprintln!("missing node-{} key k{i:08}", n.id());
                }
                Err(e) => {
                    mismatches += 1;
                    eprintln!("read err node-{}: {e}", n.id());
                }
            }
        }
    }

    // Split-brain: exactly one leader among survivors.
    let leaders: Vec<_> = nodes
        .iter()
        .filter(|n| n.role() == Role::Leader)
        .map(|n| n.id())
        .collect();
    println!(
        "phase3: leaders={leaders:?} members={:?} mismatches={mismatches}",
        membership_ids(nodes[0].as_ref())
    );

    for (_, l) in live.drain() {
        for h in l.handles {
            h.abort();
        }
        let _ = l.node.close();
    }
    let _ = std::fs::remove_dir_all(&root);

    assert_eq!(leaders.len(), 1, "split-brain");
    assert_eq!(mismatches, 0, "key mismatches");
    assert!(
        acked >= target_ops * 8 / 10,
        "too many lost acks: acked={acked} target={target_ops}"
    );
    println!("VERDICT: PASS — dynamic topology mutation survived");
}
