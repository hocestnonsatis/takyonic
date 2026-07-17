//! Step 12 Phase 2: leader assassination + failover + resurrection.
//!
//! 1. Form a 3-node cluster and elect a leader.
//! 2. Begin a concurrent write storm.
//! 3. Abruptly kill the leader (abort tasks + drop node — process death).
//! 4. Verify remaining nodes elect a new leader and resume writes.
//! 5. Resurrect the old leader on the same directory; verify it rejoins as a
//!    follower, truncates divergent uncommitted suffix if any, and matches state.
//!
//! Usage:
//!   cargo run --release --example raft_chaos

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use takyonic::{Config, Key, Role, TakyonicNode, wait_for_leader};

fn node_config(root: &std::path::Path, id: u64) -> Config {
    Config::default()
        .data_dir(root.join(format!("node-{id}")).join("data"))
        .wal_dir(root.join(format!("node-{id}")).join("wal"))
        .memtable_size_bytes(8 * 1024 * 1024)
        .l0_soft_limit(16)
        .l0_hard_limit(48)
        .l0_rapid_pool_threads(1)
        .ln_haul_pool_threads(1)
        .compaction_write_bytes_per_sec(32 * 1024 * 1024)
        .write_admission_ops_per_sec(500_000)
        .write_admission_min_ops_per_sec(10_000)
        .write_admission_burst(50_000)
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

    let root = std::env::temp_dir().join(format!("takyonic-raft-chaos-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();

    let mut endpoints = HashMap::new();
    endpoints.insert(1u64, "127.0.0.1:17001".into());
    endpoints.insert(2u64, "127.0.0.1:17002".into());
    endpoints.insert(3u64, "127.0.0.1:17003".into());

    println!("== Takyonic Raft leader-assassination crucible ==");

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
    tokio::time::sleep(Duration::from_millis(250)).await;

    let snap = || -> Vec<Arc<TakyonicNode>> {
        // Re-read each time from a temporary list — caller rebuilds after mutations.
        Vec::new()
    };
    let _ = snap;

    let initial: Vec<Arc<TakyonicNode>> = live.values().map(|l| Arc::clone(&l.node)).collect();
    let old_leader_id = wait_for_leader(&initial, Duration::from_secs(10))
        .await
        .expect("initial leader");
    println!("phase0: initial leader=node-{old_leader_id}");

    let stop_storm = Arc::new(AtomicBool::new(false));
    let storm_ok = Arc::new(AtomicU64::new(0));
    let storm_err = Arc::new(AtomicU64::new(0));
    let next_key = Arc::new(AtomicU64::new(0));

    // Write storm against whoever is currently leader (re-resolves after failover).
    let storm_endpoints = endpoints.clone();
    let storm_root = root.clone();
    let storm_handle = {
        let stop = Arc::clone(&stop_storm);
        let ok = Arc::clone(&storm_ok);
        let err = Arc::clone(&storm_err);
        let next = Arc::clone(&next_key);
        // We need a shared view of live leaders — use a channel of Arc<TakyonicNode>.
        let leader_slot: Arc<tokio::sync::RwLock<Option<Arc<TakyonicNode>>>> = Arc::new(
            tokio::sync::RwLock::new(live.get(&old_leader_id).map(|l| Arc::clone(&l.node))),
        );
        let leader_slot_writer = Arc::clone(&leader_slot);

        // Spawn resolver that updates leader_slot from remaining live nodes.
        // The main task will update it after failover; storm just reads it.

        let storm_tasks: Vec<_> = (0..8)
            .map(|w| {
                let stop = Arc::clone(&stop);
                let ok = Arc::clone(&ok);
                let err = Arc::clone(&err);
                let next = Arc::clone(&next);
                let leader_slot = Arc::clone(&leader_slot);
                tokio::spawn(async move {
                    while !stop.load(Ordering::Relaxed) {
                        let leader = leader_slot.read().await.clone();
                        let Some(leader) = leader else {
                            tokio::time::sleep(Duration::from_millis(20)).await;
                            continue;
                        };
                        if leader.role() != Role::Leader {
                            tokio::time::sleep(Duration::from_millis(20)).await;
                            continue;
                        }
                        let i = next.fetch_add(1, Ordering::Relaxed);
                        let key = format!("chaos-{i}");
                        let val = format!("v-{i}-from-{w}");
                        match tokio::time::timeout(
                            Duration::from_millis(500),
                            leader.put(key.into_bytes(), val.into_bytes()),
                        )
                        .await
                        {
                            Ok(Ok(_)) => {
                                ok.fetch_add(1, Ordering::Relaxed);
                            }
                            Ok(Err(_)) | Err(_) => {
                                err.fetch_add(1, Ordering::Relaxed);
                                tokio::time::sleep(Duration::from_millis(10)).await;
                            }
                        }
                    }
                })
            })
            .collect();

        (storm_tasks, leader_slot_writer, storm_endpoints, storm_root)
    };
    let (storm_tasks, leader_slot, _, _) = storm_handle;

    // Let the storm warm up.
    tokio::time::sleep(Duration::from_millis(400)).await;
    let pre_kill = storm_ok.load(Ordering::Relaxed);
    println!("phase1: write storm warm — {pre_kill} acked puts before assassination");

    // --- ASSASSINATION ---
    println!("phase2: ASSASSINATING leader node-{old_leader_id}");
    {
        let mut victim = live.remove(&old_leader_id).expect("victim");
        for h in victim.handles.drain(..) {
            h.abort();
        }
        // Hard drop: do not call close() — simulate power loss / SIGKILL.
        drop(victim.node);
    }
    *leader_slot.write().await = None;

    // --- FAILOVER ---
    println!("phase3: waiting for failover election among survivors");
    let survivors: Vec<Arc<TakyonicNode>> = live.values().map(|l| Arc::clone(&l.node)).collect();
    assert_eq!(survivors.len(), 2);
    let new_leader_id = wait_for_leader(&survivors, Duration::from_secs(15))
        .await
        .expect("failover leader");
    assert_ne!(
        new_leader_id, old_leader_id,
        "new leader must differ from assassinated node"
    );
    let new_leader = survivors
        .iter()
        .find(|n| n.id() == new_leader_id)
        .unwrap()
        .clone();
    println!(
        "phase3: NEW leader=node-{new_leader_id} term={}",
        new_leader.raft().term()
    );
    *leader_slot.write().await = Some(Arc::clone(&new_leader));

    // Probe: wait until the new leader can actually commit (followers caught up).
    println!("phase3: probing commit path on new leader");
    let probe_deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    let mut probed = false;
    while tokio::time::Instant::now() < probe_deadline {
        match tokio::time::timeout(
            Duration::from_secs(2),
            new_leader.put(b"failover-probe".as_slice(), b"ok".as_slice()),
        )
        .await
        {
            Ok(Ok(_)) => {
                probed = true;
                break;
            }
            Ok(Err(e)) => {
                eprintln!("probe err: {e}");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(_) => {
                eprintln!("probe timeout — waiting for follower catch-up");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
    assert!(
        probed,
        "new leader could not commit a probe put after failover"
    );
    println!(
        "phase3: probe committed; commit_index={}",
        new_leader.raft().commit_index()
    );

    let mid = storm_ok.load(Ordering::Relaxed);
    tokio::time::sleep(Duration::from_millis(800)).await;
    let post_failover = storm_ok.load(Ordering::Relaxed);
    println!(
        "phase3: writes resumed — acked {mid} → {post_failover} (+{}) err={}",
        post_failover.saturating_sub(mid),
        storm_err.load(Ordering::Relaxed)
    );
    assert!(
        post_failover > mid,
        "cluster must accept writes after failover"
    );

    stop_storm.store(true, Ordering::Relaxed);
    for t in storm_tasks {
        t.abort();
        let _ = t.await;
    }
    let total_acked = storm_ok.load(Ordering::Relaxed);
    println!(
        "phase3: storm stopped — total_acked={total_acked} errors={}",
        storm_err.load(Ordering::Relaxed)
    );

    // Wait for survivors to apply through their commit index.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let survivor_applied: Vec<_> = survivors
        .iter()
        .map(|n| (n.id(), n.engine().last_applied(), n.raft().commit_index()))
        .collect();
    println!("phase3: survivor state {survivor_applied:?}");

    // --- RESURRECTION ---
    println!("phase4: resurrecting assassinated node-{old_leader_id}");
    let phoenix = Arc::new(
        TakyonicNode::open(
            old_leader_id,
            root.join(format!("node-{old_leader_id}")),
            endpoints.clone(),
            node_config(&root, old_leader_id),
        )
        .expect("resurrect open"),
    );
    let (s, t) = phoenix.start_background();
    live.insert(
        old_leader_id,
        LiveNode {
            node: Arc::clone(&phoenix),
            handles: vec![s, t],
        },
    );

    // Allow heartbeat / catch-up replication.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let target = new_leader.raft().commit_index();
    loop {
        let role = phoenix.role();
        let applied = phoenix.engine().last_applied();
        let commit = phoenix.raft().commit_index();
        if role == Role::Follower && applied >= target && commit >= target {
            println!(
                "phase4: resurrected node-{old_leader_id} is Follower applied={applied} commit={commit}"
            );
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!(
                "resurrection stalled: role={role:?} applied={applied} commit={commit} target={target}"
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    assert_eq!(
        phoenix.role(),
        Role::Follower,
        "old leader must rejoin as follower (no split-brain)"
    );
    assert_eq!(
        live.values()
            .filter(|l| l.node.role() == Role::Leader)
            .count(),
        1,
        "exactly one leader after resurrection"
    );

    // Spot-check: keys written after failover must be visible on the phoenix.
    let sample_lo = mid.max(1);
    let sample_hi = post_failover.saturating_sub(1).max(sample_lo);
    let mut checked = 0u64;
    let mut missing = 0u64;
    for i in (sample_lo..=sample_hi).step_by(7) {
        let key = Key::new(format!("chaos-{i}").into_bytes());
        checked += 1;
        if phoenix.get(&key).expect("get").is_none() {
            // May have been an in-flight error during failover; only count if
            // the new leader still has it.
            if new_leader.get(&key).expect("get").is_some() {
                missing += 1;
            }
        }
    }
    println!("phase4: spot-check {checked} post-failover keys on phoenix; missing={missing}");
    assert_eq!(missing, 0, "resurrected node missing committed keys");

    println!("== VERDICT ==");
    println!("assassination     : PASS (leader node-{old_leader_id} killed mid-storm)");
    println!("failover          : PASS (leader node-{new_leader_id}, writes resumed)");
    println!("resurrection      : PASS (old leader rejoined as Follower, state synced)");
    println!("split-brain       : PASS (exactly one leader)");

    for l in live.values() {
        for h in &l.handles {
            h.abort();
        }
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    for l in live.values() {
        let _ = l.node.close();
    }
    let _ = std::fs::remove_dir_all(&root);
}
