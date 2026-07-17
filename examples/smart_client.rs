//! Step 17: Smart Client SDK chaos crucible.
//!
//! Application code contains ZERO retry / try-catch logic for network errors or
//! OCC aborts. The SDK's `execute_txn` absorbs leader failover and conflicts.
//!
//! 1. Spin up a 3-node cluster.
//! 2. Seed a bank via TakyonicClient.
//! 3. Run high-contention transfers through `execute_txn`.
//! 4. Assassinate the Raft leader mid-flight.
//! 5. Invariant: final bank sum == starting sum; transfers eventually Ok(()).
//!
//! Usage:
//!   cargo run --release --example smart_client

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use takyonic::{Config, Key, Role, TakyonicClient, TakyonicNode, Value, wait_for_leader};

const ACCOUNTS: u64 = 50;
const INITIAL: i64 = 1000;
const TARGET_SUM: i64 = ACCOUNTS as i64 * INITIAL;
const WORKERS: usize = 8;
/// Post-failover transfers each worker must complete after the new leader is up.
const TRANSFERS_AFTER_FAILOVER: u64 = 40;

fn node_config(root: &std::path::Path, id: u64) -> Config {
    Config::default()
        .data_dir(root.join(format!("node-{id}")).join("data"))
        .wal_dir(root.join(format!("node-{id}")).join("wal"))
        .memtable_size_bytes(4 * 1024 * 1024)
        .l0_soft_limit(16)
        .l0_hard_limit(48)
        .l0_rapid_pool_threads(1)
        .ln_haul_pool_threads(1)
        .compaction_write_bytes_per_sec(32 * 1024 * 1024)
        .write_admission_ops_per_sec(500_000)
        .write_admission_min_ops_per_sec(10_000)
        .write_admission_burst(50_000)
}

fn account_key(id: u64) -> Key {
    Key::new(format!("acct-{id:03}").into_bytes())
}

fn encode_balance(n: i64) -> Value {
    Value::new(n.to_le_bytes().to_vec())
}

fn decode_balance(v: &Value) -> i64 {
    let b = v.as_bytes();
    assert_eq!(b.len(), 8, "balance encoding");
    i64::from_le_bytes(b.try_into().unwrap())
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

    let root = std::env::temp_dir().join(format!("takyonic-smart-client-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();

    let mut endpoints = HashMap::new();
    endpoints.insert(1u64, "127.0.0.1:18001".into());
    endpoints.insert(2u64, "127.0.0.1:18002".into());
    endpoints.insert(3u64, "127.0.0.1:18003".into());
    let seeds = vec![
        "127.0.0.1:18001".to_string(),
        "127.0.0.1:18002".to_string(),
        "127.0.0.1:18003".to_string(),
    ];

    println!("== Takyonic Smart Client chaos crucible ==");
    println!("accounts={ACCOUNTS} initial={INITIAL} invariant={TARGET_SUM}");

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
    let old_leader_id = wait_for_leader(&initial, Duration::from_secs(10))
        .await
        .expect("initial leader");
    println!("phase0: initial leader=node-{old_leader_id}");

    let client = TakyonicClient::new(seeds.clone());
    client.connect().await.expect("client connect");

    // Seed accounts — no application-level retries.
    for id in 0..ACCOUNTS {
        client
            .execute_txn(|txn| {
                let key = account_key(id);
                async move {
                    txn.put(key, encode_balance(INITIAL)).await?;
                    Ok(())
                }
            })
            .await
            .expect("seed");
    }

    let mut seeded = 0i64;
    for id in 0..ACCOUNTS {
        seeded += client
            .get(account_key(id))
            .await
            .expect("seed get")
            .map(|v| decode_balance(&v))
            .unwrap_or(0);
    }
    assert_eq!(seeded, TARGET_SUM, "seed sum");
    println!("phase0: seeded sum={seeded}");

    let stop = Arc::new(AtomicBool::new(false));
    let failover_done = Arc::new(AtomicBool::new(false));
    let ok = Arc::new(AtomicU64::new(0));
    let fail = Arc::new(AtomicU64::new(0));
    let post_failover_ok = Arc::new(AtomicU64::new(0));

    let mut workers = Vec::new();
    for w in 0..WORKERS {
        let client = TakyonicClient::new(seeds.clone());
        client.connect().await.expect("worker connect");
        let stop = Arc::clone(&stop);
        let failover_done = Arc::clone(&failover_done);
        let ok = Arc::clone(&ok);
        let fail = Arc::clone(&fail);
        let post_failover_ok = Arc::clone(&post_failover_ok);
        workers.push(tokio::spawn(async move {
            let mut rng = (w as u64).wrapping_add(1).wrapping_mul(0x9E37);
            let mut after = 0u64;
            while !stop.load(Ordering::Relaxed) {
                // After failover, each worker completes a quota then exits.
                if failover_done.load(Ordering::Relaxed) && after >= TRANSFERS_AFTER_FAILOVER {
                    break;
                }
                rng ^= rng << 13;
                rng ^= rng >> 7;
                rng ^= rng << 17;
                let from = rng % ACCOUNTS;
                let to = (rng / ACCOUNTS) % ACCOUNTS;
                if from == to {
                    continue;
                }
                let amount = 1 + (rng % 40) as i64;

                // ZERO try/catch or retry logic — the SDK shields chaos.
                match client
                    .execute_txn(|txn| {
                        let from_key = account_key(from);
                        let to_key = account_key(to);
                        async move {
                            let from_bal = txn
                                .get(from_key.clone())
                                .await?
                                .map(|v| decode_balance(&v))
                                .unwrap_or(0);
                            let to_bal = txn
                                .get(to_key.clone())
                                .await?
                                .map(|v| decode_balance(&v))
                                .unwrap_or(0);
                            if from_bal < amount {
                                return Ok(());
                            }
                            txn.put(from_key, encode_balance(from_bal - amount)).await?;
                            txn.put(to_key, encode_balance(to_bal + amount)).await?;
                            Ok(())
                        }
                    })
                    .await
                {
                    Ok(()) => {
                        ok.fetch_add(1, Ordering::Relaxed);
                        if failover_done.load(Ordering::Relaxed) {
                            after += 1;
                            post_failover_ok.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    Err(e) => {
                        fail.fetch_add(1, Ordering::Relaxed);
                        eprintln!("worker {w} unexpected error: {e}");
                        break;
                    }
                }
            }
        }));
    }

    // Warm the transfer storm, then assassinate the leader while workers are live.
    let warm_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while ok.load(Ordering::Relaxed) < 80 && tokio::time::Instant::now() < warm_deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let pre_kill = ok.load(Ordering::Relaxed);
    println!("phase1: warm transfers committed={pre_kill} — assassinating leader");
    assert!(pre_kill > 0, "storm never started");

    {
        let mut victim = live.remove(&old_leader_id).expect("victim");
        for h in victim.handles.drain(..) {
            h.abort();
        }
        drop(victim.node);
    }

    let survivors: Vec<Arc<TakyonicNode>> = live.values().map(|l| Arc::clone(&l.node)).collect();
    let new_leader_id = wait_for_leader(&survivors, Duration::from_secs(20))
        .await
        .expect("failover leader");
    assert_ne!(new_leader_id, old_leader_id);
    println!(
        "phase2: NEW leader=node-{new_leader_id} term={}",
        survivors
            .iter()
            .find(|n| n.id() == new_leader_id)
            .unwrap()
            .raft()
            .term()
    );
    failover_done.store(true, Ordering::Relaxed);

    // Workers must continue through failover via SDK redirects/retries.
    for h in workers {
        h.await.expect("worker join");
    }
    stop.store(true, Ordering::Relaxed);

    let committed = ok.load(Ordering::Relaxed);
    let failed = fail.load(Ordering::Relaxed);
    let after = post_failover_ok.load(Ordering::Relaxed);
    println!("phase3: transfers ok={committed} post_failover={after} unexpected_err={failed}");
    assert!(
        after > 0,
        "no transfers committed after failover — SDK did not recover"
    );

    // Settle apply on survivors, then sum via a fresh client (auto-discovers leader).
    tokio::time::sleep(Duration::from_millis(400)).await;
    let client = TakyonicClient::new(seeds);
    client.connect().await.expect("final connect");

    let mut final_sum = 0i64;
    for id in 0..ACCOUNTS {
        final_sum += client
            .get(account_key(id))
            .await
            .expect("final get")
            .map(|v| decode_balance(&v))
            .unwrap_or(0);
    }

    // Split-brain: exactly one leader among survivors.
    let leaders: Vec<u64> = survivors
        .iter()
        .filter(|n| n.role() == Role::Leader)
        .map(|n| n.id())
        .collect();
    assert_eq!(leaders.len(), 1, "split-brain: {leaders:?}");

    println!("phase4: final sum={final_sum} (expected {TARGET_SUM})");

    for l in live.values() {
        let _ = l.node.close();
    }
    let _ = std::fs::remove_dir_all(&root);

    assert_eq!(failed, 0, "application saw unexpected errors");
    assert_eq!(
        final_sum, TARGET_SUM,
        "bank invariant violated: sum={final_sum}"
    );
    assert!(committed > 0, "no transfers committed");
    println!("VERDICT: PASS — Smart Client shielded OCC + leader assassination (ok={committed})");
}
