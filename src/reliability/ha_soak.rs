//! Three-node HA failover soak helpers.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use crate::cluster::{TakyonicNode, wait_for_leader};
use crate::config::Config;
use crate::consensus::Role;
use crate::reliability::{ReliabilityReport, env_u64};
use crate::types::Key;

/// Configuration for [`run_ha_soak`].
pub struct HaSoakConfig {
    /// Total wall-clock duration.
    pub duration: Duration,
    /// Interval between leader kill attempts.
    pub kill_every: Duration,
    /// Soft target for distinct keys written during the storm.
    pub keys: usize,
}

impl HaSoakConfig {
    /// Build from `TAKYONIC_HA_SECS` (default 600) with a derived kill interval.
    pub fn from_env() -> Self {
        let secs = env_u64("TAKYONIC_HA_SECS", 600);
        let kill = (secs / 12).max(5);
        Self {
            duration: Duration::from_secs(secs),
            kill_every: Duration::from_secs(kill),
            keys: 10_000,
        }
    }
}

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

/// Run a 3-node leader-kill soak; returns a report (never panics on invariant fail).
pub fn run_ha_soak(cfg: HaSoakConfig) -> ReliabilityReport {
    let rt = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            let mut r = ReliabilityReport::new(0);
            r.fail(format!("tokio runtime: {e}"));
            return r;
        }
    };
    rt.block_on(run_ha_soak_async(cfg))
}

async fn run_ha_soak_async(cfg: HaSoakConfig) -> ReliabilityReport {
    let mut report = ReliabilityReport::new(cfg.keys as u64);
    let root = std::env::temp_dir().join(format!(
        "takyonic-ha-soak-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&root);
    if let Err(e) = std::fs::create_dir_all(&root) {
        report.fail(format!("mkdir: {e}"));
        return report;
    }

    let base_port = 19000 + (std::process::id() % 500) * 3;
    let mut endpoints = HashMap::new();
    for id in 1u64..=3 {
        endpoints.insert(id, format!("127.0.0.1:{}", base_port + id as u32 - 1));
    }

    let mut live: HashMap<u64, LiveNode> = HashMap::new();
    for id in 1u64..=3 {
        match TakyonicNode::open(
            id,
            root.join(format!("node-{id}")),
            endpoints.clone(),
            node_config(&root, id),
        ) {
            Ok(node) => {
                let node = Arc::new(node);
                let (s, t) = node.start_background();
                live.insert(
                    id,
                    LiveNode {
                        node,
                        handles: vec![s, t],
                    },
                );
            }
            Err(e) => {
                report.fail(format!("open node-{id}: {e}"));
                cleanup(&mut live, &root).await;
                return report;
            }
        }
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    let nodes: Vec<Arc<TakyonicNode>> = live.values().map(|l| Arc::clone(&l.node)).collect();
    let leader_id = match wait_for_leader(&nodes, Duration::from_secs(15)).await {
        Ok(id) => id,
        Err(e) => {
            report.fail(format!("initial leader: {e}"));
            cleanup(&mut live, &root).await;
            return report;
        }
    };

    let stop_storm = Arc::new(AtomicBool::new(false));
    let storm_ok = Arc::new(AtomicU64::new(0));
    let next_key = Arc::new(AtomicU64::new(0));
    let leader_slot: Arc<tokio::sync::RwLock<Option<Arc<TakyonicNode>>>> =
        Arc::new(tokio::sync::RwLock::new(
            live.get(&leader_id).map(|l| Arc::clone(&l.node)),
        ));

    let storm_tasks: Vec<_> = (0..4)
        .map(|w| {
            let stop = Arc::clone(&stop_storm);
            let ok = Arc::clone(&storm_ok);
            let next = Arc::clone(&next_key);
            let leader_slot = Arc::clone(&leader_slot);
            let key_cap = cfg.keys as u64;
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
                    if i >= key_cap {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                        continue;
                    }
                    let key = format!("ha-{i}");
                    let val = format!("v-{i}-w{w}");
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
                            tokio::time::sleep(Duration::from_millis(10)).await;
                        }
                    }
                }
            })
        })
        .collect();

    let deadline = tokio::time::Instant::now() + cfg.duration;
    let mut next_kill = tokio::time::Instant::now() + cfg.kill_every;
    let mut cycles = 0u64;

    while tokio::time::Instant::now() < deadline {
        if tokio::time::Instant::now() >= next_kill {
            let current: Vec<Arc<TakyonicNode>> =
                live.values().map(|l| Arc::clone(&l.node)).collect();
            let victim_id = match wait_for_leader(&current, Duration::from_secs(10)).await {
                Ok(id) => id,
                Err(e) => {
                    report.fail(format!("pre-kill leader: {e}"));
                    break;
                }
            };
            if let Some(mut victim) = live.remove(&victim_id) {
                for h in victim.handles.drain(..) {
                    h.abort();
                }
                drop(victim.node);
            }
            *leader_slot.write().await = None;

            let survivors: Vec<Arc<TakyonicNode>> =
                live.values().map(|l| Arc::clone(&l.node)).collect();
            let new_id = match wait_for_leader(&survivors, Duration::from_secs(15)).await {
                Ok(id) => id,
                Err(e) => {
                    report.fail(format!("failover leader: {e}"));
                    break;
                }
            };
            if new_id == victim_id {
                report.fail("new leader id equals assassinated id");
                break;
            }
            let new_leader = survivors
                .iter()
                .find(|n| n.id() == new_id)
                .cloned();
            let Some(new_leader) = new_leader else {
                report.fail("new leader missing from survivors");
                break;
            };
            *leader_slot.write().await = Some(Arc::clone(&new_leader));

            // Resurrect victim.
            match TakyonicNode::open(
                victim_id,
                root.join(format!("node-{victim_id}")),
                endpoints.clone(),
                node_config(&root, victim_id),
            ) {
                Ok(node) => {
                    let phoenix = Arc::new(node);
                    let (s, t) = phoenix.start_background();
                    let catch_deadline = tokio::time::Instant::now() + Duration::from_secs(20);
                    let target = new_leader.raft().commit_index();
                    let mut ok_catchup = false;
                    while tokio::time::Instant::now() < catch_deadline {
                        if phoenix.role() == Role::Follower
                            && phoenix.engine().last_applied() >= target
                            && phoenix.raft().commit_index() >= target
                        {
                            ok_catchup = true;
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                    if !ok_catchup {
                        report.fail(format!(
                            "resurrection catch-up stalled for node-{victim_id}"
                        ));
                    }
                    live.insert(
                        victim_id,
                        LiveNode {
                            node: phoenix,
                            handles: vec![s, t],
                        },
                    );
                }
                Err(e) => {
                    report.fail(format!("resurrect node-{victim_id}: {e}"));
                    break;
                }
            }

            let leaders = live
                .values()
                .filter(|l| l.node.role() == Role::Leader)
                .count();
            if leaders != 1 {
                report.fail(format!("expected 1 leader after cycle, got {leaders}"));
                break;
            }
            cycles += 1;
            next_kill = tokio::time::Instant::now() + cfg.kill_every;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    stop_storm.store(true, Ordering::Relaxed);
    for t in storm_tasks {
        t.abort();
        let _ = t.await;
    }
    report.ops = storm_ok.load(Ordering::Relaxed);

    // Spot-check: sample acked keys on all live nodes.
    let acked = report.ops;
    if acked > 0 {
        let sample_hi = acked.saturating_sub(1);
        let sample_lo = sample_hi / 2;
        for id in live.keys().copied().collect::<Vec<_>>() {
            let node = &live.get(&id).unwrap().node;
            for i in (sample_lo..=sample_hi).step_by(11) {
                let key = Key::new(format!("ha-{i}").into_bytes());
                match node.get(&key) {
                    Ok(Some(_)) => {}
                    Ok(None) => {
                        // Only fail if majority of live nodes have it.
                        let present = live
                            .values()
                            .filter(|l| {
                                l.node
                                    .get(&key)
                                    .ok()
                                    .flatten()
                                    .is_some()
                            })
                            .count();
                        if present >= 2 {
                            report.fail(format!(
                                "node-{id} missing ha-{i} present on {present} peers"
                            ));
                        }
                    }
                    Err(e) => report.fail(format!("node-{id} get ha-{i}: {e}")),
                }
            }
        }
    }

    if cycles == 0 && cfg.duration >= Duration::from_secs(10) {
        report.fail("completed zero kill/failover cycles");
    }

    cleanup(&mut live, &root).await;
    report
}

async fn cleanup(live: &mut HashMap<u64, LiveNode>, root: &PathBuf) {
    for l in live.values() {
        for h in &l.handles {
            h.abort();
        }
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    for l in live.values() {
        let _ = l.node.close();
    }
    live.clear();
    let _ = std::fs::remove_dir_all(root);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ha_soak_config_defaults_are_sane() {
        let c = HaSoakConfig::from_env();
        assert!(c.kill_every > Duration::ZERO);
        assert!(c.duration >= c.kill_every);
    }

    #[test]
    #[ignore = "long cluster soak; run with --ignored --release"]
    fn ha_soak_short_ignored() {
        let report = run_ha_soak(HaSoakConfig {
            duration: Duration::from_secs(15),
            kill_every: Duration::from_secs(5),
            keys: 200,
        });
        assert!(report.ok(), "{}", report.summary());
    }
}
