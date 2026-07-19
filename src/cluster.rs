//! Cluster node: TakyonicEngine + RaftConsensus + tonic transport.
//!
//! Client writes go to the leader via [`TakyonicNode::put`]. The leader
//! appends to the durable Raft log (group commit), replicates with
//! `AppendEntries`, and applies to the memtable only after a quorum commit.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use tracing::info;

use crate::config::Config;
use crate::consensus::{RaftConsensus, Role};
use crate::engine::TakyonicEngine;
use crate::error::{Result, TakyonicError};
use crate::membership::ClusterMembership;
use crate::network::{self, PeerClients};
use crate::raft::RaftCommand;
use crate::raft_log::RaftLog;
use crate::types::{Key, Value};

/// One Takyonic node in a Raft group.
pub struct TakyonicNode {
    id: u64,
    addr: SocketAddr,
    engine: Arc<TakyonicEngine>,
    raft: Arc<RaftConsensus>,
    peers: Arc<PeerClients>,
}

impl TakyonicNode {
    /// Open local storage, Raft log, and peer client map.
    ///
    /// `endpoints` maps every bootstrap member (including self) to `host:port`.
    /// Initial voting membership is the full endpoint set.
    pub fn open(
        id: u64,
        root: impl AsRef<Path>,
        endpoints: HashMap<u64, String>,
        engine_config: Config,
    ) -> Result<Self> {
        Self::open_with_membership(
            id,
            root,
            endpoints.clone(),
            ClusterMembership::from_endpoints(endpoints),
            engine_config,
        )
    }

    /// Open a joining node with empty voting membership (awaits AddNode / snapshot).
    ///
    /// `self_addr` is this node's listen address. Peer endpoints are learned from
    /// the leader via InstallSnapshot / ConfigChange.
    pub fn open_joining(
        id: u64,
        root: impl AsRef<Path>,
        self_addr: impl Into<String>,
        engine_config: Config,
    ) -> Result<Self> {
        let self_addr = self_addr.into();
        let mut endpoints = HashMap::new();
        endpoints.insert(id, self_addr);
        Self::open_with_membership(
            id,
            root,
            endpoints,
            ClusterMembership::empty(),
            engine_config,
        )
    }

    /// Open with an explicit initial membership (advanced / tests).
    pub fn open_with_membership(
        id: u64,
        root: impl AsRef<Path>,
        endpoints: HashMap<u64, String>,
        membership: ClusterMembership,
        engine_config: Config,
    ) -> Result<Self> {
        let root = root.as_ref();
        std::fs::create_dir_all(root)?;
        let addr: SocketAddr = endpoints
            .get(&id)
            .ok_or_else(|| TakyonicError::Config(format!("missing endpoint for node {id}")))?
            .parse()
            .map_err(|e| TakyonicError::Config(format!("bad bind addr: {e}")))?;

        let engine = Arc::new(TakyonicEngine::open(engine_config.clone())?);
        let log = RaftLog::open(root.join("raft"))?;
        let snap = log.snapshot_meta();
        if snap.last_included_index > 0 {
            engine.set_last_applied(snap.last_included_index)?;
        }
        let raft = RaftConsensus::new_with_threshold(
            id,
            membership,
            log,
            Arc::clone(&engine),
            engine_config.raft_snapshot_threshold,
        );
        let peer_endpoints: HashMap<u64, String> = raft
            .membership()
            .endpoints()
            .iter()
            .filter(|&(&p, _)| p != id)
            .map(|(&p, a)| (p, a.clone()))
            .collect();
        // Also seed any bootstrap endpoints not yet in membership (joining hints).
        let mut peer_endpoints = peer_endpoints;
        for (p, a) in endpoints {
            if p != id {
                peer_endpoints.entry(p).or_insert(a);
            }
        }
        let peers = Arc::new(PeerClients::new(peer_endpoints));

        engine.attach_raft_node(&raft);

        Ok(Self {
            id,
            addr,
            engine,
            raft,
            peers,
        })
    }

    /// Node id.
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Bind address.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Shared engine (reads / observability).
    pub fn engine(&self) -> &Arc<TakyonicEngine> {
        &self.engine
    }

    /// Shared consensus.
    pub fn raft(&self) -> &Arc<RaftConsensus> {
        &self.raft
    }

    /// Outbound peer clients (dynamic with membership).
    pub fn peers(&self) -> &Arc<PeerClients> {
        &self.peers
    }

    /// Current role.
    pub fn role(&self) -> Role {
        self.raft.role()
    }

    /// Known leader id, if any.
    pub fn leader_id(&self) -> Option<u64> {
        self.raft.leader_id()
    }

    /// Active voting membership.
    pub fn membership(&self) -> ClusterMembership {
        self.raft.membership()
    }

    /// Point read from the local state machine.
    pub fn get(&self, key: &Key) -> Result<Option<Value>> {
        self.engine.get(key)
    }

    /// Propose a put through Raft. Only valid on the leader.
    pub async fn put(&self, key: impl Into<Key>, value: impl Into<Value>) -> Result<u64> {
        self.raft.propose(RaftCommand::put(key, value)).await
    }

    /// Propose a delete through Raft. Only valid on the leader.
    pub async fn delete(&self, key: impl Into<Key>) -> Result<u64> {
        self.raft.propose(RaftCommand::delete(key)).await
    }

    /// Propose AddNode (single-server configuration change).
    pub async fn add_node(&self, id: u64, address: impl Into<String>) -> Result<u64> {
        self.raft.add_node(id, address).await
    }

    /// Propose RemoveNode (single-server configuration change).
    pub async fn remove_node(&self, id: u64) -> Result<u64> {
        self.raft.remove_node(id).await
    }

    /// Spawn the gRPC server and the election/replication tick loop.
    ///
    /// Returns join handles; drop/`abort` them on shutdown.
    pub fn start_background(
        self: &Arc<Self>,
    ) -> (tokio::task::JoinHandle<()>, tokio::task::JoinHandle<()>) {
        let raft = Arc::clone(&self.raft);
        let engine = Arc::clone(&self.engine);
        let addr = self.addr;
        let id = self.id;
        let server = tokio::spawn(async move {
            if let Err(e) = network::serve_node(addr, id, engine, raft).await {
                tracing::error!(%e, "Raft/Client gRPC server exited");
            }
        });

        let raft = Arc::clone(&self.raft);
        let peers = Arc::clone(&self.peers);
        let self_id = self.id;
        let ticker = tokio::spawn(async move {
            let mut last_mgen = 0u64;
            loop {
                if raft.is_removed() {
                    // Passive learner: keep serving RPCs but stop elections / proposes.
                    raft.wait_kick(Duration::from_millis(200)).await;
                    continue;
                }
                let mgen = raft.membership_gen();
                if mgen != last_mgen {
                    let membership = raft.membership();
                    peers.sync_from_membership(self_id, &membership).await;
                    last_mgen = mgen;
                }
                match raft.role() {
                    Role::Leader => {
                        network::replicate_to_all(&raft, &peers).await;
                        let wait = if raft.has_pending_proposes() {
                            Duration::from_millis(1)
                        } else {
                            Duration::from_millis(50)
                        };
                        raft.wait_kick(wait).await;
                    }
                    Role::Follower | Role::Candidate => {
                        if raft.election_timed_out() {
                            network::run_election(&raft, &peers).await;
                        }
                        raft.wait_kick(Duration::from_millis(50)).await;
                    }
                }
            }
        });

        info!(node = self.id, %addr, "TakyonicNode started");
        (server, ticker)
    }

    /// Graceful shutdown of storage (does not abort background tasks).
    pub fn close(&self) -> Result<()> {
        self.engine.close()
    }
}

/// Wait until some node in `nodes` reports [`Role::Leader`], or timeout.
pub async fn wait_for_leader(nodes: &[Arc<TakyonicNode>], timeout: Duration) -> Result<u64> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        for n in nodes {
            if n.raft().is_removed() {
                continue;
            }
            if n.role() == Role::Leader {
                return Ok(n.id());
            }
            if let Some(leader) = n.leader_id() {
                // Confirm that node actually believes it is leader.
                if let Some(l) = nodes.iter().find(|x| x.id() == leader) {
                    if l.role() == Role::Leader && !l.raft().is_removed() {
                        return Ok(leader);
                    }
                }
            }
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(TakyonicError::Raft(
                "timed out waiting for leader election".into(),
            ));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::TakyonicClient;
    use crate::error::TakyonicError;
    use crate::pg::SessionState;
    use crate::schema::TableSchema;
    use crate::types::Key;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn free_port() -> u16 {
        std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
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

    async fn boot_cluster(
        n: u64,
        label: &str,
    ) -> (
        std::path::PathBuf,
        Vec<Arc<TakyonicNode>>,
        Vec<tokio::task::JoinHandle<()>>,
    ) {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("takyonic-raft-ha-{label}-{nanos}"));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();

        let mut endpoints = HashMap::new();
        for id in 1..=n {
            endpoints.insert(id, format!("127.0.0.1:{}", free_port()));
        }

        let mut nodes = Vec::new();
        let mut handles = Vec::new();
        for id in 1..=n {
            let node = Arc::new(
                TakyonicNode::open(
                    id,
                    root.join(format!("node-{id}")),
                    endpoints.clone(),
                    node_config(&root, id),
                )
                .expect("open node"),
            );
            let (s, t) = node.start_background();
            handles.push(s);
            handles.push(t);
            nodes.push(node);
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
        (root, nodes, handles)
    }

    async fn wait_key(
        nodes: &[Arc<TakyonicNode>],
        key: &Key,
        expect: Option<&[u8]>,
        timeout: Duration,
    ) {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let ok = nodes.iter().all(|n| match (n.get(key).ok().flatten(), expect) {
                (Some(v), Some(e)) => v.as_bytes() == e,
                (None, None) => true,
                _ => false,
            });
            if ok {
                return;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("timed out waiting for key replication");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn three_node_election_and_sql_insert_replicates() {
        let (root, nodes, handles) = boot_cluster(3, "sql").await;
        let leader_id = wait_for_leader(&nodes, Duration::from_secs(10))
            .await
            .expect("elect leader");
        let leader = nodes.iter().find(|n| n.id() == leader_id).unwrap();
        assert_eq!(leader.role(), Role::Leader);

        let seeds: Vec<String> = nodes.iter().map(|n| n.addr().to_string()).collect();
        let client = TakyonicClient::new(seeds);
        client.connect().await.expect("connect");
        client
            .register_table(TableSchema::new("users", "id", Vec::new()))
            .await
            .expect("register");

        for i in 1..=5 {
            client
                .execute_sql(&format!(
                    "INSERT INTO users (id, name) VALUES ('{i}', 'user{i}')"
                ))
                .await
                .expect("insert");
        }

        // Followers must eventually serve the same rows via local state machine.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            let mut all_ok = true;
            for n in &nodes {
                for i in 1..=5u64 {
                    let key = Key::new(format!("Data_users_{i}"));
                    match n.get(&key).ok().flatten() {
                        Some(v) => {
                            let s = String::from_utf8_lossy(v.as_bytes());
                            if !s.contains(&format!("user{i}")) {
                                all_ok = false;
                            }
                        }
                        None => all_ok = false,
                    }
                }
            }
            if all_ok {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("followers missing replicated INSERT rows");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        // OCC on a follower SessionState must reject (NotLeader), not silently
        // write through the local Raft stand-in.
        let follower = nodes.iter().find(|n| n.role() != Role::Leader).unwrap();
        let mut session = SessionState::new(Arc::clone(follower.engine()));
        let err = session
            .execute_sql("INSERT INTO users (id, name) VALUES ('x', 'nope')")
            .expect_err("follower must reject DML");
        assert!(
            matches!(err, TakyonicError::NotLeader { .. }),
            "expected NotLeader, got {err:?}"
        );

        for h in handles {
            h.abort();
        }
        for n in &nodes {
            let _ = n.close();
        }
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn smart_client_update_and_delete_record_via_txn_rpc() {
        let (root, nodes, handles) = boot_cluster(1, "upd-del").await;
        let leader_id = wait_for_leader(&nodes, Duration::from_secs(10))
            .await
            .expect("elect leader");
        let leader = nodes.iter().find(|n| n.id() == leader_id).unwrap();
        assert_eq!(leader.role(), Role::Leader);

        let seeds: Vec<String> = nodes.iter().map(|n| n.addr().to_string()).collect();
        let client = TakyonicClient::new(seeds);
        client.connect().await.expect("connect");
        client
            .register_table(TableSchema::new("users", "id", Vec::new()))
            .await
            .expect("register");

        client
            .execute_sql("INSERT INTO users (id, name) VALUES ('1', 'Ada')")
            .await
            .expect("insert");
        client
            .execute_sql("UPDATE users SET name = 'Ada Lovelace' WHERE id = '1'")
            .await
            .expect("update");

        let rows = client
            .execute_sql("SELECT * FROM users WHERE id = '1'")
            .await
            .expect("select after update");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("name"), Some("Ada Lovelace"));

        client
            .execute_sql("DELETE FROM users WHERE id = '1'")
            .await
            .expect("delete");
        let rows = client
            .execute_sql("SELECT * FROM users WHERE id = '1'")
            .await
            .expect("select after delete");
        assert!(rows.is_empty(), "row must be gone after DELETE");

        for h in handles {
            h.abort();
        }
        for n in &nodes {
            let _ = n.close();
        }
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn leader_crash_triggers_reelection_and_safe_writes() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("takyonic-raft-ha-crash-{nanos}"));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();

        let mut endpoints = HashMap::new();
        for id in 1u64..=3 {
            endpoints.insert(id, format!("127.0.0.1:{}", free_port()));
        }

        let mut live: HashMap<u64, (Arc<TakyonicNode>, Vec<tokio::task::JoinHandle<()>>)> =
            HashMap::new();
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
            live.insert(id, (node, vec![s, t]));
        }
        tokio::time::sleep(Duration::from_millis(200)).await;

        let all_nodes = || -> Vec<Arc<TakyonicNode>> {
            live.values().map(|(n, _)| Arc::clone(n)).collect()
        };

        let leader_id = wait_for_leader(&all_nodes(), Duration::from_secs(10))
            .await
            .expect("elect");
        let leader = Arc::clone(&live.get(&leader_id).unwrap().0);
        leader
            .put(Key::new(b"k0".as_slice()), b"v0".as_slice())
            .await
            .expect("bootstrap put");
        wait_key(
            &all_nodes(),
            &Key::new(b"k0".as_slice()),
            Some(b"v0"),
            Duration::from_secs(10),
        )
        .await;

        // Forcefully shut down the leader (abort tasks + drop node).
        let (_killed, tasks) = live.remove(&leader_id).unwrap();
        for h in tasks {
            h.abort();
        }
        drop(_killed);

        let survivors: Vec<Arc<TakyonicNode>> =
            live.values().map(|(n, _)| Arc::clone(n)).collect();
        let new_leader = wait_for_leader(&survivors, Duration::from_secs(15))
            .await
            .expect("reelect after crash");
        assert_ne!(new_leader, leader_id);

        let leader = survivors.iter().find(|n| n.id() == new_leader).unwrap();
        leader
            .put(Key::new(b"k1".as_slice()), b"v1".as_slice())
            .await
            .expect("write after failover");
        wait_key(
            &survivors,
            &Key::new(b"k1".as_slice()),
            Some(b"v1"),
            Duration::from_secs(10),
        )
        .await;

        for (_, (n, tasks)) in live {
            for h in tasks {
                h.abort();
            }
            let _ = n.close();
        }
        let _ = std::fs::remove_dir_all(root);
    }
}
