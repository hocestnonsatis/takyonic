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
use crate::object_store::ObjectStorage;
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
            None,
        )
    }

    /// Like [`Self::open`] but attaches an explicit Tier-2 [`ObjectStorage`]
    /// (local POSIX mirror, S3/MinIO, …) for storage–compute decoupling.
    pub fn open_with_object_storage(
        id: u64,
        root: impl AsRef<Path>,
        endpoints: HashMap<u64, String>,
        engine_config: Config,
        store: Arc<dyn ObjectStorage>,
    ) -> Result<Self> {
        Self::open_with_membership(
            id,
            root,
            endpoints.clone(),
            ClusterMembership::from_endpoints(endpoints),
            engine_config,
            Some(store),
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
            None,
        )
    }

    /// Open with an explicit initial membership (advanced / tests).
    pub fn open_with_membership(
        id: u64,
        root: impl AsRef<Path>,
        endpoints: HashMap<u64, String>,
        membership: ClusterMembership,
        engine_config: Config,
        object_store: Option<Arc<dyn ObjectStorage>>,
    ) -> Result<Self> {
        let root = root.as_ref();
        std::fs::create_dir_all(root)?;
        let addr: SocketAddr = endpoints
            .get(&id)
            .ok_or_else(|| TakyonicError::Config(format!("missing endpoint for node {id}")))?
            .parse()
            .map_err(|e| TakyonicError::Config(format!("bad bind addr: {e}")))?;

        let engine = match object_store {
            Some(store) => Arc::new(TakyonicEngine::open_with_object_storage(
                engine_config.clone(),
                store,
            )?),
            None => Arc::new(TakyonicEngine::open(engine_config.clone())?),
        };
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
        engine.set_shard_id(id);
        if engine.config().mpp_enabled {
            let m = raft.membership();
            let mut workers = Vec::new();
            let mut ids: Vec<u64> = m.endpoints().keys().copied().collect();
            ids.sort_unstable();
            for (slot, nid) in ids.into_iter().enumerate() {
                let address = m.address(nid).unwrap_or("127.0.0.1:0").to_string();
                workers.push(crate::mpp::WorkerEndpoint {
                    node_id: nid,
                    address,
                    slot: slot as u32,
                });
            }
            if !workers.is_empty() {
                engine.set_mpp_workers(workers);
            }
        }

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
                tracing::error!(%e, "Raft/Client/Twopc gRPC server exited");
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
    use std::sync::{Mutex, MutexGuard};
    use std::time::{SystemTime, UNIX_EPOCH};

    /// Serialize cluster port bind + Raft load so parallel tests do not steal
    /// `free_port()` bindings or starve AppendEntries under CI contention.
    fn cluster_port_lock() -> MutexGuard<'static, ()> {
        static LOCK: Mutex<()> = Mutex::new(());
        LOCK.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Hold for the lifetime of a multi-node Raft test to avoid CPU-starved
    /// replication when several 3-node clusters run concurrently.
    fn cluster_suite_lock() -> MutexGuard<'static, ()> {
        static LOCK: Mutex<()> = Mutex::new(());
        LOCK.lock().unwrap_or_else(|e| e.into_inner())
    }

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

    #[test]
    fn open_with_local_object_store_attaches_tier2() {
        use crate::object_store::LocalFileBackend;

        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("takyonic-d1-obj-{nanos}"));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(root.join("objects")).unwrap();
        let store = Arc::new(LocalFileBackend::open(root.join("objects")).unwrap());
        let port = free_port();
        let mut endpoints = HashMap::new();
        endpoints.insert(1u64, format!("127.0.0.1:{port}"));
        let node = TakyonicNode::open_with_object_storage(
            1,
            &root,
            endpoints,
            node_config(&root, 1),
            store,
        )
        .unwrap();
        assert!(
            node.engine().manifest().is_some(),
            "object storage open must attach a ManifestManager"
        );
        node.close().unwrap();
        let _ = std::fs::remove_dir_all(root);
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
        let mut nodes = Vec::new();
        let mut handles = Vec::new();
        {
            let _lock = cluster_port_lock();
            for id in 1..=n {
                endpoints.insert(id, format!("127.0.0.1:{}", free_port()));
            }
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
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
        (root, nodes, handles)
    }

    /// One-node Raft group with a chosen `id` (independent Engine-backed 2PC shard).
    async fn boot_solo_shard(
        id: u64,
        label: &str,
    ) -> (
        std::path::PathBuf,
        Arc<TakyonicNode>,
        Vec<tokio::task::JoinHandle<()>>,
    ) {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("takyonic-solo-{label}-{nanos}"));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let (node, handles) = {
            let _lock = cluster_port_lock();
            let mut endpoints = HashMap::new();
            endpoints.insert(id, format!("127.0.0.1:{}", free_port()));
            let node = Arc::new(
                TakyonicNode::open(
                    id,
                    root.join(format!("node-{id}")),
                    endpoints,
                    node_config(&root, id),
                )
                .expect("open solo"),
            );
            let (s, t) = node.start_background();
            (node, vec![s, t])
        };
        tokio::time::sleep(Duration::from_millis(200)).await;
        (root, node, handles)
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
        let _suite = cluster_suite_lock();
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
    async fn three_node_smart_client_session_sql_join() {
        let _suite = cluster_suite_lock();
        let (root, nodes, handles) = boot_cluster(3, "sess-sql").await;
        let _leader_id = wait_for_leader(&nodes, Duration::from_secs(10))
            .await
            .expect("elect leader");

        let seeds: Vec<String> = nodes.iter().map(|n| n.addr().to_string()).collect();
        let client = TakyonicClient::new(seeds);
        client.connect().await.expect("connect");

        client
            .execute_session_sql("CREATE TABLE users (id BIGINT PRIMARY KEY, name TEXT)")
            .await
            .expect("create users");
        client
            .execute_session_sql(
                "CREATE TABLE orders (id BIGINT PRIMARY KEY, user_id BIGINT, item TEXT)",
            )
            .await
            .expect("create orders");
        wait_replicas_caught_up(&nodes, Duration::from_secs(30)).await;

        client
            .execute_session_sql("INSERT INTO users (id, name) VALUES (1, 'Ada')")
            .await
            .expect("insert user");
        client
            .execute_session_sql(
                "INSERT INTO orders (id, user_id, item) VALUES (10, 1, 'book')",
            )
            .await
            .expect("insert order");
        wait_replicas_caught_up(&nodes, Duration::from_secs(30)).await;

        let joined = client
            .execute_session_sql(
                "SELECT users.name AS name, orders.item AS item \
                 FROM users INNER JOIN orders ON users.id = orders.user_id",
            )
            .await
            .expect("join");
        assert_eq!(joined.tag, "SELECT");
        assert_eq!(joined.rows.len(), 1);
        assert_eq!(joined.rows[0].get("name"), Some("Ada"));
        assert_eq!(joined.rows[0].get("item"), Some("book"));

        let err = client
            .execute_sql(
                "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id",
            )
            .await
            .expect_err("narrow path must stay pgwire-only");
        assert!(
            err.to_string().contains(crate::client::PGWIRE_ONLY_HINT),
            "unexpected: {err}"
        );

        for h in handles {
            h.abort();
        }
        for n in &nodes {
            let _ = n.close();
        }
        let _ = std::fs::remove_dir_all(root);
    }

    /// Re-resolve the Raft leader and open a SessionState against it.
    async fn leader_session(nodes: &[Arc<TakyonicNode>]) -> SessionState {
        let leader_id = wait_for_leader(nodes, Duration::from_secs(15))
            .await
            .expect("elect leader");
        let leader = nodes.iter().find(|n| n.id() == leader_id).unwrap();
        assert_eq!(leader.role(), Role::Leader);
        SessionState::new(Arc::clone(leader.engine()))
    }

    /// Poll until every replica's `last_applied` reaches the leader commit index.
    async fn wait_replicas_caught_up(nodes: &[Arc<TakyonicNode>], timeout: Duration) {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let leader_id = match wait_for_leader(nodes, Duration::from_secs(5)).await {
                Ok(id) => id,
                Err(_) if tokio::time::Instant::now() >= deadline => {
                    panic!("no leader while waiting for replica catch-up");
                }
                Err(_) => {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
            };
            let leader = nodes.iter().find(|n| n.id() == leader_id).unwrap();
            let target = leader.raft().commit_index().max(leader.engine().last_applied());
            let all_ok = nodes
                .iter()
                .all(|n| n.engine().last_applied() >= target);
            if all_ok {
                return;
            }
            if tokio::time::Instant::now() >= deadline {
                let status: Vec<_> = nodes
                    .iter()
                    .map(|n| {
                        format!(
                            "node={} role={:?} applied={} commit={}",
                            n.id(),
                            n.role(),
                            n.engine().last_applied(),
                            n.raft().commit_index()
                        )
                    })
                    .collect();
                panic!("replicas not caught up to {target}; {status:?}");
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    /// Run DDL on the current leader, retrying once leadership moves.
    async fn exec_ddl_on_leader(nodes: &[Arc<TakyonicNode>], sql: &str) {
        let mut last_err = None;
        for _ in 0..8 {
            let mut session = leader_session(nodes).await;
            match session.execute_sql(sql) {
                Ok(_) => {
                    wait_replicas_caught_up(nodes, Duration::from_secs(30)).await;
                    return;
                }
                Err(TakyonicError::NotLeader { .. }) => {
                    last_err = Some("NotLeader");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Err(e) => panic!("DDL `{sql}` failed: {e}"),
            }
        }
        panic!("DDL `{sql}` exhausted NotLeader retries ({last_err:?})");
    }

    /// Poll until `pred` is true on every node, or panic with per-node status.
    async fn wait_all_nodes<F>(nodes: &[Arc<TakyonicNode>], label: &str, timeout: Duration, mut pred: F)
    where
        F: FnMut(&TakyonicNode) -> bool,
    {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if nodes.iter().all(|n| pred(n)) {
                return;
            }
            if tokio::time::Instant::now() >= deadline {
                let status: Vec<_> = nodes
                    .iter()
                    .map(|n| {
                        format!(
                            "node={} role={:?} products={:?}",
                            n.id(),
                            n.role(),
                            n.engine().table_schema("products").map(|s| {
                                (
                                    s.primary_key.clone(),
                                    s.columns.iter().map(|c| c.name.clone()).collect::<Vec<_>>(),
                                )
                            })
                        )
                    })
                    .collect();
                panic!("{label}; status: {status:?}");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn three_node_create_table_replicates_catalog() {
        let _suite = cluster_suite_lock();
        let (root, nodes, handles) = boot_cluster(3, "cat-ddl").await;

        exec_ddl_on_leader(
            &nodes,
            "CREATE TABLE products (id BIGINT PRIMARY KEY, name TEXT, price INT)",
        )
        .await;

        wait_all_nodes(
            &nodes,
            "followers missing replicated CREATE TABLE catalog",
            Duration::from_secs(45),
            |n| {
                n.engine()
                    .table_schema("products")
                    .map(|s| {
                        s.primary_key == "id"
                            && s.columns.len() == 3
                            && s.columns.iter().any(|c| c.name == "price")
                    })
                    .unwrap_or(false)
            },
        )
        .await;

        exec_ddl_on_leader(&nodes, "ALTER TABLE products ADD COLUMN sku TEXT").await;

        wait_all_nodes(
            &nodes,
            "followers missing replicated ALTER TABLE catalog",
            Duration::from_secs(60),
            |n| {
                n.engine()
                    .table_schema("products")
                    .map(|s| s.columns.iter().any(|c| c.name == "sku"))
                    .unwrap_or(false)
            },
        )
        .await;

        // Follower DDL must reject (re-resolve so we do not race an election).
        let leader_id = wait_for_leader(&nodes, Duration::from_secs(15))
            .await
            .expect("leader for follower check");
        let follower = nodes
            .iter()
            .find(|n| n.id() != leader_id && n.role() == Role::Follower)
            .or_else(|| nodes.iter().find(|n| n.id() != leader_id))
            .expect("need a non-leader node");
        let mut fsess = SessionState::new(Arc::clone(follower.engine()));
        let err = fsess
            .execute_sql("CREATE TABLE nope (id BIGINT PRIMARY KEY)")
            .expect_err("follower must reject DDL");
        assert!(
            matches!(err, TakyonicError::NotLeader { .. }),
            "expected NotLeader, got {err:?}"
        );

        exec_ddl_on_leader(&nodes, "DROP TABLE products").await;

        wait_all_nodes(
            &nodes,
            "followers still have dropped table",
            Duration::from_secs(45),
            |n| n.engine().table_schema("products").is_err(),
        )
        .await;

        for h in handles {
            h.abort();
        }
        for n in &nodes {
            let _ = n.close();
        }
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn three_node_auth_and_stats_replicate() {
        let _suite = cluster_suite_lock();
        let (root, nodes, handles) = boot_cluster(3, "meta-a5").await;
        let leader_id = wait_for_leader(&nodes, Duration::from_secs(10))
            .await
            .expect("elect leader");
        let leader = nodes.iter().find(|n| n.id() == leader_id).unwrap();

        let mut session = SessionState::new(Arc::clone(leader.engine()));
        session
            .execute_sql("CREATE TABLE sales (id BIGINT PRIMARY KEY, region TEXT)")
            .expect("create");
        session
            .execute_sql("INSERT INTO sales (id, region) VALUES (1, 'west'), (2, 'east')")
            .expect("insert");
        session
            .execute_sql("CREATE USER analyst WITH PASSWORD 'secret'")
            .expect("create user");
        session
            .execute_sql("GRANT SELECT ON sales TO analyst")
            .expect("grant");
        session
            .execute_sql("ANALYZE sales")
            .expect("analyze");

        let deadline = tokio::time::Instant::now() + Duration::from_secs(45);
        loop {
            let auth_ok = nodes.iter().all(|n| {
                let shared = n.engine().auth_catalog();
                let auth = shared.read();
                auth.get_role("analyst").is_some()
                    && auth.has_privilege(
                        &crate::rbac::AuthContext {
                            user: "analyst".into(),
                            roles: {
                                let mut s = std::collections::BTreeSet::new();
                                s.insert("analyst".into());
                                s
                            },
                            is_superuser: false,
                        },
                        "sales",
                        crate::rbac::Privilege::Select,
                    )
            });
            let stats_ok = nodes
                .iter()
                .all(|n| n.engine().table_stats("sales").row_count >= 2);
            if auth_ok && stats_ok {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("followers missing replicated AUTH/STATS metadata");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        // Index DDL lands via CatalogUpsert (A4) — verify on all nodes.
        session
            .execute_sql("CREATE INDEX idx_sales_region ON sales(region)")
            .expect("create index");
        let deadline = tokio::time::Instant::now() + Duration::from_secs(45);
        loop {
            let ok = nodes.iter().all(|n| {
                n.engine()
                    .table_schema("sales")
                    .map(|s| s.indexes.iter().any(|i| i.name == "idx_sales_region"))
                    .unwrap_or(false)
            });
            if ok {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("followers missing replicated INDEX catalog");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        let follower = nodes.iter().find(|n| n.role() != Role::Leader).unwrap();
        let mut fsess = SessionState::new(Arc::clone(follower.engine()));
        let err = fsess
            .execute_sql("CREATE USER nope WITH PASSWORD 'x'")
            .expect_err("follower must reject AUTH DDL");
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
    async fn three_node_serve_node_exposes_twopc_service() {
        // Three independent single-node Raft groups = three Engine-backed shards
        // (replicas of one group cannot each accept 2PC proposes — only the leader can).
        let mut roots = Vec::new();
        let mut all_handles = Vec::new();
        let mut nodes = Vec::new();
        for id in 1u64..=3 {
            let (root, node, handles) = boot_solo_shard(id, &format!("twopc-b2-{id}")).await;
            let _ = wait_for_leader(std::slice::from_ref(&node), Duration::from_secs(10))
                .await
                .expect("elect leader");
            roots.push(root);
            all_handles.extend(handles);
            nodes.push(node);
        }

        let mut remotes = Vec::new();
        for n in &nodes {
            let remote = crate::twopc_service::RemoteShard::connect(n.id(), n.addr())
                .await
                .expect("connect twopc on serve_node");
            remotes.push(remote);
        }

        let remotes_for_txn = remotes.clone();
        let outcome = tokio::task::spawn_blocking(move || {
            use crate::dtxn::{
                DistTxnRequest, ShardParticipant, TransactionCoordinator, put_branch,
            };
            use crate::types::{Key, Value};
            let tc = TransactionCoordinator::new(None);
            for r in &remotes_for_txn {
                tc.register_shard(Arc::clone(r) as Arc<dyn ShardParticipant>);
            }
            tc.execute(DistTxnRequest {
                read_ts: 1,
                branches: vec![
                    put_branch(1, Key::new(b"a".as_slice()), Value::new(b"1".as_slice())),
                    put_branch(2, Key::new(b"b".as_slice()), Value::new(b"2".as_slice())),
                ],
            })
        })
        .await
        .expect("join")
        .expect("execute");
        assert!(
            matches!(
                outcome,
                crate::dtxn::DistTxnOutcome::Committed { .. }
            ),
            "expected Committed, got {outcome:?}"
        );

        use crate::dtxn::ShardParticipant;
        let v1 = remotes[0].get_at(&crate::types::Key::new(b"a".as_slice()), u64::MAX);
        assert_eq!(v1.unwrap().as_bytes(), b"1");
        let v2 = remotes[1].get_at(&crate::types::Key::new(b"b".as_slice()), u64::MAX);
        assert_eq!(v2.unwrap().as_bytes(), b"2");

        // B2: values must be visible via Engine LSM on each shard.
        assert_eq!(
            nodes[0]
                .engine()
                .get(&crate::types::Key::new(b"a".as_slice()))
                .unwrap()
                .unwrap()
                .as_bytes(),
            b"1"
        );
        assert_eq!(
            nodes[1]
                .engine()
                .get(&crate::types::Key::new(b"b".as_slice()))
                .unwrap()
                .unwrap()
                .as_bytes(),
            b"2"
        );

        for h in all_handles {
            h.abort();
        }
        for n in &nodes {
            let _ = n.close();
        }
        for root in roots {
            let _ = std::fs::remove_dir_all(root);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn client_execute_dist_txn_cross_shard() {
        let mut roots = Vec::new();
        let mut all_handles = Vec::new();
        let mut nodes = Vec::new();
        for id in 1u64..=2 {
            let (root, node, handles) = boot_solo_shard(id, &format!("cli-dtxn-{id}")).await;
            let _ = wait_for_leader(std::slice::from_ref(&node), Duration::from_secs(10))
                .await
                .expect("elect");
            roots.push(root);
            all_handles.extend(handles);
            nodes.push(node);
        }

        let endpoints: Vec<(u64, String)> = nodes
            .iter()
            .map(|n| (n.id(), n.addr().to_string()))
            .collect();
        let client = TakyonicClient::new(endpoints.iter().map(|(_, a)| a.clone()));
        let req = crate::dtxn::DistTxnRequest {
            read_ts: 0, // overwritten by coordinator begin()
            branches: vec![
                crate::dtxn::put_branch(
                    1,
                    Key::new(b"cli-a".as_slice()),
                    crate::types::Value::new(b"va".as_slice()),
                ),
                crate::dtxn::put_branch(
                    2,
                    Key::new(b"cli-b".as_slice()),
                    crate::types::Value::new(b"vb".as_slice()),
                ),
            ],
        };
        let outcome = client
            .execute_dist_txn(endpoints, req)
            .await
            .expect("dist txn");
        assert!(matches!(
            outcome,
            crate::dtxn::DistTxnOutcome::Committed { .. }
        ));
        assert_eq!(
            nodes[0]
                .engine()
                .get(&Key::new(b"cli-a".as_slice()))
                .unwrap()
                .unwrap()
                .as_bytes(),
            b"va"
        );
        assert_eq!(
            nodes[1]
                .engine()
                .get(&Key::new(b"cli-b".as_slice()))
                .unwrap()
                .unwrap()
                .as_bytes(),
            b"vb"
        );

        for h in all_handles {
            h.abort();
        }
        for n in &nodes {
            let _ = n.close();
        }
        for root in roots {
            let _ = std::fs::remove_dir_all(root);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn engine_twopc_crash_after_prepare_recovers_presumed_abort() {
        use crate::dtxn::{
            DistTxnOutcome, DistTxnRequest, ShardParticipant, TransactionCoordinator, put_branch,
        };
        use crate::types::Value;

        let mut roots = Vec::new();
        let mut all_handles = Vec::new();
        let mut nodes = Vec::new();
        for id in 1u64..=3 {
            let (root, node, handles) = boot_solo_shard(id, &format!("b4-crash-{id}")).await;
            let _ = wait_for_leader(std::slice::from_ref(&node), Duration::from_secs(10))
                .await
                .expect("elect");
            roots.push(root);
            all_handles.extend(handles);
            nodes.push(node);
        }

        let mut remotes = Vec::new();
        for n in &nodes {
            remotes.push(
                crate::twopc_service::RemoteShard::connect(n.id(), n.addr())
                    .await
                    .expect("connect"),
            );
        }

        let tc = TransactionCoordinator::new(None);
        for r in &remotes {
            tc.register_shard(Arc::clone(r) as Arc<dyn ShardParticipant>);
        }

        // Seed committed value on shard 1.
        {
            let tc = Arc::clone(&tc);
            tokio::task::spawn_blocking(move || {
                tc.execute(DistTxnRequest {
                    read_ts: 0,
                    branches: vec![put_branch(
                        1,
                        Key::new(b"acct".as_slice()),
                        Value::new(b"100".as_slice()),
                    )],
                })
                .unwrap();
            })
            .await
            .unwrap();
        }

        remotes[2]
            .inject_crash_after_prepare(true)
            .expect("inject");
        let tc2 = Arc::clone(&tc);
        let outcome = tokio::task::spawn_blocking(move || {
            tc2.execute(DistTxnRequest {
                read_ts: 0,
                branches: vec![
                    put_branch(1, Key::new(b"acct".as_slice()), Value::new(b"90".as_slice())),
                    put_branch(2, Key::new(b"y".as_slice()), Value::new(b"9".as_slice())),
                    put_branch(3, Key::new(b"z".as_slice()), Value::new(b"9".as_slice())),
                ],
            })
        })
        .await
        .unwrap()
        .unwrap();
        match &outcome {
            DistTxnOutcome::Aborted { reason, .. } => {
                assert!(
                    reason.contains("crashed after PREPARED"),
                    "unexpected: {reason}"
                );
            }
            other => panic!("expected Aborted, got {other:?}"),
        }

        // Simulate process loss of in-memory prepared, then rebuild from Raft.
        nodes[2].engine().inject_twopc_crash_after_prepare(false);
        {
            // Clear memory as if the process restarted.
            let _ = nodes[2].engine().twopc_recover_from_raft_log();
        }
        // Force empty then recover again to prove log rebuild (recover already rebuilds).
        remotes[2].recover_from_raft_log().expect("recover rpc");
        let orphans = remotes[2].orphaned_prepared();
        assert!(
            !orphans.is_empty(),
            "shard 3 must retain orphaned PREPARED after Raft rebuild; got {orphans:?}"
        );

        let tc3 = Arc::clone(&tc);
        let remote3 = Arc::clone(&remotes[2]);
        let n = tokio::task::spawn_blocking(move || tc3.recover_participant(remote3.as_ref()))
            .await
            .unwrap()
            .unwrap();
        assert!(n >= 1);
        assert!(remotes[2]
            .get(&Key::new(b"z".as_slice()))
            .is_none());
        // Bank invariant: aborted transfer must not debit seed account.
        assert_eq!(
            nodes[0]
                .engine()
                .get(&Key::new(b"acct".as_slice()))
                .unwrap()
                .unwrap()
                .as_bytes(),
            b"100"
        );
        assert!(remotes[2].orphaned_prepared().is_empty());

        for h in all_handles {
            h.abort();
        }
        for n in &nodes {
            let _ = n.close();
        }
        for root in roots {
            let _ = std::fs::remove_dir_all(root);
        }
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
