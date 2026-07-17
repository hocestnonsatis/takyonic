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
        let addr = self.addr;
        let server = tokio::spawn(async move {
            if let Err(e) = network::serve_raft(addr, raft).await {
                tracing::error!(%e, "Raft gRPC server exited");
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
                if let Some(l) = nodes.iter().find(|x| x.id() == leader)
                    && l.role() == Role::Leader
                    && !l.raft().is_removed()
                {
                    return Ok(leader);
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
