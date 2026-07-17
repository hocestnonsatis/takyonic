//! gRPC (tonic) transport for Raft RPCs.
//!
//! Maps protobuf `bytes` fields onto the `bytes` crate for zero-copy handoff
//! at the network boundary. The server delegates to [`RaftConsensus`]; the
//! client helpers are used by the cluster replication / election loops.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status, transport::Channel};
use tracing::{debug, warn};

use crate::client_service::ClientGrpcService;
use crate::consensus::RaftConsensus;
use crate::engine::TakyonicEngine;
use crate::error::{Result, TakyonicError};
use crate::raft_log::RaftLogEntry;

/// Generated prost/tonic bindings for `takyonic.raft.v1`.
pub mod proto {
    #![allow(missing_docs)]
    tonic::include_proto!("takyonic.raft.v1");
}

use proto::client_service_server::ClientServiceServer;
use proto::raft_service_client::RaftServiceClient;
use proto::raft_service_server::{RaftService, RaftServiceServer};
use proto::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    LogEntry, RequestVoteRequest, RequestVoteResponse,
};

/// gRPC service implementation backed by shared consensus state.
pub struct RaftGrpcService {
    raft: Arc<RaftConsensus>,
}

impl RaftGrpcService {
    /// Wrap a consensus node.
    pub fn new(raft: Arc<RaftConsensus>) -> Self {
        Self { raft }
    }
}

#[tonic::async_trait]
impl RaftService for RaftGrpcService {
    async fn append_entries(
        &self,
        request: Request<AppendEntriesRequest>,
    ) -> std::result::Result<Response<AppendEntriesResponse>, Status> {
        let req = request.into_inner();
        let entries = req
            .entries
            .into_iter()
            .map(|e| RaftLogEntry::new(e.term, e.index, Bytes::from(e.command)))
            .collect();
        let (term, success, match_index) = self.raft.handle_append_entries(
            req.term,
            req.leader_id,
            req.prev_log_index,
            req.prev_log_term,
            entries,
            req.leader_commit,
        );
        Ok(Response::new(AppendEntriesResponse {
            term,
            success,
            match_index,
        }))
    }

    async fn request_vote(
        &self,
        request: Request<RequestVoteRequest>,
    ) -> std::result::Result<Response<RequestVoteResponse>, Status> {
        let req = request.into_inner();
        let (term, vote_granted) = self.raft.handle_request_vote(
            req.term,
            req.candidate_id,
            req.last_log_index,
            req.last_log_term,
        );
        Ok(Response::new(RequestVoteResponse { term, vote_granted }))
    }

    async fn install_snapshot(
        &self,
        request: Request<InstallSnapshotRequest>,
    ) -> std::result::Result<Response<InstallSnapshotResponse>, Status> {
        let req = request.into_inner();
        let term = self
            .raft
            .handle_install_snapshot(
                req.term,
                req.leader_id,
                req.last_included_index,
                req.last_included_term,
                req.data.into(),
                req.done,
            )
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(InstallSnapshotResponse { term }))
    }
}

/// Outbound peer connections keyed by node id.
pub struct PeerClients {
    endpoints: RwLock<HashMap<u64, String>>,
    clients: RwLock<HashMap<u64, RaftServiceClient<Channel>>>,
}

impl PeerClients {
    /// Create with advertised `id -> host:port` map (excluding self).
    pub fn new(endpoints: HashMap<u64, String>) -> Self {
        Self {
            endpoints: RwLock::new(endpoints),
            clients: RwLock::new(HashMap::new()),
        }
    }

    /// Upsert a peer endpoint (AddNode / membership sync).
    pub async fn upsert_endpoint(&self, id: u64, address: String) {
        let mut eps = self.endpoints.write().await;
        if eps.get(&id) != Some(&address) {
            eps.insert(id, address);
            drop(eps);
            self.clients.write().await.remove(&id);
        }
    }

    /// Drop a peer endpoint and cached channel (RemoveNode).
    pub async fn remove_endpoint(&self, id: u64) {
        self.endpoints.write().await.remove(&id);
        self.clients.write().await.remove(&id);
    }

    /// Sync peer endpoints from membership (excluding `self_id`).
    pub async fn sync_from_membership(
        &self,
        self_id: u64,
        membership: &crate::membership::ClusterMembership,
    ) {
        let desired: HashMap<u64, String> = membership
            .endpoints()
            .iter()
            .filter(|&(&id, _)| id != self_id)
            .map(|(&id, addr)| (id, addr.clone()))
            .collect();
        let mut eps = self.endpoints.write().await;
        let stale: Vec<u64> = eps
            .keys()
            .copied()
            .filter(|id| !desired.contains_key(id))
            .collect();
        let mut invalidate = stale.clone();
        for id in stale {
            eps.remove(&id);
        }
        for (id, addr) in desired {
            if eps.get(&id) != Some(&addr) {
                eps.insert(id, addr);
                invalidate.push(id);
            }
        }
        drop(eps);
        if !invalidate.is_empty() {
            let mut clients = self.clients.write().await;
            for id in invalidate {
                clients.remove(&id);
            }
        }
    }

    async fn client(&self, peer: u64) -> Result<RaftServiceClient<Channel>> {
        if let Some(c) = self.clients.read().await.get(&peer).cloned() {
            return Ok(c);
        }
        let addr = self
            .endpoints
            .read()
            .await
            .get(&peer)
            .cloned()
            .ok_or_else(|| TakyonicError::Network(format!("unknown peer {peer}")))?;
        let uri = format!("http://{addr}");
        let channel = Channel::from_shared(uri.clone())
            .map_err(|e| TakyonicError::Network(e.to_string()))?
            .connect_timeout(Duration::from_millis(50))
            .timeout(Duration::from_millis(500))
            .connect()
            .await
            .map_err(|e| TakyonicError::Network(format!("connect {uri}: {e}")))?;
        let client = RaftServiceClient::new(channel)
            .max_decoding_message_size(32 * 1024 * 1024)
            .max_encoding_message_size(32 * 1024 * 1024);
        self.clients.write().await.insert(peer, client.clone());
        Ok(client)
    }

    /// Send RequestVote to a peer.
    pub async fn request_vote(
        &self,
        peer: u64,
        term: u64,
        candidate_id: u64,
        last_log_index: u64,
        last_log_term: u64,
    ) -> Result<RequestVoteResponse> {
        let mut client = self.client(peer).await?;
        let resp = client
            .request_vote(Request::new(RequestVoteRequest {
                term,
                candidate_id,
                last_log_index,
                last_log_term,
            }))
            .await
            .map_err(|e| TakyonicError::Network(e.to_string()))?;
        Ok(resp.into_inner())
    }

    /// Send AppendEntries to a peer.
    #[allow(clippy::too_many_arguments)]
    pub async fn append_entries(
        &self,
        peer: u64,
        term: u64,
        leader_id: u64,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<RaftLogEntry>,
        leader_commit: u64,
    ) -> Result<AppendEntriesResponse> {
        let mut client = self.client(peer).await?;
        let entries = entries
            .into_iter()
            .map(|e| LogEntry {
                term: e.term,
                index: e.index,
                command: e.command.to_vec(),
            })
            .collect();
        let resp = client
            .append_entries(Request::new(AppendEntriesRequest {
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            }))
            .await
            .map_err(|e| TakyonicError::Network(e.to_string()))?;
        Ok(resp.into_inner())
    }

    /// Send InstallSnapshot to a peer (single blob; caller may chunk).
    #[allow(clippy::too_many_arguments)]
    pub async fn install_snapshot(
        &self,
        peer: u64,
        term: u64,
        leader_id: u64,
        last_included_index: u64,
        last_included_term: u64,
        data: Bytes,
        done: bool,
    ) -> Result<InstallSnapshotResponse> {
        let mut client = self.client(peer).await?;
        let resp = client
            .install_snapshot(Request::new(InstallSnapshotRequest {
                term,
                leader_id,
                last_included_index,
                last_included_term,
                data: data.to_vec(),
                done,
            }))
            .await
            .map_err(|e| TakyonicError::Network(e.to_string()))?;
        Ok(resp.into_inner())
    }

    /// Stream a full snapshot blob as chunked InstallSnapshot RPCs.
    pub async fn install_snapshot_chunked(
        &self,
        peer: u64,
        term: u64,
        leader_id: u64,
        last_included_index: u64,
        last_included_term: u64,
        data: Bytes,
    ) -> Result<InstallSnapshotResponse> {
        const CHUNK: usize = 256 * 1024;
        if data.is_empty() {
            return self
                .install_snapshot(
                    peer,
                    term,
                    leader_id,
                    last_included_index,
                    last_included_term,
                    Bytes::new(),
                    true,
                )
                .await;
        }
        let mut offset = 0usize;
        let mut last = InstallSnapshotResponse { term: 0 };
        while offset < data.len() {
            let end = (offset + CHUNK).min(data.len());
            let done = end == data.len();
            let chunk = data.slice(offset..end);
            last = self
                .install_snapshot(
                    peer,
                    term,
                    leader_id,
                    last_included_index,
                    last_included_term,
                    chunk,
                    done,
                )
                .await?;
            offset = end;
        }
        Ok(last)
    }
}

/// Serve the Raft gRPC API on `addr` until the process exits.
pub async fn serve_raft(addr: SocketAddr, raft: Arc<RaftConsensus>) -> Result<()> {
    let svc = RaftGrpcService::new(raft);
    debug!(%addr, "starting Raft gRPC server");
    tonic::transport::Server::builder()
        .max_frame_size(Some(1024 * 1024))
        .add_service(
            RaftServiceServer::new(svc)
                .max_decoding_message_size(32 * 1024 * 1024)
                .max_encoding_message_size(32 * 1024 * 1024),
        )
        .serve(addr)
        .await
        .map_err(|e| TakyonicError::Network(format!("serve {addr}: {e}")))
}

/// Serve Raft + ClientService on `addr` until the process exits.
pub async fn serve_node(
    addr: SocketAddr,
    id: u64,
    engine: Arc<TakyonicEngine>,
    raft: Arc<RaftConsensus>,
) -> Result<()> {
    let raft_svc = RaftGrpcService::new(Arc::clone(&raft));
    let client_svc = ClientGrpcService::new(id, addr, engine, raft);
    debug!(%addr, "starting Raft + Client gRPC server");
    tonic::transport::Server::builder()
        .max_frame_size(Some(1024 * 1024))
        .add_service(
            RaftServiceServer::new(raft_svc)
                .max_decoding_message_size(32 * 1024 * 1024)
                .max_encoding_message_size(32 * 1024 * 1024),
        )
        .add_service(
            ClientServiceServer::new(client_svc)
                .max_decoding_message_size(32 * 1024 * 1024)
                .max_encoding_message_size(32 * 1024 * 1024),
        )
        .serve(addr)
        .await
        .map_err(|e| TakyonicError::Network(format!("serve {addr}: {e}")))
}

/// Run one election round: RequestVote all peers; become leader on quorum.
pub async fn run_election(raft: &Arc<RaftConsensus>, peers: &Arc<PeerClients>) {
    let (term, last_idx, last_term) = raft.start_election();
    peers
        .sync_from_membership(raft.id(), &raft.membership())
        .await;
    let mut votes: HashMap<u64, bool> = HashMap::new();
    let peer_ids: Vec<u64> = raft.peers();

    // Sole voter: `start_election` already recorded the self-vote; with
    // quorum == 1 there are no peers to contact — promote immediately.
    if peer_ids.is_empty() && raft.membership().quorum() <= 1 {
        raft.become_leader(term);
        replicate_to_all(raft, peers).await;
        return;
    }

    let mut tasks = Vec::new();
    for peer in peer_ids {
        let peers = Arc::clone(peers);
        let raft = Arc::clone(raft);
        tasks.push(async move {
            match peers
                .request_vote(peer, term, raft.id(), last_idx, last_term)
                .await
            {
                Ok(resp) => {
                    raft.maybe_step_down(resp.term);
                    if resp.vote_granted && resp.term == term {
                        Some(peer)
                    } else {
                        None
                    }
                }
                Err(e) => {
                    warn!(peer, %e, "RequestVote RPC failed");
                    None
                }
            }
        });
    }
    let results = futures::future::join_all(tasks).await;
    for peer in results.into_iter().flatten() {
        if raft.record_vote_granted(term, peer, &mut votes) {
            raft.become_leader(term);
            // Immediate empty heartbeat to assert leadership.
            replicate_to_all(raft, peers).await;
            return;
        }
    }
}

/// Leader: flush parked proposes, then AppendEntries or InstallSnapshot per peer.
pub async fn replicate_to_all(raft: &Arc<RaftConsensus>, peers: &Arc<PeerClients>) {
    if let Err(e) = raft.flush_pending_proposes() {
        warn!(%e, "leader propose batch flush failed");
    }
    peers
        .sync_from_membership(raft.id(), &raft.membership())
        .await;
    let peer_ids: Vec<u64> = raft.peers();
    let mut tasks = Vec::new();
    for peer in peer_ids {
        let peers = Arc::clone(peers);
        let raft = Arc::clone(raft);
        tasks.push(async move {
            if raft.peer_needs_snapshot(peer) {
                match raft.build_install_snapshot() {
                    Ok((term, leader_id, last_idx, last_term, data)) => {
                        match peers
                            .install_snapshot_chunked(
                                peer, term, leader_id, last_idx, last_term, data,
                            )
                            .await
                        {
                            Ok(resp) => {
                                raft.maybe_step_down(resp.term);
                                if resp.term == term {
                                    raft.on_snapshot_success(peer, last_idx);
                                    info_snapshot(peer, last_idx);
                                }
                            }
                            Err(e) => {
                                warn!(peer, %e, "InstallSnapshot RPC failed");
                                raft.on_peer_unreachable(peer);
                                let mut guard = peers.clients.write().await;
                                guard.remove(&peer);
                            }
                        }
                    }
                    Err(e) => warn!(peer, %e, "build InstallSnapshot failed"),
                }
                return;
            }
            let Some((term, leader_id, prev_idx, prev_term, entries, commit)) =
                raft.leader_peer_cursor(peer)
            else {
                return;
            };
            match peers
                .append_entries(peer, term, leader_id, prev_idx, prev_term, entries, commit)
                .await
            {
                Ok(resp) => {
                    raft.maybe_step_down(resp.term);
                    if resp.success {
                        raft.on_append_success(peer, resp.match_index);
                    } else {
                        raft.on_append_failure(peer, resp.term, resp.match_index);
                    }
                }
                Err(e) => {
                    warn!(peer, %e, "AppendEntries RPC failed");
                    raft.on_peer_unreachable(peer);
                    let mut guard = peers.clients.write().await;
                    guard.remove(&peer);
                }
            }
        });
    }
    let _ = futures::future::join_all(tasks).await;
}

fn info_snapshot(peer: u64, last_idx: u64) {
    tracing::info!(
        peer,
        last_included_index = last_idx,
        "InstallSnapshot succeeded"
    );
}
