//! gRPC wire for distributed 2PC participants ([`TwopcService`]).
//!
//! Each process hosts a [`LocalShard`] behind [`TwopcGrpcService`]. The
//! coordinator talks to participants through [`RemoteShard`], which implements
//! [`ShardParticipant`] over TCP (tonic).

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use parking_lot::Mutex;
use tokio::runtime::Handle;
use tonic::transport::{Channel, Server};
use tonic::{Request, Response, Status};

use crate::dtxn::{
    DistTxnId, EngineShard, LocalShard, ShardId, ShardParticipant, WriteOp,
};
use crate::engine::TakyonicEngine;
use crate::error::{Result, TakyonicError};
use crate::network::proto::twopc_service_client::TwopcServiceClient;
use crate::network::proto::twopc_service_server::{TwopcService, TwopcServiceServer};
use crate::network::proto::{
    TwopcAbortRequest, TwopcAck, TwopcCommitRequest, TwopcGetAtRequest, TwopcGetAtResponse,
    TwopcInjectRequest, TwopcOrphanedRequest, TwopcOrphanedResponse, TwopcPrepareRequest,
    TwopcRecoverRequest, TwopcWriteOp,
};
use crate::types::{CommitTs, Key, Value};

/// gRPC service wrapping a [`ShardParticipant`] (LocalShard or Engine).
pub struct TwopcGrpcService {
    participant: Arc<dyn ShardParticipant>,
    local: Option<Arc<LocalShard>>,
    engine: Option<Arc<TakyonicEngine>>,
}

impl TwopcGrpcService {
    /// Serve an in-process [`LocalShard`] (test / standalone harness).
    pub fn new(shard: Arc<LocalShard>) -> Self {
        Self {
            participant: Arc::clone(&shard) as Arc<dyn ShardParticipant>,
            local: Some(shard),
            engine: None,
        }
    }

    /// Serve Engine-backed 2PC on the production `serve_node` path.
    pub fn from_engine(engine: Arc<TakyonicEngine>, shard_id: ShardId) -> Self {
        let shard = EngineShard::new(Arc::clone(&engine), shard_id);
        Self {
            participant: shard as Arc<dyn ShardParticipant>,
            local: None,
            engine: Some(engine),
        }
    }

    fn ack_ok() -> TwopcAck {
        TwopcAck {
            ok: true,
            error: String::new(),
        }
    }

    fn ack_err(e: TakyonicError) -> TwopcAck {
        TwopcAck {
            ok: false,
            error: e.to_string(),
        }
    }

    fn decode_writes(ops: &[TwopcWriteOp]) -> Vec<WriteOp> {
        ops.iter()
            .map(|op| {
                let key = Key::new(Bytes::copy_from_slice(&op.key));
                let val = if op.has_value {
                    Some(Value::new(Bytes::copy_from_slice(&op.value)))
                } else {
                    None
                };
                (key, val)
            })
            .collect()
    }

    fn decode_reads(reads: &[crate::network::proto::TwopcReadCheck]) -> Vec<(Key, CommitTs)> {
        reads
            .iter()
            .map(|r| {
                (
                    Key::new(Bytes::copy_from_slice(&r.key)),
                    r.observed_ts,
                )
            })
            .collect()
    }
}

#[tonic::async_trait]
impl TwopcService for TwopcGrpcService {
    async fn prepare(
        &self,
        request: Request<TwopcPrepareRequest>,
    ) -> std::result::Result<Response<TwopcAck>, Status> {
        let req = request.into_inner();
        let writes = Self::decode_writes(&req.writes);
        let reads = Self::decode_reads(&req.reads);
        let ack = match self
            .participant
            .prepare(req.txn_id, req.read_ts, &writes, &reads)
        {
            Ok(()) => Self::ack_ok(),
            Err(e) => Self::ack_err(e),
        };
        Ok(Response::new(ack))
    }

    async fn commit(
        &self,
        request: Request<TwopcCommitRequest>,
    ) -> std::result::Result<Response<TwopcAck>, Status> {
        let req = request.into_inner();
        let ack = match self.participant.commit(req.txn_id, req.commit_ts) {
            Ok(()) => Self::ack_ok(),
            Err(e) => Self::ack_err(e),
        };
        Ok(Response::new(ack))
    }

    async fn abort(
        &self,
        request: Request<TwopcAbortRequest>,
    ) -> std::result::Result<Response<TwopcAck>, Status> {
        let req = request.into_inner();
        let ack = match self.participant.abort(req.txn_id) {
            Ok(()) => Self::ack_ok(),
            Err(e) => Self::ack_err(e),
        };
        Ok(Response::new(ack))
    }

    async fn orphaned_prepared(
        &self,
        _request: Request<TwopcOrphanedRequest>,
    ) -> std::result::Result<Response<TwopcOrphanedResponse>, Status> {
        Ok(Response::new(TwopcOrphanedResponse {
            txn_ids: self.participant.orphaned_prepared(),
        }))
    }

    async fn get_at(
        &self,
        request: Request<TwopcGetAtRequest>,
    ) -> std::result::Result<Response<TwopcGetAtResponse>, Status> {
        let req = request.into_inner();
        let key = Key::new(Bytes::copy_from_slice(&req.key));
        match self.participant.get_at(&key, req.read_ts) {
            Some(v) => Ok(Response::new(TwopcGetAtResponse {
                found: true,
                value: v.as_bytes().to_vec(),
            })),
            None => Ok(Response::new(TwopcGetAtResponse {
                found: false,
                value: Vec::new(),
            })),
        }
    }

    async fn inject_prepare_failure(
        &self,
        request: Request<TwopcInjectRequest>,
    ) -> std::result::Result<Response<TwopcAck>, Status> {
        let on = request.into_inner().enabled;
        if let Some(local) = &self.local {
            local.inject_prepare_failure(on);
        } else if let Some(engine) = &self.engine {
            engine.inject_twopc_prepare_failure(on);
        }
        Ok(Response::new(Self::ack_ok()))
    }

    async fn inject_crash_after_prepare(
        &self,
        request: Request<TwopcInjectRequest>,
    ) -> std::result::Result<Response<TwopcAck>, Status> {
        let on = request.into_inner().enabled;
        if let Some(local) = &self.local {
            local.inject_crash_after_prepare(on);
        } else if let Some(engine) = &self.engine {
            engine.inject_twopc_crash_after_prepare(on);
        }
        Ok(Response::new(Self::ack_ok()))
    }

    async fn recover_from_raft_log(
        &self,
        _request: Request<TwopcRecoverRequest>,
    ) -> std::result::Result<Response<TwopcAck>, Status> {
        if let Some(local) = &self.local {
            local.recover_from_raft_log();
            return Ok(Response::new(Self::ack_ok()));
        }
        if let Some(engine) = &self.engine {
            return match engine.twopc_recover_from_raft_log() {
                Ok(()) => Ok(Response::new(Self::ack_ok())),
                Err(e) => Ok(Response::new(Self::ack_err(e))),
            };
        }
        Ok(Response::new(Self::ack_ok()))
    }
}

/// Serve a single 2PC shard on an already-bound Tokio listener.
pub async fn serve_twopc_shard_listener(
    listener: tokio::net::TcpListener,
    shard: Arc<LocalShard>,
) -> Result<()> {
    Server::builder()
        .add_service(TwopcServiceServer::new(TwopcGrpcService::new(shard)))
        .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
        .await
        .map_err(|e| TakyonicError::Network(format!("twopc serve: {e}")))
}

/// Bind + serve on `addr` (prefer [`serve_twopc_shard_listener`] in tests to
/// avoid the ephemeral bind/drop race).
pub async fn serve_twopc_shard(addr: SocketAddr, shard: Arc<LocalShard>) -> Result<()> {
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| TakyonicError::Network(format!("twopc bind {addr}: {e}")))?;
    serve_twopc_shard_listener(listener, shard).await
}

/// TCP-backed [`ShardParticipant`] for the coordinator.
pub struct RemoteShard {
    id: ShardId,
    client: Mutex<TwopcServiceClient<Channel>>,
}

impl RemoteShard {
    /// Connect to a running [`TwopcGrpcService`].
    pub async fn connect(id: ShardId, addr: SocketAddr) -> Result<Arc<Self>> {
        let endpoint = format!("http://{addr}");
        let mut last_err = None;
        for _ in 0..50 {
            match TwopcServiceClient::connect(endpoint.clone()).await {
                Ok(client) => {
                    return Ok(Arc::new(Self {
                        id,
                        client: Mutex::new(client),
                    }));
                }
                Err(e) => {
                    last_err = Some(e.to_string());
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }
        }
        Err(TakyonicError::Network(format!(
            "twopc connect {endpoint}: {}",
            last_err.unwrap_or_else(|| "unknown".into())
        )))
    }

    fn encode_writes(writes: &[WriteOp]) -> Vec<TwopcWriteOp> {
        writes
            .iter()
            .map(|(k, v)| TwopcWriteOp {
                key: k.as_bytes().to_vec(),
                has_value: v.is_some(),
                value: v
                    .as_ref()
                    .map(|x| x.as_bytes().to_vec())
                    .unwrap_or_default(),
            })
            .collect()
    }

    fn map_ack(ack: TwopcAck) -> Result<()> {
        if ack.ok {
            Ok(())
        } else {
            Err(TakyonicError::Network(ack.error))
        }
    }

    fn block_on<F, T>(&self, fut: F) -> Result<T>
    where
        F: std::future::Future<Output = Result<T>>,
    {
        match Handle::try_current() {
            Ok(handle) => tokio::task::block_in_place(|| handle.block_on(fut)),
            Err(_) => {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|e| TakyonicError::Network(format!("twopc runtime: {e}")))?;
                rt.block_on(fut)
            }
        }
    }

    /// Chaos: next prepare fails with a network error.
    pub fn inject_prepare_failure(&self, on: bool) -> Result<()> {
        self.block_on(async {
            let mut c = self.client.lock().clone();
            let resp = c
                .inject_prepare_failure(TwopcInjectRequest { enabled: on })
                .await
                .map_err(|e| TakyonicError::Network(e.to_string()))?
                .into_inner();
            Self::map_ack(resp)
        })
    }

    /// Chaos: durable PREPARE then fail the ACK (and subsequent commit/abort).
    pub fn inject_crash_after_prepare(&self, on: bool) -> Result<()> {
        self.block_on(async {
            let mut c = self.client.lock().clone();
            let resp = c
                .inject_crash_after_prepare(TwopcInjectRequest { enabled: on })
                .await
                .map_err(|e| TakyonicError::Network(e.to_string()))?
                .into_inner();
            Self::map_ack(resp)
        })
    }

    /// Simulate process restart: rebuild prepared set from the Raft stand-in log.
    pub fn recover_from_raft_log(&self) -> Result<()> {
        self.block_on(async {
            let mut c = self.client.lock().clone();
            let resp = c
                .recover_from_raft_log(TwopcRecoverRequest {})
                .await
                .map_err(|e| TakyonicError::Network(e.to_string()))?
                .into_inner();
            Self::map_ack(resp)
        })
    }
}

impl ShardParticipant for RemoteShard {
    fn shard_id(&self) -> ShardId {
        self.id
    }

    fn prepare(
        &self,
        txn_id: DistTxnId,
        read_ts: CommitTs,
        writes: &[WriteOp],
        reads: &[(Key, CommitTs)],
    ) -> Result<()> {
        let writes = Self::encode_writes(writes);
        let reads: Vec<_> = reads
            .iter()
            .map(|(k, ts)| crate::network::proto::TwopcReadCheck {
                key: k.as_bytes().to_vec(),
                observed_ts: *ts,
            })
            .collect();
        self.block_on(async {
            let mut c = self.client.lock().clone();
            let resp = c
                .prepare(TwopcPrepareRequest {
                    txn_id,
                    read_ts,
                    writes,
                    reads,
                })
                .await
                .map_err(|e| TakyonicError::Network(e.to_string()))?
                .into_inner();
            Self::map_ack(resp)
        })
    }

    fn commit(&self, txn_id: DistTxnId, commit_ts: CommitTs) -> Result<()> {
        self.block_on(async {
            let mut c = self.client.lock().clone();
            let resp = c
                .commit(TwopcCommitRequest { txn_id, commit_ts })
                .await
                .map_err(|e| TakyonicError::Network(e.to_string()))?
                .into_inner();
            Self::map_ack(resp)
        })
    }

    fn abort(&self, txn_id: DistTxnId) -> Result<()> {
        self.block_on(async {
            let mut c = self.client.lock().clone();
            let resp = c
                .abort(TwopcAbortRequest { txn_id })
                .await
                .map_err(|e| TakyonicError::Network(e.to_string()))?
                .into_inner();
            Self::map_ack(resp)
        })
    }

    fn orphaned_prepared(&self) -> Vec<DistTxnId> {
        self.block_on(async {
            let mut c = self.client.lock().clone();
            let resp = c
                .orphaned_prepared(TwopcOrphanedRequest {})
                .await
                .map_err(|e| TakyonicError::Network(e.to_string()))?
                .into_inner();
            Ok(resp.txn_ids)
        })
        .unwrap_or_default()
    }

    fn get_at(&self, key: &Key, read_ts: CommitTs) -> Option<Value> {
        self.block_on(async {
            let mut c = self.client.lock().clone();
            let resp = c
                .get_at(TwopcGetAtRequest {
                    key: key.as_bytes().to_vec(),
                    read_ts,
                })
                .await
                .map_err(|e| TakyonicError::Network(e.to_string()))?
                .into_inner();
            if resp.found {
                Ok(Some(Value::new(Bytes::from(resp.value))))
            } else {
                Ok(None)
            }
        })
        .ok()
        .flatten()
    }
}

/// Bind an ephemeral localhost port and keep the listener (avoids TOCTOU).
pub async fn bind_ephemeral() -> Result<(SocketAddr, tokio::net::TcpListener)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|e| TakyonicError::Network(format!("bind: {e}")))?;
    let addr = listener
        .local_addr()
        .map_err(|e| TakyonicError::Network(format!("local_addr: {e}")))?;
    Ok((addr, listener))
}

/// Bind an ephemeral localhost port (std); prefer [`bind_ephemeral`] for servers.
pub fn ephemeral_addr() -> Result<SocketAddr> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")
        .map_err(|e| TakyonicError::Network(format!("bind: {e}")))?;
    let addr = listener
        .local_addr()
        .map_err(|e| TakyonicError::Network(format!("local_addr: {e}")))?;
    drop(listener);
    Ok(addr)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dtxn::{DistTxnOutcome, DistTxnRequest, TransactionCoordinator, put_branch};
    use crate::types::{Key, Value};

    fn key(s: &str) -> Key {
        Key::new(s.as_bytes().to_vec())
    }
    fn val(s: &str) -> Value {
        Value::new(s.as_bytes().to_vec())
    }

    struct Cluster {
        remotes: Vec<Arc<RemoteShard>>,
        abort_handles: Vec<tokio::task::AbortHandle>,
    }

    impl Drop for Cluster {
        fn drop(&mut self) {
            for h in &self.abort_handles {
                h.abort();
            }
        }
    }

    async fn start_cluster() -> Cluster {
        let mut remotes = Vec::new();
        let mut abort_handles = Vec::new();
        for id in 1u64..=3 {
            let (addr, listener) = bind_ephemeral().await.unwrap();
            let local = LocalShard::new(id);
            let handle = tokio::spawn(async move {
                let _ = serve_twopc_shard_listener(listener, local).await;
            });
            abort_handles.push(handle.abort_handle());
            let remote = RemoteShard::connect(id, addr).await.unwrap();
            remotes.push(remote);
        }
        Cluster {
            remotes,
            abort_handles,
        }
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn three_node_wire_cross_shard_commit() {
        let cluster = start_cluster().await;
        let remotes = cluster.remotes.clone();
        let outcome = tokio::task::spawn_blocking(move || {
            let tc = TransactionCoordinator::new(None);
            for r in &remotes {
                tc.register_shard(Arc::clone(r) as Arc<dyn ShardParticipant>);
            }
            tc.execute(DistTxnRequest {
                read_ts: 0,
                branches: vec![
                    put_branch(1, key("a"), val("1")),
                    put_branch(2, key("b"), val("2")),
                    put_branch(3, key("c"), val("3")),
                ],
            })
        })
        .await
        .unwrap()
        .unwrap();
        assert!(matches!(outcome, DistTxnOutcome::Committed { .. }));
        assert_eq!(
            cluster.remotes[0].get(&key("a")).unwrap().as_bytes(),
            b"1"
        );
        assert_eq!(
            cluster.remotes[1].get(&key("b")).unwrap().as_bytes(),
            b"2"
        );
        assert_eq!(
            cluster.remotes[2].get(&key("c")).unwrap().as_bytes(),
            b"3"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn three_node_wire_prepare_failure_aborts() {
        let cluster = start_cluster().await;
        let remotes = cluster.remotes.clone();
        // Seed values.
        {
            let r = remotes.clone();
            tokio::task::spawn_blocking(move || {
                let tc = TransactionCoordinator::new(None);
                for x in &r {
                    tc.register_shard(Arc::clone(x) as Arc<dyn ShardParticipant>);
                }
                tc.execute(DistTxnRequest {
                    read_ts: 0,
                    branches: vec![
                        put_branch(1, key("a"), val("100")),
                        put_branch(2, key("b"), val("100")),
                        put_branch(3, key("c"), val("100")),
                    ],
                })
                .unwrap();
            })
            .await
            .unwrap();
        }
        remotes[2].inject_prepare_failure(true).unwrap();
        let remotes2 = remotes.clone();
        let outcome = tokio::task::spawn_blocking(move || {
            let tc = TransactionCoordinator::new(None);
            for x in &remotes2 {
                tc.register_shard(Arc::clone(x) as Arc<dyn ShardParticipant>);
            }
            tc.execute(DistTxnRequest {
                read_ts: 0,
                branches: vec![
                    put_branch(1, key("a"), val("90")),
                    put_branch(2, key("b"), val("110")),
                    put_branch(3, key("c"), val("110")),
                ],
            })
        })
        .await
        .unwrap()
        .unwrap();
        assert!(matches!(outcome, DistTxnOutcome::Aborted { .. }));
        assert_eq!(cluster.remotes[0].get(&key("a")).unwrap().as_bytes(), b"100");
        assert_eq!(cluster.remotes[1].get(&key("b")).unwrap().as_bytes(), b"100");
        assert_eq!(cluster.remotes[2].get(&key("c")).unwrap().as_bytes(), b"100");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn three_node_wire_crash_after_prepare_recovers() {
        let cluster = start_cluster().await;
        let remotes = cluster.remotes.clone();
        let tc = TransactionCoordinator::new(None);
        for x in &remotes {
            tc.register_shard(Arc::clone(x) as Arc<dyn ShardParticipant>);
        }
        {
            let tc = Arc::clone(&tc);
            tokio::task::spawn_blocking(move || {
                tc.execute(DistTxnRequest {
                    read_ts: 0,
                    branches: vec![put_branch(1, key("x"), val("1"))],
                })
                .unwrap();
            })
            .await
            .unwrap();
        }

        remotes[2].inject_crash_after_prepare(true).unwrap();
        let tc2 = Arc::clone(&tc);
        let outcome = tokio::task::spawn_blocking(move || {
            tc2.execute(DistTxnRequest {
                read_ts: 0,
                branches: vec![
                    put_branch(1, key("x"), val("2")),
                    put_branch(2, key("y"), val("9")),
                    put_branch(3, key("z"), val("9")),
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
                    "unexpected abort reason: {reason}"
                );
            }
            other => panic!("expected Aborted, got {other:?}"),
        }

        let orphans_while_down = remotes[2].orphaned_prepared();
        assert!(
            !orphans_while_down.is_empty(),
            "shard 3 must have PREPARED in memory while still 'crashed'; got {orphans_while_down:?}"
        );

        remotes[2].inject_crash_after_prepare(false).unwrap();
        remotes[2].recover_from_raft_log().unwrap();
        let orphans = remotes[2].orphaned_prepared();
        assert!(
            !orphans.is_empty(),
            "shard 3 must retain orphaned PREPARED after recover_from_raft_log; got {orphans:?}"
        );
        let tc3 = Arc::clone(&tc);
        let remote3 = Arc::clone(&remotes[2]);
        let n = tokio::task::spawn_blocking(move || tc3.recover_participant(remote3.as_ref()))
            .await
            .unwrap()
            .unwrap();
        assert!(n >= 1);
        assert!(cluster.remotes[2].get(&key("z")).is_none());
        assert_eq!(cluster.remotes[0].get(&key("x")).unwrap().as_bytes(), b"1");
    }
}
