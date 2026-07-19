//! Leader-facing gRPC service for the Smart Client SDK.
//!
//! Followers reject mutating RPCs with [`TakyonicError::NotLeader`] encoded as
//! a tonic `FailedPrecondition` status plus `x-takyonic-leader-address` metadata.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::Mutex as AsyncMutex;
use tonic::{Request, Response, Status, metadata::MetadataValue};

use crate::consensus::{RaftConsensus, Role};
use crate::engine::TakyonicEngine;
use crate::error::TakyonicError;
use crate::network::proto::client_service_server::ClientService;
use crate::network::proto::{
    BeginTxnRequest, BeginTxnResponse, ExecuteQueryRequest, ExecuteQueryResponse, FilterPred,
    IndexDefMsg, KvGetRequest, KvGetResponse, KvPutRequest, KvPutResponse, PingRequest,
    PingResponse, RegisterTableRequest, RegisterTableResponse, TxnAbortRequest, TxnAbortResponse,
    TxnCommitRequest, TxnCommitResponse, TxnDeleteRecordRequest, TxnDeleteRecordResponse,
    TxnGetRequest, TxnGetResponse, TxnPutRecordRequest, TxnPutRecordResponse, TxnPutRequest,
    TxnPutResponse,
};
use crate::raft::RaftCommand;
use crate::schema::{IndexDef, Record, TableSchema, data_key, index_key};
use crate::txn::{StatsEdit, WriteOp, index_store_value};
use crate::types::{CommitTs, Key, Value};

/// Metadata key carrying the advertised leader address on NotLeader redirects.
pub const LEADER_ADDR_META: &str = "x-takyonic-leader-address";

/// gRPC status message for NotLeader redirects (stable for client parsing).
pub const NOT_LEADER_MSG: &str = "not_leader";

struct Session {
    read_ts: CommitTs,
    reads: BTreeMap<Key, CommitTs>,
    writes: BTreeMap<Key, WriteOp>,
    stats_edits: Vec<StatsEdit>,
}

/// Client KV / transaction service backed by engine + consensus.
pub struct ClientGrpcService {
    id: u64,
    addr: SocketAddr,
    engine: Arc<TakyonicEngine>,
    raft: Arc<RaftConsensus>,
    sessions: Mutex<BTreeMap<u64, Session>>,
    /// Serializes OCC validate with networked Raft propose.
    commit_mu: AsyncMutex<()>,
}

// The generated tonic boundary requires `tonic::Status` as the error type.
#[allow(clippy::result_large_err)]
impl ClientGrpcService {
    /// Construct a client service for one cluster node.
    pub fn new(
        id: u64,
        addr: SocketAddr,
        engine: Arc<TakyonicEngine>,
        raft: Arc<RaftConsensus>,
    ) -> Self {
        Self {
            id,
            addr,
            engine,
            raft,
            sessions: Mutex::new(BTreeMap::new()),
            commit_mu: AsyncMutex::new(()),
        }
    }

    fn leader_hint(&self) -> Option<String> {
        let id = self.raft.leader_id()?;
        self.raft.membership().address(id).map(str::to_string)
    }

    fn not_leader_status(&self) -> Status {
        let mut status = Status::failed_precondition(NOT_LEADER_MSG);
        if let Some(addr) = self.leader_hint() {
            if let Ok(v) = MetadataValue::try_from(addr.as_str()) {
                status.metadata_mut().insert(LEADER_ADDR_META, v);
            }
        }
        status
    }

    fn require_leader(&self) -> std::result::Result<(), Status> {
        if self.raft.role() == Role::Leader {
            Ok(())
        } else {
            Err(self.not_leader_status())
        }
    }

    fn map_err(e: TakyonicError) -> Status {
        match e {
            TakyonicError::Conflict(msg) => Status::aborted(format!("conflict:{msg}")),
            TakyonicError::NotLeader { leader_address } => {
                let mut status = Status::failed_precondition(NOT_LEADER_MSG);
                if let Some(addr) = leader_address {
                    if let Ok(v) = MetadataValue::try_from(addr.as_str()) {
                        status.metadata_mut().insert(LEADER_ADDR_META, v);
                    }
                }
                status
            }
            other => Status::internal(other.to_string()),
        }
    }

    fn is_leadership_error(e: &TakyonicError) -> bool {
        match e {
            TakyonicError::NotLeader { .. } => true,
            TakyonicError::Raft(m) => {
                m.contains("not leader")
                    || m.contains("leadership lost")
                    || m.contains("lost leadership")
            }
            _ => false,
        }
    }

    fn session_get(
        &self,
        session: &mut Session,
        key: Key,
    ) -> std::result::Result<Option<Value>, Status> {
        if let Some(op) = session.writes.get(&key) {
            return Ok(match op {
                WriteOp::Put(v) => Some(v.clone()),
                WriteOp::Delete => None,
            });
        }
        let (value, seen_ts) = self
            .engine
            .get_at_with_ts(&key, session.read_ts)
            .map_err(Self::map_err)?;
        session.reads.entry(key).or_insert(seen_ts);
        Ok(value)
    }

    fn session_put(&self, session: &mut Session, key: Key, value: Value) -> Result<(), Status> {
        if !session.reads.contains_key(&key) {
            let (_, seen_ts) = self
                .engine
                .get_at_with_ts(&key, session.read_ts)
                .map_err(Self::map_err)?;
            session.reads.insert(key.clone(), seen_ts);
        }
        session.writes.insert(key, WriteOp::Put(value));
        Ok(())
    }

    fn session_delete(&self, session: &mut Session, key: Key) -> Result<(), Status> {
        if !session.reads.contains_key(&key) {
            let (_, seen_ts) = self
                .engine
                .get_at_with_ts(&key, session.read_ts)
                .map_err(Self::map_err)?;
            session.reads.insert(key.clone(), seen_ts);
        }
        session.writes.insert(key, WriteOp::Delete);
        Ok(())
    }

    fn session_put_record(
        &self,
        session: &mut Session,
        table: &str,
        record: Record,
    ) -> Result<(), Status> {
        let schema = self.engine.table_schema(table).map_err(Self::map_err)?;
        let pk = record
            .get(&schema.primary_key)
            .ok_or_else(|| {
                Status::invalid_argument(format!(
                    "record missing primary key field `{}`",
                    schema.primary_key
                ))
            })?
            .to_string();

        let dkey = data_key(table, &pk);
        if let Some(old_val) = self.session_get(session, dkey.clone())? {
            let old = Record::decode(&old_val).map_err(Self::map_err)?;
            let mut old_idx = Vec::new();
            for idx in &schema.indexes {
                if let Some(v) = old.get(&idx.column) {
                    let encoded = index_store_value(v);
                    self.session_delete(session, index_key(table, &idx.name, &encoded, &pk))?;
                    old_idx.push((idx.name.clone(), encoded));
                }
            }
            if !old_idx.is_empty() {
                session.stats_edits.push(StatsEdit::Delete {
                    table: table.to_string(),
                    index_values: old_idx,
                });
            }
        }

        self.session_put(session, dkey, record.encode())?;
        let mut new_idx = Vec::new();
        for idx in &schema.indexes {
            let v = record.get(&idx.column).ok_or_else(|| {
                Status::invalid_argument(format!("record missing indexed field `{}`", idx.column))
            })?;
            let encoded = index_store_value(v);
            self.session_put(
                session,
                index_key(table, &idx.name, &encoded, &pk),
                Value::new(&b""[..]),
            )?;
            new_idx.push((idx.name.clone(), encoded));
        }
        session.stats_edits.push(StatsEdit::Insert {
            table: table.to_string(),
            index_values: new_idx,
        });
        Ok(())
    }

    fn session_delete_record(
        &self,
        session: &mut Session,
        table: &str,
        pk: &str,
    ) -> Result<(), Status> {
        let schema = self.engine.table_schema(table).map_err(Self::map_err)?;
        let dkey = data_key(table, pk);
        let Some(old_val) = self.session_get(session, dkey.clone())? else {
            return Ok(());
        };
        let old = Record::decode(&old_val).map_err(Self::map_err)?;
        let mut old_idx = Vec::new();
        for idx in &schema.indexes {
            if idx.is_vector() {
                continue;
            }
            if let Some(v) = old.get(&idx.column) {
                let encoded = index_store_value(v);
                self.session_delete(session, index_key(table, &idx.name, &encoded, pk))?;
                old_idx.push((idx.name.clone(), encoded));
            }
        }
        self.session_delete(session, dkey)?;
        session.stats_edits.push(StatsEdit::Delete {
            table: table.to_string(),
            index_values: old_idx,
        });
        Ok(())
    }
}

#[tonic::async_trait]
impl ClientService for ClientGrpcService {
    async fn ping(
        &self,
        _request: Request<PingRequest>,
    ) -> std::result::Result<Response<PingResponse>, Status> {
        let is_leader = self.raft.role() == Role::Leader;
        let leader_id = self.raft.leader_id().unwrap_or(0);
        let leader_address = if is_leader {
            self.addr.to_string()
        } else {
            self.leader_hint().unwrap_or_default()
        };
        Ok(Response::new(PingResponse {
            node_id: self.id,
            self_address: self.addr.to_string(),
            is_leader,
            leader_id,
            leader_address,
        }))
    }

    async fn get(
        &self,
        request: Request<KvGetRequest>,
    ) -> std::result::Result<Response<KvGetResponse>, Status> {
        self.require_leader()?;
        let key = Key::new(request.into_inner().key);
        match self.engine.get(&key) {
            Ok(Some(v)) => Ok(Response::new(KvGetResponse {
                found: true,
                value: v.as_bytes().to_vec(),
            })),
            Ok(None) => Ok(Response::new(KvGetResponse {
                found: false,
                value: Vec::new(),
            })),
            Err(e) => Err(Self::map_err(e)),
        }
    }

    async fn put(
        &self,
        request: Request<KvPutRequest>,
    ) -> std::result::Result<Response<KvPutResponse>, Status> {
        self.require_leader()?;
        let req = request.into_inner();
        let commit_ts = self
            .raft
            .propose(RaftCommand::put(Key::new(req.key), Value::new(req.value)))
            .await
            .map_err(|e| {
                if Self::is_leadership_error(&e) {
                    self.not_leader_status()
                } else {
                    Self::map_err(e)
                }
            })?;
        Ok(Response::new(KvPutResponse { commit_ts }))
    }

    async fn begin_txn(
        &self,
        _request: Request<BeginTxnRequest>,
    ) -> std::result::Result<Response<BeginTxnResponse>, Status> {
        self.require_leader()?;
        let (txn_id, read_ts) = self.engine.begin_txn_id().map_err(Self::map_err)?;
        self.sessions.lock().insert(
            txn_id,
            Session {
                read_ts,
                reads: BTreeMap::new(),
                writes: BTreeMap::new(),
                stats_edits: Vec::new(),
            },
        );
        Ok(Response::new(BeginTxnResponse { txn_id, read_ts }))
    }

    async fn txn_get(
        &self,
        request: Request<TxnGetRequest>,
    ) -> std::result::Result<Response<TxnGetResponse>, Status> {
        self.require_leader()?;
        let req = request.into_inner();
        let key = Key::new(req.key);
        let mut sessions = self.sessions.lock();
        let session = sessions
            .get_mut(&req.txn_id)
            .ok_or_else(|| Status::not_found(format!("unknown txn {}", req.txn_id)))?;

        match self.session_get(session, key)? {
            Some(v) => Ok(Response::new(TxnGetResponse {
                found: true,
                value: v.as_bytes().to_vec(),
            })),
            None => Ok(Response::new(TxnGetResponse {
                found: false,
                value: Vec::new(),
            })),
        }
    }

    async fn txn_put(
        &self,
        request: Request<TxnPutRequest>,
    ) -> std::result::Result<Response<TxnPutResponse>, Status> {
        self.require_leader()?;
        let req = request.into_inner();
        let mut sessions = self.sessions.lock();
        let session = sessions
            .get_mut(&req.txn_id)
            .ok_or_else(|| Status::not_found(format!("unknown txn {}", req.txn_id)))?;
        self.session_put(session, Key::new(req.key), Value::new(req.value))?;
        Ok(Response::new(TxnPutResponse {}))
    }

    async fn txn_put_record(
        &self,
        request: Request<TxnPutRecordRequest>,
    ) -> std::result::Result<Response<TxnPutRecordResponse>, Status> {
        self.require_leader()?;
        let req = request.into_inner();
        let record = Record::decode(&Value::new(req.record)).map_err(Self::map_err)?;
        let mut sessions = self.sessions.lock();
        let session = sessions
            .get_mut(&req.txn_id)
            .ok_or_else(|| Status::not_found(format!("unknown txn {}", req.txn_id)))?;
        self.session_put_record(session, &req.table, record)?;
        Ok(Response::new(TxnPutRecordResponse {}))
    }

    async fn txn_delete_record(
        &self,
        request: Request<TxnDeleteRecordRequest>,
    ) -> std::result::Result<Response<TxnDeleteRecordResponse>, Status> {
        self.require_leader()?;
        let req = request.into_inner();
        let mut sessions = self.sessions.lock();
        let session = sessions
            .get_mut(&req.txn_id)
            .ok_or_else(|| Status::not_found(format!("unknown txn {}", req.txn_id)))?;
        self.session_delete_record(session, &req.table, &req.pk)?;
        Ok(Response::new(TxnDeleteRecordResponse {}))
    }

    async fn txn_commit(
        &self,
        request: Request<TxnCommitRequest>,
    ) -> std::result::Result<Response<TxnCommitResponse>, Status> {
        self.require_leader()?;
        let txn_id = request.into_inner().txn_id;
        let session = self
            .sessions
            .lock()
            .remove(&txn_id)
            .ok_or_else(|| Status::not_found(format!("unknown txn {txn_id}")))?;

        if session.writes.is_empty() {
            self.engine.end_transaction(txn_id);
            return Ok(Response::new(TxnCommitResponse {
                commit_ts: session.read_ts,
            }));
        }

        let _guard = self.commit_mu.lock().await;
        if self.raft.role() != Role::Leader {
            self.engine.end_transaction(txn_id);
            return Err(self.not_leader_status());
        }

        let ops = match self.engine.prepare_txn_commit(
            txn_id,
            session.read_ts,
            &session.reads,
            &session.writes,
        ) {
            Ok(ops) => ops,
            Err(e) => return Err(Self::map_err(e)),
        };

        // WAL-before-data (ARIES): Commit must be durable before Raft apply.
        if let Err(e) = self.engine.log_txn_wal(txn_id, &session.writes) {
            self.engine.end_transaction(txn_id);
            return Err(Self::map_err(e));
        }

        match self.raft.propose(RaftCommand::txn_batch(ops)).await {
            Ok(commit_ts) => {
                self.engine.finalize_txn_commit(
                    txn_id,
                    commit_ts,
                    &session.writes,
                    &session.stats_edits,
                );
                Ok(Response::new(TxnCommitResponse { commit_ts }))
            }
            Err(e) => {
                self.engine.end_transaction(txn_id);
                if Self::is_leadership_error(&e) {
                    Err(self.not_leader_status())
                } else {
                    Err(Self::map_err(e))
                }
            }
        }
    }

    async fn txn_abort(
        &self,
        request: Request<TxnAbortRequest>,
    ) -> std::result::Result<Response<TxnAbortResponse>, Status> {
        let txn_id = request.into_inner().txn_id;
        if self.sessions.lock().remove(&txn_id).is_some() {
            self.engine.end_transaction(txn_id);
        }
        Ok(Response::new(TxnAbortResponse {}))
    }

    async fn register_table(
        &self,
        request: Request<RegisterTableRequest>,
    ) -> std::result::Result<Response<RegisterTableResponse>, Status> {
        // Schema is local metadata — allow on any node so followers stay ready
        // for leadership. Stats catalog still lives per-node.
        let req = request.into_inner();
        let indexes = req
            .indexes
            .into_iter()
            .map(|IndexDefMsg { name, column }| IndexDef::new(name, column))
            .collect();
        self.engine
            .register_table(TableSchema::new(req.name, req.primary_key, indexes))
            .map_err(Self::map_err)?;
        Ok(Response::new(RegisterTableResponse {}))
    }

    async fn execute_query(
        &self,
        request: Request<ExecuteQueryRequest>,
    ) -> std::result::Result<Response<ExecuteQueryResponse>, Status> {
        self.require_leader()?;
        let req = request.into_inner();
        let mut query = self.engine.query(req.table);
        for FilterPred { column, op, value } in req.filters {
            query = query.filter(column, &op, value).map_err(Self::map_err)?;
        }
        let explain = query.explain().map_err(Self::map_err)?;
        let records = query.execute().map_err(Self::map_err)?;
        Ok(Response::new(ExecuteQueryResponse {
            records: records
                .into_iter()
                .map(|r| r.encode().as_bytes().to_vec())
                .collect(),
            explain,
        }))
    }
}

/// Parse a tonic [`Status`] into a [`TakyonicError`], extracting NotLeader hints.
pub fn status_to_error(status: Status) -> TakyonicError {
    if status.code() == tonic::Code::FailedPrecondition
        && status.message().starts_with(NOT_LEADER_MSG)
    {
        let leader_address = status
            .metadata()
            .get(LEADER_ADDR_META)
            .and_then(|v| v.to_str().ok())
            .filter(|s| !s.is_empty())
            .map(str::to_string);
        return TakyonicError::NotLeader { leader_address };
    }
    if status.code() == tonic::Code::Aborted && status.message().starts_with("conflict:") {
        return TakyonicError::Conflict(status.message().trim_start_matches("conflict:").into());
    }
    TakyonicError::Network(status.to_string())
}
