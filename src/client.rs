//! Smart Client SDK — topology-aware routing + OCC retry middleware.
//!
//! [`TakyonicClient`] discovers the Raft leader from seed nodes, pools a tonic
//! channel to it, transparently redirects on [`TakyonicError::NotLeader`], and
//! re-executes [`TakyonicClient::execute_txn`] closures on OCC conflicts with
//! exponential backoff + jitter.
//!
//! Step 18 adds [`TakyonicClient::execute_sql`]: parse → CBO plan / MVCC
//! `put_record` → leader execution with the same OCC / NotLeader retries.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use parking_lot::RwLock as SyncRwLock;
use tokio::sync::RwLock;
use tonic::Request;
use tonic::transport::Channel;
use tracing::{debug, warn};

use crate::client_service::status_to_error;
use crate::error::{Result, TakyonicError};
use crate::executor::{self, ExecutionContext, value_to_field};
use crate::network::proto::client_service_client::ClientServiceClient;
use crate::network::proto::{
    BeginTxnRequest, ExecuteQueryRequest, FilterPred, IndexDefMsg, KvGetRequest, KvPutRequest,
    PingRequest, RegisterTableRequest, TxnAbortRequest, TxnCommitRequest, TxnDeleteRecordRequest,
    TxnGetRequest, TxnPutRecordRequest, TxnPutRequest,
};
use crate::query::FilterOp;
use crate::schema::{IndexDef, Record, TableSchema, data_key};
use crate::sql::{Expression, LogicalPlan, SqlEngine};
use crate::types::{Key, Value};

const CONNECT_TIMEOUT: Duration = Duration::from_millis(200);
const RPC_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_TXN_ATTEMPTS: u32 = 64;
const MAX_ROUTE_ATTEMPTS: u32 = 16;

#[derive(Clone)]
struct LeaderConn {
    address: String,
    client: ClientServiceClient<Channel>,
}

struct Inner {
    seeds: Vec<String>,
    leader: RwLock<Option<LeaderConn>>,
    rng: AtomicU64,
    /// Schemas known via [`TakyonicClient::register_table`] (for PK UPDATE/DELETE).
    schemas: SyncRwLock<HashMap<String, TableSchema>>,
}

/// Topology-aware Takyonic client.
#[derive(Clone)]
pub struct TakyonicClient {
    inner: Arc<Inner>,
}

impl TakyonicClient {
    /// Create a client from seed node addresses (`host:port`).
    pub fn new(seeds: impl IntoIterator<Item = impl Into<String>>) -> Self {
        let seeds: Vec<String> = seeds.into_iter().map(Into::into).collect();
        let seed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0xC0FFEE);
        Self {
            inner: Arc::new(Inner {
                seeds,
                leader: RwLock::new(None),
                rng: AtomicU64::new(seed ^ 0x9E37_79B9_7F4A_7C15),
                schemas: SyncRwLock::new(HashMap::new()),
            }),
        }
    }

    /// Discover and cache the current Raft leader (idempotent).
    pub async fn connect(&self) -> Result<()> {
        self.discover_leader().await?;
        Ok(())
    }

    /// Linearizable point get via the leader.
    pub async fn get(&self, key: impl Into<Key>) -> Result<Option<Value>> {
        let key = key.into();
        self.with_leader(|mut client| {
            let key = key.clone();
            async move {
                let resp = client
                    .get(Request::new(KvGetRequest {
                        key: key.as_bytes().to_vec(),
                    }))
                    .await
                    .map_err(status_to_error)?
                    .into_inner();
                Ok(if resp.found {
                    Some(Value::new(resp.value))
                } else {
                    None
                })
            }
        })
        .await
    }

    /// Linearizable put via the leader.
    pub async fn put(&self, key: impl Into<Key>, value: impl Into<Value>) -> Result<u64> {
        let key = key.into();
        let value = value.into();
        self.with_leader(|mut client| {
            let key = key.clone();
            let value = value.clone();
            async move {
                let resp = client
                    .put(Request::new(KvPutRequest {
                        key: key.as_bytes().to_vec(),
                        value: value.as_bytes().to_vec(),
                    }))
                    .await
                    .map_err(status_to_error)?
                    .into_inner();
                Ok(resp.commit_ts)
            }
        })
        .await
    }

    /// Register a table schema on every reachable seed (so any elected leader
    /// can serve `put_record` / CBO queries).
    pub async fn register_table(&self, schema: TableSchema) -> Result<()> {
        self.inner
            .schemas
            .write()
            .insert(schema.name.clone(), schema.clone());
        let req = RegisterTableRequest {
            name: schema.name.clone(),
            primary_key: schema.primary_key.clone(),
            indexes: schema
                .indexes
                .iter()
                .map(|IndexDef { name, column, .. }| IndexDefMsg {
                    name: name.clone(),
                    column: column.clone(),
                })
                .collect(),
        };
        // Best-effort on all seeds; leader path still required for queries.
        let mut last_ok = false;
        let mut last_err = TakyonicError::Network("no seeds for register_table".into());
        for addr in &self.inner.seeds {
            match connect_client(addr).await {
                Ok(mut client) => match client
                    .register_table(Request::new(req.clone()))
                    .await
                    .map_err(status_to_error)
                {
                    Ok(_) => last_ok = true,
                    Err(e) => last_err = e,
                },
                Err(e) => last_err = e,
            }
        }
        if last_ok { Ok(()) } else { Err(last_err) }
    }

    /// Parse `sql`, translate to a logical plan, and execute on the Raft leader.
    ///
    /// * `INSERT` → evaluate VALUES → [`Self::execute_txn`] + [`ClientTxn::put_record`]
    /// * `SELECT` → CBO on the leader (`engine.query(...).filter(...)`)
    /// * `UPDATE` / `DELETE` → PK-equality WHERE via `put_record` / [`ClientTxn::delete_record`]
    ///
    /// OCC conflicts and NotLeader redirects are retried by the SDK.
    pub async fn execute_sql(&self, sql: &str) -> Result<Vec<Record>> {
        match SqlEngine::plan(sql)? {
            LogicalPlan::Insert {
                table,
                columns,
                values,
            } => {
                let records = executor::materialize_insert_records(
                    &columns,
                    &values,
                    &ExecutionContext::new(),
                )?;
                self.execute_txn(|txn| {
                    let table = table.clone();
                    let records = records.clone();
                    async move {
                        for record in records {
                            txn.put_record(&table, record).await?;
                        }
                        Ok(())
                    }
                })
                .await?;
                Ok(Vec::new())
            }
            LogicalPlan::Select { table, filters, .. } => {
                let (records, _explain) = self.execute_select(table, filters).await?;
                Ok(records)
            }
            LogicalPlan::Update {
                table,
                assignments,
                selection,
            } => {
                let pk = extract_pk_equality(&table, selection.as_ref(), &self.inner.schemas)?;
                self.execute_txn(|txn| {
                    let table = table.clone();
                    let assignments = assignments.clone();
                    let pk = pk.clone();
                    async move {
                        let key = data_key(&table, &pk);
                        let Some(raw) = txn.get(key).await? else {
                            return Ok(());
                        };
                        let mut record = Record::decode(&raw)?;
                        let ctx = ExecutionContext::new();
                        for (col, expr) in &assignments {
                            let v = executor::evaluate(expr, &record, &ctx)?;
                            record = record.set(col.clone(), value_to_field(&v));
                        }
                        txn.put_record(table, record).await?;
                        Ok(())
                    }
                })
                .await?;
                Ok(Vec::new())
            }
            LogicalPlan::Delete { table, selection } => {
                let pk = extract_pk_equality(&table, selection.as_ref(), &self.inner.schemas)?;
                self.execute_txn(|txn| {
                    let table = table.clone();
                    let pk = pk.clone();
                    async move {
                        txn.delete_record(table, pk).await?;
                        Ok(())
                    }
                })
                .await?;
                Ok(Vec::new())
            }
            LogicalPlan::Join { .. }
            | LogicalPlan::DistributedJoin { .. }
            | LogicalPlan::Aggregate { .. }
            | LogicalPlan::DistributedAggregate { .. }
            | LogicalPlan::Sort { .. }
            | LogicalPlan::Limit { .. } => Err(TakyonicError::Sql(
                "JOIN/Aggregate/Sort/Limit require the local Volcano path (SessionState)".into(),
            )),
            LogicalPlan::Begin | LogicalPlan::Commit | LogicalPlan::Rollback => {
                Err(TakyonicError::Sql(
                    "BEGIN/COMMIT/ROLLBACK require SessionState (local pgwire session)".into(),
                ))
            }
            LogicalPlan::CreateIndex { .. }
            | LogicalPlan::DropIndex { .. }
            | LogicalPlan::CreateRole { .. }
            | LogicalPlan::DropRole { .. }
            | LogicalPlan::Grant { .. }
            | LogicalPlan::Revoke { .. }
            | LogicalPlan::GrantRole { .. }
            | LogicalPlan::Explain { .. }
            | LogicalPlan::Analyze { .. }
            | LogicalPlan::Vacuum { .. }
            | LogicalPlan::Filter { .. }
            | LogicalPlan::SubqueryAlias { .. } => Err(TakyonicError::Sql(
                "CREATE/DROP INDEX, ROLE/GRANT, ANALYZE, VACUUM, EXPLAIN, Filter, and CTE views require the local Volcano path (SessionState)"
                    .into(),
            )),
        }
    }

    /// Like [`Self::execute_sql`] for SELECT, but also returns the CBO EXPLAIN text.
    pub async fn explain_sql(&self, sql: &str) -> Result<(Vec<Record>, String)> {
        match SqlEngine::plan(sql)? {
            LogicalPlan::Select { table, filters, .. } => self.execute_select(table, filters).await,
            other => Err(TakyonicError::Sql(format!(
                "EXPLAIN requires SELECT, got {other:?}"
            ))),
        }
    }

    async fn execute_select(
        &self,
        table: String,
        filters: Vec<crate::query::Filter>,
    ) -> Result<(Vec<Record>, String)> {
        let filter_preds: Vec<FilterPred> = filters
            .into_iter()
            .map(|f| FilterPred {
                column: f.column,
                op: match f.op {
                    FilterOp::Eq => "=".into(),
                    FilterOp::Ne => "!=".into(),
                    FilterOp::Gt => ">".into(),
                    FilterOp::Gte => ">=".into(),
                    FilterOp::Lt => "<".into(),
                    FilterOp::Lte => "<=".into(),
                },
                value: f.value,
            })
            .collect();
        self.with_leader(|mut client| {
            let table = table.clone();
            let filters = filter_preds.clone();
            async move {
                let resp = client
                    .execute_query(Request::new(ExecuteQueryRequest { table, filters }))
                    .await
                    .map_err(status_to_error)?
                    .into_inner();
                let mut records = Vec::with_capacity(resp.records.len());
                for bytes in resp.records {
                    records.push(Record::decode(&Value::new(bytes))?);
                }
                Ok((records, resp.explain))
            }
        })
        .await
    }

    /// Run `body` inside a snapshot-isolation transaction.
    ///
    /// On [`TakyonicError::Conflict`] the SDK applies exponential backoff with
    /// jitter and re-executes `body` against a fresh transaction. On
    /// [`TakyonicError::NotLeader`] / transport errors the leader cache is
    /// invalidated, topology is rediscovered, and the transaction is retried.
    /// Application code must not implement its own retry loop.
    pub async fn execute_txn<F, Fut, T>(&self, body: F) -> Result<T>
    where
        F: Fn(ClientTxn) -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let mut attempt = 0u32;
        loop {
            attempt = attempt.saturating_add(1);
            if attempt > MAX_TXN_ATTEMPTS {
                return Err(TakyonicError::Conflict(
                    "execute_txn exceeded max OCC/redirect retries".into(),
                ));
            }

            let txn = match self.begin_txn().await {
                Ok(t) => t,
                Err(e) if is_retryable_route(&e) => {
                    self.invalidate_leader().await;
                    let _ = self.discover_leader().await;
                    self.backoff(attempt).await;
                    continue;
                }
                Err(e) => return Err(e),
            };

            let txn_id = txn.txn_id;
            match body(txn).await {
                Ok(value) => match self.commit_txn(txn_id).await {
                    Ok(_) => return Ok(value),
                    Err(TakyonicError::Conflict(_)) => {
                        debug!(attempt, "OCC conflict — retrying execute_txn");
                        self.backoff(attempt).await;
                        continue;
                    }
                    Err(e) if is_retryable_route(&e) => {
                        let _ = self.abort_txn(txn_id).await;
                        self.invalidate_leader().await;
                        let _ = self.discover_leader().await;
                        self.backoff(attempt).await;
                        continue;
                    }
                    Err(e) => {
                        let _ = self.abort_txn(txn_id).await;
                        return Err(e);
                    }
                },
                Err(TakyonicError::Conflict(_)) => {
                    let _ = self.abort_txn(txn_id).await;
                    self.backoff(attempt).await;
                    continue;
                }
                Err(e) if is_retryable_route(&e) => {
                    let _ = self.abort_txn(txn_id).await;
                    self.invalidate_leader().await;
                    let _ = self.discover_leader().await;
                    self.backoff(attempt).await;
                    continue;
                }
                Err(e) => {
                    let _ = self.abort_txn(txn_id).await;
                    return Err(e);
                }
            }
        }
    }

    async fn begin_txn(&self) -> Result<ClientTxn> {
        let (txn_id, read_ts) = self
            .with_leader(|mut client| async move {
                let resp = client
                    .begin_txn(Request::new(BeginTxnRequest {}))
                    .await
                    .map_err(status_to_error)?
                    .into_inner();
                Ok((resp.txn_id, resp.read_ts))
            })
            .await?;
        Ok(ClientTxn {
            client: self.clone(),
            txn_id,
            read_ts,
        })
    }

    async fn commit_txn(&self, txn_id: u64) -> Result<u64> {
        self.with_leader(|mut client| async move {
            let resp = client
                .txn_commit(Request::new(TxnCommitRequest { txn_id }))
                .await
                .map_err(status_to_error)?
                .into_inner();
            Ok(resp.commit_ts)
        })
        .await
    }

    async fn abort_txn(&self, txn_id: u64) -> Result<()> {
        // Best-effort; ignore routing failures during abort.
        let _ = self
            .with_leader(|mut client| async move {
                client
                    .txn_abort(Request::new(TxnAbortRequest { txn_id }))
                    .await
                    .map_err(status_to_error)?;
                Ok(())
            })
            .await;
        Ok(())
    }

    async fn txn_get(&self, txn_id: u64, key: Key) -> Result<Option<Value>> {
        self.with_leader(|mut client| {
            let key = key.clone();
            async move {
                let resp = client
                    .txn_get(Request::new(TxnGetRequest {
                        txn_id,
                        key: key.as_bytes().to_vec(),
                    }))
                    .await
                    .map_err(status_to_error)?
                    .into_inner();
                Ok(if resp.found {
                    Some(Value::new(resp.value))
                } else {
                    None
                })
            }
        })
        .await
    }

    async fn txn_put(&self, txn_id: u64, key: Key, value: Value) -> Result<()> {
        self.with_leader(|mut client| {
            let key = key.clone();
            let value = value.clone();
            async move {
                client
                    .txn_put(Request::new(TxnPutRequest {
                        txn_id,
                        key: key.as_bytes().to_vec(),
                        value: value.as_bytes().to_vec(),
                    }))
                    .await
                    .map_err(status_to_error)?;
                Ok(())
            }
        })
        .await
    }

    async fn txn_put_record(&self, txn_id: u64, table: String, record: Record) -> Result<()> {
        self.with_leader(|mut client| {
            let table = table.clone();
            let record = record.clone();
            async move {
                client
                    .txn_put_record(Request::new(TxnPutRecordRequest {
                        txn_id,
                        table,
                        record: record.encode().as_bytes().to_vec(),
                    }))
                    .await
                    .map_err(status_to_error)?;
                Ok(())
            }
        })
        .await
    }

    async fn txn_delete_record(&self, txn_id: u64, table: String, pk: String) -> Result<()> {
        self.with_leader(|mut client| {
            let table = table.clone();
            let pk = pk.clone();
            async move {
                client
                    .txn_delete_record(Request::new(TxnDeleteRecordRequest {
                        txn_id,
                        table,
                        pk,
                    }))
                    .await
                    .map_err(status_to_error)?;
                Ok(())
            }
        })
        .await
    }

    async fn with_leader<F, Fut, T>(&self, f: F) -> Result<T>
    where
        F: Fn(ClientServiceClient<Channel>) -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let mut route_attempt = 0u32;
        loop {
            route_attempt = route_attempt.saturating_add(1);
            if route_attempt > MAX_ROUTE_ATTEMPTS {
                return Err(TakyonicError::Network(
                    "exhausted leader redirect attempts".into(),
                ));
            }
            let conn = {
                let guard = self.inner.leader.read().await;
                match guard.clone() {
                    Some(c) => c,
                    None => {
                        drop(guard);
                        self.discover_leader().await?
                    }
                }
            };
            match f(conn.client.clone()).await {
                Ok(v) => return Ok(v),
                Err(e) if is_retryable_route(&e) => {
                    if let TakyonicError::NotLeader {
                        leader_address: Some(addr),
                    } = &e
                    {
                        debug!(%addr, "NotLeader redirect");
                        match connect_client(addr).await {
                            Ok(client) => {
                                *self.inner.leader.write().await = Some(LeaderConn {
                                    address: addr.clone(),
                                    client,
                                });
                                continue;
                            }
                            Err(err) => {
                                warn!(%addr, %err, "redirect connect failed");
                            }
                        }
                    }
                    self.invalidate_leader().await;
                    let _ = self.discover_leader().await;
                }
                Err(e) => return Err(e),
            }
        }
    }

    async fn discover_leader(&self) -> Result<LeaderConn> {
        let mut hints: Vec<String> = self.inner.seeds.clone();
        if let Some(cached) = self.inner.leader.read().await.as_ref() {
            hints.insert(0, cached.address.clone());
        }
        hints.sort();
        hints.dedup();

        let mut last_err = TakyonicError::Network("no seed nodes configured".into());
        for addr in hints {
            match self.ping_for_leader(&addr).await {
                Ok(conn) => {
                    *self.inner.leader.write().await = Some(conn.clone());
                    return Ok(conn);
                }
                Err(e) => {
                    debug!(%addr, %e, "seed ping failed");
                    last_err = e;
                }
            }
        }
        Err(last_err)
    }

    async fn ping_for_leader(&self, addr: &str) -> Result<LeaderConn> {
        let mut client = connect_client(addr).await?;
        let resp = client
            .ping(Request::new(PingRequest {}))
            .await
            .map_err(status_to_error)?
            .into_inner();
        if resp.is_leader {
            return Ok(LeaderConn {
                address: if resp.self_address.is_empty() {
                    addr.to_string()
                } else {
                    resp.self_address
                },
                client,
            });
        }
        if resp.leader_address.is_empty() {
            return Err(TakyonicError::NotLeader {
                leader_address: None,
            });
        }
        let leader_addr = resp.leader_address;
        let mut leader_client = connect_client(&leader_addr).await?;
        let confirm = leader_client
            .ping(Request::new(PingRequest {}))
            .await
            .map_err(status_to_error)?
            .into_inner();
        if !confirm.is_leader {
            return Err(TakyonicError::NotLeader {
                leader_address: Some(leader_addr),
            });
        }
        Ok(LeaderConn {
            address: leader_addr,
            client: leader_client,
        })
    }

    async fn invalidate_leader(&self) {
        *self.inner.leader.write().await = None;
    }

    async fn backoff(&self, attempt: u32) {
        let delay = jittered_backoff(attempt, &self.inner.rng);
        tokio::time::sleep(delay).await;
    }
}

/// Handle passed to [`TakyonicClient::execute_txn`] closures.
pub struct ClientTxn {
    client: TakyonicClient,
    txn_id: u64,
    read_ts: u64,
}

impl ClientTxn {
    /// Transaction id assigned by the leader.
    pub fn id(&self) -> u64 {
        self.txn_id
    }

    /// Snapshot read timestamp.
    pub fn read_ts(&self) -> u64 {
        self.read_ts
    }

    /// Snapshot get within this transaction.
    pub async fn get(&self, key: impl Into<Key>) -> Result<Option<Value>> {
        self.client.txn_get(self.txn_id, key.into()).await
    }

    /// Buffer a put in the transaction workspace.
    pub async fn put(&self, key: impl Into<Key>, value: impl Into<Value>) -> Result<()> {
        self.client
            .txn_put(self.txn_id, key.into(), value.into())
            .await
    }

    /// Insert/update a structured record (data key + secondary indexes) under MVCC.
    pub async fn put_record(&self, table: impl Into<String>, record: Record) -> Result<()> {
        self.client
            .txn_put_record(self.txn_id, table.into(), record)
            .await
    }

    /// Delete a structured record by primary key (data key + secondary indexes).
    pub async fn delete_record(
        &self,
        table: impl Into<String>,
        pk: impl Into<String>,
    ) -> Result<()> {
        self.client
            .txn_delete_record(self.txn_id, table.into(), pk.into())
            .await
    }
}

/// Extract `pk_column = literal` from a Smart Client UPDATE/DELETE WHERE clause.
fn extract_pk_equality(
    table: &str,
    selection: Option<&Expression>,
    schemas: &SyncRwLock<HashMap<String, TableSchema>>,
) -> Result<String> {
    let schema = schemas.read().get(table).cloned().ok_or_else(|| {
        TakyonicError::Sql(format!(
            "table `{table}` schema unknown to Smart Client — call register_table first"
        ))
    })?;
    let Some(expr) = selection else {
        return Err(TakyonicError::Sql(
            "Smart Client UPDATE/DELETE requires a primary-key equality WHERE clause".into(),
        ));
    };
    match expr {
        Expression::BinaryOp {
            left,
            op: FilterOp::Eq,
            right,
        } => {
            let (col, lit) = match (left.as_ref(), right.as_ref()) {
                (Expression::Column(c), Expression::Literal(v)) => (c.as_str(), v.as_str()),
                (Expression::Literal(v), Expression::Column(c)) => (c.as_str(), v.as_str()),
                _ => {
                    return Err(TakyonicError::Sql(
                        "Smart Client UPDATE/DELETE requires `pk = literal`".into(),
                    ));
                }
            };
            if col != schema.primary_key {
                return Err(TakyonicError::Sql(format!(
                    "Smart Client UPDATE/DELETE requires equality on primary key `{}`, got `{col}`",
                    schema.primary_key
                )));
            }
            Ok(lit.to_string())
        }
        _ => Err(TakyonicError::Sql(
            "Smart Client UPDATE/DELETE requires `pk = literal`".into(),
        )),
    }
}

fn is_retryable_route(err: &TakyonicError) -> bool {
    matches!(
        err,
        TakyonicError::NotLeader { .. } | TakyonicError::Network(_) | TakyonicError::Raft(_)
    )
}

async fn connect_client(addr: &str) -> Result<ClientServiceClient<Channel>> {
    let uri = if addr.starts_with("http://") || addr.starts_with("https://") {
        addr.to_string()
    } else {
        format!("http://{addr}")
    };
    let channel = Channel::from_shared(uri.clone())
        .map_err(|e| TakyonicError::Network(e.to_string()))?
        .connect_timeout(CONNECT_TIMEOUT)
        .timeout(RPC_TIMEOUT)
        .connect()
        .await
        .map_err(|e| TakyonicError::Network(format!("connect {uri}: {e}")))?;
    Ok(ClientServiceClient::new(channel))
}

fn jittered_backoff(attempt: u32, rng: &AtomicU64) -> Duration {
    // 10ms, 25ms, 50ms, then double up to 500ms.
    let base_ms = match attempt {
        0 | 1 => 10,
        2 => 25,
        3 => 50,
        n => {
            let shift = n.saturating_sub(3).min(4);
            (50u64 << shift).min(500)
        }
    };
    let noise = next_u64(rng) % (base_ms / 2 + 1);
    let half = base_ms / 4;
    let ms = base_ms
        .saturating_sub(half)
        .saturating_add(noise % (half * 2 + 1));
    Duration::from_millis(ms.max(1))
}

fn next_u64(rng: &AtomicU64) -> u64 {
    // xorshift64*
    let mut x = rng.load(Ordering::Relaxed);
    if x == 0 {
        x = 0xDEAD_BEEF_CAFE_BABE;
    }
    x ^= x >> 12;
    x ^= x << 25;
    x ^= x >> 27;
    rng.store(x, Ordering::Relaxed);
    x.wrapping_mul(0x2545_F491_4F6C_DD1D)
}
