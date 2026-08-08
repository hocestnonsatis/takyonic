//! Distributed two-phase commit (2PC) over Raft-replicated shards.
//!
//! Cross-shard transactions are coordinated by [`TransactionCoordinator`]:
//! participants persist a local `PREPARED` record to their Raft-backed log
//! ([`RaftCommand::TxnPrepare`]) before ACKing the coordinator, hold locks on
//! the write-set, then apply or roll back on `COMMIT`/`ABORT`. A shared logical
//! clock supplies a global snapshot / commit timestamp so Snapshot Isolation
//! holds across shards.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use parking_lot::{Mutex, RwLock};
use tracing::{debug, info, warn};

use crate::error::{Result, TakyonicError};
use crate::raft::RaftCommand;
use crate::tc_log::{TcDecisionLog, TcDecisionRecord};
use crate::telemetry::EngineMetrics;
use crate::types::{CommitTs, Key, Value};

/// Opaque distributed transaction identifier.
pub type DistTxnId = u64;
/// Logical shard / partition id (Raft group).
pub type ShardId = u64;

/// 2PC transaction state visible to the coordinator and participants.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TwopcState {
    /// Coordinator collecting prepare votes.
    Preparing,
    /// All participants acknowledged prepare (or this participant has).
    Prepared,
    /// Globally committed; writes applied.
    Committed,
    /// Globally aborted; write-set discarded.
    Aborted,
}

impl TwopcState {
    /// Wire / log token.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Preparing => "PREPARING",
            Self::Prepared => "PREPARED",
            Self::Committed => "COMMITTED",
            Self::Aborted => "ABORTED",
        }
    }
}

/// One key mutation in a shard-local write-set (`None` = delete).
pub type WriteOp = (Key, Option<Value>);

/// Branch of a distributed transaction destined for one shard.
#[derive(Clone, Debug)]
pub struct ShardBranch {
    /// Target shard.
    pub shard_id: ShardId,
    /// Write-set for this shard (OCC-validated at prepare).
    pub writes: Vec<WriteOp>,
    /// Keys read on this shard at `read_ts` (for OCC).
    pub reads: Vec<(Key, CommitTs)>,
}

/// Coordinator-side distributed transaction request.
#[derive(Clone, Debug)]
pub struct DistTxnRequest {
    /// Snapshot read timestamp (global).
    pub read_ts: CommitTs,
    /// Per-shard branches (must be non-empty and cover ≥1 shard).
    pub branches: Vec<ShardBranch>,
}

/// Durable participant log record (persisted via Raft before prepare ACK).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ParticipantLogRecord {
    /// Write-set locked and durable; awaiting decision.
    Prepared {
        /// Distributed txn id.
        txn_id: DistTxnId,
        /// Snapshot / provisional timestamp from the coordinator.
        read_ts: CommitTs,
        /// Write-set (not yet applied to the live store).
        writes: Vec<WriteOp>,
    },
    /// Apply prepared writes at `commit_ts`.
    Commit {
        /// Distributed txn id.
        txn_id: DistTxnId,
        /// Global commit timestamp.
        commit_ts: CommitTs,
    },
    /// Discard prepared write-set.
    Abort {
        /// Distributed txn id.
        txn_id: DistTxnId,
    },
}

impl ParticipantLogRecord {
    /// Encode as a Raft state-machine command.
    pub fn to_raft_command(&self) -> RaftCommand {
        match self {
            Self::Prepared {
                txn_id,
                read_ts,
                writes,
            } => RaftCommand::txn_prepare(*txn_id, *read_ts, writes.clone()),
            Self::Commit {
                txn_id,
                commit_ts,
            } => RaftCommand::txn_commit(*txn_id, *commit_ts, Vec::new()),
            Self::Abort { txn_id } => RaftCommand::txn_abort(*txn_id),
        }
    }

    /// Decode from a Raft 2PC command (ops required on prepare/commit).
    pub fn from_raft_command(cmd: &RaftCommand) -> Option<Self> {
        match cmd {
            RaftCommand::TxnPrepare {
                txn_id,
                read_ts,
                ops,
            } => Some(Self::Prepared {
                txn_id: *txn_id,
                read_ts: *read_ts,
                writes: ops.clone(),
            }),
            RaftCommand::TxnCommit {
                txn_id,
                commit_ts,
                ..
            } => Some(Self::Commit {
                txn_id: *txn_id,
                commit_ts: *commit_ts,
            }),
            RaftCommand::TxnAbort { txn_id } => Some(Self::Abort { txn_id: *txn_id }),
            _ => None,
        }
    }
}

/// Raft-backed participant: prepare / commit / abort + recovery query.
pub trait ShardParticipant: Send + Sync {
    /// Shard identity.
    fn shard_id(&self) -> ShardId;

    /// Phase 1: lock + durable PREPARED (Raft), do **not** apply yet.
    fn prepare(
        &self,
        txn_id: DistTxnId,
        read_ts: CommitTs,
        writes: &[WriteOp],
        reads: &[(Key, CommitTs)],
    ) -> Result<()>;

    /// Phase 2 commit: apply prepared writes at `commit_ts`.
    fn commit(&self, txn_id: DistTxnId, commit_ts: CommitTs) -> Result<()>;

    /// Phase 2 abort: drop prepared writes / release locks.
    fn abort(&self, txn_id: DistTxnId) -> Result<()>;

    /// After crash: list txn ids still in `Prepared` (need coordinator decision).
    fn orphaned_prepared(&self) -> Vec<DistTxnId>;

    /// Point get at `read_ts` (for tests / SI verification).
    fn get_at(&self, key: &Key, read_ts: CommitTs) -> Option<Value>;

    /// Latest live value.
    fn get(&self, key: &Key) -> Option<Value> {
        self.get_at(key, u64::MAX)
    }
}

/// In-process shard with MVCC map + durable prepare log (Raft stand-in).
pub struct LocalShard {
    id: ShardId,
    /// Live versions: key → (ts, value|tombstone).
    store: RwLock<BTreeMap<Key, Vec<(CommitTs, Option<Value>)>>>,
    /// Prepared but unapplied write-sets.
    prepared: Mutex<HashMap<DistTxnId, Vec<WriteOp>>>,
    /// Keys locked by a prepared txn (exclusive).
    locks: Mutex<HashMap<Key, DistTxnId>>,
    /// Append-only Raft log of 2PC commands.
    raft_log: Mutex<Vec<RaftCommand>>,
    /// Inject prepare failure (network / crash simulation).
    fail_prepare: AtomicBool,
    /// Crash after durable PREPARED but before ACK (coordinator sees timeout).
    crash_after_prepare: AtomicBool,
    /// Last-known commit ts per key (OCC).
    last_commit: Mutex<HashMap<Key, CommitTs>>,
}

impl LocalShard {
    /// Empty shard.
    pub fn new(id: ShardId) -> Arc<Self> {
        Arc::new(Self {
            id,
            store: RwLock::new(BTreeMap::new()),
            prepared: Mutex::new(HashMap::new()),
            locks: Mutex::new(HashMap::new()),
            raft_log: Mutex::new(Vec::new()),
            fail_prepare: AtomicBool::new(false),
            crash_after_prepare: AtomicBool::new(false),
            last_commit: Mutex::new(HashMap::new()),
        })
    }

    /// Next prepare will return an error (simulates shard reject / network fail).
    pub fn inject_prepare_failure(&self, on: bool) {
        self.fail_prepare.store(on, Ordering::SeqCst);
    }

    /// Persist PREPARED then pretend the process died before ACK.
    pub fn inject_crash_after_prepare(&self, on: bool) {
        self.crash_after_prepare.store(on, Ordering::SeqCst);
    }

    /// Durable Raft log length (tests).
    pub fn raft_log_len(&self) -> usize {
        self.raft_log.lock().len()
    }

    /// Whether `txn_id` still has a prepared write-set.
    pub fn is_prepared(&self, txn_id: DistTxnId) -> bool {
        self.prepared.lock().contains_key(&txn_id)
    }

    /// Replay Raft log after a simulated crash (rebuild prepared set).
    pub fn recover_from_raft_log(&self) {
        let log = self.raft_log.lock().clone();
        let mut prepared = self.prepared.lock();
        let mut locks = self.locks.lock();
        prepared.clear();
        locks.clear();
        for cmd in &log {
            match cmd {
                RaftCommand::TxnPrepare { txn_id, ops, .. } => {
                    for (k, _) in ops {
                        locks.insert(k.clone(), *txn_id);
                    }
                    prepared.insert(*txn_id, ops.clone());
                }
                RaftCommand::TxnCommit {
                    txn_id,
                    commit_ts,
                    ops,
                } => {
                    prepared.remove(txn_id);
                    for (k, _) in ops {
                        locks.remove(k);
                    }
                    self.apply_writes(ops, *commit_ts);
                }
                RaftCommand::TxnAbort { txn_id } => {
                    if let Some(writes) = prepared.remove(txn_id) {
                        for (k, _) in &writes {
                            locks.remove(k);
                        }
                    }
                }
                _ => {}
            }
        }
    }

    fn apply_writes(&self, writes: &[WriteOp], commit_ts: CommitTs) {
        let mut store = self.store.write();
        let mut last = self.last_commit.lock();
        for (key, val) in writes {
            store
                .entry(key.clone())
                .or_default()
                .push((commit_ts, val.clone()));
            last.insert(key.clone(), commit_ts);
        }
    }

    fn occ_validate(
        &self,
        read_ts: CommitTs,
        reads: &[(Key, CommitTs)],
        writes: &[WriteOp],
    ) -> Result<()> {
        let last = self.last_commit.lock();
        for (key, _) in reads {
            if let Some(&ts) = last.get(key) {
                if ts > read_ts {
                    return Err(TakyonicError::Conflict(format!(
                        "shard {}: key {:?} committed at {ts} > read_ts {read_ts}",
                        self.id,
                        String::from_utf8_lossy(key.as_bytes())
                    )));
                }
            }
        }
        for (key, _) in writes {
            if let Some(&ts) = last.get(key) {
                if ts > read_ts {
                    return Err(TakyonicError::Conflict(format!(
                        "shard {}: write key {:?} committed at {ts} > read_ts {read_ts}",
                        self.id,
                        String::from_utf8_lossy(key.as_bytes())
                    )));
                }
            }
        }
        Ok(())
    }
}

impl ShardParticipant for LocalShard {
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
        if self.fail_prepare.load(Ordering::SeqCst) {
            return Err(TakyonicError::Network(format!(
                "shard {}: injected prepare failure",
                self.id
            )));
        }
        self.occ_validate(read_ts, reads, writes)?;

        // Acquire exclusive locks (no wait → abort to avoid distributed deadlock).
        {
            let mut locks = self.locks.lock();
            for (k, _) in writes {
                if let Some(owner) = locks.get(k) {
                    if *owner != txn_id {
                        return Err(TakyonicError::Conflict(format!(
                            "shard {}: key locked by txn {owner}",
                            self.id
                        )));
                    }
                }
            }
            for (k, _) in writes {
                locks.insert(k.clone(), txn_id);
            }
        }

        // Persist PREPARED to Raft log **before** ACK (meta: not LSM-applied).
        {
            let mut log = self.raft_log.lock();
            log.push(RaftCommand::txn_prepare(txn_id, read_ts, writes.to_vec()));
        }
        self.prepared.lock().insert(txn_id, writes.to_vec());
        debug!(shard = self.id, txn_id, "PREPARED durable");

        if self.crash_after_prepare.load(Ordering::SeqCst) {
            return Err(TakyonicError::Network(format!(
                "shard {}: crashed after PREPARED",
                self.id
            )));
        }
        Ok(())
    }

    fn commit(&self, txn_id: DistTxnId, commit_ts: CommitTs) -> Result<()> {
        if self.crash_after_prepare.load(Ordering::SeqCst) {
            return Err(TakyonicError::Network(format!(
                "shard {}: down (cannot commit)",
                self.id
            )));
        }
        let writes = {
            let mut prepared = self.prepared.lock();
            prepared.remove(&txn_id).ok_or_else(|| {
                TakyonicError::Engine(format!(
                    "shard {}: commit for unknown prepared txn {txn_id}",
                    self.id
                ))
            })?
        };
        {
            let mut locks = self.locks.lock();
            for (k, _) in &writes {
                locks.remove(k);
            }
        }
        self.raft_log.lock().push(RaftCommand::txn_commit(
            txn_id,
            commit_ts,
            writes.clone(),
        ));
        self.apply_writes(&writes, commit_ts);
        debug!(shard = self.id, txn_id, commit_ts, "COMMIT applied");
        Ok(())
    }

    fn abort(&self, txn_id: DistTxnId) -> Result<()> {
        if self.crash_after_prepare.load(Ordering::SeqCst) {
            return Err(TakyonicError::Network(format!(
                "shard {}: down (cannot abort)",
                self.id
            )));
        }
        let writes = self.prepared.lock().remove(&txn_id);
        if let Some(writes) = writes {
            let mut locks = self.locks.lock();
            for (k, _) in &writes {
                locks.remove(k);
            }
        }
        self.raft_log.lock().push(RaftCommand::txn_abort(txn_id));
        debug!(shard = self.id, txn_id, "ABORT");
        Ok(())
    }

    fn orphaned_prepared(&self) -> Vec<DistTxnId> {
        self.prepared.lock().keys().copied().collect()
    }

    fn get_at(&self, key: &Key, read_ts: CommitTs) -> Option<Value> {
        let store = self.store.read();
        let versions = store.get(key)?;
        versions
            .iter()
            .rev()
            .find(|(ts, _)| *ts <= read_ts)
            .and_then(|(_, v)| v.clone())
    }
}

/// Engine-backed [`ShardParticipant`]: durable 2PC via Raft + LSM apply on commit.
pub struct EngineShard {
    engine: Arc<crate::engine::TakyonicEngine>,
    id: ShardId,
}

impl EngineShard {
    /// Wrap `engine` as shard `id` (also stores id on the engine).
    pub fn new(engine: Arc<crate::engine::TakyonicEngine>, id: ShardId) -> Arc<Self> {
        engine.set_shard_id(id);
        Arc::new(Self { engine, id })
    }

    /// Shared engine handle.
    pub fn engine(&self) -> &Arc<crate::engine::TakyonicEngine> {
        &self.engine
    }
}

impl ShardParticipant for EngineShard {
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
        self.engine.twopc_prepare(txn_id, read_ts, writes, reads)
    }

    fn commit(&self, txn_id: DistTxnId, commit_ts: CommitTs) -> Result<()> {
        self.engine.twopc_commit(txn_id, commit_ts)
    }

    fn abort(&self, txn_id: DistTxnId) -> Result<()> {
        self.engine.twopc_abort(txn_id)
    }

    fn orphaned_prepared(&self) -> Vec<DistTxnId> {
        self.engine.twopc_orphaned_prepared()
    }

    fn get_at(&self, key: &Key, read_ts: CommitTs) -> Option<Value> {
        self.engine
            .get_at_with_ts(key, read_ts)
            .ok()
            .and_then(|(v, _)| v)
    }
}

/// Global logical clock for cross-shard Snapshot Isolation.
#[derive(Default)]
pub struct GlobalClock {
    next: AtomicU64,
}

impl GlobalClock {
    /// Fresh clock starting at 1.
    pub fn new() -> Self {
        Self {
            next: AtomicU64::new(1),
        }
    }

    /// Allocate a monotonic timestamp (read or commit).
    pub fn tick(&self) -> CommitTs {
        self.next.fetch_add(1, Ordering::SeqCst)
    }

    /// Current high-water (last issued).
    pub fn now(&self) -> CommitTs {
        self.next.load(Ordering::SeqCst).saturating_sub(1)
    }
}

/// Outcome of a distributed commit attempt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DistTxnOutcome {
    /// All shards committed at `commit_ts`.
    Committed {
        /// Distributed txn id.
        txn_id: DistTxnId,
        /// Global commit timestamp.
        commit_ts: CommitTs,
    },
    /// Aborted (prepare failure / OCC / injected crash).
    Aborted {
        /// Distributed txn id.
        txn_id: DistTxnId,
        /// Human-readable reason.
        reason: String,
    },
}

/// Coordinator decision log (queried by recovering participants).
#[derive(Clone, Debug)]
pub struct CoordinatorDecision {
    /// Final state (`Committed` or `Aborted`).
    pub state: TwopcState,
    /// Commit timestamp when committed.
    pub commit_ts: Option<CommitTs>,
}

/// Two-phase commit transaction coordinator.
pub struct TransactionCoordinator {
    shards: RwLock<HashMap<ShardId, Arc<dyn ShardParticipant>>>,
    clock: GlobalClock,
    next_txn: AtomicU64,
    /// In-memory decisions (always updated; also mirrored to [`Self::decision_log`]).
    decisions: Mutex<HashMap<DistTxnId, CoordinatorDecision>>,
    /// Optional on-disk decision log (`data_dir/TC_DECISIONS`).
    decision_log: Mutex<Option<TcDecisionLog>>,
    /// Directory used when reopening the decision log after crash.
    decision_log_dir: Mutex<Option<PathBuf>>,
    /// In-flight state machine.
    inflight: Mutex<HashMap<DistTxnId, TwopcState>>,
    metrics: Option<Arc<EngineMetrics>>,
    /// Chaos: after durable decide, fail before Phase-2 participant apply.
    crash_after_decide: AtomicBool,
}

impl TransactionCoordinator {
    /// Empty in-memory coordinator (register shards before use).
    ///
    /// Decisions are **not** persisted; use [`Self::open`] for production crash safety.
    pub fn new(metrics: Option<Arc<EngineMetrics>>) -> Arc<Self> {
        Arc::new(Self {
            shards: RwLock::new(HashMap::new()),
            clock: GlobalClock::new(),
            next_txn: AtomicU64::new(1),
            decisions: Mutex::new(HashMap::new()),
            decision_log: Mutex::new(None),
            decision_log_dir: Mutex::new(None),
            inflight: Mutex::new(HashMap::new()),
            metrics,
            crash_after_decide: AtomicBool::new(false),
        })
    }

    /// Open a coordinator with a durable decision log under `data_dir`.
    pub fn open(data_dir: &Path, metrics: Option<Arc<EngineMetrics>>) -> Result<Arc<Self>> {
        let (loaded, max_id) = TcDecisionLog::load(data_dir)?;
        let log = TcDecisionLog::open(data_dir)?;
        Ok(Arc::new(Self {
            shards: RwLock::new(HashMap::new()),
            clock: GlobalClock::new(),
            next_txn: AtomicU64::new(max_id.saturating_add(1).max(1)),
            decisions: Mutex::new(loaded),
            decision_log: Mutex::new(Some(log)),
            decision_log_dir: Mutex::new(Some(data_dir.to_path_buf())),
            inflight: Mutex::new(HashMap::new()),
            metrics,
            crash_after_decide: AtomicBool::new(false),
        }))
    }

    /// Persist terminal decisions under `data_dir` (no-op if already durable).
    pub fn attach_decision_log(&self, data_dir: &Path) -> Result<()> {
        let (loaded, max_id) = TcDecisionLog::load(data_dir)?;
        {
            let mut decisions = self.decisions.lock();
            for (id, d) in loaded {
                decisions.insert(id, d);
            }
        }
        let cur = self.next_txn.load(Ordering::SeqCst);
        if max_id.saturating_add(1) > cur {
            self.next_txn.store(max_id.saturating_add(1), Ordering::SeqCst);
        }
        *self.decision_log.lock() = Some(TcDecisionLog::open(data_dir)?);
        *self.decision_log_dir.lock() = Some(data_dir.to_path_buf());
        Ok(())
    }

    /// Chaos: after durable decide, return error before participant commit/abort apply.
    pub fn inject_crash_after_decide(&self, on: bool) {
        self.crash_after_decide.store(on, Ordering::SeqCst);
    }

    /// Directory of the durable decision log, if any.
    pub fn decision_log_dir(&self) -> Option<PathBuf> {
        self.decision_log_dir.lock().clone()
    }

    fn persist_decision(&self, txn_id: DistTxnId, decision: CoordinatorDecision) -> Result<()> {
        self.decisions.lock().insert(txn_id, decision.clone());
        if let Some(log) = self.decision_log.lock().as_mut() {
            log.append_decision(&TcDecisionRecord {
                txn_id,
                state: decision.state,
                commit_ts: decision.commit_ts,
            })?;
        }
        Ok(())
    }

    /// Register a participant shard.
    pub fn register_shard(&self, shard: Arc<dyn ShardParticipant>) {
        self.shards.write().insert(shard.shard_id(), shard);
    }

    /// Shared global clock (snapshot reads).
    pub fn clock(&self) -> &GlobalClock {
        &self.clock
    }

    /// Begin a distributed txn: allocate id + global read timestamp.
    pub fn begin(&self) -> (DistTxnId, CommitTs) {
        let txn_id = self.next_txn.fetch_add(1, Ordering::SeqCst);
        let read_ts = self.clock.tick();
        self.inflight
            .lock()
            .insert(txn_id, TwopcState::Preparing);
        (txn_id, read_ts)
    }

    /// Look up the coordinator decision (recovery path).
    pub fn decision(&self, txn_id: DistTxnId) -> Option<CoordinatorDecision> {
        self.decisions.lock().get(&txn_id).cloned()
    }

    /// Drive 2PC for `req` under an already-begun `txn_id`.
    pub fn commit(&self, txn_id: DistTxnId, req: DistTxnRequest) -> Result<DistTxnOutcome> {
        if req.branches.is_empty() {
            self.finish_abort(txn_id, "empty branches".into());
            return Ok(DistTxnOutcome::Aborted {
                txn_id,
                reason: "empty branches".into(),
            });
        }

        // —— Phase 1: PREPARE ——
        self.inflight
            .lock()
            .insert(txn_id, TwopcState::Preparing);
        let shards = self.shards.read().clone();
        let mut prepared_shards: Vec<ShardId> = Vec::new();
        let mut prepare_err: Option<String> = None;

        for branch in &req.branches {
            let Some(shard) = shards.get(&branch.shard_id) else {
                prepare_err = Some(format!("unknown shard {}", branch.shard_id));
                break;
            };
            match shard.prepare(txn_id, req.read_ts, &branch.writes, &branch.reads) {
                Ok(()) => {
                    prepared_shards.push(branch.shard_id);
                    if let Some(m) = &self.metrics {
                        m.record_dtxn_prepared();
                    }
                }
                Err(e) => {
                    prepare_err = Some(e.to_string());
                    break;
                }
            }
        }

        if let Some(reason) = prepare_err {
            for sid in &prepared_shards {
                if let Some(s) = shards.get(sid) {
                    let _ = s.abort(txn_id);
                }
            }
            for branch in &req.branches {
                if !prepared_shards.contains(&branch.shard_id) {
                    if let Some(s) = shards.get(&branch.shard_id) {
                        let _ = s.abort(txn_id);
                    }
                }
            }
            if let Some(m) = &self.metrics {
                m.record_dtxn_aborted();
            }
            self.finish_abort(txn_id, reason.clone());
            warn!(txn_id, %reason, "2PC aborted in prepare");
            return Ok(DistTxnOutcome::Aborted { txn_id, reason });
        }

        self.inflight
            .lock()
            .insert(txn_id, TwopcState::Prepared);

        // —— Phase 2: COMMIT ——
        // Coordinator durable decision first (source of truth for recovery).
        let commit_ts = self.clock.tick();
        self.persist_decision(
            txn_id,
            CoordinatorDecision {
                state: TwopcState::Committed,
                commit_ts: Some(commit_ts),
            },
        )?;
        self.inflight
            .lock()
            .insert(txn_id, TwopcState::Committed);

        if self.crash_after_decide.load(Ordering::SeqCst) {
            return Err(TakyonicError::Network(
                "injected crash after durable 2PC decide".into(),
            ));
        }

        for branch in &req.branches {
            let shard = shards.get(&branch.shard_id).unwrap();
            shard.commit(txn_id, commit_ts)?;
        }
        if let Some(m) = &self.metrics {
            m.record_dtxn_committed();
        }
        info!(txn_id, commit_ts, shards = req.branches.len(), "2PC committed");
        Ok(DistTxnOutcome::Committed { txn_id, commit_ts })
    }

    /// Convenience: begin + commit in one call.
    pub fn execute(&self, mut req: DistTxnRequest) -> Result<DistTxnOutcome> {
        let (txn_id, read_ts) = self.begin();
        req.read_ts = read_ts;
        self.commit(txn_id, req)
    }

    /// Recover orphaned prepared txns on `shard` by querying coordinator decisions.
    pub fn recover_participant(&self, shard: &dyn ShardParticipant) -> Result<usize> {
        let orphans = shard.orphaned_prepared();
        let mut resolved = 0usize;
        for txn_id in orphans {
            match self.decision(txn_id) {
                Some(CoordinatorDecision {
                    state: TwopcState::Committed,
                    commit_ts: Some(ts),
                }) => {
                    shard.commit(txn_id, ts)?;
                    resolved += 1;
                }
                Some(CoordinatorDecision {
                    state: TwopcState::Aborted,
                    ..
                })
                | None => {
                    // No decision or abort → safe to roll back (presumed abort).
                    shard.abort(txn_id)?;
                    resolved += 1;
                }
                _ => {}
            }
        }
        Ok(resolved)
    }

    fn finish_abort(&self, txn_id: DistTxnId, _reason: String) {
        // Best-effort durable abort; in-memory always updated.
        let _ = self.persist_decision(
            txn_id,
            CoordinatorDecision {
                state: TwopcState::Aborted,
                commit_ts: None,
            },
        );
        self.inflight.lock().insert(txn_id, TwopcState::Aborted);
    }
}

/// Helper: build a single-key put branch.
pub fn put_branch(shard_id: ShardId, key: impl Into<Key>, value: impl Into<Value>) -> ShardBranch {
    ShardBranch {
        shard_id,
        writes: vec![(key.into(), Some(value.into()))],
        reads: Vec::new(),
    }
}

/// Route a local SI write/read workspace onto per-shard 2PC branches.
///
/// Uses [`crate::partition::PartitionRouter`] + table catalog metadata. Keys that
/// cannot be attributed to a table fall back to `default_shard`.
pub fn partition_txn_branches(
    writes: &std::collections::BTreeMap<Key, crate::txn::WriteOp>,
    reads: &std::collections::BTreeMap<Key, CommitTs>,
    schema_of: &dyn Fn(&str) -> Option<crate::schema::TableSchema>,
    router: &crate::partition::PartitionRouter,
    default_shard: ShardId,
) -> Result<Vec<ShardBranch>> {
    use crate::schema::{Record, pk_from_data_key, table_from_user_key};
    use crate::txn::WriteOp as TxnWriteOp;
    use std::collections::BTreeMap as StdBTree;

    let mut by_shard: StdBTree<ShardId, ShardBranch> = StdBTree::new();

    let resolve_shard = |key: &Key, put_val: Option<&Value>| -> ShardId {
        let Some(table) = table_from_user_key(key) else {
            return default_shard;
        };
        let Some(schema) = schema_of(&table) else {
            return default_shard;
        };
        if matches!(
            schema.partitioning,
            crate::partition::PartitioningStrategy::None
        ) {
            return default_shard;
        }
        let part_col = schema
            .partitioning
            .column()
            .unwrap_or(schema.primary_key.as_str());
        let part_val = if part_col == schema.primary_key {
            pk_from_data_key(key, &table).or_else(|| {
                // Idx_<table>_…_<pk> — last underscore segment.
                let bytes = key.as_bytes();
                let sep = bytes.iter().rposition(|&b| b == b'_')?;
                String::from_utf8(bytes[sep + 1..].to_vec()).ok()
            })
        } else if let Some(v) = put_val {
            Record::decode(v)
                .ok()
                .and_then(|r| r.get(part_col).map(str::to_string))
        } else {
            pk_from_data_key(key, &table)
        };
        let Some(part_val) = part_val else {
            return default_shard;
        };
        match router.route_key(&schema, &part_val) {
            Ok((_, node)) => node,
            Err(_) => default_shard,
        }
    };

    for (key, op) in writes {
        let put_val = match op {
            TxnWriteOp::Put(v) => Some(v),
            TxnWriteOp::Delete => None,
        };
        let shard_id = resolve_shard(key, put_val);
        let entry = by_shard.entry(shard_id).or_insert_with(|| ShardBranch {
            shard_id,
            writes: Vec::new(),
            reads: Vec::new(),
        });
        let dtxn_op = match op {
            TxnWriteOp::Put(v) => (key.clone(), Some(v.clone())),
            TxnWriteOp::Delete => (key.clone(), None),
        };
        entry.writes.push(dtxn_op);
    }

    for (key, seen_ts) in reads {
        let shard_id = resolve_shard(key, None);
        let entry = by_shard.entry(shard_id).or_insert_with(|| ShardBranch {
            shard_id,
            writes: Vec::new(),
            reads: Vec::new(),
        });
        entry.reads.push((key.clone(), *seen_ts));
    }

    if by_shard.is_empty() {
        return Ok(vec![ShardBranch {
            shard_id: default_shard,
            writes: Vec::new(),
            reads: Vec::new(),
        }]);
    }
    Ok(by_shard.into_values().collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::telemetry::EngineMetrics;
    use std::thread;

    fn key(s: &str) -> Key {
        Key::new(s.as_bytes().to_vec())
    }
    fn val(s: &str) -> Value {
        Value::new(s.as_bytes().to_vec())
    }

    #[test]
    fn cross_shard_commit_atomicity() {
        let metrics = Arc::new(EngineMetrics::new());
        let tc = TransactionCoordinator::new(Some(Arc::clone(&metrics)));
        let a = LocalShard::new(1);
        let b = LocalShard::new(2);
        tc.register_shard(a.clone());
        tc.register_shard(b.clone());

        let outcome = tc
            .execute(DistTxnRequest {
                read_ts: 0,
                branches: vec![
                    put_branch(1, key("acct:A"), val("100")),
                    put_branch(2, key("acct:B"), val("50")),
                ],
            })
            .unwrap();
        assert!(matches!(outcome, DistTxnOutcome::Committed { .. }));
        assert_eq!(a.get(&key("acct:A")).unwrap().as_bytes(), b"100");
        assert_eq!(b.get(&key("acct:B")).unwrap().as_bytes(), b"50");
        assert!(metrics.dtxn_prepared() >= 2);
        assert_eq!(metrics.dtxn_committed(), 1);
        assert!(a.raft_log_len() >= 2);
        assert!(RaftCommand::txn_prepare(1, 1, vec![]).is_meta());
        assert!(!RaftCommand::txn_commit(1, 2, vec![]).is_meta());
    }

    #[test]
    fn prepare_failure_rolls_back_all_shards() {
        let metrics = Arc::new(EngineMetrics::new());
        let tc = TransactionCoordinator::new(Some(Arc::clone(&metrics)));
        let a = LocalShard::new(1);
        let b = LocalShard::new(2);
        tc.register_shard(a.clone());
        tc.register_shard(b.clone());

        tc.execute(DistTxnRequest {
            read_ts: 0,
            branches: vec![
                put_branch(1, key("acct:A"), val("100")),
                put_branch(2, key("acct:B"), val("100")),
            ],
        })
        .unwrap();

        b.inject_prepare_failure(true);
        let outcome = tc
            .execute(DistTxnRequest {
                read_ts: 0,
                branches: vec![
                    ShardBranch {
                        shard_id: 1,
                        writes: vec![(key("acct:A"), Some(val("90")))],
                        reads: vec![(key("acct:A"), 0)],
                    },
                    ShardBranch {
                        shard_id: 2,
                        writes: vec![(key("acct:B"), Some(val("110")))],
                        reads: vec![(key("acct:B"), 0)],
                    },
                ],
            })
            .unwrap();
        assert!(matches!(outcome, DistTxnOutcome::Aborted { .. }));
        assert_eq!(a.get(&key("acct:A")).unwrap().as_bytes(), b"100");
        assert_eq!(b.get(&key("acct:B")).unwrap().as_bytes(), b"100");
        assert!(!a.is_prepared(2) && !b.is_prepared(2));
        assert!(metrics.dtxn_aborted() >= 1);
    }

    #[test]
    fn crash_after_prepared_recovers_via_coordinator() {
        let tc = TransactionCoordinator::new(None);
        let a = LocalShard::new(1);
        let b = LocalShard::new(2);
        tc.register_shard(a.clone());
        tc.register_shard(b.clone());

        tc.execute(DistTxnRequest {
            read_ts: 0,
            branches: vec![put_branch(1, key("x"), val("1"))],
        })
        .unwrap();

        b.inject_crash_after_prepare(true);
        let outcome = tc
            .execute(DistTxnRequest {
                read_ts: 0,
                branches: vec![
                    put_branch(1, key("x"), val("2")),
                    put_branch(2, key("y"), val("9")),
                ],
            })
            .unwrap();
        assert!(matches!(outcome, DistTxnOutcome::Aborted { .. }));

        b.inject_crash_after_prepare(false);
        let n = tc.recover_participant(b.as_ref()).unwrap();
        assert!(n >= 1);
        assert!(b.get(&key("y")).is_none());
        assert_eq!(a.get(&key("x")).unwrap().as_bytes(), b"1");
    }

    #[test]
    fn concurrent_cross_shard_stress_no_partial_commits() {
        let metrics = Arc::new(EngineMetrics::new());
        let tc = TransactionCoordinator::new(Some(Arc::clone(&metrics)));
        let a = LocalShard::new(1);
        let b = LocalShard::new(2);
        tc.register_shard(a.clone());
        tc.register_shard(b.clone());

        tc.execute(DistTxnRequest {
            read_ts: 0,
            branches: vec![
                put_branch(1, key("bal"), val("1000")),
                put_branch(2, key("bal"), val("1000")),
            ],
        })
        .unwrap();

        let tc2 = Arc::clone(&tc);
        let mut handles = Vec::new();
        for i in 0..32 {
            let tc = Arc::clone(&tc2);
            handles.push(thread::spawn(move || {
                let (from, to) = if i % 2 == 0 { (1u64, 2u64) } else { (2, 1) };
                let _ = tc.execute(DistTxnRequest {
                    read_ts: 0,
                    branches: vec![
                        ShardBranch {
                            shard_id: from,
                            writes: vec![(key("bal"), Some(val("x")))],
                            reads: vec![],
                        },
                        ShardBranch {
                            shard_id: to,
                            writes: vec![(key("bal"), Some(val("y")))],
                            reads: vec![],
                        },
                    ],
                });
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert!(a.get(&key("bal")).is_some());
        assert!(b.get(&key("bal")).is_some());
        assert!(a.orphaned_prepared().is_empty());
        assert!(b.orphaned_prepared().is_empty());
        assert!(metrics.dtxn_committed() + metrics.dtxn_aborted() >= 1);
    }

    #[test]
    fn snapshot_isolation_global_timestamp() {
        let tc = TransactionCoordinator::new(None);
        let a = LocalShard::new(1);
        let b = LocalShard::new(2);
        tc.register_shard(a.clone());
        tc.register_shard(b.clone());

        let (txn_id, read_ts) = tc.begin();
        assert!(read_ts >= 1);
        tc.execute(DistTxnRequest {
            read_ts: 0,
            branches: vec![put_branch(1, key("k"), val("new"))],
        })
        .unwrap();
        assert!(a.get_at(&key("k"), read_ts).is_none());
        let out = tc
            .commit(
                txn_id,
                DistTxnRequest {
                    read_ts,
                    branches: vec![put_branch(2, key("k"), val("snap"))],
                },
            )
            .unwrap();
        let DistTxnOutcome::Committed { commit_ts, .. } = out else {
            panic!("expected commit");
        };
        assert!(commit_ts > read_ts);
        assert_eq!(b.get_at(&key("k"), commit_ts).unwrap().as_bytes(), b"snap");
    }

    #[test]
    fn raft_prepare_command_roundtrip() {
        let ops = vec![(key("a"), Some(val("1")))];
        let cmd = RaftCommand::txn_prepare(42, 7, ops.clone());
        assert!(cmd.is_meta());
        let encoded = cmd.encode().unwrap();
        let decoded = RaftCommand::decode(encoded).unwrap();
        assert_eq!(decoded, cmd);
        let rec = ParticipantLogRecord::from_raft_command(&decoded).unwrap();
        assert_eq!(
            rec,
            ParticipantLogRecord::Prepared {
                txn_id: 42,
                read_ts: 7,
                writes: ops,
            }
        );
    }

    #[test]
    fn metrics_prometheus_names() {
        let m = EngineMetrics::new();
        m.record_dtxn_prepared();
        m.record_dtxn_prepared();
        m.record_dtxn_aborted();
        m.record_dtxn_committed();
        let text = m.render_prometheus(None);
        assert!(text.contains("takyonic_distributed_txn_prepared_total 2"));
        assert!(text.contains("takyonic_distributed_txn_aborted_total 1"));
        assert!(text.contains("takyonic_distributed_txn_committed_total 1"));
    }

    #[test]
    fn twopc_tc_crash_after_decide_recovers_commit() {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("takyonic-tc-decide-{nanos}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let a = LocalShard::new(1);
        let b = LocalShard::new(2);

        {
            let tc = TransactionCoordinator::open(&dir, None).unwrap();
            tc.register_shard(a.clone());
            tc.register_shard(b.clone());
            tc.inject_crash_after_decide(true);
            let err = tc
                .execute(DistTxnRequest {
                    read_ts: 0,
                    branches: vec![
                        put_branch(1, key("acct:A"), val("100")),
                        put_branch(2, key("acct:B"), val("50")),
                    ],
                })
                .expect_err("injected crash after decide");
            assert!(err.to_string().contains("crash after durable"));
            // Participants still PREPARED; values not applied yet.
            assert!(a.get(&key("acct:A")).is_none());
            assert!(b.get(&key("acct:B")).is_none());
            assert!(!a.orphaned_prepared().is_empty() || !b.orphaned_prepared().is_empty());
        }

        // Reopen TC from durable log; finish Phase-2.
        let tc2 = TransactionCoordinator::open(&dir, None).unwrap();
        tc2.register_shard(a.clone());
        tc2.register_shard(b.clone());
        let n_a = tc2.recover_participant(a.as_ref()).unwrap();
        let n_b = tc2.recover_participant(b.as_ref()).unwrap();
        assert!(n_a + n_b >= 1);
        assert_eq!(a.get(&key("acct:A")).unwrap().as_bytes(), b"100");
        assert_eq!(b.get(&key("acct:B")).unwrap().as_bytes(), b"50");
        assert!(a.orphaned_prepared().is_empty());
        assert!(b.orphaned_prepared().is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
