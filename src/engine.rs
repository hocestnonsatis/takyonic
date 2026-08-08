//! End-to-end Takyonic engine orchestration.
//!
//! This module is pure glue: it wires admission, group-commit WAL, the local
//! Raft state-machine stand-in, memtable, SST manager, and dual-pool compaction
//! into a single thread-safe facade. Primitives are not reimplemented here.
//!
//! Write path (embedded / single-node, Step 9):
//! 1. Acquire an admission token (L0 soft/hard backpressure).
//! 2. `LocalRaftNode::propose` → durable group-commit Raft log (one fsync / batch).
//! 3. Apply hook publishes into the memtable before waiters wake.
//! 4. Flush to L0 and kick the L0 Rapid pool when the memtable is full.
//!
//! Distributed write path (Step 39 / HA cluster): after
//! [`TakyonicEngine::attach_raft_node`], OCC commits and puts go through
//! networked [`crate::consensus::RaftNode::propose`]. Followers reject with
//! [`TakyonicError::NotLeader`]; the leader waits for quorum replication
//! before the entry is applied to the local LSM state machine.
//!
//! Note: the memtable remains the ordered `RwLock<BTreeMap>` from Step 2 (not
//! DashMap) because flush requires key order for SST emission.

use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Weak};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use parking_lot::{Mutex, RwLock};
use tracing::{debug, info, warn};

use crate::admission::{AdmissionController, AdmissionOutcome};
use crate::bpm::{BufferPoolManager, DEFAULT_LRU_K};
use crate::compaction::{
    CompactionEngine, SstManager, SstMeta, split_entries_by_max_bytes, sst_object_key,
};
use crate::config::Config;
use crate::consensus::{RaftConsensus, Role};
use crate::disk::{DiskManager, PAGES_V2_PREFIX, REMOTE_PAGES_KEY};
use crate::error::{Result, TakyonicError};
use crate::manifest::{ManifestManager, ManifestSst, StorageManifest};
use crate::memtable::Memtable;
use crate::object_store::{LocalFileBackend, ObjectStorage};
use crate::query::Query;
use crate::raft::{BatchApplyResult, CommittedEntry, LocalRaftNode, RaftCommand};
use crate::hnsw::HnswIndex;
use crate::rbac::{AuthCatalog, SharedAuthCatalog};
use crate::schema::{
    ColumnSpec, IndexDef, Record, TableSchema, data_table_prefix, index_table_prefix,
};
use crate::vacuum::VacuumStats;
use crate::snapshot::{SnapshotPayload, snapshot_sst_path};
use crate::sst::{SstRegistry, SstWriter};
use crate::stats::{self, StatsCatalog, TableStats};
use crate::telemetry::{EngineMetrics, MetricsManager};
use crate::txn::{IsolationLevel, SsiRegistry, StatsEdit, Transaction, TxnTracker, WriteOp};
use crate::txn_wal::{WalManager, WalRecord, records_from_writes};
use crate::dtxn::TransactionCoordinator;
use crate::types::{CommitTs, Entry, Key, Value};
use crate::vector::{VectorIndexSpec, VectorValue};
use crate::wal::{WalReader, WalWriter, segment_path};

use std::collections::{BTreeMap, HashMap};

/// Default LSM depth for a fresh engine (L0..L3).
const DEFAULT_LEVEL_COUNT: usize = 4;

/// How long a client write may wait for L0-aware admission tokens.
const DEFAULT_ADMISSION_TIMEOUT: Duration = Duration::from_secs(30);

/// Public embedded key-value engine facade.
pub struct TakyonicEngine {
    config: Config,
    /// Local Raft stand-in: propose → group-commit WAL → apply.
    raft: Mutex<Option<Arc<LocalRaftNode>>>,
    wal_dir: PathBuf,
    wal_segment: AtomicU64,
    admission: Arc<AdmissionController>,
    manager: Arc<SstManager>,
    /// Long-lived L0 Rapid + Ln Haul pools. Taken on [`Self::close`].
    compaction: Mutex<Option<CompactionEngine>>,
    /// Serializes memtable snapshot/clear during flush (not held across fsync).
    flush_mu: Mutex<()>,
    closed: AtomicBool,
    admission_timeout: Duration,
    /// Lock-free counters + WAL fsync latency histogram for observability.
    metrics: Arc<EngineMetrics>,
    /// Optional Prometheus `/metrics` HTTP scrape server (owned for Drop/shutdown).
    metrics_manager: Mutex<Option<MetricsManager>>,
    /// Active snapshot transactions (watermark = min read_ts).
    txn_tracker: TxnTracker,
    /// SERIALIZABLE SSI doom registry (rw-antidependency first-cut).
    ssi: Mutex<SsiRegistry>,
    /// Serializes OCC validation + commit assignment.
    txn_commit_mu: Mutex<()>,
    /// Latest committed timestamp per user key (OCC index).
    last_commit: Mutex<BTreeMap<Key, CommitTs>>,
    /// Registered table schemas for secondary-index projection.
    schemas: RwLock<BTreeMap<String, TableSchema>>,
    /// In-memory object comments (`COMMENT ON` / `obj_description` / `col_description`).
    comments: Arc<parking_lot::RwLock<BTreeMap<String, String>>>,
    /// In-memory HNSW graphs keyed by index name (snapshotted under `HNSW_<name>`).
    hnsw: RwLock<HashMap<String, Arc<HnswIndex>>>,
    /// RBAC / user catalog (`data_dir/AUTH`).
    auth: SharedAuthCatalog,
    /// Per-table statistics for the cost-based optimizer.
    stats: StatsCatalog,
    /// Multi-engine router (LSM default + optional per-table B-Tree stores).
    storage: Arc<crate::storage::StorageManager>,
    /// ARIES-style transactional WAL (`data_dir/TXN_WAL`).
    ///
    /// Write-sets are synced here **before** memtable apply so a crash between
    /// Commit durability and LSM apply is recovered via Redo on open.
    aries_wal: Mutex<Option<WalManager>>,
    /// Buffer pool manager (Direct I/O page cache); `None` when `bpm_pool_size == 0`.
    buffer_pool: Option<Arc<BufferPoolManager>>,
    /// Shared remote-object-storage manifest (source of truth when decoupled).
    manifest: Option<Arc<ManifestManager>>,
    /// Weak handle to networked Raft when this engine is part of a cluster.
    ///
    /// Uses [`Weak`] to avoid an `Engine ↔ RaftConsensus` reference cycle.
    /// When set, local proposes are gated / proxied through quorum Raft.
    distributed_raft: Mutex<Option<Weak<RaftConsensus>>>,
    /// Prepared 2PC write-sets (durable via [`RaftCommand::TxnPrepare`]).
    prepared_2pc: Mutex<std::collections::HashMap<u64, Vec<(Key, Option<Value>)>>>,
    /// Exclusive prepare locks for cross-shard 2PC.
    locks_2pc: Mutex<std::collections::HashMap<Key, u64>>,
    /// Chaos: next 2PC prepare fails.
    twopc_fail_prepare: AtomicBool,
    /// Chaos: after durable PREPARE, fail ACK / subsequent commit/abort.
    twopc_crash_after_prepare: AtomicBool,
    /// Logical shard id for [`crate::dtxn::EngineShard`] (defaults to 0).
    shard_id: AtomicU64,
    /// MPP worker directory (local slots or cluster endpoints) when `mpp_enabled`.
    mpp_workers: Mutex<Vec<crate::mpp::WorkerEndpoint>>,
    /// Optional test / cluster [`FragmentDispatcher`] attached to new coordinators.
    mpp_dispatcher: Mutex<Option<Arc<dyn crate::mpp::FragmentDispatcher>>>,
    /// Shared 2PC coordinator with durable decision log under `data_dir`.
    txn_coordinator: Arc<TransactionCoordinator>,
}

impl TakyonicEngine {
    /// Open (or create) an engine at the configured data/WAL directories.
    ///
    /// When [`Config::object_store_root`] is set, opens a
    /// [`LocalFileBackend`] and loads the shared storage manifest before
    /// attaching the two-tier DiskManager.
    pub fn open(config: Config) -> Result<Self> {
        let store = if let Some(root) = &config.object_store_root {
            Some(Arc::new(LocalFileBackend::open(root)?) as Arc<dyn ObjectStorage>)
        } else {
            None
        };
        Self::open_inner(config, store)
    }

    /// Open with an explicit Tier-2 [`ObjectStorage`] (S3 mock, MinIO, aws-sdk, …).
    ///
    /// On open the engine fetches `manifest/CURRENT.json` as the source of truth
    /// for SST / B-Tree / pages object keys.
    pub fn open_with_object_storage(
        config: Config,
        store: Arc<dyn ObjectStorage>,
    ) -> Result<Self> {
        Self::open_inner(config, Some(store))
    }

    fn open_inner(config: Config, remote: Option<Arc<dyn ObjectStorage>>) -> Result<Self> {
        config.validate()?;
        fs::create_dir_all(&config.data_dir)?;
        fs::create_dir_all(&config.wal_dir)?;

        let (manifest, pages_key, pages_prefix, chunk_size) = if let Some(store) = &remote {
            let mgr = Arc::new(ManifestManager::open(Arc::clone(store))?);
            let cur = mgr.current();
            let key = if cur.pages_key.is_empty() {
                REMOTE_PAGES_KEY.to_string()
            } else {
                cur.pages_key.clone()
            };
            let prefix = if cur.pages_prefix.is_empty() {
                PAGES_V2_PREFIX.to_string()
            } else {
                cur.pages_prefix.clone()
            };
            let chunk = match cur.pages_layout {
                crate::manifest::PagesLayout::ChunkV2 { chunk_size } if chunk_size > 0 => {
                    chunk_size as usize
                }
                _ => config.object_pages_chunk_bytes,
            };
            info!(
                version = cur.version,
                sst = cur.sstables.len(),
                pages = %key,
                pages_prefix = %prefix,
                chunk_size = chunk,
                "engine open: loaded remote storage manifest"
            );
            (Some(mgr), key, prefix, chunk)
        } else {
            (
                None,
                REMOTE_PAGES_KEY.to_string(),
                PAGES_V2_PREFIX.to_string(),
                config.object_pages_chunk_bytes,
            )
        };

        let registry = Arc::new(SstRegistry::new());
        let metrics = Arc::new(EngineMetrics::new());
        let buffer_pool = if config.bpm_pool_size > 0 {
            let disk = Arc::new(DiskManager::open_with_remote_layout(
                &config.data_dir,
                config.bpm_page_size,
                remote.clone(),
                pages_key,
                pages_prefix,
                chunk_size,
            )?);
            let bpm = BufferPoolManager::new_with_metrics(
                disk,
                config.bpm_pool_size,
                if config.bpm_lru_k == 0 {
                    DEFAULT_LRU_K
                } else {
                    config.bpm_lru_k
                },
                Arc::clone(&metrics),
            )?;
            registry.set_buffer_pool(Arc::clone(&bpm));
            Some(bpm)
        } else {
            None
        };
        let manager = Arc::new(SstManager::new(
            registry,
            config.data_dir.clone(),
            config.block_size_bytes,
            DEFAULT_LEVEL_COUNT,
            1,
        )?);
        manager.set_max_sst_bytes(config.max_sst_bytes);
        if let Some(store) = &remote {
            manager.attach_remote(Arc::clone(store));
        }
        let admission = Arc::new(AdmissionController::new(Arc::clone(&manager), &config)?);
        let compaction = CompactionEngine::new(Arc::clone(&manager), &config)?;

        recover_existing_ssts(&manager)?;
        if let Some(mgr) = &manifest {
            hydrate_ssts_from_manifest(&manager, &mgr.current())?;
        }
        let (wal, memtable, mut next_seq, wal_segment) = recover_wal(&config.wal_dir)?;
        // WAL segments are pruned after flush/close, so replay alone can
        // under-estimate the sequence domain. The durable SEQNO marker keeps
        // seq monotonic across restarts (newest-wins depends on it).
        next_seq = next_seq.max(read_seq_marker(&config.wal_dir));

        // ARIES Redo: replay committed txn write-sets that may not yet be in
        // the LSM WAL / memtable (crash after Commit fsync, before apply).
        let aries_batches = WalManager::recover_committed(&config.data_dir)?;
        let aries_ops: usize = aries_batches.iter().map(|(_, ops)| ops.len()).sum();
        let mut wal = wal;
        if aries_ops > 0 {
            info!(
                commits = aries_batches.len(),
                ops = aries_ops,
                "ARIES Redo applying committed txn WAL records"
            );
            WalManager::redo_into_memtable(&aries_batches, &memtable, &mut next_seq);
            // Handoff into the LSM WAL so we can safely checkpoint/truncate TXN_WAL.
            for (_txn_id, ops) in &aries_batches {
                for op in ops {
                    let seq = next_seq;
                    next_seq = next_seq.saturating_add(1);
                    match op {
                        WalRecord::Insert { key, value, .. }
                        | WalRecord::Update { key, value, .. } => {
                            wal.append(&Entry::put(
                                Key::new(key.clone()),
                                Value::new(value.clone()),
                                seq,
                            ))?;
                        }
                        WalRecord::Delete { key, .. } => {
                            wal.append(&Entry::delete(Key::new(key.clone()), seq))?;
                        }
                        WalRecord::Commit { .. } => {}
                    }
                }
            }
            wal.sync()?;
            // Truncate redo log — durable state now lives in LSM WAL / memtable.
            let _ = WalManager::create(&config.data_dir)?;
        }
        let aries_wal = WalManager::open(&config.data_dir)?;

        let config_wal_dir = config.wal_dir.clone();
        let raft = Arc::new(LocalRaftNode::new(
            wal,
            memtable,
            next_seq,
            Arc::clone(&metrics),
        ));

        let metrics_manager = if config.metrics_enabled {
            let addr: SocketAddr = config.metrics_bind.parse().map_err(|e| {
                TakyonicError::Config(format!("metrics_bind {}: {e}", config.metrics_bind))
            })?;
            let mut mgr = MetricsManager::new(Arc::clone(&metrics));
            if let Some(bpm) = &buffer_pool {
                mgr = mgr.with_bpm(Arc::clone(bpm));
            }
            Some(mgr.start_http(addr)?)
        } else {
            None
        };

        let txn_coordinator = TransactionCoordinator::open(
            &config.data_dir,
            Some(Arc::clone(&metrics)),
        )?;

        let engine = Self {
            config,
            raft: Mutex::new(Some(raft)),
            wal_dir: config_wal_dir,
            wal_segment: AtomicU64::new(wal_segment),
            admission,
            manager,
            compaction: Mutex::new(Some(compaction)),
            flush_mu: Mutex::new(()),
            closed: AtomicBool::new(false),
            admission_timeout: DEFAULT_ADMISSION_TIMEOUT,
            metrics,
            metrics_manager: Mutex::new(metrics_manager),
            txn_tracker: TxnTracker::new(),
            ssi: Mutex::new(SsiRegistry::default()),
            txn_commit_mu: Mutex::new(()),
            last_commit: Mutex::new(BTreeMap::new()),
            schemas: RwLock::new(BTreeMap::new()),
            comments: Arc::new(RwLock::new(BTreeMap::new())),
            hnsw: RwLock::new(HashMap::new()),
            auth: Arc::new(RwLock::new(AuthCatalog::new())),
            stats: StatsCatalog::new(),
            storage: Arc::new(crate::storage::StorageManager::router_only()),
            aries_wal: Mutex::new(Some(aries_wal)),
            buffer_pool,
            manifest,
            distributed_raft: Mutex::new(None),
            prepared_2pc: Mutex::new(std::collections::HashMap::new()),
            locks_2pc: Mutex::new(std::collections::HashMap::new()),
            twopc_fail_prepare: AtomicBool::new(false),
            twopc_crash_after_prepare: AtomicBool::new(false),
            shard_id: AtomicU64::new(0),
            mpp_workers: Mutex::new(Vec::new()),
            mpp_dispatcher: Mutex::new(None),
            txn_coordinator,
        };

        if engine.config.mpp_enabled {
            // Default: 3 in-process partition slots (single-node MPP simulation).
            engine.set_mpp_workers(default_local_mpp_workers(3));
        }

        // Seed an empty remote manifest on first attach; otherwise keep the
        // cluster's CURRENT.json as the source of truth (do not bump on open).
        if let Some(mgr) = &engine.manifest {
            if mgr.current().version == 0 {
                let _ = engine.publish_storage_manifest();
            }
        }

        // Reload durable table/index catalog into memory.
        let loaded = crate::catalog::load_catalog(&engine.config.data_dir)?;
        {
            let mut schemas = engine.schemas.write();
            *schemas = loaded;
            for schema in schemas.values() {
                let _ = engine.storage.register_table(schema);
                engine.stats.register_table(schema);
            }
        }
        // Durable sequences / SERIAL (survive reopen).
        crate::sql::load_sequences(&engine.config.data_dir)?;
        // B-Tree mirrors are process-local; rebuild them from durable LSM so
        // cold reopen sees the same rows (LSM = source of truth).
        {
            let btree_tables: Vec<String> = engine
                .schemas
                .read()
                .values()
                .filter(|s| s.storage_engine == crate::storage::StorageEngineKind::BTree)
                .map(|s| s.name.clone())
                .collect();
            for name in btree_tables {
                if let Err(e) = engine.hydrate_btree_from_lsm(&name) {
                    warn!(table = %name, error = %e, "B-Tree hydrate from LSM failed");
                }
            }
        }
        // Load HNSW snapshots for every vector index.
        {
            let schemas = engine.schemas.read();
            let mut hnsw = engine.hnsw.write();
            for schema in schemas.values() {
                for idx in &schema.indexes {
                    if let Some(spec) = &idx.vector {
                        let graph = HnswIndex::load(
                            &engine.config.data_dir,
                            &idx.name,
                            spec.dimension,
                            spec.metric,
                        )?;
                        hnsw.insert(idx.name.clone(), Arc::new(graph));
                    }
                }
            }
        }
        // Load RBAC catalog (seeds bootstrap postgres when missing).
        {
            let auth = AuthCatalog::load(&engine.config.data_dir)?;
            *engine.auth.write() = auth;
        }
        // Overlay persisted COMMENT ON descriptions.
        {
            let comments = load_comments(&engine.config.data_dir)?;
            *engine.comments.write() = comments;
        }
        // Overlay persisted ANALYZE statistics (row counts, NDV, MCV, histograms).
        let persisted = stats::load_stats(&engine.config.data_dir)?;
        for (table, st) in persisted {
            engine.stats.replace(&table, st);
        }

        engine.maybe_flush()?;
        engine.kick_compaction();
        info!(
            data_dir = %engine.config.data_dir.display(),
            wal_dir = %engine.wal_dir.display(),
            remote = engine.manifest.is_some(),
            "TakyonicEngine opened (group-commit + local Raft)"
        );
        Ok(engine)
    }

    /// Attach networked Raft so OCC commits / puts require leader + quorum.
    ///
    /// Followers reject mutating local commits with [`TakyonicError::NotLeader`].
    /// Leaders append to the Raft log and wait for majority replication before
    /// the state machine apply (ARIES txn WAL is still synced first).
    pub fn attach_raft_node(self: &Arc<Self>, raft: &Arc<RaftConsensus>) {
        *self.distributed_raft.lock() = Some(Arc::downgrade(raft));
        info!(
            node = raft.id(),
            "engine attached to distributed Raft (quorum commits)"
        );
    }

    /// Set the logical 2PC shard id (typically the Raft node id).
    pub fn set_shard_id(&self, id: u64) {
        self.shard_id.store(id, Ordering::Relaxed);
    }

    /// Shared 2PC coordinator (durable decision log under this engine's `data_dir`).
    pub fn txn_coordinator(&self) -> Arc<TransactionCoordinator> {
        Arc::clone(&self.txn_coordinator)
    }

    /// Logical 2PC shard id.
    pub fn shard_id(&self) -> u64 {
        self.shard_id.load(Ordering::Relaxed)
    }

    /// Chaos: next distributed prepare fails.
    pub fn inject_twopc_prepare_failure(&self, on: bool) {
        self.twopc_fail_prepare.store(on, Ordering::SeqCst);
    }

    /// Chaos: durable PREPARE then fail ACK / phase-2 RPCs.
    pub fn inject_twopc_crash_after_prepare(&self, on: bool) {
        self.twopc_crash_after_prepare.store(on, Ordering::SeqCst);
    }

    /// Replace the MPP worker directory (cluster membership or local sim slots).
    pub fn set_mpp_workers(&self, workers: Vec<crate::mpp::WorkerEndpoint>) {
        *self.mpp_workers.lock() = workers;
    }

    /// Current MPP / partition worker directory (empty when unset).
    pub fn mpp_workers(&self) -> Vec<crate::mpp::WorkerEndpoint> {
        self.mpp_workers.lock().clone()
    }

    /// Attach a pluggable fragment dispatcher used by [`Self::mpp_coordinator`].
    ///
    /// When set, this overrides the auto-wired gRPC dispatcher (tests / custom
    /// cluster wiring). Pass `None` to clear.
    pub fn set_mpp_dispatcher(
        &self,
        dispatcher: Option<Arc<dyn crate::mpp::FragmentDispatcher>>,
    ) {
        *self.mpp_dispatcher.lock() = dispatcher;
    }

    /// Number of MPP workers / partitions (0 when MPP disabled / empty).
    pub fn mpp_worker_count(&self) -> usize {
        if !self.config.mpp_enabled {
            return 0;
        }
        self.mpp_workers.lock().len()
    }

    /// Build an in-process [`crate::mpp::Coordinator`] when `mpp_enabled`.
    pub fn mpp_coordinator(self: &Arc<Self>) -> Result<crate::mpp::Coordinator> {
        if !self.config.mpp_enabled {
            return Err(TakyonicError::Config(
                "MPP is disabled (set Config::mpp_enabled)".into(),
            ));
        }
        let workers = self.mpp_workers.lock().clone();
        if workers.is_empty() {
            return Err(TakyonicError::Config(
                "MPP enabled but no workers configured".into(),
            ));
        }
        let shuffle = Arc::new(crate::shuffle::ShuffleManager::new(
            crate::shuffle::DEFAULT_SHUFFLE_BUFFER,
            Some(Arc::clone(&self.metrics)),
        ));
        let coord = crate::mpp::Coordinator::local(
            Arc::clone(self),
            shuffle,
            workers,
        );
        if let Some(d) = self.mpp_dispatcher.lock().clone() {
            coord.set_dispatcher(d);
        } else {
            coord.attach_grpc_dispatcher(Some(self.shard_id()));
        }
        Ok(coord)
    }

    /// Insert or overwrite `key`.
    ///
    /// Submits a Raft proposal; visibility requires group-commit durability.
    pub fn put(&self, key: impl Into<Key>, value: impl Into<Value>) -> Result<()> {
        self.propose(RaftCommand::put(key, value))
    }

    /// Delete `key` by writing a tombstone.
    pub fn delete(&self, key: impl Into<Key>) -> Result<()> {
        self.propose(RaftCommand::delete(key))
    }

    /// Point lookup at the latest committed version.
    pub fn get(&self, key: &Key) -> Result<Option<Value>> {
        Ok(self.get_at_with_ts(key, u64::MAX)?.0)
    }

    /// Begin a snapshot-isolation transaction at the current apply watermark.
    ///
    /// Takes `&Arc<Self>` so the returned [`Transaction`] can outlive a single
    /// stack borrow and be stored in session state.
    pub fn begin(self: &Arc<Self>) -> Result<Transaction> {
        self.begin_with_isolation(IsolationLevel::Snapshot)
    }

    /// Begin a transaction at the given isolation level.
    pub fn begin_with_isolation(
        self: &Arc<Self>,
        isolation: IsolationLevel,
    ) -> Result<Transaction> {
        self.ensure_open()?;
        let read_ts = self
            .raft_node()?
            .last_applied()
            .min(u64::MAX.saturating_sub(1));
        let txn_id = self.txn_tracker.begin(read_ts);
        self.metrics.txn_begin();
        self.publish_watermark();
        Ok(Transaction::new_with_isolation(
            Arc::clone(self),
            txn_id,
            read_ts,
            isolation,
        ))
    }

    pub(crate) fn ssi_register(&self, txn_id: u64) {
        self.ssi.lock().register(txn_id);
    }

    pub(crate) fn ssi_note_read(&self, txn_id: u64, key: &Key) {
        self.ssi.lock().note_read(txn_id, key);
    }

    /// Snapshot read: highest version with `commit_ts <= read_ts`.
    ///
    /// Returns `(value, seen_ts)` where `seen_ts` is 0 if the key was absent.
    pub fn get_at_with_ts(
        &self,
        key: &Key,
        read_ts: CommitTs,
    ) -> Result<(Option<Value>, CommitTs)> {
        self.ensure_open()?;
        // Multi-engine: B-Tree tables are authoritative in the storage router.
        if let Some(table) = crate::schema::table_from_user_key(key) {
            if self.storage.engine_kind(&table) == crate::storage::StorageEngineKind::BTree {
                if let Ok(store) = self.storage.btree(&table) {
                    return Ok(match store.get_at(key, read_ts) {
                        Some(e) if e.tombstone => (None, e.seq),
                        Some(e) => (e.value, e.seq),
                        None => (None, 0),
                    });
                }
            }
        }

        let mut best: Option<crate::types::Entry> = None;

        let node = self.raft_node()?;
        if let Some(e) = node.memtable().get_at(key, read_ts) {
            best = Some(e);
        }
        drop(node);

        let levels = self.manager.level_count();
        for level in 0..levels {
            let mut files = self.manager.level_files(level);
            if level == 0 {
                files.sort_by_key(|meta| std::cmp::Reverse(meta.id));
            }
            for meta in files {
                if level > 0 && (key < &meta.smallest || key > &meta.largest) {
                    continue;
                }
                let Some(pin) = self.manager.registry().pin(meta.id) else {
                    continue;
                };
                if let Some(entry) = pin.reader().get_entry_at(key, read_ts)? {
                    match &best {
                        Some(b) if b.seq >= entry.seq => {}
                        _ => best = Some(entry),
                    }
                }
            }
        }

        match best {
            Some(e) if e.tombstone => Ok((None, e.seq)),
            Some(e) => Ok((e.value, e.seq)),
            None => Ok((None, 0)),
        }
    }

    pub(crate) fn end_transaction(&self, txn_id: u64) {
        self.ssi.lock().unregister(txn_id);
        self.txn_tracker.end(txn_id);
        self.metrics.txn_end();
        self.publish_watermark();
    }

    /// Allocate a remote/local txn id at the current apply watermark.
    pub(crate) fn begin_txn_id(&self) -> Result<(u64, CommitTs)> {
        self.ensure_open()?;
        let read_ts = self
            .raft_node()?
            .last_applied()
            .min(u64::MAX.saturating_sub(1));
        let txn_id = self.txn_tracker.begin(read_ts);
        self.metrics.txn_begin();
        self.publish_watermark();
        Ok((txn_id, read_ts))
    }

    pub(crate) fn commit_transaction(
        &self,
        txn_id: u64,
        read_ts: CommitTs,
        isolation: IsolationLevel,
        reads: &BTreeMap<Key, CommitTs>,
        writes: &BTreeMap<Key, WriteOp>,
        stats_edits: &[StatsEdit],
    ) -> Result<CommitTs> {
        self.ensure_open()?;
        let t0 = Instant::now();
        if writes.is_empty() {
            self.end_transaction(txn_id);
            return Ok(read_ts);
        }

        // Serialize OCC validate with propose so two overlapping SI
        // transactions cannot both pass validation before either commits.
        let _occ = self.txn_commit_mu.lock();
        if isolation.is_serializable() && self.ssi.lock().is_doomed(txn_id) {
            self.end_transaction(txn_id);
            return Err(TakyonicError::Conflict(
                "SSI conflict: concurrent write to a key this transaction read \
                 (write-skew / rw-antidependency)"
                    .into(),
            ));
        }
        let ops = self.prepare_txn_commit_unlocked(txn_id, read_ts, reads, writes)?;
        if isolation.is_serializable() {
            let wk = crate::txn::write_keys(writes);
            self.ssi.lock().doom_concurrent_readers(txn_id, &wk);
        }
        // WAL-before-data: durable Commit must hit disk before memtable apply.
        self.log_txn_wal(txn_id, writes)?;

        let commit_ts = if let Some(raft) = self.upgrade_distributed_raft() {
            // Cluster path: quorum Raft log → apply; reject / hint on followers.
            if let Err(e) = self.require_leader(&raft) {
                self.end_transaction(txn_id);
                return Err(e);
            }
            match self.block_on_propose(&raft, RaftCommand::txn_batch(ops)) {
                Ok(commit_ts) => {
                    self.finalize_txn_commit(txn_id, commit_ts, writes, stats_edits);
                    commit_ts
                }
                Err(e) => {
                    self.end_transaction(txn_id);
                    return Err(e);
                }
            }
        } else {
            let node = self.raft_node()?;
            let commit_ts = node.propose(RaftCommand::txn_batch(ops))?;
            self.mirror_btree_writes(writes, commit_ts);
            self.note_committed_keys(writes.keys().cloned(), commit_ts);
            for edit in stats_edits {
                self.apply_stats_edit(edit);
            }
            self.maybe_flush_node(&node)?;
            self.end_transaction(txn_id);
            commit_ts
        };
        drop(_occ);
        self.metrics.record_txn_commit(t0.elapsed());
        Ok(commit_ts)
    }

    /// OCC-validate + admit a write-set; returns the Raft command ops.
    ///
    /// On conflict / admission failure the transaction is ended. Callers that
    /// propose via networked Raft must serialize this with propose (e.g. an
    /// async mutex) then call [`Self::finalize_txn_commit`] on success.
    pub(crate) fn prepare_txn_commit(
        &self,
        txn_id: u64,
        read_ts: CommitTs,
        reads: &BTreeMap<Key, CommitTs>,
        writes: &BTreeMap<Key, WriteOp>,
    ) -> Result<Vec<(Key, Option<Value>)>> {
        let _occ = self.txn_commit_mu.lock();
        self.prepare_txn_commit_unlocked(txn_id, read_ts, reads, writes)
    }

    fn prepare_txn_commit_unlocked(
        &self,
        txn_id: u64,
        read_ts: CommitTs,
        reads: &BTreeMap<Key, CommitTs>,
        writes: &BTreeMap<Key, WriteOp>,
    ) -> Result<Vec<(Key, Option<Value>)>> {
        // OCC: any read-set key committed after our snapshot ⇒ conflict.
        {
            let last = self.last_commit.lock();
            for key in reads.keys().chain(writes.keys()) {
                if let Some(&ts) = last.get(key) {
                    if ts > read_ts {
                        drop(last);
                        self.end_transaction(txn_id);
                        return Err(TakyonicError::Conflict(format!(
                            "key {:?} committed at {ts} > read_ts {read_ts}",
                            String::from_utf8_lossy(key.as_bytes())
                        )));
                    }
                }
            }
        }

        let ops: Vec<(Key, Option<Value>)> = writes
            .iter()
            .map(|(k, op)| match op {
                WriteOp::Put(v) => (k.clone(), Some(v.clone())),
                WriteOp::Delete => (k.clone(), None),
            })
            .collect();

        match self
            .admission
            .acquire_timeout(ops.len().max(1) as u64, self.admission_timeout)?
        {
            AdmissionOutcome::Acquired => {}
            AdmissionOutcome::TimedOut => {
                self.end_transaction(txn_id);
                return Err(TakyonicError::Admission(
                    "timed out waiting for write admission".into(),
                ));
            }
        }
        Ok(ops)
    }

    /// Append OCC write-set + `Commit` to the ARIES txn WAL and `sync_data`.
    ///
    /// Must run **before** the write-set is applied to the memtable / LSM.
    pub(crate) fn log_txn_wal(
        &self,
        txn_id: u64,
        writes: &BTreeMap<Key, WriteOp>,
    ) -> Result<()> {
        if writes.is_empty() {
            return Ok(());
        }
        let records = records_from_writes(writes);
        let mut guard = self.aries_wal.lock();
        let wal = guard
            .as_mut()
            .ok_or_else(|| TakyonicError::Engine("txn WAL not open".into()))?;
        wal.append_committed_txn(txn_id, &records)?;
        Ok(())
    }

    /// Record OCC timestamps + stats after a successful networked Raft commit.
    ///
    /// Call only after the write-set is already applied (propose has returned).
    /// Prefer ending the txn *after* any flush that might run under the apply path.
    pub(crate) fn finalize_txn_commit(
        &self,
        txn_id: u64,
        commit_ts: CommitTs,
        writes: &BTreeMap<Key, WriteOp>,
        stats_edits: &[StatsEdit],
    ) {
        self.mirror_btree_writes(writes, commit_ts);
        self.note_committed_keys(writes.keys().cloned(), commit_ts);
        for edit in stats_edits {
            self.apply_stats_edit(edit);
        }
        self.end_transaction(txn_id);
    }

    /// Mirror committed keys belonging to B-Tree tables into [`StorageManager`].
    ///
    /// Durable bytes already live in LSM via the Raft apply path; this keeps
    /// the process-local B-Tree cache coherent for subsequent SI reads.
    fn mirror_btree_writes(&self, writes: &BTreeMap<Key, WriteOp>, commit_ts: CommitTs) {
        for (key, op) in writes {
            let Some(table) = crate::schema::table_from_user_key(key) else {
                continue;
            };
            if self.storage.engine_kind(&table) != crate::storage::StorageEngineKind::BTree {
                continue;
            }
            let Ok(store) = self.storage.btree(&table) else {
                continue;
            };
            match op {
                WriteOp::Put(v) => store.apply(Entry::put(key.clone(), v.clone(), commit_ts)),
                WriteOp::Delete => store.apply(Entry::delete(key.clone(), commit_ts)),
            }
        }
    }

    fn apply_stats_edit(&self, edit: &StatsEdit) {
        match edit {
            StatsEdit::Insert {
                table,
                index_values,
            } => self.stats.on_insert(table, index_values),
            StatsEdit::Delete {
                table,
                index_values,
            } => self.stats.on_delete(table, index_values),
            StatsEdit::VectorUpsert {
                index,
                pk,
                vector_text,
            } => {
                if let Err(e) = self.hnsw_upsert(index, pk, vector_text) {
                    warn!(index = %index, pk = %pk, error = %e, "HNSW upsert failed");
                }
            }
            StatsEdit::VectorDelete { index, pk } => {
                self.hnsw_delete(index, pk);
            }
        }
    }

    /// Apply deferred transaction stats / vector edits after a successful commit.
    pub fn apply_txn_stats_edits(&self, edits: &[StatsEdit]) {
        for edit in edits {
            self.apply_stats_edit(edit);
        }
    }

    /// Look up an in-memory HNSW graph by index name.
    pub fn hnsw_index(&self, name: &str) -> Option<Arc<HnswIndex>> {
        self.hnsw.read().get(name).cloned()
    }

    /// k-NN search against a named HNSW index.
    pub fn hnsw_search(
        &self,
        index: &str,
        query: &VectorValue,
        k: usize,
    ) -> Result<Vec<(f32, String)>> {
        let graph = self.hnsw_index(index).ok_or_else(|| {
            TakyonicError::Sql(format!("unknown vector index `{index}`"))
        })?;
        graph.search_knn(query, k)
    }

    fn hnsw_upsert(&self, index: &str, pk: &str, vector_text: &str) -> Result<()> {
        let graph = self.hnsw_index(index).ok_or_else(|| {
            TakyonicError::Sql(format!("unknown vector index `{index}`"))
        })?;
        let vec = VectorValue::from_text(vector_text)?;
        graph.insert(pk, vec)?;
        Ok(())
    }

    fn hnsw_delete(&self, index: &str, pk: &str) {
        if let Some(graph) = self.hnsw_index(index) {
            graph.delete(pk);
        }
    }

    fn save_all_hnsw(&self) -> Result<()> {
        let hnsw = self.hnsw.read();
        for graph in hnsw.values() {
            graph.save(&self.config.data_dir)?;
        }
        Ok(())
    }

    /// Update the OCC index for keys applied at `commit_ts` (local or replica).
    pub(crate) fn note_committed_keys(
        &self,
        keys: impl IntoIterator<Item = Key>,
        commit_ts: CommitTs,
    ) {
        let mut last = self.last_commit.lock();
        for key in keys {
            last.insert(key, commit_ts);
        }
    }

    /// Phase-1 2PC prepare: OCC + locks + durable [`RaftCommand::TxnPrepare`].
    pub fn twopc_prepare(
        &self,
        txn_id: u64,
        read_ts: CommitTs,
        writes: &[(Key, Option<Value>)],
        reads: &[(Key, CommitTs)],
    ) -> Result<()> {
        self.ensure_open()?;
        if self.twopc_fail_prepare.load(Ordering::SeqCst) {
            return Err(TakyonicError::Network(format!(
                "shard {}: injected prepare failure",
                self.shard_id()
            )));
        }

        let _occ = self.txn_commit_mu.lock();
        {
            let last = self.last_commit.lock();
            for (key, _) in reads {
                if let Some(&ts) = last.get(key) {
                    if ts > read_ts {
                        return Err(TakyonicError::Conflict(format!(
                            "shard {}: key {:?} committed at {ts} > read_ts {read_ts}",
                            self.shard_id(),
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
                            self.shard_id(),
                            String::from_utf8_lossy(key.as_bytes())
                        )));
                    }
                }
            }
        }

        {
            let mut locks = self.locks_2pc.lock();
            for (k, _) in writes {
                if let Some(owner) = locks.get(k) {
                    if *owner != txn_id {
                        return Err(TakyonicError::Conflict(format!(
                            "shard {}: key locked by txn {owner}",
                            self.shard_id()
                        )));
                    }
                }
            }
            for (k, _) in writes {
                locks.insert(k.clone(), txn_id);
            }
        }

        let cmd = RaftCommand::txn_prepare(txn_id, read_ts, writes.to_vec());
        if let Err(e) = self.propose_twopc(cmd) {
            let mut locks = self.locks_2pc.lock();
            for (k, _) in writes {
                if locks.get(k) == Some(&txn_id) {
                    locks.remove(k);
                }
            }
            return Err(e);
        }

        if self.twopc_crash_after_prepare.load(Ordering::SeqCst) {
            return Err(TakyonicError::Network(format!(
                "shard {}: crashed after PREPARED",
                self.shard_id()
            )));
        }
        Ok(())
    }

    /// Phase-2 2PC commit: durable [`RaftCommand::TxnCommit`] + LSM apply.
    pub fn twopc_commit(&self, txn_id: u64, commit_ts: CommitTs) -> Result<()> {
        self.ensure_open()?;
        if self.twopc_crash_after_prepare.load(Ordering::SeqCst) {
            return Err(TakyonicError::Network(format!(
                "shard {}: down (cannot commit)",
                self.shard_id()
            )));
        }
        let writes = self
            .prepared_2pc
            .lock()
            .get(&txn_id)
            .cloned()
            .ok_or_else(|| {
                TakyonicError::Engine(format!(
                    "shard {}: commit for unknown prepared txn {txn_id}",
                    self.shard_id()
                ))
            })?;
        self.propose_twopc(RaftCommand::txn_commit(txn_id, commit_ts, writes))
    }

    /// Phase-2 2PC abort: durable [`RaftCommand::TxnAbort`].
    pub fn twopc_abort(&self, txn_id: u64) -> Result<()> {
        self.ensure_open()?;
        if self.twopc_crash_after_prepare.load(Ordering::SeqCst) {
            return Err(TakyonicError::Network(format!(
                "shard {}: down (cannot abort)",
                self.shard_id()
            )));
        }
        self.propose_twopc(RaftCommand::txn_abort(txn_id))
    }

    /// Txn ids still in `Prepared` (recovery / coordinator query).
    pub fn twopc_orphaned_prepared(&self) -> Vec<u64> {
        self.prepared_2pc.lock().keys().copied().collect()
    }

    /// Rebuild in-memory prepared/locks from the durable Raft log (crash recovery).
    ///
    /// Committed writes are assumed already applied to the LSM; this only
    /// restores orphaned `PREPARED` records so the coordinator can abort/commit.
    pub fn twopc_recover_from_raft_log(&self) -> Result<()> {
        self.ensure_open()?;
        let Some(raft) = self.upgrade_distributed_raft() else {
            // Embedded stand-in has no durable 2PC command log to scan.
            return Ok(());
        };
        let log = raft.log();
        let start = log.snapshot_meta().last_included_index.saturating_add(1);
        let last = log.last_index();

        let mut prepared = std::collections::HashMap::new();
        let mut locks = std::collections::HashMap::new();
        for idx in start..=last {
            let Some(entry) = log.entry(idx) else {
                continue;
            };
            let Ok(cmd) = RaftCommand::decode(entry.command.clone()) else {
                continue;
            };
            match cmd {
                RaftCommand::TxnPrepare { txn_id, ops, .. } => {
                    for (k, _) in &ops {
                        locks.insert(k.clone(), txn_id);
                    }
                    prepared.insert(txn_id, ops);
                }
                RaftCommand::TxnCommit { txn_id, ops, .. } => {
                    prepared.remove(&txn_id);
                    for (k, _) in &ops {
                        locks.remove(k);
                    }
                }
                RaftCommand::TxnAbort { txn_id } => {
                    if let Some(ops) = prepared.remove(&txn_id) {
                        for (k, _) in &ops {
                            locks.remove(k);
                        }
                    }
                }
                _ => {}
            }
        }
        *self.prepared_2pc.lock() = prepared;
        *self.locks_2pc.lock() = locks;
        Ok(())
    }

    /// Propose a 2PC command; local stand-in also applies prepared/abort side effects.
    fn propose_twopc(&self, command: RaftCommand) -> Result<()> {
        if let Some(raft) = self.upgrade_distributed_raft() {
            self.require_leader(&raft)?;
            let _ = self.block_on_propose(&raft, command)?;
            return Ok(());
        }
        let node = self.raft_node()?;
        let _ = node.propose(command.clone())?;
        self.apply_twopc_side_effect(&command)?;
        Ok(())
    }

    fn apply_twopc_side_effect(&self, command: &RaftCommand) -> Result<()> {
        match command {
            RaftCommand::TxnPrepare { txn_id, ops, .. } => {
                {
                    let mut locks = self.locks_2pc.lock();
                    for (k, _) in ops {
                        locks.insert(k.clone(), *txn_id);
                    }
                }
                self.prepared_2pc.lock().insert(*txn_id, ops.clone());
                Ok(())
            }
            RaftCommand::TxnAbort { txn_id } => {
                if let Some(writes) = self.prepared_2pc.lock().remove(txn_id) {
                    let mut locks = self.locks_2pc.lock();
                    for (k, _) in &writes {
                        locks.remove(k);
                    }
                }
                Ok(())
            }
            RaftCommand::TxnCommit {
                txn_id,
                commit_ts,
                ops,
            } => {
                self.prepared_2pc.lock().remove(txn_id);
                {
                    let mut locks = self.locks_2pc.lock();
                    for (k, _) in ops {
                        locks.remove(k);
                    }
                }
                self.note_committed_keys(ops.iter().map(|(k, _)| k.clone()), *commit_ts);
                Ok(())
            }
            _ => Ok(()),
        }
    }

    /// Register a table schema (enables `put_record` / CBO queries).
    ///
    /// Persists the catalog to `data_dir/CATALOG`. With distributed Raft attached,
    /// proposes [`RaftCommand::CatalogUpsert`] and installs on apply (all replicas).
    pub fn register_table(&self, schema: TableSchema) -> Result<()> {
        self.ensure_open()?;
        self.replicate_or_install_schema(schema)
    }

    /// Install one table into the local catalog (stats, storage router, `CATALOG` file).
    fn install_catalog_schema(&self, schema: TableSchema) -> Result<()> {
        self.stats.register_table(&schema);
        self.storage.register_table(&schema)?;
        let name = schema.name.clone();
        let is_btree = schema.storage_engine == crate::storage::StorageEngineKind::BTree;
        let mut schemas = self.schemas.write();
        schemas.insert(schema.name.clone(), schema);
        crate::catalog::save_catalog(&self.config.data_dir, &schemas)?;
        drop(schemas);
        // New tables usually have no LSM rows yet; hydrate is a no-op then.
        // Replica apply / late register may already have durable keys.
        if is_btree {
            let _ = self.hydrate_btree_from_lsm(&name)?;
        }
        Ok(())
    }

    /// Rebuild the in-memory B-Tree mirror for `table` from durable LSM versions.
    ///
    /// Durability contract: Raft/WAL → LSM is the source of truth for `BTREE`
    /// tables. The B-Tree is a read-optimized cache kept in sync by
    /// [`Self::mirror_btree_writes`] and rebuilt here on open / register.
    fn hydrate_btree_from_lsm(&self, table: &str) -> Result<u64> {
        if self.storage.engine_kind(table) != crate::storage::StorageEngineKind::BTree {
            return Ok(0);
        }
        let store = self.storage.btree(table)?;
        let mut applied = 0u64;
        for prefix in [data_table_prefix(table), index_table_prefix(table)] {
            for entry in self.collect_versions_prefix(prefix.as_ref())? {
                store.apply(entry);
                applied += 1;
            }
        }
        if applied > 0 {
            debug!(table = %table, versions = applied, "hydrated B-Tree mirror from LSM");
        }
        Ok(applied)
    }

    /// Remove one table from the local catalog file (does not tombstone rows).
    fn remove_catalog_schema(&self, name: &str) -> Result<()> {
        let mut schemas = self.schemas.write();
        schemas.remove(name);
        crate::catalog::save_catalog(&self.config.data_dir, &schemas)?;
        Ok(())
    }

    /// Leader proposes catalog upsert when distributed Raft is attached; else local install.
    fn replicate_or_install_schema(&self, schema: TableSchema) -> Result<()> {
        if let Some(raft) = self.upgrade_distributed_raft() {
            self.require_leader(&raft)?;
            let _ = self.block_on_propose(&raft, RaftCommand::catalog_upsert(&schema))?;
            return Ok(());
        }
        self.install_catalog_schema(schema)
    }

    /// Leader proposes catalog drop when distributed Raft is attached; else local remove.
    fn replicate_or_remove_schema(&self, name: &str) -> Result<()> {
        if let Some(raft) = self.upgrade_distributed_raft() {
            self.require_leader(&raft)?;
            let _ = self.block_on_propose(&raft, RaftCommand::catalog_drop(name))?;
            return Ok(());
        }
        self.remove_catalog_schema(name)
    }

    /// Create a table from SQL DDL (`CREATE TABLE`), persist catalog columns.
    pub fn create_table(
        &self,
        name: &str,
        primary_key: &str,
        columns: Vec<ColumnSpec>,
        if_not_exists: bool,
    ) -> Result<()> {
        self.ensure_open()?;
        if self.schemas.read().contains_key(name) {
            if if_not_exists {
                return Ok(());
            }
            return Err(TakyonicError::Engine(format!(
                "table `{name}` already exists"
            )));
        }
        if columns.is_empty() {
            return Err(TakyonicError::Sql(
                "CREATE TABLE requires at least one column".into(),
            ));
        }
        if !columns.iter().any(|c| c.name == primary_key) {
            return Err(TakyonicError::Sql(format!(
                "PRIMARY KEY column `{primary_key}` is not in the column list"
            )));
        }
        let schema = TableSchema::new(name, primary_key, Vec::new()).with_columns(columns);
        self.register_table(schema)
    }

    /// Drop a table from the catalog and tombstone its `Data_` / `Idx_` keys.
    pub fn drop_table(self: &Arc<Self>, name: &str, if_exists: bool) -> Result<()> {
        self.ensure_open()?;
        let schema = match self.table_schema(name) {
            Ok(s) => s,
            Err(_) if if_exists => return Ok(()),
            Err(e) => return Err(e),
        };

        // Drop secondary / vector indexes first (catalog + keys / HNSW).
        let index_names: Vec<String> = schema.indexes.iter().map(|i| i.name.clone()).collect();
        for idx_name in index_names {
            self.drop_index(&idx_name, true)?;
        }

        // Tombstone heap rows under Data_<table>_.
        let prefix = data_table_prefix(name);
        let read_ts = self.last_applied();
        let keys = self.scan_prefix_keys(&prefix, read_ts)?;
        if !keys.is_empty() {
            let mut txn = self.begin()?;
            for key in keys {
                txn.delete(key)?;
            }
            txn.commit()?;
        }

        self.replicate_or_remove_schema(name)?;
        crate::sql::drop_sequences_owned_by_table(name);
        Ok(())
    }

    /// Apply `ALTER TABLE` ADD/DROP/RENAME operations and persist the catalog.
    pub fn alter_table(
        self: &Arc<Self>,
        name: &str,
        operations: &[crate::sql::AlterTableOp],
    ) -> Result<()> {
        self.ensure_open()?;
        let mut current = name.to_string();
        for op in operations {
            match op {
                crate::sql::AlterTableOp::AddColumn {
                    column,
                    if_not_exists,
                    ..
                } => self.add_column(&current, column, *if_not_exists)?,
                crate::sql::AlterTableOp::DropColumn {
                    name: col,
                    if_exists,
                } => self.drop_column(&current, col, *if_exists)?,
                crate::sql::AlterTableOp::RenameColumn { old_name, new_name } => {
                    self.rename_column(&current, old_name, new_name)?
                }
                crate::sql::AlterTableOp::RenameTable { new_name } => {
                    self.rename_table(&current, new_name)?;
                    current = new_name.clone();
                }
                crate::sql::AlterTableOp::SetDataType { name: col, data_type } => {
                    self.set_column_type(&current, col, data_type)?
                }
            }
        }
        Ok(())
    }

    fn add_column(&self, table: &str, column: &ColumnSpec, if_not_exists: bool) -> Result<()> {
        let mut schema = self.table_schema(table)?;
        if schema.columns.iter().any(|c| c.name == column.name) {
            if if_not_exists {
                return Ok(());
            }
            return Err(TakyonicError::Sql(format!(
                "column `{}` already exists on table `{table}`",
                column.name
            )));
        }
        // Legacy tables with empty columns: seed PK so catalog stays coherent.
        if schema.columns.is_empty() {
            schema.columns.push(ColumnSpec::new(
                schema.primary_key.clone(),
                "TEXT",
            ));
        }
        schema.columns.push(column.clone());
        self.replicate_or_install_schema(schema)
    }

    fn drop_column(
        self: &Arc<Self>,
        table: &str,
        column: &str,
        if_exists: bool,
    ) -> Result<()> {
        let schema = self.table_schema(table)?;
        if column == schema.primary_key {
            return Err(TakyonicError::Sql(format!(
                "cannot DROP primary key column `{column}`"
            )));
        }
        let has_col = schema.columns.iter().any(|c| c.name == column);
        if !has_col {
            if if_exists {
                return Ok(());
            }
            return Err(TakyonicError::Sql(format!(
                "column `{column}` does not exist on table `{table}`"
            )));
        }

        // Drop secondary / vector indexes that reference this column.
        let index_names: Vec<String> = schema
            .indexes
            .iter()
            .filter(|i| i.column == column)
            .map(|i| i.name.clone())
            .collect();
        for idx_name in index_names {
            self.drop_index(&idx_name, true)?;
        }

        let mut schema = self.table_schema(table)?;
        schema.columns.retain(|c| c.name != column);
        self.replicate_or_install_schema(schema)?;
        crate::sql::drop_sequence_owned_by_column(table, column);
        Ok(())
    }

    fn rename_column(self: &Arc<Self>, table: &str, old_name: &str, new_name: &str) -> Result<()> {
        if old_name == new_name {
            return Ok(());
        }
        let schema = self.table_schema(table)?;
        let has_old = schema.columns.iter().any(|c| c.name == old_name)
            || schema.primary_key == old_name;
        if !has_old {
            return Err(TakyonicError::Sql(format!(
                "column `{old_name}` does not exist on table `{table}`"
            )));
        }
        if schema.columns.iter().any(|c| c.name == new_name) || schema.primary_key == new_name {
            return Err(TakyonicError::Sql(format!(
                "column `{new_name}` already exists on table `{table}`"
            )));
        }

        // Rewrite rows under the old catalog so index deletes see old field names.
        let mut txn = self.begin()?;
        let rows = txn.scan_table_records(table)?;
        let old_pk_field = schema.primary_key.clone();
        for row in &rows {
            let pk = row.get(&old_pk_field).ok_or_else(|| {
                TakyonicError::Sql(format!(
                    "row missing primary key `{old_pk_field}` during RENAME COLUMN"
                ))
            })?;
            txn.delete_record(table, pk)?;
        }
        txn.commit()?;

        let mut schema = self.table_schema(table)?;
        for col in &mut schema.columns {
            if col.name == old_name {
                col.name = new_name.to_string();
            }
        }
        if schema.primary_key == old_name {
            schema.primary_key = new_name.to_string();
        }
        for idx in &mut schema.indexes {
            if idx.column == old_name {
                idx.column = new_name.to_string();
            }
        }
        self.replicate_or_install_schema(schema)?;

        if !rows.is_empty() {
            let mut txn = self.begin()?;
            for mut row in rows {
                if let Some(val) = row.fields.remove(old_name) {
                    row.fields.insert(new_name.to_string(), val);
                }
                txn.put_record(table, row)?;
            }
            txn.commit()?;
        }
        Ok(())
    }

    fn rename_table(self: &Arc<Self>, old_name: &str, new_name: &str) -> Result<()> {
        if old_name == new_name {
            return Ok(());
        }
        if self.schemas.read().contains_key(new_name) {
            return Err(TakyonicError::Sql(format!(
                "table `{new_name}` already exists"
            )));
        }
        let schema = self.table_schema(old_name)?;
        let mut txn = self.begin()?;
        let rows = txn.scan_table_records(old_name)?;
        let pk_field = schema.primary_key.clone();
        for row in &rows {
            let pk = row.get(&pk_field).ok_or_else(|| {
                TakyonicError::Sql(format!(
                    "row missing primary key `{pk_field}` during RENAME TABLE"
                ))
            })?;
            txn.delete_record(old_name, pk)?;
        }
        txn.commit()?;

        // Move catalog entry (indexes stay on the schema object).
        let mut new_schema = schema;
        new_schema.name = new_name.to_string();
        self.replicate_or_remove_schema(old_name)?;
        self.replicate_or_install_schema(new_schema)?;

        if !rows.is_empty() {
            let mut txn = self.begin()?;
            for row in rows {
                txn.put_record(new_name, row)?;
            }
            txn.commit()?;
        }
        Ok(())
    }

    fn set_column_type(&self, table: &str, column: &str, data_type: &str) -> Result<()> {
        let mut schema = self.table_schema(table)?;
        let Some(col) = schema.columns.iter_mut().find(|c| c.name == column) else {
            // Legacy empty column list: seed PK then fail if not that column.
            if schema.columns.is_empty() && schema.primary_key == column {
                schema.columns.push(ColumnSpec::new(
                    schema.primary_key.clone(),
                    data_type,
                ));
                return self.replicate_or_install_schema(schema);
            }
            return Err(TakyonicError::Sql(format!(
                "column `{column}` does not exist on table `{table}`"
            )));
        };
        col.data_type = data_type.to_string();
        self.replicate_or_install_schema(schema)
    }

    /// Multi-engine storage router (B-Tree tables + optional sidecar LSM).
    pub fn storage_manager(&self) -> &Arc<crate::storage::StorageManager> {
        &self.storage
    }

    /// Borrow a registered table schema.
    pub fn table_schema(&self, table: &str) -> Result<TableSchema> {
        self.schemas
            .read()
            .get(table)
            .cloned()
            .ok_or_else(|| TakyonicError::Engine(format!("unknown table `{table}`")))
    }

    /// One hot→cold partition move for a partitioned table; persists `PMAP` in catalog.
    ///
    /// Measures approximate per-node row load via a snapshot scan, then applies
    /// [`crate::partition::Rebalancer::tick`]. Returns `None` when already balanced.
    pub fn rebalance_table(
        self: &Arc<Self>,
        table: &str,
    ) -> Result<Option<crate::partition::RebalanceMove>> {
        self.ensure_open()?;
        let schema = self.table_schema(table)?;
        if matches!(
            schema.partitioning,
            crate::partition::PartitioningStrategy::None
        ) {
            return Err(TakyonicError::Sql(format!(
                "REBALANCE requires a partitioned table; `{table}` has no partitioning"
            )));
        }
        if schema.partition_map.assignments.is_empty() {
            return Err(TakyonicError::Sql(format!(
                "REBALANCE: table `{table}` has an empty partition map"
            )));
        }
        let col = schema
            .partitioning
            .column()
            .unwrap_or(schema.primary_key.as_str())
            .to_string();
        let rb = crate::partition::Rebalancer::new(schema.partition_map.clone());
        let mut txn = self.begin()?;
        let rows = txn.scan_table_records(table)?;
        drop(txn);
        for row in &rows {
            let key = row.get(&col).unwrap_or("");
            let pid = schema.partitioning.partition_id(key);
            let node = schema.partition_map.node_for(pid);
            rb.observe_load(node, 1);
        }
        let Some(mv) = rb.tick()? else {
            return Ok(None);
        };
        let mut next = schema;
        next.partition_map = rb.partition_map();
        self.register_table(next)?;
        Ok(Some(mv))
    }

    /// Snapshot of all registered table schemas (sorted by name via [`BTreeMap`]).
    pub fn list_table_schemas(&self) -> Vec<TableSchema> {
        self.schemas.read().values().cloned().collect()
    }

    /// Shared comment map for session scalar helpers (`obj_description` / `col_description`).
    pub fn comments(&self) -> Arc<parking_lot::RwLock<BTreeMap<String, String>>> {
        Arc::clone(&self.comments)
    }

    /// `COMMENT ON TABLE` / clear (`None`); persisted under `data_dir/COMMENTS`.
    pub fn set_table_comment(&self, table: &str, comment: Option<&str>) -> Result<()> {
        self.ensure_open()?;
        let key = format!("t:{}", table.trim().to_ascii_lowercase());
        let mut next = self.comments.read().clone();
        match comment {
            Some(c) => {
                next.insert(key, c.to_string());
            }
            None => {
                next.remove(&key);
            }
        }
        self.replicate_or_install_comments(next)
    }

    /// `COMMENT ON COLUMN table.col` / clear (`None`); persisted under `data_dir/COMMENTS`.
    pub fn set_column_comment(&self, table: &str, column: &str, comment: Option<&str>) -> Result<()> {
        self.ensure_open()?;
        let key = format!(
            "c:{}.{}",
            table.trim().to_ascii_lowercase(),
            column.trim().to_ascii_lowercase()
        );
        let mut next = self.comments.read().clone();
        match comment {
            Some(c) => {
                next.insert(key, c.to_string());
            }
            None => {
                next.remove(&key);
            }
        }
        self.replicate_or_install_comments(next)
    }

    /// `COMMENT ON ROLE` / `COMMENT ON DATABASE` (shared objects); keys `r:` / `d:`.
    pub fn set_shared_comment(
        &self,
        kind: &str,
        name: &str,
        comment: Option<&str>,
    ) -> Result<()> {
        self.ensure_open()?;
        let prefix = match kind {
            "role" | "r" => "r",
            "database" | "d" => "d",
            other => {
                return Err(TakyonicError::Sql(format!(
                    "unsupported shared comment kind `{other}`"
                )));
            }
        };
        if prefix == "d" && !crate::rbac::database_exists(name) {
            return Err(TakyonicError::Sql(format!(
                "database \"{}\" does not exist",
                crate::rbac::database_name_leaf(name)
            )));
        }
        let key = format!("{prefix}:{}", name.trim().to_ascii_lowercase());
        let mut next = self.comments.read().clone();
        match comment {
            Some(c) => {
                next.insert(key, c.to_string());
            }
            None => {
                next.remove(&key);
            }
        }
        self.replicate_or_install_comments(next)
    }

    fn install_comments_catalog(&self, map: BTreeMap<String, String>) -> Result<()> {
        save_comments(&self.config.data_dir, &map)?;
        *self.comments.write() = map;
        Ok(())
    }

    fn replicate_or_install_comments(&self, map: BTreeMap<String, String>) -> Result<()> {
        if let Some(raft) = self.upgrade_distributed_raft() {
            self.require_leader(&raft)?;
            let blob = bytes::Bytes::from(encode_comments(&map).into_bytes());
            let _ = self.block_on_propose(&raft, RaftCommand::comments_replace(blob))?;
            return Ok(());
        }
        self.install_comments_catalog(map)
    }

    /// Shared-object comment for `shobj_description(name, catalog)`.
    pub fn shared_comment(&self, kind: &str, name: &str) -> Option<String> {
        let prefix = match kind {
            "role" | "r" | "pg_authid" | "pg_roles" => "r",
            "database" | "d" | "pg_database" => "d",
            _ => return None,
        };
        let key = format!("{prefix}:{}", name.trim().to_ascii_lowercase());
        self.comments.read().get(&key).cloned()
    }

    /// Table comment for `obj_description(name)` (name-based until OID catalog exists).
    pub fn table_comment(&self, table: &str) -> Option<String> {
        let key = format!("t:{}", table.trim().to_ascii_lowercase());
        self.comments.read().get(&key).cloned()
    }

    /// Column comment for `col_description(table, column)` (name-based).
    pub fn column_comment(&self, table: &str, column: &str) -> Option<String> {
        let key = format!(
            "c:{}.{}",
            table.trim().to_ascii_lowercase(),
            column.trim().to_ascii_lowercase()
        );
        self.comments.read().get(&key).cloned()
    }

    /// Flatten column name → catalog type token across all registered tables.
    ///
    /// Duplicate column names across tables: last writer wins (sufficient for
    /// pgwire OID hints on projected result sets).
    pub fn column_type_hints(&self) -> std::collections::HashMap<String, String> {
        let mut hints = std::collections::HashMap::new();
        for schema in self.schemas.read().values() {
            for col in &schema.columns {
                hints.insert(col.name.clone(), col.data_type.clone());
            }
        }
        hints
    }

    /// Find which table owns a secondary index by name.
    pub fn find_index(&self, index_name: &str) -> Result<(TableSchema, IndexDef)> {
        for schema in self.schemas.read().values() {
            if let Some(idx) = schema.indexes.iter().find(|i| i.name == index_name) {
                return Ok((schema.clone(), idx.clone()));
            }
        }
        Err(TakyonicError::Engine(format!(
            "unknown index `{index_name}`"
        )))
    }

    /// Create a secondary index, persist the catalog, and backfill from live rows.
    pub fn create_index(
        self: &Arc<Self>,
        name: &str,
        table: &str,
        column: &str,
        if_not_exists: bool,
    ) -> Result<()> {
        self.create_index_inner(name, table, column, if_not_exists, None)
    }

    /// Create an HNSW vector index and backfill embeddings from live rows.
    pub fn create_vector_index(
        self: &Arc<Self>,
        name: &str,
        table: &str,
        column: &str,
        if_not_exists: bool,
        spec: VectorIndexSpec,
    ) -> Result<()> {
        self.create_index_inner(name, table, column, if_not_exists, Some(spec))
    }

    fn create_index_inner(
        self: &Arc<Self>,
        name: &str,
        table: &str,
        column: &str,
        if_not_exists: bool,
        vector: Option<VectorIndexSpec>,
    ) -> Result<()> {
        self.ensure_open()?;
        {
            let schemas = self.schemas.read();
            if schemas.values().any(|s| s.indexes.iter().any(|i| i.name == name)) {
                if if_not_exists {
                    return Ok(());
                }
                return Err(TakyonicError::Sql(format!(
                    "index `{name}` already exists"
                )));
            }
            let schema = schemas.get(table).ok_or_else(|| {
                TakyonicError::Sql(format!("unknown table `{table}`"))
            })?;
            if schema.primary_key == column {
                return Err(TakyonicError::Sql(
                    "cannot create secondary index on the primary key column".into(),
                ));
            }
            if schema.indexes.iter().any(|i| i.column == column) {
                return Err(TakyonicError::Sql(format!(
                    "column `{column}` already has a secondary index"
                )));
            }
        }

        let index = match &vector {
            Some(spec) => IndexDef::vector(name, column, spec.clone()),
            None => IndexDef::new(name, column),
        };
        {
            let mut schema = self.table_schema(table)?;
            schema.indexes.push(index.clone());
            self.replicate_or_install_schema(schema)?;
        }

        if let Some(spec) = vector {
            let graph = Arc::new(HnswIndex::new(name, spec.dimension, spec.metric));
            let mut txn = self.begin()?;
            let records = txn.scan_table_records(table)?;
            let schema = txn.table_schema(table)?;
            for record in &records {
                let Some(v) = record.get(column) else {
                    continue;
                };
                let pk = record
                    .get(&schema.primary_key)
                    .ok_or_else(|| {
                        TakyonicError::Engine(format!(
                            "row missing primary key `{}`",
                            schema.primary_key
                        ))
                    })?
                    .to_string();
                let vec = VectorValue::from_text(v)?;
                graph.insert(pk, vec)?;
            }
            // Drop the scan txn without committing (read-only).
            txn.abort();
            graph.save(&self.config.data_dir)?;
            self.hnsw.write().insert(name.to_string(), graph);
            return Ok(());
        }

        // Backfill B-Tree index keys for existing rows under one MVCC transaction.
        let mut txn = self.begin()?;
        let records = txn.scan_table_records(table)?;
        let schema = txn.table_schema(table)?;
        let mut index_values = Vec::new();
        for record in &records {
            let Some(v) = record.get(column) else {
                continue;
            };
            let pk = record
                .get(&schema.primary_key)
                .ok_or_else(|| {
                    TakyonicError::Engine(format!(
                        "row missing primary key `{}`",
                        schema.primary_key
                    ))
                })?
                .to_string();
            let encoded = crate::txn::index_store_value(v);
            txn.put(
                crate::schema::index_key(table, name, &encoded, &pk),
                crate::types::Value::new(&b""[..]),
            )?;
            index_values.push((name.to_string(), encoded));
        }
        txn.commit()?;
        // Update NDV without bumping row_count (rows already counted).
        self.stats.on_index_backfill(table, &index_values);
        Ok(())
    }

    /// Drop a secondary index, delete its keys, and persist the catalog.
    pub fn drop_index(self: &Arc<Self>, name: &str, if_exists: bool) -> Result<()> {
        self.ensure_open()?;
        let (table, _idx) = match self.find_index(name) {
            Ok((schema, idx)) => (schema.name, idx),
            Err(_) if if_exists => return Ok(()),
            Err(e) => return Err(e),
        };

        // Tombstone all index entries (B-Tree only).
        if !_idx.is_vector() {
            let prefix = crate::schema::index_column_prefix(&table, name);
            let read_ts = self.last_applied();
            let keys = self.scan_prefix_keys(&prefix, read_ts)?;
            let mut txn = self.begin()?;
            for key in keys {
                txn.delete(key)?;
            }
            txn.commit()?;
        } else {
            self.hnsw.write().remove(name);
            let path = HnswIndex::snapshot_path(&self.config.data_dir, name);
            let _ = fs::remove_file(path);
        }

        if let Ok(mut schema) = self.table_schema(&table) {
            schema.indexes.retain(|i| i.name != name);
            self.replicate_or_install_schema(schema)?;
        }
        Ok(())
    }

    /// Shared RBAC catalog (for PgWire AuthSource + SessionState).
    pub fn auth_catalog(&self) -> SharedAuthCatalog {
        Arc::clone(&self.auth)
    }

    /// Create a role / login user and persist AUTH.
    pub fn create_role(
        &self,
        name: &str,
        can_login: bool,
        is_superuser: bool,
        password: Option<&str>,
        if_not_exists: bool,
    ) -> Result<()> {
        self.ensure_open()?;
        let mut next = self.auth.read().clone();
        next.create_role(name, can_login, is_superuser, password, if_not_exists)?;
        self.replicate_or_install_auth(next)
    }

    /// Drop a role / user.
    pub fn drop_role(&self, name: &str, if_exists: bool) -> Result<()> {
        self.ensure_open()?;
        let mut next = self.auth.read().clone();
        next.drop_role(name, if_exists)?;
        self.replicate_or_install_auth(next)
    }

    /// `GRANT <priv> ON <table> TO <grantee>`.
    pub fn grant_privilege(
        &self,
        grantee: &str,
        table: &str,
        privileges: &[crate::rbac::Privilege],
    ) -> Result<()> {
        self.ensure_open()?;
        let _ = self.table_schema(table)?;
        let mut next = self.auth.read().clone();
        next.grant(grantee, table, privileges)?;
        self.replicate_or_install_auth(next)
    }

    /// `REVOKE <priv> ON <table> FROM <grantee>`.
    pub fn revoke_privilege(
        &self,
        grantee: &str,
        table: &str,
        privileges: &[crate::rbac::Privilege],
    ) -> Result<()> {
        self.ensure_open()?;
        let mut next = self.auth.read().clone();
        next.revoke(grantee, table, privileges)?;
        self.replicate_or_install_auth(next)
    }

    /// `GRANT <priv> ON SCHEMA <schema> TO <grantee>`.
    pub fn grant_schema_privilege(
        &self,
        grantee: &str,
        schema: &str,
        privileges: &[crate::rbac::SchemaPrivilege],
    ) -> Result<()> {
        self.ensure_open()?;
        let mut next = self.auth.read().clone();
        next.grant_schema(grantee, schema, privileges)?;
        self.replicate_or_install_auth(next)
    }

    /// `REVOKE <priv> ON SCHEMA <schema> FROM <grantee>`.
    pub fn revoke_schema_privilege(
        &self,
        grantee: &str,
        schema: &str,
        privileges: &[crate::rbac::SchemaPrivilege],
    ) -> Result<()> {
        self.ensure_open()?;
        let mut next = self.auth.read().clone();
        next.revoke_schema(grantee, schema, privileges)?;
        self.replicate_or_install_auth(next)
    }

    /// `GRANT SELECT (col) ON <table> TO <grantee>`.
    pub fn grant_column_privilege(
        &self,
        grantee: &str,
        table: &str,
        specs: &[crate::rbac::ColumnGrantSpec],
    ) -> Result<()> {
        self.ensure_open()?;
        let _ = self.table_schema(table)?;
        let mut next = self.auth.read().clone();
        next.grant_columns(grantee, table, specs)?;
        self.replicate_or_install_auth(next)
    }

    /// `REVOKE SELECT (col) ON <table> FROM <grantee>`.
    pub fn revoke_column_privilege(
        &self,
        grantee: &str,
        table: &str,
        specs: &[crate::rbac::ColumnGrantSpec],
    ) -> Result<()> {
        self.ensure_open()?;
        let mut next = self.auth.read().clone();
        next.revoke_columns(grantee, table, specs)?;
        self.replicate_or_install_auth(next)
    }

    /// `GRANT <role> TO <member>`.
    pub fn grant_role_membership(&self, role: &str, member: &str) -> Result<()> {
        self.ensure_open()?;
        let mut next = self.auth.read().clone();
        next.grant_membership(role, member)?;
        self.replicate_or_install_auth(next)
    }

    fn install_auth_catalog(&self, catalog: crate::rbac::AuthCatalog) -> Result<()> {
        catalog.save(&self.config.data_dir)?;
        *self.auth.write() = catalog;
        Ok(())
    }

    fn replicate_or_install_auth(&self, catalog: crate::rbac::AuthCatalog) -> Result<()> {
        if let Some(raft) = self.upgrade_distributed_raft() {
            self.require_leader(&raft)?;
            let blob = bytes::Bytes::from(catalog.encode().into_bytes());
            let _ = self.block_on_propose(&raft, RaftCommand::auth_replace(blob))?;
            return Ok(());
        }
        self.install_auth_catalog(catalog)
    }

    /// Snapshot of table statistics for the optimizer.
    pub fn table_stats(&self, table: &str) -> TableStats {
        self.stats.get(table)
    }

    /// Replace in-memory stats for `table` after `ANALYZE` and persist `STATS`.
    pub fn apply_analyzed_stats(&self, table: &str, new_stats: TableStats) -> Result<()> {
        self.ensure_open()?;
        let _ = self.table_schema(table)?;
        let mut snapshot = self.stats.snapshot_all();
        snapshot.insert(table.to_string(), new_stats);
        self.replicate_or_install_stats(snapshot)
    }

    fn install_stats_catalog(
        &self,
        snapshot: std::collections::BTreeMap<String, TableStats>,
    ) -> Result<()> {
        stats::save_stats(&self.config.data_dir, &snapshot)?;
        self.stats.load_all(snapshot);
        Ok(())
    }

    fn replicate_or_install_stats(
        &self,
        snapshot: std::collections::BTreeMap<String, TableStats>,
    ) -> Result<()> {
        if let Some(raft) = self.upgrade_distributed_raft() {
            self.require_leader(&raft)?;
            let blob = bytes::Bytes::from(stats::encode_stats(&snapshot).into_bytes());
            let _ = self.block_on_propose(&raft, RaftCommand::stats_replace(blob))?;
            return Ok(());
        }
        self.install_stats_catalog(snapshot)
    }

    /// Scan `table` under `txn`, compute statistics, and persist them.
    pub fn analyze_table(
        &self,
        txn: &mut crate::txn::Transaction,
        table: &str,
    ) -> Result<TableStats> {
        self.ensure_open()?;
        let schema = self.table_schema(table)?;
        // B-Tree tables: sample versions from the multi-engine router.
        if schema.storage_engine == crate::storage::StorageEngineKind::BTree {
            let _read_ts = txn.read_ts();
            let entries = self.storage.sample_entries(table)?;
            let records: Vec<Record> = entries
                .into_iter()
                .filter_map(|e| e.value.as_ref().and_then(|v| Record::decode(v).ok()))
                .collect();
            let computed = stats::compute_table_stats(&schema, &records);
            self.apply_analyzed_stats(table, computed.clone())?;
            return Ok(computed);
        }
        let records = txn.scan_table_records(table)?;
        let computed = stats::compute_table_stats(&schema, &records);
        self.apply_analyzed_stats(table, computed.clone())?;
        Ok(computed)
    }

    /// Buffer pool manager (when enabled in config).
    pub fn buffer_pool(&self) -> Option<&Arc<BufferPoolManager>> {
        self.buffer_pool.as_ref()
    }

    /// Shared storage manifest manager (when Tier-2 object storage is attached).
    pub fn manifest(&self) -> Option<&Arc<ManifestManager>> {
        self.manifest.as_ref()
    }

    /// Publish the current SST / pages inventory to the remote manifest.
    pub fn publish_storage_manifest(&self) -> Result<Option<StorageManifest>> {
        let Some(mgr) = &self.manifest else {
            return Ok(None);
        };
        let mut next = mgr.current();
        next.sstables = self
            .manager
            .all_files()
            .into_iter()
            .map(|m| ManifestSst {
                id: m.id,
                path: if self.manager.has_remote() {
                    sst_object_key(m.level, m.id)
                } else {
                    m.path.to_string_lossy().into_owned()
                },
                level: m.level as u32,
            })
            .collect();
        if next.pages_key.is_empty() {
            next.pages_key = REMOTE_PAGES_KEY.to_string();
        }
        if next.pages_prefix.is_empty() {
            next.pages_prefix = PAGES_V2_PREFIX.to_string();
        }
        // Always publish V2 layout once the engine is writing through DiskManager chunks.
        let chunk = self
            .buffer_pool
            .as_ref()
            .map(|bpm| bpm.disk().chunk_size() as u64)
            .unwrap_or(crate::manifest::DEFAULT_PAGES_CHUNK_BYTES);
        next.pages_layout = crate::manifest::PagesLayout::ChunkV2 { chunk_size: chunk };
        // Let ManifestManager assign the next monotonic version.
        next.version = 0;
        // Preserve btree_roots from prior publishes.
        Ok(Some(mgr.publish(next)?))
    }

    /// Flush all dirty BPM pages (checkpoint / eviction durability).
    pub fn checkpoint_buffer_pool(&self) -> Result<()> {
        if let Some(bpm) = &self.buffer_pool {
            bpm.flush_all()?;
        }
        Ok(())
    }

    /// Current MVCC Vacuum watermark (oldest active epoch, or last-applied when idle).
    pub fn mvcc_watermark(&self) -> CommitTs {
        self.publish_watermark();
        self.manager.mvcc_watermark()
    }

    /// Number of registered active snapshot transactions.
    pub fn active_txn_count(&self) -> usize {
        self.txn_tracker.active_count()
    }

    /// Approximate on-disk SST bytes currently catalogued.
    pub fn sst_total_bytes(&self) -> u64 {
        self.manager
            .all_files()
            .into_iter()
            .map(|m| m.file_size)
            .sum()
    }

    /// Approximate heap size for `pg_relation_size` / `pg_table_size`.
    pub fn relation_heap_bytes(&self, table: &str) -> Result<u64> {
        let _ = self.table_schema(table)?;
        let versions = self.count_versions_prefix(data_table_prefix(table).as_ref())?;
        Ok(versions.saturating_mul(crate::oid::RELATION_SIZE_BYTES_PER_VERSION))
    }

    /// Approximate heap+index size for `pg_total_relation_size`.
    pub fn relation_total_bytes(&self, table: &str) -> Result<u64> {
        let _ = self.table_schema(table)?;
        let versions = self.table_version_count(table)?;
        Ok(versions.saturating_mul(crate::oid::RELATION_SIZE_BYTES_PER_VERSION))
    }

    /// Snapshot of relation size estimates for session scalar helpers.
    pub fn relation_size_catalog(&self) -> crate::oid::RelationSizeCatalog {
        let mut cat = crate::oid::RelationSizeCatalog::default();
        for schema in self.list_table_schemas() {
            let name = schema.name.clone();
            let heap = self.relation_heap_bytes(&name).unwrap_or(0);
            let total = self.relation_total_bytes(&name).unwrap_or(heap);
            cat.insert(
                &name,
                crate::oid::RelationSizeEntry {
                    heap_bytes: heap,
                    total_bytes: total.max(heap),
                },
            );
        }
        cat
    }

    /// Count all MVCC versions under `Data_<table>_` and `Idx_<table>_`.
    pub fn table_version_count(&self, table: &str) -> Result<u64> {
        let data = data_table_prefix(table);
        let idx = index_table_prefix(table);
        Ok(self.count_versions_prefix(data.as_ref())? + self.count_versions_prefix(idx.as_ref())?)
    }

    /// Run `VACUUM` on `table`: GC dead heap + index versions below the watermark,
    /// flush, and drain compaction so SST space is reclaimed.
    pub fn vacuum_table(self: &Arc<Self>, table: &str) -> Result<VacuumStats> {
        self.ensure_open()?;
        let t0 = Instant::now();
        let schema = self.table_schema(table)?;
        self.publish_watermark();
        let watermark = self.manager.mvcc_watermark();

        // B-Tree tables: GC versions in the multi-engine router, not the LSM.
        if schema.storage_engine == crate::storage::StorageEngineKind::BTree {
            let before = self
                .storage
                .btree(table)
                .map(|b| b.len() as u64)
                .unwrap_or(0);
            let removed = self.storage.vacuum_table(table, watermark)?;
            let after = self
                .storage
                .btree(table)
                .map(|b| b.len() as u64)
                .unwrap_or(0);
            self.metrics.record_vacuum(t0.elapsed());
            return Ok(VacuumStats {
                table: table.to_string(),
                watermark,
                memtable_removed: removed,
                versions_before: before,
                versions_after: after,
                sst_bytes_before: 0,
                sst_bytes_after: 0,
                dead_heap_versions: removed,
                dead_index_versions: 0,
            });
        }

        let data_prefix = data_table_prefix(table);
        let idx_prefix = index_table_prefix(table);
        let versions_before = self.table_version_count(table)?;
        let sst_bytes_before = self.sst_total_bytes();

        let heap_versions = self.collect_versions_prefix(data_prefix.as_ref())?;
        let index_versions = self.collect_versions_prefix(idx_prefix.as_ref())?;
        let dead_heap = count_dead_across_keys(&heap_versions, watermark);
        let dead_index = count_dead_across_keys(&index_versions, watermark);

        // Primary + secondary GC while no snapshot pins the watermark.
        let node = self.raft_node()?;
        let removed_data = node
            .memtable()
            .gc_below_watermark_prefix(watermark, data_prefix.as_ref());
        let removed_idx = node
            .memtable()
            .gc_below_watermark_prefix(watermark, idx_prefix.as_ref());
        drop(node);

        // Persist GC'd memtable state and rewrite SSTs via compaction GC.
        self.force_flush()?;
        self.drain_compaction()?;

        // Index cleanup: tombstone any dangling Idx_ keys, then a second GC pass.
        let index_purged = self.purge_dangling_index_entries(table, watermark)?;
        let mut removed_extra = 0u64;
        if index_purged > 0 {
            self.publish_watermark();
            let wm2 = self.manager.mvcc_watermark();
            let node = self.raft_node()?;
            removed_extra = node
                .memtable()
                .gc_below_watermark_prefix(wm2, idx_prefix.as_ref());
            drop(node);
            self.force_flush()?;
            self.drain_compaction()?;
        }

        let versions_after = self.table_version_count(table)?;
        let sst_bytes_after = self.sst_total_bytes();

        // Prune HNSW nodes whose PK is no longer live under the watermark snapshot.
        self.vacuum_hnsw_for_table(table)?;

        self.metrics.record_vacuum(t0.elapsed());
        Ok(VacuumStats {
            table: table.to_string(),
            watermark,
            memtable_removed: removed_data + removed_idx + index_purged + removed_extra,
            versions_before,
            versions_after,
            sst_bytes_before,
            sst_bytes_after,
            dead_heap_versions: dead_heap,
            dead_index_versions: dead_index,
        })
    }

    fn vacuum_hnsw_for_table(self: &Arc<Self>, table: &str) -> Result<()> {
        let schema = self.table_schema(table)?;
        let vector_indexes: Vec<_> = schema
            .indexes
            .iter()
            .filter(|i| i.is_vector())
            .cloned()
            .collect();
        if vector_indexes.is_empty() {
            return Ok(());
        }
        let mut txn = self.begin()?;
        let live: std::collections::HashSet<String> = txn
            .scan_table_records(table)?
            .into_iter()
            .filter_map(|r| r.get(&schema.primary_key).map(str::to_string))
            .collect();
        txn.abort();
        for idx in vector_indexes {
            if let Some(graph) = self.hnsw_index(&idx.name) {
                let pruned = graph.retain_pks(&live);
                if pruned > 0 {
                    graph.save(&self.config.data_dir)?;
                }
            }
        }
        Ok(())
    }

    /// Start a cost-based query against `table`.
    pub fn query(&self, table: impl Into<String>) -> Query<'_> {
        Query::new(self, table)
    }

    /// Visible user keys at `read_ts` whose bytes start with `prefix`.
    pub fn scan_prefix_keys(&self, prefix: &[u8], read_ts: CommitTs) -> Result<Vec<Key>> {
        self.ensure_open()?;
        // Multi-engine: if prefix is a B-Tree table's data/index space, scan there.
        if let Ok(pref_key) = std::str::from_utf8(prefix) {
            if let Some(table) = pref_key
                .strip_prefix("Data_")
                .or_else(|| pref_key.strip_prefix("Idx_"))
                .and_then(|r| r.split('_').next())
            {
                if self.storage.engine_kind(table) == crate::storage::StorageEngineKind::BTree {
                    if let Ok(store) = self.storage.btree(table) {
                        return Ok(store
                            .scan_at(prefix, read_ts)
                            .into_iter()
                            .map(|e| e.key)
                            .collect());
                    }
                }
            }
        }

        let mut best: BTreeMap<Key, Entry> = BTreeMap::new();

        let node = self.raft_node()?;
        for entry in node.memtable().scan_prefix_at(prefix, read_ts) {
            best.insert(entry.key.clone(), entry);
        }
        drop(node);

        let levels = self.manager.level_count();
        for level in 0..levels {
            let mut files = self.manager.level_files(level);
            if level == 0 {
                files.sort_by_key(|meta| std::cmp::Reverse(meta.id));
            }
            for meta in files {
                // Skip files that cannot contain the prefix.
                if level > 0 {
                    let pref_key = Key::new(bytes::Bytes::copy_from_slice(prefix));
                    if meta.largest.as_bytes() < prefix {
                        continue;
                    }
                    // Upper bound: prefix with 0xFF… is hard; use starts_with on bounds.
                    if !meta.smallest.as_bytes().starts_with(prefix)
                        && !meta.largest.as_bytes().starts_with(prefix)
                        && meta.smallest.as_bytes() > prefix
                    {
                        let _ = pref_key;
                        continue;
                    }
                }
                let Some(pin) = self.manager.registry().pin(meta.id) else {
                    continue;
                };
                let entries = pin.reader().entries()?;
                let mut current: Option<Key> = None;
                let mut resolved = false;
                for entry in entries {
                    if !entry.key.as_bytes().starts_with(prefix) {
                        continue;
                    }
                    if current.as_ref() != Some(&entry.key) {
                        current = Some(entry.key.clone());
                        resolved = false;
                    }
                    if resolved || entry.seq > read_ts {
                        continue;
                    }
                    resolved = true;
                    match best.get(&entry.key) {
                        Some(existing) if existing.seq >= entry.seq => {}
                        _ if !entry.tombstone => {
                            best.insert(entry.key.clone(), entry);
                        }
                        _ => {
                            if best.get(&entry.key).is_none_or(|e| e.seq < entry.seq) {
                                best.remove(&entry.key);
                            }
                        }
                    }
                }
            }
        }

        Ok(best.into_keys().collect())
    }

    fn publish_watermark(&self) {
        let wm = match self.txn_tracker.watermark() {
            Some(oldest) => oldest,
            None => {
                // No active snapshots: next `begin` will use `last_applied`.
                // Publish `last_applied + 1` so shadowed versions strictly older
                // than the newest committed row become Vacuum/compaction-eligible.
                self.raft
                    .lock()
                    .as_ref()
                    .map(|n| n.last_applied().saturating_add(1))
                    .unwrap_or(0)
            }
        };
        self.manager.set_mvcc_watermark(wm);
    }

    /// Flush residual memtable, stop the group-commit flusher, shut down pools.
    pub fn close(&self) -> Result<()> {
        if self.closed.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        if let Some(mut mgr) = self.metrics_manager.lock().take() {
            mgr.shutdown();
        }
        // Flush dirty BPM frames to Tier-1 (+ write-through Tier-2) before teardown.
        let _ = self.checkpoint_buffer_pool();
        if self.manifest.is_some() {
            let _ = self.publish_storage_manifest();
        }
        let _ = self.save_all_hnsw();
        let Some(node) = self.raft.lock().take() else {
            // Already dismantled (e.g. [`Self::abandon_for_crash_test`]).
            let _ = self.aries_wal.lock().take();
            let _ = self.compaction.lock().take();
            return Ok(());
        };
        self.flush_node(&node)?;
        let current = self.wal_segment.load(Ordering::Relaxed);
        let mut wal = node.shutdown()?;
        wal.sync()?;
        // Persist the sequence high-water mark BEFORE pruning WAL history:
        // once segments are gone, replay can no longer reconstruct next_seq,
        // and a regressed seq would break newest-wins against existing SSTs.
        write_seq_marker(&self.wal_dir, node.next_index())?;
        // Safe to prune: final flush covered the memtable; no concurrent writers.
        prune_wal_segments_below(&self.wal_dir, current)?;
        // Checkpoint: SST/LSM durable → truncate ARIES redo log.
        if let Some(mut aries) = self.aries_wal.lock().take() {
            let _ = aries.checkpoint_truncate();
        }
        let compaction = self.compaction.lock().take();
        drop(compaction);
        info!("TakyonicEngine closed");
        Ok(())
    }

    /// Simulate a hard crash: release file handles **without** flushing the
    /// memtable to SST or pruning WAL / ARIES logs.
    ///
    /// Used by crash-recovery integration tests. After this returns the engine
    /// is closed; reopen the same directories to exercise Redo recovery.
    pub fn abandon_for_crash_test(&self) -> Result<()> {
        if self.closed.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        if let Some(mut mgr) = self.metrics_manager.lock().take() {
            mgr.shutdown();
        }
        // Stop group-commit flusher so WAL fds are released, but do NOT flush
        // memtable → SST and do NOT prune / checkpoint.
        if let Some(node) = self.raft.lock().take() {
            let _ = node.shutdown();
        }
        // Drop ARIES handle without truncating (Commit records stay on disk).
        let _ = self.aries_wal.lock().take();
        let _ = self.compaction.lock().take();
        warn!("TakyonicEngine abandoned (crash simulation — no flush)");
        Ok(())
    }

    /// Shared memtable (flush / observability).
    pub fn memtable(&self) -> Arc<Memtable> {
        self.raft
            .lock()
            .as_ref()
            .map(|n| Arc::clone(n.memtable()))
            .unwrap_or_else(|| Arc::new(Memtable::new()))
    }

    /// Shared leveled SST catalog.
    pub fn manager(&self) -> &Arc<SstManager> {
        &self.manager
    }

    /// Shared L0-aware admission controller.
    pub fn admission(&self) -> &Arc<AdmissionController> {
        &self.admission
    }

    /// Shared engine telemetry.
    pub fn metrics(&self) -> &Arc<EngineMetrics> {
        &self.metrics
    }

    /// Borrow the engine configuration.
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Bound address of the Prometheus `/metrics` server, if running.
    pub fn metrics_bind_addr(&self) -> Option<SocketAddr> {
        self.metrics_manager
            .lock()
            .as_ref()
            .and_then(|m| m.bind_addr)
    }

    /// Render Prometheus exposition text (same body as `GET /metrics`).
    pub fn render_metrics(&self) -> String {
        self.metrics
            .render_prometheus(self.buffer_pool.as_deref())
    }

    /// Average group-commit batch size observed so far.
    pub fn avg_group_batch_size(&self) -> f64 {
        self.metrics.avg_group_batch_size()
    }

    /// Apply a contiguous committed Raft batch to the state machine.
    ///
    /// Used by the distributed consensus layer after a quorum commit. Entries
    /// must already be durable in the Raft log; this path only publishes into
    /// the memtable and may flush to L0. It does **not** re-append to the
    /// engine WAL (the Raft log is the source of truth for replication).
    pub fn apply_committed(&self, entries: &[CommittedEntry]) -> Result<BatchApplyResult> {
        if entries.is_empty() {
            return Ok(BatchApplyResult {
                applied: 0,
                last_applied: self
                    .raft
                    .lock()
                    .as_ref()
                    .map(|n| n.last_applied())
                    .unwrap_or(0),
            });
        }
        // Meta side-effects (catalog / AUTH / STATS / COMMENTS) must run *before*
        // `apply_log` advances `last_applied`. Otherwise a failed install leaves
        // the index past the entry and followers never retry the catalog update.
        // 2PC side-effects stay after apply_log (commit may depend on LSM apply).
        for committed in entries {
            match &committed.command {
                RaftCommand::CatalogUpsert { table, schema } => {
                    let text = std::str::from_utf8(schema.as_ref()).map_err(|e| {
                        TakyonicError::Raft(format!("CatalogUpsert utf8: {e}"))
                    })?;
                    let parsed = crate::catalog::parse_table_block(text)?;
                    if parsed.name != *table {
                        return Err(TakyonicError::Raft(format!(
                            "CatalogUpsert key `{table}` != TABLE `{}`",
                            parsed.name
                        )));
                    }
                    self.install_catalog_schema(parsed)?;
                }
                RaftCommand::CatalogDrop { table } => {
                    self.remove_catalog_schema(table)?;
                }
                RaftCommand::AuthReplace { blob } => {
                    let text = std::str::from_utf8(blob.as_ref()).map_err(|e| {
                        TakyonicError::Raft(format!("AuthReplace utf8: {e}"))
                    })?;
                    let catalog = crate::rbac::AuthCatalog::parse(text)?;
                    self.install_auth_catalog(catalog)?;
                }
                RaftCommand::StatsReplace { blob } => {
                    let text = std::str::from_utf8(blob.as_ref()).map_err(|e| {
                        TakyonicError::Raft(format!("StatsReplace utf8: {e}"))
                    })?;
                    let snapshot = stats::parse_stats(text)?;
                    self.install_stats_catalog(snapshot)?;
                }
                RaftCommand::CommentsReplace { blob } => {
                    let text = std::str::from_utf8(blob.as_ref()).map_err(|e| {
                        TakyonicError::Raft(format!("CommentsReplace utf8: {e}"))
                    })?;
                    let map = parse_comments(text)?;
                    self.install_comments_catalog(map)?;
                }
                _ => {}
            }
        }

        let node = self.raft_node()?;
        let result = node.apply_log(entries)?;
        // Keep the OCC index warm on every replica so failover preserves SI.
        for committed in entries {
            match &committed.command {
                RaftCommand::CatalogUpsert { .. }
                | RaftCommand::CatalogDrop { .. }
                | RaftCommand::AuthReplace { .. }
                | RaftCommand::StatsReplace { .. }
                | RaftCommand::CommentsReplace { .. } => {}
                RaftCommand::TxnPrepare { .. }
                | RaftCommand::TxnAbort { .. }
                | RaftCommand::TxnCommit { .. } => {
                    self.apply_twopc_side_effect(&committed.command)?;
                }
                _ if committed.command.is_meta() => {}
                _ => {
                    let keys: Vec<Key> = match &committed.command {
                        RaftCommand::Put { key, .. } | RaftCommand::Delete { key } => {
                            vec![key.clone()]
                        }
                        RaftCommand::TxnBatch { ops } => {
                            ops.iter().map(|(k, _)| k.clone()).collect()
                        }
                        _ => Vec::new(),
                    };
                    if !keys.is_empty() {
                        self.note_committed_keys(keys, committed.index);
                    }
                }
            }
        }
        self.maybe_flush_node(&node)?;
        Ok(result)
    }

    /// Highest index applied to the local state machine.
    pub fn last_applied(&self) -> u64 {
        self.raft
            .lock()
            .as_ref()
            .map(|n| n.last_applied())
            .unwrap_or(0)
    }

    /// Force a memtable → L0 flush (Raft snapshot precondition).
    pub fn force_flush(&self) -> Result<()> {
        let node = self.raft_node()?;
        self.flush_node(&node)
    }

    /// Advance `last_applied` after recovering from a Raft snapshot boundary.
    pub fn set_last_applied(&self, index: u64) -> Result<()> {
        let node = self.raft_node()?;
        node.set_last_applied(index);
        Ok(())
    }

    /// Package active SSTables into a Raft snapshot blob (call after [`Self::force_flush`]).
    pub fn export_sst_snapshot(
        &self,
        last_included_index: u64,
        last_included_term: u64,
        membership: crate::membership::ClusterMembership,
    ) -> Result<bytes::Bytes> {
        let metas = self.manager.all_files();
        let payload = SnapshotPayload::from_metas(
            last_included_index,
            last_included_term,
            membership,
            &metas,
        )?;
        Ok(payload.encode())
    }

    /// Wipe local LSM state and install SSTables from a Raft snapshot blob.
    ///
    /// Returns the membership frozen inside the snapshot (may be empty for v1).
    pub fn install_sst_snapshot(
        &self,
        data: bytes::Bytes,
        last_included_index: u64,
        last_included_term: u64,
    ) -> Result<crate::membership::ClusterMembership> {
        let payload = SnapshotPayload::decode(data)?;
        if payload.last_included_index != last_included_index
            || payload.last_included_term != last_included_term
        {
            return Err(TakyonicError::Raft(
                "snapshot metadata mismatch with InstallSnapshot RPC".into(),
            ));
        }
        let membership = payload.membership.clone();
        let _flush = self.flush_mu.lock();
        let node = self.raft_node()?;
        node.memtable().clear();

        // Materialize SST files under data_dir, then swap the catalog atomically.
        let mut metas = Vec::with_capacity(payload.files.len());
        for file in &payload.files {
            let path = snapshot_sst_path(self.manager.data_dir(), file.level, file.id);
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }
            let tmp = path.with_extension("sst.tmp");
            {
                use std::io::Write;
                let mut f = fs::File::create(&tmp)?;
                f.write_all(&file.data)?;
                f.sync_all()?;
            }
            fs::rename(&tmp, &path)?;
            metas.push(SstMeta {
                id: file.id,
                level: file.level,
                path,
                smallest: file.smallest.clone(),
                largest: file.largest.clone(),
                file_size: file.data.len() as u64,
            });
        }
        if let Some(parent) = self.manager.data_dir().parent() {
            let _ = fs::File::open(parent).and_then(|f| f.sync_all());
        }
        self.manager.replace_all(metas)?;
        node.set_last_applied(last_included_index);
        // Rotate engine WAL so recovery does not re-apply pre-snapshot memtable noise.
        let next = self.wal_segment.fetch_add(1, Ordering::Relaxed) + 1;
        let wal_path = segment_path(&self.wal_dir, next);
        let new_wal = WalWriter::create(&wal_path)?;
        node.rotate_wal(new_wal)?;
        info!(
            last_included_index,
            last_included_term,
            files = payload.files.len(),
            "installed SST snapshot"
        );
        Ok(membership)
    }

    fn raft_node(&self) -> Result<Arc<LocalRaftNode>> {
        self.ensure_open()?;
        self.raft
            .lock()
            .as_ref()
            .cloned()
            .ok_or_else(|| TakyonicError::Engine("engine is closed".into()))
    }

    fn upgrade_distributed_raft(&self) -> Option<Arc<RaftConsensus>> {
        self.distributed_raft
            .lock()
            .as_ref()
            .and_then(|w| w.upgrade())
    }

    fn require_leader(&self, raft: &RaftConsensus) -> Result<()> {
        if raft.role() == Role::Leader {
            Ok(())
        } else {
            Err(TakyonicError::NotLeader {
                leader_address: raft.leader_address(),
            })
        }
    }

    /// Run an async Raft propose from a sync OCC / put path.
    ///
    /// Uses `block_in_place` so the multi-thread runtime can keep driving
    /// election / AppendEntries while we wait for quorum.
    fn block_on_propose(&self, raft: &Arc<RaftConsensus>, command: RaftCommand) -> Result<u64> {
        let handle = tokio::runtime::Handle::try_current().map_err(|_| {
            TakyonicError::Raft(
                "distributed Raft commit requires a Tokio runtime (use TakyonicClient or async propose)"
                    .into(),
            )
        })?;
        tokio::task::block_in_place(|| handle.block_on(raft.propose(command)))
    }

    fn propose(&self, command: RaftCommand) -> Result<()> {
        match self.admission.acquire_timeout(1, self.admission_timeout)? {
            AdmissionOutcome::Acquired => {}
            AdmissionOutcome::TimedOut => {
                return Err(TakyonicError::Admission(
                    "timed out waiting for write admission".into(),
                ));
            }
        }
        let keys: Vec<Key> = match &command {
            RaftCommand::Put { key, .. } | RaftCommand::Delete { key } => vec![key.clone()],
            RaftCommand::TxnBatch { ops } | RaftCommand::TxnCommit { ops, .. } => {
                ops.iter().map(|(k, _)| k.clone()).collect()
            }
            _ => Vec::new(),
        };

        if let Some(raft) = self.upgrade_distributed_raft() {
            self.require_leader(&raft)?;
            let commit_ts = self.block_on_propose(&raft, command)?;
            if !keys.is_empty() {
                self.note_committed_keys(keys, commit_ts);
            }
            // Apply may have flushed; check memtable size via local stand-in.
            let node = self.raft_node()?;
            self.maybe_flush_node(&node)?;
            return Ok(());
        }

        let node = self.raft_node()?;
        let commit_ts = node.propose(command)?;
        if !keys.is_empty() {
            self.note_committed_keys(keys, commit_ts);
        }
        self.maybe_flush_node(&node)?;
        Ok(())
    }

    fn maybe_flush(&self) -> Result<()> {
        let node = self.raft_node()?;
        self.maybe_flush_node(&node)
    }

    fn maybe_flush_node(&self, node: &LocalRaftNode) -> Result<()> {
        if node.memtable().approx_size_bytes() >= self.config.memtable_size_bytes {
            self.flush_node(node)?;
        }
        Ok(())
    }

    /// Snapshot memtable → L0 SST(s), rotate WAL. Does not hold locks across fsync.
    ///
    /// Large memtables are split into multiple L0 files so each stays under
    /// [`Config::max_sst_bytes`].
    fn flush_node(&self, node: &LocalRaftNode) -> Result<()> {
        let _flush = self.flush_mu.lock();
        let memtable = node.memtable();
        if memtable.is_empty() {
            return Ok(());
        }
        let entries = memtable.drain_entries();
        if entries.is_empty() {
            return Ok(());
        }
        drop(_flush);

        let slices = split_entries_by_max_bytes(&entries, self.manager.max_sst_bytes());
        let mut total_entries = 0usize;
        let mut last_id = 0u64;
        for slice in slices {
            let smallest = slice.first().expect("non-empty").key.clone();
            let largest = slice.last().expect("non-empty").key.clone();
            let id = self.manager.allocate_sst_id();
            let path = self
                .manager
                .data_dir()
                .join("L0")
                .join(format!("{id:020}.sst"));
            let info = SstWriter::write(id, &path, slice, self.manager.block_size())?;
            let meta = SstMeta::from_info(0, info, smallest, largest)?;
            self.manager.add_sst(meta)?;
            total_entries += slice.len();
            last_id = id;
        }
        self.metrics.record_flush();

        let next = self.wal_segment.fetch_add(1, Ordering::Relaxed) + 1;
        let wal_path = segment_path(&self.wal_dir, next);
        let new_wal = WalWriter::create(&wal_path)?;
        node.rotate_wal(new_wal)?;
        // Keep the previous segment: durable-but-not-yet-flushed applies may
        // still reference it until the next flush cycle. Full prune on close.
        debug!(
            sst_id = last_id,
            entries = total_entries,
            "flushed memtable to L0"
        );
        // Persist dirty BPM pages so eviction/checkpoint state matches SST install.
        self.checkpoint_buffer_pool()?;
        self.kick_compaction();
        Ok(())
    }

    fn kick_compaction(&self) {
        let guard = self.compaction.lock();
        let Some(engine) = guard.as_ref() else {
            return;
        };
        match engine.submit_l0() {
            Ok(Some(_)) => debug!("scheduled L0 Rapid compaction"),
            Ok(None) => {}
            Err(error) => warn!(%error, "failed to schedule L0 Rapid compaction"),
        }
        let last_source = self.manager.level_count().saturating_sub(2);
        for level in 1..=last_source {
            match engine.submit_ln(level) {
                Ok(Some(_)) => debug!(level, "scheduled Ln Haul compaction"),
                Ok(None) => {}
                Err(error) => warn!(%error, level, "failed to schedule Ln Haul compaction"),
            }
        }
    }

    /// Submit and wait for compaction jobs until no more work is available.
    fn drain_compaction(&self) -> Result<()> {
        for _ in 0..64 {
            let mut progressed = false;
            let tickets = {
                let guard = self.compaction.lock();
                let Some(engine) = guard.as_ref() else {
                    return Ok(());
                };
                let mut tickets = Vec::new();
                if let Some(t) = engine.submit_l0()? {
                    tickets.push(t);
                }
                let last_source = self.manager.level_count().saturating_sub(2);
                for level in 1..=last_source {
                    if let Some(t) = engine.submit_ln(level)? {
                        tickets.push(t);
                    }
                }
                tickets
            };
            for ticket in tickets {
                ticket.wait()?;
                progressed = true;
            }
            if !progressed {
                break;
            }
        }
        Ok(())
    }

    fn count_versions_prefix(&self, prefix: &[u8]) -> Result<u64> {
        Ok(self.collect_versions_prefix(prefix)?.len() as u64)
    }

    /// All MVCC versions under `prefix` from memtable + SSTs (may include duplicates
    /// across levels; callers group by user key + seq when classifying dead).
    fn collect_versions_prefix(&self, prefix: &[u8]) -> Result<Vec<Entry>> {
        self.ensure_open()?;
        let mut by_key_seq: BTreeMap<(Key, CommitTs), Entry> = BTreeMap::new();

        let node = self.raft_node()?;
        for e in node.memtable().scan_all_versions_prefix(prefix) {
            by_key_seq.insert((e.key.clone(), e.seq), e);
        }
        drop(node);

        let levels = self.manager.level_count();
        for level in 0..levels {
            for meta in self.manager.level_files(level) {
                let Some(pin) = self.manager.registry().pin(meta.id) else {
                    continue;
                };
                for entry in pin.reader().entries()? {
                    if !entry.key.as_bytes().starts_with(prefix) {
                        continue;
                    }
                    by_key_seq
                        .entry((entry.key.clone(), entry.seq))
                        .or_insert(entry);
                }
            }
        }
        Ok(by_key_seq.into_values().collect())
    }

    /// Tombstone secondary-index keys that still point at values no longer present
    /// on the live (watermark-visible) heap row.
    fn purge_dangling_index_entries(self: &Arc<Self>, table: &str, watermark: CommitTs) -> Result<u64> {
        use crate::schema::index_key;
        use crate::txn::index_store_value;

        let schema = self.table_schema(table)?;
        if schema.indexes.is_empty() {
            return Ok(0);
        }
        let data_prefix = data_table_prefix(table);
        let heap = self.collect_versions_prefix(data_prefix.as_ref())?;
        let mut by_key: BTreeMap<Key, Vec<Entry>> = BTreeMap::new();
        for e in heap {
            by_key.entry(e.key.clone()).or_default().push(e);
        }

        let mut txn = self.begin()?;
        let mut purged = 0u64;
        for (_dkey, mut versions) in by_key {
            versions.sort_by_key(|e| std::cmp::Reverse(e.seq));
            let survivors_ts: Vec<CommitTs> = versions.iter().map(|e| e.seq).collect();
            let survivors = crate::epoch::survivors_for_key(&survivors_ts, watermark);
            // Live content: newest survivor (highest commit_ts among survivors).
            let Some(live_ts) = survivors.iter().copied().max() else {
                continue;
            };
            let Some(live_entry) = versions.iter().find(|e| e.seq == live_ts) else {
                continue;
            };
            if live_entry.tombstone {
                // Row deleted: any remaining Idx_ keys for old puts should already
                // be tombstoned by delete_record; GC will drop shadowed versions.
                continue;
            }
            let Some(val) = &live_entry.value else {
                continue;
            };
            let Ok(live_rec) = Record::decode(val) else {
                continue;
            };
            let Some(pk) = live_rec.get(&schema.primary_key).map(str::to_string) else {
                continue;
            };

            for e in &versions {
                if e.tombstone || survivors.contains(&e.seq) {
                    continue;
                }
                let Some(v) = &e.value else {
                    continue;
                };
                let Ok(old) = Record::decode(v) else {
                    continue;
                };
                for idx in &schema.indexes {
                    let Some(old_v) = old.get(&idx.column) else {
                        continue;
                    };
                    if live_rec.get(&idx.column) == Some(old_v) {
                        continue;
                    }
                    let encoded = index_store_value(old_v);
                    let ikey = index_key(table, &idx.name, &encoded, &pk);
                    if txn.get(ikey.clone())?.is_some() {
                        txn.delete(ikey)?;
                        purged += 1;
                    }
                }
            }
        }
        if purged > 0 {
            txn.commit()?;
        } else {
            txn.abort();
        }
        Ok(purged)
    }

    fn ensure_open(&self) -> Result<()> {
        if self.closed.load(Ordering::Acquire) {
            return Err(TakyonicError::Engine("engine is closed".into()));
        }
        Ok(())
    }
}

fn count_dead_across_keys(entries: &[Entry], watermark: CommitTs) -> u64 {
    let mut by_key: BTreeMap<&Key, Vec<CommitTs>> = BTreeMap::new();
    for e in entries {
        by_key.entry(&e.key).or_default().push(e.seq);
    }
    let mut dead = 0u64;
    for versions in by_key.values_mut() {
        versions.sort_by_key(|ts| std::cmp::Reverse(*ts));
        versions.dedup();
        dead += crate::epoch::dead_versions_for_key(versions, watermark).len() as u64;
    }
    dead
}

fn default_local_mpp_workers(n: u32) -> Vec<crate::mpp::WorkerEndpoint> {
    (0..n)
        .map(|slot| crate::mpp::WorkerEndpoint {
            node_id: u64::from(slot) + 1,
            address: format!("local-{slot}"),
            slot,
        })
        .collect()
}

impl Drop for TakyonicEngine {
    fn drop(&mut self) {
        if let Err(error) = self.close() {
            // Tests often `remove_dir_all` before the engine Arc drops; treat
            // missing paths as a benign teardown race.
            if !matches!(&error, TakyonicError::Io(e) if e.kind() == std::io::ErrorKind::NotFound)
            {
                warn!(%error, "error while closing TakyonicEngine on drop");
            }
        }
    }
}

fn recover_wal(wal_dir: &std::path::Path) -> Result<(WalWriter, Arc<Memtable>, u64, u64)> {
    let memtable = Arc::new(Memtable::new());
    let mut next_seq = 1u64;
    let mut highest_segment = 0u64;

    let mut segments = list_wal_segments(wal_dir)?;
    segments.sort_unstable();
    for segment in &segments {
        let path = segment_path(wal_dir, *segment);
        let mut reader = WalReader::open(&path)?;
        reader.replay(|entry| {
            next_seq = next_seq.max(entry.seq.saturating_add(1));
            memtable.apply(entry);
        })?;
        if reader.has_torn_tail() {
            let valid = reader.last_valid_offset();
            drop(reader);
            let file = fs::OpenOptions::new().write(true).open(&path)?;
            file.set_len(valid)?;
            file.sync_data()?;
        } else {
            drop(reader);
        }
        highest_segment = highest_segment.max(*segment);
    }

    let wal = if highest_segment == 0 {
        WalWriter::create(segment_path(wal_dir, 1))?
    } else {
        WalWriter::open_append(segment_path(wal_dir, highest_segment))?
    };
    let wal_segment = if highest_segment == 0 {
        1
    } else {
        highest_segment
    };
    Ok((wal, memtable, next_seq, wal_segment))
}

fn recover_existing_ssts(manager: &SstManager) -> Result<()> {
    for level in 0..manager.level_count() {
        let dir = manager.data_dir().join(format!("L{level}"));
        if !dir.exists() {
            continue;
        }
        let mut files = Vec::new();
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            let path = entry.path();
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            let Some(stem) = name.strip_suffix(".sst") else {
                continue;
            };
            if let Ok(id) = stem.parse::<u64>() {
                files.push((id, path));
            }
        }
        files.sort_by_key(|(id, _)| *id);
        for (id, path) in files {
            manager.recover_sst_file(level, id, path)?;
        }
    }
    Ok(())
}

/// Pull SSTs listed in the shared manifest from object storage into the local
/// data dir when this node has an empty / partial Tier-1 cache.
fn hydrate_ssts_from_manifest(
    manager: &SstManager,
    manifest: &StorageManifest,
) -> Result<()> {
    if !manager.has_remote() || manifest.sstables.is_empty() {
        return Ok(());
    }
    let present: std::collections::HashSet<u64> =
        manager.all_files().into_iter().map(|m| m.id).collect();
    let mut hydrated = 0u64;
    for sst in &manifest.sstables {
        if present.contains(&sst.id) {
            continue;
        }
        let level = sst.level as usize;
        let local = manager
            .data_dir()
            .join(format!("L{level}"))
            .join(format!("{:020}.sst", sst.id));
        let key = if sst.path.starts_with("sst/") {
            sst.path.clone()
        } else {
            sst_object_key(level, sst.id)
        };
        info!(
            id = sst.id,
            level,
            %key,
            path = %local.display(),
            "hydrating SST from object store"
        );
        manager.download_sst(&key, &local)?;
        manager.recover_sst_file(level, sst.id, local)?;
        hydrated += 1;
    }
    if hydrated > 0 {
        info!(
            hydrated,
            manifest_version = manifest.version,
            "manifest hydrate complete"
        );
    }
    Ok(())
}

fn prune_wal_segments_below(wal_dir: &std::path::Path, keep_from: u64) -> Result<()> {
    for segment in list_wal_segments(wal_dir)? {
        if segment < keep_from {
            let path = segment_path(wal_dir, segment);
            match fs::remove_file(&path) {
                Ok(()) => debug!(path = %path.display(), "pruned flushed WAL segment"),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
    }
    Ok(())
}

/// Durable `next_seq` high-water mark, updated on clean close.
///
/// Written atomically (temp + rename + dir sync) so a crash mid-update leaves
/// either the old or the new marker, never a torn one.
fn write_seq_marker(wal_dir: &std::path::Path, next_seq: u64) -> Result<()> {
    use std::io::Write;
    let tmp = wal_dir.join("SEQNO.tmp");
    let dst = wal_dir.join("SEQNO");
    {
        let mut file = fs::File::create(&tmp)?;
        file.write_all(format!("{next_seq}").as_bytes())?;
        file.sync_all()?;
    }
    fs::rename(&tmp, &dst)?;
    fs::File::open(wal_dir)?.sync_all()?;
    Ok(())
}

/// Best-effort read of the SEQNO marker; absence or garbage reads as 0.
///
/// The marker only ever raises `next_seq` (max’d with WAL replay), so a stale
/// or missing marker is always safe.
fn read_seq_marker(wal_dir: &std::path::Path) -> u64 {
    fs::read_to_string(wal_dir.join("SEQNO"))
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(0)
}

fn list_wal_segments(wal_dir: &std::path::Path) -> Result<Vec<u64>> {
    let mut out = Vec::new();
    if !wal_dir.exists() {
        return Ok(out);
    }
    for entry in fs::read_dir(wal_dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let Some(stem) = name.strip_suffix(".wal") else {
            continue;
        };
        if let Ok(id) = stem.parse::<u64>() {
            out.push(id);
        }
    }
    Ok(out)
}

/// On-disk object comments file (`COMMENT ON` / `obj_description`).
pub const COMMENTS_FILE: &str = "COMMENTS";

fn comments_path(data_dir: &Path) -> PathBuf {
    data_dir.join(COMMENTS_FILE)
}

fn escape_comment_value(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '\t' => out.push_str("\\t"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            other => out.push(other),
        }
    }
    out
}

fn unescape_comment_value(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.next() {
                Some('\\') => out.push('\\'),
                Some('t') => out.push('\t'),
                Some('n') => out.push('\n'),
                Some('r') => out.push('\r'),
                Some(other) => {
                    out.push('\\');
                    out.push(other);
                }
                None => out.push('\\'),
            }
        } else {
            out.push(c);
        }
    }
    out
}

/// Serialize comments to COMMENTS file text (Raft `CommentsReplace` payload).
pub fn encode_comments(map: &BTreeMap<String, String>) -> String {
    let mut out = String::from("# Takyonic COMMENTS\n");
    for (k, v) in map {
        out.push_str(k);
        out.push('\t');
        out.push_str(&escape_comment_value(v));
        out.push('\n');
    }
    out
}

/// Parse COMMENTS file text.
pub fn parse_comments(text: &str) -> Result<BTreeMap<String, String>> {
    let mut map = BTreeMap::new();
    for line in text.lines() {
        let line = line.trim_end();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((key, value)) = line.split_once('\t') else {
            return Err(TakyonicError::Integrity(format!(
                "bad COMMENTS line (expected key\\tvalue): {line}"
            )));
        };
        map.insert(key.to_string(), unescape_comment_value(value));
    }
    Ok(map)
}

/// Load comments from `data_dir/COMMENTS` (empty map when missing).
pub fn load_comments(data_dir: &Path) -> Result<BTreeMap<String, String>> {
    let path = comments_path(data_dir);
    if !path.exists() {
        return Ok(BTreeMap::new());
    }
    let text = fs::read_to_string(&path)?;
    parse_comments(&text)
}

/// Atomically rewrite `data_dir/COMMENTS`.
pub fn save_comments(data_dir: &Path, map: &BTreeMap<String, String>) -> Result<()> {
    fs::create_dir_all(data_dir)?;
    let path = comments_path(data_dir);
    let tmp = data_dir.join(format!("{COMMENTS_FILE}.tmp"));
    {
        use std::io::Write;
        let mut f = fs::File::create(&tmp)?;
        f.write_all(encode_comments(map).as_bytes())?;
        f.sync_all()?;
    }
    fs::rename(&tmp, &path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::Record;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_config(name: &str, memtable_bytes: usize) -> Config {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("takyonic-engine-{name}-{nanos}"));
        Config::default()
            .data_dir(root.join("data"))
            .wal_dir(root.join("wal"))
            .memtable_size_bytes(memtable_bytes)
            .block_size_bytes(64)
            .l0_soft_limit(8)
            .l0_hard_limit(32)
            .l0_rapid_pool_threads(1)
            .ln_haul_pool_threads(1)
            .compaction_write_bytes_per_sec(1024 * 1024 * 1024)
            .write_admission_ops_per_sec(100_000)
            .write_admission_min_ops_per_sec(1_000)
            .write_admission_burst(10_000)
    }

    #[test]
    fn comments_replace_installs_via_apply_committed() {
        let config = temp_config("comments-raft-apply", 64 * 1024 * 1024);
        let data_dir = config.data_dir.clone();
        let engine = TakyonicEngine::open(config).unwrap();
        let mut map = BTreeMap::new();
        map.insert("t:users".to_string(), "via raft".to_string());
        map.insert("r:postgres".to_string(), "super".to_string());
        let blob = bytes::Bytes::from(encode_comments(&map).into_bytes());
        let next = engine.last_applied() + 1;
        engine
            .apply_committed(&[CommittedEntry::new(
                next,
                RaftCommand::comments_replace(blob),
            )])
            .unwrap();
        assert_eq!(engine.table_comment("users").as_deref(), Some("via raft"));
        assert_eq!(
            engine.shared_comment("role", "postgres").as_deref(),
            Some("super")
        );
        let loaded = load_comments(&data_dir).unwrap();
        assert_eq!(loaded.get("t:users").map(String::as_str), Some("via raft"));
        engine.close().unwrap();
    }

    #[test]
    fn comments_file_roundtrip_and_engine_reload() {
        let config = temp_config("comments-persist", 64 * 1024 * 1024);
        let root = config.data_dir.parent().unwrap().to_path_buf();
        let data_dir = config.data_dir.clone();
        {
            let engine = TakyonicEngine::open(config).unwrap();
            engine
                .register_table(TableSchema::new("users", "id", vec![]))
                .unwrap();
            engine
                .set_table_comment("users", Some("people\twith\nescapes"))
                .unwrap();
            engine
                .set_column_comment("users", "id", Some("pk"))
                .unwrap();
            assert_eq!(
                engine.table_comment("users").as_deref(),
                Some("people\twith\nescapes")
            );
            engine.close().unwrap();
        }
        let reopened = TakyonicEngine::open(
            Config::default()
                .data_dir(data_dir.clone())
                .wal_dir(root.join("wal"))
                .memtable_size_bytes(64 * 1024 * 1024)
                .block_size_bytes(64)
                .l0_soft_limit(8)
                .l0_hard_limit(32)
                .l0_rapid_pool_threads(1)
                .ln_haul_pool_threads(1)
                .compaction_write_bytes_per_sec(1024 * 1024 * 1024)
                .write_admission_ops_per_sec(100_000)
                .write_admission_min_ops_per_sec(1_000)
                .write_admission_burst(10_000),
        )
        .unwrap();
        assert_eq!(
            reopened.table_comment("users").as_deref(),
            Some("people\twith\nescapes")
        );
        assert_eq!(reopened.column_comment("users", "id").as_deref(), Some("pk"));
        reopened.close().unwrap();
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn put_get_delete_roundtrip() {
        let config = temp_config("kv", 64 * 1024 * 1024);
        let root = config.data_dir.parent().unwrap().to_path_buf();
        let engine = TakyonicEngine::open(config).unwrap();
        engine.put(&b"hello"[..], &b"world"[..]).unwrap();
        assert_eq!(
            engine
                .get(&Key::new(&b"hello"[..]))
                .unwrap()
                .unwrap()
                .as_bytes(),
            b"world"
        );
        engine.delete(&b"hello"[..]).unwrap();
        assert!(engine.get(&Key::new(&b"hello"[..])).unwrap().is_none());
        engine.close().unwrap();
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn flush_makes_data_readable_from_sst() {
        let config = temp_config("flush", 64);
        let root = config.data_dir.parent().unwrap().to_path_buf();
        let engine = TakyonicEngine::open(config).unwrap();
        engine.put(&b"alpha"[..], &b"1"[..]).unwrap();
        engine.put(&b"bravo"[..], &b"2"[..]).unwrap();
        assert_eq!(
            engine
                .get(&Key::new(&b"alpha"[..]))
                .unwrap()
                .unwrap()
                .as_bytes(),
            b"1"
        );
        assert_eq!(
            engine
                .get(&Key::new(&b"bravo"[..]))
                .unwrap()
                .unwrap()
                .as_bytes(),
            b"2"
        );
        engine.close().unwrap();
        let has_sst = (0..engine.manager.level_count())
            .any(|level| !engine.manager.level_files(level).is_empty());
        assert!(
            has_sst,
            "expected flushed data to land in at least one SST level"
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn reopen_serves_flushed_sst_data() {
        let config = temp_config("reopen", 64 * 1024 * 1024);
        let root = config.data_dir.parent().unwrap().to_path_buf();
        let wal_dir = config.wal_dir.clone();
        let data_dir = config.data_dir.clone();
        {
            let engine = TakyonicEngine::open(config).unwrap();
            engine.put(&b"persist"[..], &b"yes"[..]).unwrap();
            engine.close().unwrap();
        }
        let reopened = TakyonicEngine::open(
            Config::default()
                .data_dir(data_dir)
                .wal_dir(wal_dir)
                .memtable_size_bytes(64 * 1024 * 1024)
                .l0_rapid_pool_threads(1)
                .ln_haul_pool_threads(1)
                .compaction_write_bytes_per_sec(1024 * 1024 * 1024),
        )
        .unwrap();
        assert_eq!(
            reopened
                .get(&Key::new(&b"persist"[..]))
                .unwrap()
                .unwrap()
                .as_bytes(),
            b"yes"
        );
        reopened.close().unwrap();
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn closed_engine_rejects_writes() {
        let config = temp_config("closed", 1024 * 1024);
        let root = config.data_dir.parent().unwrap().to_path_buf();
        let engine = TakyonicEngine::open(config).unwrap();
        engine.close().unwrap();
        assert!(matches!(
            engine.put(&b"x"[..], &b"y"[..]),
            Err(TakyonicError::Engine(_))
        ));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn concurrent_puts_group_commit() {
        let config = temp_config("gc", 64 * 1024 * 1024);
        let root = config.data_dir.parent().unwrap().to_path_buf();
        let engine = Arc::new(TakyonicEngine::open(config).unwrap());
        let mut handles = Vec::new();
        for t in 0..8 {
            let engine = Arc::clone(&engine);
            handles.push(std::thread::spawn(move || {
                for i in 0..100 {
                    let key = format!("t{t}-k{i}");
                    engine.put(key.into_bytes(), b"v".as_slice()).unwrap();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert!(
            engine.avg_group_batch_size() > 1.0,
            "expected coalescing, avg batch={}",
            engine.avg_group_batch_size()
        );
        assert_eq!(
            engine
                .get(&Key::new(b"t0-k0".as_slice()))
                .unwrap()
                .unwrap()
                .as_bytes(),
            b"v"
        );
        engine.close().unwrap();
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn create_table_persists_in_catalog_across_reopen() {
        let config = temp_config("tbl-catalog", 64 * 1024 * 1024);
        let root = config.data_dir.parent().unwrap().to_path_buf();
        let wal_dir = config.wal_dir.clone();
        let data_dir = config.data_dir.clone();
        {
            let engine = Arc::new(TakyonicEngine::open(config).unwrap());
            engine
                .create_table(
                    "items",
                    "id",
                    vec![
                        ColumnSpec::new("id", "BIGINT"),
                        ColumnSpec::new("name", "TEXT"),
                    ],
                    false,
                )
                .unwrap();
            let schema = engine.table_schema("items").unwrap();
            assert_eq!(schema.columns.len(), 2);
            assert_eq!(schema.columns[1].name, "name");
            engine.close().unwrap();
        }
        let reopened = Arc::new(
            TakyonicEngine::open(
                Config::default()
                    .data_dir(data_dir)
                    .wal_dir(wal_dir)
                    .memtable_size_bytes(64 * 1024 * 1024)
                    .l0_rapid_pool_threads(1)
                    .ln_haul_pool_threads(1)
                    .compaction_write_bytes_per_sec(1024 * 1024 * 1024),
            )
            .unwrap(),
        );
        let schema = reopened.table_schema("items").unwrap();
        assert_eq!(schema.primary_key, "id");
        assert_eq!(schema.columns[0].data_type, "BIGINT");
        assert_eq!(schema.columns[1].data_type, "TEXT");
        reopened.close().unwrap();
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn create_index_persists_in_catalog_across_reopen() {
        let config = temp_config("idx-catalog", 64 * 1024 * 1024);
        let root = config.data_dir.parent().unwrap().to_path_buf();
        let wal_dir = config.wal_dir.clone();
        let data_dir = config.data_dir.clone();
        {
            let engine = Arc::new(TakyonicEngine::open(config).unwrap());
            engine
                .register_table(TableSchema::new("employees", "id", vec![]))
                .unwrap();
            engine
                .create_index("idx_dept", "employees", "department", false)
                .unwrap();
            let schema = engine.table_schema("employees").unwrap();
            assert_eq!(schema.indexes.len(), 1);
            assert_eq!(schema.indexes[0].name, "idx_dept");
            assert_eq!(schema.indexes[0].column, "department");
            engine.close().unwrap();
        }
        let reopened = Arc::new(
            TakyonicEngine::open(
                Config::default()
                    .data_dir(data_dir)
                    .wal_dir(wal_dir)
                    .memtable_size_bytes(64 * 1024 * 1024)
                    .l0_rapid_pool_threads(1)
                    .ln_haul_pool_threads(1)
                    .compaction_write_bytes_per_sec(1024 * 1024 * 1024),
            )
            .unwrap(),
        );
        let schema = reopened.table_schema("employees").unwrap();
        assert_eq!(schema.indexes[0].name, "idx_dept");
        let (found, idx) = reopened.find_index("idx_dept").unwrap();
        assert_eq!(found.name, "employees");
        assert_eq!(idx.column, "department");
        reopened.close().unwrap();
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn create_user_grant_survives_engine_reopen() {
        use crate::pg::SessionState;
        use crate::rbac::Privilege;

        let config = temp_config("auth-reopen", 64 * 1024 * 1024);
        let root = config.data_dir.parent().unwrap().to_path_buf();
        let wal_dir = config.wal_dir.clone();
        let data_dir = config.data_dir.clone();
        {
            let engine = Arc::new(TakyonicEngine::open(config).unwrap());
            engine
                .register_table(TableSchema::new("employees", "id", vec![]))
                .unwrap();
            let mut admin = SessionState::new(Arc::clone(&engine));
            admin
                .execute_sql("CREATE USER analyst WITH PASSWORD 'secret'")
                .unwrap();
            admin
                .execute_sql("GRANT SELECT ON employees TO analyst")
                .unwrap();
            engine.close().unwrap();
        }
        let engine = Arc::new(
            TakyonicEngine::open(
                Config::default()
                    .data_dir(data_dir)
                    .wal_dir(wal_dir)
                    .memtable_size_bytes(64 * 1024 * 1024)
                    .l0_rapid_pool_threads(1)
                    .ln_haul_pool_threads(1)
                    .compaction_write_bytes_per_sec(1024 * 1024 * 1024),
            )
            .unwrap(),
        );
        {
            let auth = engine.auth_catalog();
            let cat = auth.read();
            assert!(cat.get_role("analyst").is_some());
            assert!(cat.verify_password("analyst", "secret"));
            let ctx = cat.auth_context("analyst").unwrap();
            assert!(cat.has_privilege(&ctx, "employees", Privilege::Select));
        }
        let analyst = SessionState::as_user(Arc::clone(&engine), "analyst").unwrap();
        assert_eq!(analyst.current_user(), "analyst");
        engine.close().unwrap();
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn analyze_stats_persist_across_reopen() {
        let config = temp_config("analyze-stats", 64 * 1024 * 1024);
        let root = config.data_dir.parent().unwrap().to_path_buf();
        let wal_dir = config.wal_dir.clone();
        let data_dir = config.data_dir.clone();
        {
            let engine = Arc::new(TakyonicEngine::open(config).unwrap());
            engine
                .register_table(TableSchema::new(
                    "employees",
                    "id",
                    vec![IndexDef::new("idx_dept", "department")],
                ))
                .unwrap();
            let mut txn = engine.begin().unwrap();
            for i in 1..=10 {
                let dept = if i <= 8 { "Sales" } else { "Engineering" };
                txn.put_record(
                    "employees",
                    Record::new()
                        .set("id", i.to_string())
                        .set("department", dept)
                        .set("salary", (i * 100).to_string()),
                )
                .unwrap();
            }
            txn.commit().unwrap();

            let mut txn = engine.begin().unwrap();
            let st = engine.analyze_table(&mut txn, "employees").unwrap();
            txn.abort();
            assert_eq!(st.row_count, 10);
            assert_eq!(st.columns.get("department").unwrap().ndv, 2);
            assert_eq!(
                st.columns.get("department").unwrap().min.as_deref(),
                Some("Engineering")
            );
            assert_eq!(
                st.columns.get("department").unwrap().max.as_deref(),
                Some("Sales")
            );
            engine.close().unwrap();
        }
        let reopened = Arc::new(
            TakyonicEngine::open(
                Config::default()
                    .data_dir(data_dir)
                    .wal_dir(wal_dir)
                    .memtable_size_bytes(64 * 1024 * 1024)
                    .l0_rapid_pool_threads(1)
                    .ln_haul_pool_threads(1)
                    .compaction_write_bytes_per_sec(1024 * 1024 * 1024),
            )
            .unwrap(),
        );
        let st = reopened.table_stats("employees");
        assert_eq!(st.row_count, 10);
        assert_eq!(st.columns.get("department").unwrap().ndv, 2);
        assert!(!st.columns.get("department").unwrap().mcv.is_empty());
        reopened.close().unwrap();
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn put_delete_record_maintains_secondary_index_keys() {
        let config = temp_config("idx-mvcc", 64 * 1024 * 1024);
        let root = config.data_dir.parent().unwrap().to_path_buf();
        let engine = Arc::new(TakyonicEngine::open(config).unwrap());
        engine
            .register_table(TableSchema::new(
                "employees",
                "id",
                vec![IndexDef::new("idx_dept", "department")],
            ))
            .unwrap();

        let mut txn = engine.begin().unwrap();
        txn.put_record(
            "employees",
            crate::schema::Record::new()
                .set("id", "1")
                .set("department", "Engineering")
                .set("salary", "9000"),
        )
        .unwrap();
        txn.commit().unwrap();

        let idx_key =
            crate::schema::index_key("employees", "idx_dept", "Engineering", "1");
        assert!(
            engine.get(&idx_key).unwrap().is_some(),
            "insert must write secondary index key"
        );
        assert!(
            engine
                .get(&crate::schema::data_key("employees", "1"))
                .unwrap()
                .is_some(),
            "insert must write primary data key"
        );

        let mut txn = engine.begin().unwrap();
        txn.delete_record("employees", "1").unwrap();
        txn.commit().unwrap();

        assert!(
            engine.get(&idx_key).unwrap().is_none(),
            "delete must remove secondary index key"
        );
        assert!(
            engine
                .get(&crate::schema::data_key("employees", "1"))
                .unwrap()
                .is_none(),
            "delete must remove primary data key"
        );

        engine.close().unwrap();
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn aries_wal_recovers_committed_txn_after_crash_abandon() {
        let config = temp_config("aries-crash", 64 * 1024 * 1024);
        let root = config.data_dir.parent().unwrap().to_path_buf();
        let wal_dir = config.wal_dir.clone();
        let data_dir = config.data_dir.clone();

        {
            let engine = Arc::new(TakyonicEngine::open(config).unwrap());
            engine
                .register_table(TableSchema::new("users", "id", vec![]))
                .unwrap();

            let mut txn = engine.begin().unwrap();
            txn.put_record(
                "users",
                crate::schema::Record::new()
                    .set("id", "1")
                    .set("name", "Ada")
                    .set("age", "36"),
            )
            .unwrap();
            txn.put_record(
                "users",
                crate::schema::Record::new()
                    .set("id", "2")
                    .set("name", "Bob")
                    .set("age", "25"),
            )
            .unwrap();
            txn.commit().unwrap();

            // Hard crash: no memtable→SST flush, no WAL prune, no ARIES checkpoint.
            // Leak the Arc so `Drop`/`close` never run (true power-loss simulation).
            engine.abandon_for_crash_test().unwrap();
            std::mem::forget(engine);
        }

        // Reopen: LSM WAL + ARIES Redo must restore committed rows.
        let engine = Arc::new(
            TakyonicEngine::open(
                Config::default()
                    .data_dir(data_dir)
                    .wal_dir(wal_dir)
                    .memtable_size_bytes(64 * 1024 * 1024)
                    .l0_rapid_pool_threads(1)
                    .ln_haul_pool_threads(1)
                    .compaction_write_bytes_per_sec(1024 * 1024 * 1024),
            )
            .unwrap(),
        );
        // Catalog is durable separately.
        assert!(engine.table_schema("users").is_ok());

        let mut txn = engine.begin().unwrap();
        let rows = txn.scan_table_records("users").unwrap();
        txn.abort();
        assert_eq!(rows.len(), 2, "expected Ada+Bob recovered, got {rows:?}");
        let mut names: Vec<_> = rows
            .iter()
            .filter_map(|r| r.get("name").map(str::to_string))
            .collect();
        names.sort();
        assert_eq!(names, vec!["Ada".to_string(), "Bob".to_string()]);

        engine.close().unwrap();
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn metrics_http_scrape_reflects_jit_bpm_and_txn() {
        use crate::pg::SessionState;
        use crate::schema::IndexDef;
        use std::io::{Read, Write};
        use std::net::TcpStream;
        use std::thread;
        use std::time::Duration;

        let config = temp_config("metrics-e2e", 64 * 1024 * 1024)
            .bpm_pool_size(64)
            .bpm_page_size(4096)
            .bpm_lru_k(2)
            .metrics_enabled(true)
            .metrics_bind("127.0.0.1:0");
        let root = config.data_dir.parent().unwrap().to_path_buf();
        let engine = Arc::new(TakyonicEngine::open(config).unwrap());
        let addr = engine
            .metrics_bind_addr()
            .expect("metrics server must bind");

        engine
            .register_table(TableSchema::new(
                "employees",
                "id",
                vec![IndexDef::new("age", "age")],
            ))
            .unwrap();
        let mut session = SessionState::new(Arc::clone(&engine));
        session
            .execute_sql(
                "INSERT INTO employees (id, age, salary, tax_rate) VALUES \
                 (1, 25, 90, 1), (2, 35, 100, 2), (3, 40, 200, 3), (4, 28, 80, 1), (5, 50, 150, 2)",
            )
            .unwrap();
        // Trigger JIT push pipeline.
        let result = session
            .execute_sql("SELECT SUM(salary * tax_rate) FROM employees WHERE age > 30")
            .unwrap();
        assert_eq!(result.rows.len(), 1);

        // Force SST + BPM traffic.
        engine.force_flush().unwrap();
        let mut txn = engine.begin().unwrap();
        let _ = txn.scan_table_records("employees").unwrap();
        txn.abort();

        assert!(engine.metrics().txn_commits() >= 1);
        assert!(
            engine.metrics().jit_compilations() >= 1 || engine.metrics().jit_executions() >= 1,
            "JIT should have compiled or executed"
        );

        thread::sleep(Duration::from_millis(50));
        let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(2)).unwrap();
        stream
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .unwrap();
        let mut resp = String::new();
        stream.read_to_string(&mut resp).unwrap();
        assert!(resp.contains("200 OK"), "resp={resp}");
        assert!(
            resp.contains("takyonic_txn_commits_total"),
            "missing txn commits: {resp}"
        );
        assert!(
            resp.contains("takyonic_bpm_hits_total") || resp.contains("takyonic_bpm_misses_total"),
            "missing BPM series: {resp}"
        );
        assert!(
            resp.contains("takyonic_jit_compilations_total")
                || resp.contains("takyonic_jit_executions_total"),
            "missing JIT series: {resp}"
        );

        engine.close().unwrap();
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn btree_table_survives_engine_reopen_via_lsm_hydrate() {
        let config = temp_config("btree-hydrate", 64 * 1024 * 1024);
        let root = config.data_dir.parent().unwrap().to_path_buf();
        let wal_dir = config.wal_dir.clone();
        let data_dir = config.data_dir.clone();
        {
            let engine = Arc::new(TakyonicEngine::open(config).unwrap());
            engine
                .register_table(
                    TableSchema::new("hot", "id", vec![])
                        .with_engine(crate::storage::StorageEngineKind::BTree),
                )
                .unwrap();
            for i in 0..5 {
                let mut txn = engine.begin().unwrap();
                txn.put_record(
                    "hot",
                    Record::new()
                        .set("id", i.to_string())
                        .set("v", format!("v{i}")),
                )
                .unwrap();
                txn.commit().unwrap();
            }
            // Read path must use the B-Tree mirror (not only LSM).
            let mut txn = engine.begin().unwrap();
            assert_eq!(
                txn.get_record("hot", "3").unwrap().unwrap().get("v").unwrap(),
                "v3"
            );
            txn.abort();
            engine.close().unwrap();
        }
        let reopened = Arc::new(
            TakyonicEngine::open(
                Config::default()
                    .data_dir(data_dir)
                    .wal_dir(wal_dir)
                    .memtable_size_bytes(64 * 1024 * 1024)
                    .l0_rapid_pool_threads(1)
                    .ln_haul_pool_threads(1)
                    .compaction_write_bytes_per_sec(1024 * 1024 * 1024),
            )
            .unwrap(),
        );
        assert_eq!(
            reopened.table_schema("hot").unwrap().storage_engine,
            crate::storage::StorageEngineKind::BTree
        );
        // Without LSM→BTree hydrate, the in-memory mirror would be empty here.
        let mut txn = reopened.begin().unwrap();
        let row = txn.get_record("hot", "3").unwrap().unwrap();
        assert_eq!(row.get("v").unwrap(), "v3");
        let keys = reopened
            .scan_prefix_keys(data_table_prefix("hot").as_ref(), txn.read_ts())
            .unwrap();
        assert_eq!(keys.len(), 5);
        txn.abort();
        reopened.close().unwrap();
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn btree_table_vacuum_and_analyze_via_storage_manager() {
        let config = temp_config("btree-vac", 64 * 1024 * 1024);
        let root = config.data_dir.parent().unwrap().to_path_buf();
        let engine = Arc::new(TakyonicEngine::open(config).unwrap());
        let schema = TableSchema::new("hot", "id", vec![]).with_engine(
            crate::storage::StorageEngineKind::BTree,
        );
        engine.register_table(schema).unwrap();

        for i in 0..30 {
            let mut txn = engine.begin().unwrap();
            txn.put_record(
                "hot",
                Record::new()
                    .set("id", i.to_string())
                    .set("v", format!("a{i}")),
            )
            .unwrap();
            txn.commit().unwrap();
        }
        // Second version on a subset → dead tuples after watermark advances.
        for i in 0..10 {
            let mut txn = engine.begin().unwrap();
            txn.put_record(
                "hot",
                Record::new()
                    .set("id", i.to_string())
                    .set("v", format!("b{i}")),
            )
            .unwrap();
            txn.commit().unwrap();
        }

        let mut txn = engine.begin().unwrap();
        let st = engine.analyze_table(&mut txn, "hot").unwrap();
        txn.abort();
        assert!(st.row_count >= 20);

        let vac = engine.vacuum_table("hot").unwrap();
        assert_eq!(vac.table, "hot");
        // B-Tree VACUUM should reclaim at least the overwritten versions.
        assert!(
            vac.memtable_removed >= 1 || vac.dead_heap_versions >= 1,
            "expected dead version reclaim, got {vac:?}"
        );

        let mut txn = engine.begin().unwrap();
        let row = txn.get_record("hot", "0").unwrap().unwrap();
        txn.abort();
        assert_eq!(row.get("v").unwrap(), "b0");

        engine.close().unwrap();
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn engine_open_loads_remote_manifest_and_hydrates_pages() {
        use crate::object_store::{InMemoryObjectStore, S3Backend};
        use crate::page::DEFAULT_PAGE_SIZE;

        let store = InMemoryObjectStore::new();
        let remote: Arc<dyn ObjectStorage> =
            Arc::new(S3Backend::mock("eng", Arc::clone(&store) as Arc<dyn ObjectStorage>));

        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root_a = std::env::temp_dir().join(format!("takyonic-eng-remote-a-{nanos}"));
        let cfg_a = Config::default()
            .data_dir(root_a.join("data"))
            .wal_dir(root_a.join("wal"))
            .memtable_size_bytes(64 * 1024 * 1024)
            .bpm_pool_size(16)
            .bpm_page_size(DEFAULT_PAGE_SIZE)
            .l0_rapid_pool_threads(1)
            .ln_haul_pool_threads(1)
            .compaction_write_bytes_per_sec(1024 * 1024 * 1024);

        let engine_a =
            Arc::new(TakyonicEngine::open_with_object_storage(cfg_a, Arc::clone(&remote)).unwrap());
        assert!(engine_a.manifest().is_some());
        let published = engine_a.publish_storage_manifest().unwrap().unwrap();
        assert!(published.version >= 1);
        published.verify().unwrap();

        let bpm = Arc::clone(engine_a.buffer_pool().unwrap());
        let guard = bpm.new_page().unwrap();
        let page_id = guard.page_id();
        guard.write(|data| {
            data[..6].copy_from_slice(b"REMOTE");
        });
        drop(guard);
        bpm.flush_all().unwrap();
        engine_a.close().unwrap();

        // Cold start: wipe local Tier-1 cache, reopen from shared object store.
        let _ = fs::remove_dir_all(&root_a);
        let root_b = std::env::temp_dir().join(format!("takyonic-eng-remote-b-{nanos}"));
        let cfg_b = Config::default()
            .data_dir(root_b.join("data"))
            .wal_dir(root_b.join("wal"))
            .memtable_size_bytes(64 * 1024 * 1024)
            .bpm_pool_size(16)
            .bpm_page_size(DEFAULT_PAGE_SIZE)
            .l0_rapid_pool_threads(1)
            .ln_haul_pool_threads(1)
            .compaction_write_bytes_per_sec(1024 * 1024 * 1024);

        let engine_b =
            Arc::new(TakyonicEngine::open_with_object_storage(cfg_b, Arc::clone(&remote)).unwrap());
        let m = engine_b.manifest().unwrap().current();
        assert!(m.version >= 1);
        m.verify().unwrap();

        let bpm_b = Arc::clone(engine_b.buffer_pool().unwrap());
        let before = bpm_b.stats().remote_fetches;
        let guard = bpm_b.fetch_page(page_id).unwrap();
        guard.read(|data| {
            assert_eq!(&data[..6], b"REMOTE");
        });
        drop(guard);
        assert!(bpm_b.stats().remote_fetches > before);

        engine_b.close().unwrap();
        let _ = fs::remove_dir_all(root_b);
    }

    /// D2: default [`Config::object_store_root`] path — after flush+close, wiping
    /// Tier-1 and reopening must hydrate SSTs (and serve KV) from the shared store.
    #[test]
    fn object_store_root_cold_restart_hydrates_sst_and_kv() {
        use crate::page::DEFAULT_PAGE_SIZE;

        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("takyonic-d2-hydrate-{nanos}"));
        let _ = fs::remove_dir_all(&root);
        let objects = root.join("objects");
        let data = root.join("data");
        let wal = root.join("wal");

        let cfg = Config::default()
            .data_dir(&data)
            .wal_dir(&wal)
            .object_store_root(&objects)
            .memtable_size_bytes(64 * 1024 * 1024)
            .bpm_pool_size(16)
            .bpm_page_size(DEFAULT_PAGE_SIZE)
            .l0_rapid_pool_threads(1)
            .ln_haul_pool_threads(1)
            .compaction_write_bytes_per_sec(1024 * 1024 * 1024);

        let engine = TakyonicEngine::open(cfg).unwrap();
        assert!(engine.manifest().is_some());
        engine.put(&b"d2-key"[..], &b"from-object-store"[..]).unwrap();
        engine.force_flush().unwrap();
        let published = engine.publish_storage_manifest().unwrap().unwrap();
        assert!(
            !published.sstables.is_empty(),
            "flush must publish at least one SST into the shared manifest"
        );
        published.verify().unwrap();

        // Also exercise pages Tier-2 so cold reopen can hydrate BPM frames.
        let bpm = Arc::clone(engine.buffer_pool().unwrap());
        let guard = bpm.new_page().unwrap();
        let page_id = guard.page_id();
        guard.write(|data| {
            data[..8].copy_from_slice(b"D2PAGES!");
        });
        drop(guard);
        bpm.flush_all().unwrap();
        engine.close().unwrap();

        // Wipe Tier-1 only; shared object store keeps CURRENT.json + SST + pages.
        fs::remove_dir_all(&data).unwrap();
        fs::remove_dir_all(&wal).unwrap();

        let cfg2 = Config::default()
            .data_dir(root.join("data-cold"))
            .wal_dir(root.join("wal-cold"))
            .object_store_root(&objects)
            .memtable_size_bytes(64 * 1024 * 1024)
            .bpm_pool_size(16)
            .bpm_page_size(DEFAULT_PAGE_SIZE)
            .l0_rapid_pool_threads(1)
            .ln_haul_pool_threads(1)
            .compaction_write_bytes_per_sec(1024 * 1024 * 1024);

        let cold = TakyonicEngine::open(cfg2).unwrap();
        let m = cold.manifest().unwrap().current();
        assert!(
            m.version >= published.version,
            "cold open must load published manifest version"
        );
        assert!(
            !m.sstables.is_empty(),
            "cold open must see SST inventory from object store"
        );
        assert!(
            !cold.manager.all_files().is_empty(),
            "hydrate_ssts_from_manifest must install local SST files"
        );
        assert_eq!(
            cold.get(&Key::new(&b"d2-key"[..]))
                .unwrap()
                .unwrap()
                .as_bytes(),
            b"from-object-store"
        );

        let bpm_cold = Arc::clone(cold.buffer_pool().unwrap());
        let before = bpm_cold.stats().remote_fetches;
        let g = bpm_cold.fetch_page(page_id).unwrap();
        g.read(|data| assert_eq!(&data[..8], b"D2PAGES!"));
        drop(g);
        assert!(
            bpm_cold.stats().remote_fetches > before,
            "BPM miss must hydrate page bytes from object store"
        );

        cold.close().unwrap();
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn concurrent_cross_node_reads_share_manifest_integrity() {
        use crate::object_store::{InMemoryObjectStore, S3Backend};
        use crate::page::DEFAULT_PAGE_SIZE;
        use std::thread;

        let store = InMemoryObjectStore::new();
        let remote: Arc<dyn ObjectStorage> =
            Arc::new(S3Backend::mock("cluster", Arc::clone(&store) as Arc<dyn ObjectStorage>));

        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root_w = std::env::temp_dir().join(format!("takyonic-eng-writer-{nanos}"));
        let writer = Arc::new(
            TakyonicEngine::open_with_object_storage(
                Config::default()
                    .data_dir(root_w.join("data"))
                    .wal_dir(root_w.join("wal"))
                    .memtable_size_bytes(64 * 1024 * 1024)
                    .bpm_pool_size(32)
                    .bpm_page_size(DEFAULT_PAGE_SIZE)
                    .l0_rapid_pool_threads(1)
                    .ln_haul_pool_threads(1)
                    .compaction_write_bytes_per_sec(1024 * 1024 * 1024),
                Arc::clone(&remote),
            )
            .unwrap(),
        );
        let bpm = Arc::clone(writer.buffer_pool().unwrap());
        let mut ids = Vec::new();
        for i in 0..8u8 {
            let g = bpm.new_page().unwrap();
            ids.push(g.page_id());
            g.write(|data| {
                data[0] = i;
                data[1] = 0xFF - i;
            });
        }
        bpm.flush_all().unwrap();
        let expected_ver = writer.publish_storage_manifest().unwrap().unwrap().version;
        // Avoid close() bumping the shared manifest before readers attach.
        writer.abandon_for_crash_test().unwrap();

        let mut handles = Vec::new();
        for n in 0..3 {
            let remote = Arc::clone(&remote);
            let ids = ids.clone();
            let expected_ver = expected_ver;
            handles.push(thread::spawn(move || {
                let root = std::env::temp_dir()
                    .join(format!("takyonic-eng-reader-{nanos}-{n}"));
                let eng = Arc::new(
                    TakyonicEngine::open_with_object_storage(
                        Config::default()
                            .data_dir(root.join("data"))
                            .wal_dir(root.join("wal"))
                            .memtable_size_bytes(64 * 1024 * 1024)
                            .bpm_pool_size(32)
                            .bpm_page_size(DEFAULT_PAGE_SIZE)
                            .l0_rapid_pool_threads(1)
                            .ln_haul_pool_threads(1)
                            .compaction_write_bytes_per_sec(1024 * 1024 * 1024),
                        remote,
                    )
                    .unwrap(),
                );
                let m = eng.manifest().unwrap().current();
                assert_eq!(m.version, expected_ver);
                m.verify().unwrap();
                let bpm = Arc::clone(eng.buffer_pool().unwrap());
                for (i, &id) in ids.iter().enumerate() {
                    let g = bpm.fetch_page(id).unwrap();
                    g.read(|data| {
                        assert_eq!(data[0], i as u8);
                        assert_eq!(data[1], 0xFF - i as u8);
                    });
                }
                // Readers must not publish (would race the shared version).
                eng.abandon_for_crash_test().unwrap();
                let _ = fs::remove_dir_all(root);
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        let _ = fs::remove_dir_all(root_w);
    }
}
