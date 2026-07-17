//! End-to-end Takyonic engine orchestration.
//!
//! This module is pure glue: it wires admission, group-commit WAL, the local
//! Raft state-machine stand-in, memtable, SST manager, and dual-pool compaction
//! into a single thread-safe facade. Primitives are not reimplemented here.
//!
//! Write path (Step 9):
//! 1. Acquire an admission token (L0 soft/hard backpressure).
//! 2. `LocalRaftNode::propose` → durable group-commit Raft log (one fsync / batch).
//! 3. Apply hook publishes into the memtable before waiters wake.
//! 4. Flush to L0 and kick the L0 Rapid pool when the memtable is full.
//!
//! Note: the memtable remains the ordered `RwLock<BTreeMap>` from Step 2 (not
//! DashMap) because flush requires key order for SST emission.

use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use parking_lot::{Mutex, RwLock};
use tracing::{debug, info, warn};

use crate::admission::{AdmissionController, AdmissionOutcome};
use crate::compaction::{CompactionEngine, SstManager, SstMeta};
use crate::config::Config;
use crate::error::{Result, TakyonicError};
use crate::memtable::Memtable;
use crate::query::Query;
use crate::raft::{BatchApplyResult, CommittedEntry, LocalRaftNode, RaftCommand};
use crate::schema::TableSchema;
use crate::snapshot::{SnapshotPayload, snapshot_sst_path};
use crate::sst::{SstRegistry, SstWriter};
use crate::stats::{StatsCatalog, TableStats};
use crate::telemetry::EngineMetrics;
use crate::txn::{StatsEdit, Transaction, TxnTracker, WriteOp};
use crate::types::{CommitTs, Entry, Key, Value};
use crate::wal::{WalReader, WalWriter, segment_path};

use std::collections::BTreeMap;

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
    /// Active snapshot transactions (watermark = min read_ts).
    txn_tracker: TxnTracker,
    /// Serializes OCC validation + commit assignment.
    txn_commit_mu: Mutex<()>,
    /// Latest committed timestamp per user key (OCC index).
    last_commit: Mutex<BTreeMap<Key, CommitTs>>,
    /// Registered table schemas for secondary-index projection.
    schemas: RwLock<BTreeMap<String, TableSchema>>,
    /// Per-table statistics for the cost-based optimizer.
    stats: StatsCatalog,
}

impl TakyonicEngine {
    /// Open (or create) an engine at the configured data/WAL directories.
    pub fn open(config: Config) -> Result<Self> {
        config.validate()?;
        fs::create_dir_all(&config.data_dir)?;
        fs::create_dir_all(&config.wal_dir)?;

        let registry = Arc::new(SstRegistry::new());
        let manager = Arc::new(SstManager::new(
            registry,
            config.data_dir.clone(),
            config.block_size_bytes,
            DEFAULT_LEVEL_COUNT,
            1,
        )?);
        let admission = Arc::new(AdmissionController::new(Arc::clone(&manager), &config)?);
        let compaction = CompactionEngine::new(Arc::clone(&manager), &config)?;

        recover_existing_ssts(&manager)?;
        let (wal, memtable, mut next_seq, wal_segment) = recover_wal(&config.wal_dir)?;
        // WAL segments are pruned after flush/close, so replay alone can
        // under-estimate the sequence domain. The durable SEQNO marker keeps
        // seq monotonic across restarts (newest-wins depends on it).
        next_seq = next_seq.max(read_seq_marker(&config.wal_dir));
        let config_wal_dir = config.wal_dir.clone();
        let metrics = Arc::new(EngineMetrics::new());
        let raft = Arc::new(LocalRaftNode::new(
            wal,
            memtable,
            next_seq,
            Arc::clone(&metrics),
        ));

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
            txn_tracker: TxnTracker::new(),
            txn_commit_mu: Mutex::new(()),
            last_commit: Mutex::new(BTreeMap::new()),
            schemas: RwLock::new(BTreeMap::new()),
            stats: StatsCatalog::new(),
        };

        engine.maybe_flush()?;
        engine.kick_compaction();
        info!(
            data_dir = %engine.config.data_dir.display(),
            wal_dir = %engine.wal_dir.display(),
            "TakyonicEngine opened (group-commit + local Raft)"
        );
        Ok(engine)
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
    pub fn begin(&self) -> Result<Transaction<'_>> {
        self.ensure_open()?;
        let read_ts = self
            .raft_node()?
            .last_applied()
            .min(u64::MAX.saturating_sub(1));
        let txn_id = self.txn_tracker.begin(read_ts);
        self.publish_watermark();
        Ok(Transaction::new(self, txn_id, read_ts))
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
        self.txn_tracker.end(txn_id);
        self.publish_watermark();
    }

    pub(crate) fn commit_transaction(
        &self,
        txn_id: u64,
        read_ts: CommitTs,
        reads: &BTreeMap<Key, CommitTs>,
        writes: &BTreeMap<Key, WriteOp>,
        stats_edits: &[StatsEdit],
    ) -> Result<CommitTs> {
        self.ensure_open()?;
        if writes.is_empty() {
            self.end_transaction(txn_id);
            return Ok(read_ts);
        }

        let _occ = self.txn_commit_mu.lock();

        // OCC: any read-set key committed after our snapshot ⇒ conflict.
        {
            let last = self.last_commit.lock();
            for key in reads.keys().chain(writes.keys()) {
                if let Some(&ts) = last.get(key)
                    && ts > read_ts
                {
                    drop(last);
                    self.end_transaction(txn_id);
                    return Err(TakyonicError::Conflict(format!(
                        "key {:?} committed at {ts} > read_ts {read_ts}",
                        String::from_utf8_lossy(key.as_bytes())
                    )));
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

        let node = self.raft_node()?;
        let commit_ts = node.propose(RaftCommand::txn_batch(ops))?;
        {
            let mut last = self.last_commit.lock();
            for key in writes.keys() {
                last.insert(key.clone(), commit_ts);
            }
        }
        for edit in stats_edits {
            match edit {
                StatsEdit::Insert {
                    table,
                    index_values,
                } => self.stats.on_insert(table, index_values),
                StatsEdit::Delete {
                    table,
                    index_values,
                } => self.stats.on_delete(table, index_values),
            }
        }
        self.maybe_flush_node(&node)?;
        self.end_transaction(txn_id);
        Ok(commit_ts)
    }

    /// Register a table schema (enables `put_record` / CBO queries).
    pub fn register_table(&self, schema: TableSchema) -> Result<()> {
        self.ensure_open()?;
        self.stats.register_table(&schema);
        self.schemas.write().insert(schema.name.clone(), schema);
        Ok(())
    }

    /// Borrow a registered table schema.
    pub fn table_schema(&self, table: &str) -> Result<TableSchema> {
        self.schemas
            .read()
            .get(table)
            .cloned()
            .ok_or_else(|| TakyonicError::Engine(format!("unknown table `{table}`")))
    }

    /// Snapshot of table statistics for the optimizer.
    pub fn table_stats(&self, table: &str) -> TableStats {
        self.stats.get(table)
    }

    /// Start a cost-based query against `table`.
    pub fn query(&self, table: impl Into<String>) -> Query<'_> {
        Query::new(self, table)
    }

    /// Visible user keys at `read_ts` whose bytes start with `prefix`.
    pub fn scan_prefix_keys(&self, prefix: &[u8], read_ts: CommitTs) -> Result<Vec<Key>> {
        self.ensure_open()?;
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
        let wm = self.txn_tracker.watermark().unwrap_or_else(|| {
            self.raft
                .lock()
                .as_ref()
                .map(|n| n.last_applied())
                .unwrap_or(0)
        });
        self.manager.set_mvcc_watermark(wm);
    }

    /// Flush residual memtable, stop the group-commit flusher, shut down pools.
    pub fn close(&self) -> Result<()> {
        if self.closed.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        let node = self
            .raft
            .lock()
            .take()
            .ok_or_else(|| TakyonicError::Engine("engine already closed".into()))?;
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
        let compaction = self.compaction.lock().take();
        drop(compaction);
        info!("TakyonicEngine closed");
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
        let node = self.raft_node()?;
        let result = node.apply_log(entries)?;
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
            RaftCommand::TxnBatch { ops } => ops.iter().map(|(k, _)| k.clone()).collect(),
            _ => Vec::new(),
        };
        let node = self.raft_node()?;
        let commit_ts = node.propose(command)?;
        if !keys.is_empty() {
            let mut last = self.last_commit.lock();
            for key in keys {
                last.insert(key, commit_ts);
            }
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

    /// Snapshot memtable → L0 SST, rotate WAL. Does not hold locks across fsync.
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

        let smallest = entries.first().expect("non-empty").key.clone();
        let largest = entries.last().expect("non-empty").key.clone();
        let id = self.manager.allocate_sst_id();
        let path = self
            .manager
            .data_dir()
            .join("L0")
            .join(format!("{id:020}.sst"));
        let info = SstWriter::write(id, &path, &entries, self.manager.block_size())?;
        let meta = SstMeta::from_info(0, info, smallest, largest)?;
        self.manager.add_sst(meta)?;
        self.metrics.record_flush();

        let next = self.wal_segment.fetch_add(1, Ordering::Relaxed) + 1;
        let wal_path = segment_path(&self.wal_dir, next);
        let new_wal = WalWriter::create(&wal_path)?;
        node.rotate_wal(new_wal)?;
        // Keep the previous segment: durable-but-not-yet-flushed applies may
        // still reference it until the next flush cycle. Full prune on close.
        debug!(
            sst_id = id,
            entries = entries.len(),
            "flushed memtable to L0"
        );
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

    fn ensure_open(&self) -> Result<()> {
        if self.closed.load(Ordering::Acquire) {
            return Err(TakyonicError::Engine("engine is closed".into()));
        }
        Ok(())
    }
}

impl Drop for TakyonicEngine {
    fn drop(&mut self) {
        if let Err(error) = self.close() {
            warn!(%error, "error while closing TakyonicEngine on drop");
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

#[cfg(test)]
mod tests {
    use super::*;
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
}
