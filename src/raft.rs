//! Raft state-machine adapter for Takyonic.
//!
//! Client proposals may pass through [`AdmissionController`], but committed
//! Raft entries never do: once consensus commits an entry, the state machine
//! must apply it. Committed batches are appended to the dedicated WAL, synced
//! once with `sync_data`, then made visible in the memtable. Compaction runs on
//! separately paced worker pools and never shares this apply mutex or WAL file.

use std::fs::OpenOptions;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use parking_lot::Mutex;
use tracing::{debug, trace};

use crate::admission::{AdmissionController, AdmissionOutcome};
use crate::error::{Result, TakyonicError};
use crate::group_commit::GroupCommitWal;
use crate::memtable::Memtable;
use crate::types::{Entry, Key, Value};
use crate::wal::{WalReader, WalWriter};

const COMMAND_VERSION: u8 = 1;
const COMMAND_PUT: u8 = 1;
const COMMAND_DELETE: u8 = 2;
const COMMAND_ADD_NODE: u8 = 3;
const COMMAND_REMOVE_NODE: u8 = 4;
const COMMAND_NOOP: u8 = 5;
const COMMAND_TXN_BATCH: u8 = 6;

/// Replicated command stored in the Raft log.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RaftCommand {
    /// Set `key` to `value`.
    Put {
        /// User key.
        key: Key,
        /// User value.
        value: Value,
    },
    /// Delete `key` by writing a tombstone.
    Delete {
        /// User key.
        key: Key,
    },
    /// Single-server membership change: add a voting member.
    AddNode {
        /// New node id.
        id: u64,
        /// Advertised `host:port`.
        address: String,
    },
    /// Single-server membership change: remove a voting member.
    RemoveNode {
        /// Node id to evict.
        id: u64,
    },
    /// Empty leader noop (commits prior-term entries per Raft §5.4.2).
    Noop,
    /// Atomic multi-key write set sharing one commit timestamp.
    ///
    /// Each op is `(key, Some(value))` for put or `(key, None)` for delete.
    TxnBatch {
        /// Ordered write operations.
        ops: Vec<(Key, Option<Value>)>,
    },
}

impl RaftCommand {
    /// Construct a replicated put.
    pub fn put(key: impl Into<Key>, value: impl Into<Value>) -> Self {
        Self::Put {
            key: key.into(),
            value: value.into(),
        }
    }

    /// Construct a replicated delete.
    pub fn delete(key: impl Into<Key>) -> Self {
        Self::Delete { key: key.into() }
    }

    /// Construct an AddNode configuration change.
    pub fn add_node(id: u64, address: impl Into<String>) -> Self {
        Self::AddNode {
            id,
            address: address.into(),
        }
    }

    /// Construct a RemoveNode configuration change.
    pub fn remove_node(id: u64) -> Self {
        Self::RemoveNode { id }
    }

    /// Construct a leader noop.
    pub fn noop() -> Self {
        Self::Noop
    }

    /// Construct an atomic transaction write batch.
    pub fn txn_batch(ops: Vec<(Key, Option<Value>)>) -> Self {
        Self::TxnBatch { ops }
    }

    /// True for membership-change entries (not applied to the KV memtable).
    pub fn is_config_change(&self) -> bool {
        matches!(self, Self::AddNode { .. } | Self::RemoveNode { .. })
    }

    /// True when the command has no memtable effect (config or noop).
    pub fn is_meta(&self) -> bool {
        self.is_config_change() || matches!(self, Self::Noop)
    }

    /// Expand into one or more LSM entries at `commit_ts`.
    pub fn to_entries(&self, commit_ts: u64) -> Result<Vec<Entry>> {
        match self {
            Self::Put { key, value } => Ok(vec![Entry::put(key.clone(), value.clone(), commit_ts)]),
            Self::Delete { key } => Ok(vec![Entry::delete(key.clone(), commit_ts)]),
            Self::TxnBatch { ops } => {
                let mut out = Vec::with_capacity(ops.len());
                for (key, value) in ops {
                    match value {
                        Some(v) => out.push(Entry::put(key.clone(), v.clone(), commit_ts)),
                        None => out.push(Entry::delete(key.clone(), commit_ts)),
                    }
                }
                Ok(out)
            }
            Self::AddNode { .. } | Self::RemoveNode { .. } | Self::Noop => Err(
                TakyonicError::Raft("meta command cannot become LSM entries".into()),
            ),
        }
    }

    /// Encode for a zero-copy-friendly Raft/network boundary.
    pub fn encode(&self) -> Result<Bytes> {
        match self {
            Self::Put { key, value } => {
                Self::encode_kv(COMMAND_PUT, key.as_bytes(), value.as_bytes())
            }
            Self::Delete { key } => Self::encode_kv(COMMAND_DELETE, key.as_bytes(), &[]),
            Self::AddNode { id, address } => {
                let mut id_bytes = [0u8; 8];
                id_bytes.copy_from_slice(&id.to_le_bytes());
                Self::encode_kv(COMMAND_ADD_NODE, address.as_bytes(), &id_bytes)
            }
            Self::RemoveNode { id } => {
                let mut id_bytes = [0u8; 8];
                id_bytes.copy_from_slice(&id.to_le_bytes());
                Self::encode_kv(COMMAND_REMOVE_NODE, &[], &id_bytes)
            }
            Self::Noop => Self::encode_kv(COMMAND_NOOP, &[], &[]),
            Self::TxnBatch { ops } => {
                let mut body = BytesMut::new();
                body.put_u32_le(ops.len() as u32);
                for (key, value) in ops {
                    match value {
                        Some(v) => {
                            body.put_u8(1);
                            body.put_u32_le(key.as_bytes().len() as u32);
                            body.put_u32_le(v.as_bytes().len() as u32);
                            body.put_slice(key.as_bytes());
                            body.put_slice(v.as_bytes());
                        }
                        None => {
                            body.put_u8(2);
                            body.put_u32_le(key.as_bytes().len() as u32);
                            body.put_u32_le(0);
                            body.put_slice(key.as_bytes());
                        }
                    }
                }
                Self::encode_kv(COMMAND_TXN_BATCH, &[], &body)
            }
        }
    }

    fn encode_kv(opcode: u8, key: &[u8], value: &[u8]) -> Result<Bytes> {
        let key_len = u32::try_from(key.len())
            .map_err(|_| TakyonicError::Raft("Raft command key exceeds u32".into()))?;
        let value_len = u32::try_from(value.len())
            .map_err(|_| TakyonicError::Raft("Raft command value exceeds u32".into()))?;
        let mut out = BytesMut::with_capacity(10 + key.len() + value.len());
        out.put_u8(COMMAND_VERSION);
        out.put_u8(opcode);
        out.put_u32_le(key_len);
        out.put_u32_le(value_len);
        out.put_slice(key);
        out.put_slice(value);
        Ok(out.freeze())
    }

    /// Decode and strictly validate a replicated command.
    pub fn decode(encoded: Bytes) -> Result<Self> {
        if encoded.len() < 10 {
            return Err(TakyonicError::Raft("Raft command is truncated".into()));
        }
        if encoded[0] != COMMAND_VERSION {
            return Err(TakyonicError::Raft(format!(
                "unsupported Raft command version {}",
                encoded[0]
            )));
        }
        let opcode = encoded[1];
        let key_len = u32::from_le_bytes(encoded[2..6].try_into().unwrap()) as usize;
        let value_len = u32::from_le_bytes(encoded[6..10].try_into().unwrap()) as usize;
        let expected = 10usize
            .checked_add(key_len)
            .and_then(|len| len.checked_add(value_len))
            .ok_or_else(|| TakyonicError::Raft("Raft command length overflow".into()))?;
        if encoded.len() != expected {
            return Err(TakyonicError::Raft(
                "Raft command payload length mismatch".into(),
            ));
        }
        let key_bytes = encoded.slice(10..10 + key_len);
        let value = encoded.slice(10 + key_len..);
        match opcode {
            COMMAND_PUT => Ok(Self::Put {
                key: Key::new(key_bytes),
                value: Value::new(value),
            }),
            COMMAND_DELETE if value_len == 0 => Ok(Self::Delete {
                key: Key::new(key_bytes),
            }),
            COMMAND_DELETE => Err(TakyonicError::Raft(
                "Raft delete command contains a value".into(),
            )),
            COMMAND_ADD_NODE => {
                if value_len != 8 {
                    return Err(TakyonicError::Raft(
                        "AddNode command requires 8-byte node id".into(),
                    ));
                }
                let id = u64::from_le_bytes(value.as_ref().try_into().unwrap());
                let address = String::from_utf8(key_bytes.to_vec())
                    .map_err(|e| TakyonicError::Raft(format!("AddNode address utf8: {e}")))?;
                Ok(Self::AddNode { id, address })
            }
            COMMAND_REMOVE_NODE => {
                if key_len != 0 || value_len != 8 {
                    return Err(TakyonicError::Raft(
                        "RemoveNode command requires empty key and 8-byte id".into(),
                    ));
                }
                let id = u64::from_le_bytes(value.as_ref().try_into().unwrap());
                Ok(Self::RemoveNode { id })
            }
            COMMAND_NOOP if key_len == 0 && value_len == 0 => Ok(Self::Noop),
            COMMAND_NOOP => Err(TakyonicError::Raft(
                "Noop command must have empty key and value".into(),
            )),
            COMMAND_TXN_BATCH if key_len == 0 => {
                let mut body = value;
                if body.len() < 4 {
                    return Err(TakyonicError::Raft("TxnBatch truncated".into()));
                }
                let n = u32::from_le_bytes(body.split_to(4).as_ref().try_into().unwrap()) as usize;
                let mut ops = Vec::with_capacity(n);
                for _ in 0..n {
                    if body.is_empty() {
                        return Err(TakyonicError::Raft("TxnBatch op truncated".into()));
                    }
                    let kind = body.split_to(1)[0];
                    if body.len() < 8 {
                        return Err(TakyonicError::Raft("TxnBatch lengths truncated".into()));
                    }
                    let klen =
                        u32::from_le_bytes(body.split_to(4).as_ref().try_into().unwrap()) as usize;
                    let vlen =
                        u32::from_le_bytes(body.split_to(4).as_ref().try_into().unwrap()) as usize;
                    if body.len() < klen + vlen {
                        return Err(TakyonicError::Raft("TxnBatch payload truncated".into()));
                    }
                    let key = Key::new(body.split_to(klen));
                    let val_bytes = body.split_to(vlen);
                    match kind {
                        1 => ops.push((key, Some(Value::new(val_bytes)))),
                        2 if vlen == 0 => ops.push((key, None)),
                        _ => {
                            return Err(TakyonicError::Raft(format!(
                                "unknown TxnBatch op kind {kind}"
                            )));
                        }
                    }
                }
                Ok(Self::TxnBatch { ops })
            }
            COMMAND_TXN_BATCH => Err(TakyonicError::Raft(
                "TxnBatch must have empty key framing".into(),
            )),
            _ => Err(TakyonicError::Raft(format!(
                "unknown Raft command opcode {opcode}"
            ))),
        }
    }

    fn into_entry(self, index: u64) -> Result<Entry> {
        match self {
            Self::Put { key, value } => Ok(Entry::put(key, value, index)),
            Self::Delete { key } => Ok(Entry::delete(key, index)),
            Self::AddNode { .. } | Self::RemoveNode { .. } | Self::Noop | Self::TxnBatch { .. } => {
                Err(TakyonicError::Raft(
                    "command cannot become a single LSM entry; use to_entries".into(),
                ))
            }
        }
    }

    /// Convert into an LSM [`Entry`] keyed by the Raft log index.
    pub fn to_entry(&self, index: u64) -> Result<Entry> {
        self.clone().into_entry(index)
    }
}

/// One committed Raft log entry passed to the state machine.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommittedEntry {
    /// Monotonic committed Raft log index.
    pub index: u64,
    /// Replicated key-value command.
    pub command: RaftCommand,
}

impl CommittedEntry {
    /// Construct a committed entry.
    pub fn new(index: u64, command: RaftCommand) -> Self {
        Self { index, command }
    }
}

/// Result of applying one committed entry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ApplyStatus {
    /// The entry was durably appended and applied.
    Applied,
    /// Its index was already included in the local applied prefix.
    AlreadyApplied,
}

/// Result of applying a committed batch with one WAL sync.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BatchApplyResult {
    /// Number of newly appended and applied entries.
    pub applied: usize,
    /// Highest locally applied Raft index.
    pub last_applied: u64,
}

/// Point-in-time state-machine snapshot (structural wiring for Raft install).
///
/// Payload bytes use the `bytes` crate for zero-copy handoff across the
/// Raft/network boundary. Full SST-based snapshot serialization is a later step.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RaftSnapshot {
    /// Highest applied index covered by this snapshot.
    pub last_included_index: u64,
    /// Opaque snapshot blob (zero-copy `Bytes`).
    pub data: Bytes,
}

impl RaftSnapshot {
    /// Construct a snapshot.
    pub fn new(last_included_index: u64, data: impl Into<Bytes>) -> Self {
        Self {
            last_included_index,
            data: data.into(),
        }
    }
}

/// Minimal interface expected by a Raft integration layer.
pub trait RaftStateMachineApi: Send + Sync {
    /// Durably apply a contiguous committed batch (`apply_log`).
    fn apply_committed(&self, entries: &[CommittedEntry]) -> Result<BatchApplyResult>;

    /// Highest locally applied Raft index.
    fn last_applied(&self) -> u64;

    /// Capture a state-machine snapshot for Raft log truncation / catch-up.
    fn snapshot(&self) -> Result<RaftSnapshot>;

    /// Install a previously captured snapshot, advancing `last_applied`.
    fn apply_snapshot(&self, snap: &RaftSnapshot) -> Result<()>;
}

struct ApplyState {
    wal: WalWriter,
}

/// WAL-backed Takyonic Raft state machine.
pub struct RaftStateMachine {
    apply: Mutex<ApplyState>,
    memtable: Arc<Memtable>,
    admission: Option<Arc<AdmissionController>>,
    last_applied: AtomicU64,
}

impl RaftStateMachine {
    /// Recover a state machine from a WAL, repairing an incomplete final record.
    ///
    /// A missing WAL is created. A checksum mismatch is fatal; only a physically
    /// incomplete trailing record is truncated to the last validated boundary.
    pub fn recover(
        wal_path: impl Into<PathBuf>,
        admission: Option<Arc<AdmissionController>>,
    ) -> Result<Self> {
        let wal_path = wal_path.into();
        let memtable = Arc::new(Memtable::new());
        let mut last_applied = 0u64;

        let wal = if wal_path.exists() {
            let mut reader = WalReader::open(&wal_path)?;
            reader.replay(|entry| {
                last_applied = last_applied.max(entry.seq);
                memtable.apply(entry);
            })?;
            let valid_len = reader.last_valid_offset();
            let torn_tail = reader.has_torn_tail();
            drop(reader);

            if torn_tail {
                let file = OpenOptions::new().write(true).open(&wal_path)?;
                file.set_len(valid_len)?;
                file.sync_data()?;
                debug!(path = %wal_path.display(), valid_len, "repaired torn WAL tail");
            }
            WalWriter::open_append(&wal_path)?
        } else {
            WalWriter::create(&wal_path)?
        };

        Ok(Self {
            apply: Mutex::new(ApplyState { wal }),
            memtable,
            admission,
            last_applied: AtomicU64::new(last_applied),
        })
    }

    /// Admit a client proposal before it enters Raft consensus.
    ///
    /// Committed application intentionally bypasses this method.
    pub fn admit_proposal(&self, operations: u64, timeout: Duration) -> Result<AdmissionOutcome> {
        match &self.admission {
            Some(admission) => admission.acquire_timeout(operations, timeout),
            None => Ok(AdmissionOutcome::Acquired),
        }
    }

    /// Apply one committed entry.
    pub fn apply(&self, entry: CommittedEntry) -> Result<ApplyStatus> {
        if entry.index <= self.last_applied() {
            return Ok(ApplyStatus::AlreadyApplied);
        }
        let result = self.apply_committed(std::slice::from_ref(&entry))?;
        Ok(if result.applied == 0 {
            ApplyStatus::AlreadyApplied
        } else {
            ApplyStatus::Applied
        })
    }

    /// Read the latest in-memory value, excluding tombstones.
    pub fn get(&self, key: &Key) -> Option<Value> {
        self.memtable.get(key)
    }

    /// Shared active memtable for flush orchestration.
    pub fn memtable(&self) -> &Arc<Memtable> {
        &self.memtable
    }

    /// Path of the dedicated state-machine WAL.
    pub fn wal_path(&self) -> PathBuf {
        self.apply.lock().wal.path().to_path_buf()
    }
}

impl RaftStateMachineApi for RaftStateMachine {
    fn apply_committed(&self, entries: &[CommittedEntry]) -> Result<BatchApplyResult> {
        if entries
            .windows(2)
            .any(|pair| pair[0].index >= pair[1].index)
        {
            return Err(TakyonicError::Raft(
                "committed batch indices must be strictly increasing".into(),
            ));
        }

        // One mutex owns ordering and the dedicated WAL append/sync sequence.
        // No compaction lock or channel is touched on this fast path.
        let mut state = self.apply.lock();
        let current = self.last_applied.load(Ordering::Acquire);
        let mut expected = current.checked_add(1).ok_or_else(|| {
            TakyonicError::Raft("last_applied cannot advance beyond u64::MAX".into())
        })?;
        let mut pending = Vec::new();
        let mut highest = current;
        for committed in entries {
            if committed.index <= current {
                continue;
            }
            if committed.index != expected {
                return Err(TakyonicError::Raft(format!(
                    "committed index gap: expected {expected}, got {}",
                    committed.index
                )));
            }
            if !committed.command.is_meta() {
                pending.extend(committed.command.to_entries(committed.index)?);
            }
            highest = committed.index;
            expected = expected.saturating_add(1);
        }

        if highest == current {
            return Ok(BatchApplyResult {
                applied: 0,
                last_applied: current,
            });
        }

        for entry in &pending {
            state.wal.append(entry)?;
        }
        // Group commit: one fdatasync-style call for the entire committed batch.
        if !pending.is_empty() {
            state.wal.sync()?;
            for entry in pending.iter().cloned() {
                self.memtable.apply(entry);
            }
        }
        self.last_applied.store(highest, Ordering::Release);
        let applied = (highest - current) as usize;
        trace!(
            applied,
            last_applied = highest,
            "Raft batch durably applied"
        );
        Ok(BatchApplyResult {
            applied,
            last_applied: highest,
        })
    }

    fn last_applied(&self) -> u64 {
        self.last_applied.load(Ordering::Acquire)
    }

    fn snapshot(&self) -> Result<RaftSnapshot> {
        // Structural stub: encode last_applied as a tiny Bytes payload until
        // SST-backed snapshots land. Keeps the Raft boundary on `bytes`.
        let idx = self.last_applied();
        let mut buf = BytesMut::with_capacity(8);
        buf.put_u64_le(idx);
        Ok(RaftSnapshot::new(idx, buf.freeze()))
    }

    fn apply_snapshot(&self, snap: &RaftSnapshot) -> Result<()> {
        if snap.last_included_index < self.last_applied() {
            return Ok(());
        }
        self.last_applied
            .store(snap.last_included_index, Ordering::Release);
        Ok(())
    }
}

/// Single-node Raft stand-in for local group-commit benchmarking and for
/// wiring `TakyonicEngine` until a networked Raft library is integrated.
///
/// Write path:
/// 1. `propose` encodes a [`RaftCommand`] → LSM [`Entry`] (index = log seq).
/// 2. Durable append via [`GroupCommitWal`] (many writers, one `sync_data`).
/// 3. Apply hook publishes into the memtable **before** waiters wake
///    (local commit == durable on a single node).
///
/// Memtable publish never happens before the log entry is durable.
pub struct LocalRaftNode {
    group_wal: GroupCommitWal,
    memtable: Arc<Memtable>,
    next_index: AtomicU64,
    last_applied: Arc<AtomicU64>,
    metrics: Arc<crate::telemetry::EngineMetrics>,
}

impl LocalRaftNode {
    /// Construct a local node owning an already-recovered memtable and WAL.
    ///
    /// Spawns the group-commit flusher with an apply hook that publishes into
    /// `memtable` only after each batch `sync_data` succeeds.
    pub fn new(
        wal: WalWriter,
        memtable: Arc<Memtable>,
        next_index: u64,
        metrics: Arc<crate::telemetry::EngineMetrics>,
    ) -> Self {
        let last = next_index.saturating_sub(1);
        let last_applied = Arc::new(AtomicU64::new(last));
        let mt = Arc::clone(&memtable);
        let applied = Arc::clone(&last_applied);
        let metrics_hook = Arc::clone(&metrics);
        let hook: crate::group_commit::ApplyHook = Arc::new(move |entries: &[Entry]| {
            for entry in entries {
                let seq = entry.seq;
                mt.apply(entry.clone());
                applied.fetch_max(seq, Ordering::Release);
                metrics_hook.record_op();
            }
            Ok(())
        });
        let group_wal = GroupCommitWal::start(wal, Some(Arc::clone(&metrics)), Some(hook));
        Self {
            group_wal,
            memtable,
            next_index: AtomicU64::new(next_index.max(1)),
            last_applied,
            metrics,
        }
    }

    /// Propose a command: durable group-commit log, then apply via the flusher hook.
    ///
    /// Returns the assigned Raft log index. Returns only after durability **and**
    /// memtable publish for this entry's batch.
    pub fn propose(&self, command: RaftCommand) -> Result<u64> {
        let index = self.next_index.fetch_add(1, Ordering::Relaxed);
        let entries = command.to_entries(index)?;
        if entries.len() == 1 {
            self.group_wal.submit(entries.into_iter().next().unwrap())?;
        } else {
            self.group_wal.submit_batch(entries)?;
        }
        Ok(index)
    }

    /// Apply a contiguous committed batch that is **already durable** (networked
    /// Raft deliver path). Publishes into the memtable only.
    pub fn apply_log(&self, entries: &[CommittedEntry]) -> Result<BatchApplyResult> {
        let current = self.last_applied.load(Ordering::Acquire);
        let mut expected = current.saturating_add(1);
        let mut applied = 0usize;
        for committed in entries {
            if committed.index <= current {
                continue;
            }
            if committed.index != expected {
                return Err(TakyonicError::Raft(format!(
                    "apply_log gap: expected {expected}, got {}",
                    committed.index
                )));
            }
            if committed.command.is_meta() {
                self.last_applied
                    .fetch_max(committed.index, Ordering::Release);
            } else {
                for entry in committed.command.to_entries(committed.index)? {
                    self.memtable.apply(entry);
                }
                self.last_applied
                    .fetch_max(committed.index, Ordering::Release);
                self.metrics.record_op();
            }
            expected = expected.saturating_add(1);
            applied += 1;
        }
        Ok(BatchApplyResult {
            applied,
            last_applied: self.last_applied.load(Ordering::Acquire),
        })
    }

    /// Shared memtable.
    pub fn memtable(&self) -> &Arc<Memtable> {
        &self.memtable
    }

    /// Highest applied index.
    pub fn last_applied(&self) -> u64 {
        self.last_applied.load(Ordering::Acquire)
    }

    /// Set applied index after installing a Raft snapshot.
    pub fn set_last_applied(&self, index: u64) {
        self.last_applied.store(index, Ordering::Release);
        let next = index.saturating_add(1).max(1);
        self.next_index.store(next, Ordering::Relaxed);
    }

    /// Next index that will be assigned to a proposal.
    pub fn next_index(&self) -> u64 {
        self.next_index.load(Ordering::Relaxed)
    }

    /// Rotate the underlying group-commit WAL segment.
    pub fn rotate_wal(&self, new_wal: WalWriter) -> Result<()> {
        self.group_wal.rotate(new_wal)
    }

    /// Group-commit telemetry handle.
    pub fn group_wal(&self) -> &GroupCommitWal {
        &self.group_wal
    }

    /// Shut down the flusher and return the final WAL writer.
    pub fn shutdown(&self) -> Result<WalWriter> {
        self.group_wal.shutdown()
    }

    /// Capture a lightweight snapshot of the applied prefix.
    pub fn snapshot(&self) -> Result<RaftSnapshot> {
        let idx = self.last_applied();
        let mut buf = BytesMut::with_capacity(8);
        buf.put_u64_le(idx);
        Ok(RaftSnapshot::new(idx, buf.freeze()))
    }
}

impl RaftStateMachineApi for LocalRaftNode {
    fn apply_committed(&self, entries: &[CommittedEntry]) -> Result<BatchApplyResult> {
        // Networked Raft path: persist via group commit (apply hook publishes),
        // then confirm last_applied.
        if entries.is_empty() {
            return Ok(BatchApplyResult {
                applied: 0,
                last_applied: LocalRaftNode::last_applied(self),
            });
        }
        let current = LocalRaftNode::last_applied(self);
        let mut applied = 0usize;
        for committed in entries.iter().filter(|e| e.index > current) {
            if committed.command.is_meta() {
                self.last_applied
                    .fetch_max(committed.index, Ordering::Release);
            } else {
                let batch = committed.command.to_entries(committed.index)?;
                self.group_wal.submit_batch(batch)?;
            }
            applied += 1;
        }
        Ok(BatchApplyResult {
            applied,
            last_applied: LocalRaftNode::last_applied(self),
        })
    }

    fn last_applied(&self) -> u64 {
        LocalRaftNode::last_applied(self)
    }

    fn snapshot(&self) -> Result<RaftSnapshot> {
        LocalRaftNode::snapshot(self)
    }

    fn apply_snapshot(&self, snap: &RaftSnapshot) -> Result<()> {
        if snap.last_included_index < LocalRaftNode::last_applied(self) {
            return Ok(());
        }
        self.last_applied
            .store(snap.last_included_index, Ordering::Release);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_wal(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("takyonic-raft-{name}-{nanos}.wal"))
    }

    #[test]
    fn command_codec_roundtrip_and_validation() {
        let commands = [
            RaftCommand::put(&b"key"[..], &b"value"[..]),
            RaftCommand::delete(&b"key"[..]),
            RaftCommand::add_node(4, "127.0.0.1:19004"),
            RaftCommand::remove_node(4),
            RaftCommand::noop(),
        ];
        for command in commands {
            let encoded = command.encode().unwrap();
            assert_eq!(RaftCommand::decode(encoded).unwrap(), command);
        }
        assert!(RaftCommand::decode(Bytes::from_static(b"short")).is_err());
    }

    #[test]
    fn batch_apply_is_ordered_durable_and_idempotent() {
        let path = temp_wal("batch");
        let machine = RaftStateMachine::recover(&path, None).unwrap();
        let result = machine
            .apply_committed(&[
                CommittedEntry::new(1, RaftCommand::put(&b"a"[..], &b"one"[..])),
                CommittedEntry::new(2, RaftCommand::put(&b"b"[..], &b"two"[..])),
                CommittedEntry::new(3, RaftCommand::delete(&b"a"[..])),
            ])
            .unwrap();
        assert_eq!(result.applied, 3);
        assert_eq!(machine.last_applied(), 3);
        assert!(machine.get(&Key::new(&b"a"[..])).is_none());
        assert_eq!(
            machine.get(&Key::new(&b"b"[..])).unwrap().as_bytes(),
            b"two"
        );
        assert_eq!(
            machine
                .apply(CommittedEntry::new(3, RaftCommand::delete(&b"a"[..])))
                .unwrap(),
            ApplyStatus::AlreadyApplied
        );
        assert!(
            machine
                .apply(CommittedEntry::new(
                    5,
                    RaftCommand::put(&b"x"[..], &b"gap"[..])
                ))
                .is_err()
        );
        drop(machine);

        let recovered = RaftStateMachine::recover(&path, None).unwrap();
        assert_eq!(recovered.last_applied(), 3);
        assert!(recovered.get(&Key::new(&b"a"[..])).is_none());
        assert_eq!(
            recovered.get(&Key::new(&b"b"[..])).unwrap().as_bytes(),
            b"two"
        );
        drop(recovered);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn recovery_truncates_only_incomplete_tail() {
        let path = temp_wal("torn");
        let machine = RaftStateMachine::recover(&path, None).unwrap();
        machine
            .apply(CommittedEntry::new(
                1,
                RaftCommand::put(&b"k"[..], &b"v"[..]),
            ))
            .unwrap();
        drop(machine);
        let valid_len = std::fs::metadata(&path).unwrap().len();

        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        // A torn length prefix may decode to an absurd size; physical EOF
        // still identifies it as an incomplete final record.
        file.write_all(&[0xff, 0xff, 0xff, 0xff, 1, 2, 3]).unwrap();
        file.sync_all().unwrap();
        drop(file);
        assert!(std::fs::metadata(&path).unwrap().len() > valid_len);

        let recovered = RaftStateMachine::recover(&path, None).unwrap();
        assert_eq!(recovered.last_applied(), 1);
        assert_eq!(std::fs::metadata(&path).unwrap().len(), valid_len);
        assert_eq!(
            recovered.get(&Key::new(&b"k"[..])).unwrap().as_bytes(),
            b"v"
        );
        assert_eq!(
            recovered
                .admit_proposal(1, Duration::from_millis(1))
                .unwrap(),
            AdmissionOutcome::Acquired
        );
        drop(recovered);
        std::fs::remove_file(path).unwrap();
    }
}
