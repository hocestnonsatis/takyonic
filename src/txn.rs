//! Snapshot-isolation transactions with optimistic concurrency control.
//!
//! A [`Transaction`] buffers writes in a local workspace and tracks every key
//! it reads. On [`Transaction::commit`], the engine validates that no key in
//! the read-set was overwritten by a commit with `CommitTs > read_ts`. On
//! success the write-set is proposed as a single Raft `TxnBatch` sharing one
//! commit timestamp.
//!
//! With [`IsolationLevel::Serializable`], an additional SSI first-cut marks
//! concurrent readers of committed write keys as doomed (write-skew abort).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use crate::engine::TakyonicEngine;
use crate::epoch::EpochManager;
use crate::error::{Result, TakyonicError};
use crate::schema::{Record, data_key, data_table_prefix, encode_sortable_int, index_key};
use crate::types::{CommitTs, Key, Value};

/// Active-transaction registry for watermark tracking ([`EpochManager`]).
pub type TxnTracker = EpochManager;

/// SQL isolation mode for a local [`Transaction`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum IsolationLevel {
    /// Snapshot Isolation + OCC (PostgreSQL `repeatable read` / `read committed`).
    #[default]
    Snapshot,
    /// Minimal SSI: SI+OCC plus rw-antidependency doom for concurrent readers.
    Serializable,
}

impl IsolationLevel {
    /// Map a normalized GUC value (`serializable`, `repeatable read`, …).
    pub fn from_guc(value: &str) -> Self {
        if value.eq_ignore_ascii_case("serializable") {
            Self::Serializable
        } else {
            Self::Snapshot
        }
    }

    /// True when SSI read tracking / doom checks are active.
    pub fn is_serializable(self) -> bool {
        matches!(self, Self::Serializable)
    }
}

/// One buffered write in a transaction workspace (`None` = delete).
#[derive(Clone, Debug)]
pub enum WriteOp {
    /// Put `value` at commit time.
    Put(Value),
    /// Versioned tombstone delete.
    Delete,
}

/// Stats mutation deferred until a successful commit.
#[derive(Clone, Debug)]
pub enum StatsEdit {
    /// Inserted a row into `table` with indexed (index_name, value) pairs.
    Insert {
        /// Table name.
        table: String,
        /// Index column values written.
        index_values: Vec<(String, String)>,
    },
    /// Deleted a row from `table`.
    Delete {
        /// Table name.
        table: String,
        /// Index column values removed.
        index_values: Vec<(String, String)>,
    },
    /// Upsert a vector into an HNSW index (applied after OCC commit).
    VectorUpsert {
        /// Vector index name.
        index: String,
        /// Primary key.
        pk: String,
        /// Encoded embedding text (`[0.1,0.2,…]`).
        vector_text: String,
    },
    /// Remove a vector from an HNSW index.
    VectorDelete {
        /// Vector index name.
        index: String,
        /// Primary key.
        pk: String,
    },
}

/// Snapshot-isolation transaction spawned via [`TakyonicEngine::begin`].
///
/// Owns an [`Arc`] to the engine so sessions can store an active transaction
/// without self-referential lifetimes.
pub struct Transaction {
    engine: Arc<TakyonicEngine>,
    txn_id: u64,
    read_ts: CommitTs,
    isolation: IsolationLevel,
    /// Keys observed and the commit_ts of the version seen (0 = missing).
    reads: BTreeMap<Key, CommitTs>,
    /// Local write workspace.
    writes: BTreeMap<Key, WriteOp>,
    /// Deferred catalog stats updates.
    stats_edits: Vec<StatsEdit>,
    finished: bool,
}

impl Transaction {
    #[allow(dead_code)] // Snapshot convenience; begin paths use new_with_isolation.
    pub(crate) fn new(engine: Arc<TakyonicEngine>, txn_id: u64, read_ts: CommitTs) -> Self {
        Self::new_with_isolation(engine, txn_id, read_ts, IsolationLevel::Snapshot)
    }

    pub(crate) fn new_with_isolation(
        engine: Arc<TakyonicEngine>,
        txn_id: u64,
        read_ts: CommitTs,
        isolation: IsolationLevel,
    ) -> Self {
        if isolation.is_serializable() {
            engine.ssi_register(txn_id);
        }
        Self {
            engine,
            txn_id,
            read_ts,
            isolation,
            reads: BTreeMap::new(),
            writes: BTreeMap::new(),
            stats_edits: Vec::new(),
            finished: false,
        }
    }

    /// Snapshot read timestamp acquired at `begin`.
    pub fn read_ts(&self) -> CommitTs {
        self.read_ts
    }

    /// Transaction id (for debugging).
    pub fn id(&self) -> u64 {
        self.txn_id
    }

    /// Isolation level for this transaction.
    pub fn isolation(&self) -> IsolationLevel {
        self.isolation
    }

    /// Snapshot get: workspace first, then engine at `read_ts`.
    pub fn get(&mut self, key: impl Into<Key>) -> Result<Option<Value>> {
        self.ensure_open()?;
        let key = key.into();
        if let Some(op) = self.writes.get(&key) {
            return Ok(match op {
                WriteOp::Put(v) => Some(v.clone()),
                WriteOp::Delete => None,
            });
        }
        let (value, seen_ts) = self.engine.get_at_with_ts(&key, self.read_ts)?;
        if self.isolation.is_serializable() {
            self.engine.ssi_note_read(self.txn_id, &key);
        }
        self.reads.entry(key).or_insert(seen_ts);
        Ok(value)
    }

    /// Buffer a put into the transaction workspace (not yet durable).
    pub fn put(&mut self, key: impl Into<Key>, value: impl Into<Value>) -> Result<()> {
        self.ensure_open()?;
        let key = key.into();
        // Track write-set keys in the read-set for write-write OCC (SI).
        if !self.reads.contains_key(&key) {
            let (_, seen_ts) = self.engine.get_at_with_ts(&key, self.read_ts)?;
            if self.isolation.is_serializable() {
                self.engine.ssi_note_read(self.txn_id, &key);
            }
            self.reads.insert(key.clone(), seen_ts);
        }
        self.writes.insert(key, WriteOp::Put(value.into()));
        Ok(())
    }

    /// Buffer a delete into the transaction workspace.
    pub fn delete(&mut self, key: impl Into<Key>) -> Result<()> {
        self.ensure_open()?;
        let key = key.into();
        if !self.reads.contains_key(&key) {
            let (_, seen_ts) = self.engine.get_at_with_ts(&key, self.read_ts)?;
            if self.isolation.is_serializable() {
                self.engine.ssi_note_read(self.txn_id, &key);
            }
            self.reads.insert(key.clone(), seen_ts);
        }
        self.writes.insert(key, WriteOp::Delete);
        Ok(())
    }

    /// Look up a registered table schema via the engine catalog.
    pub fn table_schema(&self, table: &str) -> Result<crate::schema::TableSchema> {
        self.engine.table_schema(table)
    }

    /// Borrow the underlying engine (catalog / metrics).
    pub fn engine(&self) -> &TakyonicEngine {
        &self.engine
    }

    /// Shared engine handle (Vacuum / DDL that needs `&Arc<Engine>`).
    pub fn engine_arc(&self) -> Arc<TakyonicEngine> {
        Arc::clone(&self.engine)
    }

    /// Run [`TakyonicEngine::vacuum_table`] for `table`.
    pub fn vacuum_table(&self, table: &str) -> Result<crate::vacuum::VacuumStats> {
        self.engine_arc().vacuum_table(table)
    }

    /// Point lookup of a structured record by primary key (`Data_<table>_<pk>`).
    ///
    /// Uses the same workspace overlay + OCC read-set tracking as [`Self::get`].
    /// Returns `Ok(None)` when the key is absent or tombstoned at this snapshot.
    pub fn get_record(&mut self, table: &str, pk: &str) -> Result<Option<Record>> {
        self.ensure_open()?;
        // Ensure the table exists in the catalog.
        let _schema = self.engine.table_schema(table)?;
        let key = data_key(table, pk);
        match self.get(key)? {
            Some(val) => Ok(Some(Record::decode(&val)?)),
            None => Ok(None),
        }
    }

    /// MVCC table scan: visible `Data_<table>_*` records at this snapshot,
    /// including the local write workspace (puts visible, deletes hidden).
    ///
    /// Every visited key is added to the OCC read-set.
    pub fn scan_table_records(&mut self, table: &str) -> Result<Vec<Record>> {
        self.ensure_open()?;
        // Ensure the table exists in the catalog before scanning.
        let _schema = self.engine.table_schema(table)?;
        let prefix = data_table_prefix(table);
        let mut keys = self.engine.scan_prefix_keys(&prefix, self.read_ts)?;

        // Overlay uncommitted workspace mutations under the same prefix.
        for (k, op) in &self.writes {
            if !k.as_bytes().starts_with(&prefix) {
                continue;
            }
            match op {
                WriteOp::Put(_) => {
                    if !keys.iter().any(|existing| existing == k) {
                        keys.push(k.clone());
                    }
                }
                WriteOp::Delete => {
                    keys.retain(|existing| existing != k);
                }
            }
        }
        keys.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));

        let mut out = Vec::with_capacity(keys.len());
        for key in keys {
            match self.get(key)? {
                Some(val) => out.push(Record::decode(&val)?),
                None => {} // tombstone / deleted in workspace
            }
        }
        Ok(out)
    }

    /// Look up records via a secondary index equality probe (`Idx_<table>_<index>_<value>_*`).
    ///
    /// Two-step: scan index keys → extract PKs → fetch full data rows. Workspace
    /// overlays (uncommitted puts/deletes) are applied to both index and data keys.
    pub fn lookup_by_index(
        &mut self,
        table: &str,
        index: &str,
        value: &str,
    ) -> Result<Vec<Record>> {
        self.ensure_open()?;
        let schema = self.engine.table_schema(table)?;
        let idx = schema
            .indexes
            .iter()
            .find(|i| i.name == index)
            .ok_or_else(|| TakyonicError::Sql(format!("unknown index `{index}` on `{table}`")))?;
        if idx.is_vector() {
            return Err(TakyonicError::Sql(format!(
                "index `{index}` is a vector index; use ORDER BY col <-> query LIMIT k"
            )));
        }
        let encoded = index_store_value(value);
        let prefix = crate::schema::index_eq_prefix(table, &idx.name, &encoded);
        let mut keys = self.engine.scan_prefix_keys(&prefix, self.read_ts)?;

        // Overlay workspace index mutations.
        for (k, op) in &self.writes {
            if !k.as_bytes().starts_with(&prefix) {
                continue;
            }
            match op {
                WriteOp::Put(_) => {
                    if !keys.iter().any(|e| e == k) {
                        keys.push(k.clone());
                    }
                }
                WriteOp::Delete => {
                    keys.retain(|e| e != k);
                }
            }
        }
        keys.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));

        let mut out = Vec::new();
        for key in keys {
            // Touch OCC read-set for the index key.
            let _ = self.get(key.clone())?;
            let Some(pk) = crate::schema::pk_from_index_key(&key, &prefix) else {
                continue;
            };
            if let Some(record) = self.get_record(table, &pk)? {
                out.push(record);
            }
        }
        Ok(out)
    }

    /// Insert/update a structured record: writes the data key and all secondary
    /// index keys in this same atomic transaction.
    pub fn put_record(&mut self, table: &str, record: Record) -> Result<()> {
        self.ensure_open()?;
        let schema = self.engine.table_schema(table)?.clone();
        let pk = record
            .get(&schema.primary_key)
            .ok_or_else(|| {
                TakyonicError::Engine(format!(
                    "record missing primary key field `{}`",
                    schema.primary_key
                ))
            })?
            .to_string();

        let dkey = data_key(table, &pk);
        // If replacing, drop old index entries first.
        if let Some(old_val) = self.get(dkey.clone())? {
            let old = Record::decode(&old_val)?;
            let mut old_idx = Vec::new();
            for idx in &schema.indexes {
                if idx.is_vector() {
                    self.stats_edits.push(StatsEdit::VectorDelete {
                        index: idx.name.clone(),
                        pk: pk.clone(),
                    });
                    continue;
                }
                if let Some(v) = old.get(&idx.column) {
                    let encoded = index_store_value(v);
                    self.delete(index_key(table, &idx.name, &encoded, &pk))?;
                    old_idx.push((idx.name.clone(), encoded));
                }
            }
            if !old_idx.is_empty() {
                self.stats_edits.push(StatsEdit::Delete {
                    table: table.to_string(),
                    index_values: old_idx,
                });
            }
        }

        self.put(dkey, record.encode())?;
        let mut new_idx = Vec::new();
        for idx in &schema.indexes {
            let v = record.get(&idx.column).ok_or_else(|| {
                TakyonicError::Engine(format!("record missing indexed field `{}`", idx.column))
            })?;
            if idx.is_vector() {
                self.stats_edits.push(StatsEdit::VectorUpsert {
                    index: idx.name.clone(),
                    pk: pk.clone(),
                    vector_text: v.to_string(),
                });
                continue;
            }
            let encoded = index_store_value(v);
            // Empty value — PK lives in the key.
            self.put(
                index_key(table, &idx.name, &encoded, &pk),
                Value::new(&b""[..]),
            )?;
            new_idx.push((idx.name.clone(), encoded));
        }
        self.stats_edits.push(StatsEdit::Insert {
            table: table.to_string(),
            index_values: new_idx,
        });
        Ok(())
    }

    /// Delete a structured record and its secondary index keys.
    pub fn delete_record(&mut self, table: &str, pk: &str) -> Result<()> {
        self.ensure_open()?;
        let schema = self.engine.table_schema(table)?.clone();
        let dkey = data_key(table, pk);
        let Some(old_val) = self.get(dkey.clone())? else {
            return Ok(());
        };
        let old = Record::decode(&old_val)?;
        let mut old_idx = Vec::new();
        for idx in &schema.indexes {
            if idx.is_vector() {
                self.stats_edits.push(StatsEdit::VectorDelete {
                    index: idx.name.clone(),
                    pk: pk.to_string(),
                });
                continue;
            }
            if let Some(v) = old.get(&idx.column) {
                let encoded = index_store_value(v);
                self.delete(index_key(table, &idx.name, &encoded, pk))?;
                old_idx.push((idx.name.clone(), encoded));
            }
        }
        self.delete(dkey)?;
        self.stats_edits.push(StatsEdit::Delete {
            table: table.to_string(),
            index_values: old_idx,
        });
        Ok(())
    }

    /// OCC-validate and commit the write-set.
    ///
    /// Returns the assigned `commit_ts` on success. On conflict returns
    /// [`TakyonicError::Conflict`] — callers should abort and retry.
    pub fn commit(mut self) -> Result<CommitTs> {
        self.ensure_open()?;
        let commit_ts = self.engine.commit_transaction(
            self.txn_id,
            self.read_ts,
            self.isolation,
            &self.reads,
            &self.writes,
            &self.stats_edits,
        )?;
        self.finished = true;
        Ok(commit_ts)
    }

    /// Snapshot of the workspace for a distributed (2PC) commit path.
    ///
    /// Marks the local [`Transaction`] finished without applying writes; the
    /// caller must run [`crate::dtxn::TransactionCoordinator`] (or abort) and
    /// then apply any [`StatsEdit`]s on success.
    pub fn into_dist_workspace(mut self) -> DistTxnWorkspace {
        let _ = self.ensure_open();
        self.finished = true;
        self.engine.end_transaction(self.txn_id);
        DistTxnWorkspace {
            read_ts: self.read_ts,
            isolation: self.isolation,
            reads: std::mem::take(&mut self.reads),
            writes: std::mem::take(&mut self.writes),
            stats_edits: std::mem::take(&mut self.stats_edits),
        }
    }

    /// Borrow the buffered write-set (for partition routing probes).
    pub fn writes(&self) -> &BTreeMap<Key, WriteOp> {
        &self.writes
    }

    /// Borrow the OCC read-set.
    pub fn reads(&self) -> &BTreeMap<Key, CommitTs> {
        &self.reads
    }

    /// Explicitly abort without committing.
    pub fn abort(mut self) {
        if !self.finished {
            self.engine.end_transaction(self.txn_id);
            self.finished = true;
        }
    }

    fn ensure_open(&self) -> Result<()> {
        if self.finished {
            return Err(TakyonicError::Engine("transaction already finished".into()));
        }
        Ok(())
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        if !self.finished {
            self.engine.end_transaction(self.txn_id);
            self.finished = true;
        }
    }
}

/// Detached transaction workspace for cross-shard 2PC.
#[derive(Clone, Debug)]
pub struct DistTxnWorkspace {
    /// Snapshot read timestamp.
    pub read_ts: CommitTs,
    /// Isolation mode (SSI doom is local-engine only for now).
    pub isolation: IsolationLevel,
    /// OCC read-set.
    pub reads: BTreeMap<Key, CommitTs>,
    /// Buffered writes.
    pub writes: BTreeMap<Key, WriteOp>,
    /// Deferred stats / vector catalog edits (apply on coordinator after commit).
    pub stats_edits: Vec<StatsEdit>,
}

/// Per-txn SSI tracking (minimal write-skew doom).
#[derive(Default)]
struct SsiTxnTrack {
    reads: BTreeSet<Key>,
    doomed: bool,
}

/// Active SERIALIZABLE transactions for rw-antidependency checks.
#[derive(Default)]
pub(crate) struct SsiRegistry {
    active: std::collections::HashMap<u64, SsiTxnTrack>,
}

impl SsiRegistry {
    pub(crate) fn register(&mut self, txn_id: u64) {
        self.active.entry(txn_id).or_default();
    }

    pub(crate) fn note_read(&mut self, txn_id: u64, key: &Key) {
        if let Some(t) = self.active.get_mut(&txn_id) {
            t.reads.insert(key.clone());
        }
    }

    pub(crate) fn is_doomed(&self, txn_id: u64) -> bool {
        self.active.get(&txn_id).is_some_and(|t| t.doomed)
    }

    /// Mark every other active SSI txn that read a key in `write_keys` as doomed.
    pub(crate) fn doom_concurrent_readers(&mut self, writer_id: u64, write_keys: &BTreeSet<Key>) {
        if write_keys.is_empty() {
            return;
        }
        for (id, track) in self.active.iter_mut() {
            if *id == writer_id {
                continue;
            }
            if write_keys.iter().any(|k| track.reads.contains(k)) {
                track.doomed = true;
            }
        }
    }

    pub(crate) fn unregister(&mut self, txn_id: u64) {
        self.active.remove(&txn_id);
    }
}

/// Keys touched by a write batch (for tests / metrics).
pub fn write_keys(writes: &BTreeMap<Key, WriteOp>) -> BTreeSet<Key> {
    writes.keys().cloned().collect()
}

pub(crate) fn index_store_value(raw: &str) -> String {
    if let Ok(n) = raw.parse::<i64>() {
        encode_sortable_int(n)
    } else {
        raw.to_string()
    }
}
