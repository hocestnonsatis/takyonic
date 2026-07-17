//! Snapshot-isolation transactions with optimistic concurrency control.
//!
//! A [`Transaction`] buffers writes in a local workspace and tracks every key
//! it reads. On [`Transaction::commit`], the engine validates that no key in
//! the read-set was overwritten by a commit with `CommitTs > read_ts`. On
//! success the write-set is proposed as a single Raft `TxnBatch` sharing one
//! commit timestamp.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;

use crate::engine::TakyonicEngine;
use crate::error::{Result, TakyonicError};
use crate::schema::{Record, data_key, encode_sortable_int, index_key};
use crate::types::{CommitTs, Key, Value};

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
}

/// Active-transaction registry for watermark tracking.
#[derive(Debug, Default)]
pub struct TxnTracker {
    /// txn_id → read_ts
    active: Mutex<BTreeMap<u64, CommitTs>>,
    next_id: AtomicU64,
}

impl TxnTracker {
    /// Create an empty tracker.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a new transaction at `read_ts`; returns txn id.
    pub fn begin(&self, read_ts: CommitTs) -> u64 {
        let id = self
            .next_id
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        self.active.lock().insert(id, read_ts);
        id
    }

    /// Unregister a finished/aborted transaction.
    pub fn end(&self, txn_id: u64) {
        self.active.lock().remove(&txn_id);
    }

    /// Oldest active `read_ts`, or `None` if no transactions are open.
    pub fn watermark(&self) -> Option<CommitTs> {
        self.active.lock().values().copied().min()
    }
}

/// Snapshot-isolation transaction spawned via [`TakyonicEngine::begin`].
pub struct Transaction<'a> {
    engine: &'a TakyonicEngine,
    txn_id: u64,
    read_ts: CommitTs,
    /// Keys observed and the commit_ts of the version seen (0 = missing).
    reads: BTreeMap<Key, CommitTs>,
    /// Local write workspace.
    writes: BTreeMap<Key, WriteOp>,
    /// Deferred catalog stats updates.
    stats_edits: Vec<StatsEdit>,
    finished: bool,
}

impl<'a> Transaction<'a> {
    pub(crate) fn new(engine: &'a TakyonicEngine, txn_id: u64, read_ts: CommitTs) -> Self {
        Self {
            engine,
            txn_id,
            read_ts,
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
            self.reads.insert(key.clone(), seen_ts);
        }
        self.writes.insert(key, WriteOp::Delete);
        Ok(())
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
            &self.reads,
            &self.writes,
            &self.stats_edits,
        )?;
        self.finished = true;
        Ok(commit_ts)
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

impl Drop for Transaction<'_> {
    fn drop(&mut self) {
        if !self.finished {
            self.engine.end_transaction(self.txn_id);
            self.finished = true;
        }
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
