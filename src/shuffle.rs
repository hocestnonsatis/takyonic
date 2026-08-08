//! Exchange operator and shuffle framework for MPP query execution.
//!
//! [`ShuffleManager`] holds per-query partition buffers with bounded capacity
//! (network / producer backpressure). [`ExchangeExec`] is the Volcano-side
//! bridge: it drains an input stream, hash- or range-partitions rows, and
//! pushes batches into the manager (local and/or remote via gRPC).

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use crossbeam_channel::{Receiver, Sender, TryRecvError, TrySendError, bounded};
use parking_lot::Mutex;
use xxhash_rust::xxh3::xxh3_64;

use crate::error::{Result, TakyonicError};
use crate::executor::{ExecutionContext, Executor, evaluate};
use crate::schema::Record;
use crate::sql::Expression;
use crate::telemetry::EngineMetrics;

/// Default per-partition buffer capacity (row batches).
pub const DEFAULT_SHUFFLE_BUFFER: usize = 64;
/// Rows per push batch when spilling into a partition buffer.
pub const DEFAULT_BATCH_ROWS: usize = 128;

/// How rows are assigned to shuffle partitions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Distribution {
    /// `hash(key columns) % partition_count`.
    Hash {
        /// Column / expression names used as the distribution key.
        keys: Vec<String>,
    },
    /// Range buckets over a single sortable string column.
    Range {
        /// Partitioning column.
        key: String,
        /// Inclusive lower bounds per partition (length == partition_count).
        /// Partition `i` owns `[bounds[i], bounds[i+1])` (last is unbounded).
        bounds: Vec<String>,
    },
    /// Round-robin (load balancing when no key is available).
    RoundRobin,
}

impl Distribution {
    /// Map a row to a partition index in `0..partition_count`.
    pub fn partition_of(&self, row: &Record, partition_count: u32, rr_counter: &AtomicU64) -> u32 {
        let n = partition_count.max(1);
        match self {
            Distribution::Hash { keys } => {
                let mut h = 0u64;
                for k in keys {
                    let v = row.get(k).unwrap_or("");
                    h ^= xxh3_64(v.as_bytes());
                    h = h.rotate_left(13);
                }
                (h % u64::from(n)) as u32
            }
            Distribution::Range { key, bounds } => {
                let v = row.get(key).unwrap_or("");
                if bounds.is_empty() {
                    return 0;
                }
                for (i, b) in bounds.iter().enumerate().skip(1) {
                    if v < b.as_str() {
                        return (i as u32 - 1).min(n - 1);
                    }
                }
                n - 1
            }
            Distribution::RoundRobin => (rr_counter.fetch_add(1, Ordering::Relaxed) % u64::from(n))
                as u32,
        }
    }
}

/// Hash a single string into `0..partition_count` (used for virtual table shards).
pub fn hash_partition(key: &str, partition_count: u32) -> u32 {
    let n = partition_count.max(1);
    (xxh3_64(key.as_bytes()) % u64::from(n)) as u32
}

/// Identity of one shuffle stream within a query.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub struct ShuffleKey {
    /// Coordinator-assigned query id.
    pub query_id: u64,
    /// Shuffle stage id within the query.
    pub shuffle_id: u64,
}

#[derive(Debug)]
struct PartitionBuffer {
    tx: Sender<ShuffleMsg>,
    rx: Receiver<ShuffleMsg>,
    eos: bool,
}

#[derive(Debug)]
enum ShuffleMsg {
    Rows(Vec<Record>),
    Eos,
}

/// Shared shuffle buffer registry with bounded channels (backpressure).
pub struct ShuffleManager {
    partitions: Mutex<HashMap<(ShuffleKey, u32), Arc<Mutex<PartitionBuffer>>>>,
    capacity: usize,
    metrics: Option<Arc<EngineMetrics>>,
    /// Round-robin counter for [`Distribution::RoundRobin`].
    rr: AtomicU64,
}

impl Default for ShuffleManager {
    fn default() -> Self {
        Self::new(DEFAULT_SHUFFLE_BUFFER, None)
    }
}

impl ShuffleManager {
    /// Create a manager with `capacity` buffered batches per partition.
    pub fn new(capacity: usize, metrics: Option<Arc<EngineMetrics>>) -> Self {
        Self {
            partitions: Mutex::new(HashMap::new()),
            capacity: capacity.max(1),
            metrics,
            rr: AtomicU64::new(0),
        }
    }

    /// Ensure a partition buffer exists for `(key, partition)`.
    pub fn open_partition(&self, key: ShuffleKey, partition: u32) {
        let mut map = self.partitions.lock();
        map.entry((key, partition)).or_insert_with(|| {
            let (tx, rx) = bounded(self.capacity);
            Arc::new(Mutex::new(PartitionBuffer {
                tx,
                rx,
                eos: false,
            }))
        });
    }

    /// Open `partition_count` buffers for a shuffle stage.
    pub fn open_shuffle(&self, key: ShuffleKey, partition_count: u32) {
        for p in 0..partition_count.max(1) {
            self.open_partition(key, p);
        }
    }

    /// Push a batch (non-blocking). Returns `false` when the buffer is full
    /// (caller should back off / retry — network backpressure).
    pub fn try_push(
        &self,
        key: ShuffleKey,
        partition: u32,
        rows: &[Record],
        eos: bool,
    ) -> Result<bool> {
        self.open_partition(key, partition);
        let buf = {
            let map = self.partitions.lock();
            Arc::clone(map.get(&(key, partition)).expect("just opened"))
        };
        let mut guard = buf.lock();
        if !rows.is_empty() {
            match guard.tx.try_send(ShuffleMsg::Rows(rows.to_vec())) {
                Ok(()) => {
                    if let Some(m) = &self.metrics {
                        m.record_mpp_shuffle_sent(rows.len() as u64);
                    }
                }
                Err(TrySendError::Full(_)) => {
                    if let Some(m) = &self.metrics {
                        m.record_mpp_shuffle_backpressure();
                    }
                    return Ok(false);
                }
                Err(TrySendError::Disconnected(_)) => {
                    return Err(TakyonicError::Engine("shuffle partition closed".into()));
                }
            }
        }
        if eos {
            // Prefer the PartitionBuffer flag so a capacity-1 channel can still
            // close after accepting a full Rows batch (avoid Rows+Eos deadlock).
            guard.eos = true;
            let _ = guard.tx.try_send(ShuffleMsg::Eos);
        }
        Ok(true)
    }

    /// Push with retries; keeps ownership of `rows` until accepted.
    pub fn push_blocking(
        &self,
        key: ShuffleKey,
        partition: u32,
        rows: Vec<Record>,
        eos: bool,
    ) -> Result<()> {
        self.open_partition(key, partition);
        let buf = {
            let map = self.partitions.lock();
            Arc::clone(map.get(&(key, partition)).expect("just opened"))
        };
        if !rows.is_empty() {
            let n = rows.len() as u64;
            loop {
                let guard = buf.lock();
                match guard.tx.try_send(ShuffleMsg::Rows(rows.clone())) {
                    Ok(()) => {
                        if let Some(m) = &self.metrics {
                            m.record_mpp_shuffle_sent(n);
                        }
                        break;
                    }
                    Err(TrySendError::Full(_)) => {
                        if let Some(m) = &self.metrics {
                            m.record_mpp_shuffle_backpressure();
                        }
                        drop(guard);
                        std::thread::sleep(Duration::from_millis(1));
                    }
                    Err(TrySendError::Disconnected(_)) => {
                        return Err(TakyonicError::Engine("shuffle partition closed".into()));
                    }
                }
            }
        }
        if eos {
            let mut guard = buf.lock();
            guard.eos = true;
            // Best-effort Eos message; flag is authoritative for try_fetch.
            let _ = guard.tx.try_send(ShuffleMsg::Eos);
        }
        Ok(())
    }

    /// Drain available rows from a partition (non-blocking). `eos` is true when
    /// the producer has closed the stream and the buffer is empty.
    pub fn try_fetch(&self, key: ShuffleKey, partition: u32) -> Result<(Vec<Record>, bool)> {
        self.open_partition(key, partition);
        let buf = {
            let map = self.partitions.lock();
            Arc::clone(map.get(&(key, partition)).expect("just opened"))
        };
        let mut guard = buf.lock();
        let mut out = Vec::new();
        loop {
            match guard.rx.try_recv() {
                Ok(ShuffleMsg::Rows(rows)) => {
                    if let Some(m) = &self.metrics {
                        m.record_mpp_shuffle_recv(rows.len() as u64);
                    }
                    out.extend(rows);
                }
                Ok(ShuffleMsg::Eos) => {
                    guard.eos = true;
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    guard.eos = true;
                    break;
                }
            }
        }
        let eos = guard.eos && out.is_empty() && guard.rx.is_empty();
        Ok((out, eos))
    }

    /// Block until at least one batch arrives or EOS.
    pub fn fetch_blocking(&self, key: ShuffleKey, partition: u32) -> Result<(Vec<Record>, bool)> {
        loop {
            let (rows, eos) = self.try_fetch(key, partition)?;
            if !rows.is_empty() || eos {
                return Ok((rows, eos));
            }
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    /// Drop all buffers for a shuffle stage.
    pub fn close(&self, key: ShuffleKey) {
        let mut map = self.partitions.lock();
        map.retain(|(k, _), _| k != &key);
    }

    /// Round-robin counter (shared with [`ExchangeExec`]).
    pub fn rr_counter(&self) -> &AtomicU64 {
        &self.rr
    }
}

/// Volcano exchange: partitions child rows and pushes into [`ShuffleManager`].
///
/// On the consumer side, construct with `consume_only` to pull from a local
/// partition instead of producing.
pub struct ExchangeExec {
    child: Option<Box<dyn Executor>>,
    manager: Arc<ShuffleManager>,
    key: ShuffleKey,
    distribution: Distribution,
    partition_count: u32,
    /// When set, this exchange is a consumer for `local_partition`.
    consume_partition: Option<u32>,
    /// Buffered rows ready to emit (consumer mode).
    pending: VecDeque<Record>,
    consumer_eos: bool,
    /// Producer: remaining partitions that still need EOS.
    produced: bool,
    ctx: ExecutionContext,
    /// Optional expressions evaluated into distribution key columns on the row.
    key_exprs: Vec<(String, Expression)>,
}

impl ExchangeExec {
    /// Producer exchange: drain `child`, push into shuffle partitions.
    pub fn producer(
        child: Box<dyn Executor>,
        manager: Arc<ShuffleManager>,
        key: ShuffleKey,
        distribution: Distribution,
        partition_count: u32,
        ctx: ExecutionContext,
        key_exprs: Vec<(String, Expression)>,
    ) -> Self {
        manager.open_shuffle(key, partition_count);
        Self {
            child: Some(child),
            manager,
            key,
            distribution,
            partition_count: partition_count.max(1),
            consume_partition: None,
            pending: VecDeque::new(),
            consumer_eos: false,
            produced: false,
            ctx,
            key_exprs,
        }
    }

    /// Consumer exchange: pull rows for one local partition.
    pub fn consumer(
        manager: Arc<ShuffleManager>,
        key: ShuffleKey,
        partition: u32,
        partition_count: u32,
    ) -> Self {
        manager.open_shuffle(key, partition_count);
        Self {
            child: None,
            manager,
            key,
            distribution: Distribution::RoundRobin,
            partition_count: partition_count.max(1),
            consume_partition: Some(partition),
            pending: VecDeque::new(),
            consumer_eos: false,
            produced: false,
            ctx: ExecutionContext::new(),
            key_exprs: Vec::new(),
        }
    }

    fn produce_all(&mut self) -> Result<()> {
        if self.produced {
            return Ok(());
        }
        let Some(child) = self.child.as_mut() else {
            return Ok(());
        };
        let mut batches: HashMap<u32, Vec<Record>> = HashMap::new();
        for p in 0..self.partition_count {
            batches.insert(p, Vec::new());
        }
        while let Some(mut row) = child.next_row()? {
            for (name, expr) in &self.key_exprs {
                let v = evaluate(expr, &row, &self.ctx)?;
                row = row.set(name, v.to_display());
            }
            let p = self.distribution.partition_of(
                &row,
                self.partition_count,
                self.manager.rr_counter(),
            );
            batches.entry(p).or_default().push(row);
            if batches.get(&p).map(|b| b.len()).unwrap_or(0) >= DEFAULT_BATCH_ROWS {
                let batch = batches.get_mut(&p).unwrap().drain(..).collect();
                self.manager.push_blocking(self.key, p, batch, false)?;
            }
        }
        for (p, batch) in batches {
            self.manager.push_blocking(self.key, p, batch, true)?;
        }
        self.produced = true;
        Ok(())
    }
}

impl Executor for ExchangeExec {
    fn next_row(&mut self) -> Result<Option<Record>> {
        if let Some(part) = self.consume_partition {
            if let Some(row) = self.pending.pop_front() {
                return Ok(Some(row));
            }
            if self.consumer_eos {
                return Ok(None);
            }
            let (rows, eos) = self.manager.fetch_blocking(self.key, part)?;
            self.pending.extend(rows);
            self.consumer_eos = eos && self.pending.is_empty();
            return Ok(self.pending.pop_front());
        }
        // Producer mode: run the push once, then yield nothing (pipeline break).
        self.produce_all()?;
        Ok(None)
    }
}

/// Encode a list of records for gRPC transport.
pub fn encode_rows(rows: &[Record]) -> Vec<Bytes> {
    rows.iter().map(|r| r.encode().into_bytes()).collect()
}

/// Decode gRPC row payloads.
pub fn decode_rows(payloads: &[impl AsRef<[u8]>]) -> Result<Vec<Record>> {
    payloads
        .iter()
        .map(|p| {
            let v = crate::types::Value::new(Bytes::copy_from_slice(p.as_ref()));
            Record::decode(&v)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::ValuesExec;
    use crate::schema::Record;

    #[test]
    fn hash_distribution_is_stable() {
        let d = Distribution::Hash {
            keys: vec!["dept".into()],
        };
        let rr = AtomicU64::new(0);
        let r1 = Record::new().set("dept", "Eng").set("sal", "10");
        let r2 = Record::new().set("dept", "Eng").set("sal", "20");
        assert_eq!(
            d.partition_of(&r1, 4, &rr),
            d.partition_of(&r2, 4, &rr)
        );
    }

    #[test]
    fn exchange_producer_consumer_roundtrip() {
        let mgr = Arc::new(ShuffleManager::new(8, None));
        let key = ShuffleKey {
            query_id: 1,
            shuffle_id: 1,
        };
        let rows = vec![
            Record::new().set("id", "1").set("dept", "A"),
            Record::new().set("id", "2").set("dept", "B"),
            Record::new().set("id", "3").set("dept", "A"),
        ];
        let child = Box::new(ValuesExec::new(rows));
        let mut prod = ExchangeExec::producer(
            child,
            Arc::clone(&mgr),
            key,
            Distribution::Hash {
                keys: vec!["dept".into()],
            },
            2,
            ExecutionContext::new(),
            Vec::new(),
        );
        assert!(prod.next_row().unwrap().is_none());

        let mut got = Vec::new();
        for p in 0..2 {
            let mut cons = ExchangeExec::consumer(Arc::clone(&mgr), key, p, 2);
            while let Some(r) = cons.next_row().unwrap() {
                got.push(r);
            }
        }
        assert_eq!(got.len(), 3);
        mgr.close(key);
    }
}
