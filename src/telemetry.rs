//! Lightweight, lock-free engine telemetry + Prometheus export.
//!
//! Hot-path recording is one or more atomic `fetch_add`s (no locks, no alloc).
//! [`MetricsManager`] optionally serves `/metrics` on a dedicated OS thread so
//! scraping never blocks the LSM / PgWire / Raft loops.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use tracing::{debug, warn};

use crate::bpm::BufferPoolManager;

/// Sub-bucket resolution bits per power-of-two octave (8 sub-buckets ≈ 12.5%
/// relative error, plenty for p99 fsync telemetry).
const SUB_BITS: u64 = 3;
const SUB_BUCKETS: u64 = 1 << SUB_BITS;
/// 256 buckets cover ~1µs through >2 hours; the last bucket clamps overflow.
const NUM_BUCKETS: usize = 256;

fn bucket_index(micros: u64) -> usize {
    let v = micros.max(1);
    let msb = 63 - u64::from(v.leading_zeros());
    let idx = if msb < SUB_BITS {
        v
    } else {
        let shift = msb - SUB_BITS;
        ((msb - SUB_BITS + 1) << SUB_BITS) + ((v >> shift) & (SUB_BUCKETS - 1))
    };
    (idx as usize).min(NUM_BUCKETS - 1)
}

/// Inclusive upper bound (µs) of a bucket, used when reporting percentiles.
fn bucket_upper_micros(index: usize) -> u64 {
    let idx = index as u64;
    if idx < SUB_BUCKETS {
        return idx;
    }
    let group = idx >> SUB_BITS;
    let sub = idx & (SUB_BUCKETS - 1);
    let msb = group + SUB_BITS - 1;
    let sub_width = 1u64 << (msb - SUB_BITS);
    (1u64 << msb) + sub * sub_width + (sub_width - 1)
}

/// Concurrent log-scale latency histogram (microsecond resolution).
#[derive(Debug)]
pub struct LatencyHistogram {
    buckets: Box<[AtomicU64; NUM_BUCKETS]>,
    /// Approximate sum of sample durations (µs) for Prometheus `_sum`.
    sum_micros: AtomicU64,
    count: AtomicU64,
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self::new()
    }
}

impl LatencyHistogram {
    /// Create an empty histogram.
    pub fn new() -> Self {
        Self {
            buckets: Box::new([const { AtomicU64::new(0) }; NUM_BUCKETS]),
            sum_micros: AtomicU64::new(0),
            count: AtomicU64::new(0),
        }
    }

    /// Record one latency sample. Lock-free; safe on the hot path.
    pub fn record(&self, latency: Duration) {
        let micros = u64::try_from(latency.as_micros()).unwrap_or(u64::MAX);
        self.buckets[bucket_index(micros)].fetch_add(1, Ordering::Relaxed);
        self.sum_micros.fetch_add(micros, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    /// Take a point-in-time copy of all bucket counts.
    pub fn snapshot(&self) -> HistogramSnapshot {
        let counts = self
            .buckets
            .iter()
            .map(|b| b.load(Ordering::Relaxed))
            .collect();
        HistogramSnapshot {
            counts,
            sum_micros: self.sum_micros.load(Ordering::Relaxed),
            count: self.count.load(Ordering::Relaxed),
        }
    }

    fn prometheus_pair(&self, name: &str, help: &str, out: &mut String) {
        let count = self.count.load(Ordering::Relaxed);
        let sum = self.sum_micros.load(Ordering::Relaxed) as f64 / 1_000_000.0;
        out.push_str(&format!("# HELP {name}_seconds {help}\n"));
        out.push_str(&format!("# TYPE {name}_seconds summary\n"));
        out.push_str(&format!("{name}_seconds_sum {sum}\n"));
        out.push_str(&format!("{name}_seconds_count {count}\n"));
    }
}

/// Immutable copy of histogram counts; supports diffing and percentiles.
#[derive(Clone, Debug)]
pub struct HistogramSnapshot {
    counts: Vec<u64>,
    sum_micros: u64,
    count: u64,
}

impl HistogramSnapshot {
    /// Total number of recorded samples.
    pub fn count(&self) -> u64 {
        if self.count > 0 {
            self.count
        } else {
            self.counts.iter().sum()
        }
    }

    /// Samples recorded since an `earlier` snapshot of the same histogram.
    pub fn diff(&self, earlier: &HistogramSnapshot) -> HistogramSnapshot {
        let counts = self
            .counts
            .iter()
            .zip(&earlier.counts)
            .map(|(now, then)| now.saturating_sub(*then))
            .collect();
        HistogramSnapshot {
            counts,
            sum_micros: self.sum_micros.saturating_sub(earlier.sum_micros),
            count: self.count.saturating_sub(earlier.count),
        }
    }

    /// Approximate percentile in microseconds (`q` in `0.0..=1.0`).
    ///
    /// Returns `None` when the snapshot holds no samples.
    pub fn percentile_micros(&self, q: f64) -> Option<u64> {
        let total = self.count();
        if total == 0 {
            return None;
        }
        let target = ((q.clamp(0.0, 1.0) * total as f64).ceil() as u64).max(1);
        let mut seen = 0u64;
        for (idx, &c) in self.counts.iter().enumerate() {
            seen += c;
            if seen >= target {
                return Some(bucket_upper_micros(idx));
            }
        }
        Some(bucket_upper_micros(NUM_BUCKETS - 1))
    }

    /// Upper bound (µs) of the highest non-empty bucket.
    pub fn max_micros(&self) -> Option<u64> {
        self.counts
            .iter()
            .rposition(|&c| c > 0)
            .map(bucket_upper_micros)
    }
}

/// Global lock-free engine performance metrics (`Metrics` in the roadmap).
#[derive(Debug, Default)]
pub struct EngineMetrics {
    // —— legacy / WAL ——
    ops_applied: AtomicU64,
    flushes: AtomicU64,
    group_commits: AtomicU64,
    group_commit_ops: AtomicU64,
    wal_sync: LatencyHistogram,

    // —— Buffer pool ——
    bpm_hits: AtomicU64,
    bpm_misses: AtomicU64,
    bpm_evictions: AtomicU64,
    bpm_flushes: AtomicU64,
    bpm_flush: LatencyHistogram,

    // —— JIT ——
    jit_compilations: AtomicU64,
    jit_compile: LatencyHistogram,
    jit_executions: AtomicU64,
    jit_interpreter_fallbacks: AtomicU64,

    // —— Raft ——
    raft_heartbeats: AtomicU64,
    raft_elections: AtomicU64,
    raft_election: LatencyHistogram,
    raft_append: LatencyHistogram,

    // —— Transactions / VACUUM ——
    txn_commits: AtomicU64,
    txn_commit: LatencyHistogram,
    txn_active: AtomicU64,
    vacuum_cycles: AtomicU64,
    vacuum: LatencyHistogram,

    // —— MPP ——
    mpp_shuffle_sent: AtomicU64,
    mpp_shuffle_recv: AtomicU64,
    mpp_shuffle_backpressure: AtomicU64,
    mpp_fragments: AtomicU64,

    // —— Distributed 2PC ——
    dtxn_prepared: AtomicU64,
    dtxn_committed: AtomicU64,
    dtxn_aborted: AtomicU64,
}

impl EngineMetrics {
    /// Create zeroed metrics.
    pub fn new() -> Self {
        Self::default()
    }

    // —— WAL / legacy ——

    /// Record one durable WAL `sync_data` duration.
    pub fn record_wal_sync(&self, latency: Duration) {
        self.wal_sync.record(latency);
    }

    /// Record one group-commit flush covering `batch_ops` entries.
    pub fn record_group_commit(&self, batch_ops: u64) {
        self.group_commits.fetch_add(1, Ordering::Relaxed);
        self.group_commit_ops
            .fetch_add(batch_ops, Ordering::Relaxed);
    }

    /// Count one applied write (put or delete).
    pub fn record_op(&self) {
        self.ops_applied.fetch_add(1, Ordering::Relaxed);
    }

    /// Count one memtable → L0 flush.
    pub fn record_flush(&self) {
        self.flushes.fetch_add(1, Ordering::Relaxed);
    }

    /// Total writes applied so far.
    pub fn ops_applied(&self) -> u64 {
        self.ops_applied.load(Ordering::Relaxed)
    }

    /// Total memtable flushes so far.
    pub fn flushes(&self) -> u64 {
        self.flushes.load(Ordering::Relaxed)
    }

    /// Total group-commit fsync batches.
    pub fn group_commits(&self) -> u64 {
        self.group_commits.load(Ordering::Relaxed)
    }

    /// Total ops covered by group-commit batches.
    pub fn group_commit_ops(&self) -> u64 {
        self.group_commit_ops.load(Ordering::Relaxed)
    }

    /// Average ops per group-commit batch.
    pub fn avg_group_batch_size(&self) -> f64 {
        let batches = self.group_commits() as f64;
        if batches == 0.0 {
            0.0
        } else {
            self.group_commit_ops() as f64 / batches
        }
    }

    /// Snapshot of the WAL `append_sync` latency histogram.
    pub fn wal_sync_snapshot(&self) -> HistogramSnapshot {
        self.wal_sync.snapshot()
    }

    // —— Buffer pool ——

    /// Buffer pool cache hit.
    pub fn record_bpm_hit(&self) {
        self.bpm_hits.fetch_add(1, Ordering::Relaxed);
    }

    /// Buffer pool cache miss (disk read).
    pub fn record_bpm_miss(&self) {
        self.bpm_misses.fetch_add(1, Ordering::Relaxed);
    }

    /// Frame eviction.
    pub fn record_bpm_eviction(&self) {
        self.bpm_evictions.fetch_add(1, Ordering::Relaxed);
    }

    /// Dirty page flush with duration.
    pub fn record_bpm_flush(&self, latency: Duration) {
        self.bpm_flushes.fetch_add(1, Ordering::Relaxed);
        self.bpm_flush.record(latency);
    }

    /// BPM hit ratio in `[0, 1]` (0 when no accesses).
    pub fn bpm_hit_ratio(&self) -> f64 {
        let h = self.bpm_hits.load(Ordering::Relaxed) as f64;
        let m = self.bpm_misses.load(Ordering::Relaxed) as f64;
        let t = h + m;
        if t == 0.0 {
            0.0
        } else {
            h / t
        }
    }

    /// Buffer pool cache hits.
    pub fn bpm_hits(&self) -> u64 {
        self.bpm_hits.load(Ordering::Relaxed)
    }
    /// Buffer pool cache misses.
    pub fn bpm_misses(&self) -> u64 {
        self.bpm_misses.load(Ordering::Relaxed)
    }
    /// Buffer pool frame evictions.
    pub fn bpm_evictions(&self) -> u64 {
        self.bpm_evictions.load(Ordering::Relaxed)
    }

    // —— JIT ——

    /// Successful Cranelift compilation of one expression.
    pub fn record_jit_compile(&self, latency: Duration) {
        self.jit_compilations.fetch_add(1, Ordering::Relaxed);
        self.jit_compile.record(latency);
    }

    /// One row (or push-pipeline invocation) evaluated via compiled code.
    pub fn record_jit_execution(&self) {
        self.jit_executions.fetch_add(1, Ordering::Relaxed);
    }

    /// Expression fell back to the interpreter.
    pub fn record_jit_interpreter_fallback(&self) {
        self.jit_interpreter_fallbacks.fetch_add(1, Ordering::Relaxed);
    }

    /// Total successful JIT compilations.
    pub fn jit_compilations(&self) -> u64 {
        self.jit_compilations.load(Ordering::Relaxed)
    }
    /// Total JIT native evaluations.
    pub fn jit_executions(&self) -> u64 {
        self.jit_executions.load(Ordering::Relaxed)
    }
    /// Total interpreter fallbacks (compile or evaluate).
    pub fn jit_interpreter_fallbacks(&self) -> u64 {
        self.jit_interpreter_fallbacks.load(Ordering::Relaxed)
    }

    // —— Raft ——

    /// Leader heartbeat / AppendEntries empty round-trip.
    pub fn record_raft_heartbeat(&self) {
        self.raft_heartbeats.fetch_add(1, Ordering::Relaxed);
    }

    /// Election started → became leader (or abandoned).
    pub fn record_raft_election(&self, latency: Duration) {
        self.raft_elections.fetch_add(1, Ordering::Relaxed);
        self.raft_election.record(latency);
    }

    /// Log replication / AppendEntries latency sample.
    pub fn record_raft_append(&self, latency: Duration) {
        self.raft_append.record(latency);
    }

    /// Total Raft heartbeats observed.
    pub fn raft_heartbeats(&self) -> u64 {
        self.raft_heartbeats.load(Ordering::Relaxed)
    }
    /// Total Raft elections that completed.
    pub fn raft_elections(&self) -> u64 {
        self.raft_elections.load(Ordering::Relaxed)
    }

    // —— Transactions ——

    /// Successful OCC commit with wall-clock duration.
    pub fn record_txn_commit(&self, latency: Duration) {
        self.txn_commits.fetch_add(1, Ordering::Relaxed);
        self.txn_commit.record(latency);
    }

    /// Active transaction opened.
    pub fn txn_begin(&self) {
        self.txn_active.fetch_add(1, Ordering::Relaxed);
    }

    /// Active transaction closed (commit or abort).
    pub fn txn_end(&self) {
        self.txn_active.fetch_sub(1, Ordering::Relaxed);
    }

    /// One VACUUM cycle with duration.
    pub fn record_vacuum(&self, latency: Duration) {
        self.vacuum_cycles.fetch_add(1, Ordering::Relaxed);
        self.vacuum.record(latency);
    }

    /// Total successful OCC commits.
    pub fn txn_commits(&self) -> u64 {
        self.txn_commits.load(Ordering::Relaxed)
    }
    /// Currently open MVCC transactions.
    pub fn txn_active(&self) -> u64 {
        self.txn_active.load(Ordering::Relaxed)
    }
    /// Completed VACUUM cycles.
    pub fn vacuum_cycles(&self) -> u64 {
        self.vacuum_cycles.load(Ordering::Relaxed)
    }

    // —— MPP ——

    /// Rows (or batches) pushed into a shuffle partition.
    pub fn record_mpp_shuffle_sent(&self, n: u64) {
        self.mpp_shuffle_sent.fetch_add(n, Ordering::Relaxed);
    }

    /// Rows pulled from a shuffle partition.
    pub fn record_mpp_shuffle_recv(&self, n: u64) {
        self.mpp_shuffle_recv.fetch_add(n, Ordering::Relaxed);
    }

    /// One rejected / full shuffle push (producer must retry — backpressure).
    pub fn record_mpp_shuffle_backpressure(&self) {
        self.mpp_shuffle_backpressure.fetch_add(1, Ordering::Relaxed);
    }

    /// One MPP fragment executed on this node.
    pub fn record_mpp_fragment(&self) {
        self.mpp_fragments.fetch_add(1, Ordering::Relaxed);
    }

    /// Total shuffle rows sent.
    pub fn mpp_shuffle_sent(&self) -> u64 {
        self.mpp_shuffle_sent.load(Ordering::Relaxed)
    }
    /// Total shuffle rows received.
    pub fn mpp_shuffle_recv(&self) -> u64 {
        self.mpp_shuffle_recv.load(Ordering::Relaxed)
    }
    /// Total shuffle backpressure (full-buffer) events.
    pub fn mpp_shuffle_backpressure(&self) -> u64 {
        self.mpp_shuffle_backpressure.load(Ordering::Relaxed)
    }
    /// Total MPP fragments executed.
    pub fn mpp_fragments(&self) -> u64 {
        self.mpp_fragments.load(Ordering::Relaxed)
    }

    // —— Distributed 2PC ——

    /// One shard successfully prepared a distributed txn branch.
    pub fn record_dtxn_prepared(&self) {
        self.dtxn_prepared.fetch_add(1, Ordering::Relaxed);
    }

    /// One distributed txn reached global COMMIT.
    pub fn record_dtxn_committed(&self) {
        self.dtxn_committed.fetch_add(1, Ordering::Relaxed);
    }

    /// One distributed txn was aborted (prepare failure / OCC / crash).
    pub fn record_dtxn_aborted(&self) {
        self.dtxn_aborted.fetch_add(1, Ordering::Relaxed);
    }

    /// Shard-level prepare ACKs.
    pub fn dtxn_prepared(&self) -> u64 {
        self.dtxn_prepared.load(Ordering::Relaxed)
    }
    /// Globally committed distributed transactions.
    pub fn dtxn_committed(&self) -> u64 {
        self.dtxn_committed.load(Ordering::Relaxed)
    }
    /// Globally aborted distributed transactions.
    pub fn dtxn_aborted(&self) -> u64 {
        self.dtxn_aborted.load(Ordering::Relaxed)
    }

    /// Serialize all counters into Prometheus exposition format.
    pub fn render_prometheus(&self, bpm: Option<&BufferPoolManager>) -> String {
        let mut out = String::with_capacity(4096);
        out.push_str("# Takyonic engine metrics\n");

        // Prefer live BPM stats when available (authoritative for hit ratio).
        let (hits, misses, evictions, flushes) = if let Some(bpm) = bpm {
            let s = bpm.stats();
            (s.hits, s.misses, s.evictions, s.flushes)
        } else {
            (
                self.bpm_hits(),
                self.bpm_misses(),
                self.bpm_evictions(),
                self.bpm_flushes.load(Ordering::Relaxed),
            )
        };
        let ratio = {
            let t = hits + misses;
            if t == 0 {
                0.0
            } else {
                hits as f64 / t as f64
            }
        };

        counter(
            &mut out,
            "takyonic_bpm_hits_total",
            "Buffer pool cache hits",
            hits,
        );
        counter(
            &mut out,
            "takyonic_bpm_misses_total",
            "Buffer pool cache misses",
            misses,
        );
        gauge(
            &mut out,
            "takyonic_bpm_hit_ratio",
            "Buffer pool hit ratio (0..1)",
            ratio,
        );
        counter(
            &mut out,
            "takyonic_bpm_evictions_total",
            "Buffer pool frame evictions",
            evictions,
        );
        counter(
            &mut out,
            "takyonic_bpm_flushes_total",
            "Buffer pool dirty page flushes",
            flushes,
        );
        self.bpm_flush.prometheus_pair(
            "takyonic_bpm_flush",
            "Dirty page flush latency",
            &mut out,
        );

        counter(
            &mut out,
            "takyonic_jit_compilations_total",
            "JIT expression compilations",
            self.jit_compilations(),
        );
        self.jit_compile.prometheus_pair(
            "takyonic_jit_compile",
            "JIT compilation latency",
            &mut out,
        );
        counter(
            &mut out,
            "takyonic_jit_executions_total",
            "Rows / invocations evaluated with JIT",
            self.jit_executions(),
        );
        counter(
            &mut out,
            "takyonic_jit_interpreter_fallbacks_total",
            "Expressions that fell back to the interpreter",
            self.jit_interpreter_fallbacks(),
        );

        counter(
            &mut out,
            "takyonic_raft_heartbeats_total",
            "Raft leader heartbeats sent / processed",
            self.raft_heartbeats(),
        );
        counter(
            &mut out,
            "takyonic_raft_elections_total",
            "Raft elections started",
            self.raft_elections(),
        );
        self.raft_election.prometheus_pair(
            "takyonic_raft_election",
            "Raft election duration",
            &mut out,
        );
        self.raft_append.prometheus_pair(
            "takyonic_raft_append",
            "Raft AppendEntries / log sync latency",
            &mut out,
        );

        counter(
            &mut out,
            "takyonic_txn_commits_total",
            "Successful OCC commits",
            self.txn_commits(),
        );
        self.txn_commit.prometheus_pair(
            "takyonic_txn_commit",
            "OCC commit latency",
            &mut out,
        );
        gauge(
            &mut out,
            "takyonic_txn_active",
            "Currently open MVCC transactions",
            self.txn_active() as f64,
        );
        counter(
            &mut out,
            "takyonic_vacuum_cycles_total",
            "VACUUM cycles completed",
            self.vacuum_cycles(),
        );
        self.vacuum
            .prometheus_pair("takyonic_vacuum", "VACUUM cycle duration", &mut out);

        counter(
            &mut out,
            "takyonic_mpp_shuffle_rows_sent_total",
            "MPP shuffle rows pushed",
            self.mpp_shuffle_sent(),
        );
        counter(
            &mut out,
            "takyonic_mpp_shuffle_rows_recv_total",
            "MPP shuffle rows fetched",
            self.mpp_shuffle_recv(),
        );
        counter(
            &mut out,
            "takyonic_mpp_shuffle_backpressure_total",
            "MPP shuffle full-buffer backpressure events",
            self.mpp_shuffle_backpressure(),
        );
        counter(
            &mut out,
            "takyonic_mpp_fragments_total",
            "MPP fragments executed on this node",
            self.mpp_fragments(),
        );

        counter(
            &mut out,
            "takyonic_distributed_txn_prepared_total",
            "2PC shard prepare acknowledgements",
            self.dtxn_prepared(),
        );
        counter(
            &mut out,
            "takyonic_distributed_txn_committed_total",
            "2PC globally committed transactions",
            self.dtxn_committed(),
        );
        counter(
            &mut out,
            "takyonic_distributed_txn_aborted_total",
            "2PC globally aborted transactions",
            self.dtxn_aborted(),
        );

        counter(
            &mut out,
            "takyonic_ops_applied_total",
            "Applied put/delete operations",
            self.ops_applied(),
        );
        counter(
            &mut out,
            "takyonic_memtable_flushes_total",
            "Memtable → L0 flushes",
            self.flushes(),
        );
        counter(
            &mut out,
            "takyonic_group_commits_total",
            "Group-commit WAL batches",
            self.group_commits(),
        );
        self.wal_sync
            .prometheus_pair("takyonic_wal_sync", "WAL sync_data latency", &mut out);

        out
    }
}

fn counter(out: &mut String, name: &str, help: &str, v: u64) {
    out.push_str(&format!("# HELP {name} {help}\n"));
    out.push_str(&format!("# TYPE {name} counter\n"));
    out.push_str(&format!("{name} {v}\n"));
}

fn gauge(out: &mut String, name: &str, help: &str, v: f64) {
    out.push_str(&format!("# HELP {name} {help}\n"));
    out.push_str(&format!("# TYPE {name} gauge\n"));
    out.push_str(&format!("{name} {v}\n"));
}

/// Owns shared [`EngineMetrics`] and an optional background Prometheus HTTP server.
pub struct MetricsManager {
    metrics: Arc<EngineMetrics>,
    bpm: Option<Arc<BufferPoolManager>>,
    shutdown: Arc<AtomicBool>,
    join: Option<JoinHandle<()>>,
    /// Bound address when the scrape server is running.
    pub bind_addr: Option<SocketAddr>,
}

impl MetricsManager {
    /// Metrics only (no HTTP server).
    pub fn new(metrics: Arc<EngineMetrics>) -> Self {
        Self {
            metrics,
            bpm: None,
            shutdown: Arc::new(AtomicBool::new(false)),
            join: None,
            bind_addr: None,
        }
    }

    /// Attach a buffer pool so `/metrics` can report authoritative BPM stats.
    pub fn with_bpm(mut self, bpm: Arc<BufferPoolManager>) -> Self {
        self.bpm = Some(bpm);
        self
    }

    /// Shared metrics handle.
    pub fn metrics(&self) -> &Arc<EngineMetrics> {
        &self.metrics
    }

    /// Start a non-blocking scrape server on `addr` (e.g. `127.0.0.1:9090`).
    pub fn start_http(mut self, addr: SocketAddr) -> crate::error::Result<Self> {
        let listener = TcpListener::bind(addr).map_err(|e| {
            crate::error::TakyonicError::Config(format!("metrics bind {addr}: {e}"))
        })?;
        listener.set_nonblocking(true)?;
        let bound = listener.local_addr()?;
        let metrics = Arc::clone(&self.metrics);
        let bpm = self.bpm.clone();
        let shutdown = Arc::clone(&self.shutdown);
        self.join = Some(thread::Builder::new()
            .name("takyonic-metrics".into())
            .spawn(move || metrics_http_loop(listener, metrics, bpm, shutdown))
            .map_err(|e| {
                crate::error::TakyonicError::Engine(format!("metrics thread: {e}"))
            })?);
        self.bind_addr = Some(bound);
        debug!(%bound, "Prometheus /metrics server started");
        Ok(self)
    }

    /// Render current metrics (same body as `/metrics`).
    pub fn render(&self) -> String {
        self.metrics
            .render_prometheus(self.bpm.as_deref())
    }

    /// Stop the HTTP thread (best-effort).
    pub fn shutdown(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
        if let Some(addr) = self.bind_addr {
            // Unblock accept with a dummy connect.
            let _ = TcpStream::connect_timeout(&addr, Duration::from_millis(50));
        }
        if let Some(h) = self.join.take() {
            let _ = h.join();
        }
    }
}

impl Drop for MetricsManager {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn metrics_http_loop(
    listener: TcpListener,
    metrics: Arc<EngineMetrics>,
    bpm: Option<Arc<BufferPoolManager>>,
    shutdown: Arc<AtomicBool>,
) {
    while !shutdown.load(Ordering::Relaxed) {
        match listener.accept() {
            Ok((mut stream, _)) => {
                if let Err(e) = handle_metrics_request(&mut stream, &metrics, bpm.as_deref()) {
                    debug!(error = %e, "metrics HTTP handler error");
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(25));
            }
            Err(e) => {
                if !shutdown.load(Ordering::Relaxed) {
                    warn!(error = %e, "metrics accept failed");
                }
                thread::sleep(Duration::from_millis(50));
            }
        }
    }
}

fn handle_metrics_request(
    stream: &mut TcpStream,
    metrics: &EngineMetrics,
    bpm: Option<&BufferPoolManager>,
) -> std::io::Result<()> {
    let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
    let mut buf = [0u8; 1024];
    let n = stream.read(&mut buf)?;
    let req = String::from_utf8_lossy(&buf[..n]);
    let path = req.lines().next().unwrap_or("");
    let body = if path.contains("GET /metrics") || path.contains("GET / ") {
        metrics.render_prometheus(bpm)
    } else {
        String::from("not found\n")
    };
    let status = if path.contains("/metrics") || path.contains("GET / ") {
        "200 OK"
    } else {
        "404 Not Found"
    };
    let resp = format!(
        "HTTP/1.1 {status}\r\nContent-Type: text/plain; version=0.0.4; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(resp.as_bytes())?;
    Ok(())
}

/// Time a closure and return `(result, elapsed)`.
pub fn timed<T>(f: impl FnOnce() -> T) -> (T, Duration) {
    let t0 = Instant::now();
    let v = f();
    (v, t0.elapsed())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Barrier;

    #[test]
    fn bucket_roundtrip_is_monotonic_and_bounding() {
        let mut values: Vec<u64> = (0..30u32)
            .flat_map(|exp| [1u64 << exp, (1u64 << exp) + 1, (1u64 << exp) * 3 / 2])
            .collect();
        values.sort_unstable();
        let cap = bucket_upper_micros(NUM_BUCKETS - 1);
        let mut prev_upper = 0;
        for v in values {
            let idx = bucket_index(v);
            let upper = bucket_upper_micros(idx);
            assert!(upper >= v.min(cap), "bucket upper {upper} < sample {v}");
            assert!(
                upper >= prev_upper,
                "non-monotonic: {upper} after {prev_upper} for sample {v}"
            );
            prev_upper = upper;
        }
    }

    #[test]
    fn percentiles_track_recorded_samples() {
        let h = LatencyHistogram::new();
        for _ in 0..99 {
            h.record(Duration::from_micros(100));
        }
        h.record(Duration::from_millis(50));
        let snap = h.snapshot();
        assert_eq!(snap.count(), 100);
        let p50 = snap.percentile_micros(0.50).unwrap();
        assert!((90..=120).contains(&p50), "p50 was {p50}µs");
        let p99 = snap.percentile_micros(0.99).unwrap();
        assert!(p99 <= 120, "p99 should still be in the 100µs bucket: {p99}");
        let p100 = snap.percentile_micros(1.0).unwrap();
        assert!(p100 >= 50_000, "max sample must appear at p100: {p100}");
    }

    #[test]
    fn snapshot_diff_isolates_interval() {
        let h = LatencyHistogram::new();
        h.record(Duration::from_micros(10));
        let first = h.snapshot();
        h.record(Duration::from_micros(10_000));
        let second = h.snapshot();
        let interval = second.diff(&first);
        assert_eq!(interval.count(), 1);
        assert!(interval.percentile_micros(0.5).unwrap() >= 10_000);
    }

    #[test]
    fn concurrent_atomic_increments() {
        let m = Arc::new(EngineMetrics::new());
        let barrier = Arc::new(Barrier::new(8));
        let mut handles = Vec::new();
        for _ in 0..8 {
            let m = Arc::clone(&m);
            let b = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                b.wait();
                for _ in 0..1000 {
                    m.record_bpm_hit();
                    m.record_jit_execution();
                    m.record_txn_commit(Duration::from_micros(1));
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(m.bpm_hits(), 8000);
        assert_eq!(m.jit_executions(), 8000);
        assert_eq!(m.txn_commits(), 8000);
    }

    #[test]
    fn prometheus_text_contains_core_series() {
        let m = EngineMetrics::new();
        m.record_bpm_hit();
        m.record_bpm_miss();
        m.record_jit_compile(Duration::from_micros(50));
        m.record_jit_execution();
        m.record_raft_heartbeat();
        m.record_txn_commit(Duration::from_millis(1));
        m.record_vacuum(Duration::from_millis(2));
        let text = m.render_prometheus(None);
        assert!(text.contains("takyonic_bpm_hits_total 1"));
        assert!(text.contains("takyonic_bpm_hit_ratio"));
        assert!(text.contains("takyonic_jit_compilations_total 1"));
        assert!(text.contains("takyonic_jit_executions_total 1"));
        assert!(text.contains("takyonic_raft_heartbeats_total 1"));
        assert!(text.contains("takyonic_txn_commits_total 1"));
        assert!(text.contains("takyonic_vacuum_cycles_total 1"));
    }

    #[test]
    fn metrics_http_server_serves_prometheus() {
        let metrics = Arc::new(EngineMetrics::new());
        metrics.record_bpm_hit();
        metrics.record_jit_execution();
        let mgr = MetricsManager::new(Arc::clone(&metrics))
            .start_http("127.0.0.1:0".parse().unwrap())
            .unwrap();
        let addr = mgr.bind_addr.expect("bound");
        thread::sleep(Duration::from_millis(50));
        let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(2)).unwrap();
        stream
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .unwrap();
        let mut resp = String::new();
        stream.read_to_string(&mut resp).unwrap();
        assert!(resp.contains("200 OK"), "resp={resp}");
        assert!(resp.contains("takyonic_bpm_hits_total 1"));
        assert!(resp.contains("takyonic_jit_executions_total 1"));
        drop(mgr);
    }

    #[test]
    fn metrics_overhead_under_one_percent() {
        // Model a cheap query fragment (scan a small buffer + arithmetic). Atomic
        // metric updates must stay well under 1% of that work in release builds.
        let m = EngineMetrics::new();
        let iters = 20_000u64;
        let buf: Vec<u64> = (0..512).collect();

        let work = |acc: &mut u64| {
            for &v in &buf {
                *acc = acc.wrapping_mul(v.wrapping_add(1)).wrapping_add(v);
            }
        };

        // Warmup so first-run noise (freq scaling, cold cache) does not dominate.
        let mut acc = 1u64;
        for _ in 0..3 {
            for _ in 0..iters / 10 {
                work(&mut acc);
                m.record_bpm_hit();
            }
        }
        std::hint::black_box(acc);

        // Best-of-N ratios: under load a single sample can spike past the limit.
        let mut best_overhead = f64::INFINITY;
        let mut best_pair = (Duration::ZERO, Duration::ZERO);
        for _ in 0..5 {
            let mut acc = 1u64;
            let t0 = Instant::now();
            for _ in 0..iters {
                work(&mut acc);
            }
            let baseline = t0.elapsed();
            std::hint::black_box(acc);

            let mut acc = 1u64;
            let t1 = Instant::now();
            for _ in 0..iters {
                work(&mut acc);
                m.record_bpm_hit();
            }
            let with_metrics = t1.elapsed();
            std::hint::black_box(acc);

            let overhead = (with_metrics.as_nanos() as f64 - baseline.as_nanos() as f64)
                / baseline.as_nanos().max(1) as f64;
            if overhead < best_overhead {
                best_overhead = overhead;
                best_pair = (baseline, with_metrics);
            }
        }

        // Debug builds inflate atomic cost; release must stay under 1%.
        // Debug limit is loose (scheduler noise); we still assert a ceiling.
        let limit = if cfg!(debug_assertions) { 0.50 } else { 0.01 };
        assert!(
            best_overhead < limit,
            "metrics overhead {best_overhead:.4} (limit={limit}, baseline={:?}, with={:?})",
            best_pair.0,
            best_pair.1
        );
        eprintln!(
            "metrics overhead ≈ {:.3}% (best-of-5; baseline={:?}, with={:?})",
            best_overhead * 100.0,
            best_pair.0,
            best_pair.1
        );
    }
}
