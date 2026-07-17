//! Lightweight, lock-free engine telemetry.
//!
//! Designed for the hot write path: recording a latency sample is one atomic
//! `fetch_add` into a log-scale bucket, with no locks and no allocation. A
//! monitoring thread takes cheap [`HistogramSnapshot`]s and diffs consecutive
//! snapshots to compute per-interval percentiles.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

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
        }
    }

    /// Record one latency sample. Lock-free; safe on the hot path.
    pub fn record(&self, latency: Duration) {
        let micros = u64::try_from(latency.as_micros()).unwrap_or(u64::MAX);
        self.buckets[bucket_index(micros)].fetch_add(1, Ordering::Relaxed);
    }

    /// Take a point-in-time copy of all bucket counts.
    pub fn snapshot(&self) -> HistogramSnapshot {
        let counts = self
            .buckets
            .iter()
            .map(|b| b.load(Ordering::Relaxed))
            .collect();
        HistogramSnapshot { counts }
    }
}

/// Immutable copy of histogram counts; supports diffing and percentiles.
#[derive(Clone, Debug)]
pub struct HistogramSnapshot {
    counts: Vec<u64>,
}

impl HistogramSnapshot {
    /// Total number of recorded samples.
    pub fn count(&self) -> u64 {
        self.counts.iter().sum()
    }

    /// Samples recorded since an `earlier` snapshot of the same histogram.
    pub fn diff(&self, earlier: &HistogramSnapshot) -> HistogramSnapshot {
        let counts = self
            .counts
            .iter()
            .zip(&earlier.counts)
            .map(|(now, then)| now.saturating_sub(*then))
            .collect();
        HistogramSnapshot { counts }
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

/// Shared counters and histograms published by [`crate::TakyonicEngine`].
#[derive(Debug, Default)]
pub struct EngineMetrics {
    ops_applied: AtomicU64,
    flushes: AtomicU64,
    group_commits: AtomicU64,
    group_commit_ops: AtomicU64,
    wal_sync: LatencyHistogram,
}

impl EngineMetrics {
    /// Create zeroed metrics.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record one durable WAL `sync_data` duration (one sample per group-commit batch).
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
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
