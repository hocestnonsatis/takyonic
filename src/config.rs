//! Global engine configuration.

use std::path::PathBuf;

use crate::error::{Result, TakyonicError};

/// Tunables for the Takyonic storage engine.
///
/// Fields marked as placeholders feed later roadmap steps (dual-pool compaction,
/// L0-aware token-bucket admission) but are validated up front.
#[derive(Clone, Debug)]
pub struct Config {
    /// Root directory for SST files and manifests.
    pub data_dir: PathBuf,
    /// Directory for WAL segments (kept separate so WAL fsync stays on a hot path).
    pub wal_dir: PathBuf,

    /// Soft memtable size limit in bytes before flush is scheduled.
    pub memtable_size_bytes: usize,
    /// Target SST data block size in bytes.
    pub block_size_bytes: usize,

    /// L0 file count at which write pacing begins (Step 5 token bucket).
    pub l0_soft_limit: usize,
    /// L0 file count at which writes are stalled / rejected.
    pub l0_hard_limit: usize,

    /// Worker threads in the L0 → L1 rapid compaction pool (Step 4).
    pub l0_rapid_pool_threads: usize,
    /// Worker threads in the L1 → L2+ haul compaction pool (Step 4).
    pub ln_haul_pool_threads: usize,
    /// Aggregate compaction write bandwidth cap in bytes/second.
    ///
    /// This protects WAL and Raft fsync latency from background I/O starvation.
    pub compaction_write_bytes_per_sec: u64,
    /// Bounded queue depth for each physical compaction pool.
    pub compaction_queue_depth: usize,

    /// Unconstrained token refill rate below the L0 soft limit (operations/sec).
    pub write_admission_ops_per_sec: u64,
    /// Minimum refill rate immediately below the L0 hard limit.
    pub write_admission_min_ops_per_sec: u64,
    /// Maximum accumulated write-operation burst.
    pub write_admission_burst: u64,

    /// Compact the Raft log after this many in-memory entries (0 = disabled).
    pub raft_snapshot_threshold: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./takyonic-data"),
            wal_dir: PathBuf::from("./takyonic-wal"),
            memtable_size_bytes: 64 * 1024 * 1024,
            block_size_bytes: 4 * 1024,
            l0_soft_limit: 4,
            l0_hard_limit: 12,
            l0_rapid_pool_threads: 2,
            ln_haul_pool_threads: 2,
            compaction_write_bytes_per_sec: 64 * 1024 * 1024,
            compaction_queue_depth: 64,
            write_admission_ops_per_sec: 200_000,
            write_admission_min_ops_per_sec: 10_000,
            write_admission_burst: 20_000,
            raft_snapshot_threshold: 10_000,
        }
    }
}

impl Config {
    /// Create a config with default values.
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Builder-style setter for [`Self::data_dir`].
    #[inline]
    pub fn data_dir(mut self, path: impl Into<PathBuf>) -> Self {
        self.data_dir = path.into();
        self
    }

    /// Builder-style setter for [`Self::wal_dir`].
    #[inline]
    pub fn wal_dir(mut self, path: impl Into<PathBuf>) -> Self {
        self.wal_dir = path.into();
        self
    }

    /// Builder-style setter for [`Self::memtable_size_bytes`].
    #[inline]
    pub fn memtable_size_bytes(mut self, bytes: usize) -> Self {
        self.memtable_size_bytes = bytes;
        self
    }

    /// Builder-style setter for [`Self::block_size_bytes`].
    #[inline]
    pub fn block_size_bytes(mut self, bytes: usize) -> Self {
        self.block_size_bytes = bytes;
        self
    }

    /// Builder-style setter for [`Self::l0_soft_limit`].
    #[inline]
    pub fn l0_soft_limit(mut self, limit: usize) -> Self {
        self.l0_soft_limit = limit;
        self
    }

    /// Builder-style setter for [`Self::l0_hard_limit`].
    #[inline]
    pub fn l0_hard_limit(mut self, limit: usize) -> Self {
        self.l0_hard_limit = limit;
        self
    }

    /// Builder-style setter for [`Self::l0_rapid_pool_threads`].
    #[inline]
    pub fn l0_rapid_pool_threads(mut self, n: usize) -> Self {
        self.l0_rapid_pool_threads = n;
        self
    }

    /// Builder-style setter for [`Self::ln_haul_pool_threads`].
    #[inline]
    pub fn ln_haul_pool_threads(mut self, n: usize) -> Self {
        self.ln_haul_pool_threads = n;
        self
    }

    /// Builder-style setter for compaction write bandwidth.
    #[inline]
    pub fn compaction_write_bytes_per_sec(mut self, bytes: u64) -> Self {
        self.compaction_write_bytes_per_sec = bytes;
        self
    }

    /// Builder-style setter for each compaction pool's queue depth.
    #[inline]
    pub fn compaction_queue_depth(mut self, depth: usize) -> Self {
        self.compaction_queue_depth = depth;
        self
    }

    /// Builder-style setter for the normal write admission rate.
    #[inline]
    pub fn write_admission_ops_per_sec(mut self, rate: u64) -> Self {
        self.write_admission_ops_per_sec = rate;
        self
    }

    /// Builder-style setter for the minimum soft-throttled write rate.
    #[inline]
    pub fn write_admission_min_ops_per_sec(mut self, rate: u64) -> Self {
        self.write_admission_min_ops_per_sec = rate;
        self
    }

    /// Builder-style setter for write-admission burst capacity.
    #[inline]
    pub fn write_admission_burst(mut self, burst: u64) -> Self {
        self.write_admission_burst = burst;
        self
    }

    /// Builder-style setter for Raft log snapshot / compaction threshold.
    #[inline]
    pub fn raft_snapshot_threshold(mut self, threshold: u64) -> Self {
        self.raft_snapshot_threshold = threshold;
        self
    }

    /// Validate invariants. Call before opening the engine.
    pub fn validate(&self) -> Result<()> {
        if self.data_dir.as_os_str().is_empty() {
            return Err(TakyonicError::Config("data_dir must not be empty".into()));
        }
        if self.wal_dir.as_os_str().is_empty() {
            return Err(TakyonicError::Config("wal_dir must not be empty".into()));
        }
        if self.memtable_size_bytes == 0 {
            return Err(TakyonicError::Config(
                "memtable_size_bytes must be > 0".into(),
            ));
        }
        if self.block_size_bytes == 0 {
            return Err(TakyonicError::Config("block_size_bytes must be > 0".into()));
        }
        if self.l0_soft_limit == 0 {
            return Err(TakyonicError::Config("l0_soft_limit must be > 0".into()));
        }
        if self.l0_hard_limit == 0 {
            return Err(TakyonicError::Config("l0_hard_limit must be > 0".into()));
        }
        if self.l0_soft_limit > self.l0_hard_limit {
            return Err(TakyonicError::Config(
                "l0_soft_limit must be <= l0_hard_limit".into(),
            ));
        }
        if self.l0_rapid_pool_threads == 0 {
            return Err(TakyonicError::Config(
                "l0_rapid_pool_threads must be > 0".into(),
            ));
        }
        if self.ln_haul_pool_threads == 0 {
            return Err(TakyonicError::Config(
                "ln_haul_pool_threads must be > 0".into(),
            ));
        }
        if self.compaction_write_bytes_per_sec == 0 {
            return Err(TakyonicError::Config(
                "compaction_write_bytes_per_sec must be > 0".into(),
            ));
        }
        if self.compaction_queue_depth == 0 {
            return Err(TakyonicError::Config(
                "compaction_queue_depth must be > 0".into(),
            ));
        }
        if self.write_admission_ops_per_sec == 0 {
            return Err(TakyonicError::Config(
                "write_admission_ops_per_sec must be > 0".into(),
            ));
        }
        if self.write_admission_min_ops_per_sec == 0
            || self.write_admission_min_ops_per_sec > self.write_admission_ops_per_sec
        {
            return Err(TakyonicError::Config(
                "write_admission_min_ops_per_sec must be in 1..=write_admission_ops_per_sec".into(),
            ));
        }
        if self.write_admission_burst == 0 {
            return Err(TakyonicError::Config(
                "write_admission_burst must be > 0".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_valid() {
        Config::default().validate().unwrap();
    }

    #[test]
    fn rejects_zero_memtable_size() {
        let cfg = Config::default().memtable_size_bytes(0);
        assert!(matches!(cfg.validate(), Err(TakyonicError::Config(_))));
    }

    #[test]
    fn rejects_soft_above_hard_l0() {
        let cfg = Config::default().l0_soft_limit(10).l0_hard_limit(5);
        assert!(matches!(cfg.validate(), Err(TakyonicError::Config(_))));
    }
}
