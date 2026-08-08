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

    /// Number of frames in the Buffer Pool Manager (0 = disabled).
    pub bpm_pool_size: usize,
    /// BPM page size in bytes (must be a power of two; typically 4096).
    pub bpm_page_size: usize,
    /// LRU-K parameter (track last K accesses); `2` is scan-resistant.
    pub bpm_lru_k: usize,

    /// Enable the Prometheus `/metrics` HTTP scrape server.
    pub metrics_enabled: bool,
    /// Bind address for the metrics server (ignored when disabled).
    pub metrics_bind: String,
    /// Prefer MPP fragments for GROUP BY / JOIN when a multi-node cluster is attached.
    pub mpp_enabled: bool,

    /// Optional POSIX root used as Tier-2 [`crate::object_store::LocalFileBackend`].
    ///
    /// When set, [`crate::engine::TakyonicEngine::open`] attaches remote object
    /// storage, loads the shared [`crate::manifest::ManifestManager`] on startup,
    /// and routes BPM pages through the two-tier DiskManager cache.
    pub object_store_root: Option<PathBuf>,

    /// S3-compatible endpoint URL (e.g. `http://minio:9000`). When set with
    /// [`Self::s3_bucket`], the server opens via
    /// [`crate::object_store::S3Backend`] (`--features s3`).
    pub s3_endpoint: Option<String>,
    /// Target bucket for [`Self::s3_endpoint`] (created on connect if missing).
    pub s3_bucket: Option<String>,
    /// AWS / MinIO region (default `us-east-1`).
    pub s3_region: String,
    /// Static access key for MinIO / path-style S3 (optional; else default chain).
    pub s3_access_key: Option<String>,
    /// Static secret key paired with [`Self::s3_access_key`].
    pub s3_secret_key: Option<String>,

    /// Remote pages chunk size in bytes (V2 layout). Must be a multiple of
    /// [`Self::bpm_page_size`]. Default 64 MiB.
    pub object_pages_chunk_bytes: usize,

    /// Soft upper bound on a single SST file size (flush + compaction split).
    ///
    /// Default **1 GiB** so each object-store PutObject stays under the AWS
    /// 5 GiB single-object limit without multipart upload (see
    /// [`crate::object_store::assert_put_object_size`]).
    pub max_sst_bytes: u64,
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
            bpm_pool_size: 1024,
            bpm_page_size: 4 * 1024,
            bpm_lru_k: 2,
            metrics_enabled: false,
            metrics_bind: "127.0.0.1:9090".into(),
            mpp_enabled: false,
            object_store_root: None,
            s3_endpoint: None,
            s3_bucket: None,
            s3_region: "us-east-1".into(),
            s3_access_key: None,
            s3_secret_key: None,
            object_pages_chunk_bytes: 64 * 1024 * 1024,
            max_sst_bytes: 1024 * 1024 * 1024,
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

    /// Builder-style setter for buffer pool frame count (`0` disables BPM).
    #[inline]
    pub fn bpm_pool_size(mut self, frames: usize) -> Self {
        self.bpm_pool_size = frames;
        self
    }

    /// Builder-style setter for BPM page size.
    #[inline]
    pub fn bpm_page_size(mut self, bytes: usize) -> Self {
        self.bpm_page_size = bytes;
        self
    }

    /// Builder-style setter for LRU-K parameter.
    #[inline]
    pub fn bpm_lru_k(mut self, k: usize) -> Self {
        self.bpm_lru_k = k;
        self
    }

    /// Enable / disable the Prometheus metrics HTTP server.
    #[inline]
    pub fn metrics_enabled(mut self, enabled: bool) -> Self {
        self.metrics_enabled = enabled;
        self
    }

    /// Bind address for `/metrics` (e.g. `127.0.0.1:9090` or `127.0.0.1:0`).
    #[inline]
    pub fn metrics_bind(mut self, addr: impl Into<String>) -> Self {
        self.metrics_bind = addr.into();
        self
    }

    /// Enable MPP distributed query planning when a cluster is attached.
    #[inline]
    pub fn mpp_enabled(mut self, enabled: bool) -> Self {
        self.mpp_enabled = enabled;
        self
    }

    /// Attach a local directory as Tier-2 object storage (storage–compute decoupling).
    #[inline]
    pub fn object_store_root(mut self, path: impl Into<PathBuf>) -> Self {
        self.object_store_root = Some(path.into());
        self
    }

    /// S3-compatible endpoint (MinIO / AWS). Requires `--features s3` at runtime.
    #[inline]
    pub fn s3_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.s3_endpoint = Some(endpoint.into());
        self
    }

    /// S3 bucket name used with [`Self::s3_endpoint`].
    #[inline]
    pub fn s3_bucket(mut self, bucket: impl Into<String>) -> Self {
        self.s3_bucket = Some(bucket.into());
        self
    }

    /// S3 / MinIO region string.
    #[inline]
    pub fn s3_region(mut self, region: impl Into<String>) -> Self {
        self.s3_region = region.into();
        self
    }

    /// Static access key for MinIO-style endpoints.
    #[inline]
    pub fn s3_access_key(mut self, key: impl Into<String>) -> Self {
        self.s3_access_key = Some(key.into());
        self
    }

    /// Static secret key for MinIO-style endpoints.
    #[inline]
    pub fn s3_secret_key(mut self, key: impl Into<String>) -> Self {
        self.s3_secret_key = Some(key.into());
        self
    }

    /// True when S3 endpoint + bucket are both configured.
    #[inline]
    pub fn s3_configured(&self) -> bool {
        self.s3_endpoint.is_some() && self.s3_bucket.is_some()
    }

    /// Remote pages V2 chunk size (must be a multiple of [`Self::bpm_page_size`]).
    #[inline]
    pub fn object_pages_chunk_bytes(mut self, bytes: usize) -> Self {
        self.object_pages_chunk_bytes = bytes;
        self
    }

    /// Maximum SST file size before flush/compaction split (bytes).
    #[inline]
    pub fn max_sst_bytes(mut self, bytes: u64) -> Self {
        self.max_sst_bytes = bytes;
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
        if self.bpm_pool_size > 0 {
            if self.bpm_page_size == 0 || !self.bpm_page_size.is_power_of_two() {
                return Err(TakyonicError::Config(
                    "bpm_page_size must be a non-zero power of two".into(),
                ));
            }
            if self.bpm_lru_k == 0 {
                return Err(TakyonicError::Config("bpm_lru_k must be > 0".into()));
            }
        }
        if self.object_pages_chunk_bytes == 0
            || (self.bpm_page_size > 0
                && self.object_pages_chunk_bytes % self.bpm_page_size != 0)
        {
            return Err(TakyonicError::Config(
                "object_pages_chunk_bytes must be a non-zero multiple of bpm_page_size".into(),
            ));
        }
        if self.max_sst_bytes == 0 {
            return Err(TakyonicError::Config("max_sst_bytes must be > 0".into()));
        }
        if self.max_sst_bytes >= crate::object_store::AWS_S3_PUT_OBJECT_MAX_BYTES {
            return Err(TakyonicError::Config(format!(
                "max_sst_bytes ({}) must be < AWS PutObject limit ({}); \
                 multipart upload is not implemented",
                self.max_sst_bytes,
                crate::object_store::AWS_S3_PUT_OBJECT_MAX_BYTES
            )));
        }
        if self.object_pages_chunk_bytes as u64
            >= crate::object_store::AWS_S3_PUT_OBJECT_MAX_BYTES
        {
            return Err(TakyonicError::Config(format!(
                "object_pages_chunk_bytes ({}) must be < AWS PutObject limit ({})",
                self.object_pages_chunk_bytes,
                crate::object_store::AWS_S3_PUT_OBJECT_MAX_BYTES
            )));
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

    #[test]
    fn s3_configured_requires_endpoint_and_bucket() {
        let partial = Config::default().s3_endpoint("http://127.0.0.1:9000");
        assert!(!partial.s3_configured());
        let full = Config::default()
            .s3_endpoint("http://minio:9000")
            .s3_bucket("takyonic")
            .s3_access_key("minioadmin")
            .s3_secret_key("minioadmin");
        assert!(full.s3_configured());
        assert_eq!(full.s3_region, "us-east-1");
    }
}
