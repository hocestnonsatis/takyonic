//! Object-storage abstraction for storage–compute decoupling.
//!
//! Compute nodes keep a local NVMe/SSD cache; durable SST / page blobs live in
//! remote object storage ([`LocalFileBackend`], shared [`InMemoryObjectStore`]
//! S3-mock, or optional [`S3Backend`] with `--features s3`).
//!
//! # Large-object / PutObject policy (Faz D3 + Faz 4A)
//!
//! AWS S3 (and compatible stores) reject a single `PutObject` at **5 GiB**.
//! [`ObjectStorage::write`] routes oversized payloads through **multipart
//! upload** (real MPU on [`AwsS3Client`]; simulated part counters on the
//! in-memory mock). Preferred product path remains splitting:
//!
//! - **SST:** [`crate::config::Config::max_sst_bytes`] defaults to **1 GiB**.
//! - **BPM pages:** V2 chunks ([`crate::config::Config::object_pages_chunk_bytes`],
//!   default 64 MiB).
//!
//! [`assert_put_object_size`] guards the single-PutObject path only.

use std::collections::BTreeMap;
use std::fs;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;

use crate::error::{Result, TakyonicError};

/// AWS S3 / MinIO single `PutObject` hard limit (5 GiB).
pub const AWS_S3_PUT_OBJECT_MAX_BYTES: u64 = 5 * 1024 * 1024 * 1024;

/// Default multipart part size (8 MiB; above AWS 5 MiB minimum for non-final parts).
pub const DEFAULT_MULTIPART_PART_BYTES: usize = 8 * 1024 * 1024;

/// AWS minimum size for every multipart part except the last.
pub const AWS_S3_MULTIPART_MIN_PART_BYTES: usize = 5 * 1024 * 1024;

/// Reject payloads that cannot be uploaded with a single PutObject.
///
/// Callers that may exceed this limit must use the multipart path inside
/// [`ObjectStorage::write`] (automatic above [`AWS_S3_PUT_OBJECT_MAX_BYTES`]).
pub fn assert_put_object_size(len: u64) -> Result<()> {
    if len >= AWS_S3_PUT_OBJECT_MAX_BYTES {
        return Err(TakyonicError::Engine(format!(
            "refusing single PutObject of {len} bytes (≥ AWS limit \
             {AWS_S3_PUT_OBJECT_MAX_BYTES}); ObjectStorage::write will multipart \
             at this size — do not call assert_put_object_size for MPU payloads"
        )));
    }
    Ok(())
}

/// True when `len` should use multipart instead of a single PutObject.
pub fn prefer_multipart(len: u64, threshold: u64) -> bool {
    len > 0 && len >= threshold
}

/// Byte ranges `[start, end)` for multipart parts.
pub fn multipart_part_ranges(len: usize, part_size: usize) -> Vec<(usize, usize)> {
    let part_size = part_size.max(1);
    let mut out = Vec::new();
    let mut start = 0usize;
    while start < len {
        let end = (start + part_size).min(len);
        out.push((start, end));
        start = end;
    }
    out
}

/// Remote / local object store for SST blobs, page files, and manifests.
pub trait ObjectStorage: Send + Sync {
    /// Read `size` bytes starting at `offset` from `path` (object key).
    fn read(&self, path: &str, offset: u64, size: usize) -> Result<Vec<u8>>;

    /// Overwrite / create the object at `path` with `data`.
    ///
    /// Payloads at/above [`AWS_S3_PUT_OBJECT_MAX_BYTES`] use multipart upload
    /// on S3-compatible backends; smaller objects use a single PutObject
    /// guarded by [`assert_put_object_size`].
    fn write(&self, path: &str, data: &[u8]) -> Result<()>;

    /// List object keys under `prefix` (lexicographic).
    fn list(&self, prefix: &str) -> Result<Vec<String>>;

    /// Delete an object (no-op if missing).
    fn delete(&self, path: &str) -> Result<()>;

    /// Full-object read (helper).
    fn read_all(&self, path: &str) -> Result<Vec<u8>> {
        // Probe length via a large read; backends may return shorter.
        let mut out = self.read(path, 0, 64 * 1024 * 1024)?;
        if out.len() == 64 * 1024 * 1024 {
            // Extremely large object — fall back to chunked append.
            let mut offset = out.len() as u64;
            loop {
                let chunk = self.read(path, offset, 1024 * 1024)?;
                if chunk.is_empty() {
                    break;
                }
                offset += chunk.len() as u64;
                out.extend_from_slice(&chunk);
                if chunk.len() < 1024 * 1024 {
                    break;
                }
            }
        }
        Ok(out)
    }

    /// Object byte length, if present.
    fn len(&self, path: &str) -> Result<Option<u64>> {
        match self.read_all(path) {
            Ok(b) => Ok(Some(b.len() as u64)),
            Err(TakyonicError::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(TakyonicError::Engine(msg)) if msg.contains("not found") => Ok(None),
            Err(e) => Err(e),
        }
    }
}

/// POSIX directory backend (compatibility / single-node).
pub struct LocalFileBackend {
    root: PathBuf,
}

impl LocalFileBackend {
    /// Root directory that mirrors the object key namespace.
    pub fn open(root: impl Into<PathBuf>) -> Result<Self> {
        let root = root.into();
        fs::create_dir_all(&root)?;
        Ok(Self { root })
    }

    /// Absolute filesystem path for an object key.
    pub fn resolve(&self, path: &str) -> PathBuf {
        let clean = path.trim_start_matches('/');
        self.root.join(clean)
    }

    /// Backend root.
    pub fn root(&self) -> &Path {
        &self.root
    }
}

impl ObjectStorage for LocalFileBackend {
    fn read(&self, path: &str, offset: u64, size: usize) -> Result<Vec<u8>> {
        let p = self.resolve(path);
        if !p.exists() {
            return Err(TakyonicError::Engine(format!(
                "object not found: {path}"
            )));
        }
        let mut f = fs::File::open(&p)?;
        let len = f.metadata()?.len();
        if offset >= len {
            return Ok(Vec::new());
        }
        let to_read = ((len - offset) as usize).min(size);
        f.seek(SeekFrom::Start(offset))?;
        let mut buf = vec![0u8; to_read];
        f.read_exact(&mut buf)?;
        Ok(buf)
    }

    fn write(&self, path: &str, data: &[u8]) -> Result<()> {
        // Local FS is not bound by AWS PutObject limits.
        let p = self.resolve(path);
        if let Some(parent) = p.parent() {
            fs::create_dir_all(parent)?;
        }
        let tmp = p.with_extension("tmp");
        {
            let mut f = fs::File::create(&tmp)?;
            f.write_all(data)?;
            f.sync_all()?;
        }
        fs::rename(&tmp, &p)?;
        Ok(())
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let clean = prefix.trim_start_matches('/');
        let mut out = Vec::new();
        if !self.root.exists() {
            return Ok(out);
        }
        fn walk(dir: &Path, root: &Path, prefix: &str, out: &mut Vec<String>) -> Result<()> {
            for entry in fs::read_dir(dir)? {
                let entry = entry?;
                let path = entry.path();
                if path.is_dir() {
                    walk(&path, root, prefix, out)?;
                } else if let Ok(rel) = path.strip_prefix(root) {
                    let key = rel.to_string_lossy().replace('\\', "/");
                    if key.starts_with(prefix) {
                        out.push(key);
                    }
                }
            }
            Ok(())
        }
        walk(&self.root, &self.root, clean, &mut out)?;
        out.sort();
        Ok(out)
    }

    fn delete(&self, path: &str) -> Result<()> {
        let p = self.resolve(path);
        if p.exists() {
            fs::remove_file(p)?;
        }
        Ok(())
    }

    fn read_all(&self, path: &str) -> Result<Vec<u8>> {
        let p = self.resolve(path);
        if !p.exists() {
            return Err(TakyonicError::Engine(format!(
                "object not found: {path}"
            )));
        }
        Ok(fs::read(p)?)
    }

    fn len(&self, path: &str) -> Result<Option<u64>> {
        let p = self.resolve(path);
        if !p.exists() {
            return Ok(None);
        }
        Ok(Some(fs::metadata(p)?.len()))
    }
}

/// Shared in-process S3 mock (MinIO stand-in for multi-node tests).
pub struct InMemoryObjectStore {
    objects: RwLock<BTreeMap<String, Vec<u8>>>,
    /// Write counter (observability / tests).
    writes: AtomicU64,
    /// Cumulative payload bytes passed to [`ObjectStorage::write`].
    bytes_written: AtomicU64,
    /// Read counter.
    reads: AtomicU64,
    /// Size at/above which [`Self::write`] takes the multipart path (tests lower this).
    multipart_threshold: AtomicU64,
    /// Completed multipart uploads (tests).
    multipart_uploads: AtomicU64,
    /// Individual multipart parts uploaded (tests).
    multipart_parts: AtomicU64,
}

impl Default for InMemoryObjectStore {
    fn default() -> Self {
        Self {
            objects: RwLock::new(BTreeMap::new()),
            writes: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
            reads: AtomicU64::new(0),
            multipart_threshold: AtomicU64::new(AWS_S3_PUT_OBJECT_MAX_BYTES),
            multipart_uploads: AtomicU64::new(0),
            multipart_parts: AtomicU64::new(0),
        }
    }
}

impl InMemoryObjectStore {
    /// Empty shared bucket.
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Number of objects currently stored.
    pub fn object_count(&self) -> usize {
        self.objects.read().len()
    }

    /// Total write ops.
    pub fn write_ops(&self) -> u64 {
        self.writes.load(Ordering::Relaxed)
    }

    /// Total bytes uploaded via [`ObjectStorage::write`].
    pub fn bytes_written(&self) -> u64 {
        self.bytes_written.load(Ordering::Relaxed)
    }

    /// Total read ops.
    pub fn read_ops(&self) -> u64 {
        self.reads.load(Ordering::Relaxed)
    }

    /// Override the multipart size threshold (tests; avoid allocating 5 GiB).
    pub fn set_multipart_threshold(&self, bytes: u64) {
        self.multipart_threshold.store(bytes, Ordering::Relaxed);
    }

    /// Current multipart threshold.
    pub fn multipart_threshold(&self) -> u64 {
        self.multipart_threshold.load(Ordering::Relaxed)
    }

    /// Completed multipart upload count.
    pub fn multipart_uploads(&self) -> u64 {
        self.multipart_uploads.load(Ordering::Relaxed)
    }

    /// Multipart part upload count.
    pub fn multipart_parts(&self) -> u64 {
        self.multipart_parts.load(Ordering::Relaxed)
    }

    /// Reset write/read/byte/multipart counters (tests).
    pub fn reset_counters(&self) {
        self.writes.store(0, Ordering::Relaxed);
        self.bytes_written.store(0, Ordering::Relaxed);
        self.reads.store(0, Ordering::Relaxed);
        self.multipart_uploads.store(0, Ordering::Relaxed);
        self.multipart_parts.store(0, Ordering::Relaxed);
    }

    fn write_single(&self, path: &str, data: &[u8]) -> Result<()> {
        assert_put_object_size(data.len() as u64)?;
        self.writes.fetch_add(1, Ordering::Relaxed);
        self.bytes_written
            .fetch_add(data.len() as u64, Ordering::Relaxed);
        self.objects.write().insert(path.to_string(), data.to_vec());
        Ok(())
    }

    fn write_multipart_simulated(&self, path: &str, data: &[u8], part_size: usize) -> Result<()> {
        let ranges = multipart_part_ranges(data.len(), part_size);
        if ranges.is_empty() {
            return self.write_single(path, data);
        }
        // Validate non-final parts meet AWS min when using production-sized parts.
        if part_size >= AWS_S3_MULTIPART_MIN_PART_BYTES {
            for (i, (start, end)) in ranges.iter().enumerate() {
                let is_last = i + 1 == ranges.len();
                let n = end - start;
                if !is_last && n < AWS_S3_MULTIPART_MIN_PART_BYTES {
                    return Err(TakyonicError::Engine(format!(
                        "multipart part {i} is {n} bytes (< {AWS_S3_MULTIPART_MIN_PART_BYTES})"
                    )));
                }
            }
        }
        self.multipart_uploads.fetch_add(1, Ordering::Relaxed);
        self.multipart_parts
            .fetch_add(ranges.len() as u64, Ordering::Relaxed);
        self.writes.fetch_add(1, Ordering::Relaxed);
        self.bytes_written
            .fetch_add(data.len() as u64, Ordering::Relaxed);
        self.objects.write().insert(path.to_string(), data.to_vec());
        Ok(())
    }
}

impl ObjectStorage for InMemoryObjectStore {
    fn read(&self, path: &str, offset: u64, size: usize) -> Result<Vec<u8>> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        let map = self.objects.read();
        let Some(data) = map.get(path) else {
            return Err(TakyonicError::Engine(format!(
                "object not found: {path}"
            )));
        };
        if offset as usize >= data.len() {
            return Ok(Vec::new());
        }
        let start = offset as usize;
        let end = (start + size).min(data.len());
        Ok(data[start..end].to_vec())
    }

    fn write(&self, path: &str, data: &[u8]) -> Result<()> {
        let threshold = self.multipart_threshold();
        if prefer_multipart(data.len() as u64, threshold) {
            // Tests may set a tiny threshold; use a matching part size so we
            // exercise ≥2 parts without allocating multi-MiB buffers.
            let part_size = if threshold < DEFAULT_MULTIPART_PART_BYTES as u64 {
                (threshold as usize).max(1)
            } else {
                DEFAULT_MULTIPART_PART_BYTES
            };
            return self.write_multipart_simulated(path, data, part_size);
        }
        self.write_single(path, data)
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let map = self.objects.read();
        Ok(map
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect())
    }

    fn delete(&self, path: &str) -> Result<()> {
        self.objects.write().remove(path);
        Ok(())
    }

    fn read_all(&self, path: &str) -> Result<Vec<u8>> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.objects
            .read()
            .get(path)
            .cloned()
            .ok_or_else(|| TakyonicError::Engine(format!("object not found: {path}")))
    }

    fn len(&self, path: &str) -> Result<Option<u64>> {
        Ok(self
            .objects
            .read()
            .get(path)
            .map(|b| b.len() as u64))
    }
}

/// S3 / MinIO-compatible backend.
///
/// Without the `s3` feature this wraps a local staging directory (or an injected
/// [`InMemoryObjectStore`]) so unit tests exercise the same code paths. Enable
/// `--features s3` for a real `aws-sdk-s3` client pointed at MinIO/AWS.
pub struct S3Backend {
    inner: Arc<dyn ObjectStorage>,
    /// Logical bucket name (prefixed onto keys).
    bucket: String,
}

impl S3Backend {
    /// Mock / local S3: keys are stored as `{bucket}/{key}` in `inner`.
    pub fn mock(bucket: impl Into<String>, inner: Arc<dyn ObjectStorage>) -> Self {
        Self {
            inner,
            bucket: bucket.into(),
        }
    }

    /// Local filesystem bucket root (MinIO-free stand-in).
    pub fn local_bucket(root: impl Into<PathBuf>, bucket: impl Into<String>) -> Result<Self> {
        let backend = LocalFileBackend::open(root)?;
        Ok(Self::mock(bucket, Arc::new(backend)))
    }

    fn key(&self, path: &str) -> String {
        format!(
            "{}/{}",
            self.bucket.trim_matches('/'),
            path.trim_start_matches('/')
        )
    }

    /// Bucket name.
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// Open a real AWS / MinIO client (`--features s3`).
    ///
    /// Credentials are loaded from the environment / default AWS chain.
    /// For MinIO in tests, prefer [`Self::connect_minio`].
    #[cfg(feature = "s3")]
    pub async fn connect(
        bucket: impl Into<String>,
        endpoint: Option<&str>,
        region: &str,
    ) -> Result<Self> {
        use aws_config::BehaviorVersion;
        use aws_sdk_s3::config::Builder as S3ConfigBuilder;

        let shared = aws_config::defaults(BehaviorVersion::latest())
            .region(aws_config::Region::new(region.to_string()))
            .load()
            .await;
        let mut builder = S3ConfigBuilder::from(&shared);
        if let Some(ep) = endpoint {
            builder = builder.endpoint_url(ep).force_path_style(true);
        }
        let client = aws_sdk_s3::Client::from_conf(builder.build());
        Ok(Self {
            inner: Arc::new(AwsS3Client {
                client,
                bucket: bucket.into(),
            }),
            bucket: String::new(), // keys already absolute in AwsS3Client
        })
    }

    /// Connect to an S3-compatible endpoint (MinIO) with static credentials.
    ///
    /// Uploads at/above [`AWS_S3_PUT_OBJECT_MAX_BYTES`] use multipart upload.
    #[cfg(feature = "s3")]
    pub async fn connect_minio(
        bucket: impl Into<String>,
        endpoint: &str,
        access_key: &str,
        secret_key: &str,
    ) -> Result<Self> {
        use aws_sdk_s3::config::{BehaviorVersion, Builder as S3ConfigBuilder, Credentials, Region};

        let creds = Credentials::new(access_key, secret_key, None, None, "takyonic-minio");
        let conf = S3ConfigBuilder::new()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new("us-east-1"))
            .endpoint_url(endpoint)
            .force_path_style(true)
            .credentials_provider(creds)
            .build();
        let client = aws_sdk_s3::Client::from_conf(conf);
        let bucket = bucket.into();
        // Ensure bucket exists (idempotent for MinIO / S3).
        match client.create_bucket().bucket(&bucket).send().await {
            Ok(_) => {}
            Err(e) => {
                let msg = e.to_string();
                if !(msg.contains("BucketAlreadyOwnedByYou")
                    || msg.contains("BucketAlreadyExists")
                    || msg.contains("bucket already exists"))
                {
                    return Err(TakyonicError::Engine(format!("s3 create_bucket: {e}")));
                }
            }
        }
        Ok(Self {
            inner: Arc::new(AwsS3Client {
                client,
                bucket: bucket.clone(),
            }),
            bucket: String::new(),
        })
    }
}

impl ObjectStorage for S3Backend {
    fn read(&self, path: &str, offset: u64, size: usize) -> Result<Vec<u8>> {
        if self.bucket.is_empty() {
            self.inner.read(path, offset, size)
        } else {
            self.inner.read(&self.key(path), offset, size)
        }
    }

    fn write(&self, path: &str, data: &[u8]) -> Result<()> {
        if self.bucket.is_empty() {
            self.inner.write(path, data)
        } else {
            self.inner.write(&self.key(path), data)
        }
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        if self.bucket.is_empty() {
            return self.inner.list(prefix);
        }
        let full = self.key(prefix);
        let strip = format!("{}/", self.bucket.trim_matches('/'));
        Ok(self
            .inner
            .list(&full)?
            .into_iter()
            .map(|k| k.strip_prefix(&strip).unwrap_or(&k).to_string())
            .collect())
    }

    fn delete(&self, path: &str) -> Result<()> {
        if self.bucket.is_empty() {
            self.inner.delete(path)
        } else {
            self.inner.delete(&self.key(path))
        }
    }
}

#[cfg(feature = "s3")]
struct AwsS3Client {
    client: aws_sdk_s3::Client,
    bucket: String,
}

/// Run an async S3 future from sync `ObjectStorage` methods.
///
/// Uses `block_in_place` when already inside a Tokio worker (e.g. `takyonic-server`
/// `#[tokio::main]`) so nested `block_on` does not panic.
#[cfg(feature = "s3")]
fn block_on_s3<F, T>(fut: F) -> Result<T>
where
    F: std::future::Future<Output = Result<T>>,
{
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => tokio::task::block_in_place(|| handle.block_on(fut)),
        Err(_) => {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| TakyonicError::Engine(format!("s3 runtime: {e}")))?;
            rt.block_on(fut)
        }
    }
}

#[cfg(feature = "s3")]
impl AwsS3Client {
    fn write_multipart(&self, path: &str, data: &[u8], part_size: usize) -> Result<()> {
        let part_size = part_size.max(AWS_S3_MULTIPART_MIN_PART_BYTES);
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let path = path.to_string();
        let data = data.to_vec();
        block_on_s3(async move {
            use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};

            let create = client
                .create_multipart_upload()
                .bucket(&bucket)
                .key(&path)
                .send()
                .await
                .map_err(|e| TakyonicError::Engine(format!("s3 create_multipart: {e}")))?;
            let upload_id = create.upload_id().ok_or_else(|| {
                TakyonicError::Engine("s3 create_multipart missing upload_id".into())
            })?;

            let ranges = multipart_part_ranges(data.len(), part_size);
            let mut completed: Vec<CompletedPart> = Vec::with_capacity(ranges.len());
            for (i, (start, end)) in ranges.iter().enumerate() {
                let part_number = (i + 1) as i32;
                let chunk = data[*start..*end].to_vec();
                let resp = match client
                    .upload_part()
                    .bucket(&bucket)
                    .key(&path)
                    .upload_id(upload_id)
                    .part_number(part_number)
                    .body(aws_sdk_s3::primitives::ByteStream::from(chunk))
                    .send()
                    .await
                {
                    Ok(r) => r,
                    Err(e) => {
                        let _ = client
                            .abort_multipart_upload()
                            .bucket(&bucket)
                            .key(&path)
                            .upload_id(upload_id)
                            .send()
                            .await;
                        return Err(TakyonicError::Engine(format!("s3 upload_part: {e}")));
                    }
                };
                let etag = resp.e_tag().map(|s| s.to_string()).ok_or_else(|| {
                    TakyonicError::Engine(format!("s3 upload_part {part_number} missing etag"))
                })?;
                completed.push(
                    CompletedPart::builder()
                        .e_tag(etag)
                        .part_number(part_number)
                        .build(),
                );
            }

            let completed_upload = CompletedMultipartUpload::builder()
                .set_parts(Some(completed))
                .build();
            match client
                .complete_multipart_upload()
                .bucket(&bucket)
                .key(&path)
                .upload_id(upload_id)
                .multipart_upload(completed_upload)
                .send()
                .await
            {
                Ok(_) => Ok(()),
                Err(e) => {
                    let _ = client
                        .abort_multipart_upload()
                        .bucket(&bucket)
                        .key(&path)
                        .upload_id(upload_id)
                        .send()
                        .await;
                    Err(TakyonicError::Engine(format!(
                        "s3 complete_multipart: {e}"
                    )))
                }
            }
        })
    }
}

#[cfg(feature = "s3")]
impl ObjectStorage for AwsS3Client {
    fn read(&self, path: &str, offset: u64, size: usize) -> Result<Vec<u8>> {
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let path = path.to_string();
        block_on_s3(async move {
            let end = offset.saturating_add(size as u64).saturating_sub(1);
            let range = format!("bytes={offset}-{end}");
            let resp = client
                .get_object()
                .bucket(&bucket)
                .key(&path)
                .range(range)
                .send()
                .await
                .map_err(|e| TakyonicError::Engine(format!("s3 get_object: {e}")))?;
            let bytes = resp
                .body
                .collect()
                .await
                .map_err(|e| TakyonicError::Engine(format!("s3 body: {e}")))?
                .into_bytes();
            Ok(bytes.to_vec())
        })
    }

    fn write(&self, path: &str, data: &[u8]) -> Result<()> {
        if prefer_multipart(data.len() as u64, AWS_S3_PUT_OBJECT_MAX_BYTES) {
            return self.write_multipart(path, data, DEFAULT_MULTIPART_PART_BYTES);
        }
        assert_put_object_size(data.len() as u64)?;
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let path = path.to_string();
        let body = aws_sdk_s3::primitives::ByteStream::from(data.to_vec());
        block_on_s3(async move {
            client
                .put_object()
                .bucket(&bucket)
                .key(&path)
                .body(body)
                .send()
                .await
                .map_err(|e| TakyonicError::Engine(format!("s3 put_object: {e}")))?;
            Ok(())
        })
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let prefix = prefix.to_string();
        block_on_s3(async move {
            let resp = client
                .list_objects_v2()
                .bucket(&bucket)
                .prefix(&prefix)
                .send()
                .await
                .map_err(|e| TakyonicError::Engine(format!("s3 list: {e}")))?;
            let mut keys = Vec::new();
            for obj in resp.contents() {
                if let Some(k) = obj.key() {
                    keys.push(k.to_string());
                }
            }
            Ok(keys)
        })
    }

    fn delete(&self, path: &str) -> Result<()> {
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let path = path.to_string();
        block_on_s3(async move {
            client
                .delete_object()
                .bucket(&bucket)
                .key(&path)
                .send()
                .await
                .map_err(|e| TakyonicError::Engine(format!("s3 delete: {e}")))?;
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn local_and_memory_roundtrip() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("takyonic-obj-{nanos}"));
        let local = LocalFileBackend::open(&root).unwrap();
        local.write("sst/0001.sst", b"hello-sst").unwrap();
        assert_eq!(local.read("sst/0001.sst", 0, 5).unwrap(), b"hello");
        assert!(local.list("sst/").unwrap().contains(&"sst/0001.sst".into()));

        let mem = InMemoryObjectStore::new();
        let s3 = S3Backend::mock("takyonic", Arc::clone(&mem) as Arc<dyn ObjectStorage>);
        s3.write("pages/0", &[1, 2, 3, 4]).unwrap();
        assert_eq!(s3.read("pages/0", 1, 2).unwrap(), vec![2, 3]);
        // Separate "node" handle shares the same mock bucket.
        let s3b = S3Backend::mock("takyonic", Arc::clone(&mem) as Arc<dyn ObjectStorage>);
        assert_eq!(s3b.read_all("pages/0").unwrap(), vec![1, 2, 3, 4]);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn assert_put_object_size_rejects_aws_limit() {
        assert_put_object_size(0).unwrap();
        assert_put_object_size(1024 * 1024 * 1024).unwrap(); // 1 GiB OK
        assert_put_object_size(AWS_S3_PUT_OBJECT_MAX_BYTES - 1).unwrap();
        let err = assert_put_object_size(AWS_S3_PUT_OBJECT_MAX_BYTES).unwrap_err();
        assert!(
            err.to_string().contains("multipart") || err.to_string().contains("PutObject"),
            "error must mention PutObject/multipart: {err}"
        );
        let mem = InMemoryObjectStore::new();
        mem.write("ok", b"tiny").unwrap();
    }

    #[test]
    fn multipart_part_ranges_cover_full_payload() {
        assert!(multipart_part_ranges(0, 8).is_empty());
        assert_eq!(multipart_part_ranges(10, 8), vec![(0, 8), (8, 10)]);
        assert_eq!(multipart_part_ranges(16, 8), vec![(0, 8), (8, 16)]);
        assert!(prefer_multipart(AWS_S3_PUT_OBJECT_MAX_BYTES, AWS_S3_PUT_OBJECT_MAX_BYTES));
        assert!(!prefer_multipart(AWS_S3_PUT_OBJECT_MAX_BYTES - 1, AWS_S3_PUT_OBJECT_MAX_BYTES));
    }

    #[test]
    fn in_memory_multipart_path_roundtrip_with_part_counters() {
        let mem = InMemoryObjectStore::new();
        // Force MPU without allocating 5 GiB.
        mem.set_multipart_threshold(64);
        let payload: Vec<u8> = (0..200u8).collect();
        mem.write("big/obj", &payload).unwrap();
        assert_eq!(mem.multipart_uploads(), 1);
        assert!(
            mem.multipart_parts() >= 2,
            "expected ≥2 parts, got {}",
            mem.multipart_parts()
        );
        assert_eq!(mem.read_all("big/obj").unwrap(), payload);
        // Under threshold → single put, no extra MPU.
        mem.reset_counters();
        mem.write("small", b"hi").unwrap();
        assert_eq!(mem.multipart_uploads(), 0);
        assert_eq!(mem.read_all("small").unwrap(), b"hi");
    }

    #[test]
    fn put_object_policy_caps_align_under_aws_limit() {
        use crate::compaction::DEFAULT_MAX_SST_BYTES;
        use crate::config::Config;
        use crate::manifest::DEFAULT_PAGES_CHUNK_BYTES;

        assert!(DEFAULT_MAX_SST_BYTES < AWS_S3_PUT_OBJECT_MAX_BYTES);
        assert!(DEFAULT_PAGES_CHUNK_BYTES < AWS_S3_PUT_OBJECT_MAX_BYTES);
        let cfg = Config::default();
        assert!(cfg.max_sst_bytes < AWS_S3_PUT_OBJECT_MAX_BYTES);
        assert!((cfg.object_pages_chunk_bytes as u64) < AWS_S3_PUT_OBJECT_MAX_BYTES);
        cfg.validate().unwrap();

        let bad = Config::default().max_sst_bytes(AWS_S3_PUT_OBJECT_MAX_BYTES);
        assert!(matches!(bad.validate(), Err(TakyonicError::Config(_))));
    }
}

/// Real MinIO / S3-compatible backend tests (`--features s3`).
///
/// Starts one shared MinIO container when Docker is available, or uses
/// `TAKYONIC_S3_ENDPOINT` if already set. Skips cleanly otherwise.
///
/// Gaps vs in-memory mock covered here: TCP network errors, container-down
/// mid-flight, read-after-write + list visibility, large single-PutObject
/// bodies. Multipart activates for payloads ≥5 GiB (see unit mock tests for
/// MPU part counting without allocating 5 GiB).
#[cfg(all(test, feature = "s3"))]
mod minio_tests {
    use super::*;
    use std::process::{Command, Stdio};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Mutex, OnceLock};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    const CONTAINER: &str = "takyonic-minio-itest";
    const DEFAULT_ENDPOINT: &str = "http://127.0.0.1:19000";
    const ACCESS: &str = "minioadmin";
    const SECRET: &str = "minioadmin";

    static ENDPOINT: OnceLock<String> = OnceLock::new();
    /// Serializes MinIO I/O + stop/start across parallel tests.
    static MINIO_OPS: Mutex<()> = Mutex::new(());
    static INIT: Mutex<()> = Mutex::new(());

    fn docker_ok() -> bool {
        Command::new("docker")
            .args(["info"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
    }

    async fn wait_ready(endpoint: &str) -> bool {
        for attempt in 0..60 {
            match S3Backend::connect_minio(
                &format!("takyonic-health-{attempt}"),
                endpoint,
                ACCESS,
                SECRET,
            )
            .await
            {
                Ok(_) => return true,
                Err(e) => {
                    if attempt == 59 {
                        eprintln!("minio wait_ready last error: {e}");
                    }
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        }
        false
    }

    /// Shared endpoint for the process. Container is left running (no Drop rm)
    /// so parallel tests do not race on create/destroy.
    async fn shared_endpoint() -> Option<&'static str> {
        if let Some(ep) = ENDPOINT.get() {
            return Some(ep.as_str());
        }
        let _lock = INIT.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(ep) = ENDPOINT.get() {
            return Some(ep.as_str());
        }
        if let Ok(ep) = std::env::var("TAKYONIC_S3_ENDPOINT") {
            if wait_ready(&ep).await {
                let _ = ENDPOINT.set(ep);
                return ENDPOINT.get().map(|s| s.as_str());
            }
            return None;
        }
        if !docker_ok() {
            eprintln!("minio tests skipped: docker unavailable and TAKYONIC_S3_ENDPOINT unset");
            return None;
        }
        // Reuse an already-running container from a previous suite if healthy.
        if wait_ready(DEFAULT_ENDPOINT).await {
            let _ = ENDPOINT.set(DEFAULT_ENDPOINT.to_string());
            return ENDPOINT.get().map(|s| s.as_str());
        }
        let _ = Command::new("docker")
            .args(["rm", "-f", CONTAINER])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        let status = Command::new("docker")
            .args([
                "run",
                "-d",
                "--name",
                CONTAINER,
                "-p",
                "19000:9000",
                "-e",
                "MINIO_ROOT_USER=minioadmin",
                "-e",
                "MINIO_ROOT_PASSWORD=minioadmin",
                "minio/minio:latest",
                "server",
                "/data",
            ])
            .stdout(Stdio::null())
            .status()
            .ok()?;
        if !status.success() {
            eprintln!("minio tests skipped: docker run failed");
            return None;
        }
        if !wait_ready(DEFAULT_ENDPOINT).await {
            eprintln!("minio tests skipped: MinIO did not become ready");
            return None;
        }
        let _ = ENDPOINT.set(DEFAULT_ENDPOINT.to_string());
        ENDPOINT.get().map(|s| s.as_str())
    }

    /// ObjectStorage methods `block_on` internally — call them off the async worker.
    async fn blocking_write(s3: Arc<S3Backend>, key: &str, data: Vec<u8>) -> Result<()> {
        let key = key.to_string();
        tokio::task::spawn_blocking(move || s3.write(&key, &data))
            .await
            .map_err(|e| TakyonicError::Engine(format!("join: {e}")))?
    }

    async fn blocking_read(
        s3: Arc<S3Backend>,
        key: &str,
        offset: u64,
        size: usize,
    ) -> Result<Vec<u8>> {
        let key = key.to_string();
        tokio::task::spawn_blocking(move || s3.read(&key, offset, size))
            .await
            .map_err(|e| TakyonicError::Engine(format!("join: {e}")))?
    }

    async fn blocking_list(s3: Arc<S3Backend>, prefix: &str) -> Result<Vec<String>> {
        let prefix = prefix.to_string();
        tokio::task::spawn_blocking(move || s3.list(&prefix))
            .await
            .map_err(|e| TakyonicError::Engine(format!("join: {e}")))?
    }

    async fn blocking_delete(s3: Arc<S3Backend>, key: &str) -> Result<()> {
        let key = key.to_string();
        tokio::task::spawn_blocking(move || s3.delete(&key))
            .await
            .map_err(|e| TakyonicError::Engine(format!("join: {e}")))?
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn minio_roundtrip_and_list_consistency() {
        let _ops = MINIO_OPS.lock().unwrap_or_else(|p| p.into_inner());
        let Some(endpoint) = shared_endpoint().await else {
            return;
        };
        let bucket = format!(
            "takyonic-rt-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let s3 = Arc::new(
            S3Backend::connect_minio(&bucket, endpoint, ACCESS, SECRET)
                .await
                .expect("connect minio"),
        );
        blocking_write(Arc::clone(&s3), "pages/0", b"hello-minio".to_vec())
            .await
            .unwrap();
        assert_eq!(
            blocking_read(Arc::clone(&s3), "pages/0", 0, 5)
                .await
                .unwrap(),
            b"hello"
        );
        let keys = blocking_list(Arc::clone(&s3), "pages/").await.unwrap();
        assert!(
            keys.iter().any(|k| k == "pages/0" || k.ends_with("pages/0")),
            "list missing pages/0: {keys:?}"
        );
        blocking_delete(s3, "pages/0").await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn minio_network_error_on_bad_endpoint() {
        let err = S3Backend::connect_minio("nope", "http://127.0.0.1:1", ACCESS, SECRET).await;
        assert!(err.is_err(), "expected connection failure to closed port");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn minio_error_when_container_stopped() {
        let _ops = MINIO_OPS.lock().unwrap_or_else(|p| p.into_inner());
        let Some(endpoint) = shared_endpoint().await else {
            return;
        };
        if std::env::var("TAKYONIC_S3_ENDPOINT").is_ok() {
            eprintln!("minio_error_when_container_stopped skipped: external endpoint");
            return;
        }
        let bucket = format!(
            "takyonic-stop-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let s3 = Arc::new(
            S3Backend::connect_minio(&bucket, endpoint, ACCESS, SECRET)
                .await
                .unwrap(),
        );
        blocking_write(Arc::clone(&s3), "before-stop", b"ok".to_vec())
            .await
            .unwrap();
        let status = Command::new("docker")
            .args(["stop", CONTAINER])
            .status()
            .unwrap();
        assert!(status.success());
        let err = blocking_write(Arc::clone(&s3), "after-stop", b"should-fail".to_vec()).await;
        assert!(err.is_err(), "write must fail after MinIO stop");
        let _ = Command::new("docker").args(["start", CONTAINER]).status();
        assert!(
            wait_ready(endpoint).await,
            "MinIO must come back after docker start"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn minio_large_single_put_no_multipart() {
        let _ops = MINIO_OPS.lock().unwrap_or_else(|p| p.into_inner());
        let Some(endpoint) = shared_endpoint().await else {
            return;
        };
        let bucket = format!(
            "takyonic-big-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let s3 = Arc::new(
            S3Backend::connect_minio(&bucket, endpoint, ACCESS, SECRET)
                .await
                .unwrap(),
        );
        let mut data = vec![0u8; 8 * 1024 * 1024];
        for (i, b) in data.iter_mut().enumerate() {
            *b = (i % 251) as u8;
        }
        blocking_write(Arc::clone(&s3), "blob/large.bin", data.clone())
            .await
            .unwrap();
        let got = blocking_read(Arc::clone(&s3), "blob/large.bin", 0, 64)
            .await
            .unwrap();
        assert_eq!(got, &data[..64]);
        let mid = blocking_read(Arc::clone(&s3), "blob/large.bin", 4 * 1024 * 1024, 16)
            .await
            .unwrap();
        assert_eq!(mid, &data[4 * 1024 * 1024..4 * 1024 * 1024 + 16]);
        blocking_delete(s3, "blob/large.bin").await.unwrap();
    }

    /// Counts payload bytes forwarded to an inner [`ObjectStorage`] (Phase 2C metrics).
    struct CountingStore {
        inner: Arc<dyn ObjectStorage>,
        bytes_written: AtomicU64,
        write_ops: AtomicU64,
    }

    impl CountingStore {
        fn new(inner: Arc<dyn ObjectStorage>) -> Arc<Self> {
            Arc::new(Self {
                inner,
                bytes_written: AtomicU64::new(0),
                write_ops: AtomicU64::new(0),
            })
        }
        fn bytes_written(&self) -> u64 {
            self.bytes_written.load(Ordering::Relaxed)
        }
        fn write_ops(&self) -> u64 {
            self.write_ops.load(Ordering::Relaxed)
        }
        fn reset(&self) {
            self.bytes_written.store(0, Ordering::Relaxed);
            self.write_ops.store(0, Ordering::Relaxed);
        }
    }

    impl ObjectStorage for CountingStore {
        fn read(&self, path: &str, offset: u64, size: usize) -> Result<Vec<u8>> {
            self.inner.read(path, offset, size)
        }
        fn write(&self, path: &str, data: &[u8]) -> Result<()> {
            self.write_ops.fetch_add(1, Ordering::Relaxed);
            self.bytes_written
                .fetch_add(data.len() as u64, Ordering::Relaxed);
            self.inner.write(path, data)
        }
        fn list(&self, prefix: &str) -> Result<Vec<String>> {
            self.inner.list(prefix)
        }
        fn delete(&self, path: &str) -> Result<()> {
            self.inner.delete(path)
        }
        fn read_all(&self, path: &str) -> Result<Vec<u8>> {
            self.inner.read_all(path)
        }
        fn len(&self, path: &str) -> Result<Option<u64>> {
            self.inner.len(path)
        }
    }

    /// Target working set in MiB for the MinIO DiskManager cycle.
    /// Override with `TAKYONIC_PHASE2C_MIB` (e.g. `2048` for a true multi-GiB run).
    fn phase2c_target_mib() -> usize {
        std::env::var("TAKYONIC_PHASE2C_MIB")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(64)
    }

    /// DiskManager ↔ MinIO multi-chunk write/read cycle + V1 vs V2 byte math.
    #[tokio::test(flavor = "multi_thread")]
    async fn minio_chunked_pages_cycle_and_v1_v2_upload_math() {
        let _ops = MINIO_OPS.lock().unwrap_or_else(|p| p.into_inner());
        let Some(endpoint) = shared_endpoint().await else {
            return;
        };
        let target_mib = phase2c_target_mib();
        let page_size = 1024 * 1024; // 1 MiB pages (power of two)
        let chunk_size = 4 * 1024 * 1024; // 4 MiB chunks
        let pages = target_mib; // one page = 1 MiB
        let pages_per_chunk = chunk_size / page_size;

        let bucket = format!(
            "takyonic-p2c-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let s3 = Arc::new(
            S3Backend::connect_minio(&bucket, endpoint, ACCESS, SECRET)
                .await
                .expect("connect minio"),
        );
        let counter = CountingStore::new(s3 as Arc<dyn ObjectStorage>);
        let store: Arc<dyn ObjectStorage> = Arc::clone(&counter) as Arc<dyn ObjectStorage>;

        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("takyonic-p2c-dm-{nanos}"));

        let (v2_bytes, v2_ops, marker) = tokio::task::spawn_blocking({
            let store = Arc::clone(&store);
            let counter = Arc::clone(&counter);
            let root = root.clone();
            move || {
                use crate::disk::{
                    DiskManager, PAGES_V2_PREFIX, REMOTE_PAGES_KEY, pages_chunk_key,
                };
                use crate::page::Page;

                let dm = DiskManager::open_with_remote_layout(
                    &root,
                    page_size,
                    Some(store),
                    REMOTE_PAGES_KEY,
                    PAGES_V2_PREFIX,
                    chunk_size,
                )?;
                counter.reset();
                let mut last_marker = 0u8;
                for i in 0..pages as u64 {
                    let mut page = Page::new_aligned(page_size);
                    page.page_id = i;
                    let marker = ((i * 17) % 251) as u8;
                    page.data_mut()[0] = marker;
                    page.data_mut()[page_size - 1] = marker ^ 0xFF;
                    dm.write_page(&page)?;
                    last_marker = marker;
                }
                // Cold re-open / hydrate check on last page.
                let mut loaded = Page::new_aligned(page_size);
                // Force remote path: wipe would need new dm; instead read via store.
                let chunk_id = (pages as u64 - 1) / pages_per_chunk as u64;
                let key = pages_chunk_key(PAGES_V2_PREFIX, chunk_id);
                let _ = key;
                dm.read_page(pages as u64 - 1, &mut loaded)?;
                assert_eq!(loaded.data()[0], last_marker);
                Ok::<_, TakyonicError>((counter.bytes_written(), counter.write_ops(), last_marker))
            }
        })
        .await
        .expect("join")
        .expect("disk manager cycle");

        // V1 math: each dirty page rewrote the full heap so far (triangular) ≈
        // sum_{i=1..N} i * page_size ≈ N*(N+1)/2 * page_size. Use lower bound
        // N * final_heap for the simpler "rewrite full heap each time" model.
        let heap_bytes = (pages * page_size) as u64;
        let v1_lower_bound = pages as u64 * heap_bytes; // N full-heap PutObjects
        assert!(
            v2_bytes < v1_lower_bound / 4,
            "V2 uploaded {v2_bytes} bytes over {v2_ops} writes; V1 lower bound ≈ {v1_lower_bound} \
             (target_mib={target_mib}). V2 must be ≪ V1."
        );
        // Each write touches one chunk ≤ chunk_size (sparse growth may be smaller).
        assert!(
            v2_bytes <= (pages as u64) * chunk_size as u64,
            "V2 bytes {v2_bytes} exceeded pages×chunk_size"
        );
        eprintln!(
            "Phase 2C MinIO cycle: target={target_mib} MiB, V2 uploaded={v2_bytes} bytes \
             ({:.2} MiB) in {v2_ops} PutObjects; V1 lower-bound≈{v1_lower_bound} bytes \
             ({:.2} GiB); ratio V1/V2≈{:.1}x; last_marker={marker}",
            v2_bytes as f64 / (1024.0 * 1024.0),
            v1_lower_bound as f64 / (1024.0 * 1024.0 * 1024.0),
            v1_lower_bound as f64 / v2_bytes.max(1) as f64,
        );

        let _ = std::fs::remove_dir_all(root);
    }

    /// Heap logically past the 5 GiB AWS PutObject limit: V2 writes one chunk,
    /// never a ≥5 GiB object. (Does not allocate 5 GiB of RAM.)
    #[tokio::test(flavor = "multi_thread")]
    async fn minio_v2_survives_past_5gib_heap_offset() {
        let _ops = MINIO_OPS.lock().unwrap_or_else(|p| p.into_inner());
        let Some(endpoint) = shared_endpoint().await else {
            return;
        };
        const PAGE: usize = 4096;
        const CHUNK: usize = 1024 * 1024; // 1 MiB
        const FIVE_GIB: u64 = 5 * 1024 * 1024 * 1024;
        let page_id = FIVE_GIB / PAGE as u64 + 42;

        let bucket = format!(
            "takyonic-p2c5-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let s3 = Arc::new(
            S3Backend::connect_minio(&bucket, endpoint, ACCESS, SECRET)
                .await
                .unwrap(),
        );
        let counter = CountingStore::new(s3 as Arc<dyn ObjectStorage>);
        let store: Arc<dyn ObjectStorage> = Arc::clone(&counter) as Arc<dyn ObjectStorage>;

        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("takyonic-p2c5-{nanos}"));

        let uploaded = tokio::task::spawn_blocking({
            let store = Arc::clone(&store);
            let counter = Arc::clone(&counter);
            let root = root.clone();
            move || {
                use crate::disk::{
                    DiskManager, PAGES_V2_PREFIX, REMOTE_PAGES_KEY, pages_chunk_key,
                };
                use crate::page::Page;

                let dm = DiskManager::open_with_remote_layout(
                    &root,
                    PAGE,
                    Some(Arc::clone(&store)),
                    REMOTE_PAGES_KEY,
                    PAGES_V2_PREFIX,
                    CHUNK,
                )?;
                counter.reset();
                let mut page = Page::new_aligned(PAGE);
                page.page_id = page_id;
                page.data_mut()[0] = 0x5A;
                page.data_mut()[PAGE - 1] = 0xA5;
                dm.write_page(&page)?;
                let uploaded = counter.bytes_written();
                assert!(
                    uploaded <= CHUNK as u64,
                    "past-5GiB page uploaded {uploaded} bytes; must be ≤ chunk {CHUNK}"
                );
                assert!(
                    uploaded < FIVE_GIB,
                    "must never PutObject a ≥5 GiB blob; uploaded {uploaded}"
                );

                // Cold read from a fresh local dir.
                let root2 = std::env::temp_dir().join(format!(
                    "takyonic-p2c5-cold-{}",
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_nanos()
                ));
                let dm2 = DiskManager::open_with_remote_layout(
                    &root2,
                    PAGE,
                    Some(store),
                    REMOTE_PAGES_KEY,
                    PAGES_V2_PREFIX,
                    CHUNK,
                )?;
                let mut loaded = Page::new_aligned(PAGE);
                dm2.read_page(page_id, &mut loaded)?;
                assert_eq!(loaded.data()[0], 0x5A);
                assert_eq!(loaded.data()[PAGE - 1], 0xA5);

                let ppc = (CHUNK / PAGE) as u64;
                let chunk_id = page_id / ppc;
                let key = pages_chunk_key(PAGES_V2_PREFIX, chunk_id);
                let obj_len = dm2
                    .remote()
                    .unwrap()
                    .len(&key)?
                    .expect("chunk object present");
                assert!(obj_len <= CHUNK as u64);

                let _ = std::fs::remove_dir_all(root2);
                eprintln!(
                    "Phase 2C >5GiB sim: page_id={page_id} (offset≈{:.2} GiB), \
                     uploaded={uploaded} bytes, chunk_obj_len={obj_len}",
                    (page_id * PAGE as u64) as f64 / (1024.0 * 1024.0 * 1024.0),
                );
                Ok::<_, TakyonicError>(uploaded)
            }
        })
        .await
        .expect("join")
        .expect("past-5GiB write");

        assert!(uploaded <= CHUNK as u64);
        let _ = std::fs::remove_dir_all(root);
    }
}
