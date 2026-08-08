//! Direct-I/O disk manager for fixed-size pages.
//!
//! On Linux, files are opened with `O_DIRECT` so reads/writes bypass the OS page
//! cache. Buffers must be page-size aligned (enforced by [`crate::page::Page`]).
//!
//! The primary `PAGES` file holds mutable BPM pages (Tier-1 local NVMe/SSD cache).
//! When an [`ObjectStorage`] backend is attached, page misses hydrate from remote
//! object storage (Tier-2) into the local file before serving the query engine.

use std::collections::{BTreeMap, HashMap};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use parking_lot::Mutex;

use crate::error::{Result, TakyonicError};
use crate::object_store::ObjectStorage;
use crate::page::{INVALID_PAGE_ID, Page, PageId};

/// On-disk page file name under `data_dir`.
pub const PAGE_FILE_NAME: &str = "PAGES";
/// Default remote object key for the legacy V1 pages blob.
pub const REMOTE_PAGES_KEY: &str = "pages/PAGES";
/// Default prefix for V2 chunked page objects.
pub const PAGES_V2_PREFIX: &str = "pages/v2";
/// Default chunk size for remote page objects (64 MiB).
pub const DEFAULT_PAGES_CHUNK_BYTES: usize = 64 * 1024 * 1024;

/// Encode a secondary-file cached page id: high 32 bits = file id, low 32 = page index.
#[inline]
pub fn file_page_id(file_id: u32, page_index: u32) -> PageId {
    ((file_id as u64) << 32) | (page_index as u64)
}

/// Whether `page_id` refers to a read-only secondary file page (not the PAGES heap).
#[inline]
pub fn is_file_cache_page(page_id: PageId) -> bool {
    page_id != INVALID_PAGE_ID && (page_id >> 32) != 0
}

/// Object key for chunk `chunk_id` under `prefix`.
#[inline]
pub fn pages_chunk_key(prefix: &str, chunk_id: u64) -> String {
    format!("{prefix}/chunk-{chunk_id:020}")
}

/// Manages a page-aligned heap file plus optional registered read-only files.
pub struct DiskManager {
    page_size: usize,
    path: PathBuf,
    file: Mutex<File>,
    /// Next page id to allocate via [`Self::allocate_page`] (low 32-bit space).
    next_page_id: AtomicU64,
    /// Whether the primary file was opened with `O_DIRECT`.
    direct_io: bool,
    next_file_id: AtomicU32,
    /// Registered secondary files for read-through caching.
    files: Mutex<HashMap<u32, PathBuf>>,
    /// Tier-2 remote object store (optional).
    remote: Option<Arc<dyn ObjectStorage>>,
    /// Legacy V1 pages blob key (migration source).
    remote_pages_key: String,
    /// V2 chunk key prefix.
    pages_prefix: String,
    /// Remote chunk size in bytes (multiple of `page_size`).
    chunk_size: usize,
    /// Pages hydrated from remote → local cache.
    remote_fetches: AtomicU64,
    /// Pages written through to remote.
    remote_writes: AtomicU64,
}

impl DiskManager {
    /// Create or open the primary page file at `data_dir/PAGES` (local only).
    pub fn open(data_dir: impl AsRef<Path>, page_size: usize) -> Result<Self> {
        Self::open_with_remote(data_dir, page_size, None, REMOTE_PAGES_KEY)
    }

    /// Open with an optional remote object store (chunked V2 layout, default 64 MiB chunks).
    pub fn open_with_remote(
        data_dir: impl AsRef<Path>,
        page_size: usize,
        remote: Option<Arc<dyn ObjectStorage>>,
        remote_pages_key: impl Into<String>,
    ) -> Result<Self> {
        Self::open_with_remote_layout(
            data_dir,
            page_size,
            remote,
            remote_pages_key,
            PAGES_V2_PREFIX,
            DEFAULT_PAGES_CHUNK_BYTES,
        )
    }

    /// Open with explicit V2 chunk layout parameters.
    pub fn open_with_remote_layout(
        data_dir: impl AsRef<Path>,
        page_size: usize,
        remote: Option<Arc<dyn ObjectStorage>>,
        remote_pages_key: impl Into<String>,
        pages_prefix: impl Into<String>,
        chunk_size: usize,
    ) -> Result<Self> {
        if page_size == 0 || !page_size.is_power_of_two() {
            return Err(TakyonicError::Config(
                "disk manager page_size must be a non-zero power of two".into(),
            ));
        }
        if remote.is_some() {
            if chunk_size == 0 || chunk_size % page_size != 0 {
                return Err(TakyonicError::Config(
                    "pages chunk_size must be a non-zero multiple of page_size".into(),
                ));
            }
        }
        std::fs::create_dir_all(data_dir.as_ref())?;
        let path = data_dir.as_ref().join(PAGE_FILE_NAME);
        let (file, direct_io) = open_page_file(&path, true)?;
        let meta_len = file.metadata()?.len();
        let mut next = if meta_len == 0 {
            0
        } else {
            meta_len / page_size as u64
        };

        let remote_pages_key = remote_pages_key.into();
        let pages_prefix = pages_prefix.into();

        // Migrate legacy V1 blob → chunks before sizing cold start.
        if let Some(store) = &remote {
            migrate_v1_blob_if_needed(
                store.as_ref(),
                &remote_pages_key,
                &pages_prefix,
                page_size,
                chunk_size,
            )?;
        }

        // Cold start: local cache empty → derive length from remote chunks (or V1).
        if next == 0 {
            if let Some(store) = &remote {
                next = remote_num_pages(
                    store.as_ref(),
                    &remote_pages_key,
                    &pages_prefix,
                    page_size,
                    chunk_size,
                )?;
            }
        }

        Ok(Self {
            page_size,
            path,
            file: Mutex::new(file),
            next_page_id: AtomicU64::new(next.min(u32::MAX as u64)),
            direct_io,
            next_file_id: AtomicU32::new(1), // 0 reserved for primary
            files: Mutex::new(HashMap::new()),
            remote,
            remote_pages_key,
            pages_prefix,
            chunk_size,
            remote_fetches: AtomicU64::new(0),
            remote_writes: AtomicU64::new(0),
        })
    }

    /// Page size used for all I/O.
    #[inline]
    pub fn page_size(&self) -> usize {
        self.page_size
    }

    /// Whether Direct I/O (`O_DIRECT`) is active on the primary file.
    #[inline]
    pub fn uses_direct_io(&self) -> bool {
        self.direct_io
    }

    /// Path of the primary page file (Tier-1 local cache).
    #[inline]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Whether a remote object store is attached.
    #[inline]
    pub fn has_remote(&self) -> bool {
        self.remote.is_some()
    }

    /// Legacy V1 remote pages object key (migration source).
    #[inline]
    pub fn remote_pages_key(&self) -> &str {
        &self.remote_pages_key
    }

    /// V2 chunk key prefix.
    #[inline]
    pub fn pages_prefix(&self) -> &str {
        &self.pages_prefix
    }

    /// Remote chunk size in bytes.
    #[inline]
    pub fn chunk_size(&self) -> usize {
        self.chunk_size
    }

    /// Count of Tier-2 → Tier-1 hydrations.
    #[inline]
    pub fn remote_fetches(&self) -> u64 {
        self.remote_fetches.load(Ordering::Relaxed)
    }

    /// Count of Tier-1 → Tier-2 write-throughs.
    #[inline]
    pub fn remote_writes(&self) -> u64 {
        self.remote_writes.load(Ordering::Relaxed)
    }

    /// Borrow the remote store (if any).
    pub fn remote(&self) -> Option<&Arc<dyn ObjectStorage>> {
        self.remote.as_ref()
    }

    /// Register a read-only file (e.g. SST) for page-aligned cached reads.
    pub fn register_file(&self, path: impl AsRef<Path>) -> u32 {
        let id = self.next_file_id.fetch_add(1, Ordering::Relaxed);
        self.files
            .lock()
            .insert(id, path.as_ref().to_path_buf());
        id
    }

    /// Allocate a new mutable page id in the primary PAGES file.
    pub fn allocate_page(&self) -> PageId {
        self.next_page_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Highest allocated primary page id (exclusive).
    pub fn num_pages(&self) -> u64 {
        self.next_page_id.load(Ordering::Relaxed)
    }

    /// Read `page_id` into `page`'s aligned buffer.
    ///
    /// Two-tier path: serve from local cache when present; on miss (local EOF /
    /// zeros beyond known remote length) fetch the page range from object
    /// storage, persist it locally, then return.
    pub fn read_page(&self, page_id: PageId, page: &mut Page) -> Result<()> {
        if page.page_size() != self.page_size {
            return Err(TakyonicError::Engine(
                "page frame size mismatch for disk read".into(),
            ));
        }
        if is_file_cache_page(page_id) {
            let file_id = (page_id >> 32) as u32;
            let page_index = page_id as u32;
            return self.read_registered_page(file_id, page_index, page);
        }
        let offset = page_id
            .checked_mul(self.page_size as u64)
            .ok_or_else(|| TakyonicError::Engine("page offset overflow".into()))?;

        // Tier 1: local NVMe/SSD cache.
        {
            let mut file = self.file.lock();
            let file_len = file.metadata()?.len();
            if offset + self.page_size as u64 <= file_len {
                file.seek(SeekFrom::Start(offset))?;
                file.read_exact(page.data_mut())?;
                page.page_id = page_id;
                page.dirty = false;
                return Ok(());
            }
        }

        // Tier 2: remote chunk → hydrate local cache.
        if let Some(store) = &self.remote {
            match self.read_page_remote(store.as_ref(), page_id) {
                Ok(bytes) if !bytes.is_empty() => {
                    page.data_mut().fill(0);
                    let n = bytes.len().min(self.page_size);
                    page.data_mut()[..n].copy_from_slice(&bytes[..n]);
                    self.remote_fetches.fetch_add(1, Ordering::Relaxed);
                    self.write_page_local(page_id, page.data())?;
                    page.page_id = page_id;
                    page.dirty = false;
                    return Ok(());
                }
                Ok(_) | Err(_) => {
                    // Missing remote page → treat as zero page (fresh allocation).
                }
            }
        }

        page.data_mut().fill(0);
        page.page_id = page_id;
        page.dirty = false;
        Ok(())
    }

    /// Write a mutable primary page (no-ops for file-cache pages).
    ///
    /// Always updates the local cache; when remote is configured, write-through
    /// only the touched V2 chunk (not the entire heap). Prefer
    /// [`Self::write_page_snapshots_coalesced`] when flushing many dirty pages
    /// so each chunk is uploaded once.
    pub fn write_page(&self, page: &Page) -> Result<()> {
        if page.page_size() != self.page_size {
            return Err(TakyonicError::Engine(
                "page frame size mismatch for disk write".into(),
            ));
        }
        self.write_page_snapshots_coalesced(&[(page.page_id, page.data())])
    }

    /// Write many primary-page snapshots, coalescing remote PutObject by chunk.
    ///
    /// Each page is written to the local `PAGES` file; then for every distinct
    /// V2 chunk that received at least one page, a single RMW upload runs
    /// (read remote chunk → overlay pages → one `ObjectStorage::write`).
    pub fn write_page_snapshots_coalesced(&self, pages: &[(PageId, &[u8])]) -> Result<()> {
        if pages.is_empty() {
            return Ok(());
        }
        let mut by_chunk: BTreeMap<u64, Vec<(PageId, &[u8])>> = BTreeMap::new();
        for &(page_id, data) in pages {
            if page_id == INVALID_PAGE_ID {
                return Err(TakyonicError::Engine("cannot write invalid page id".into()));
            }
            if is_file_cache_page(page_id) {
                continue;
            }
            if data.len() != self.page_size {
                return Err(TakyonicError::Engine(
                    "page frame size mismatch for disk write".into(),
                ));
            }
            self.write_page_local(page_id, data)?;
            if self.remote.is_some() {
                by_chunk
                    .entry(self.chunk_id_for_page(page_id))
                    .or_default()
                    .push((page_id, data));
            }
        }
        for overlays in by_chunk.values() {
            self.upload_chunk_with_overlays(overlays)?;
        }
        Ok(())
    }

    fn write_page_local(&self, page_id: PageId, data: &[u8]) -> Result<()> {
        let offset = page_id
            .checked_mul(self.page_size as u64)
            .ok_or_else(|| TakyonicError::Engine("page offset overflow".into()))?;
        let mut file = self.file.lock();
        let need = offset + self.page_size as u64;
        if file.metadata()?.len() < need {
            extend_file(&mut file, need, self.page_size)?;
        }
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(data)?;
        file.sync_data()?;
        Ok(())
    }

    fn pages_per_chunk(&self) -> u64 {
        (self.chunk_size / self.page_size) as u64
    }

    fn chunk_id_for_page(&self, page_id: PageId) -> u64 {
        page_id / self.pages_per_chunk()
    }

    fn offset_in_chunk(&self, page_id: PageId) -> usize {
        ((page_id % self.pages_per_chunk()) as usize) * self.page_size
    }

    fn read_page_remote(&self, store: &dyn ObjectStorage, page_id: PageId) -> Result<Vec<u8>> {
        let chunk_id = self.chunk_id_for_page(page_id);
        let key = pages_chunk_key(&self.pages_prefix, chunk_id);
        let offset = self.offset_in_chunk(page_id) as u64;
        store.read(&key, offset, self.page_size)
    }

    /// RMW one V2 chunk: overlay all pages in `overlays` (same chunk), one PutObject.
    fn upload_chunk_with_overlays(&self, overlays: &[(PageId, &[u8])]) -> Result<()> {
        let Some(store) = &self.remote else {
            return Ok(());
        };
        if overlays.is_empty() {
            return Ok(());
        }
        let chunk_id = self.chunk_id_for_page(overlays[0].0);
        let key = pages_chunk_key(&self.pages_prefix, chunk_id);
        let mut blob = match store.read_all(&key) {
            Ok(b) => b,
            Err(_) => Vec::new(),
        };
        for &(page_id, data) in overlays {
            debug_assert_eq!(self.chunk_id_for_page(page_id), chunk_id);
            let offset = self.offset_in_chunk(page_id);
            let need = offset + self.page_size;
            if blob.len() < need {
                blob.resize(need, 0);
            }
            blob[offset..need].copy_from_slice(data);
        }
        store.write(&key, &blob)?;
        self.remote_writes.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Upload the entire local PAGES file to remote as V2 chunks (checkpoint).
    pub fn sync_pages_to_remote(&self) -> Result<()> {
        let Some(store) = &self.remote else {
            return Ok(());
        };
        let bytes = std::fs::read(&self.path)?;
        if bytes.is_empty() {
            return Ok(());
        }
        let ppc = self.pages_per_chunk() as usize;
        let page_size = self.page_size;
        let total_pages = bytes.len() / page_size;
        let mut chunk_id = 0u64;
        while (chunk_id as usize) * ppc < total_pages {
            let start_page = (chunk_id as usize) * ppc;
            let end_page = (start_page + ppc).min(total_pages);
            let start = start_page * page_size;
            let end = end_page * page_size;
            let key = pages_chunk_key(&self.pages_prefix, chunk_id);
            store.write(&key, &bytes[start..end])?;
            self.remote_writes.fetch_add(1, Ordering::Relaxed);
            chunk_id += 1;
        }
        Ok(())
    }

    /// Flush primary file to durable storage.
    pub fn sync(&self) -> Result<()> {
        self.file.lock().sync_all()?;
        Ok(())
    }

    fn read_registered_page(
        &self,
        file_id: u32,
        page_index: u32,
        page: &mut Page,
    ) -> Result<()> {
        let path = self
            .files
            .lock()
            .get(&file_id)
            .cloned()
            .ok_or_else(|| TakyonicError::Engine(format!("unknown disk file id {file_id}")))?;
        let (mut file, _) = open_page_file(&path, false)?;
        let offset = page_index as u64 * self.page_size as u64;
        let file_len = file.metadata()?.len();
        page.data_mut().fill(0);
        if offset < file_len {
            let to_read = ((file_len - offset) as usize).min(self.page_size);
            // O_DIRECT requires full-page reads; read into the full frame then
            // rely on zeros beyond EOF (already filled).
            if to_read == self.page_size {
                file.seek(SeekFrom::Start(offset))?;
                file.read_exact(page.data_mut())?;
            } else {
                // Short tail: use buffered open for the partial page.
                let mut buf_file = OpenOptions::new().read(true).open(&path)?;
                buf_file.seek(SeekFrom::Start(offset))?;
                buf_file.read_exact(&mut page.data_mut()[..to_read])?;
            }
        }
        page.page_id = file_page_id(file_id, page_index);
        page.dirty = false;
        Ok(())
    }
}

/// Split a legacy V1 `pages/PAGES` blob into V2 chunks when chunks are absent.
fn migrate_v1_blob_if_needed(
    store: &dyn ObjectStorage,
    v1_key: &str,
    prefix: &str,
    page_size: usize,
    chunk_size: usize,
) -> Result<()> {
    let existing = store.list(prefix)?;
    let has_chunks = existing.iter().any(|k| k.contains("/chunk-"));
    if has_chunks {
        return Ok(());
    }
    let Ok(blob) = store.read_all(v1_key) else {
        return Ok(());
    };
    if blob.is_empty() {
        return Ok(());
    }
    let ppc = chunk_size / page_size;
    let total_pages = blob.len() / page_size;
    let mut chunk_id = 0u64;
    while (chunk_id as usize) * ppc < total_pages {
        let start_page = (chunk_id as usize) * ppc;
        let end_page = (start_page + ppc).min(total_pages);
        let start = start_page * page_size;
        let end = end_page * page_size;
        let key = pages_chunk_key(prefix, chunk_id);
        store.write(&key, &blob[start..end])?;
        chunk_id += 1;
    }
    // Keep V1 blob for readers that have not yet upgraded; new writes go to V2.
    Ok(())
}

fn remote_num_pages(
    store: &dyn ObjectStorage,
    v1_key: &str,
    prefix: &str,
    page_size: usize,
    chunk_size: usize,
) -> Result<u64> {
    let ppc = (chunk_size / page_size) as u64;
    let keys = store.list(prefix).unwrap_or_default();
    let mut max_pages = 0u64;
    for key in keys {
        if let Some(id_str) = key.rsplit("chunk-").next() {
            if let Ok(chunk_id) = id_str.parse::<u64>() {
                if let Ok(Some(len)) = store.len(&key) {
                    let pages_in = len / page_size as u64;
                    max_pages = max_pages.max(chunk_id * ppc + pages_in);
                }
            }
        }
    }
    if max_pages > 0 {
        return Ok(max_pages);
    }
    // Fall back to V1 blob length (pre-migration or empty prefix).
    if let Ok(Some(remote_len)) = store.len(v1_key) {
        return Ok(remote_len / page_size as u64);
    }
    Ok(0)
}

fn open_page_file(path: &Path, create: bool) -> Result<(File, bool)> {
    #[cfg(target_os = "linux")]
    {
        let mut opts = OpenOptions::new();
        opts.read(true);
        if create {
            opts.write(true).create(true);
        }
        opts.custom_flags(libc::O_DIRECT);
        match opts.open(path) {
            Ok(f) => return Ok((f, true)),
            Err(e) => {
                tracing::debug!(
                    path = %path.display(),
                    error = %e,
                    "O_DIRECT open failed; falling back to buffered I/O"
                );
            }
        }
    }
    let mut opts = OpenOptions::new();
    opts.read(true);
    if create {
        opts.write(true).create(true);
    }
    Ok((opts.open(path)?, false))
}

fn extend_file(file: &mut File, need: u64, page_size: usize) -> Result<()> {
    let len = file.metadata()?.len();
    if len >= need {
        return Ok(());
    }
    // Sparse grow (O(1)): avoid writing zeroes for every page up to a high
    // page_id — critical once remote chunks allow logical heaps ≫ RAM/disk.
    let aligned = need
        .div_ceil(page_size as u64)
        .saturating_mul(page_size as u64);
    file.set_len(aligned)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::object_store::{InMemoryObjectStore, ObjectStorage, S3Backend};
    use crate::page::DEFAULT_PAGE_SIZE;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("takyonic-disk-{name}-{nanos}"));
        std::fs::create_dir_all(&root).unwrap();
        root
    }

    #[test]
    fn read_write_roundtrip() {
        let root = temp_dir("rw");
        let dm = DiskManager::open(&root, DEFAULT_PAGE_SIZE).unwrap();
        let id = dm.allocate_page();
        let mut page = Page::new_aligned(DEFAULT_PAGE_SIZE);
        page.page_id = id;
        page.data_mut()[0] = 0xAB;
        page.data_mut()[4095] = 0xCD;
        dm.write_page(&page).unwrap();

        let mut loaded = Page::new_aligned(DEFAULT_PAGE_SIZE);
        dm.read_page(id, &mut loaded).unwrap();
        assert_eq!(loaded.data()[0], 0xAB);
        assert_eq!(loaded.data()[4095], 0xCD);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn s3_mock_shared_between_nodes() {
        let store = InMemoryObjectStore::new();
        let s3: Arc<dyn ObjectStorage> =
            Arc::new(S3Backend::mock("bucket", Arc::clone(&store) as Arc<dyn ObjectStorage>));

        let root_a = temp_dir("node-a");
        let dm_a =
            DiskManager::open_with_remote(&root_a, DEFAULT_PAGE_SIZE, Some(Arc::clone(&s3)), REMOTE_PAGES_KEY)
                .unwrap();
        let id = dm_a.allocate_page();
        let mut page = Page::new_aligned(DEFAULT_PAGE_SIZE);
        page.page_id = id;
        page.data_mut()[0] = 0x42;
        page.data_mut()[100] = 0x99;
        dm_a.write_page(&page).unwrap();
        assert!(dm_a.remote_writes() >= 1);

        // Separate node with empty local cache, same S3 mock.
        let root_b = temp_dir("node-b");
        let dm_b =
            DiskManager::open_with_remote(&root_b, DEFAULT_PAGE_SIZE, Some(s3), REMOTE_PAGES_KEY)
                .unwrap();
        let mut loaded = Page::new_aligned(DEFAULT_PAGE_SIZE);
        dm_b.read_page(id, &mut loaded).unwrap();
        assert_eq!(loaded.data()[0], 0x42);
        assert_eq!(loaded.data()[100], 0x99);
        assert!(dm_b.remote_fetches() >= 1);

        let _ = std::fs::remove_dir_all(root_a);
        let _ = std::fs::remove_dir_all(root_b);
    }

    #[test]
    fn cold_start_hydrates_from_object_storage() {
        let store = InMemoryObjectStore::new();
        let s3: Arc<dyn ObjectStorage> =
            Arc::new(S3Backend::mock("takyonic", Arc::clone(&store) as Arc<dyn ObjectStorage>));
        let root1 = temp_dir("warm");
        let dm = DiskManager::open_with_remote(
            &root1,
            DEFAULT_PAGE_SIZE,
            Some(Arc::clone(&s3)),
            REMOTE_PAGES_KEY,
        )
        .unwrap();
        let id = dm.allocate_page();
        let mut page = Page::new_aligned(DEFAULT_PAGE_SIZE);
        page.page_id = id;
        page.data_mut()[..4].copy_from_slice(b"COLD");
        dm.write_page(&page).unwrap();
        drop(dm);
        let _ = std::fs::remove_dir_all(&root1);

        // Brand-new empty local dir → must fetch from S3 mock.
        let root2 = temp_dir("cold");
        let dm2 =
            DiskManager::open_with_remote(&root2, DEFAULT_PAGE_SIZE, Some(s3), REMOTE_PAGES_KEY)
                .unwrap();
        let mut loaded = Page::new_aligned(DEFAULT_PAGE_SIZE);
        dm2.read_page(id, &mut loaded).unwrap();
        assert_eq!(&loaded.data()[..4], b"COLD");
        assert!(dm2.remote_fetches() >= 1);
        // Second read hits Tier-1 local cache.
        let fetches = dm2.remote_fetches();
        dm2.read_page(id, &mut loaded).unwrap();
        assert_eq!(dm2.remote_fetches(), fetches);
        let _ = std::fs::remove_dir_all(root2);
    }

    #[test]
    fn three_node_shared_object_store_restart_integrity() {
        use crate::bpm::BufferPoolManager;
        use crate::manifest::{ManifestManager, ManifestSst, StorageManifest};
        use crate::bpm::DEFAULT_LRU_K;

        let store = InMemoryObjectStore::new();
        let remote: Arc<dyn ObjectStorage> =
            Arc::new(S3Backend::mock("cluster", Arc::clone(&store) as Arc<dyn ObjectStorage>));

        // Node 1 writes pages + publishes manifest.
        let root1 = temp_dir("n1");
        let dm1 = Arc::new(
            DiskManager::open_with_remote(
                &root1,
                DEFAULT_PAGE_SIZE,
                Some(Arc::clone(&remote)),
                REMOTE_PAGES_KEY,
            )
            .unwrap(),
        );
        let bpm1 = BufferPoolManager::new(Arc::clone(&dm1), 8, DEFAULT_LRU_K).unwrap();
        let mut expected = Vec::new();
        for i in 0..6u8 {
            let g = bpm1.new_page().unwrap();
            let id = g.page_id();
            g.write(|d| {
                d[0] = i;
                d[1] = 0xFF - i;
            });
            expected.push((id, i));
            drop(g);
        }
        bpm1.flush_all().unwrap();
        dm1.sync_pages_to_remote().unwrap();

        let manifest_mgr =
            ManifestManager::open(Arc::clone(&remote)).unwrap();
        let mut m = StorageManifest::new();
        m.pages_key = REMOTE_PAGES_KEY.into();
        m.sstables.push(ManifestSst {
            id: 1,
            path: "sst/L0/1.sst".into(),
            level: 0,
        });
        // Also park a small SST blob for cross-node read.
        remote.write("sst/L0/1.sst", b"SST-PAYLOAD").unwrap();
        manifest_mgr.publish(m).unwrap();

        drop(bpm1);
        drop(dm1);
        let _ = std::fs::remove_dir_all(&root1);

        // Restart all 3 nodes with empty local disks, shared S3 mock.
        for node in 0..3 {
            let root = temp_dir(&format!("restart-{node}"));
            let dm = Arc::new(
                DiskManager::open_with_remote(
                    &root,
                    DEFAULT_PAGE_SIZE,
                    Some(Arc::clone(&remote)),
                    REMOTE_PAGES_KEY,
                )
                .unwrap(),
            );
            let bpm = BufferPoolManager::new(Arc::clone(&dm), 8, DEFAULT_LRU_K).unwrap();
            let mgr = ManifestManager::open(Arc::clone(&remote)).unwrap();
            let loaded = mgr.current();
            assert!(loaded.version >= 1);
            assert_eq!(loaded.sstables[0].path, "sst/L0/1.sst");
            assert_eq!(
                remote.read_all("sst/L0/1.sst").unwrap(),
                b"SST-PAYLOAD"
            );

            for (id, byte) in &expected {
                let g = bpm.fetch_page(*id).unwrap();
                assert_eq!(g.read(|d| d[0]), *byte, "node {node} page {id}");
                assert_eq!(g.read(|d| d[1]), 0xFF - *byte);
            }
            assert!(
                bpm.stats().remote_fetches >= 1 || dm.remote_fetches() >= 1,
                "node {node} must hydrate from object storage"
            );
            let _ = std::fs::remove_dir_all(root);
        }
    }

    #[test]
    fn coalesced_snapshots_one_put_per_chunk_preserves_siblings() {
        let chunk_pages = 2usize;
        let chunk_size = chunk_pages * DEFAULT_PAGE_SIZE;
        let store = InMemoryObjectStore::new();
        let remote: Arc<dyn ObjectStorage> = Arc::clone(&store) as Arc<dyn ObjectStorage>;
        let root = temp_dir("coalesce-siblings");
        let dm = DiskManager::open_with_remote_layout(
            &root,
            DEFAULT_PAGE_SIZE,
            Some(Arc::clone(&remote)),
            REMOTE_PAGES_KEY,
            PAGES_V2_PREFIX,
            chunk_size,
        )
        .unwrap();

        // Seed page 0 alone (chunk 0).
        let mut p0 = Page::new_aligned(DEFAULT_PAGE_SIZE);
        p0.page_id = 0;
        p0.data_mut()[0] = 0x11;
        dm.write_page(&p0).unwrap();

        store.reset_counters();
        // Coalesce page 1 + page 2 (chunks 0 and 1) in one call.
        let mut p1 = Page::new_aligned(DEFAULT_PAGE_SIZE);
        p1.page_id = 1;
        p1.data_mut()[0] = 0x22;
        let mut p2 = Page::new_aligned(DEFAULT_PAGE_SIZE);
        p2.page_id = 2;
        p2.data_mut()[0] = 0x33;
        dm.write_page_snapshots_coalesced(&[
            (p1.page_id, p1.data()),
            (p2.page_id, p2.data()),
        ])
        .unwrap();

        assert_eq!(
            store.write_ops(),
            2,
            "two chunks touched → two PutObjects, not three pages"
        );

        let mut loaded = Page::new_aligned(DEFAULT_PAGE_SIZE);
        dm.read_page(0, &mut loaded).unwrap();
        assert_eq!(loaded.data()[0], 0x11, "sibling page 0 must survive RMW");
        dm.read_page(1, &mut loaded).unwrap();
        assert_eq!(loaded.data()[0], 0x22);
        dm.read_page(2, &mut loaded).unwrap();
        assert_eq!(loaded.data()[0], 0x33);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn chunked_remote_write_uploads_only_touched_chunk() {
        // Tiny chunks (2 pages) so a far page_id would explode V1 blob size.
        let chunk_pages = 2usize;
        let chunk_size = chunk_pages * DEFAULT_PAGE_SIZE;
        let store = InMemoryObjectStore::new();
        let remote: Arc<dyn ObjectStorage> = Arc::clone(&store) as Arc<dyn ObjectStorage>;

        let root = temp_dir("chunk-bound");
        let dm = DiskManager::open_with_remote_layout(
            &root,
            DEFAULT_PAGE_SIZE,
            Some(Arc::clone(&remote)),
            REMOTE_PAGES_KEY,
            PAGES_V2_PREFIX,
            chunk_size,
        )
        .unwrap();

        let mut page0 = Page::new_aligned(DEFAULT_PAGE_SIZE);
        page0.page_id = 0;
        page0.data_mut()[0] = 0x11;
        dm.write_page(&page0).unwrap();

        store.reset_counters();
        let far_id: PageId = 100;
        let mut page = Page::new_aligned(DEFAULT_PAGE_SIZE);
        page.page_id = far_id;
        page.data_mut()[0] = 0xAB;
        dm.write_page(&page).unwrap();

        let uploaded = store.bytes_written();
        let v1_would_be = (far_id as usize + 1) * DEFAULT_PAGE_SIZE;
        assert!(
            uploaded <= chunk_size as u64,
            "chunked upload {uploaded} must be ≤ chunk_size {chunk_size}; V1 would upload ~{v1_would_be}"
        );
        assert!(
            uploaded < v1_would_be as u64 / 2,
            "upload {uploaded} should be far below V1 heap rewrite {v1_would_be}"
        );

        let root2 = temp_dir("chunk-cold");
        let dm2 = DiskManager::open_with_remote_layout(
            &root2,
            DEFAULT_PAGE_SIZE,
            Some(remote),
            REMOTE_PAGES_KEY,
            PAGES_V2_PREFIX,
            chunk_size,
        )
        .unwrap();
        let mut loaded = Page::new_aligned(DEFAULT_PAGE_SIZE);
        dm2.read_page(far_id, &mut loaded).unwrap();
        assert_eq!(loaded.data()[0], 0xAB);
        let _ = std::fs::remove_dir_all(root);
        let _ = std::fs::remove_dir_all(root2);
    }

    #[test]
    fn v1_blob_migrates_to_chunks_on_open() {
        let chunk_size = 2 * DEFAULT_PAGE_SIZE;
        let store = InMemoryObjectStore::new();
        let mut blob = vec![0u8; 3 * DEFAULT_PAGE_SIZE];
        blob[0] = 0xA1;
        blob[DEFAULT_PAGE_SIZE] = 0xA2;
        blob[2 * DEFAULT_PAGE_SIZE] = 0xA3;
        store.write(REMOTE_PAGES_KEY, &blob).unwrap();

        let root = temp_dir("migrate-v1");
        let dm = DiskManager::open_with_remote_layout(
            &root,
            DEFAULT_PAGE_SIZE,
            Some(Arc::clone(&store) as Arc<dyn ObjectStorage>),
            REMOTE_PAGES_KEY,
            PAGES_V2_PREFIX,
            chunk_size,
        )
        .unwrap();

        let chunks = store.list(PAGES_V2_PREFIX).unwrap();
        assert!(
            chunks.iter().any(|k| k.contains("chunk-")),
            "expected V2 chunks after migration, got {chunks:?}"
        );
        assert!(dm.num_pages() >= 3);

        for (id, expect) in [(0u64, 0xA1u8), (1, 0xA2), (2, 0xA3)] {
            let mut loaded = Page::new_aligned(DEFAULT_PAGE_SIZE);
            dm.read_page(id, &mut loaded).unwrap();
            assert_eq!(loaded.data()[0], expect, "page {id}");
        }
        let _ = std::fs::remove_dir_all(root);
    }
}
