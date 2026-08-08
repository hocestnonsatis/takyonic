//! Buffer Pool Manager with scan-resistant LRU-K eviction.
//!
//! Frames are pre-allocated at construction. Callers pin pages while using them;
//! only unpinned frames may be evicted. Dirty victims are flushed via
//! [`DiskManager`] before reuse.
//!
//! **Two-tier I/O:** Tier-1 is the local NVMe/SSD page file (and in-memory frames).
//! On a pool miss, [`DiskManager::read_page`] may hydrate from Tier-2 remote
//! [`crate::object_store::ObjectStorage`] into the local cache before the frame
//! is filled — tracked as [`BpmStats::remote_fetches`].

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use parking_lot::{Mutex, RwLock};

use crate::disk::{DiskManager, is_file_cache_page};
use crate::error::{Result, TakyonicError};
use crate::page::{INVALID_PAGE_ID, Page, PageId};
use crate::telemetry::EngineMetrics;

/// Default K for LRU-K (track last K accesses).
pub const DEFAULT_LRU_K: usize = 2;

/// RAII pin guard — unpins on drop.
pub struct PageGuard {
    bpm: Arc<BufferPoolManager>,
    page_id: PageId,
    frame: usize,
}

impl PageGuard {
    /// Logical page id.
    #[inline]
    pub fn page_id(&self) -> PageId {
        self.page_id
    }

    /// Read page bytes under the buffer-pool lock.
    pub fn read<R>(&self, f: impl FnOnce(&[u8]) -> R) -> R {
        let frames = self.bpm.frames.read();
        let page = &frames[self.frame].page;
        debug_assert_eq!(
            page.page_id, self.page_id,
            "PageGuard frame recycled under pin"
        );
        f(page.data())
    }

    /// Mutate page bytes and mark the frame dirty.
    pub fn write<R>(&self, f: impl FnOnce(&mut [u8]) -> R) -> R {
        self.bpm.mark_dirty(self.page_id);
        let mut frames = self.bpm.frames.write();
        let page = &mut frames[self.frame].page;
        debug_assert_eq!(
            page.page_id, self.page_id,
            "PageGuard frame recycled under pin"
        );
        f(page.data_mut())
    }

    /// Explicitly mark dirty (transaction write-set / external mutation).
    pub fn set_dirty(&self) {
        self.bpm.mark_dirty(self.page_id);
    }
}

impl Drop for PageGuard {
    fn drop(&mut self) {
        let _ = self.bpm.unpin(self.page_id);
    }
}

struct Frame {
    page: Page,
    /// Access history (oldest → newest), at most `k` entries.
    history: VecDeque<u64>,
}

/// Pre-allocated buffer pool with LRU-K replacement.
///
/// # Lock order (must never invert)
///
/// When acquiring more than one of these locks, always use this hierarchy
/// (parent before child). Holding a child while waiting for a parent deadlocks.
///
/// 1. `page_table`
/// 2. `free_list` — never held together with `page_table`
/// 3. `frames` (`RwLock`)
///
/// Every path that touches both `page_table` and `frames` takes them as
/// `page_table` then `frames` (hit, miss publish, `new_page`, `unpin`,
/// `mark_dirty`, `flush_page`, `pin_count`, `evict_victim`). `allocate_frame`'s
/// free-list path takes `free_list` then `frames`, and drops both before
/// `evict_victim` acquires `page_table`. Disk I/O may run under `frames`
/// (and under `page_table`+`frames` on dirty eviction / `flush_page`) but
/// never re-enters BPM locks.
pub struct BufferPoolManager {
    disk: Arc<DiskManager>,
    k: usize,
    frames: RwLock<Vec<Frame>>,
    /// page_id → frame index
    page_table: Mutex<HashMap<PageId, usize>>,
    /// Free frame indices.
    free_list: Mutex<VecDeque<usize>>,
    /// Logical clock for access timestamps.
    clock: AtomicU64,
    /// Observability counters (local).
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
    flushes: AtomicU64,
    /// Tier-2 remote object-store hydrations observed on miss path.
    remote_fetches: AtomicU64,
    /// Optional shared engine metrics (dual-write).
    metrics: Option<Arc<EngineMetrics>>,
}

impl BufferPoolManager {
    /// Create a pool with `pool_size` frames backed by `disk`.
    pub fn new(disk: Arc<DiskManager>, pool_size: usize, k: usize) -> Result<Arc<Self>> {
        Self::new_inner(disk, pool_size, k, None)
    }

    /// Same as [`Self::new`], dual-writing counters into `metrics`.
    pub fn new_with_metrics(
        disk: Arc<DiskManager>,
        pool_size: usize,
        k: usize,
        metrics: Arc<EngineMetrics>,
    ) -> Result<Arc<Self>> {
        Self::new_inner(disk, pool_size, k, Some(metrics))
    }

    fn new_inner(
        disk: Arc<DiskManager>,
        pool_size: usize,
        k: usize,
        metrics: Option<Arc<EngineMetrics>>,
    ) -> Result<Arc<Self>> {
        if pool_size == 0 {
            return Err(TakyonicError::Config("buffer pool size must be > 0".into()));
        }
        if k == 0 {
            return Err(TakyonicError::Config("LRU-K k must be > 0".into()));
        }
        let page_size = disk.page_size();
        let mut frames = Vec::with_capacity(pool_size);
        let mut free = VecDeque::with_capacity(pool_size);
        for i in 0..pool_size {
            frames.push(Frame {
                page: Page::new_aligned(page_size),
                history: VecDeque::with_capacity(k),
            });
            free.push_back(i);
        }
        Ok(Arc::new(Self {
            disk,
            k,
            frames: RwLock::new(frames),
            page_table: Mutex::new(HashMap::new()),
            free_list: Mutex::new(free),
            clock: AtomicU64::new(1),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
            flushes: AtomicU64::new(0),
            remote_fetches: AtomicU64::new(0),
            metrics,
        }))
    }

    /// Number of frames in the pool.
    pub fn pool_size(&self) -> usize {
        self.frames.read().len()
    }

    /// Backing disk manager.
    pub fn disk(&self) -> &Arc<DiskManager> {
        &self.disk
    }

    /// Cache hit / miss / eviction / flush / remote-fetch counters.
    pub fn stats(&self) -> BpmStats {
        BpmStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            flushes: self.flushes.load(Ordering::Relaxed),
            remote_fetches: self.remote_fetches.load(Ordering::Relaxed),
        }
    }

    /// Fetch a page-aligned region from a registered secondary file into the pool.
    pub fn fetch_file_page(
        self: &Arc<Self>,
        file_id: u32,
        page_index: u32,
    ) -> Result<PageGuard> {
        let page_id = crate::disk::file_page_id(file_id, page_index);
        self.fetch_page(page_id)
    }

    /// Fetch `page_id` into the pool, pin it, and return a guard.
    pub fn fetch_page(self: &Arc<Self>, page_id: PageId) -> Result<PageGuard> {
        if page_id == INVALID_PAGE_ID {
            return Err(TakyonicError::Engine("cannot fetch invalid page id".into()));
        }

        loop {
            // Hit path: look up + pin under a consistent lock order
            // (page_table → frames) so eviction cannot recycle the frame between
            // the two steps.
            {
                let table = self.page_table.lock();
                if let Some(&frame) = table.get(&page_id) {
                    let mut frames = self.frames.write();
                    let f = &mut frames[frame];
                    if f.page.page_id != page_id {
                        // Stale table entry (should not happen after eviction fix);
                        // drop the mapping and fall through to a miss.
                        drop(frames);
                        drop(table);
                        self.page_table.lock().remove(&page_id);
                        continue;
                    }
                    f.page.pin_count = f.page.pin_count.saturating_add(1);
                    self.record_access(&mut f.history);
                    drop(frames);
                    drop(table);
                    self.note_hit();
                    return Ok(PageGuard {
                        bpm: Arc::clone(self),
                        page_id,
                        frame,
                    });
                }
            }

            self.note_miss();
            let remote_before = self.disk.remote_fetches();
            // `allocate_frame` returns a frame already claimed with pin_count=1.
            let frame = self.allocate_frame()?;

            // Disk I/O without holding the page table (other pages stay concurrent).
            // pin_count=1 keeps eviction from reclaiming this frame mid-read.
            {
                let mut frames = self.frames.write();
                let f = &mut frames[frame];
                debug_assert_eq!(f.page.pin_count, 1);
                debug_assert!(!f.page.is_occupied());
                self.disk.read_page(page_id, &mut f.page)?;
                f.page.pin_count = 1;
                f.page.dirty = false;
                f.history.clear();
                self.record_access(&mut f.history);
            }

            let remote_after = self.disk.remote_fetches();
            if remote_after > remote_before {
                self.remote_fetches
                    .fetch_add(remote_after - remote_before, Ordering::Relaxed);
            }

            // Publish, or lose the race to another miss that finished first.
            {
                let mut table = self.page_table.lock();
                if table.contains_key(&page_id) {
                    // Another thread installed this page — free our frame and retry hit.
                    drop(table);
                    {
                        let mut frames = self.frames.write();
                        frames[frame].page.reset();
                        frames[frame].history.clear();
                    }
                    self.free_list.lock().push_back(frame);
                    continue;
                }
                let mut frames = self.frames.write();
                let f = &mut frames[frame];
                debug_assert_eq!(f.page.page_id, page_id);
                debug_assert_eq!(f.page.pin_count, 1);
                table.insert(page_id, frame);
                drop(frames);
                drop(table);
            }

            return Ok(PageGuard {
                bpm: Arc::clone(self),
                page_id,
                frame,
            });
        }
    }

    /// Allocate a new page on disk, pin a zeroed frame, return guard.
    pub fn new_page(self: &Arc<Self>) -> Result<PageGuard> {
        let page_id = self.disk.allocate_page();
        let frame = self.allocate_frame()?;
        {
            let mut table = self.page_table.lock();
            let mut frames = self.frames.write();
            let f = &mut frames[frame];
            f.page.reset();
            f.page.page_id = page_id;
            f.page.pin_count = 1;
            f.page.dirty = true; // new page must be written eventually
            f.history.clear();
            self.record_access(&mut f.history);
            table.insert(page_id, frame);
        }
        Ok(PageGuard {
            bpm: Arc::clone(self),
            page_id,
            frame,
        })
    }

    /// Decrement pin count for `page_id`.
    pub fn unpin(&self, page_id: PageId) -> Result<()> {
        let table = self.page_table.lock();
        let Some(&frame) = table.get(&page_id) else {
            return Err(TakyonicError::Engine(format!(
                "unpin: page {page_id} not in buffer pool"
            )));
        };
        let mut frames = self.frames.write();
        let f = &mut frames[frame];
        if f.page.page_id != page_id {
            return Err(TakyonicError::Engine(
                "buffer pool page table desync".into(),
            ));
        }
        if f.page.pin_count == 0 {
            return Err(TakyonicError::Engine(format!(
                "unpin: page {page_id} already has pin_count 0"
            )));
        }
        f.page.pin_count -= 1;
        Ok(())
    }

    /// Mark a cached page dirty (transaction write-set / mutation).
    pub fn mark_dirty(&self, page_id: PageId) {
        let table = self.page_table.lock();
        if let Some(&frame) = table.get(&page_id) {
            let mut frames = self.frames.write();
            if frames[frame].page.page_id == page_id {
                frames[frame].page.dirty = true;
            }
        }
    }

    /// Flush all dirty pages to disk (checkpoint).
    ///
    /// When the disk manager has remote object storage, dirty pages are
    /// uploaded with chunk coalescing (one PutObject per touched V2 chunk).
    pub fn flush_all(&self) -> Result<()> {
        let snaps: Vec<(PageId, Vec<u8>)> = {
            let frames = self.frames.read();
            frames
                .iter()
                .filter(|f| f.page.is_occupied() && f.page.dirty)
                .map(|f| (f.page.page_id, f.page.data().to_vec()))
                .collect()
        };
        if !snaps.is_empty() {
            let t0 = Instant::now();
            let refs: Vec<(PageId, &[u8])> =
                snaps.iter().map(|(id, data)| (*id, data.as_slice())).collect();
            self.disk.write_page_snapshots_coalesced(&refs)?;
            let elapsed = t0.elapsed();
            for _ in 0..snaps.len() {
                self.note_flush(elapsed);
            }
        }
        let mut frames = self.frames.write();
        for f in frames.iter_mut() {
            if f.page.dirty {
                f.page.dirty = false;
            }
        }
        self.disk.sync()?;
        Ok(())
    }

    /// Flush a single dirty page if present.
    pub fn flush_page(&self, page_id: PageId) -> Result<()> {
        let table = self.page_table.lock();
        let Some(&frame) = table.get(&page_id) else {
            return Ok(());
        };
        let mut frames = self.frames.write();
        let f = &mut frames[frame];
        if f.page.page_id != page_id {
            return Ok(());
        }
        if f.page.dirty {
            let t0 = Instant::now();
            self.disk.write_page(&f.page)?;
            f.page.dirty = false;
            self.note_flush(t0.elapsed());
        }
        Ok(())
    }

    /// Whether `page_id` is currently resident.
    pub fn contains(&self, page_id: PageId) -> bool {
        self.page_table.lock().contains_key(&page_id)
    }

    /// Pin count for a resident page (`None` if not cached).
    pub fn pin_count(&self, page_id: PageId) -> Option<u32> {
        let table = self.page_table.lock();
        let frame = *table.get(&page_id)?;
        let frames = self.frames.read();
        let f = &frames[frame];
        if f.page.page_id != page_id {
            return None;
        }
        Some(f.page.pin_count)
    }

    fn record_access(&self, history: &mut VecDeque<u64>) {
        let now = self.clock.fetch_add(1, Ordering::Relaxed);
        if history.len() == self.k {
            history.pop_front();
        }
        history.push_back(now);
    }

    fn allocate_frame(&self) -> Result<usize> {
        // Claim a free frame under the frames lock so eviction cannot hand the
        // same index to another thread between pop and pin.
        {
            let mut free = self.free_list.lock();
            let mut frames = self.frames.write();
            while let Some(idx) = free.pop_front() {
                let f = &mut frames[idx];
                if f.page.pin_count == 0 && !f.page.is_occupied() {
                    f.page.pin_count = 1;
                    f.history.clear();
                    return Ok(idx);
                }
                // Stale free-list entry (frame already reused) — skip.
            }
        }
        self.evict_victim()
    }

    /// LRU-K victim: among unpinned frames, maximize backward K-distance.
    /// Pages with fewer than K accesses get infinite distance (prefer eviction)
    /// — this is what makes sequential scans fail to poison the cache.
    ///
    /// Lock order: `page_table` then `frames`. The victim is removed from the
    /// page table **before** the frame is reset so a concurrent hit cannot
    /// observe a stale frame index. The returned frame is claimed (`pin_count=1`).
    fn evict_victim(&self) -> Result<usize> {
        let mut table = self.page_table.lock();
        let now = self.clock.load(Ordering::Relaxed);
        let mut frames = self.frames.write();
        let mut best: Option<(usize, u64)> = None; // (frame, distance)

        for (idx, f) in frames.iter().enumerate() {
            if f.page.pin_count > 0 {
                continue;
            }
            if !f.page.is_occupied() {
                // Free frame not on free_list — claim it.
                frames[idx].page.pin_count = 1;
                frames[idx].history.clear();
                return Ok(idx);
            }
            let dist = backward_k_distance(&f.history, self.k, now);
            let replace = best
                .as_ref()
                .map(|(_, d)| dist > *d)
                .unwrap_or(true);
            if replace {
                best = Some((idx, dist));
            }
        }

        let Some((victim, _)) = best else {
            return Err(TakyonicError::Engine(
                "buffer pool exhausted: all pages are pinned".into(),
            ));
        };

        let page_id = frames[victim].page.page_id;
        if page_id != INVALID_PAGE_ID {
            table.remove(&page_id);
        }
        if frames[victim].page.dirty && !is_file_cache_page(page_id) {
            let t0 = Instant::now();
            self.disk.write_page(&frames[victim].page)?;
            frames[victim].page.dirty = false;
            self.note_flush(t0.elapsed());
        }
        frames[victim].page.reset();
        frames[victim].page.pin_count = 1; // claim for caller
        frames[victim].history.clear();
        drop(frames);
        drop(table);

        self.note_eviction();
        Ok(victim)
    }

    #[inline]
    fn note_hit(&self) {
        self.hits.fetch_add(1, Ordering::Relaxed);
        if let Some(m) = &self.metrics {
            m.record_bpm_hit();
        }
    }

    #[inline]
    fn note_miss(&self) {
        self.misses.fetch_add(1, Ordering::Relaxed);
        if let Some(m) = &self.metrics {
            m.record_bpm_miss();
        }
    }

    #[inline]
    fn note_eviction(&self) {
        self.evictions.fetch_add(1, Ordering::Relaxed);
        if let Some(m) = &self.metrics {
            m.record_bpm_eviction();
        }
    }

    #[inline]
    fn note_flush(&self, latency: std::time::Duration) {
        self.flushes.fetch_add(1, Ordering::Relaxed);
        if let Some(m) = &self.metrics {
            m.record_bpm_flush(latency);
        }
    }
}

fn backward_k_distance(history: &VecDeque<u64>, k: usize, now: u64) -> u64 {
    if history.len() < k {
        // Fewer than K references → treat as cold / scan traffic.
        u64::MAX
    } else {
        // history[0] is the K-th most recent access time.
        now.saturating_sub(history[0])
    }
}

/// Snapshot of BPM counters.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BpmStats {
    /// Pages served from memory.
    pub hits: u64,
    /// Pages loaded from disk / remote.
    pub misses: u64,
    /// Frames recycled via LRU-K.
    pub evictions: u64,
    /// Dirty pages written to disk.
    pub flushes: u64,
    /// Tier-2 object-store hydrations during misses.
    pub remote_fetches: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page::DEFAULT_PAGE_SIZE;
    use std::path::PathBuf;
    use std::sync::mpsc;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    fn temp_bpm(pool: usize) -> (Arc<BufferPoolManager>, PathBuf) {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("takyonic-bpm-{nanos}"));
        std::fs::create_dir_all(&root).unwrap();
        let disk = Arc::new(DiskManager::open(&root, DEFAULT_PAGE_SIZE).unwrap());
        let bpm = BufferPoolManager::new(disk, pool, DEFAULT_LRU_K).unwrap();
        (bpm, root)
    }

    #[test]
    fn unpin_allows_eviction_and_flushes_dirty() {
        let (bpm, root) = temp_bpm(2);
        let p0 = bpm.new_page().unwrap();
        let id0 = p0.page_id();
        p0.write(|d| d[0] = 42);
        drop(p0); // unpin

        let p1 = bpm.new_page().unwrap();
        let id1 = p1.page_id();
        p1.write(|d| d[0] = 7);
        drop(p1);

        // Pool full — next new page must evict one (both unpinned).
        let p2 = bpm.new_page().unwrap();
        assert!(!bpm.contains(id0) || !bpm.contains(id1));
        drop(p2);

        // Dirty victim was flushed: re-fetch should see data.
        if !bpm.contains(id0) {
            let g = bpm.fetch_page(id0).unwrap();
            assert_eq!(g.read(|d| d[0]), 42);
        } else {
            let g = bpm.fetch_page(id1).unwrap();
            assert_eq!(g.read(|d| d[0]), 7);
        }
        assert!(bpm.stats().evictions >= 1);
        assert!(bpm.stats().flushes >= 1);
        let _ = std::fs::remove_dir_all(root);
    }

    /// Faz 1A: N dirty pages in C chunks → C PutObjects on flush_all (not N).
    #[test]
    fn coalesced_flush_all_uploads_once_per_chunk() {
        use crate::disk::{PAGES_V2_PREFIX, REMOTE_PAGES_KEY};
        use crate::object_store::InMemoryObjectStore;

        let chunk_pages = 4usize;
        let chunk_size = chunk_pages * DEFAULT_PAGE_SIZE;
        let store = InMemoryObjectStore::new();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("takyonic-bpm-coalesce-{nanos}"));
        std::fs::create_dir_all(&root).unwrap();
        let disk = Arc::new(
            DiskManager::open_with_remote_layout(
                &root,
                DEFAULT_PAGE_SIZE,
                Some(Arc::clone(&store) as Arc<dyn crate::object_store::ObjectStorage>),
                REMOTE_PAGES_KEY,
                PAGES_V2_PREFIX,
                chunk_size,
            )
            .unwrap(),
        );
        // Pool large enough to hold all dirty pages without eviction mid-setup.
        let bpm = BufferPoolManager::new(Arc::clone(&disk), 16, DEFAULT_LRU_K).unwrap();

        // 8 pages → 2 chunks (0..3 and 4..7).
        let n_pages = 8usize;
        let mut guards = Vec::new();
        for i in 0..n_pages {
            let g = bpm.new_page().unwrap();
            assert_eq!(g.page_id(), i as u64);
            let byte = 0xA0 + i as u8;
            g.write(|d| d[0] = byte);
            guards.push(g);
        }
        drop(guards); // unpin but keep dirty + resident

        store.reset_counters();
        bpm.flush_all().unwrap();

        let writes = store.write_ops();
        let chunks = 2u64;
        assert_eq!(
            writes, chunks,
            "flush_all must upload once per chunk ({chunks}), not per page ({n_pages}); got {writes}"
        );
        assert!(
            writes < n_pages as u64,
            "coalesce must beat per-page write-through"
        );

        // Cold read via new DiskManager must see all bytes.
        let root2 = std::env::temp_dir().join(format!("takyonic-bpm-coalesce-cold-{nanos}"));
        std::fs::create_dir_all(&root2).unwrap();
        let dm2 = DiskManager::open_with_remote_layout(
            &root2,
            DEFAULT_PAGE_SIZE,
            Some(Arc::clone(&store) as Arc<dyn crate::object_store::ObjectStorage>),
            REMOTE_PAGES_KEY,
            PAGES_V2_PREFIX,
            chunk_size,
        )
        .unwrap();
        for i in 0..n_pages {
            let mut loaded = Page::new_aligned(DEFAULT_PAGE_SIZE);
            dm2.read_page(i as u64, &mut loaded).unwrap();
            assert_eq!(loaded.data()[0], 0xA0 + i as u8, "page {i}");
        }
        let _ = std::fs::remove_dir_all(root);
        let _ = std::fs::remove_dir_all(root2);
    }

    #[test]
    fn pinned_page_never_evicted() {
        let (bpm, root) = temp_bpm(2);
        let hot = bpm.new_page().unwrap();
        let hot_id = hot.page_id();
        // Keep hot pinned.
        let _cold1 = bpm.new_page().unwrap();
        // Third allocation must fail or evict cold — hot stays.
        let r = bpm.new_page();
        match r {
            Ok(g) => {
                assert!(bpm.contains(hot_id));
                assert!(bpm.pin_count(hot_id).unwrap() >= 1);
                drop(g);
            }
            Err(e) => {
                // All pinned (hot + cold1) → exhausted.
                assert!(e.to_string().contains("pinned") || e.to_string().contains("exhausted"));
            }
        }
        drop(hot);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn lru_k_scan_resistance_keeps_hot_page() {
        let (bpm, root) = temp_bpm(4);
        // Create 4 pages and establish hot page with many accesses (K=2).
        let mut ids = Vec::new();
        for _ in 0..4 {
            let g = bpm.new_page().unwrap();
            ids.push(g.page_id());
            drop(g);
        }
        let hot = ids[0];
        for _ in 0..20 {
            let g = bpm.fetch_page(hot).unwrap();
            drop(g);
        }

        // Sequential flood: allocate many new pages (scan).
        for _ in 0..32 {
            let g = bpm.new_page().unwrap();
            drop(g); // single access → cold under LRU-K
        }

        assert!(
            bpm.contains(hot),
            "hot page must survive sequential scan flood"
        );
        let _ = std::fs::remove_dir_all(root);
    }

    /// Concurrent fetch + eviction must not desync the page table or return
    /// another page's bytes (the crash_recovery BPM race).
    #[test]
    fn concurrent_fetch_with_eviction_preserves_page_identity() {
        let (bpm, root) = temp_bpm(8);
        let mut ids = Vec::new();
        for i in 0..32u8 {
            let g = bpm.new_page().unwrap();
            g.write(|d| {
                d[0] = i;
                d[1] = 0xA5;
            });
            ids.push(g.page_id());
            drop(g);
        }

        let n_threads = 8;
        let iters = 2_500;
        let barrier = Arc::new(std::sync::Barrier::new(n_threads));
        let mut handles = Vec::with_capacity(n_threads);
        for t in 0..n_threads {
            let bpm = Arc::clone(&bpm);
            let ids = ids.clone();
            let barrier = Arc::clone(&barrier);
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                for i in 0..iters {
                    let idx = (t.wrapping_mul(17).wrapping_add(i)) % ids.len();
                    let id = ids[idx];
                    let expected = idx as u8;
                    let g = bpm
                        .fetch_page(id)
                        .unwrap_or_else(|e| panic!("fetch page {id}: {e}"));
                    let (marker, magic) = g.read(|d| (d[0], d[1]));
                    assert_eq!(
                        (marker, magic),
                        (expected, 0xA5),
                        "frame returned wrong page bytes for id={id}"
                    );
                    drop(g);
                }
            }));
        }
        for h in handles {
            h.join().expect("worker panicked");
        }
        let _ = std::fs::remove_dir_all(root);
    }

    /// Hang/deadlock detector for hit + allocate + evict under high concurrency.
    ///
    /// Runs the workload on a background thread and **fails** if it has not
    /// finished within [`DEADLOCK_TIMEOUT`] (as opposed to hanging until the
    /// test harness times out). Style mirrors the 50×12-writer crash_recovery
    /// stress: small pool, large working set, many concurrent fetchers.
    #[test]
    fn high_concurrency_hit_evict_allocate_completes_within_timeout() {
        const DEADLOCK_TIMEOUT: Duration = Duration::from_secs(60);
        // Pool must be ≥ thread count so simultaneous pins cannot exhaust the
        // pool (that is a capacity error, not a deadlock). Pages ≫ pool so
        // allocate/evict still runs under contention.
        const N_THREADS: usize = 12;
        const POOL: usize = 16;
        const PAGES: usize = 64;
        const ITERS: usize = 8_000;

        let (done_tx, done_rx) = mpsc::channel();
        std::thread::spawn(move || {
            let result = std::panic::catch_unwind(|| {
                let (bpm, root) = temp_bpm(POOL);
                let mut ids = Vec::with_capacity(PAGES);
                for i in 0..PAGES {
                    let g = bpm.new_page().unwrap();
                    g.write(|d| {
                        d[0] = (i % 256) as u8;
                        d[1] = 0x5A;
                    });
                    ids.push(g.page_id());
                    drop(g);
                }

                let barrier = Arc::new(std::sync::Barrier::new(N_THREADS));
                let mut handles = Vec::with_capacity(N_THREADS);
                for t in 0..N_THREADS {
                    let bpm = Arc::clone(&bpm);
                    let ids = ids.clone();
                    let barrier = Arc::clone(&barrier);
                    handles.push(std::thread::spawn(move || {
                        barrier.wait();
                        for i in 0..ITERS {
                            // Mix hit-heavy and eviction-forcing access patterns.
                            let idx = if i % 7 == 0 {
                                // Cold-ish: spread across the full working set.
                                (t.wrapping_mul(31).wrapping_add(i)) % ids.len()
                            } else {
                                // Hot subset: keep a few pages pinned in the race.
                                (t.wrapping_mul(3).wrapping_add(i / 4)) % (ids.len().min(16))
                            };
                            let id = ids[idx];
                            let g = bpm
                                .fetch_page(id)
                                .unwrap_or_else(|e| panic!("fetch page {id}: {e}"));
                            let (marker, magic) = g.read(|d| (d[0], d[1]));
                            assert_eq!(
                                (marker, magic),
                                ((idx % 256) as u8, 0x5A),
                                "wrong page bytes under concurrent hit/evict"
                            );
                            // Occasionally dirty so eviction must flush under locks.
                            if i % 11 == 0 {
                                g.write(|d| d[2] = d[2].wrapping_add(1));
                            }
                            drop(g);
                            if i % 53 == 0 {
                                let _ = bpm.flush_page(id);
                            }
                        }
                    }));
                }
                for h in handles {
                    h.join().expect("worker panicked");
                }
                // Also exercise allocate while the pool is full (evict path).
                for _ in 0..POOL * 4 {
                    let g = bpm.new_page().unwrap();
                    drop(g);
                }
                let _ = std::fs::remove_dir_all(root);
            });
            let _ = done_tx.send(result);
        });

        match done_rx.recv_timeout(DEADLOCK_TIMEOUT) {
            Ok(Ok(())) => {}
            Ok(Err(payload)) => std::panic::resume_unwind(payload),
            Err(mpsc::RecvTimeoutError::Timeout) => {
                panic!(
                    "BPM hang/deadlock: hit/evict/allocate workload did not complete within {DEADLOCK_TIMEOUT:?}"
                );
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                panic!("BPM hang/deadlock worker disconnected without finishing");
            }
        }
    }
}
