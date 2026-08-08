//! Standalone LSM-Tree storage engine facade.
//!
//! Wraps the production [`Memtable`] + leveled [`SstManager`] +
//! [`CompactionEngine`] into an explicit `LSMStorage` API with atomic memtable
//! flush and an [`LSMReader`] that K-way-merges the memtable with on-disk SSTs
//! (Bloom-filtered point lookups).

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;
use tracing::debug;

use crate::compaction::{CompactionEngine, KWayMergeIterator, SstManager, SstMeta};
use crate::config::Config;
use crate::error::Result;
use crate::memtable::Memtable;
use crate::sst::{SstRegistry, SstWriter};
use crate::types::{CommitTs, Entry, Key, Value};

/// Background compaction worker (alias of the dual-pool [`CompactionEngine`]).
pub type CompactionManager = CompactionEngine;

/// Log-structured merge-tree store: memtable front-end + immutable SSTables.
pub struct LSMStorage {
    data_dir: PathBuf,
    memtable: Arc<Memtable>,
    /// Frozen memtable awaiting SST serialization (at most one).
    immutable: Mutex<Option<Arc<Memtable>>>,
    manager: Arc<SstManager>,
    compaction: Mutex<Option<CompactionManager>>,
    next_ts: AtomicU64,
    block_size: usize,
    memtable_size_bytes: usize,
}

impl LSMStorage {
    /// Open (or create) an LSM store under `data_dir` with default config knobs.
    pub fn open(data_dir: impl Into<PathBuf>) -> Result<Self> {
        let data_dir = data_dir.into();
        std::fs::create_dir_all(&data_dir)?;
        let cfg = Config::default()
            .data_dir(data_dir.clone())
            .wal_dir(data_dir.join("wal"))
            .memtable_size_bytes(64 * 1024 * 1024)
            .block_size_bytes(4 * 1024)
            .l0_rapid_pool_threads(1)
            .ln_haul_pool_threads(1)
            .compaction_write_bytes_per_sec(1024 * 1024 * 1024);
        Self::open_with_config(&cfg)
    }

    /// Open using engine [`Config`] (block size, memtable limit, compaction pools).
    pub fn open_with_config(config: &Config) -> Result<Self> {
        std::fs::create_dir_all(&config.data_dir)?;
        let registry = Arc::new(SstRegistry::new());
        let manager = Arc::new(SstManager::new(
            registry,
            config.data_dir.clone(),
            config.block_size_bytes,
            4,
            1,
        )?);
        recover_existing_ssts(&manager)?;
        let compaction = CompactionEngine::new(Arc::clone(&manager), config)?;
        Ok(Self {
            data_dir: config.data_dir.clone(),
            memtable: Arc::new(Memtable::new()),
            immutable: Mutex::new(None),
            manager,
            compaction: Mutex::new(Some(compaction)),
            next_ts: AtomicU64::new(1),
            block_size: config.block_size_bytes,
            memtable_size_bytes: config.memtable_size_bytes,
        })
    }

    /// Data directory root.
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// Shared memtable (active write buffer).
    pub fn memtable(&self) -> &Arc<Memtable> {
        &self.memtable
    }

    /// Leveled SST catalog.
    pub fn manager(&self) -> &Arc<SstManager> {
        &self.manager
    }

    /// Allocate the next commit timestamp.
    pub fn alloc_ts(&self) -> CommitTs {
        self.next_ts.fetch_add(1, Ordering::Relaxed)
    }

    /// Put at a fresh timestamp (may trigger auto-flush).
    pub fn put(&self, key: impl Into<Key>, value: impl Into<Value>) -> Result<CommitTs> {
        let ts = self.alloc_ts();
        self.memtable.apply(Entry::put(key, value, ts));
        self.maybe_flush()?;
        Ok(ts)
    }

    /// Delete at a fresh timestamp.
    pub fn delete(&self, key: impl Into<Key>) -> Result<CommitTs> {
        let ts = self.alloc_ts();
        self.memtable.apply(Entry::delete(key, ts));
        self.maybe_flush()?;
        Ok(ts)
    }

    /// Apply an entry with an explicit timestamp (MVCC / replay).
    pub fn apply(&self, entry: Entry) -> Result<()> {
        let _ = self
            .next_ts
            .fetch_max(entry.seq.saturating_add(1), Ordering::Relaxed);
        self.memtable.apply(entry);
        self.maybe_flush()?;
        Ok(())
    }

    /// Snapshot point lookup via [`LSMReader`].
    pub fn get_at(&self, key: &Key, read_ts: CommitTs) -> Result<Option<Entry>> {
        LSMReader::new(self)?.get_at(key, read_ts)
    }

    /// Latest live value.
    pub fn get(&self, key: &Key) -> Result<Option<Value>> {
        Ok(self.get_at(key, u64::MAX)?.and_then(|e| {
            if e.tombstone {
                None
            } else {
                e.value
            }
        }))
    }

    fn maybe_flush(&self) -> Result<()> {
        if self.memtable.approx_size_bytes() >= self.memtable_size_bytes {
            self.flush()?;
        }
        Ok(())
    }

    /// Freeze the active memtable and serialize it to an L0 SSTable.
    ///
    /// Concurrent writers continue into a fresh memtable after the freeze.
    pub fn flush(&self) -> Result<Option<u64>> {
        // Freeze: swap in an empty active table; serialize the old one.
        let frozen = {
            let mut imm = self.immutable.lock();
            if imm.is_some() {
                // Previous flush still in flight — force it first.
                drop(imm);
                self.flush_immutable()?;
                return self.flush();
            }
            if self.memtable.is_empty() {
                return Ok(None);
            }
            let frozen = Arc::new(Memtable::new());
            // Move entries: drain active into frozen by taking ownership.
            let entries = self.memtable.drain_entries();
            for e in entries {
                frozen.apply(e);
            }
            *imm = Some(Arc::clone(&frozen));
            frozen
        };
        let id = self.flush_memtable_to_l0(&frozen)?;
        *self.immutable.lock() = None;
        self.kick_compaction();
        Ok(Some(id))
    }

    fn flush_immutable(&self) -> Result<()> {
        let frozen = self.immutable.lock().clone();
        if let Some(frozen) = frozen {
            let _ = self.flush_memtable_to_l0(&frozen)?;
            *self.immutable.lock() = None;
            self.kick_compaction();
        }
        Ok(())
    }

    fn flush_memtable_to_l0(&self, mem: &Memtable) -> Result<u64> {
        let entries = mem.iter_entries();
        if entries.is_empty() {
            mem.clear();
            return Ok(0);
        }
        let smallest = entries.first().unwrap().key.clone();
        let largest = entries.last().unwrap().key.clone();
        let id = self.manager.allocate_sst_id();
        let path = self
            .manager
            .data_dir()
            .join("L0")
            .join(format!("{id:020}.sst"));
        std::fs::create_dir_all(path.parent().unwrap())?;
        let info = SstWriter::write(id, &path, &entries, self.block_size)?;
        let meta = SstMeta::from_info(0, info, smallest, largest)?;
        self.manager.add_sst(meta)?;
        mem.clear();
        debug!(sst_id = id, entries = entries.len(), "LSMStorage flushed memtable → L0");
        Ok(id)
    }

    fn kick_compaction(&self) {
        let guard = self.compaction.lock();
        let Some(engine) = guard.as_ref() else {
            return;
        };
        let _ = engine.submit_l0();
        let last = self.manager.level_count().saturating_sub(2);
        for level in 1..=last {
            let _ = engine.submit_ln(level);
        }
    }

    /// Drain pending compaction work (tests).
    pub fn drain_compaction(&self) -> Result<()> {
        // Compaction runs on background threads; give them a moment.
        std::thread::sleep(std::time::Duration::from_millis(50));
        Ok(())
    }

    /// Publish MVCC watermark for compaction GC.
    pub fn set_watermark(&self, watermark: CommitTs) {
        self.manager.set_mvcc_watermark(watermark);
    }

    /// Drop closed compaction pools.
    pub fn close(&self) -> Result<()> {
        let _ = self.flush();
        let _ = self.compaction.lock().take();
        Ok(())
    }
}

/// Snapshot reader: memtable(s) ⊕ leveled SSTs with Bloom short-circuit.
pub struct LSMReader<'a> {
    storage: &'a LSMStorage,
}

impl<'a> LSMReader<'a> {
    /// Borrow a reader against `storage`.
    pub fn new(storage: &'a LSMStorage) -> Result<Self> {
        Ok(Self { storage })
    }

    /// Point lookup at `read_ts` (Bloom filter skips cold SST files).
    pub fn get_at(&self, key: &Key, read_ts: CommitTs) -> Result<Option<Entry>> {
        let mut best: Option<Entry> = None;

        if let Some(e) = self.storage.memtable.get_at(key, read_ts) {
            best = Some(e);
        }
        if let Some(imm) = self.storage.immutable.lock().as_ref() {
            if let Some(e) = imm.get_at(key, read_ts) {
                match &best {
                    Some(b) if b.seq >= e.seq => {}
                    _ => best = Some(e),
                }
            }
        }

        let levels = self.storage.manager.level_count();
        for level in 0..levels {
            let mut files = self.storage.manager.level_files(level);
            if level == 0 {
                files.sort_by_key(|m| std::cmp::Reverse(m.id));
            }
            for meta in files {
                if level > 0 && (key < &meta.smallest || key > &meta.largest) {
                    continue;
                }
                let Some(pin) = self.storage.manager.registry().pin(meta.id) else {
                    continue;
                };
                // Bloom: definitive miss → skip block I/O.
                if !pin.reader().may_contain(key)? {
                    continue;
                }
                if let Some(entry) = pin.reader().get_entry_at(key, read_ts)? {
                    match &best {
                        Some(b) if b.seq >= entry.seq => {}
                        _ => best = Some(entry),
                    }
                }
            }
        }
        Ok(best)
    }

    /// K-way merge of memtable + all SST runs at `watermark` (full scan).
    pub fn merge_scan(&self, watermark: CommitTs) -> Result<Vec<Entry>> {
        let mut runs: Vec<Vec<Entry>> = Vec::new();
        runs.push(self.storage.memtable.iter_entries());
        if let Some(imm) = self.storage.immutable.lock().as_ref() {
            runs.push(imm.iter_entries());
        }
        for meta in self.storage.manager.all_files() {
            let Some(pin) = self.storage.manager.registry().pin(meta.id) else {
                continue;
            };
            let mut entries = Vec::new();
            for b in 0..pin.reader().block_count() {
                entries.extend(pin.reader().block_entries(b)?);
            }
            runs.push(entries);
        }
        let mut merge = KWayMergeIterator::from_sorted_runs(runs, watermark)?;
        let mut out = Vec::new();
        while let Some(e) = merge.next_entry()? {
            out.push(e);
        }
        Ok(out)
    }
}

fn recover_existing_ssts(manager: &SstManager) -> Result<()> {
    for level in 0..manager.level_count() {
        let dir = manager.data_dir().join(format!("L{level}"));
        if !dir.exists() {
            continue;
        }
        let mut files = Vec::new();
        for entry in std::fs::read_dir(&dir)? {
            let entry = entry?;
            let path = entry.path();
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            let Some(stem) = name.strip_suffix(".sst") else {
                continue;
            };
            if let Ok(id) = stem.parse::<u64>() {
                files.push((id, path));
            }
        }
        files.sort_by_key(|(id, _)| *id);
        for (id, path) in files {
            manager.recover_sst_file(level, id, path)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_lsm(name: &str) -> (LSMStorage, PathBuf) {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("takyonic-lsm-{name}-{nanos}"));
        let _ = std::fs::remove_dir_all(&root);
        let store = LSMStorage::open(&root).unwrap();
        (store, root)
    }

    #[test]
    fn sst_roundtrip_after_flush() {
        let (store, root) = temp_lsm("sst");
        store.put(&b"a"[..], &b"1"[..]).unwrap();
        store.put(&b"b"[..], &b"2"[..]).unwrap();
        let id = store.flush().unwrap().expect("flushed");
        assert!(id > 0);
        assert!(store.memtable().is_empty());
        assert_eq!(
            store.get(&Key::new(&b"a"[..])).unwrap().unwrap().as_bytes(),
            b"1"
        );
        assert_eq!(
            store.get(&Key::new(&b"b"[..])).unwrap().unwrap().as_bytes(),
            b"2"
        );
        // Bloom must not false-negative for present keys.
        let files = store.manager().level_files(0);
        assert!(!files.is_empty());
        let pin = store.manager().registry().pin(files[0].id).unwrap();
        assert!(pin.reader().may_contain(&Key::new(&b"a"[..])).unwrap());
        store.close().unwrap();
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn kway_merge_picks_latest_across_overlapping_ssts() {
        let runs = vec![
            vec![
                Entry::put(&b"k"[..], &b"old"[..], 1),
                Entry::put(&b"x"[..], &b"1"[..], 1),
            ],
            vec![
                Entry::put(&b"k"[..], &b"new"[..], 5),
                Entry::delete(&b"x"[..], 6),
            ],
            vec![Entry::put(&b"k"[..], &b"mid"[..], 3)],
        ];
        let mut merge = KWayMergeIterator::from_sorted_runs(runs, 100).unwrap();
        let mut by_key = std::collections::BTreeMap::new();
        while let Some(e) = merge.next_entry().unwrap() {
            by_key.insert(e.key.clone(), e);
        }
        let k = by_key.get(&Key::new(&b"k"[..])).unwrap();
        assert_eq!(k.seq, 5);
        assert_eq!(k.value.as_ref().unwrap().as_bytes(), b"new");
        // x deleted at ts=6 — tombstone kept as newest below watermark.
        let x = by_key.get(&Key::new(&b"x"[..])).unwrap();
        assert!(x.tombstone);
    }

    #[test]
    fn lsm_reader_merge_scan_after_multi_flush() {
        let (_store, root) = temp_lsm("merge");
        // Tiny memtable so each put flushes.
        let cfg = Config::default()
            .data_dir(root.join("data2"))
            .wal_dir(root.join("wal2"))
            .memtable_size_bytes(1) // force flush every write
            .block_size_bytes(64)
            .l0_rapid_pool_threads(1)
            .ln_haul_pool_threads(1)
            .compaction_write_bytes_per_sec(1024 * 1024 * 1024);
        let store = LSMStorage::open_with_config(&cfg).unwrap();
        store.put(&b"k"[..], &b"v1"[..]).unwrap();
        store.put(&b"k"[..], &b"v2"[..]).unwrap();
        store.put(&b"k"[..], &b"v3"[..]).unwrap();
        let _ = store.flush();
        let reader = LSMReader::new(&store).unwrap();
        let e = reader.get_at(&Key::new(&b"k"[..]), u64::MAX).unwrap().unwrap();
        assert_eq!(e.value.unwrap().as_bytes(), b"v3");
        store.close().unwrap();
        let _ = std::fs::remove_dir_all(root);
    }
}
