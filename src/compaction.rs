//! Leveled compaction with optimistic concurrency and physically separate pools.
//!
//! The critical protocol is:
//! 1. Pick inputs and reserve every participating [`SstId`] under the catalog lock.
//! 2. Pin, read, merge, pace I/O, and build the output without that lock.
//! 3. Reacquire the lock, verify reservations, and atomically install metadata.
//!
//! L0→L1 and L1→L2+ jobs travel through separate bounded channels and
//! long-lived worker sets, avoiding head-of-line blocking between urgent L0
//! reduction and heavy background compactions.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, Sender, TrySendError};
use parking_lot::{Condvar, Mutex};
use tracing::{debug, info_span};

use crate::config::Config;
use crate::error::{Result, TakyonicError};
use crate::sst::{DeleteStatus, SstId, SstInfo, SstPin, SstRegistry, SstWriter};
use crate::types::{Entry, Key};

/// Catalog metadata for one immutable SST.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SstMeta {
    /// Stable SST identifier.
    pub id: SstId,
    /// Current LSM level.
    pub level: usize,
    /// Immutable file path.
    pub path: PathBuf,
    /// Smallest user key in the file.
    pub smallest: Key,
    /// Largest user key in the file.
    pub largest: Key,
    /// Physical file size.
    pub file_size: u64,
}

impl SstMeta {
    /// Build catalog metadata from writer output and known key bounds.
    pub fn from_info(level: usize, info: SstInfo, smallest: Key, largest: Key) -> Result<Self> {
        if smallest > largest {
            return Err(TakyonicError::Compaction(
                "SST smallest key exceeds largest key".into(),
            ));
        }
        if info.entry_count == 0 {
            return Err(TakyonicError::Compaction(
                "empty SSTs are not added to the leveled catalog".into(),
            ));
        }
        Ok(Self {
            id: info.id,
            level,
            path: info.path,
            smallest,
            largest,
            file_size: info.file_size,
        })
    }

    fn overlaps(&self, smallest: &Key, largest: &Key) -> bool {
        self.smallest <= *largest && self.largest >= *smallest
    }
}

#[derive(Default)]
struct CatalogState {
    levels: Vec<Vec<SstMeta>>,
    reserved: HashSet<SstId>,
}

#[derive(Clone, Debug)]
struct CompactionPlan {
    source_level: usize,
    output_level: usize,
    inputs: Vec<SstMeta>,
    reserved: HashSet<SstId>,
    output_id: SstId,
}

/// Thread-safe leveled SST catalog and OCC reservation owner.
pub struct SstManager {
    state: Mutex<CatalogState>,
    registry: Arc<SstRegistry>,
    data_dir: PathBuf,
    block_size: usize,
    next_id: AtomicU64,
    l0_generation: Mutex<u64>,
    l0_changed: Condvar,
    /// Oldest active transaction read_ts — compaction may drop shadowed
    /// versions strictly older than this watermark.
    mvcc_watermark: AtomicU64,
}

impl SstManager {
    /// Create a catalog with `level_count` levels.
    pub fn new(
        registry: Arc<SstRegistry>,
        data_dir: impl Into<PathBuf>,
        block_size: usize,
        level_count: usize,
        next_sst_id: SstId,
    ) -> Result<Self> {
        if level_count < 2 {
            return Err(TakyonicError::Config(
                "compaction requires at least two levels".into(),
            ));
        }
        if block_size == 0 {
            return Err(TakyonicError::Config(
                "compaction block_size must be > 0".into(),
            ));
        }
        Ok(Self {
            state: Mutex::new(CatalogState {
                levels: vec![Vec::new(); level_count],
                reserved: HashSet::new(),
            }),
            registry,
            data_dir: data_dir.into(),
            block_size,
            next_id: AtomicU64::new(next_sst_id),
            l0_generation: Mutex::new(0),
            l0_changed: Condvar::new(),
            mvcc_watermark: AtomicU64::new(0),
        })
    }

    /// Publish the MVCC GC watermark (oldest active transaction `read_ts`).
    pub fn set_mvcc_watermark(&self, watermark: u64) {
        self.mvcc_watermark
            .store(watermark, AtomicOrdering::Release);
    }

    /// Current MVCC GC watermark.
    pub fn mvcc_watermark(&self) -> u64 {
        self.mvcc_watermark.load(AtomicOrdering::Acquire)
    }

    /// Register an existing immutable SST and add it to the level catalog.
    pub fn add_sst(&self, meta: SstMeta) -> Result<()> {
        let mut state = self.state.lock();
        if meta.level >= state.levels.len() {
            return Err(TakyonicError::Compaction(format!(
                "level {} is out of range",
                meta.level
            )));
        }
        if state.levels.iter().flatten().any(|file| file.id == meta.id) {
            return Err(TakyonicError::Compaction(format!(
                "SST {} already exists in catalog",
                meta.id
            )));
        }
        if meta.level > 0
            && state.levels[meta.level]
                .iter()
                .any(|file| file.overlaps(&meta.smallest, &meta.largest))
        {
            return Err(TakyonicError::Compaction(format!(
                "SST {} overlaps an existing file in level {}",
                meta.id, meta.level
            )));
        }
        self.registry.register(meta.id, &meta.path)?;
        let level = meta.level;
        state.levels[level].push(meta);
        sort_level(&mut state.levels[level], level);
        drop(state);
        if level == 0 {
            self.notify_l0_changed();
        }
        Ok(())
    }

    /// Snapshot metadata for a level.
    pub fn level_files(&self, level: usize) -> Vec<SstMeta> {
        self.state
            .lock()
            .levels
            .get(level)
            .cloned()
            .unwrap_or_default()
    }

    /// Snapshot the currently OCC-reserved file identifiers.
    pub fn reserved_ids(&self) -> HashSet<SstId> {
        self.state.lock().reserved.clone()
    }

    /// Current number of L0 files used by write admission control.
    pub fn l0_file_count(&self) -> usize {
        self.state.lock().levels[0].len()
    }

    /// Shared mmap registry used by read and compaction paths.
    pub fn registry(&self) -> &Arc<SstRegistry> {
        &self.registry
    }

    /// Allocate the next immutable SST identifier (flush / compaction outputs).
    pub fn allocate_sst_id(&self) -> SstId {
        self.next_id.fetch_add(1, AtomicOrdering::Relaxed)
    }

    /// Configured data-block size used when writing SSTs.
    #[inline]
    pub fn block_size(&self) -> usize {
        self.block_size
    }

    /// Root directory that stores leveled SST files.
    #[inline]
    pub fn data_dir(&self) -> &std::path::Path {
        &self.data_dir
    }

    /// Remove every SST from the catalog and registry (best-effort unlink).
    pub fn clear_all(&self) -> Result<()> {
        let metas: Vec<SstMeta> = {
            let state = self.state.lock();
            state.levels.iter().flatten().cloned().collect()
        };
        {
            let mut state = self.state.lock();
            for level in &mut state.levels {
                level.clear();
            }
            state.reserved.clear();
        }
        for meta in metas {
            let _ = self.registry.retire(meta.id);
        }
        self.notify_l0_changed();
        Ok(())
    }

    /// Wipe the catalog and install a fresh set of SSTs (snapshot apply).
    pub fn replace_all(&self, metas: Vec<SstMeta>) -> Result<()> {
        self.clear_all()?;
        let mut max_id = 0u64;
        for meta in metas {
            max_id = max_id.max(meta.id);
            self.add_sst(meta)?;
        }
        self.ensure_next_sst_id_at_least(max_id.saturating_add(1));
        Ok(())
    }

    /// Flat list of every SST currently in the catalog.
    pub fn all_files(&self) -> Vec<SstMeta> {
        let state = self.state.lock();
        state.levels.iter().flatten().cloned().collect()
    }

    /// Number of LSM levels in the catalog.
    pub fn level_count(&self) -> usize {
        self.state.lock().levels.len()
    }

    /// Ensure subsequent [`Self::allocate_sst_id`] calls stay above `min_next`.
    pub fn ensure_next_sst_id_at_least(&self, min_next: SstId) {
        let mut cur = self.next_id.load(AtomicOrdering::Relaxed);
        while cur < min_next {
            match self.next_id.compare_exchange_weak(
                cur,
                min_next,
                AtomicOrdering::Relaxed,
                AtomicOrdering::Relaxed,
            ) {
                Ok(_) => break,
                Err(observed) => cur = observed,
            }
        }
    }

    /// Register an on-disk SST discovered during engine recovery.
    pub fn recover_sst_file(&self, level: usize, id: SstId, path: PathBuf) -> Result<()> {
        let file_size = std::fs::metadata(&path)?.len();
        self.registry.register(id, &path)?;
        let pin = self.registry.pin(id).ok_or_else(|| {
            TakyonicError::Compaction(format!("recovered SST {id} could not be pinned"))
        })?;
        let entries = pin.reader().entries()?;
        let smallest = entries
            .first()
            .ok_or_else(|| TakyonicError::Compaction(format!("recovered SST {id} is empty")))?
            .key
            .clone();
        let largest = entries.last().expect("non-empty").key.clone();
        drop(pin);

        let mut state = self.state.lock();
        if level >= state.levels.len() {
            return Err(TakyonicError::Compaction(format!(
                "level {level} is out of range"
            )));
        }
        if state.levels.iter().flatten().any(|file| file.id == id) {
            return Err(TakyonicError::Compaction(format!(
                "SST {id} already exists in catalog"
            )));
        }
        if level > 0
            && state.levels[level]
                .iter()
                .any(|file| file.overlaps(&smallest, &largest))
        {
            return Err(TakyonicError::Compaction(format!(
                "SST {id} overlaps an existing file in level {level}"
            )));
        }
        state.levels[level].push(SstMeta {
            id,
            level,
            path,
            smallest,
            largest,
            file_size,
        });
        sort_level(&mut state.levels[level], level);
        drop(state);
        if level == 0 {
            self.notify_l0_changed();
        }
        self.ensure_next_sst_id_at_least(id.saturating_add(1));
        Ok(())
    }

    fn pick(&self, source_level: usize) -> Result<Option<CompactionPlan>> {
        let mut state = self.state.lock();
        if source_level + 1 >= state.levels.len() {
            return Err(TakyonicError::Compaction(format!(
                "cannot compact terminal level {source_level}"
            )));
        }

        let source_files = state.levels[source_level].clone();
        for candidate in source_files {
            if state.reserved.contains(&candidate.id) {
                continue;
            }

            let mut selected_source = vec![candidate.clone()];
            let mut smallest = candidate.smallest.clone();
            let mut largest = candidate.largest.clone();

            // L0 ranges may overlap. Select the transitive overlap closure so
            // concurrent plans cannot create overlapping L1 outputs when L1 is empty.
            if source_level == 0 {
                loop {
                    let mut changed = false;
                    for file in &state.levels[0] {
                        if selected_source.iter().any(|picked| picked.id == file.id) {
                            continue;
                        }
                        if file.overlaps(&smallest, &largest) {
                            if state.reserved.contains(&file.id) {
                                selected_source.clear();
                                break;
                            }
                            smallest = smallest.min(file.smallest.clone());
                            largest = largest.max(file.largest.clone());
                            selected_source.push(file.clone());
                            changed = true;
                        }
                    }
                    if selected_source.is_empty() || !changed {
                        break;
                    }
                }
                if selected_source.is_empty() {
                    continue;
                }
            }

            let target: Vec<_> = state.levels[source_level + 1]
                .iter()
                .filter(|file| file.overlaps(&smallest, &largest))
                .cloned()
                .collect();
            if target.iter().any(|file| state.reserved.contains(&file.id)) {
                continue;
            }

            let mut inputs = selected_source;
            inputs.extend(target);
            let reserved: HashSet<_> = inputs.iter().map(|file| file.id).collect();
            state.reserved.extend(reserved.iter().copied());
            let output_id = self.next_id.fetch_add(1, AtomicOrdering::Relaxed);
            debug!(
                source_level,
                output_level = source_level + 1,
                output_id,
                inputs = inputs.len(),
                "compaction picked and reserved"
            );
            return Ok(Some(CompactionPlan {
                source_level,
                output_level: source_level + 1,
                inputs,
                reserved,
                output_id,
            }));
        }
        Ok(None)
    }

    fn abort(&self, plan: &CompactionPlan) {
        let mut state = self.state.lock();
        for id in &plan.reserved {
            state.reserved.remove(id);
        }
    }

    fn install(&self, plan: &CompactionPlan, output: SstMeta) -> Result<()> {
        let mut state = self.state.lock();
        if !plan.reserved.iter().all(|id| state.reserved.contains(id)) {
            return Err(TakyonicError::Compaction(
                "reservation lost before compaction install".into(),
            ));
        }
        let present: HashSet<_> = state
            .levels
            .iter()
            .flatten()
            .filter(|file| plan.reserved.contains(&file.id))
            .map(|file| file.id)
            .collect();
        if present != plan.reserved {
            return Err(TakyonicError::Compaction(
                "compaction inputs changed before install".into(),
            ));
        }
        if output.level != plan.output_level {
            return Err(TakyonicError::Compaction(
                "compaction output level does not match plan".into(),
            ));
        }
        if state.levels[output.level].iter().any(|file| {
            !plan.reserved.contains(&file.id) && file.overlaps(&output.smallest, &output.largest)
        }) {
            return Err(TakyonicError::Compaction(
                "compaction output overlaps an unreserved target file".into(),
            ));
        }

        for level in &mut state.levels {
            level.retain(|file| !plan.reserved.contains(&file.id));
        }
        let output_level = output.level;
        state.levels[output_level].push(output);
        sort_level(&mut state.levels[output_level], output_level);
        for id in &plan.reserved {
            state.reserved.remove(id);
        }
        drop(state);
        if plan.source_level == 0 {
            self.notify_l0_changed();
        }
        Ok(())
    }

    fn output_path(&self, level: usize, id: SstId) -> PathBuf {
        self.data_dir
            .join(format!("L{level}"))
            .join(format!("{id:020}.sst"))
    }

    pub(crate) fn l0_generation(&self) -> u64 {
        *self.l0_generation.lock()
    }

    pub(crate) fn wait_for_l0_change(&self, observed: u64, timeout: Duration) -> u64 {
        let mut generation = self.l0_generation.lock();
        if *generation == observed {
            self.l0_changed.wait_for(&mut generation, timeout);
        }
        *generation
    }

    fn notify_l0_changed(&self) {
        let mut generation = self.l0_generation.lock();
        *generation = generation.wrapping_add(1);
        self.l0_changed.notify_all();
    }
}

fn sort_level(files: &mut [SstMeta], level: usize) {
    if level == 0 {
        files.sort_by_key(|file| file.id);
    } else {
        files.sort_by(|left, right| left.smallest.cmp(&right.smallest));
    }
}

#[derive(Eq, PartialEq)]
struct HeapItem {
    entry: Entry,
    source: usize,
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .entry
            .key
            .cmp(&self.entry.key)
            .then_with(|| self.entry.seq.cmp(&other.entry.seq))
            .then_with(|| self.source.cmp(&other.source))
    }
}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

trait EntrySource {
    fn next_entry(&mut self) -> Result<Option<Entry>>;
}

struct SstCursor {
    pin: SstPin,
    next_block: usize,
    entries: std::vec::IntoIter<Entry>,
}

impl SstCursor {
    fn new(pin: SstPin) -> Self {
        Self {
            pin,
            next_block: 0,
            entries: Vec::new().into_iter(),
        }
    }
}

impl EntrySource for SstCursor {
    fn next_entry(&mut self) -> Result<Option<Entry>> {
        loop {
            if let Some(entry) = self.entries.next() {
                return Ok(Some(entry));
            }
            if self.next_block >= self.pin.reader().block_count() {
                return Ok(None);
            }
            let block = self.pin.reader().block_entries(self.next_block)?;
            self.next_block += 1;
            self.entries = block.into_iter();
        }
    }
}

#[cfg(test)]
struct VecSource(std::vec::IntoIter<Entry>);

#[cfg(test)]
impl EntrySource for VecSource {
    fn next_entry(&mut self) -> Result<Option<Entry>> {
        Ok(self.0.next())
    }
}

/// K-way merge over block-at-a-time pinned SST streams.
///
/// Emits every version that must remain visible given `watermark`:
/// - all versions with `commit_ts >= watermark`
/// - plus the newest version with `commit_ts < watermark` (snapshot floor)
///
/// Shadowed versions older than the watermark are dropped (MVCC GC).
struct MergeIterator<S> {
    sources: Vec<S>,
    heap: BinaryHeap<HeapItem>,
    watermark: u64,
    /// Buffered versions for the current user key (newest-first), drained one-by-one.
    pending: Vec<Entry>,
}

impl<S: EntrySource> MergeIterator<S> {
    fn new(sources: Vec<S>, watermark: u64) -> Result<Self> {
        let mut this = Self {
            sources,
            heap: BinaryHeap::new(),
            watermark,
            pending: Vec::new(),
        };
        for source in 0..this.sources.len() {
            this.push_current(source)?;
        }
        Ok(this)
    }

    fn push_current(&mut self, source: usize) -> Result<()> {
        if let Some(entry) = self.sources[source].next_entry()? {
            self.heap.push(HeapItem { entry, source });
        }
        Ok(())
    }

    fn next_entry(&mut self) -> Result<Option<Entry>> {
        if let Some(entry) = self.pending.pop() {
            return Ok(Some(entry));
        }
        let Some(first) = self.heap.pop() else {
            return Ok(None);
        };
        let key = first.entry.key.clone();
        let mut versions = vec![first.entry];
        self.push_current(first.source)?;

        while self.heap.peek().is_some_and(|item| item.entry.key == key) {
            let duplicate = self.heap.pop().expect("peeked heap item");
            versions.push(duplicate.entry);
            self.push_current(duplicate.source)?;
        }
        // Newest first.
        versions.sort_by_key(|b| std::cmp::Reverse(b.seq));
        // Deduplicate identical commit timestamps (keep first).
        versions.dedup_by(|a, b| a.seq == b.seq);

        let mut kept = Vec::new();
        let mut kept_below = false;
        for v in versions {
            if v.seq >= self.watermark {
                kept.push(v);
            } else if !kept_below {
                kept.push(v);
                kept_below = true;
            }
            // else: shadowed and older than watermark → GC drop
        }
        // Emit newest-first within the key (SST: user ASC, ts DESC).
        kept.sort_by_key(|b| std::cmp::Reverse(b.seq));
        kept.reverse();
        self.pending = kept;
        Ok(self.pending.pop())
    }
}

struct IoPacer {
    bytes_per_second: u64,
    next_slot: Mutex<Instant>,
}

impl IoPacer {
    fn new(bytes_per_second: u64) -> Self {
        Self {
            bytes_per_second,
            next_slot: Mutex::new(Instant::now()),
        }
    }

    fn pace(&self, bytes: usize) {
        let nanos = ((bytes as u128) * 1_000_000_000u128 / self.bytes_per_second as u128)
            .min(u64::MAX as u128) as u64;
        let duration = Duration::from_nanos(nanos);
        let now = Instant::now();
        let mut next = self.next_slot.lock();
        let start = (*next).max(now);
        *next = start.checked_add(duration).unwrap_or(start);
        drop(next);
        if start > now {
            thread::sleep(start - now);
        }
    }
}

/// Physical worker pool that executed a compaction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CompactionPool {
    /// Latency-sensitive L0→L1 pool.
    L0Rapid,
    /// Heavy L1→L2+ pool.
    LnHaul,
}

/// Successful compaction outcome.
#[derive(Clone, Debug)]
pub struct CompactionResult {
    /// Pool that executed the work.
    pub pool: CompactionPool,
    /// Installed output metadata.
    pub output: SstMeta,
    /// Input files removed from the level catalog.
    pub input_ids: Vec<SstId>,
    /// Input files whose unlink remains deferred by external read pins.
    pub deferred_deletes: Vec<SstId>,
}

/// Completion handle for one scheduled compaction.
pub struct CompactionTicket {
    receiver: Receiver<Result<CompactionResult>>,
}

impl CompactionTicket {
    /// Block until the job completes.
    pub fn wait(self) -> Result<CompactionResult> {
        self.receiver.recv().map_err(|_| {
            TakyonicError::Compaction("compaction worker exited without a result".into())
        })?
    }

    /// Wait up to `timeout` for completion.
    pub fn wait_timeout(self, timeout: Duration) -> Result<Option<CompactionResult>> {
        match self.receiver.recv_timeout(timeout) {
            Ok(result) => result.map(Some),
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => Ok(None),
            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => Err(
                TakyonicError::Compaction("compaction worker disconnected".into()),
            ),
        }
    }
}

struct CompactionJob {
    plan: CompactionPlan,
    completion: Sender<Result<CompactionResult>>,
}

enum WorkerMessage {
    Compact(CompactionJob),
    Shutdown,
}

/// Dual-pool long-lived compaction engine.
pub struct CompactionEngine {
    manager: Arc<SstManager>,
    rapid_tx: Sender<WorkerMessage>,
    haul_tx: Sender<WorkerMessage>,
    rapid_workers: Vec<JoinHandle<()>>,
    haul_workers: Vec<JoinHandle<()>>,
}

impl CompactionEngine {
    /// Start physically separate L0 Rapid and Ln Haul worker pools.
    pub fn new(manager: Arc<SstManager>, config: &Config) -> Result<Self> {
        config.validate()?;
        let pacer = Arc::new(IoPacer::new(config.compaction_write_bytes_per_sec));
        let (rapid_tx, rapid_rx) = crossbeam_channel::bounded(config.compaction_queue_depth);
        let (haul_tx, haul_rx) = crossbeam_channel::bounded(config.compaction_queue_depth);
        let rapid_workers = spawn_workers(
            "takyonic-l0-rapid",
            config.l0_rapid_pool_threads,
            CompactionPool::L0Rapid,
            rapid_rx,
            Arc::clone(&manager),
            Arc::clone(&pacer),
        )?;
        let haul_workers = spawn_workers(
            "takyonic-ln-haul",
            config.ln_haul_pool_threads,
            CompactionPool::LnHaul,
            haul_rx,
            Arc::clone(&manager),
            pacer,
        )?;
        Ok(Self {
            manager,
            rapid_tx,
            haul_tx,
            rapid_workers,
            haul_workers,
        })
    }

    /// Pick and enqueue one L0→L1 compaction.
    pub fn submit_l0(&self) -> Result<Option<CompactionTicket>> {
        self.submit(0, &self.rapid_tx)
    }

    /// Pick and enqueue one L1→L2+ compaction from `source_level`.
    pub fn submit_ln(&self, source_level: usize) -> Result<Option<CompactionTicket>> {
        if source_level == 0 {
            return Err(TakyonicError::Compaction(
                "L0 work must use submit_l0".into(),
            ));
        }
        self.submit(source_level, &self.haul_tx)
    }

    fn submit(
        &self,
        source_level: usize,
        sender: &Sender<WorkerMessage>,
    ) -> Result<Option<CompactionTicket>> {
        let Some(plan) = self.manager.pick(source_level)? else {
            return Ok(None);
        };
        let (completion, receiver) = crossbeam_channel::bounded(1);
        let job = CompactionJob {
            plan: plan.clone(),
            completion,
        };
        match sender.try_send(WorkerMessage::Compact(job)) {
            Ok(()) => Ok(Some(CompactionTicket { receiver })),
            Err(TrySendError::Full(_)) => {
                self.manager.abort(&plan);
                Err(TakyonicError::Compaction(
                    "compaction pool queue is full".into(),
                ))
            }
            Err(TrySendError::Disconnected(_)) => {
                self.manager.abort(&plan);
                Err(TakyonicError::Compaction(
                    "compaction pool is disconnected".into(),
                ))
            }
        }
    }
}

impl Drop for CompactionEngine {
    fn drop(&mut self) {
        for _ in 0..self.rapid_workers.len() {
            let _ = self.rapid_tx.send(WorkerMessage::Shutdown);
        }
        for _ in 0..self.haul_workers.len() {
            let _ = self.haul_tx.send(WorkerMessage::Shutdown);
        }
        for worker in self.rapid_workers.drain(..) {
            let _ = worker.join();
        }
        for worker in self.haul_workers.drain(..) {
            let _ = worker.join();
        }
    }
}

fn spawn_workers(
    name: &str,
    count: usize,
    pool: CompactionPool,
    receiver: Receiver<WorkerMessage>,
    manager: Arc<SstManager>,
    pacer: Arc<IoPacer>,
) -> Result<Vec<JoinHandle<()>>> {
    let mut workers = Vec::with_capacity(count);
    for index in 0..count {
        let receiver = receiver.clone();
        let manager = Arc::clone(&manager);
        let pacer = Arc::clone(&pacer);
        let thread_name = format!("{name}-{index}");
        let handle = thread::Builder::new()
            .name(thread_name)
            .spawn(move || worker_loop(pool, receiver, manager, pacer))?;
        workers.push(handle);
    }
    Ok(workers)
}

fn worker_loop(
    pool: CompactionPool,
    receiver: Receiver<WorkerMessage>,
    manager: Arc<SstManager>,
    pacer: Arc<IoPacer>,
) {
    while let Ok(message) = receiver.recv() {
        match message {
            WorkerMessage::Compact(job) => {
                let span = info_span!(
                    "compaction",
                    ?pool,
                    source_level = job.plan.source_level,
                    output_level = job.plan.output_level,
                    output_id = job.plan.output_id
                );
                let plan = job.plan.clone();
                let _entered = span.enter();
                let result = execute_plan(pool, &manager, &pacer, job.plan);
                if result.is_err() {
                    manager.abort(&plan);
                }
                let _ = job.completion.send(result);
            }
            WorkerMessage::Shutdown => break,
        }
    }
}

fn execute_plan(
    pool: CompactionPool,
    manager: &SstManager,
    pacer: &IoPacer,
    plan: CompactionPlan,
) -> Result<CompactionResult> {
    let mut sources = Vec::with_capacity(plan.inputs.len());
    for input in &plan.inputs {
        let pin = manager.registry.pin(input.id).ok_or_else(|| {
            TakyonicError::Compaction(format!("SST {} could not be pinned", input.id))
        })?;
        sources.push(SstCursor::new(pin));
    }

    let mut merge = MergeIterator::new(sources, manager.mvcc_watermark())?;
    let mut merged = Vec::new();
    while let Some(entry) = merge.next_entry()? {
        merged.push(entry);
    }
    let first = merged
        .first()
        .ok_or_else(|| TakyonicError::Compaction("compaction produced an empty output".into()))?;
    let smallest = first.key.clone();
    let largest = merged
        .last()
        .expect("non-empty compaction output")
        .key
        .clone();
    let output_path = manager.output_path(plan.output_level, plan.output_id);
    let info = SstWriter::write_paced(
        plan.output_id,
        &output_path,
        &merged,
        manager.block_size,
        |bytes| pacer.pace(bytes),
    )?;
    let output = SstMeta::from_info(plan.output_level, info, smallest, largest)?;

    if let Err(register_error) = manager.registry.register(output.id, &output.path) {
        let _ = std::fs::remove_file(&output.path);
        return Err(register_error);
    }
    if let Err(install_error) = manager.install(&plan, output.clone()) {
        let _ = manager.registry.retire(output.id);
        return Err(install_error);
    }

    // Our worker pins must drop before retirement. External reader pins may
    // remain, in which case SstRegistry safely defers physical unlink.
    drop(merge);
    let mut deferred_deletes = Vec::new();
    for input in &plan.inputs {
        match manager.registry.retire(input.id)? {
            DeleteStatus::Deferred => deferred_deletes.push(input.id),
            DeleteStatus::Deleted | DeleteStatus::NotFound => {}
        }
    }
    let input_ids = plan.inputs.iter().map(|input| input.id).collect();
    debug!(
        ?pool,
        output_id = output.id,
        inputs = plan.inputs.len(),
        "compaction installed"
    );
    Ok(CompactionResult {
        pool,
        output,
        input_ids,
        deferred_deletes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("takyonic-compact-{name}-{nanos}"));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    fn manager(dir: &std::path::Path) -> Arc<SstManager> {
        Arc::new(SstManager::new(Arc::new(SstRegistry::new()), dir, 64, 4, 100).unwrap())
    }

    fn write_add(manager: &SstManager, level: usize, id: SstId, entries: &[Entry]) {
        let path = manager.data_dir.join(format!("input-{level}-{id:04}.sst"));
        let info = SstWriter::write(id, path, entries, manager.block_size).unwrap();
        let meta = SstMeta::from_info(
            level,
            info,
            entries.first().unwrap().key.clone(),
            entries.last().unwrap().key.clone(),
        )
        .unwrap();
        manager.add_sst(meta).unwrap();
    }

    #[test]
    fn merge_iterator_keeps_versions_above_watermark() {
        let sources = vec![
            vec![
                Entry::put(&b"a"[..], &b"old"[..], 1),
                Entry::put(&b"b"[..], &b"value"[..], 1),
            ],
            vec![
                Entry::put(&b"a"[..], &b"new"[..], 5),
                Entry::delete(&b"b"[..], 6),
            ],
        ];
        let sources = sources
            .into_iter()
            .map(|source| VecSource(source.into_iter()))
            .collect();
        // Watermark 100: only the newest version below watermark is kept per key.
        let mut merge = MergeIterator::new(sources, 100).unwrap();
        let mut merged = Vec::new();
        while let Some(entry) = merge.next_entry().unwrap() {
            merged.push(entry);
        }
        assert_eq!(merged.len(), 2);
        assert_eq!(merged[0].key.as_bytes(), b"a");
        assert_eq!(merged[0].seq, 5);
        assert_eq!(merged[0].value.as_ref().unwrap().as_bytes(), b"new");
        assert_eq!(merged[1].key.as_bytes(), b"b");
        assert!(merged[1].tombstone);
        assert_eq!(merged[1].seq, 6);
    }

    #[test]
    fn merge_iterator_gc_drops_shadowed_below_watermark() {
        let sources = vec![VecSource(
            vec![
                Entry::put(&b"k"[..], &b"v1"[..], 1),
                Entry::put(&b"k"[..], &b"v2"[..], 5),
                Entry::put(&b"k"[..], &b"v3"[..], 10),
            ]
            .into_iter(),
        )];
        // Watermark 8: keep v3 (10>=8) and newest below (v2@5); drop v1.
        let mut merge = MergeIterator::new(sources, 8).unwrap();
        let mut merged = Vec::new();
        while let Some(entry) = merge.next_entry().unwrap() {
            merged.push(entry);
        }
        assert_eq!(merged.len(), 2);
        assert_eq!(merged[0].seq, 10);
        assert_eq!(merged[1].seq, 5);
    }

    #[test]
    fn occ_allows_non_overlapping_parallel_l0_picks() {
        let dir = temp_dir("occ");
        let manager = manager(&dir);
        write_add(&manager, 0, 1, &[Entry::put(&b"a"[..], &b"1"[..], 1)]);
        write_add(&manager, 0, 2, &[Entry::put(&b"z"[..], &b"2"[..], 2)]);

        let first = manager.pick(0).unwrap().unwrap();
        let second = manager.pick(0).unwrap().unwrap();
        assert!(first.reserved.is_disjoint(&second.reserved));
        assert_eq!(manager.reserved_ids().len(), 2);
        manager.abort(&first);
        manager.abort(&second);
        drop(manager);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn rapid_and_haul_pools_install_outputs() {
        let dir = temp_dir("pools");
        let manager = manager(&dir);
        write_add(
            &manager,
            0,
            10,
            &[
                Entry::put(&b"a"[..], &b"old"[..], 1),
                Entry::put(&b"c"[..], &b"three"[..], 1),
            ],
        );
        write_add(
            &manager,
            0,
            11,
            &[
                Entry::put(&b"a"[..], &b"new"[..], 9),
                Entry::put(&b"b"[..], &b"two"[..], 2),
            ],
        );
        let pinned_input_path = manager.level_files(0)[0].path.clone();
        let external_input_pin = manager.registry.pin(10).unwrap();

        let config = Config::default()
            .data_dir(&dir)
            .l0_rapid_pool_threads(1)
            .ln_haul_pool_threads(1)
            .compaction_write_bytes_per_sec(1024 * 1024 * 1024);
        let engine = CompactionEngine::new(Arc::clone(&manager), &config).unwrap();

        let rapid = engine.submit_l0().unwrap().unwrap().wait().unwrap();
        assert_eq!(rapid.pool, CompactionPool::L0Rapid);
        assert!(manager.level_files(0).is_empty());
        assert_eq!(manager.level_files(1).len(), 1);
        assert!(rapid.deferred_deletes.contains(&10));
        assert!(pinned_input_path.exists());
        assert_eq!(
            external_input_pin
                .reader()
                .get(&Key::new(&b"a"[..]))
                .unwrap()
                .unwrap()
                .as_bytes(),
            b"old"
        );
        drop(external_input_pin);
        assert_eq!(manager.registry.reap(10).unwrap(), DeleteStatus::Deleted);
        assert!(!pinned_input_path.exists());

        let rapid_pin = manager.registry.pin(rapid.output.id).unwrap();
        assert_eq!(
            rapid_pin
                .reader()
                .get(&Key::new(&b"a"[..]))
                .unwrap()
                .unwrap()
                .as_bytes(),
            b"new"
        );
        drop(rapid_pin);

        let haul = engine.submit_ln(1).unwrap().unwrap().wait().unwrap();
        assert_eq!(haul.pool, CompactionPool::LnHaul);
        assert!(manager.level_files(1).is_empty());
        assert_eq!(manager.level_files(2).len(), 1);

        drop(engine);
        drop(manager);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
