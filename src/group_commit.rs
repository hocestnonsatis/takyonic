//! Group-commit WAL flusher.
//!
//! Concurrent writers submit payloads and park on a [`parking_lot::Condvar`].
//! A dedicated flusher thread drains the entire pending batch, performs **one**
//! `sync_data` for the group, runs an optional apply hook (Raft state-machine
//! publish), then wakes every waiter. This amortizes fsync cost across writers
//! and is the primary lever for breaking the per-op ~7k ops/sec Termux ceiling.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread::{self, JoinHandle};
use std::time::Instant;

use crossbeam_channel::{Receiver, Sender, TryRecvError, unbounded};
use parking_lot::{Condvar, Mutex};
use tracing::{debug, warn};

use crate::error::{Result, TakyonicError};
use crate::telemetry::EngineMetrics;
use crate::types::Entry;
use crate::wal::WalWriter;

/// Called after a batch is durable and before waiters are woken.
///
/// Used by [`crate::raft::LocalRaftNode`] to publish into the memtable so apply
/// never races ahead of (or behind) durability.
pub type ApplyHook = Arc<dyn Fn(&[Entry]) -> Result<()> + Send + Sync>;

/// Shared completion slot: flusher writes the result, waiter parks until set.
struct CommitNotify {
    result: Mutex<Option<Result<()>>>,
    cv: Condvar,
}

impl CommitNotify {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            result: Mutex::new(None),
            cv: Condvar::new(),
        })
    }

    fn wait(&self) -> Result<()> {
        let mut guard = self.result.lock();
        while guard.is_none() {
            self.cv.wait(&mut guard);
        }
        guard.take().expect("completion set")
    }

    fn complete(&self, result: Result<()>) {
        let mut guard = self.result.lock();
        *guard = Some(result);
        self.cv.notify_all();
    }
}

enum FlusherMsg {
    Append {
        entry: Entry,
        notify: Arc<CommitNotify>,
    },
    /// Many entries, one waiter — true group-commit for Raft log batches.
    AppendBatch {
        entries: Vec<Entry>,
        notify: Arc<CommitNotify>,
    },
    Rotate {
        new_wal: WalWriter,
        notify: Arc<CommitNotify>,
    },
    Shutdown,
}

/// Dedicated group-commit WAL: many writers, one fsync per batch.
pub struct GroupCommitWal {
    tx: Sender<FlusherMsg>,
    closed: Arc<AtomicBool>,
    flusher: Mutex<Option<JoinHandle<Result<WalWriter>>>>,
    ops_committed: Arc<AtomicU64>,
    batches: Arc<AtomicU64>,
}

impl GroupCommitWal {
    /// Spawn the flusher thread owning `wal`.
    ///
    /// `apply_hook`, when set, runs after each successful batch `sync_data` and
    /// before waiters are released.
    pub fn start(
        wal: WalWriter,
        metrics: Option<Arc<EngineMetrics>>,
        apply_hook: Option<ApplyHook>,
    ) -> Self {
        let (tx, rx) = unbounded();
        let closed = Arc::new(AtomicBool::new(false));
        let ops_committed = Arc::new(AtomicU64::new(0));
        let batches = Arc::new(AtomicU64::new(0));
        let ops = Arc::clone(&ops_committed);
        let batch_counter = Arc::clone(&batches);
        let flusher = thread::Builder::new()
            .name("takyonic-wal-flusher".into())
            .spawn(move || flusher_loop(wal, rx, metrics, apply_hook, ops, batch_counter))
            .expect("spawn WAL flusher");
        Self {
            tx,
            closed,
            flusher: Mutex::new(Some(flusher)),
            ops_committed,
            batches,
        }
    }

    /// Submit one entry; blocks until it is durable (and applied, if a hook is set).
    pub fn submit(&self, entry: Entry) -> Result<()> {
        if self.closed.load(Ordering::Acquire) {
            return Err(TakyonicError::Engine("group-commit WAL is closed".into()));
        }
        let notify = CommitNotify::new();
        self.tx
            .send(FlusherMsg::Append {
                entry,
                notify: Arc::clone(&notify),
            })
            .map_err(|_| TakyonicError::Engine("WAL flusher channel closed".into()))?;
        notify.wait()
    }

    /// Submit many entries under **one** `sync_data` (network / Raft log batching).
    pub fn submit_batch(&self, entries: Vec<Entry>) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        if self.closed.load(Ordering::Acquire) {
            return Err(TakyonicError::Engine("group-commit WAL is closed".into()));
        }
        let notify = CommitNotify::new();
        self.tx
            .send(FlusherMsg::AppendBatch {
                entries,
                notify: Arc::clone(&notify),
            })
            .map_err(|_| TakyonicError::Engine("WAL flusher channel closed".into()))?;
        notify.wait()
    }

    /// Atomically swap the underlying WAL writer (segment rotation).
    ///
    /// Pending appends are flushed to the old segment first.
    pub fn rotate(&self, new_wal: WalWriter) -> Result<()> {
        if self.closed.load(Ordering::Acquire) {
            return Err(TakyonicError::Engine("group-commit WAL is closed".into()));
        }
        let notify = CommitNotify::new();
        self.tx
            .send(FlusherMsg::Rotate {
                new_wal,
                notify: Arc::clone(&notify),
            })
            .map_err(|_| TakyonicError::Engine("WAL flusher channel closed".into()))?;
        notify.wait()
    }

    /// Stop accepting submits, flush remaining work, and join the flusher.
    ///
    /// Safe to call once; subsequent calls return an error. Does not consume
    /// `self` so callers holding an `Arc` can shut down cleanly.
    pub fn shutdown(&self) -> Result<WalWriter> {
        self.closed.store(true, Ordering::Release);
        let _ = self.tx.send(FlusherMsg::Shutdown);
        let handle = self
            .flusher
            .lock()
            .take()
            .ok_or_else(|| TakyonicError::Engine("WAL flusher already joined".into()))?;
        handle
            .join()
            .map_err(|_| TakyonicError::Engine("WAL flusher panicked".into()))?
    }

    /// Total entries made durable through group commit.
    pub fn ops_committed(&self) -> u64 {
        self.ops_committed.load(Ordering::Relaxed)
    }

    /// Number of `sync_data` batches performed.
    pub fn batches(&self) -> u64 {
        self.batches.load(Ordering::Relaxed)
    }

    /// Average batch size so far (ops / batches).
    pub fn avg_batch_size(&self) -> f64 {
        let batches = self.batches() as f64;
        if batches == 0.0 {
            0.0
        } else {
            self.ops_committed() as f64 / batches
        }
    }
}

fn flusher_loop(
    mut wal: WalWriter,
    rx: Receiver<FlusherMsg>,
    metrics: Option<Arc<EngineMetrics>>,
    apply_hook: Option<ApplyHook>,
    ops_committed: Arc<AtomicU64>,
    batches: Arc<AtomicU64>,
) -> Result<WalWriter> {
    loop {
        let first = match rx.recv() {
            Ok(msg) => msg,
            Err(_) => return Ok(wal),
        };

        let mut batch: Vec<(Entry, Arc<CommitNotify>)> = Vec::new();
        let mut rotate: Option<(WalWriter, Arc<CommitNotify>)> = None;
        let mut shutdown = false;

        match first {
            FlusherMsg::Append { entry, notify } => batch.push((entry, notify)),
            FlusherMsg::AppendBatch { entries, notify } => {
                for entry in entries {
                    batch.push((entry, Arc::clone(&notify)));
                }
            }
            FlusherMsg::Rotate { new_wal, notify } => rotate = Some((new_wal, notify)),
            FlusherMsg::Shutdown => shutdown = true,
        }

        if !shutdown && rotate.is_none() {
            loop {
                match rx.try_recv() {
                    Ok(FlusherMsg::Append { entry, notify }) => batch.push((entry, notify)),
                    Ok(FlusherMsg::AppendBatch { entries, notify }) => {
                        for entry in entries {
                            batch.push((entry, Arc::clone(&notify)));
                        }
                    }
                    Ok(FlusherMsg::Rotate { new_wal, notify }) => {
                        rotate = Some((new_wal, notify));
                        break;
                    }
                    Ok(FlusherMsg::Shutdown) => {
                        shutdown = true;
                        break;
                    }
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => {
                        shutdown = true;
                        break;
                    }
                }
            }
        }

        if !batch.is_empty() {
            let start = Instant::now();
            let entries: Vec<Entry> = batch.iter().map(|(e, _)| e.clone()).collect();
            let result: Result<()> = (|| {
                wal.append_batch_sync(&entries)?;
                if let Some(hook) = &apply_hook {
                    hook(&entries)?;
                }
                Ok(())
            })();
            let elapsed = start.elapsed();
            if let Some(m) = &metrics {
                m.record_wal_sync(elapsed);
                m.record_group_commit(batch.len() as u64);
            }
            ops_committed.fetch_add(batch.len() as u64, Ordering::Relaxed);
            batches.fetch_add(1, Ordering::Relaxed);
            debug!(batch = batch.len(), ?elapsed, "group-commit WAL flush");
            match &result {
                Ok(()) => {
                    for (_, notify) in &batch {
                        notify.complete(Ok(()));
                    }
                }
                Err(error) => {
                    let msg = error.to_string();
                    for (_, notify) in &batch {
                        notify.complete(Err(TakyonicError::Engine(format!(
                            "group-commit WAL flush failed: {msg}"
                        ))));
                    }
                    warn!(%error, "group-commit WAL flush failed");
                }
            }
        }

        if let Some((new_wal, notify)) = rotate {
            wal = new_wal;
            notify.complete(Ok(()));
            debug!("group-commit WAL rotated");
        }

        if shutdown {
            if let Err(error) = wal.sync() {
                warn!(%error, "final WAL sync on shutdown failed");
            }
            return Ok(wal);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Key;
    use crate::wal::WalReader;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_wal(name: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("takyonic-gc-{name}-{nanos}.wal"))
    }

    #[test]
    fn concurrent_submits_coalesce_into_batches() {
        let path = temp_wal("coalesce");
        let metrics = Arc::new(EngineMetrics::new());
        let gc = GroupCommitWal::start(
            WalWriter::create(&path).unwrap(),
            Some(Arc::clone(&metrics)),
            None,
        );
        let gc = Arc::new(gc);
        let barrier = Arc::new(std::sync::Barrier::new(64));
        let mut handles = Vec::new();
        for i in 0..64u64 {
            let gc = Arc::clone(&gc);
            let barrier = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                barrier.wait();
                gc.submit(Entry::put(
                    format!("k{i}").into_bytes(),
                    format!("v{i}").into_bytes(),
                    i + 1,
                ))
                .unwrap();
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert!(gc.batches() >= 1);
        assert_eq!(gc.ops_committed(), 64);
        assert!(
            gc.avg_batch_size() > 1.0,
            "avg batch size was {}",
            gc.avg_batch_size()
        );
        drop(gc.shutdown().unwrap());

        let mut reader = WalReader::open(&path).unwrap();
        let mut count = 0;
        reader
            .replay(|e| {
                count += 1;
                assert!(e.value.is_some());
            })
            .unwrap();
        assert_eq!(count, 64);
        assert!(metrics.group_commits() >= 1);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn apply_hook_runs_before_waiters_return() {
        let path = temp_wal("hook");
        let applied = Arc::new(AtomicU64::new(0));
        let counter = Arc::clone(&applied);
        let hook: ApplyHook = Arc::new(move |entries| {
            counter.fetch_add(entries.len() as u64, Ordering::SeqCst);
            Ok(())
        });
        let gc = GroupCommitWal::start(WalWriter::create(&path).unwrap(), None, Some(hook));
        gc.submit(Entry::put(&b"a"[..], &b"1"[..], 1)).unwrap();
        assert_eq!(applied.load(Ordering::SeqCst), 1);
        drop(gc.shutdown().unwrap());
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn rotate_preserves_durability_across_segments() {
        let dir = temp_wal("rotate-dir");
        std::fs::create_dir_all(&dir).unwrap();
        let a = dir.join("000001.wal");
        let b = dir.join("000002.wal");
        let gc = GroupCommitWal::start(WalWriter::create(&a).unwrap(), None, None);
        gc.submit(Entry::put(&b"one"[..], &b"1"[..], 1)).unwrap();
        gc.rotate(WalWriter::create(&b).unwrap()).unwrap();
        gc.submit(Entry::put(&b"two"[..], &b"2"[..], 2)).unwrap();
        drop(gc.shutdown().unwrap());

        let mut r1 = WalReader::open(&a).unwrap();
        let mut keys = Vec::new();
        r1.replay(|e| keys.push(e.key)).unwrap();
        assert_eq!(keys, vec![Key::new(&b"one"[..])]);

        let mut r2 = WalReader::open(&b).unwrap();
        keys.clear();
        r2.replay(|e| keys.push(e.key)).unwrap();
        assert_eq!(keys, vec![Key::new(&b"two"[..])]);
        let _ = std::fs::remove_dir_all(dir);
    }
}
