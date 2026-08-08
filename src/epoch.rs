//! Epoch / watermark tracking for MVCC snapshot isolation and VACUUM.
//!
//! Every active transaction registers its `read_ts` (epoch). The **watermark**
//! is the oldest active epoch: Vacuum may only drop versions that no snapshot
//! at-or-after the watermark still needs.
//!
//! Visibility rule (same as compaction GC):
//! - keep every version with `commit_ts >= watermark`
//! - keep the newest version with `commit_ts < watermark` (snapshot floor)
//! - all older shadowed versions are **dead** and safe to reclaim

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;

use crate::types::CommitTs;

/// Tracks active transaction epochs and publishes the VACUUM watermark.
#[derive(Debug, Default)]
pub struct EpochManager {
    /// txn_id → read_ts (epoch)
    active: Mutex<BTreeMap<u64, CommitTs>>,
    next_id: AtomicU64,
}

impl EpochManager {
    /// Create an empty epoch manager.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a transaction at `read_ts`; returns a unique txn id.
    pub fn begin(&self, read_ts: CommitTs) -> u64 {
        let id = self
            .next_id
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        self.active.lock().insert(id, read_ts);
        id
    }

    /// Unregister a finished / aborted transaction.
    pub fn end(&self, txn_id: u64) {
        self.active.lock().remove(&txn_id);
    }

    /// Oldest active `read_ts`, or `None` if no transactions are open.
    pub fn watermark(&self) -> Option<CommitTs> {
        self.active.lock().values().copied().min()
    }

    /// Number of currently registered active transactions.
    pub fn active_count(&self) -> usize {
        self.active.lock().len()
    }

    /// Snapshot of `(txn_id, read_ts)` for diagnostics / tests.
    pub fn active_epochs(&self) -> Vec<(u64, CommitTs)> {
        self.active
            .lock()
            .iter()
            .map(|(&id, &ts)| (id, ts))
            .collect()
    }

    /// Effective Vacuum watermark: oldest active epoch, or `fallback` when idle.
    pub fn vacuum_watermark(&self, fallback: CommitTs) -> CommitTs {
        self.watermark().unwrap_or(fallback)
    }
}

/// Whether a version at `commit_ts` is dead given the ordered (newest-first)
/// version list of one user key and the Vacuum watermark.
///
/// A version is dead iff it is **not** among the survivors selected by
/// [`survivors_for_key`].
pub fn is_dead_version(versions_newest_first: &[CommitTs], commit_ts: CommitTs, watermark: CommitTs) -> bool {
    !survivors_for_key(versions_newest_first, watermark).contains(&commit_ts)
}

/// Commit timestamps that must remain visible under `watermark`
/// (newest-first input; returned newest-first).
pub fn survivors_for_key(versions_newest_first: &[CommitTs], watermark: CommitTs) -> Vec<CommitTs> {
    let mut kept = Vec::new();
    let mut kept_below = false;
    for &ts in versions_newest_first {
        if ts >= watermark {
            kept.push(ts);
        } else if !kept_below {
            kept.push(ts);
            kept_below = true;
        }
    }
    kept
}

/// Commit timestamps that are safe to drop under `watermark`.
pub fn dead_versions_for_key(versions_newest_first: &[CommitTs], watermark: CommitTs) -> Vec<CommitTs> {
    let survivors = survivors_for_key(versions_newest_first, watermark);
    versions_newest_first
        .iter()
        .copied()
        .filter(|ts| !survivors.contains(ts))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn watermark_tracks_oldest_active_epoch() {
        let epochs = EpochManager::new();
        assert_eq!(epochs.watermark(), None);
        assert_eq!(epochs.active_count(), 0);

        let t1 = epochs.begin(10);
        assert_eq!(epochs.watermark(), Some(10));
        let t2 = epochs.begin(5);
        assert_eq!(epochs.watermark(), Some(5));
        let t3 = epochs.begin(20);
        assert_eq!(epochs.watermark(), Some(5));
        assert_eq!(epochs.active_count(), 3);

        epochs.end(t2);
        assert_eq!(epochs.watermark(), Some(10));
        epochs.end(t1);
        assert_eq!(epochs.watermark(), Some(20));
        epochs.end(t3);
        assert_eq!(epochs.watermark(), None);
        assert_eq!(epochs.vacuum_watermark(99), 99);
    }

    #[test]
    fn dead_versions_respect_snapshot_floor() {
        // Newest first: v3@10, v2@5, v1@1. Watermark 8 → keep 10 and 5; drop 1.
        let versions = vec![10, 5, 1];
        assert_eq!(survivors_for_key(&versions, 8), vec![10, 5]);
        assert_eq!(dead_versions_for_key(&versions, 8), vec![1]);
        assert!(is_dead_version(&versions, 1, 8));
        assert!(!is_dead_version(&versions, 5, 8));
        assert!(!is_dead_version(&versions, 10, 8));
    }

    #[test]
    fn long_running_reader_protects_all_visible_versions() {
        // Active reader at epoch 1 → watermark 1 → nothing below floor is droppable
        // when every version is >= 1.
        let versions = vec![10, 5, 1];
        assert!(dead_versions_for_key(&versions, 1).is_empty());
    }
}
