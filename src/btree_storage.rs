//! In-memory MVCC B-Tree storage engine (read-friendly / baseline for LSM benches).
//!
//! Random point updates hit a [`BTreeMap`] keyed by [`InternalKey`]. This is the
//! counterpart to [`crate::lsm_storage::LSMStorage`] for tables marked
//! [`crate::storage::StorageEngineKind::BTree`].
//!
//! # Durability
//!
//! The B-Tree itself is **not** durable. For `BTREE` tables, the engine still
//! commits through Raft/WAL → LSM (memtable/SST). After each commit, matching
//! keys are mirrored here for fast SI reads. On `Engine::open`, the mirror is
//! **hydrated from LSM** so cold restarts see the same data. Treat LSM as the
//! source of truth; this map is a read-optimized cache.

use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;

use crate::types::{CommitTs, Entry, InternalKey, Key, Value};

#[derive(Clone, Debug)]
struct Slot {
    value: Option<Value>,
    tombstone: bool,
}

/// Concurrent multi-version B-Tree store.
#[derive(Debug, Default)]
pub struct BTreeStorage {
    map: RwLock<BTreeMap<InternalKey, Slot>>,
    next_ts: AtomicU64,
}

impl BTreeStorage {
    /// Empty store; commit timestamps start at 1.
    pub fn new() -> Self {
        Self {
            map: RwLock::new(BTreeMap::new()),
            next_ts: AtomicU64::new(1),
        }
    }

    /// Allocate the next commit timestamp.
    pub fn alloc_ts(&self) -> CommitTs {
        self.next_ts.fetch_add(1, Ordering::Relaxed)
    }

    /// Number of versioned entries.
    pub fn len(&self) -> usize {
        self.map.read().len()
    }

    /// Whether the store is empty.
    pub fn is_empty(&self) -> bool {
        self.map.read().is_empty()
    }

    /// Apply a put/delete at an explicit commit timestamp.
    pub fn apply(&self, entry: Entry) {
        let ikey = entry.internal_key();
        let mut map = self.map.write();
        map.insert(
            ikey,
            Slot {
                value: entry.value,
                tombstone: entry.tombstone,
            },
        );
        let _ = self
            .next_ts
            .fetch_max(entry.seq.saturating_add(1), Ordering::Relaxed);
    }

    /// Put `value` at a freshly allocated timestamp; returns that timestamp.
    pub fn put(&self, key: impl Into<Key>, value: impl Into<Value>) -> CommitTs {
        let ts = self.alloc_ts();
        self.apply(Entry::put(key, value, ts));
        ts
    }

    /// Tombstone `key` at a fresh timestamp.
    pub fn delete(&self, key: impl Into<Key>) -> CommitTs {
        let ts = self.alloc_ts();
        self.apply(Entry::delete(key, ts));
        ts
    }

    /// Snapshot point lookup.
    pub fn get_at(&self, key: &Key, read_ts: CommitTs) -> Option<Entry> {
        let map = self.map.read();
        let start = InternalKey::new(key.clone(), read_ts);
        let (ikey, slot) = map.range(start..).next()?;
        if ikey.user_key != *key {
            return None;
        }
        Some(Entry {
            key: key.clone(),
            value: slot.value.clone(),
            seq: ikey.commit_ts,
            tombstone: slot.tombstone,
        })
    }

    /// Latest live value.
    pub fn get(&self, key: &Key) -> Option<Value> {
        self.get_at(key, u64::MAX)
            .and_then(|e| if e.tombstone { None } else { e.value })
    }

    /// Prefix / full scan at `read_ts` (newest visible version per user key).
    pub fn scan_at(&self, prefix: &[u8], read_ts: CommitTs) -> Vec<Entry> {
        let map = self.map.read();
        let mut out = Vec::new();
        let mut current_user: Option<Key> = None;
        let mut resolved = false;
        for (ikey, slot) in map.iter() {
            let uk = ikey.user_key.as_bytes();
            if !uk.starts_with(prefix) {
                if !prefix.is_empty() && uk > prefix {
                    break;
                }
                continue;
            }
            if current_user.as_ref() != Some(&ikey.user_key) {
                current_user = Some(ikey.user_key.clone());
                resolved = false;
            }
            if resolved || ikey.commit_ts > read_ts {
                continue;
            }
            resolved = true;
            if !slot.tombstone {
                out.push(Entry {
                    key: ikey.user_key.clone(),
                    value: slot.value.clone(),
                    seq: ikey.commit_ts,
                    tombstone: false,
                });
            }
        }
        out
    }

    /// Drop versions shadowed below `watermark` (VACUUM for B-Tree tables).
    pub fn vacuum_below(&self, watermark: CommitTs) -> u64 {
        let mut map = self.map.write();
        let mut by_key: BTreeMap<Key, Vec<CommitTs>> = BTreeMap::new();
        for ikey in map.keys() {
            by_key
                .entry(ikey.user_key.clone())
                .or_default()
                .push(ikey.commit_ts);
        }
        let mut removed = 0u64;
        for (user, mut versions) in by_key {
            versions.sort_by_key(|t| std::cmp::Reverse(*t));
            let mut keep_below = false;
            for &ts in &versions {
                let drop = if ts >= watermark {
                    false
                } else if !keep_below {
                    keep_below = true;
                    false
                } else {
                    true
                };
                if drop {
                    map.remove(&InternalKey::new(user.clone(), ts));
                    removed += 1;
                }
            }
        }
        removed
    }

    /// All entries sorted for export / ANALYZE sampling.
    pub fn snapshot_entries(&self) -> Vec<Entry> {
        let map = self.map.read();
        map.iter()
            .map(|(ikey, slot)| Entry {
                key: ikey.user_key.clone(),
                value: slot.value.clone(),
                seq: ikey.commit_ts,
                tombstone: slot.tombstone,
            })
            .collect()
    }

    /// Range of internal keys for testing.
    pub fn range_raw(
        &self,
        start: Bound<InternalKey>,
        end: Bound<InternalKey>,
    ) -> Vec<(InternalKey, bool)> {
        let map = self.map.read();
        map.range((start, end))
            .map(|(k, s)| (k.clone(), s.tombstone))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_get_mvcc_and_vacuum() {
        let s = BTreeStorage::new();
        let t1 = s.put(&b"k"[..], &b"v1"[..]);
        let t2 = s.put(&b"k"[..], &b"v2"[..]);
        assert!(t2 > t1);
        assert_eq!(s.get_at(&Key::new(&b"k"[..]), t1).unwrap().value.unwrap().as_bytes(), b"v1");
        assert_eq!(s.get(&Key::new(&b"k"[..])).unwrap().as_bytes(), b"v2");
        // Watermark strictly above the latest version: keep t2, drop t1.
        let removed = s.vacuum_below(t2 + 1);
        assert!(removed >= 1);
        assert_eq!(s.get(&Key::new(&b"k"[..])).unwrap().as_bytes(), b"v2");
    }
}
