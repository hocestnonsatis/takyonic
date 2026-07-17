//! Concurrent multi-version memtable (active write buffer).
//!
//! Uses a [`parking_lot::RwLock`] around a [`BTreeMap`] keyed by
//! [`InternalKey`] so all MVCC versions coexist until flush. Ordering is
//! user-key ascending, commit-ts descending (flush-friendly for SST emission).

use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::atomic::{AtomicUsize, Ordering};

use parking_lot::RwLock;

use crate::types::{CommitTs, Entry, InternalKey, Key, Value};

/// Approximate per-entry metadata overhead counted toward size limits.
const ENTRY_OVERHEAD: usize = 40;

/// Value payload for one internal-key version.
#[derive(Clone, Debug)]
struct Slot {
    value: Option<Value>,
    tombstone: bool,
}

/// Highly concurrent multi-version memtable used on the ingestion path.
#[derive(Debug, Default)]
pub struct Memtable {
    map: RwLock<BTreeMap<InternalKey, Slot>>,
    /// Approximate logical size in bytes (keys + values + overhead).
    approx_size: AtomicUsize,
}

impl Memtable {
    /// Create an empty memtable.
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Approximate size used for flush scheduling.
    #[inline]
    pub fn approx_size_bytes(&self) -> usize {
        self.approx_size.load(Ordering::Relaxed)
    }

    /// Number of versioned entries currently held.
    #[inline]
    pub fn len(&self) -> usize {
        self.map.read().len()
    }

    /// Whether the memtable holds no entries.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.map.read().is_empty()
    }

    /// Insert a new version. Same `(user_key, commit_ts)` is idempotent (ignored).
    pub fn apply(&self, entry: Entry) {
        let key_len = entry.key.as_bytes().len();
        let val_len = entry
            .value
            .as_ref()
            .map(|v| v.as_bytes().len())
            .unwrap_or(0);
        let add = key_len + val_len + ENTRY_OVERHEAD;
        let ikey = entry.internal_key();

        let mut map = self.map.write();
        if map.contains_key(&ikey) {
            return;
        }
        map.insert(
            ikey,
            Slot {
                value: entry.value,
                tombstone: entry.tombstone,
            },
        );
        self.approx_size.fetch_add(add, Ordering::Relaxed);
    }

    /// Latest live value (ignores snapshot — equivalent to `get_at(key, u64::MAX)`).
    pub fn get(&self, key: &Key) -> Option<Value> {
        self.get_at(key, u64::MAX)
            .and_then(|e| if e.tombstone { None } else { e.value })
    }

    /// Snapshot point lookup: highest `commit_ts <= read_ts`.
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

    /// Visible (non-tombstone) user keys at `read_ts` whose bytes start with `prefix`.
    pub fn scan_prefix_at(&self, prefix: &[u8], read_ts: CommitTs) -> Vec<Entry> {
        let map = self.map.read();
        let mut out = Vec::new();
        let mut current_user: Option<Key> = None;
        let mut resolved = false;
        for (ikey, slot) in map.iter() {
            let uk = ikey.user_key.as_bytes();
            if !uk.starts_with(prefix) {
                if uk > prefix {
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

    /// Latest committed version for `key` (any ts), including tombstones.
    pub fn latest_entry(&self, key: &Key) -> Option<Entry> {
        let map = self.map.read();
        let start = InternalKey::new(key.clone(), u64::MAX);
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

    /// Point lookup including tombstones at the latest version.
    pub fn get_entry(&self, key: &Key) -> Option<Entry> {
        self.latest_entry(key)
    }

    /// Whether the latest version of this key is a tombstone.
    pub fn is_tombstone(&self, key: &Key) -> bool {
        self.latest_entry(key).map(|e| e.tombstone).unwrap_or(false)
    }

    /// Highest commit timestamp present for `key`, if any.
    pub fn latest_commit_ts(&self, key: &Key) -> Option<CommitTs> {
        self.latest_entry(key).map(|e| e.seq)
    }

    /// Ordered snapshot of all versions (internal-key order) for flush / tests.
    pub fn iter_entries(&self) -> Vec<Entry> {
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

    /// Atomically take all versions for flush handoff (empty afterwards).
    pub fn drain_entries(&self) -> Vec<Entry> {
        let mut map = self.map.write();
        let entries: Vec<Entry> = map
            .iter()
            .map(|(ikey, slot)| Entry {
                key: ikey.user_key.clone(),
                value: slot.value.clone(),
                seq: ikey.commit_ts,
                tombstone: slot.tombstone,
            })
            .collect();
        map.clear();
        self.approx_size.store(0, Ordering::Relaxed);
        entries
    }

    /// Clear all entries (after a successful flush handoff / snapshot install).
    pub fn clear(&self) {
        self.map.write().clear();
        self.approx_size.store(0, Ordering::Relaxed);
    }

    /// Drop versions that compaction GC would remove (tests / forced GC).
    pub fn gc_below_watermark(&self, watermark: CommitTs) {
        let mut map = self.map.write();
        let keys: Vec<Key> = map
            .keys()
            .map(|k| k.user_key.clone())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();
        for user_key in keys {
            let versions: Vec<InternalKey> = map
                .range((
                    Bound::Included(InternalKey::new(user_key.clone(), u64::MAX)),
                    Bound::Unbounded,
                ))
                .take_while(|(k, _)| k.user_key == user_key)
                .map(|(k, _)| k.clone())
                .collect();
            let mut kept_below = false;
            for ikey in versions {
                if ikey.commit_ts >= watermark {
                    continue;
                }
                if !kept_below {
                    kept_below = true;
                    continue;
                }
                map.remove(&ikey);
            }
        }
        // Recompute approx size lazily by scanning (rare path).
        let size: usize = map
            .iter()
            .map(|(k, s)| {
                k.user_key.as_bytes().len()
                    + s.value.as_ref().map(|v| v.as_bytes().len()).unwrap_or(0)
                    + ENTRY_OVERHEAD
            })
            .sum();
        self.approx_size.store(size, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn put_get_roundtrip() {
        let mt = Memtable::new();
        mt.apply(Entry::put(&b"a"[..], &b"1"[..], 1));
        assert_eq!(mt.get(&Key::new(&b"a"[..])).unwrap().as_bytes(), b"1");
    }

    #[test]
    fn keeps_multiple_versions() {
        let mt = Memtable::new();
        mt.apply(Entry::put(&b"k"[..], &b"old"[..], 1));
        mt.apply(Entry::put(&b"k"[..], &b"new"[..], 2));
        assert_eq!(mt.len(), 2);
        assert_eq!(mt.get(&Key::new(&b"k"[..])).unwrap().as_bytes(), b"new");
        assert_eq!(
            mt.get_at(&Key::new(&b"k"[..]), 1)
                .unwrap()
                .value
                .unwrap()
                .as_bytes(),
            b"old"
        );
    }

    #[test]
    fn snapshot_ignores_future_writes() {
        let mt = Memtable::new();
        mt.apply(Entry::put(&b"k"[..], &b"v1"[..], 5));
        mt.apply(Entry::put(&b"k"[..], &b"v2"[..], 10));
        let e = mt.get_at(&Key::new(&b"k"[..]), 7).unwrap();
        assert_eq!(e.seq, 5);
        assert_eq!(e.value.unwrap().as_bytes(), b"v1");
    }

    #[test]
    fn tombstone_hides_value_at_snapshot() {
        let mt = Memtable::new();
        mt.apply(Entry::put(&b"k"[..], &b"v"[..], 1));
        mt.apply(Entry::delete(&b"k"[..], 2));
        assert!(mt.get(&Key::new(&b"k"[..])).is_none());
        assert!(mt.get_at(&Key::new(&b"k"[..]), 2).unwrap().tombstone);
        assert_eq!(
            mt.get_at(&Key::new(&b"k"[..]), 1)
                .unwrap()
                .value
                .unwrap()
                .as_bytes(),
            b"v"
        );
    }

    #[test]
    fn iter_is_sorted_by_internal_key() {
        let mt = Memtable::new();
        mt.apply(Entry::put(&b"c"[..], &b"3"[..], 1));
        mt.apply(Entry::put(&b"a"[..], &b"1"[..], 2));
        mt.apply(Entry::put(&b"a"[..], &b"0"[..], 1));
        mt.apply(Entry::put(&b"b"[..], &b"2"[..], 3));
        let keys: Vec<_> = mt
            .iter_entries()
            .into_iter()
            .map(|e| (e.key.into_bytes(), e.seq))
            .collect();
        assert_eq!(
            keys,
            vec![
                (Bytes::from_static(b"a"), 2),
                (Bytes::from_static(b"a"), 1),
                (Bytes::from_static(b"b"), 3),
                (Bytes::from_static(b"c"), 1),
            ]
        );
    }
}
