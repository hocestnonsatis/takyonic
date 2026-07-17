//! Core domain types for keys, values, and LSM entries.
//!
//! All payload buffers are backed by [`bytes::Bytes`] for cheap cloning and
//! zero-copy handoff across network and disk boundaries.
//!
//! Step 15 introduces logical MVCC: each write is an
//! [`InternalKey`]`(UserKey, CommitTs)` with a [`ValueType`]. Memtables and
//! SSTables sort by user key ascending, then commit timestamp descending so
//! scans hit the newest visible version first.

use std::cmp::Ordering;

use bytes::Bytes;

/// User key bytes. Ordered lexicographically (byte-wise).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Key(Bytes);

impl Key {
    /// Create a key from any bytes-compatible buffer.
    #[inline]
    pub fn new(data: impl Into<Bytes>) -> Self {
        Self(data.into())
    }

    /// Borrow the raw key bytes.
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Cheap clone of the underlying [`Bytes`].
    #[inline]
    pub fn into_bytes(self) -> Bytes {
        self.0
    }
}

impl AsRef<[u8]> for Key {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl From<Vec<u8>> for Key {
    #[inline]
    fn from(value: Vec<u8>) -> Self {
        Self::new(value)
    }
}

impl From<&'static [u8]> for Key {
    #[inline]
    fn from(value: &'static [u8]) -> Self {
        Self::new(value)
    }
}

impl PartialOrd for Key {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Key {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.as_ref().cmp(other.0.as_ref())
    }
}

/// User value bytes.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Value(Bytes);

impl Value {
    /// Create a value from any bytes-compatible buffer.
    #[inline]
    pub fn new(data: impl Into<Bytes>) -> Self {
        Self(data.into())
    }

    /// Borrow the raw value bytes.
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Cheap clone of the underlying [`Bytes`].
    #[inline]
    pub fn into_bytes(self) -> Bytes {
        self.0
    }
}

impl AsRef<[u8]> for Value {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl From<Vec<u8>> for Value {
    #[inline]
    fn from(value: Vec<u8>) -> Self {
        Self::new(value)
    }
}

impl From<&'static [u8]> for Value {
    #[inline]
    fn from(value: &'static [u8]) -> Self {
        Self::new(value)
    }
}

/// Monotonic commit timestamp assigned when a transaction (or single write)
/// becomes durable. Alias of the historical WAL/Raft sequence number.
pub type CommitTs = u64;

/// Historical alias — prefer [`CommitTs`] for MVCC code.
pub type SequenceNumber = CommitTs;

/// Whether an internal key carries a put or a versioned delete.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ValueType {
    /// Live value at this commit timestamp.
    Put,
    /// Tombstone delete at this commit timestamp.
    Tombstone,
}

/// Versioned LSM key: user key + commit timestamp.
///
/// Ordering is **user key ascending**, then **commit timestamp descending**
/// so iterators always encounter the newest version of a key first.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct InternalKey {
    /// Logical user key.
    pub user_key: Key,
    /// Commit timestamp of this version.
    pub commit_ts: CommitTs,
}

impl InternalKey {
    /// Construct an internal key.
    #[inline]
    pub fn new(user_key: impl Into<Key>, commit_ts: CommitTs) -> Self {
        Self {
            user_key: user_key.into(),
            commit_ts,
        }
    }
}

impl PartialOrd for InternalKey {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for InternalKey {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        match self.user_key.cmp(&other.user_key) {
            Ordering::Equal => other.commit_ts.cmp(&self.commit_ts),
            ord => ord,
        }
    }
}

/// A single LSM point entry: put or tombstone delete at a commit timestamp.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    /// User key.
    pub key: Key,
    /// Present for puts; `None` for tombstones.
    pub value: Option<Value>,
    /// Commit timestamp / write order.
    pub seq: CommitTs,
    /// When true, this entry deletes `key` at `seq`.
    pub tombstone: bool,
}

impl Entry {
    /// Construct a put entry.
    #[inline]
    pub fn put(key: impl Into<Key>, value: impl Into<Value>, seq: CommitTs) -> Self {
        Self {
            key: key.into(),
            value: Some(value.into()),
            seq,
            tombstone: false,
        }
    }

    /// Construct a delete (tombstone) entry.
    #[inline]
    pub fn delete(key: impl Into<Key>, seq: CommitTs) -> Self {
        Self {
            key: key.into(),
            value: None,
            seq,
            tombstone: true,
        }
    }

    /// Whether this entry is a tombstone delete.
    #[inline]
    pub fn is_tombstone(&self) -> bool {
        self.tombstone
    }

    /// Value type marker.
    #[inline]
    pub fn value_type(&self) -> ValueType {
        if self.tombstone {
            ValueType::Tombstone
        } else {
            ValueType::Put
        }
    }

    /// Internal key for this version.
    #[inline]
    pub fn internal_key(&self) -> InternalKey {
        InternalKey::new(self.key.clone(), self.seq)
    }
}

/// Internal key ordering for memtables / merge iterators:
/// user key ascending, then sequence descending (newer wins).
impl PartialOrd for Entry {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Entry {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        self.internal_key().cmp(&other.internal_key())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_orders_lexicographically() {
        let a = Key::new(&b"a"[..]);
        let b = Key::new(&b"b"[..]);
        assert!(a < b);
    }

    #[test]
    fn entry_newer_seq_sorts_before_older_same_key() {
        let older = Entry::put(&b"k"[..], &b"v1"[..], 1);
        let newer = Entry::put(&b"k"[..], &b"v2"[..], 2);
        assert!(newer < older);
    }

    #[test]
    fn internal_key_ts_descending() {
        let a10 = InternalKey::new(&b"a"[..], 10);
        let a5 = InternalKey::new(&b"a"[..], 5);
        assert!(a10 < a5);
        assert!(InternalKey::new(&b"a"[..], 5) < InternalKey::new(&b"b"[..], 1));
    }

    #[test]
    fn tombstone_has_no_value() {
        let e = Entry::delete(&b"k"[..], 3);
        assert!(e.is_tombstone());
        assert!(e.value.is_none());
        assert_eq!(e.value_type(), ValueType::Tombstone);
    }
}
