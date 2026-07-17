//! Durable Raft log backed by [`crate::group_commit::GroupCommitWal`].
//!
//! Each Raft log entry is encoded as an LSM [`Entry`] so we reuse the existing
//! checksummed, group-committed WAL format. The apply hook is intentionally
//! **absent**: consensus decides when an entry is committed; the state machine
//! is applied separately via [`crate::cluster::TakyonicNode`].
//!
//! Step 13 adds prefix compaction: when a snapshot is taken, entries through
//! `last_included_index` are discarded and [`SnapshotMeta`] is durably recorded.

use std::path::{Path, PathBuf};

use bytes::{BufMut, Bytes, BytesMut};
use parking_lot::{Mutex, RwLock};

use crate::error::{Result, TakyonicError};
use crate::group_commit::GroupCommitWal;
use crate::snapshot::SnapshotMeta;
use crate::types::{Entry, Key, Value};
use crate::wal::{WalReader, WalWriter};

/// One Raft log entry (term + index + opaque command bytes).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RaftLogEntry {
    /// Leader term that created this entry.
    pub term: u64,
    /// Monotonic log index (1-based).
    pub index: u64,
    /// Encoded [`crate::raft::RaftCommand`] payload (`bytes` zero-copy).
    pub command: Bytes,
}

impl RaftLogEntry {
    /// Construct a log entry.
    pub fn new(term: u64, index: u64, command: impl Into<Bytes>) -> Self {
        Self {
            term,
            index,
            command: command.into(),
        }
    }

    fn to_wal_entry(&self) -> Entry {
        // Key = big-endian index for ordered recovery; value = term || command.
        let mut key = [0u8; 8];
        key.copy_from_slice(&self.index.to_be_bytes());
        let mut val = BytesMut::with_capacity(8 + self.command.len());
        val.put_u64_le(self.term);
        val.put_slice(&self.command);
        Entry::put(
            Key::new(Bytes::copy_from_slice(&key)),
            Value::new(val.freeze()),
            self.index,
        )
    }

    fn from_wal_entry(entry: &Entry) -> Result<Self> {
        let value = entry
            .value
            .as_ref()
            .ok_or_else(|| TakyonicError::Raft("raft log entry missing value".into()))?;
        let bytes = value.as_bytes();
        if bytes.len() < 8 {
            return Err(TakyonicError::Raft("raft log value truncated".into()));
        }
        let term = u64::from_le_bytes(bytes[..8].try_into().unwrap());
        let command = Bytes::copy_from_slice(&bytes[8..]);
        Ok(Self {
            term,
            index: entry.seq,
            command,
        })
    }
}

/// In-memory Raft log with a group-committed durable shadow.
pub struct RaftLog {
    entries: RwLock<Vec<RaftLogEntry>>,
    /// Serializes index allocation + memory publish around durable batches.
    allocate: Mutex<()>,
    wal: GroupCommitWal,
    path: PathBuf,
    dir: PathBuf,
    /// Compacted prefix boundary (0 = no snapshot yet).
    snapshot: Mutex<SnapshotMeta>,
}

impl RaftLog {
    /// Create or recover a Raft log under `dir/raft.log`.
    pub fn open(dir: impl AsRef<Path>) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;
        let path = dir.join("raft.log");
        let snap = SnapshotMeta::read_from_dir(&dir)?.unwrap_or(SnapshotMeta {
            last_included_index: 0,
            last_included_term: 0,
        });
        let mut recovered = Vec::new();
        let wal = if path.exists() {
            let mut reader = WalReader::open(&path)?;
            reader.replay(|entry| {
                if let Ok(e) = RaftLogEntry::from_wal_entry(&entry) {
                    if e.index > snap.last_included_index {
                        recovered.push(e);
                    }
                }
            })?;
            if reader.has_torn_tail() {
                let valid = reader.last_valid_offset();
                drop(reader);
                let file = std::fs::OpenOptions::new().write(true).open(&path)?;
                file.set_len(valid)?;
                file.sync_data()?;
            }
            GroupCommitWal::start(WalWriter::open_append(&path)?, None, None)
        } else {
            GroupCommitWal::start(WalWriter::create(&path)?, None, None)
        };
        recovered.sort_by_key(|e| e.index);
        recovered.retain(|e| e.index > snap.last_included_index);
        Ok(Self {
            entries: RwLock::new(recovered),
            allocate: Mutex::new(()),
            wal,
            path,
            dir,
            snapshot: Mutex::new(snap),
        })
    }

    /// Snapshot compaction boundary, if any (`index == 0` means none).
    pub fn snapshot_meta(&self) -> SnapshotMeta {
        *self.snapshot.lock()
    }

    /// First index still present in the log (or `snapshot_index + 1`).
    pub fn first_index(&self) -> u64 {
        let snap = self.snapshot_meta().last_included_index;
        self.entries
            .read()
            .first()
            .map(|e| e.index)
            .unwrap_or(snap + 1)
    }

    /// Last log index, or the snapshot index when the suffix is empty.
    pub fn last_index(&self) -> u64 {
        self.entries
            .read()
            .last()
            .map(|e| e.index)
            .unwrap_or_else(|| self.snapshot_meta().last_included_index)
    }

    /// Term of the last entry / snapshot boundary.
    pub fn last_term(&self) -> u64 {
        self.entries
            .read()
            .last()
            .map(|e| e.term)
            .unwrap_or_else(|| self.snapshot_meta().last_included_term)
    }

    /// Number of in-memory (non-compacted) entries.
    pub fn len(&self) -> usize {
        self.entries.read().len()
    }

    /// Whether the in-memory suffix is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.read().is_empty()
    }

    /// Term at `index`, if present (including the snapshot boundary index).
    pub fn term_at(&self, index: u64) -> Option<u64> {
        if index == 0 {
            return Some(0);
        }
        let snap = self.snapshot_meta();
        if index == snap.last_included_index && snap.last_included_index > 0 {
            return Some(snap.last_included_term);
        }
        if index <= snap.last_included_index {
            return None;
        }
        self.entries
            .read()
            .iter()
            .find(|e| e.index == index)
            .map(|e| e.term)
    }

    /// Entry at `index` (not available for compacted indices).
    pub fn entry(&self, index: u64) -> Option<RaftLogEntry> {
        self.entries
            .read()
            .iter()
            .find(|e| e.index == index)
            .cloned()
    }

    /// Slice of entries with index >= `start`.
    pub fn entries_from(&self, start: u64) -> Vec<RaftLogEntry> {
        self.entries
            .read()
            .iter()
            .filter(|e| e.index >= start)
            .cloned()
            .collect()
    }

    /// Append a single entry (must be last_index+1). Durable before return.
    pub fn append(&self, entry: RaftLogEntry) -> Result<()> {
        self.append_batch(std::slice::from_ref(&entry))
    }

    /// Append a contiguous batch under **one** group-commit fsync.
    pub fn append_batch(&self, new_entries: &[RaftLogEntry]) -> Result<()> {
        if new_entries.is_empty() {
            return Ok(());
        }
        let _gate = self.allocate.lock();
        {
            let expected = self.last_index_unlocked() + 1;
            for (i, entry) in new_entries.iter().enumerate() {
                if entry.index != expected + i as u64 {
                    return Err(TakyonicError::Raft(format!(
                        "raft log append gap: expected index {}, got {}",
                        expected + i as u64,
                        entry.index
                    )));
                }
            }
        }
        let wal_entries: Vec<_> = new_entries.iter().map(|e| e.to_wal_entry()).collect();
        self.wal.submit_batch(wal_entries)?;
        self.entries.write().extend(new_entries.iter().cloned());
        Ok(())
    }

    /// Append many encoded commands starting at last_index+1; returns first index.
    pub fn append_commands(&self, term: u64, commands: Vec<Bytes>) -> Result<u64> {
        if commands.is_empty() {
            return Ok(self.last_index());
        }
        let _gate = self.allocate.lock();
        let start = self.last_index_unlocked() + 1;
        let entries: Vec<_> = commands
            .into_iter()
            .enumerate()
            .map(|(i, cmd)| RaftLogEntry::new(term, start + i as u64, cmd))
            .collect();
        let wal_entries: Vec<_> = entries.iter().map(|e| e.to_wal_entry()).collect();
        self.wal.submit_batch(wal_entries)?;
        self.entries.write().extend(entries);
        Ok(start)
    }

    /// Delete all entries with index > `index` (conflict resolution).
    pub fn truncate_after(&self, index: u64) {
        self.entries.write().retain(|e| e.index <= index);
    }

    /// Compact the log through `last_included_index`, discarding the durable prefix.
    ///
    /// Persists [`SnapshotMeta`], rewrites `raft.log` with only the trailing
    /// suffix (`index > last_included_index`), and updates in-memory state.
    pub fn compact_through(&self, last_included_index: u64, last_included_term: u64) -> Result<()> {
        if last_included_index == 0 {
            return Ok(());
        }
        let _gate = self.allocate.lock();
        let snap = *self.snapshot.lock();
        if last_included_index <= snap.last_included_index {
            return Ok(());
        }
        let entry_term = self
            .entries
            .read()
            .iter()
            .find(|e| e.index == last_included_index)
            .map(|e| e.term);
        if entry_term != Some(last_included_term) {
            return Err(TakyonicError::Raft(format!(
                "compact term mismatch at {last_included_index}: expected {last_included_term}, got {entry_term:?}"
            )));
        }

        let remaining: Vec<RaftLogEntry> = self
            .entries
            .read()
            .iter()
            .filter(|e| e.index > last_included_index)
            .cloned()
            .collect();

        let meta = SnapshotMeta {
            last_included_index,
            last_included_term,
        };
        SnapshotMeta::write_to_dir(&self.dir, meta)?;

        // Rewrite durable WAL with only the trailing suffix.
        let tmp = self.dir.join("raft.log.tmp");
        {
            let mut writer = WalWriter::create(&tmp)?;
            if !remaining.is_empty() {
                let wal_entries: Vec<_> = remaining.iter().map(|e| e.to_wal_entry()).collect();
                writer.append_batch_sync(&wal_entries)?;
            } else {
                writer.sync()?;
            }
        }
        std::fs::rename(&tmp, &self.path)?;
        std::fs::File::open(&self.dir)?.sync_all()?;
        self.wal.rotate(WalWriter::open_append(&self.path)?)?;

        *self.snapshot.lock() = meta;
        *self.entries.write() = remaining;
        Ok(())
    }

    /// Install a remote snapshot boundary, discarding all local log entries.
    pub fn install_snapshot(
        &self,
        last_included_index: u64,
        last_included_term: u64,
    ) -> Result<()> {
        let _gate = self.allocate.lock();
        let meta = SnapshotMeta {
            last_included_index,
            last_included_term,
        };
        SnapshotMeta::write_to_dir(&self.dir, meta)?;
        let tmp = self.dir.join("raft.log.tmp");
        {
            let mut writer = WalWriter::create(&tmp)?;
            writer.sync()?;
        }
        std::fs::rename(&tmp, &self.path)?;
        std::fs::File::open(&self.dir)?.sync_all()?;
        self.wal.rotate(WalWriter::open_append(&self.path)?)?;
        *self.snapshot.lock() = meta;
        self.entries.write().clear();
        Ok(())
    }

    /// Whether candidate's log is at least as up-to-date as ours (Raft §5.4.1).
    pub fn is_up_to_date(&self, last_index: u64, last_term: u64) -> bool {
        let my_term = self.last_term();
        let my_index = self.last_index();
        last_term > my_term || (last_term == my_term && last_index >= my_index)
    }

    /// Path of the durable log file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Directory holding the log + snapshot metadata.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Shut down the group-commit flusher.
    pub fn shutdown(&self) -> Result<()> {
        drop(self.wal.shutdown()?);
        Ok(())
    }

    fn last_index_unlocked(&self) -> u64 {
        self.entries
            .read()
            .last()
            .map(|e| e.index)
            .unwrap_or_else(|| self.snapshot.lock().last_included_index)
    }
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
        std::env::temp_dir().join(format!("takyonic-raftlog-{name}-{nanos}"))
    }

    #[test]
    fn append_and_recover() {
        let dir = temp_dir("recover");
        {
            let log = RaftLog::open(&dir).unwrap();
            log.append(RaftLogEntry::new(1, 1, Bytes::from_static(b"a")))
                .unwrap();
            log.append(RaftLogEntry::new(1, 2, Bytes::from_static(b"b")))
                .unwrap();
            log.shutdown().unwrap();
        }
        let log = RaftLog::open(&dir).unwrap();
        assert_eq!(log.last_index(), 2);
        assert_eq!(log.term_at(2), Some(1));
        assert_eq!(log.entry(1).unwrap().command.as_ref(), b"a");
        log.shutdown().unwrap();
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn compact_discards_prefix_and_recovers() {
        let dir = temp_dir("compact");
        {
            let log = RaftLog::open(&dir).unwrap();
            for i in 1..=5 {
                log.append(RaftLogEntry::new(1, i, Bytes::from(vec![i as u8])))
                    .unwrap();
            }
            log.compact_through(3, 1).unwrap();
            assert_eq!(log.snapshot_meta().last_included_index, 3);
            assert_eq!(log.first_index(), 4);
            assert_eq!(log.term_at(3), Some(1));
            assert!(log.entry(2).is_none());
            assert_eq!(log.entry(4).unwrap().command.as_ref(), &[4]);
            log.shutdown().unwrap();
        }
        let log = RaftLog::open(&dir).unwrap();
        assert_eq!(log.snapshot_meta().last_included_index, 3);
        assert_eq!(log.last_index(), 5);
        assert!(log.entry(3).is_none());
        assert_eq!(log.term_at(3), Some(1));
        log.shutdown().unwrap();
        let _ = std::fs::remove_dir_all(dir);
    }
}
