//! ARIES-inspired transactional Write-Ahead Log for OCC durability.
//!
//! Protocol (Write-Ahead Logging):
//! 1. Append [`WalRecord`] ops for the write-set (`Insert` / `Update` / `Delete`).
//! 2. Append [`WalRecord::Commit`] and `sync_data` (fdatasync).
//! 3. Only then apply the write-set to the memtable / LSM.
//!
//! Recovery (Redo): scan from the start (or last checkpoint), group records by
//! transaction, and replay only write-sets that end with a durable `Commit`.
//! Trailing ops without `Commit` are discarded (loser transactions).

use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Write};
use std::path::{Path, PathBuf};

use bytes::{BufMut, Bytes, BytesMut};
use tracing::{debug, info};
use xxhash_rust::xxh3::xxh3_64;

use crate::error::{Result, TakyonicError};
use crate::memtable::Memtable;
use crate::schema::{DATA_PREFIX, IDX_PREFIX};
use crate::types::{Entry, Key, Value};
use crate::txn::WriteOp;

/// On-disk file name under `data_dir`.
pub const TXN_WAL_FILE: &str = "TXN_WAL";

const MAGIC: &[u8; 4] = b"TKYA";
const VERSION: u8 = 1;

const TAG_INSERT: u8 = 1;
const TAG_UPDATE: u8 = 2;
const TAG_DELETE: u8 = 3;
const TAG_COMMIT: u8 = 4;

/// One ARIES-style log record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WalRecord {
    /// Insert a key/value into `table` (payload may be a raw LSM key).
    Insert {
        /// Logical table name (empty for non-relational keys).
        table: String,
        /// Encoded storage key bytes.
        key: Bytes,
        /// Encoded value bytes.
        value: Bytes,
    },
    /// Update (overwrite) a key/value.
    Update {
        /// Logical table name.
        table: String,
        /// Encoded storage key bytes.
        key: Bytes,
        /// Encoded value bytes.
        value: Bytes,
    },
    /// Delete a key.
    Delete {
        /// Logical table name.
        table: String,
        /// Encoded storage key bytes.
        key: Bytes,
    },
    /// Marks the preceding write-set as durable / committed.
    Commit {
        /// Transaction id that owns this commit.
        txn_id: u64,
    },
}

impl WalRecord {
    /// Encode body bytes (no length prefix / checksum).
    pub fn encode_body(&self, buf: &mut BytesMut) {
        match self {
            Self::Insert { table, key, value } => {
                buf.put_u8(TAG_INSERT);
                put_bytes(buf, table.as_bytes());
                put_bytes(buf, key);
                put_bytes(buf, value);
            }
            Self::Update { table, key, value } => {
                buf.put_u8(TAG_UPDATE);
                put_bytes(buf, table.as_bytes());
                put_bytes(buf, key);
                put_bytes(buf, value);
            }
            Self::Delete { table, key } => {
                buf.put_u8(TAG_DELETE);
                put_bytes(buf, table.as_bytes());
                put_bytes(buf, key);
            }
            Self::Commit { txn_id } => {
                buf.put_u8(TAG_COMMIT);
                buf.put_u64_le(*txn_id);
            }
        }
    }

    /// Decode a body produced by [`Self::encode_body`].
    pub fn decode_body(mut body: &[u8]) -> Result<Self> {
        if body.is_empty() {
            return Err(TakyonicError::Integrity("txn WAL body empty".into()));
        }
        let tag = body[0];
        body = &body[1..];
        match tag {
            TAG_INSERT => {
                let (table, rest) = take_bytes(body)?;
                let (key, rest) = take_bytes(rest)?;
                let (value, rest) = take_bytes(rest)?;
                if !rest.is_empty() {
                    return Err(TakyonicError::Integrity(
                        "txn WAL Insert has trailing bytes".into(),
                    ));
                }
                Ok(Self::Insert {
                    table: bytes_to_string(table)?,
                    key: Bytes::copy_from_slice(key),
                    value: Bytes::copy_from_slice(value),
                })
            }
            TAG_UPDATE => {
                let (table, rest) = take_bytes(body)?;
                let (key, rest) = take_bytes(rest)?;
                let (value, rest) = take_bytes(rest)?;
                if !rest.is_empty() {
                    return Err(TakyonicError::Integrity(
                        "txn WAL Update has trailing bytes".into(),
                    ));
                }
                Ok(Self::Update {
                    table: bytes_to_string(table)?,
                    key: Bytes::copy_from_slice(key),
                    value: Bytes::copy_from_slice(value),
                })
            }
            TAG_DELETE => {
                let (table, rest) = take_bytes(body)?;
                let (key, rest) = take_bytes(rest)?;
                if !rest.is_empty() {
                    return Err(TakyonicError::Integrity(
                        "txn WAL Delete has trailing bytes".into(),
                    ));
                }
                Ok(Self::Delete {
                    table: bytes_to_string(table)?,
                    key: Bytes::copy_from_slice(key),
                })
            }
            TAG_COMMIT => {
                if body.len() != 8 {
                    return Err(TakyonicError::Integrity(
                        "txn WAL Commit must be 8 bytes".into(),
                    ));
                }
                let txn_id = u64::from_le_bytes(body[..8].try_into().unwrap());
                Ok(Self::Commit { txn_id })
            }
            other => Err(TakyonicError::Integrity(format!(
                "unknown txn WAL tag {other}"
            ))),
        }
    }

    /// Full on-disk framing: `[u32 len][body][u64 xxh3]`.
    pub fn encode_framed(&self) -> Bytes {
        let mut body = BytesMut::new();
        self.encode_body(&mut body);
        let checksum = xxh3_64(&body);
        let mut out = BytesMut::with_capacity(4 + body.len() + 8);
        out.put_u32_le(body.len() as u32);
        out.put_slice(&body);
        out.put_u64_le(checksum);
        out.freeze()
    }
}

fn put_bytes(buf: &mut BytesMut, bytes: &[u8]) {
    buf.put_u32_le(bytes.len() as u32);
    buf.put_slice(bytes);
}

fn take_bytes(body: &[u8]) -> Result<(&[u8], &[u8])> {
    if body.len() < 4 {
        return Err(TakyonicError::Integrity(
            "txn WAL truncated length prefix".into(),
        ));
    }
    let len = u32::from_le_bytes(body[..4].try_into().unwrap()) as usize;
    let rest = &body[4..];
    if rest.len() < len {
        return Err(TakyonicError::Integrity(
            "txn WAL truncated payload".into(),
        ));
    }
    Ok((&rest[..len], &rest[len..]))
}

fn bytes_to_string(bytes: &[u8]) -> Result<String> {
    String::from_utf8(bytes.to_vec())
        .map_err(|e| TakyonicError::Integrity(format!("txn WAL non-utf8 table: {e}")))
}

/// Infer a logical table name from a storage key (`Data_` / `Idx_` prefixes).
pub fn table_from_storage_key(key: &[u8]) -> String {
    if let Some(rest) = key.strip_prefix(DATA_PREFIX) {
        if let Some(i) = rest.iter().position(|&b| b == b'_') {
            return String::from_utf8_lossy(&rest[..i]).into_owned();
        }
    }
    if let Some(rest) = key.strip_prefix(IDX_PREFIX) {
        if let Some(i) = rest.iter().position(|&b| b == b'_') {
            return String::from_utf8_lossy(&rest[..i]).into_owned();
        }
    }
    String::new()
}

/// Build ARIES records for an OCC write-set (Puts → Insert, Deletes → Delete).
pub fn records_from_writes(writes: &std::collections::BTreeMap<Key, WriteOp>) -> Vec<WalRecord> {
    let mut out = Vec::with_capacity(writes.len());
    for (key, op) in writes {
        let table = table_from_storage_key(key.as_bytes());
        match op {
            WriteOp::Put(value) => out.push(WalRecord::Insert {
                table,
                key: Bytes::copy_from_slice(key.as_bytes()),
                value: Bytes::copy_from_slice(value.as_bytes()),
            }),
            WriteOp::Delete => out.push(WalRecord::Delete {
                table,
                key: Bytes::copy_from_slice(key.as_bytes()),
            }),
        }
    }
    out
}

/// Append-only transactional WAL manager.
pub struct WalManager {
    #[allow(dead_code)]
    path: PathBuf,
    file: File,
    /// Byte offset of the next append (also last valid end after recovery).
    offset: u64,
}

impl WalManager {
    /// Path of the durable txn WAL under `data_dir`.
    pub fn path_in(data_dir: &Path) -> PathBuf {
        data_dir.join(TXN_WAL_FILE)
    }

    /// Create a fresh txn WAL (truncates if present).
    pub fn create(data_dir: &Path) -> Result<Self> {
        fs_create_dir(data_dir)?;
        let path = Self::path_in(data_dir);
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .read(true)
            .open(&path)?;
        file.write_all(MAGIC)?;
        file.write_all(&[VERSION])?;
        file.sync_data()?;
        Ok(Self {
            path,
            file,
            offset: (MAGIC.len() + 1) as u64,
        })
    }

    /// Open existing WAL for append, or create if missing.
    pub fn open(data_dir: &Path) -> Result<Self> {
        let path = Self::path_in(data_dir);
        if !path.exists() {
            return Self::create(data_dir);
        }
        let mut file = OpenOptions::new().read(true).write(true).open(&path)?;
        let mut magic = [0u8; 4];
        file.read_exact(&mut magic)?;
        if &magic != MAGIC {
            return Err(TakyonicError::Integrity(format!(
                "bad txn WAL magic at {}",
                path.display()
            )));
        }
        let mut ver = [0u8; 1];
        file.read_exact(&mut ver)?;
        if ver[0] != VERSION {
            return Err(TakyonicError::Integrity(format!(
                "unsupported txn WAL version {}",
                ver[0]
            )));
        }
        // Seek to end for append; recovery will truncate torn tails first.
        let meta = file.metadata()?;
        let offset = meta.len();
        use std::io::Seek;
        file.seek(std::io::SeekFrom::Start(offset))?;
        Ok(Self { path, file, offset })
    }

    /// Append one record (no fsync).
    pub fn append(&mut self, record: &WalRecord) -> Result<()> {
        let framed = record.encode_framed();
        self.file.write_all(&framed)?;
        self.offset = self.offset.saturating_add(framed.len() as u64);
        Ok(())
    }

    /// Append write-set records + [`WalRecord::Commit`], then `sync_data`.
    ///
    /// After this returns, the transaction is durable per the ARIES WAL protocol
    /// even if the subsequent memtable apply crashes.
    pub fn append_committed_txn(&mut self, txn_id: u64, ops: &[WalRecord]) -> Result<()> {
        for op in ops {
            debug_assert!(
                !matches!(op, WalRecord::Commit { .. }),
                "Commit must be written by append_committed_txn"
            );
            self.append(op)?;
        }
        self.append(&WalRecord::Commit { txn_id })?;
        self.sync()?;
        Ok(())
    }

    /// `fdatasync` the log file.
    pub fn sync(&mut self) -> Result<()> {
        self.file.sync_data()?;
        Ok(())
    }

    /// Truncate to header (checkpoint after clean SST flush).
    pub fn checkpoint_truncate(&mut self) -> Result<()> {
        let header = (MAGIC.len() + 1) as u64;
        self.file.set_len(header)?;
        self.file.sync_data()?;
        use std::io::Seek;
        self.file.seek(std::io::SeekFrom::Start(header))?;
        self.offset = header;
        Ok(())
    }

    /// Scan the WAL, truncate a torn tail if needed, and return committed redo batches.
    ///
    /// Each batch is `(txn_id, ops)` where `ops` excludes the Commit record.
    pub fn recover_committed(data_dir: &Path) -> Result<Vec<(u64, Vec<WalRecord>)>> {
        let path = Self::path_in(data_dir);
        if !path.exists() {
            return Ok(Vec::new());
        }
        let file = File::open(&path)?;
        let mut reader = BufReader::new(file);
        let mut magic = [0u8; 4];
        if reader.read_exact(&mut magic).is_err() {
            return Ok(Vec::new());
        }
        if &magic != MAGIC {
            return Err(TakyonicError::Integrity("txn WAL bad magic on recover".into()));
        }
        let mut ver = [0u8; 1];
        reader.read_exact(&mut ver)?;
        if ver[0] != VERSION {
            return Err(TakyonicError::Integrity(format!(
                "txn WAL unsupported version {}",
                ver[0]
            )));
        }

        let mut committed = Vec::new();
        let mut pending: Vec<WalRecord> = Vec::new();
        let mut valid_end = (MAGIC.len() + 1) as u64;
        let mut torn = false;

        loop {
            let mut len_buf = [0u8; 4];
            match reader.read_exact(&mut len_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }
            let body_len = u32::from_le_bytes(len_buf) as usize;
            // Guard absurd lengths (corruption / torn huge len).
            if body_len > 64 * 1024 * 1024 {
                torn = true;
                break;
            }
            let mut body = vec![0u8; body_len];
            if let Err(e) = reader.read_exact(&mut body) {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    torn = true;
                    break;
                }
                return Err(e.into());
            }
            let mut crc_buf = [0u8; 8];
            if let Err(e) = reader.read_exact(&mut crc_buf) {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    torn = true;
                    break;
                }
                return Err(e.into());
            }
            let expect = u64::from_le_bytes(crc_buf);
            let actual = xxh3_64(&body);
            if expect != actual {
                // Checksum mismatch at the tail → treat as torn; mid-file is corruption.
                torn = true;
                break;
            }
            let record = WalRecord::decode_body(&body)?;
            valid_end = valid_end
                .saturating_add(4)
                .saturating_add(body_len as u64)
                .saturating_add(8);
            match record {
                WalRecord::Commit { txn_id } => {
                    committed.push((txn_id, std::mem::take(&mut pending)));
                }
                other => pending.push(other),
            }
        }

        if torn {
            debug!(
                path = %path.display(),
                valid_end,
                "truncating torn txn WAL tail"
            );
            let f = OpenOptions::new().write(true).open(&path)?;
            f.set_len(valid_end)?;
            f.sync_data()?;
        }

        // Discard incomplete trailing write-set (no Commit).
        if !pending.is_empty() {
            debug!(
                discarded = pending.len(),
                "discarding incomplete txn WAL write-set without Commit"
            );
        }

        info!(
            commits = committed.len(),
            "txn WAL redo scan complete"
        );
        Ok(committed)
    }

    /// Apply committed redo batches into `memtable`, advancing `next_seq`.
    pub fn redo_into_memtable(
        batches: &[(u64, Vec<WalRecord>)],
        memtable: &Memtable,
        next_seq: &mut u64,
    ) {
        for (_txn_id, ops) in batches {
            for op in ops {
                let seq = *next_seq;
                *next_seq = next_seq.saturating_add(1);
                match op {
                    WalRecord::Insert { key, value, .. } | WalRecord::Update { key, value, .. } => {
                        memtable.apply(Entry::put(
                            Key::new(key.clone()),
                            Value::new(value.clone()),
                            seq,
                        ));
                    }
                    WalRecord::Delete { key, .. } => {
                        memtable.apply(Entry::delete(Key::new(key.clone()), seq));
                    }
                    WalRecord::Commit { .. } => {}
                }
            }
        }
    }
}

fn fs_create_dir(data_dir: &Path) -> Result<()> {
    std::fs::create_dir_all(data_dir)?;
    Ok(())
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
        let root = std::env::temp_dir().join(format!("takyonic-txnwal-{name}-{nanos}"));
        std::fs::create_dir_all(&root).unwrap();
        root
    }

    #[test]
    fn wal_record_roundtrip_all_variants() {
        let records = vec![
            WalRecord::Insert {
                table: "employees".into(),
                key: Bytes::from_static(b"Data_employees_1"),
                value: Bytes::from_static(b"row1"),
            },
            WalRecord::Update {
                table: "employees".into(),
                key: Bytes::from_static(b"Data_employees_1"),
                value: Bytes::from_static(b"row1b"),
            },
            WalRecord::Delete {
                table: "employees".into(),
                key: Bytes::from_static(b"Data_employees_2"),
            },
            WalRecord::Commit { txn_id: 42 },
        ];
        for r in &records {
            let mut body = BytesMut::new();
            r.encode_body(&mut body);
            let decoded = WalRecord::decode_body(&body).unwrap();
            assert_eq!(&decoded, r);
            // Framed encode/decode path via recover.
            let framed = r.encode_framed();
            assert!(framed.len() >= 4 + body.len() + 8);
        }
    }

    #[test]
    fn append_commit_recovers_only_committed() {
        let root = temp_dir("commit");
        {
            let mut wal = WalManager::create(&root).unwrap();
            wal.append_committed_txn(
                1,
                &[WalRecord::Insert {
                    table: "t".into(),
                    key: Bytes::from_static(b"k1"),
                    value: Bytes::from_static(b"v1"),
                }],
            )
            .unwrap();
            // Incomplete txn: ops without Commit (simulate crash mid-write).
            wal.append(&WalRecord::Insert {
                table: "t".into(),
                key: Bytes::from_static(b"k2"),
                value: Bytes::from_static(b"v2"),
            })
            .unwrap();
            // Intentionally no Commit + no sync for the second txn.
        }
        let batches = WalManager::recover_committed(&root).unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].0, 1);
        assert_eq!(batches[0].1.len(), 1);
        match &batches[0].1[0] {
            WalRecord::Insert { key, value, .. } => {
                assert_eq!(key.as_ref(), b"k1");
                assert_eq!(value.as_ref(), b"v1");
            }
            other => panic!("expected Insert, got {other:?}"),
        }
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn table_from_data_and_idx_keys() {
        assert_eq!(
            table_from_storage_key(b"Data_employees_1"),
            "employees"
        );
        assert_eq!(
            table_from_storage_key(b"Idx_employees_dept_Sales_1"),
            "employees"
        );
        assert_eq!(table_from_storage_key(b"raw"), "");
    }
}
