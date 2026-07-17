//! Write-Ahead Log — durable ingestion path.
//!
//! Design goals for this module (hard constraint: never starve WAL fsync):
//! - Dedicated file under `wal_dir`, independent of SST / compaction I/O.
//! - Append-only encoding with XXH3 checksums for corruption detection.
//! - Durability via [`std::fs::File::sync_data`] (`fdatasync`) — metadata-light
//!   compared to full `sync_all`, keeping the fsync path cheap.
//!
//! Record layout (little-endian):
//! ```text
//! [u32 body_len][body...][u64 xxh3(body)]
//! body = [u8 flags][u64 seq][u32 key_len][key][u32 val_len][val]
//! flags bit0 = tombstone (val_len must be 0)
//! ```

use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Write};
use std::path::{Path, PathBuf};

use bytes::{BufMut, BytesMut};
use tracing::{debug, trace};
use xxhash_rust::xxh3::xxh3_64;

use crate::error::{Result, TakyonicError};
use crate::types::{Entry, Key, Value};

/// File magic written at the start of a new WAL segment.
const WAL_MAGIC: &[u8; 4] = b"TKYW";
/// On-disk format version.
const WAL_VERSION: u8 = 1;
/// Flag: entry is a tombstone delete.
const FLAG_TOMBSTONE: u8 = 0x01;

/// Encode a single WAL body (without length prefix or checksum).
fn encode_body(entry: &Entry, buf: &mut BytesMut) {
    let mut flags = 0u8;
    if entry.tombstone {
        flags |= FLAG_TOMBSTONE;
    }
    buf.put_u8(flags);
    buf.put_u64_le(entry.seq);
    let key = entry.key.as_bytes();
    buf.put_u32_le(key.len() as u32);
    buf.put_slice(key);
    match &entry.value {
        Some(v) if !entry.tombstone => {
            let val = v.as_bytes();
            buf.put_u32_le(val.len() as u32);
            buf.put_slice(val);
        }
        _ => {
            buf.put_u32_le(0);
        }
    }
}

/// Decode body bytes into an [`Entry`].
fn decode_body(mut body: &[u8]) -> Result<Entry> {
    if body.is_empty() {
        return Err(TakyonicError::Integrity("WAL body empty".into()));
    }
    let flags = body[0];
    body = &body[1..];
    if body.len() < 8 + 4 {
        return Err(TakyonicError::Integrity(
            "WAL body truncated (seq/key_len)".into(),
        ));
    }
    let seq = u64::from_le_bytes(body[..8].try_into().unwrap());
    body = &body[8..];
    let key_len = u32::from_le_bytes(body[..4].try_into().unwrap()) as usize;
    body = &body[4..];
    if body.len() < key_len + 4 {
        return Err(TakyonicError::Integrity("WAL body truncated (key)".into()));
    }
    let key = Key::new(bytes::Bytes::copy_from_slice(&body[..key_len]));
    body = &body[key_len..];
    let val_len = u32::from_le_bytes(body[..4].try_into().unwrap()) as usize;
    body = &body[4..];
    if body.len() != val_len {
        return Err(TakyonicError::Integrity(
            "WAL body truncated (value)".into(),
        ));
    }
    let tombstone = flags & FLAG_TOMBSTONE != 0;
    if tombstone {
        if val_len != 0 {
            return Err(TakyonicError::Integrity(
                "WAL tombstone with non-empty value".into(),
            ));
        }
        Ok(Entry::delete(key, seq))
    } else {
        let value = Value::new(bytes::Bytes::copy_from_slice(body));
        Ok(Entry::put(key, value, seq))
    }
}

/// Append-only WAL writer with lightweight durability.
pub struct WalWriter {
    path: PathBuf,
    file: File,
    /// Scratch buffer reused across appends to avoid per-write allocation.
    scratch: BytesMut,
}

impl WalWriter {
    /// Create a new WAL segment at `path`, writing magic + version header.
    pub fn create(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .read(true)
            .open(&path)?;
        file.write_all(WAL_MAGIC)?;
        file.write_all(&[WAL_VERSION])?;
        // Ensure header hits durable storage before any records.
        file.sync_data()?;
        debug!(path = %path.display(), "WAL segment created");
        Ok(Self {
            path,
            file,
            scratch: BytesMut::with_capacity(256),
        })
    }

    /// Open an existing WAL for append (does not rewrite header).
    pub fn open_append(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let file = OpenOptions::new().append(true).read(true).open(&path)?;
        Ok(Self {
            path,
            file,
            scratch: BytesMut::with_capacity(256),
        })
    }

    /// Path of this WAL segment.
    #[inline]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Append one record. Does **not** fsync — call [`Self::sync`] for durability.
    pub fn append(&mut self, entry: &Entry) -> Result<()> {
        self.scratch.clear();
        encode_body(entry, &mut self.scratch);
        let body = self.scratch.as_ref();
        let checksum = xxh3_64(body);
        let body_len = body.len() as u32;

        self.file.write_all(&body_len.to_le_bytes())?;
        self.file.write_all(body)?;
        self.file.write_all(&checksum.to_le_bytes())?;
        trace!(seq = entry.seq, body_len, "WAL append");
        Ok(())
    }

    /// Append then `sync_data` — the durable ingestion primitive.
    ///
    /// Uses `fdatasync`-style durability (file data only) so WAL stays on a
    /// fast path and is not coupled to compaction directory metadata ops.
    pub fn append_sync(&mut self, entry: &Entry) -> Result<()> {
        self.append(entry)?;
        self.sync()
    }

    /// Append many records without syncing. Prefer
    /// [`crate::group_commit::GroupCommitWal`] for concurrent writers.
    pub fn append_batch(&mut self, entries: &[Entry]) -> Result<()> {
        for entry in entries {
            self.append(entry)?;
        }
        Ok(())
    }

    /// Append a batch then perform **one** `sync_data` for the whole group.
    pub fn append_batch_sync(&mut self, entries: &[Entry]) -> Result<()> {
        self.append_batch(entries)?;
        self.sync()
    }

    /// Flush OS buffers and `sync_data` the WAL file.
    #[inline]
    pub fn sync(&mut self) -> Result<()> {
        self.file.flush()?;
        self.file.sync_data()?;
        Ok(())
    }
}

/// Streaming reader for WAL recovery / replay into a memtable.
pub struct WalReader {
    reader: BufReader<File>,
    path: PathBuf,
    file_len: u64,
    last_valid_offset: u64,
    torn_tail: bool,
}

impl WalReader {
    /// Open a WAL segment and validate magic/version.
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let file = File::open(&path)?;
        let file_len = file.metadata()?.len();
        let mut reader = BufReader::new(file);
        let mut magic = [0u8; 4];
        reader.read_exact(&mut magic)?;
        if &magic != WAL_MAGIC {
            return Err(TakyonicError::Integrity(format!(
                "bad WAL magic in {}",
                path.display()
            )));
        }
        let mut ver = [0u8; 1];
        reader.read_exact(&mut ver)?;
        if ver[0] != WAL_VERSION {
            return Err(TakyonicError::Integrity(format!(
                "unsupported WAL version {} in {}",
                ver[0],
                path.display()
            )));
        }
        Ok(Self {
            reader,
            path,
            file_len,
            last_valid_offset: (WAL_MAGIC.len() + 1) as u64,
            torn_tail: false,
        })
    }

    /// Path of the segment being read.
    #[inline]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Byte offset immediately after the last fully validated record.
    #[inline]
    pub fn last_valid_offset(&self) -> u64 {
        self.last_valid_offset
    }

    /// Whether recovery encountered an incomplete trailing record.
    #[inline]
    pub fn has_torn_tail(&self) -> bool {
        self.torn_tail
    }

    /// Read the next record, or `Ok(None)` at clean EOF or a torn final record.
    pub fn read_next(&mut self) -> Result<Option<Entry>> {
        let mut len_buf = [0u8; 4];
        match self.reader.read(&mut len_buf[..1]) {
            Ok(0) => return Ok(None),
            Ok(1) => {}
            Ok(_) => unreachable!("single-byte read returned more than one byte"),
            Err(e) => return Err(e.into()),
        }
        if let Err(error) = self.reader.read_exact(&mut len_buf[1..]) {
            if error.kind() == std::io::ErrorKind::UnexpectedEof {
                self.torn_tail = true;
                return Ok(None);
            }
            return Err(error.into());
        }
        let body_len = u32::from_le_bytes(len_buf) as usize;
        let record_end = self
            .last_valid_offset
            .checked_add(4)
            .and_then(|offset| offset.checked_add(body_len as u64))
            .and_then(|offset| offset.checked_add(8));
        if record_end.is_none_or(|end| end > self.file_len) {
            self.torn_tail = true;
            return Ok(None);
        }
        // Sanity cap: 64 MiB single record.
        if body_len > 64 * 1024 * 1024 {
            return Err(TakyonicError::Integrity(format!(
                "WAL record too large: {body_len} bytes"
            )));
        }
        let mut body = vec![0u8; body_len];
        if let Err(error) = self.reader.read_exact(&mut body) {
            if error.kind() == std::io::ErrorKind::UnexpectedEof {
                self.torn_tail = true;
                return Ok(None);
            }
            return Err(error.into());
        }
        let mut crc_buf = [0u8; 8];
        if let Err(error) = self.reader.read_exact(&mut crc_buf) {
            if error.kind() == std::io::ErrorKind::UnexpectedEof {
                self.torn_tail = true;
                return Ok(None);
            }
            return Err(error.into());
        }
        let expected = u64::from_le_bytes(crc_buf);
        let actual = xxh3_64(&body);
        if expected != actual {
            return Err(TakyonicError::Integrity(format!(
                "WAL checksum mismatch: expected {expected:#x}, got {actual:#x}"
            )));
        }
        let entry = decode_body(&body)?;
        self.last_valid_offset += 4 + body_len as u64 + 8;
        Ok(Some(entry))
    }

    /// Replay all records into `apply`, in file order.
    pub fn replay<F>(&mut self, mut apply: F) -> Result<u64>
    where
        F: FnMut(Entry),
    {
        let mut count = 0u64;
        while let Some(entry) = self.read_next()? {
            apply(entry);
            count += 1;
        }
        debug!(path = %self.path.display(), count, "WAL replay complete");
        Ok(count)
    }
}

/// Helper: next default segment path `wal_dir/000001.wal`.
pub fn segment_path(wal_dir: &Path, id: u64) -> PathBuf {
    wal_dir.join(format!("{id:06}.wal"))
}

/// Encode/decode helpers exposed for tests.
#[cfg(test)]
pub(crate) fn roundtrip_entry(entry: &Entry) -> Result<Entry> {
    let mut buf = BytesMut::new();
    encode_body(entry, &mut buf);
    let sum = xxh3_64(&buf);
    assert_eq!(sum, xxh3_64(buf.as_ref()));
    decode_body(&buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memtable::Memtable;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_wal_path(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("takyonic-wal-{name}-{nanos}.wal"))
    }

    #[test]
    fn encode_decode_put_and_delete() {
        let put = Entry::put(&b"key"[..], &b"val"[..], 7);
        let got = roundtrip_entry(&put).unwrap();
        assert_eq!(got, put);

        let del = Entry::delete(&b"key"[..], 8);
        let got = roundtrip_entry(&del).unwrap();
        assert_eq!(got, del);
    }

    #[test]
    fn append_sync_and_replay_into_memtable() {
        let path = temp_wal_path("replay");
        let _ = std::fs::remove_file(&path);

        {
            let mut wal = WalWriter::create(&path).unwrap();
            wal.append_sync(&Entry::put(&b"a"[..], &b"1"[..], 1))
                .unwrap();
            wal.append_sync(&Entry::put(&b"b"[..], &b"2"[..], 2))
                .unwrap();
            wal.append_sync(&Entry::delete(&b"a"[..], 3)).unwrap();
        }

        let mt = Memtable::new();
        let mut reader = WalReader::open(&path).unwrap();
        let n = reader.replay(|e| mt.apply(e)).unwrap();
        assert_eq!(n, 3);
        assert!(mt.get(&Key::new(&b"a"[..])).is_none());
        assert!(mt.is_tombstone(&Key::new(&b"a"[..])));
        assert_eq!(mt.get(&Key::new(&b"b"[..])).unwrap().as_bytes(), b"2");

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn checksum_detects_corruption() {
        let path = temp_wal_path("corrupt");
        let _ = std::fs::remove_file(&path);

        {
            let mut wal = WalWriter::create(&path).unwrap();
            wal.append_sync(&Entry::put(&b"x"[..], &b"y"[..], 1))
                .unwrap();
        }

        // Flip a byte in the body region (after 4-byte magic + 1-byte version + 4-byte len).
        let mut data = std::fs::read(&path).unwrap();
        let flip_at = 4 + 1 + 4; // first body byte
        data[flip_at] ^= 0xff;
        std::fs::write(&path, &data).unwrap();

        let mut reader = WalReader::open(&path).unwrap();
        let err = reader.read_next().unwrap_err();
        assert!(matches!(err, TakyonicError::Integrity(_)));

        let _ = std::fs::remove_file(&path);
    }
}
