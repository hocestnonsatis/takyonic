//! Immutable SST files, mmap readers, and reader pinning.
//!
//! SST files are created through a temporary file and atomically renamed. Once
//! registered, they are immutable. [`SstRegistry`] is the only deletion path:
//! retirement first prevents new pins, then unlink is deferred until every
//! [`SstPin`] has dropped. This prevents an mmap reader from touching storage
//! after compaction has truncated or removed its backing file.

use std::fs::{File, OpenOptions};
use std::io::{Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::{BufMut, Bytes, BytesMut};
use dashmap::DashMap;
use memmap2::{Mmap, MmapOptions};
use parking_lot::Mutex;
use tracing::{debug, trace};
use xxhash_rust::xxh3::xxh3_64;

use crate::error::{Result, TakyonicError};
use crate::types::{Entry, Key, Value};

/// Stable SST file identifier.
pub type SstId = u64;

const MAGIC: &[u8; 4] = b"TKYS";
const VERSION: u32 = 1;
const HEADER_LEN: usize = 8;
const FOOTER_MAGIC: &[u8; 4] = b"TSFT";
const FOOTER_LEN: usize = 36;
const CHECKSUM_LEN: usize = 8;
const FLAG_TOMBSTONE: u8 = 1;
const BLOOM_BITS_PER_KEY: usize = 10;
const BLOOM_HASHES: u8 = 7;

#[derive(Clone, Debug)]
struct BlockMeta {
    first_key: Bytes,
    last_key: Bytes,
    offset: u64,
    len: u32,
}

/// Metadata returned after an SST has been durably written.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SstInfo {
    /// Stable file identifier.
    pub id: SstId,
    /// Final immutable path.
    pub path: PathBuf,
    /// Number of entries written.
    pub entry_count: u64,
    /// Total file size in bytes.
    pub file_size: u64,
}

/// Immutable SST writer.
pub struct SstWriter;

impl SstWriter {
    /// Write sorted entries to an SST using data blocks near `block_size`.
    ///
    /// The file is built at a sibling temporary path, synced, and atomically
    /// renamed to `path`. Entries must be strictly ordered by user key.
    pub fn write(
        id: SstId,
        path: impl Into<PathBuf>,
        entries: &[Entry],
        block_size: usize,
    ) -> Result<SstInfo> {
        Self::write_inner(id, path.into(), entries, block_size, |_| {})
    }

    /// Write an SST while invoking `pace` before each substantial disk write.
    ///
    /// Used by compaction workers to cap background bandwidth and preserve the
    /// WAL/Raft fsync fast path. Normal memtable flushes use [`Self::write`].
    pub(crate) fn write_paced<F>(
        id: SstId,
        path: impl Into<PathBuf>,
        entries: &[Entry],
        block_size: usize,
        pace: F,
    ) -> Result<SstInfo>
    where
        F: FnMut(usize),
    {
        Self::write_inner(id, path.into(), entries, block_size, pace)
    }

    fn write_inner<F>(
        id: SstId,
        path: PathBuf,
        entries: &[Entry],
        block_size: usize,
        mut pace: F,
    ) -> Result<SstInfo>
    where
        F: FnMut(usize),
    {
        if block_size == 0 {
            return Err(TakyonicError::Config("SST block_size must be > 0".into()));
        }
        validate_entries(entries)?;

        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let temp_path = temporary_path(&path, id);
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temp_path)?;

        let result = (|| {
            file.write_all(MAGIC)?;
            file.write_all(&VERSION.to_le_bytes())?;

            let mut metas = Vec::new();
            let mut block_entries: Vec<&Entry> = Vec::new();
            let mut estimated = 4usize;

            for entry in entries {
                let encoded_len = encoded_entry_len(entry);
                if !block_entries.is_empty() && estimated + encoded_len > block_size {
                    // Never split versions of the same user key across blocks —
                    // index ranges are user-key based and get_at must see the
                    // full version chain in one block.
                    let same_user = block_entries.last().is_some_and(|e| e.key == entry.key);
                    if !same_user {
                        metas.push(write_data_block(&mut file, &block_entries, &mut pace)?);
                        block_entries.clear();
                        estimated = 4;
                    }
                }
                block_entries.push(entry);
                estimated += encoded_len;
            }
            if !block_entries.is_empty() {
                metas.push(write_data_block(&mut file, &block_entries, &mut pace)?);
            }

            let index = encode_index(&metas);
            let index_offset = file.stream_position()?;
            pace(index.len() + CHECKSUM_LEN);
            write_checksummed_block(&mut file, &index)?;
            let index_len = checked_u32(index.len() + CHECKSUM_LEN, "index block")?;

            let filter = encode_filter(entries);
            let filter_offset = file.stream_position()?;
            pace(filter.len() + CHECKSUM_LEN);
            write_checksummed_block(&mut file, &filter)?;
            let filter_len = checked_u32(filter.len() + CHECKSUM_LEN, "filter block")?;

            file.write_all(&index_offset.to_le_bytes())?;
            file.write_all(&index_len.to_le_bytes())?;
            file.write_all(&filter_offset.to_le_bytes())?;
            file.write_all(&filter_len.to_le_bytes())?;
            file.write_all(&(entries.len() as u64).to_le_bytes())?;
            file.write_all(FOOTER_MAGIC)?;
            file.flush()?;
            file.sync_all()?;

            std::fs::rename(&temp_path, &path)?;
            if let Some(parent) = path.parent() {
                File::open(parent)?.sync_all()?;
            }
            let file_size = std::fs::metadata(&path)?.len();
            debug!(id, path = %path.display(), entries = entries.len(), file_size, "SST written");
            Ok(SstInfo {
                id,
                path: path.clone(),
                entry_count: entries.len() as u64,
                file_size,
            })
        })();

        if result.is_err() {
            let _ = std::fs::remove_file(&temp_path);
        }
        result
    }
}

fn validate_entries(entries: &[Entry]) -> Result<()> {
    for entry in entries {
        match (entry.tombstone, entry.value.is_some()) {
            (true, true) => {
                return Err(TakyonicError::Integrity(
                    "SST tombstone must not contain a value".into(),
                ));
            }
            (false, false) => {
                return Err(TakyonicError::Integrity(
                    "SST put must contain a value".into(),
                ));
            }
            _ => {}
        }
    }
    if entries
        .windows(2)
        .any(|pair| match pair[0].key.cmp(&pair[1].key) {
            std::cmp::Ordering::Greater => true,
            std::cmp::Ordering::Equal => pair[0].seq <= pair[1].seq,
            std::cmp::Ordering::Less => false,
        })
    {
        return Err(TakyonicError::Integrity(
            "SST entries must be ordered by user key ASC, commit_ts DESC".into(),
        ));
    }
    Ok(())
}

fn temporary_path(path: &Path, id: SstId) -> PathBuf {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("sst");
    path.with_file_name(format!(".{name}.{id}.tmp"))
}

fn encoded_entry_len(entry: &Entry) -> usize {
    8 + 1
        + 4
        + 4
        + entry.key.as_bytes().len()
        + entry
            .value
            .as_ref()
            .map(|value| value.as_bytes().len())
            .unwrap_or(0)
}

fn checked_u32(value: usize, what: &str) -> Result<u32> {
    u32::try_from(value).map_err(|_| TakyonicError::Integrity(format!("{what} exceeds u32 length")))
}

fn write_data_block<F>(file: &mut File, entries: &[&Entry], pace: &mut F) -> Result<BlockMeta>
where
    F: FnMut(usize),
{
    let mut payload = BytesMut::with_capacity(
        4 + entries
            .iter()
            .map(|entry| encoded_entry_len(entry))
            .sum::<usize>(),
    );
    payload.put_u32_le(checked_u32(entries.len(), "data block entry count")?);
    for entry in entries {
        encode_entry(entry, &mut payload)?;
    }

    let offset = file.stream_position()?;
    pace(payload.len() + CHECKSUM_LEN);
    write_checksummed_block(file, &payload)?;
    let len = checked_u32(payload.len() + CHECKSUM_LEN, "data block")?;
    Ok(BlockMeta {
        first_key: Bytes::copy_from_slice(entries[0].key.as_bytes()),
        last_key: Bytes::copy_from_slice(entries[entries.len() - 1].key.as_bytes()),
        offset,
        len,
    })
}

fn encode_entry(entry: &Entry, out: &mut BytesMut) -> Result<()> {
    out.put_u64_le(entry.seq);
    out.put_u8(if entry.tombstone { FLAG_TOMBSTONE } else { 0 });
    out.put_u32_le(checked_u32(entry.key.as_bytes().len(), "key")?);
    let value_len = entry
        .value
        .as_ref()
        .map(|value| value.as_bytes().len())
        .unwrap_or(0);
    out.put_u32_le(checked_u32(value_len, "value")?);
    out.put_slice(entry.key.as_bytes());
    if let Some(value) = &entry.value {
        out.put_slice(value.as_bytes());
    }
    Ok(())
}

fn write_checksummed_block(file: &mut File, payload: &[u8]) -> Result<()> {
    file.write_all(payload)?;
    file.write_all(&xxh3_64(payload).to_le_bytes())?;
    Ok(())
}

fn encode_index(metas: &[BlockMeta]) -> BytesMut {
    let mut out = BytesMut::new();
    out.put_u32_le(metas.len() as u32);
    for meta in metas {
        out.put_u32_le(meta.first_key.len() as u32);
        out.put_slice(&meta.first_key);
        out.put_u32_le(meta.last_key.len() as u32);
        out.put_slice(&meta.last_key);
        out.put_u64_le(meta.offset);
        out.put_u32_le(meta.len);
    }
    out
}

fn bloom_hashes(key: &[u8], bit_count: usize) -> impl Iterator<Item = usize> {
    let hash = xxh3_64(key);
    let delta = hash.rotate_left(17) | 1;
    (0..BLOOM_HASHES)
        .map(move |n| hash.wrapping_add(u64::from(n).wrapping_mul(delta)) as usize % bit_count)
}

fn encode_filter(entries: &[Entry]) -> BytesMut {
    let bit_count = (entries.len() * BLOOM_BITS_PER_KEY)
        .max(64)
        .next_multiple_of(8);
    let mut bits = vec![0u8; bit_count / 8];
    for entry in entries {
        for bit in bloom_hashes(entry.key.as_bytes(), bit_count) {
            bits[bit / 8] |= 1 << (bit % 8);
        }
    }
    let mut out = BytesMut::with_capacity(5 + bits.len());
    out.put_u32_le(bit_count as u32);
    out.put_u8(BLOOM_HASHES);
    out.put_slice(&bits);
    out
}

/// mmap-backed immutable SST reader.
pub struct SstReader {
    mmap: Mmap,
    index: Vec<BlockMeta>,
    filter_offset: usize,
    filter_len: usize,
    entry_count: u64,
}

impl SstReader {
    /// Open and validate an immutable SST file.
    ///
    /// Kept private so every mmap is owned by [`SstRegistry`] and therefore
    /// participates in the mandatory pin/retire lifecycle.
    fn open(path: impl AsRef<Path>) -> Result<Self> {
        let file = File::open(path)?;
        // SAFETY: SstReader is only exposed through the pinning registry for
        // production use. Registered files are immutable, and retirement never
        // truncates/unlinks while a pin (and therefore this mmap) is active.
        let mmap = unsafe { MmapOptions::new().map(&file)? };
        if mmap.len() < HEADER_LEN + FOOTER_LEN {
            return Err(TakyonicError::Integrity("SST file is too short".into()));
        }
        if &mmap[..4] != MAGIC {
            return Err(TakyonicError::Integrity("invalid SST magic".into()));
        }
        if read_u32(&mmap, 4)? != VERSION {
            return Err(TakyonicError::Integrity("unsupported SST version".into()));
        }

        let footer = mmap.len() - FOOTER_LEN;
        let index_offset = read_u64(&mmap, footer)? as usize;
        let index_len = read_u32(&mmap, footer + 8)? as usize;
        let filter_offset = read_u64(&mmap, footer + 12)? as usize;
        let filter_len = read_u32(&mmap, footer + 20)? as usize;
        let entry_count = read_u64(&mmap, footer + 24)?;
        if &mmap[footer + 32..footer + 36] != FOOTER_MAGIC {
            return Err(TakyonicError::Integrity("invalid SST footer magic".into()));
        }
        validate_range(index_offset, index_len, footer, "index")?;
        validate_range(filter_offset, filter_len, footer, "filter")?;
        if index_offset + index_len > filter_offset {
            return Err(TakyonicError::Integrity(
                "SST index/filter ranges overlap".into(),
            ));
        }

        let index_payload =
            checked_payload(&mmap[index_offset..index_offset + index_len], "index")?;
        let index = decode_index(index_payload, index_offset)?;
        let _ = checked_payload(&mmap[filter_offset..filter_offset + filter_len], "filter")?;

        Ok(Self {
            mmap,
            index,
            filter_offset,
            filter_len,
            entry_count,
        })
    }

    /// Number of entries declared in the SST footer.
    #[inline]
    pub fn entry_count(&self) -> u64 {
        self.entry_count
    }

    /// Number of data blocks.
    #[inline]
    pub fn block_count(&self) -> usize {
        self.index.len()
    }

    /// Return whether the Bloom filter may contain `key`.
    ///
    /// `false` is definitive; `true` requires an index/data-block lookup.
    pub fn may_contain(&self, key: &Key) -> Result<bool> {
        let block = &self.mmap[self.filter_offset..self.filter_offset + self.filter_len];
        let payload = checked_payload(block, "filter")?;
        if payload.len() < 5 {
            return Err(TakyonicError::Integrity(
                "SST filter block is truncated".into(),
            ));
        }
        let bit_count = u32::from_le_bytes(payload[..4].try_into().unwrap()) as usize;
        let hash_count = payload[4];
        let bits = &payload[5..];
        if bit_count == 0 || !bit_count.is_multiple_of(8) || bits.len() != bit_count / 8 {
            return Err(TakyonicError::Integrity(
                "invalid SST filter dimensions".into(),
            ));
        }
        if hash_count != BLOOM_HASHES {
            return Err(TakyonicError::Integrity(
                "unsupported SST filter hash count".into(),
            ));
        }
        Ok(
            bloom_hashes(key.as_bytes(), bit_count)
                .all(|bit| bits[bit / 8] & (1 << (bit % 8)) != 0),
        )
    }

    /// Point lookup including tombstones (latest version).
    pub fn get_entry(&self, key: &Key) -> Result<Option<Entry>> {
        self.get_entry_at(key, u64::MAX)
    }

    /// Snapshot point lookup: highest `commit_ts <= read_ts`.
    pub fn get_entry_at(
        &self,
        key: &Key,
        read_ts: crate::types::CommitTs,
    ) -> Result<Option<Entry>> {
        if !self.may_contain(key)? {
            return Ok(None);
        }
        let mut best: Option<Entry> = None;
        for meta in &self.index {
            if meta.last_key.as_ref() < key.as_bytes() {
                continue;
            }
            if meta.first_key.as_ref() > key.as_bytes() {
                break;
            }
            let start = meta.offset as usize;
            let block = &self.mmap[start..start + meta.len as usize];
            let payload = checked_payload(block, "data")?;
            if let Some(entry) = decode_data_lookup_at(payload, key, read_ts)? {
                match &best {
                    Some(b) if b.seq >= entry.seq => {}
                    _ => best = Some(entry),
                }
            }
        }
        Ok(best)
    }

    /// Point lookup returning only live values.
    pub fn get(&self, key: &Key) -> Result<Option<Value>> {
        Ok(self
            .get_entry(key)?
            .and_then(|entry| (!entry.tombstone).then_some(entry.value).flatten()))
    }

    /// Decode all entries in key order for compaction.
    ///
    /// The caller must hold the surrounding [`SstPin`] for the duration of
    /// this call. Every data-block checksum is verified before decoding.
    pub fn entries(&self) -> Result<Vec<Entry>> {
        let mut entries = Vec::with_capacity(self.entry_count as usize);
        for block in 0..self.index.len() {
            entries.extend(self.block_entries(block)?);
        }
        if entries.len() as u64 != self.entry_count {
            return Err(TakyonicError::Integrity(format!(
                "SST footer declares {} entries, decoded {}",
                self.entry_count,
                entries.len()
            )));
        }
        Ok(entries)
    }

    pub(crate) fn block_entries(&self, block: usize) -> Result<Vec<Entry>> {
        let meta = self
            .index
            .get(block)
            .ok_or_else(|| TakyonicError::Integrity("SST block index out of range".into()))?;
        let start = meta.offset as usize;
        let bytes = &self.mmap[start..start + meta.len as usize];
        let payload = checked_payload(bytes, "data")?;
        let mut entries = Vec::new();
        decode_data_entries(payload, &mut entries)?;
        Ok(entries)
    }
}

fn read_u32(data: &[u8], offset: usize) -> Result<u32> {
    let bytes = data
        .get(offset..offset + 4)
        .ok_or_else(|| TakyonicError::Integrity("truncated u32".into()))?;
    Ok(u32::from_le_bytes(bytes.try_into().unwrap()))
}

fn read_u64(data: &[u8], offset: usize) -> Result<u64> {
    let bytes = data
        .get(offset..offset + 8)
        .ok_or_else(|| TakyonicError::Integrity("truncated u64".into()))?;
    Ok(u64::from_le_bytes(bytes.try_into().unwrap()))
}

fn validate_range(offset: usize, len: usize, end: usize, name: &str) -> Result<()> {
    if len < CHECKSUM_LEN
        || offset < HEADER_LEN
        || offset
            .checked_add(len)
            .is_none_or(|block_end| block_end > end)
    {
        return Err(TakyonicError::Integrity(format!(
            "invalid SST {name} block range"
        )));
    }
    Ok(())
}

fn checked_payload<'a>(block: &'a [u8], name: &str) -> Result<&'a [u8]> {
    if block.len() < CHECKSUM_LEN {
        return Err(TakyonicError::Integrity(format!(
            "SST {name} block is too short"
        )));
    }
    let split = block.len() - CHECKSUM_LEN;
    let payload = &block[..split];
    let expected = u64::from_le_bytes(block[split..].try_into().unwrap());
    let actual = xxh3_64(payload);
    if expected != actual {
        return Err(TakyonicError::Integrity(format!(
            "SST {name} checksum mismatch"
        )));
    }
    Ok(payload)
}

fn decode_index(payload: &[u8], data_end: usize) -> Result<Vec<BlockMeta>> {
    let mut cursor = SliceCursor::new(payload);
    let count = cursor.u32()? as usize;
    let mut metas = Vec::with_capacity(count);
    let mut previous_last: Option<Bytes> = None;
    for _ in 0..count {
        let first_key_len = cursor.u32()? as usize;
        let first_key = cursor.bytes(first_key_len)?;
        let last_key_len = cursor.u32()? as usize;
        let last_key = cursor.bytes(last_key_len)?;
        let offset = cursor.u64()?;
        let len = cursor.u32()?;
        validate_range(offset as usize, len as usize, data_end, "data")?;
        if first_key > last_key {
            return Err(TakyonicError::Integrity(
                "SST index has inverted key range".into(),
            ));
        }
        if previous_last
            .as_ref()
            .is_some_and(|last| last.as_ref() > first_key.as_ref())
        {
            return Err(TakyonicError::Integrity(
                "SST index key ranges overlap".into(),
            ));
        }
        previous_last = Some(last_key.clone());
        metas.push(BlockMeta {
            first_key,
            last_key,
            offset,
            len,
        });
    }
    cursor.finish()?;
    Ok(metas)
}

fn decode_data_lookup_at(
    payload: &[u8],
    target: &Key,
    read_ts: crate::types::CommitTs,
) -> Result<Option<Entry>> {
    let mut cursor = SliceCursor::new(payload);
    let count = cursor.u32()? as usize;
    for _ in 0..count {
        let seq = cursor.u64()?;
        let flags = cursor.u8()?;
        if flags & !FLAG_TOMBSTONE != 0 {
            return Err(TakyonicError::Integrity("unknown SST entry flags".into()));
        }
        let key_len = cursor.u32()? as usize;
        let value_len = cursor.u32()? as usize;
        let key = cursor.bytes(key_len)?;
        let value = cursor.bytes(value_len)?;
        match key.as_ref().cmp(target.as_bytes()) {
            std::cmp::Ordering::Less => continue,
            std::cmp::Ordering::Greater => return Ok(None),
            std::cmp::Ordering::Equal => {
                if seq > read_ts {
                    continue;
                }
                let tombstone = flags & FLAG_TOMBSTONE != 0;
                if tombstone && !value.is_empty() {
                    return Err(TakyonicError::Integrity("SST tombstone has a value".into()));
                }
                return Ok(Some(if tombstone {
                    Entry::delete(Key::new(key), seq)
                } else {
                    Entry::put(Key::new(key), Value::new(value), seq)
                }));
            }
        }
    }
    cursor.finish()?;
    Ok(None)
}

fn decode_data_entries(payload: &[u8], entries: &mut Vec<Entry>) -> Result<()> {
    let mut cursor = SliceCursor::new(payload);
    let count = cursor.u32()? as usize;
    for _ in 0..count {
        let seq = cursor.u64()?;
        let flags = cursor.u8()?;
        if flags & !FLAG_TOMBSTONE != 0 {
            return Err(TakyonicError::Integrity("unknown SST entry flags".into()));
        }
        let key_len = cursor.u32()? as usize;
        let value_len = cursor.u32()? as usize;
        let key = Key::new(cursor.bytes(key_len)?);
        let value = cursor.bytes(value_len)?;
        let tombstone = flags & FLAG_TOMBSTONE != 0;
        if tombstone {
            if !value.is_empty() {
                return Err(TakyonicError::Integrity("SST tombstone has a value".into()));
            }
            entries.push(Entry::delete(key, seq));
        } else {
            entries.push(Entry::put(key, Value::new(value), seq));
        }
    }
    cursor.finish()
}

struct SliceCursor<'a> {
    data: &'a [u8],
    offset: usize,
}

impl<'a> SliceCursor<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, offset: 0 }
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or_else(|| TakyonicError::Integrity("SST length overflow".into()))?;
        let bytes = self
            .data
            .get(self.offset..end)
            .ok_or_else(|| TakyonicError::Integrity("SST block is truncated".into()))?;
        self.offset = end;
        Ok(bytes)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }

    fn bytes(&mut self, len: usize) -> Result<Bytes> {
        Ok(Bytes::copy_from_slice(self.take(len)?))
    }

    fn finish(self) -> Result<()> {
        if self.offset != self.data.len() {
            return Err(TakyonicError::Integrity(
                "SST block has trailing bytes".into(),
            ));
        }
        Ok(())
    }
}

struct SstHandle {
    id: SstId,
    path: PathBuf,
    reader: SstReader,
}

/// Reference-counted read pin. Keeping this value alive keeps the mmap and its
/// backing SST alive, even after compaction retires the file.
#[derive(Clone)]
pub struct SstPin(Arc<SstHandle>);

impl SstPin {
    /// Pinned SST identifier.
    #[inline]
    pub fn id(&self) -> SstId {
        self.0.id
    }

    /// Pinned SST path.
    #[inline]
    pub fn path(&self) -> &Path {
        &self.0.path
    }

    /// Access the mmap reader while this pin is alive.
    #[inline]
    pub fn reader(&self) -> &SstReader {
        &self.0.reader
    }
}

/// Result of retiring or reaping an SST.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeleteStatus {
    /// New pins are blocked, but existing readers still hold references.
    Deferred,
    /// The mmap was dropped and the backing file was unlinked.
    Deleted,
    /// No active or retired SST exists for this identifier.
    NotFound,
}

/// Concurrent SST registry enforcing Pin/Unpin before file deletion.
#[derive(Default)]
pub struct SstRegistry {
    active: DashMap<SstId, Arc<SstHandle>>,
    retired: DashMap<SstId, Arc<SstHandle>>,
    lifecycle: Mutex<()>,
}

impl SstRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// mmap and register an immutable SST for reads.
    pub fn register(&self, id: SstId, path: impl Into<PathBuf>) -> Result<()> {
        let _lifecycle = self.lifecycle.lock();
        if self.active.contains_key(&id) || self.retired.contains_key(&id) {
            return Err(TakyonicError::Integrity(format!(
                "SST id {id} is already registered"
            )));
        }
        let path = path.into();
        let reader = SstReader::open(&path)?;
        let handle = Arc::new(SstHandle { id, path, reader });
        self.active.insert(id, handle);
        Ok(())
    }

    /// Acquire a read pin. Returns `None` after retirement begins.
    pub fn pin(&self, id: SstId) -> Option<SstPin> {
        self.active
            .get(&id)
            .map(|handle| SstPin(Arc::clone(handle.value())))
    }

    /// Prevent new pins and attempt safe deletion.
    ///
    /// If readers remain, the file moves to the retired set and is not
    /// truncated or unlinked. Call [`Self::reap`] after readers drain.
    pub fn retire(&self, id: SstId) -> Result<DeleteStatus> {
        {
            let _lifecycle = self.lifecycle.lock();
            if let Some((_, handle)) = self.active.remove(&id) {
                self.retired.insert(id, handle);
                trace!(id, "SST retired; new pins disabled");
            } else if !self.retired.contains_key(&id) {
                return Ok(DeleteStatus::NotFound);
            }
        }
        self.reap(id)
    }

    /// Unlink a retired SST only when no [`SstPin`] remains.
    pub fn reap(&self, id: SstId) -> Result<DeleteStatus> {
        let _lifecycle = self.lifecycle.lock();
        let Some(handle) = self.retired.get(&id) else {
            return Ok(DeleteStatus::NotFound);
        };
        if Arc::strong_count(handle.value()) != 1 {
            return Ok(DeleteStatus::Deferred);
        }
        let path = handle.path.clone();
        drop(handle);

        let Some((_, handle)) = self.retired.remove(&id) else {
            return Ok(DeleteStatus::NotFound);
        };
        // No API can create new references once retired. Drop the final mmap
        // before unlinking, satisfying the strict no-unlink-while-pinned rule.
        drop(handle);
        match std::fs::remove_file(&path) {
            Ok(()) => {
                debug!(id, path = %path.display(), "retired SST unlinked");
                Ok(DeleteStatus::Deleted)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(DeleteStatus::Deleted),
            Err(error) => Err(error.into()),
        }
    }

    /// Number of SSTs accepting new pins.
    pub fn active_len(&self) -> usize {
        self.active.len()
    }

    /// Number of retired SSTs waiting for their final pin to drop.
    pub fn retired_len(&self) -> usize {
        self.retired.len()
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
        let path = std::env::temp_dir().join(format!("takyonic-sst-{name}-{nanos}"));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    fn sample_entries() -> Vec<Entry> {
        vec![
            Entry::put(&b"alpha"[..], &b"one"[..], 1),
            Entry::put(&b"bravo"[..], &b"two"[..], 2),
            Entry::delete(&b"charlie"[..], 3),
            Entry::put(&b"delta"[..], &b"four"[..], 4),
        ]
    }

    #[test]
    fn write_mmap_and_lookup_across_blocks() {
        let dir = temp_dir("lookup");
        let path = dir.join("000001.sst");
        let info = SstWriter::write(1, &path, &sample_entries(), 32).unwrap();
        assert_eq!(info.entry_count, 4);

        let registry = SstRegistry::new();
        registry.register(1, &path).unwrap();
        let pin = registry.pin(1).unwrap();
        let reader = pin.reader();
        assert!(reader.block_count() > 1);
        assert_eq!(
            reader
                .get(&Key::new(&b"bravo"[..]))
                .unwrap()
                .unwrap()
                .as_bytes(),
            b"two"
        );
        assert!(reader.get(&Key::new(&b"charlie"[..])).unwrap().is_none());
        assert!(
            reader
                .get_entry(&Key::new(&b"charlie"[..]))
                .unwrap()
                .unwrap()
                .tombstone
        );
        assert!(reader.get(&Key::new(&b"missing"[..])).unwrap().is_none());
        drop(pin);
        assert_eq!(registry.retire(1).unwrap(), DeleteStatus::Deleted);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn corruption_is_detected() {
        let dir = temp_dir("corrupt");
        let path = dir.join("000002.sst");
        SstWriter::write(2, &path, &sample_entries(), 64).unwrap();
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[HEADER_LEN + 2] ^= 0x80;
        std::fs::write(&path, bytes).unwrap();

        let registry = SstRegistry::new();
        registry.register(2, &path).unwrap();
        let pin = registry.pin(2).unwrap();
        let error = pin
            .reader()
            .get_entry(&Key::new(&b"alpha"[..]))
            .unwrap_err();
        assert!(matches!(error, TakyonicError::Integrity(_)));
        drop(pin);
        assert_eq!(registry.retire(2).unwrap(), DeleteStatus::Deleted);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn pin_defers_unlink_until_reader_drops() {
        let dir = temp_dir("pin");
        let path = dir.join("000003.sst");
        SstWriter::write(3, &path, &sample_entries(), 64).unwrap();

        let registry = SstRegistry::new();
        registry.register(3, &path).unwrap();
        let pin = registry.pin(3).unwrap();
        assert_eq!(registry.retire(3).unwrap(), DeleteStatus::Deferred);
        assert!(path.exists());
        assert!(registry.pin(3).is_none());
        assert_eq!(
            pin.reader()
                .get(&Key::new(&b"alpha"[..]))
                .unwrap()
                .unwrap()
                .as_bytes(),
            b"one"
        );

        drop(pin);
        assert_eq!(registry.reap(3).unwrap(), DeleteStatus::Deleted);
        assert!(!path.exists());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn unsorted_input_is_rejected_without_final_file() {
        let dir = temp_dir("unsorted");
        let path = dir.join("000004.sst");
        let entries = vec![
            Entry::put(&b"z"[..], &b"1"[..], 1),
            Entry::put(&b"a"[..], &b"2"[..], 2),
        ];
        assert!(SstWriter::write(4, &path, &entries, 64).is_err());
        assert!(!path.exists());
        std::fs::remove_dir_all(dir).unwrap();
    }
}
