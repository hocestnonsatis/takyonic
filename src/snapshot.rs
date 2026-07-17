//! SST-backed Raft snapshot codec.
//!
//! Because Takyonic is an LSM, a Raft snapshot is a flush of the memtable plus
//! a durable packaging of the active leveled SSTables — not a dump of a huge
//! in-memory tree. Wire format uses the `bytes` crate for zero-copy handoff.

use std::io::Write;
use std::path::{Path, PathBuf};

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::compaction::SstMeta;
use crate::error::{Result, TakyonicError};
use crate::membership::ClusterMembership;
use crate::types::Key;

const MAGIC: &[u8; 4] = b"TKYS";
const VERSION: u32 = 2;
const VERSION_V1: u32 = 1;

/// Metadata persisted beside the Raft log (`SNAPSHOT.meta`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SnapshotMeta {
    /// Highest applied index covered by the snapshot.
    pub last_included_index: u64,
    /// Term of the entry at [`Self::last_included_index`].
    pub last_included_term: u64,
}

impl SnapshotMeta {
    /// Encode to 16 little-endian bytes.
    pub fn encode(&self) -> [u8; 16] {
        let mut buf = [0u8; 16];
        buf[..8].copy_from_slice(&self.last_included_index.to_le_bytes());
        buf[8..].copy_from_slice(&self.last_included_term.to_le_bytes());
        buf
    }

    /// Decode from 16 little-endian bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 16 {
            return Err(TakyonicError::Raft("SNAPSHOT.meta truncated".into()));
        }
        Ok(Self {
            last_included_index: u64::from_le_bytes(bytes[..8].try_into().unwrap()),
            last_included_term: u64::from_le_bytes(bytes[8..16].try_into().unwrap()),
        })
    }

    /// Atomically write metadata under `dir/SNAPSHOT.meta`.
    pub fn write_to_dir(dir: &Path, meta: SnapshotMeta) -> Result<()> {
        std::fs::create_dir_all(dir)?;
        let tmp = dir.join("SNAPSHOT.meta.tmp");
        let dst = dir.join("SNAPSHOT.meta");
        {
            let mut file = std::fs::File::create(&tmp)?;
            file.write_all(&meta.encode())?;
            file.sync_all()?;
        }
        std::fs::rename(&tmp, &dst)?;
        std::fs::File::open(dir)?.sync_all()?;
        Ok(())
    }

    /// Load `dir/SNAPSHOT.meta` if present.
    pub fn read_from_dir(dir: &Path) -> Result<Option<SnapshotMeta>> {
        let path = dir.join("SNAPSHOT.meta");
        if !path.exists() {
            return Ok(None);
        }
        let bytes = std::fs::read(&path)?;
        Ok(Some(Self::decode(&bytes)?))
    }
}

/// One SST file packaged inside a snapshot blob.
#[derive(Clone, Debug)]
pub struct SnapshotSst {
    /// LSM level.
    pub level: usize,
    /// Stable SST id.
    pub id: u64,
    /// Smallest user key.
    pub smallest: Key,
    /// Largest user key.
    pub largest: Key,
    /// Raw SST file bytes.
    pub data: Bytes,
}

/// Full SST snapshot payload exchanged over `InstallSnapshot`.
#[derive(Clone, Debug)]
pub struct SnapshotPayload {
    /// Applied index covered by this snapshot.
    pub last_included_index: u64,
    /// Term at that index.
    pub last_included_term: u64,
    /// Cluster membership frozen at the snapshot boundary.
    pub membership: ClusterMembership,
    /// Packaged SSTables.
    pub files: Vec<SnapshotSst>,
}

impl SnapshotPayload {
    /// Encode into a contiguous `Bytes` blob (version 2 includes membership).
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_slice(MAGIC);
        buf.put_u32_le(VERSION);
        buf.put_u64_le(self.last_included_index);
        buf.put_u64_le(self.last_included_term);
        let mem = self.membership.encode();
        buf.put_u32_le(mem.len() as u32);
        buf.put_slice(&mem);
        buf.put_u32_le(self.files.len() as u32);
        for f in &self.files {
            buf.put_u32_le(f.level as u32);
            buf.put_u64_le(f.id);
            let sk = f.smallest.as_bytes();
            let lk = f.largest.as_bytes();
            buf.put_u32_le(sk.len() as u32);
            buf.put_slice(sk);
            buf.put_u32_le(lk.len() as u32);
            buf.put_slice(lk);
            buf.put_u64_le(f.data.len() as u64);
            buf.put_slice(&f.data);
        }
        buf.freeze()
    }

    /// Decode a blob produced by [`Self::encode`] (accepts v1 and v2).
    pub fn decode(mut data: Bytes) -> Result<Self> {
        if data.remaining() < 4 + 4 + 8 + 8 + 4 {
            return Err(TakyonicError::Raft("snapshot payload truncated".into()));
        }
        let mut magic = [0u8; 4];
        data.copy_to_slice(&mut magic);
        if &magic != MAGIC {
            return Err(TakyonicError::Raft("bad snapshot magic".into()));
        }
        let version = data.get_u32_le();
        let last_included_index = data.get_u64_le();
        let last_included_term = data.get_u64_le();
        let membership = if version == VERSION {
            if data.remaining() < 4 {
                return Err(TakyonicError::Raft(
                    "snapshot membership length truncated".into(),
                ));
            }
            let mem_len = data.get_u32_le() as usize;
            if data.remaining() < mem_len {
                return Err(TakyonicError::Raft("snapshot membership truncated".into()));
            }
            ClusterMembership::decode(data.copy_to_bytes(mem_len))?
        } else if version == VERSION_V1 {
            ClusterMembership::empty()
        } else {
            return Err(TakyonicError::Raft(format!(
                "unsupported snapshot version {version}"
            )));
        };
        if data.remaining() < 4 {
            return Err(TakyonicError::Raft("snapshot file count truncated".into()));
        }
        let n = data.get_u32_le() as usize;
        let mut files = Vec::with_capacity(n);
        for _ in 0..n {
            if data.remaining() < 4 + 8 + 4 {
                return Err(TakyonicError::Raft("snapshot file header truncated".into()));
            }
            let level = data.get_u32_le() as usize;
            let id = data.get_u64_le();
            let sk_len = data.get_u32_le() as usize;
            if data.remaining() < sk_len + 4 {
                return Err(TakyonicError::Raft(
                    "snapshot smallest key truncated".into(),
                ));
            }
            let smallest = Key::new(data.copy_to_bytes(sk_len));
            let lk_len = data.get_u32_le() as usize;
            if data.remaining() < lk_len + 8 {
                return Err(TakyonicError::Raft("snapshot largest key truncated".into()));
            }
            let largest = Key::new(data.copy_to_bytes(lk_len));
            let data_len = data.get_u64_le() as usize;
            if data.remaining() < data_len {
                return Err(TakyonicError::Raft("snapshot SST body truncated".into()));
            }
            let file_data = data.copy_to_bytes(data_len);
            files.push(SnapshotSst {
                level,
                id,
                smallest,
                largest,
                data: file_data,
            });
        }
        Ok(Self {
            last_included_index,
            last_included_term,
            membership,
            files,
        })
    }

    /// Build a payload by reading live SST files listed in `metas`.
    pub fn from_metas(
        last_included_index: u64,
        last_included_term: u64,
        membership: ClusterMembership,
        metas: &[SstMeta],
    ) -> Result<Self> {
        let mut files = Vec::with_capacity(metas.len());
        for meta in metas {
            let data = std::fs::read(&meta.path).map_err(|e| {
                TakyonicError::Raft(format!(
                    "read SST {} for snapshot: {e}",
                    meta.path.display()
                ))
            })?;
            files.push(SnapshotSst {
                level: meta.level,
                id: meta.id,
                smallest: meta.smallest.clone(),
                largest: meta.largest.clone(),
                data: Bytes::from(data),
            });
        }
        Ok(Self {
            last_included_index,
            last_included_term,
            membership,
            files,
        })
    }
}

/// Destination path for an installed SST under `data_dir`.
pub fn snapshot_sst_path(data_dir: &Path, level: usize, id: u64) -> PathBuf {
    data_dir
        .join(format!("L{level}"))
        .join(format!("{id:020}.sst"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn meta_roundtrip() {
        let m = SnapshotMeta {
            last_included_index: 42,
            last_included_term: 7,
        };
        assert_eq!(SnapshotMeta::decode(&m.encode()).unwrap(), m);
    }

    #[test]
    fn payload_roundtrip_empty() {
        let p = SnapshotPayload {
            last_included_index: 10,
            last_included_term: 2,
            membership: ClusterMembership::from_endpoints([(1, "a:1".into()), (2, "a:2".into())]),
            files: vec![],
        };
        let decoded = SnapshotPayload::decode(p.encode()).unwrap();
        assert_eq!(decoded.last_included_index, 10);
        assert_eq!(decoded.files.len(), 0);
        assert_eq!(decoded.membership.len(), 2);
    }
}
