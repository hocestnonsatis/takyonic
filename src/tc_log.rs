//! Durable 2PC coordinator decision log.
//!
//! Before Phase-2 apply, the coordinator appends a [`TcDecisionRecord`] and
//! `sync_data`s so a crash mid-commit can still resolve orphaned PREPARED
//! participants (instead of presumed-abort).

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use bytes::{BufMut, BytesMut};
use xxhash_rust::xxh3::xxh3_64;

use crate::dtxn::{CoordinatorDecision, DistTxnId, TwopcState};
use crate::error::{Result, TakyonicError};
use crate::types::CommitTs;

/// On-disk file name under `data_dir`.
pub const TC_DECISIONS_FILE: &str = "TC_DECISIONS";

const MAGIC: &[u8; 4] = b"TKYC";
const VERSION: u8 = 1;
const TAG_COMMITTED: u8 = 1;
const TAG_ABORTED: u8 = 2;

/// One durable coordinator decision.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TcDecisionRecord {
    /// Distributed transaction id.
    pub txn_id: DistTxnId,
    /// Final state (`Committed` or `Aborted`).
    pub state: TwopcState,
    /// Commit timestamp when committed.
    pub commit_ts: Option<CommitTs>,
}

impl TcDecisionRecord {
    fn encode_body(&self, buf: &mut BytesMut) {
        let tag = match self.state {
            TwopcState::Committed => TAG_COMMITTED,
            TwopcState::Aborted => TAG_ABORTED,
            other => panic!("tc log only stores terminal decisions, got {other:?}"),
        };
        buf.put_u8(tag);
        buf.put_u64_le(self.txn_id);
        buf.put_u64_le(self.commit_ts.unwrap_or(0));
    }

    fn decode_body(mut body: &[u8]) -> Result<Self> {
        if body.len() < 1 + 8 + 8 {
            return Err(TakyonicError::Integrity("tc decision body truncated".into()));
        }
        let tag = body[0];
        body = &body[1..];
        let txn_id = u64::from_le_bytes(body[..8].try_into().unwrap());
        body = &body[8..];
        let ts = u64::from_le_bytes(body[..8].try_into().unwrap());
        if !body[8..].is_empty() {
            return Err(TakyonicError::Integrity(
                "tc decision body has trailing bytes".into(),
            ));
        }
        match tag {
            TAG_COMMITTED => Ok(Self {
                txn_id,
                state: TwopcState::Committed,
                commit_ts: Some(ts),
            }),
            TAG_ABORTED => Ok(Self {
                txn_id,
                state: TwopcState::Aborted,
                commit_ts: None,
            }),
            _ => Err(TakyonicError::Integrity(format!(
                "unknown tc decision tag {tag}"
            ))),
        }
    }

    fn encode_framed(&self) -> Vec<u8> {
        let mut body = BytesMut::new();
        self.encode_body(&mut body);
        let checksum = xxh3_64(&body);
        let mut out = Vec::with_capacity(4 + body.len() + 8);
        out.extend_from_slice(&(body.len() as u32).to_le_bytes());
        out.extend_from_slice(&body);
        out.extend_from_slice(&checksum.to_le_bytes());
        out
    }
}

/// Append-only, fsynced coordinator decision log.
pub struct TcDecisionLog {
    path: PathBuf,
    file: File,
}

impl TcDecisionLog {
    /// Path of the durable file under `data_dir`.
    pub fn path_in(data_dir: &Path) -> PathBuf {
        data_dir.join(TC_DECISIONS_FILE)
    }

    /// Create a fresh log (truncates if present).
    pub fn create(data_dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(data_dir)?;
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
        Ok(Self { path, file })
    }

    /// Open existing log for append, or create if missing.
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
                "bad tc decision magic at {}",
                path.display()
            )));
        }
        let mut ver = [0u8; 1];
        file.read_exact(&mut ver)?;
        if ver[0] != VERSION {
            return Err(TakyonicError::Integrity(format!(
                "unsupported tc decision version {}",
                ver[0]
            )));
        }
        let meta = file.metadata()?;
        file.seek(SeekFrom::Start(meta.len()))?;
        Ok(Self { path, file })
    }

    /// Append one terminal decision and `sync_data`.
    pub fn append_decision(&mut self, record: &TcDecisionRecord) -> Result<()> {
        if !matches!(
            record.state,
            TwopcState::Committed | TwopcState::Aborted
        ) {
            return Err(TakyonicError::Engine(
                "tc log only accepts Committed/Aborted decisions".into(),
            ));
        }
        let framed = record.encode_framed();
        self.file.write_all(&framed)?;
        self.file.sync_data()?;
        Ok(())
    }

    /// Load all decisions (later record wins). Also returns max `txn_id` seen.
    pub fn load(data_dir: &Path) -> Result<(HashMap<DistTxnId, CoordinatorDecision>, DistTxnId)> {
        let path = Self::path_in(data_dir);
        if !path.exists() {
            return Ok((HashMap::new(), 0));
        }
        let mut file = OpenOptions::new().read(true).open(&path)?;
        let mut magic = [0u8; 4];
        if file.read_exact(&mut magic).is_err() {
            return Ok((HashMap::new(), 0));
        }
        if &magic != MAGIC {
            return Err(TakyonicError::Integrity(format!(
                "bad tc decision magic at {}",
                path.display()
            )));
        }
        let mut ver = [0u8; 1];
        file.read_exact(&mut ver)?;
        if ver[0] != VERSION {
            return Err(TakyonicError::Integrity(format!(
                "unsupported tc decision version {}",
                ver[0]
            )));
        }

        let mut map = HashMap::new();
        let mut max_id = 0u64;
        loop {
            let mut len_buf = [0u8; 4];
            match file.read_exact(&mut len_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }
            let len = u32::from_le_bytes(len_buf) as usize;
            if len == 0 || len > 1024 * 1024 {
                // Torn / corrupt length — stop (presumed abort for unread).
                break;
            }
            let mut body = vec![0u8; len];
            if file.read_exact(&mut body).is_err() {
                break;
            }
            let mut csum_buf = [0u8; 8];
            if file.read_exact(&mut csum_buf).is_err() {
                break;
            }
            let expect = u64::from_le_bytes(csum_buf);
            if xxh3_64(&body) != expect {
                return Err(TakyonicError::Integrity(
                    "tc decision checksum mismatch".into(),
                ));
            }
            let rec = TcDecisionRecord::decode_body(&body)?;
            max_id = max_id.max(rec.txn_id);
            map.insert(
                rec.txn_id,
                CoordinatorDecision {
                    state: rec.state,
                    commit_ts: rec.commit_ts,
                },
            );
        }
        Ok((map, max_id))
    }

    /// On-disk path.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_and_reload_survives() {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("takyonic-tc-log-{nanos}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let mut log = TcDecisionLog::create(&dir).unwrap();
        log.append_decision(&TcDecisionRecord {
            txn_id: 7,
            state: TwopcState::Committed,
            commit_ts: Some(42),
        })
        .unwrap();
        log.append_decision(&TcDecisionRecord {
            txn_id: 8,
            state: TwopcState::Aborted,
            commit_ts: None,
        })
        .unwrap();
        drop(log);

        let (map, max_id) = TcDecisionLog::load(&dir).unwrap();
        assert_eq!(max_id, 8);
        assert_eq!(map.get(&7).unwrap().state, TwopcState::Committed);
        assert_eq!(map.get(&7).unwrap().commit_ts, Some(42));
        assert_eq!(map.get(&8).unwrap().state, TwopcState::Aborted);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
