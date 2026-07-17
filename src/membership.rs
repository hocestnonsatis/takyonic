//! Replicated Raft cluster membership (single-server configuration changes).
//!
//! The active voter set is part of the replicated state. Per Ongaro's
//! single-server change rule, a node applies a `ConfigChange` to its local
//! membership as soon as the entry is appended (leader or follower), and rolls
//! the membership back if that suffix is later truncated.

use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::path::Path;

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::error::{Result, TakyonicError};

const MAGIC: &[u8; 4] = b"TKYM";
const VERSION: u32 = 1;

/// Active voting membership plus advertised endpoints.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ClusterMembership {
    /// Voting member ids (includes self when this node is a voter).
    members: BTreeSet<u64>,
    /// Advertised `host:port` for each known member (including self).
    endpoints: BTreeMap<u64, String>,
}

impl ClusterMembership {
    /// Empty membership (joining node awaiting InstallSnapshot / AddNode).
    pub fn empty() -> Self {
        Self::default()
    }

    /// Bootstrap from an `id -> address` map (typical cold start).
    pub fn from_endpoints(endpoints: impl IntoIterator<Item = (u64, String)>) -> Self {
        let endpoints: BTreeMap<u64, String> = endpoints.into_iter().collect();
        let members = endpoints.keys().copied().collect();
        Self { members, endpoints }
    }

    /// Voting member ids in ascending order.
    pub fn members(&self) -> impl Iterator<Item = u64> + '_ {
        self.members.iter().copied()
    }

    /// Number of voting members.
    pub fn len(&self) -> usize {
        self.members.len()
    }

    /// True when there are no voting members.
    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    /// Whether `id` is a voting member.
    pub fn contains(&self, id: u64) -> bool {
        self.members.contains(&id)
    }

    /// Majority quorum size for the current membership (`n/2 + 1`).
    #[allow(clippy::manual_div_ceil)]
    pub fn quorum(&self) -> usize {
        if self.members.is_empty() {
            return usize::MAX; // unreachable quorum — never elect / commit alone
        }
        self.members.len() / 2 + 1
    }

    /// Peer ids excluding `self_id`.
    pub fn peers_except(&self, self_id: u64) -> Vec<u64> {
        self.members
            .iter()
            .copied()
            .filter(|&m| m != self_id)
            .collect()
    }

    /// Lookup advertised address.
    pub fn address(&self, id: u64) -> Option<&str> {
        self.endpoints.get(&id).map(String::as_str)
    }

    /// All known endpoints.
    pub fn endpoints(&self) -> &BTreeMap<u64, String> {
        &self.endpoints
    }

    /// Apply an AddNode change (immediate-effect).
    pub fn add_node(&mut self, id: u64, address: String) {
        self.members.insert(id);
        self.endpoints.insert(id, address);
    }

    /// Apply a RemoveNode change (immediate-effect).
    pub fn remove_node(&mut self, id: u64) {
        self.members.remove(&id);
        // Keep endpoint around briefly so a leader can still dial during catch-up;
        // callers may prune PeerClients separately.
    }

    /// Encode for snapshot / on-disk persistence.
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_slice(MAGIC);
        buf.put_u32_le(VERSION);
        buf.put_u32_le(self.members.len() as u32);
        for &id in &self.members {
            let addr = self.endpoints.get(&id).map(String::as_str).unwrap_or("");
            buf.put_u64_le(id);
            let bytes = addr.as_bytes();
            buf.put_u32_le(bytes.len() as u32);
            buf.put_slice(bytes);
        }
        // Also persist non-member endpoint hints (e.g. recently removed).
        let extras: Vec<_> = self
            .endpoints
            .iter()
            .filter(|(id, _)| !self.members.contains(id))
            .collect();
        buf.put_u32_le(extras.len() as u32);
        for (&id, addr) in extras {
            buf.put_u64_le(id);
            let bytes = addr.as_bytes();
            buf.put_u32_le(bytes.len() as u32);
            buf.put_slice(bytes);
        }
        buf.freeze()
    }

    /// Decode a blob produced by [`Self::encode`].
    pub fn decode(mut data: Bytes) -> Result<Self> {
        if data.remaining() < 4 + 4 + 4 {
            return Err(TakyonicError::Raft("membership blob truncated".into()));
        }
        let mut magic = [0u8; 4];
        data.copy_to_slice(&mut magic);
        if &magic != MAGIC {
            return Err(TakyonicError::Raft("bad membership magic".into()));
        }
        let version = data.get_u32_le();
        if version != VERSION {
            return Err(TakyonicError::Raft(format!(
                "unsupported membership version {version}"
            )));
        }
        let n = data.get_u32_le() as usize;
        let mut members = BTreeSet::new();
        let mut endpoints = BTreeMap::new();
        for _ in 0..n {
            if data.remaining() < 8 + 4 {
                return Err(TakyonicError::Raft("membership member truncated".into()));
            }
            let id = data.get_u64_le();
            let len = data.get_u32_le() as usize;
            if data.remaining() < len {
                return Err(TakyonicError::Raft("membership address truncated".into()));
            }
            let addr = String::from_utf8(data.copy_to_bytes(len).to_vec())
                .map_err(|e| TakyonicError::Raft(format!("membership address utf8: {e}")))?;
            members.insert(id);
            endpoints.insert(id, addr);
        }
        if data.remaining() >= 4 {
            let extras = data.get_u32_le() as usize;
            for _ in 0..extras {
                if data.remaining() < 8 + 4 {
                    break;
                }
                let id = data.get_u64_le();
                let len = data.get_u32_le() as usize;
                if data.remaining() < len {
                    break;
                }
                let addr = String::from_utf8(data.copy_to_bytes(len).to_vec()).unwrap_or_default();
                endpoints.entry(id).or_insert(addr);
            }
        }
        Ok(Self { members, endpoints })
    }

    /// Atomically persist under `dir/MEMBERSHIP.meta`.
    pub fn write_to_dir(dir: &Path, membership: &ClusterMembership) -> Result<()> {
        std::fs::create_dir_all(dir)?;
        let tmp = dir.join("MEMBERSHIP.meta.tmp");
        let dst = dir.join("MEMBERSHIP.meta");
        {
            let mut file = std::fs::File::create(&tmp)?;
            file.write_all(&membership.encode())?;
            file.sync_all()?;
        }
        std::fs::rename(&tmp, &dst)?;
        std::fs::File::open(dir)?.sync_all()?;
        Ok(())
    }

    /// Load `dir/MEMBERSHIP.meta` if present.
    pub fn read_from_dir(dir: &Path) -> Result<Option<ClusterMembership>> {
        let path = dir.join("MEMBERSHIP.meta");
        if !path.exists() {
            return Ok(None);
        }
        let bytes = std::fs::read(&path)?;
        Ok(Some(Self::decode(Bytes::from(bytes))?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_and_quorum() {
        let mut m = ClusterMembership::from_endpoints([
            (1, "127.0.0.1:1".into()),
            (2, "127.0.0.1:2".into()),
            (3, "127.0.0.1:3".into()),
        ]);
        assert_eq!(m.quorum(), 2);
        m.add_node(4, "127.0.0.1:4".into());
        assert_eq!(m.quorum(), 3);
        m.remove_node(1);
        assert_eq!(m.quorum(), 2);
        assert!(!m.contains(1));
        let decoded = ClusterMembership::decode(m.encode()).unwrap();
        assert_eq!(decoded, m);
    }
}
