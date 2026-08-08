//! Shared storage manifest for remote object stores.
//!
//! Remote object storage is not locally atomic, so the cluster treats a single
//! JSON document (`manifest/CURRENT.json`) as the source of truth for active
//! SSTables, B-Tree roots, and the pages blob. Nodes load it on startup and
//! publish a new versioned snapshot after durable flushes.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tracing::info;
use xxhash_rust::xxh3::xxh3_64;

use crate::error::{Result, TakyonicError};
use crate::object_store::ObjectStorage;

/// Default object key for the current manifest pointer.
pub const MANIFEST_CURRENT_KEY: &str = "manifest/CURRENT.json";
/// Object key prefix for versioned manifest snapshots.
pub const MANIFEST_VERSION_PREFIX: &str = "manifest/versions/";
/// Default prefix for chunked (V2) remote pages.
pub const DEFAULT_PAGES_PREFIX: &str = "pages/v2";
/// Default remote pages chunk size (64 MiB). Must be a multiple of the BPM page size.
pub const DEFAULT_PAGES_CHUNK_BYTES: u64 = 64 * 1024 * 1024;

/// How BPM heap pages are laid out in object storage.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PagesLayout {
    /// Legacy single blob at [`StorageManifest::pages_key`] (full-object RMW).
    BlobV1,
    /// Fixed-size chunks under [`StorageManifest::pages_prefix`].
    ChunkV2 {
        /// Chunk object size in bytes (multiple of page size).
        chunk_size: u64,
    },
}

impl Default for PagesLayout {
    /// Missing field on old manifests → treat as V1 blob (safe read path).
    fn default() -> Self {
        Self::BlobV1
    }
}

/// One durable SSTable referenced by the shared manifest.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ManifestSst {
    /// SST file id.
    pub id: u64,
    /// Object key (e.g. `sst/L0/00000000000000000001.sst`).
    pub path: String,
    /// LSM level.
    pub level: u32,
}

/// Global database state snapshot stored in object storage.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct StorageManifest {
    /// Monotonic manifest version (CAS-style publish).
    pub version: u64,
    /// Active SST files.
    pub sstables: Vec<ManifestSst>,
    /// Logical table → B-Tree root object key.
    pub btree_roots: BTreeMap<String, String>,
    /// Object key of the primary pages blob (BPM heap) — V1 layout / migration source.
    pub pages_key: String,
    /// V2 chunk key prefix (e.g. `pages/v2`).
    #[serde(default = "default_pages_prefix")]
    pub pages_prefix: String,
    /// Remote pages object layout.
    #[serde(default)]
    pub pages_layout: PagesLayout,
    /// xxh3 of the canonical JSON body (excluding this field) for integrity.
    #[serde(default)]
    pub checksum: u64,
}

fn default_pages_prefix() -> String {
    DEFAULT_PAGES_PREFIX.to_string()
}

impl StorageManifest {
    /// Fresh empty manifest using chunked V2 pages (new deployments).
    pub fn new() -> Self {
        Self {
            version: 0,
            sstables: Vec::new(),
            btree_roots: BTreeMap::new(),
            pages_key: "pages/PAGES".into(),
            pages_prefix: DEFAULT_PAGES_PREFIX.into(),
            pages_layout: PagesLayout::ChunkV2 {
                chunk_size: DEFAULT_PAGES_CHUNK_BYTES,
            },
            checksum: 0,
        }
    }

    /// Recompute and set [`Self::checksum`].
    pub fn seal(&mut self) {
        self.checksum = 0;
        let body = serde_json::to_vec(self).unwrap_or_default();
        self.checksum = xxh3_64(&body);
    }

    /// Verify checksum matches the sealed body.
    pub fn verify(&self) -> Result<()> {
        let mut clone = self.clone();
        let expected = clone.checksum;
        clone.checksum = 0;
        let body = serde_json::to_vec(&clone)
            .map_err(|e| TakyonicError::Integrity(format!("manifest encode: {e}")))?;
        let got = xxh3_64(&body);
        if got != expected && expected != 0 {
            return Err(TakyonicError::Integrity(format!(
                "manifest checksum mismatch: expected {expected}, got {got}"
            )));
        }
        Ok(())
    }

    /// Encode as JSON bytes (sealed).
    pub fn to_json(&mut self) -> Result<Vec<u8>> {
        self.seal();
        serde_json::to_vec_pretty(self)
            .map_err(|e| TakyonicError::Engine(format!("manifest json: {e}")))
    }

    /// Decode + verify.
    pub fn from_json(bytes: &[u8]) -> Result<Self> {
        let m: Self = serde_json::from_slice(bytes)
            .map_err(|e| TakyonicError::Integrity(format!("manifest parse: {e}")))?;
        m.verify()?;
        Ok(m)
    }
}

/// Loads / publishes the shared manifest against an [`ObjectStorage`].
pub struct ManifestManager {
    store: Arc<dyn ObjectStorage>,
    current_key: String,
    cached: RwLock<StorageManifest>,
    publishes: AtomicU64,
}

impl ManifestManager {
    /// Attach to `store`, loading `CURRENT` if present (else empty v0).
    pub fn open(store: Arc<dyn ObjectStorage>) -> Result<Self> {
        Self::open_with_key(store, MANIFEST_CURRENT_KEY)
    }

    /// Open with a custom current-pointer key.
    pub fn open_with_key(store: Arc<dyn ObjectStorage>, current_key: impl Into<String>) -> Result<Self> {
        let current_key = current_key.into();
        let cached = match store.read_all(&current_key) {
            Ok(bytes) => {
                let m = StorageManifest::from_json(&bytes)?;
                info!(version = m.version, sst = m.sstables.len(), "loaded storage manifest");
                m
            }
            Err(_) => StorageManifest::new(),
        };
        Ok(Self {
            store,
            current_key,
            cached: RwLock::new(cached),
            publishes: AtomicU64::new(0),
        })
    }

    /// Shared object store.
    pub fn store(&self) -> &Arc<dyn ObjectStorage> {
        &self.store
    }

    /// In-memory snapshot of the latest manifest.
    pub fn current(&self) -> StorageManifest {
        self.cached.read().clone()
    }

    /// Force re-fetch from object storage (cold start / peer publish).
    pub fn reload(&self) -> Result<StorageManifest> {
        let bytes = self.store.read_all(&self.current_key)?;
        let m = StorageManifest::from_json(&bytes)?;
        *self.cached.write() = m.clone();
        Ok(m)
    }

    /// Publish `next` as `version = current+1` (rejects stale writers).
    pub fn publish(&self, mut next: StorageManifest) -> Result<StorageManifest> {
        let current_ver = self.cached.read().version;
        if next.version != 0 && next.version <= current_ver {
            return Err(TakyonicError::Engine(format!(
                "stale manifest publish: next={} current={current_ver}",
                next.version
            )));
        }
        next.version = current_ver + 1;
        let bytes = next.to_json()?;
        // Versioned snapshot then atomic pointer swap (best-effort on S3).
        let ver_key = format!("{MANIFEST_VERSION_PREFIX}{:020}.json", next.version);
        self.store.write(&ver_key, &bytes)?;
        self.store.write(&self.current_key, &bytes)?;
        *self.cached.write() = next.clone();
        self.publishes.fetch_add(1, Ordering::Relaxed);
        info!(version = next.version, "published storage manifest");
        Ok(next)
    }

    /// Register / replace an SST entry and publish.
    pub fn add_sst(&self, sst: ManifestSst) -> Result<StorageManifest> {
        let mut m = self.current();
        m.sstables.retain(|s| s.id != sst.id);
        m.sstables.push(sst);
        m.sstables.sort_by_key(|s| s.id);
        self.publish(m)
    }

    /// Set a B-Tree root object key and publish.
    pub fn set_btree_root(&self, table: &str, root_key: &str) -> Result<StorageManifest> {
        let mut m = self.current();
        m.btree_roots
            .insert(table.to_string(), root_key.to_string());
        self.publish(m)
    }

    /// Publish count (tests).
    pub fn publish_count(&self) -> u64 {
        self.publishes.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::object_store::InMemoryObjectStore;

    #[test]
    fn publish_reload_roundtrip() {
        let store = InMemoryObjectStore::new();
        let mgr = ManifestManager::open(Arc::clone(&store) as Arc<dyn ObjectStorage>).unwrap();
        let mut m = mgr.current();
        m.sstables.push(ManifestSst {
            id: 7,
            path: "sst/L0/7.sst".into(),
            level: 0,
        });
        m.btree_roots.insert("users".into(), "btree/users.root".into());
        mgr.publish(m).unwrap();

        // Separate node: empty local state, hydrate from shared store.
        let mgr2 = ManifestManager::open(store as Arc<dyn ObjectStorage>).unwrap();
        let loaded = mgr2.current();
        assert_eq!(loaded.version, 1);
        assert_eq!(loaded.sstables.len(), 1);
        assert_eq!(loaded.btree_roots.get("users").unwrap(), "btree/users.root");
        assert!(matches!(
            loaded.pages_layout,
            PagesLayout::ChunkV2 { .. }
        ));
        loaded.verify().unwrap();
    }

    #[test]
    fn legacy_json_without_pages_layout_is_blob_v1() {
        let stripped = br#"{"version":1,"sstables":[],"btree_roots":{},"pages_key":"pages/PAGES","checksum":0}"#;
        let raw: StorageManifest = serde_json::from_slice(stripped).unwrap();
        assert!(matches!(raw.pages_layout, PagesLayout::BlobV1));
        assert_eq!(raw.pages_prefix, DEFAULT_PAGES_PREFIX);
    }
}
