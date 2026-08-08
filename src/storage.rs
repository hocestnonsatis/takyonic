//! Multi-engine storage manager: route tables to LSM or B-Tree backends.
//!
//! Takyonic's default path is the leveled LSM-Tree ([`LSMStorage`]). Tables can
//! be marked [`StorageEngineKind::BTree`] for read-heavy / latency-sensitive
//! workloads; the manager keeps a per-table [`BTreeStorage`] and routes
//! get/put/scan/vacuum accordingly.
//!
//! # B-Tree durability model
//!
//! `BTREE` is a **read path**, not a separate durable store. Commits still land
//! in the engine LSM via Raft/WAL; [`crate::engine::TakyonicEngine`] mirrors
//! those writes into [`BTreeStorage`] and rebuilds the mirror from LSM on open.
//! Without that hydrate step, a cold process would see an empty B-Tree even
//! though durable bytes exist in WAL/SST.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use parking_lot::RwLock;

use crate::btree_storage::BTreeStorage;
use crate::error::{Result, TakyonicError};
use crate::lsm_storage::LSMStorage;
use crate::schema::TableSchema;
use crate::types::{CommitTs, Entry, Key, Value};

/// Per-table physical storage strategy.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum StorageEngineKind {
    /// Leveled LSM-Tree (memtable → SST → compaction). Default / write-optimized.
    #[default]
    Lsm,
    /// In-memory MVCC B-Tree mirror (LSM remains durable; see module docs).
    BTree,
}

impl StorageEngineKind {
    /// Parse catalog / DDL token (`LSM`, `BTREE`, …).
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_uppercase().as_str() {
            "LSM" | "LSMTREE" | "LSM_TREE" => Some(Self::Lsm),
            "BTREE" | "B-TREE" | "BTREE_MAP" => Some(Self::BTree),
            _ => None,
        }
    }

    /// Catalog token.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Lsm => "LSM",
            Self::BTree => "BTREE",
        }
    }
}

/// Routes reads/writes by table configuration across LSM and B-Tree engines.
pub struct StorageManager {
    lsm: Option<Arc<LSMStorage>>,
    btrees: RwLock<HashMap<String, Arc<BTreeStorage>>>,
    engines: RwLock<HashMap<String, StorageEngineKind>>,
}

impl StorageManager {
    /// Open a manager with a fresh LSM store under `data_dir`.
    pub fn open(data_dir: impl Into<PathBuf>) -> Result<Self> {
        let lsm = Arc::new(LSMStorage::open(data_dir)?);
        Ok(Self {
            lsm: Some(lsm),
            btrees: RwLock::new(HashMap::new()),
            engines: RwLock::new(HashMap::new()),
        })
    }

    /// B-Tree routing only (engine already owns the primary LSM).
    pub fn router_only() -> Self {
        Self {
            lsm: None,
            btrees: RwLock::new(HashMap::new()),
            engines: RwLock::new(HashMap::new()),
        }
    }

    /// Wrap an existing LSM store.
    pub fn from_lsm(lsm: Arc<LSMStorage>) -> Self {
        Self {
            lsm: Some(lsm),
            btrees: RwLock::new(HashMap::new()),
            engines: RwLock::new(HashMap::new()),
        }
    }

    /// Shared LSM engine (when this manager owns one).
    pub fn lsm(&self) -> Option<&Arc<LSMStorage>> {
        self.lsm.as_ref()
    }

    fn require_lsm(&self) -> Result<&Arc<LSMStorage>> {
        self.lsm
            .as_ref()
            .ok_or_else(|| TakyonicError::Engine("no LSM store on this StorageManager".into()))
    }

    /// Register (or replace) a table's storage engine.
    pub fn register_table(&self, schema: &TableSchema) -> Result<()> {
        let kind = schema.storage_engine;
        self.engines
            .write()
            .insert(schema.name.clone(), kind);
        if kind == StorageEngineKind::BTree {
            self.btrees
                .write()
                .entry(schema.name.clone())
                .or_insert_with(|| Arc::new(BTreeStorage::new()));
        }
        Ok(())
    }

    /// Engine kind for `table` (default LSM if unregistered).
    pub fn engine_kind(&self, table: &str) -> StorageEngineKind {
        self.engines
            .read()
            .get(table)
            .copied()
            .unwrap_or(StorageEngineKind::Lsm)
    }

    /// Borrow the B-Tree store for `table` (error if not a B-Tree table).
    pub fn btree(&self, table: &str) -> Result<Arc<BTreeStorage>> {
        self.btrees
            .read()
            .get(table)
            .cloned()
            .ok_or_else(|| TakyonicError::Engine(format!("no B-Tree store for `{table}`")))
    }

    /// Put into the engine configured for `table`.
    pub fn put_raw(&self, table: &str, key: Key, value: Value) -> Result<CommitTs> {
        match self.engine_kind(table) {
            StorageEngineKind::Lsm => self.require_lsm()?.put(key, value),
            StorageEngineKind::BTree => {
                let ts = self.btree(table)?.put(key, value);
                Ok(ts)
            }
        }
    }

    /// Point get routed by table engine.
    pub fn get_raw(&self, table: &str, key: &Key) -> Result<Option<Value>> {
        match self.engine_kind(table) {
            StorageEngineKind::Lsm => self.require_lsm()?.get(key),
            StorageEngineKind::BTree => Ok(self.btree(table)?.get(key)),
        }
    }

    /// Snapshot get.
    pub fn get_at(&self, table: &str, key: &Key, read_ts: CommitTs) -> Result<Option<Entry>> {
        match self.engine_kind(table) {
            StorageEngineKind::Lsm => self.require_lsm()?.get_at(key, read_ts),
            StorageEngineKind::BTree => Ok(self.btree(table)?.get_at(key, read_ts)),
        }
    }

    /// VACUUM dead versions below `watermark` for `table`.
    pub fn vacuum_table(&self, table: &str, watermark: CommitTs) -> Result<u64> {
        match self.engine_kind(table) {
            StorageEngineKind::BTree => Ok(self.btree(table)?.vacuum_below(watermark)),
            StorageEngineKind::Lsm => {
                let lsm = self.require_lsm()?;
                lsm.set_watermark(watermark);
                let _ = lsm.flush()?;
                lsm.drain_compaction()?;
                Ok(0)
            }
        }
    }

    /// Entries for ANALYZE sampling.
    pub fn sample_entries(&self, table: &str) -> Result<Vec<Entry>> {
        match self.engine_kind(table) {
            StorageEngineKind::BTree => Ok(self.btree(table)?.snapshot_entries()),
            StorageEngineKind::Lsm => {
                let lsm = self.require_lsm()?;
                let reader = crate::lsm_storage::LSMReader::new(lsm)?;
                reader.merge_scan(u64::MAX)
            }
        }
    }

    /// Flush LSM memtable (no-op when this manager has no LSM).
    pub fn flush_lsm(&self) -> Result<Option<u64>> {
        match &self.lsm {
            Some(lsm) => lsm.flush(),
            None => Ok(None),
        }
    }

    /// Shut down background compaction.
    pub fn close(&self) -> Result<()> {
        if let Some(lsm) = &self.lsm {
            lsm.close()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::TableSchema;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn routes_btree_and_lsm_tables() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("takyonic-sm-{nanos}"));
        let mgr = StorageManager::open(&root).unwrap();

        let lsm_t = TableSchema::new("logs", "id", vec![]).with_engine(StorageEngineKind::Lsm);
        let bt_t = TableSchema::new("users", "id", vec![]).with_engine(StorageEngineKind::BTree);
        mgr.register_table(&lsm_t).unwrap();
        mgr.register_table(&bt_t).unwrap();

        mgr.put_raw("logs", Key::new(&b"Data_logs_1"[..]), Value::new(&b"a"[..]))
            .unwrap();
        mgr.put_raw("users", Key::new(&b"Data_users_1"[..]), Value::new(&b"b"[..]))
            .unwrap();

        assert_eq!(
            mgr.get_raw("logs", &Key::new(&b"Data_logs_1"[..]))
                .unwrap()
                .unwrap()
                .as_bytes(),
            b"a"
        );
        assert_eq!(
            mgr.get_raw("users", &Key::new(&b"Data_users_1"[..]))
                .unwrap()
                .unwrap()
                .as_bytes(),
            b"b"
        );
        assert!(mgr.vacuum_table("users", 100).is_ok());
        mgr.close().unwrap();
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn vacuum_and_analyze_sample_across_engines() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("takyonic-sm-vac-{nanos}"));
        let mgr = StorageManager::open(&root).unwrap();

        let lsm_t = TableSchema::new("logs", "id", vec![]).with_engine(StorageEngineKind::Lsm);
        let bt_t = TableSchema::new("kv", "id", vec![]).with_engine(StorageEngineKind::BTree);
        mgr.register_table(&lsm_t).unwrap();
        mgr.register_table(&bt_t).unwrap();

        for i in 0..20u32 {
            let k = format!("Data_logs_{i}");
            mgr.put_raw("logs", Key::new(k.into_bytes()), Value::new(&b"x"[..]))
                .unwrap();
            let k = format!("Data_kv_{i}");
            mgr.put_raw("kv", Key::new(k.into_bytes()), Value::new(&b"y"[..]))
                .unwrap();
        }
        // Overwrite a few B-Tree keys to create dead versions.
        for i in 0..5u32 {
            let k = format!("Data_kv_{i}");
            mgr.put_raw("kv", Key::new(k.into_bytes()), Value::new(&b"z"[..]))
                .unwrap();
        }
        let removed = mgr.vacuum_table("kv", 100).unwrap();
        assert!(removed >= 1, "btree vacuum should reclaim old versions");
        assert!(mgr.vacuum_table("logs", 100).is_ok());

        let lsm_sample = mgr.sample_entries("logs").unwrap();
        let bt_sample = mgr.sample_entries("kv").unwrap();
        assert!(!lsm_sample.is_empty());
        assert!(!bt_sample.is_empty());
        mgr.close().unwrap();
        let _ = std::fs::remove_dir_all(root);
    }

    /// Compare B-Tree vs LSM ingestion throughput (1M rows in release, 50k in debug).
    #[test]
    fn write_throughput_btree_vs_lsm() {
        let n: u64 = if cfg!(debug_assertions) {
            50_000
        } else {
            1_000_000
        };
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("takyonic-bench-{nanos}"));
        let mgr = StorageManager::open(&root).unwrap();
        mgr.register_table(
            &TableSchema::new("lsm_t", "id", vec![]).with_engine(StorageEngineKind::Lsm),
        )
        .unwrap();
        mgr.register_table(
            &TableSchema::new("bt_t", "id", vec![]).with_engine(StorageEngineKind::BTree),
        )
        .unwrap();

        let t0 = std::time::Instant::now();
        for i in 0..n {
            let k = format!("Data_lsm_t_{i:08}");
            mgr.put_raw("lsm_t", Key::new(k.into_bytes()), Value::new(&b"v"[..]))
                .unwrap();
        }
        let lsm_secs = t0.elapsed().as_secs_f64().max(1e-9);
        let lsm_rps = n as f64 / lsm_secs;

        let t1 = std::time::Instant::now();
        for i in 0..n {
            let k = format!("Data_bt_t_{i:08}");
            mgr.put_raw("bt_t", Key::new(k.into_bytes()), Value::new(&b"v"[..]))
                .unwrap();
        }
        let bt_secs = t1.elapsed().as_secs_f64().max(1e-9);
        let bt_rps = n as f64 / bt_secs;

        eprintln!(
            "write_throughput n={n}: LSM={lsm_rps:.0} rows/s ({lsm_secs:.3}s) \
             BTree={bt_rps:.0} rows/s ({bt_secs:.3}s)"
        );
        assert!(lsm_rps > 0.0 && bt_rps > 0.0);
        // Spot-check last keys survived.
        let last = format!("Data_lsm_t_{:08}", n - 1);
        assert!(mgr
            .get_raw("lsm_t", &Key::new(last.into_bytes()))
            .unwrap()
            .is_some());
        let last = format!("Data_bt_t_{:08}", n - 1);
        assert!(mgr
            .get_raw("bt_t", &Key::new(last.into_bytes()))
            .unwrap()
            .is_some());
        mgr.close().unwrap();
        let _ = std::fs::remove_dir_all(root);
    }
}
