//! Optional demo catalog bootstrap (`users` table).
//!
//! Production / empty clusters start with no application tables: create them via
//! SQL (`CREATE TABLE …`) or the register API. The classic pgwire README demo
//! can still seed `users(id, status, city)` when explicitly enabled (server
//! default for back-compat).

use crate::error::Result;
use crate::schema::{IndexDef, TableSchema};
use crate::TakyonicEngine;

/// Outcome of [`ensure_demo_users`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DemoSeedOutcome {
    /// `--no-demo-bootstrap` / `TAKYONIC_DEMO_BOOTSTRAP=0`.
    SkippedDisabled,
    /// Catalog already has `users` (idempotent reopen / peer race).
    SkippedAlreadyPresent,
    /// Registered the demo schema.
    Registered,
}

/// Classic demo schema: `users` with secondary indexes on `status` and `city`.
pub fn demo_users_schema() -> TableSchema {
    TableSchema::new(
        "users",
        "id",
        vec![
            IndexDef::new("status", "status"),
            IndexDef::new("city", "city"),
        ],
    )
}

/// Whether demo seed should run given the flag and catalog presence.
pub fn should_seed_demo_users(enabled: bool, users_present: bool) -> bool {
    enabled && !users_present
}

/// Register the demo `users` table when `enabled` and the catalog lacks it.
///
/// Idempotent: safe to call on every node / restart (migrate-on-empty).
pub fn ensure_demo_users(engine: &TakyonicEngine, enabled: bool) -> Result<DemoSeedOutcome> {
    let users_present = engine.table_schema("users").is_ok();
    if !should_seed_demo_users(enabled, users_present) {
        return Ok(if !enabled {
            DemoSeedOutcome::SkippedDisabled
        } else {
            DemoSeedOutcome::SkippedAlreadyPresent
        });
    }
    engine.register_table(demo_users_schema())?;
    Ok(DemoSeedOutcome::Registered)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::schema::ColumnSpec;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_engine(tag: &str) -> (Arc<TakyonicEngine>, std::path::PathBuf) {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("takyonic-demo-{tag}-{nanos}"));
        let cfg = Config::default()
            .data_dir(root.join("data"))
            .wal_dir(root.join("wal"))
            .memtable_size_bytes(64 * 1024 * 1024)
            .l0_rapid_pool_threads(1)
            .ln_haul_pool_threads(1)
            .compaction_write_bytes_per_sec(1024 * 1024 * 1024);
        let engine = Arc::new(TakyonicEngine::open(cfg).unwrap());
        (engine, root)
    }

    #[test]
    fn should_seed_only_when_enabled_and_missing() {
        assert!(should_seed_demo_users(true, false));
        assert!(!should_seed_demo_users(true, true));
        assert!(!should_seed_demo_users(false, false));
        assert!(!should_seed_demo_users(false, true));
    }

    #[test]
    fn ensure_skips_when_disabled_leaving_empty_catalog() {
        let (engine, root) = temp_engine("off");
        assert_eq!(
            ensure_demo_users(&engine, false).unwrap(),
            DemoSeedOutcome::SkippedDisabled
        );
        assert!(engine.table_schema("users").is_err());
        // Empty cluster: SQL DDL still works without hardcoded users.
        engine
            .create_table(
                "items",
                "id",
                vec![
                    ColumnSpec::new("id", "BIGINT"),
                    ColumnSpec::new("name", "TEXT"),
                ],
                false,
            )
            .unwrap();
        assert!(engine.table_schema("items").is_ok());
        engine.close().unwrap();
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn ensure_registers_once_then_idempotent() {
        let (engine, root) = temp_engine("on");
        assert_eq!(
            ensure_demo_users(&engine, true).unwrap(),
            DemoSeedOutcome::Registered
        );
        let schema = engine.table_schema("users").unwrap();
        assert_eq!(schema.primary_key, "id");
        assert_eq!(schema.indexes.len(), 2);
        assert_eq!(
            ensure_demo_users(&engine, true).unwrap(),
            DemoSeedOutcome::SkippedAlreadyPresent
        );
        engine.close().unwrap();
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn ensure_survives_reopen_without_re_register() {
        let (engine, root) = temp_engine("reopen");
        let data_dir = engine.config().data_dir.clone();
        let wal_dir = engine.config().wal_dir.clone();
        ensure_demo_users(&engine, true).unwrap();
        engine.close().unwrap();

        let reopened = Arc::new(
            TakyonicEngine::open(
                Config::default()
                    .data_dir(data_dir)
                    .wal_dir(wal_dir)
                    .memtable_size_bytes(64 * 1024 * 1024)
                    .l0_rapid_pool_threads(1)
                    .ln_haul_pool_threads(1)
                    .compaction_write_bytes_per_sec(1024 * 1024 * 1024),
            )
            .unwrap(),
        );
        assert_eq!(
            ensure_demo_users(&reopened, true).unwrap(),
            DemoSeedOutcome::SkippedAlreadyPresent
        );
        reopened.close().unwrap();
        let _ = std::fs::remove_dir_all(root);
    }
}
