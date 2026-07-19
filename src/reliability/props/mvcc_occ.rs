//! MVCC Snapshot Isolation + OCC conflict properties.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use proptest::prelude::*;

use crate::config::Config;
use crate::engine::TakyonicEngine;
use crate::error::TakyonicError;
use crate::types::{Key, Value};

fn temp_engine() -> Arc<TakyonicEngine> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = std::env::temp_dir().join(format!("takyonic-prop-mvcc-{nanos}"));
    let cfg = Config::default()
        .data_dir(root.join("data"))
        .wal_dir(root.join("wal"))
        .memtable_size_bytes(8 * 1024 * 1024)
        .block_size_bytes(256)
        .l0_soft_limit(32)
        .l0_hard_limit(64)
        .l0_rapid_pool_threads(1)
        .ln_haul_pool_threads(1)
        .compaction_write_bytes_per_sec(1024 * 1024 * 1024)
        .write_admission_ops_per_sec(100_000)
        .write_admission_min_ops_per_sec(1_000)
        .write_admission_burst(10_000);
    Arc::new(TakyonicEngine::open(cfg).expect("open"))
}

fn k(s: &str) -> Key {
    Key::new(s.as_bytes().to_vec())
}
fn v(s: &str) -> Value {
    Value::new(s.as_bytes().to_vec())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    #[test]
    fn prop_lost_update_detected(
        v1 in "[a-z]{1,8}",
        v2 in "[a-z]{1,8}",
    ) {
        let engine = temp_engine();
        {
            let mut t = engine.begin().unwrap();
            t.put(k("x"), v("0")).unwrap();
            t.commit().unwrap();
        }
        let mut a = engine.begin().unwrap();
        let mut b = engine.begin().unwrap();
        let _ = a.get(k("x")).unwrap();
        let _ = b.get(k("x")).unwrap();
        a.put(k("x"), v(&v1)).unwrap();
        b.put(k("x"), v(&v2)).unwrap();
        let ra = a.commit();
        let rb = b.commit();
        let a_ok = ra.is_ok();
        let b_ok = rb.is_ok();
        prop_assert!(
            a_ok ^ b_ok || (!a_ok && !b_ok),
            "both committed without conflict: a={ra:?} b={rb:?}"
        );
        if let Err(ref e) = ra {
            prop_assert!(
                matches!(e, TakyonicError::Conflict(_)),
                "a err: {e:?}"
            );
        }
        if let Err(ref e) = rb {
            prop_assert!(
                matches!(e, TakyonicError::Conflict(_)),
                "b err: {e:?}"
            );
        }
        engine.close().ok();
    }

    #[test]
    fn prop_snapshot_read_stable_under_concurrent_commit(
        witness in "[a-z]{1,8}",
    ) {
        let engine = temp_engine();
        {
            let mut t = engine.begin().unwrap();
            t.put(k("k"), v("old")).unwrap();
            t.commit().unwrap();
        }
        let mut reader = engine.begin().unwrap();
        let first = reader.get(k("k")).unwrap();
        {
            let mut w = engine.begin().unwrap();
            w.put(k("k"), v(&witness)).unwrap();
            w.commit().unwrap();
        }
        let second = reader.get(k("k")).unwrap();
        prop_assert_eq!(first, second, "SI read must be stable at read_ts");
        reader.abort();
        engine.close().ok();
    }

    #[test]
    fn prop_bank_sum_preserved(
        schedule in prop::collection::vec((0usize..8, 0usize..8, 1i64..20), 1..40),
    ) {
        let engine = temp_engine();
        const N: usize = 8;
        const INIT: i64 = 100;
        for i in 0..N {
            let mut t = engine.begin().unwrap();
            t.put(k(&format!("a{i}")), v(&INIT.to_string())).unwrap();
            t.commit().unwrap();
        }
        for (from, to, amt) in schedule {
            if from == to {
                continue;
            }
            let mut attempts = 0;
            loop {
                attempts += 1;
                let mut t = engine.begin().unwrap();
                let fb = t
                    .get(k(&format!("a{from}")))
                    .unwrap()
                    .and_then(|val| {
                        String::from_utf8_lossy(val.as_bytes())
                            .parse::<i64>()
                            .ok()
                    })
                    .unwrap_or(0);
                let tb = t
                    .get(k(&format!("a{to}")))
                    .unwrap()
                    .and_then(|val| {
                        String::from_utf8_lossy(val.as_bytes())
                            .parse::<i64>()
                            .ok()
                    })
                    .unwrap_or(0);
                if fb < amt {
                    t.abort();
                    break;
                }
                t.put(k(&format!("a{from}")), v(&(fb - amt).to_string()))
                    .unwrap();
                t.put(k(&format!("a{to}")), v(&(tb + amt).to_string()))
                    .unwrap();
                match t.commit() {
                    Ok(_) => break,
                    Err(TakyonicError::Conflict(_)) if attempts < 32 => continue,
                    Err(e) => prop_assert!(false, "transfer err: {e}"),
                }
            }
        }
        let mut sum = 0i64;
        {
            let mut t = engine.begin().unwrap();
            for i in 0..N {
                let bal = t
                    .get(k(&format!("a{i}")))
                    .unwrap()
                    .and_then(|val| {
                        String::from_utf8_lossy(val.as_bytes())
                            .parse::<i64>()
                            .ok()
                    })
                    .unwrap_or(0);
                sum += bal;
            }
            t.abort();
        }
        prop_assert_eq!(sum, INIT * N as i64);
        engine.close().ok();
    }
}
