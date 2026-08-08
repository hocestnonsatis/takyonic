//! Chaos coverage for production Faz W–AA paths (2PC TC log, COPY, MPP).
//!
//! These are in-process fault injections (abandon / inject flags / flaky
//! dispatch), not full SIGKILL process crucibles — those remain in
//! `examples/crash_recovery` and `reliability/continuous`.

#[cfg(test)]
mod tests {
    use std::collections::HashMap as StdHashMap;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    use crate::config::Config;
    use crate::dtxn::{
        DistTxnRequest, LocalShard, ShardParticipant, TransactionCoordinator, put_branch,
    };
    use crate::engine::TakyonicEngine;
    use crate::error::{Result, TakyonicError};
    use crate::mpp::{
        Coordinator, DistAggKind, FragmentDispatcher, FragmentSpec, Worker, WorkerEndpoint,
    };
    use crate::pg::SessionState;
    use crate::schema::{ColumnSpec, Record, TableSchema};
    use crate::shuffle::ShuffleManager;
    use crate::types::{Key, Value};

    fn key(s: &str) -> Key {
        Key::new(s.as_bytes().to_vec())
    }
    fn val(s: &str) -> Value {
        Value::new(s.as_bytes().to_vec())
    }

    fn temp_root(tag: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("takyonic-waa-{tag}-{nanos}"));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        root
    }

    fn open_engine(root: &std::path::Path) -> Arc<TakyonicEngine> {
        let config = Config::default()
            .data_dir(root.join("data"))
            .wal_dir(root.join("wal"))
            .memtable_size_bytes(8 * 1024 * 1024)
            .l0_rapid_pool_threads(1)
            .ln_haul_pool_threads(1)
            .compaction_write_bytes_per_sec(1024 * 1024 * 1024);
        Arc::new(TakyonicEngine::open(config).unwrap())
    }

    /// Faz W: durable `TC_DECISIONS` survives crash-after-decide.
    #[test]
    fn waa_twopc_crash_after_decide_recovers() {
        let root = temp_root("tc");
        let a = LocalShard::new(1);
        let b = LocalShard::new(2);
        {
            let tc = TransactionCoordinator::open(&root, None).unwrap();
            tc.register_shard(a.clone());
            tc.register_shard(b.clone());
            tc.inject_crash_after_decide(true);
            let err = tc
                .execute(DistTxnRequest {
                    read_ts: 0,
                    branches: vec![
                        put_branch(1, key("acct:A"), val("100")),
                        put_branch(2, key("acct:B"), val("50")),
                    ],
                })
                .expect_err("injected crash");
            assert!(
                err.to_string().contains("crash after durable"),
                "{err}"
            );
        }
        let tc2 = TransactionCoordinator::open(&root, None).unwrap();
        tc2.register_shard(a.clone());
        tc2.register_shard(b.clone());
        let _ = tc2.recover_participant(a.as_ref()).unwrap();
        let _ = tc2.recover_participant(b.as_ref()).unwrap();
        assert_eq!(a.get(&key("acct:A")).unwrap().as_bytes(), b"100");
        assert_eq!(b.get(&key("acct:B")).unwrap().as_bytes(), b"50");
        let _ = fs::remove_dir_all(root);
    }

    /// Faz AA: each COPY FROM row is auto-commit INSERT — prefix survives abandon.
    #[test]
    fn waa_copy_partial_prefix_survives_abandon() {
        let root = temp_root("copy");
        let data_dir = root.join("data");
        let wal_dir = root.join("wal");
        let table = "cpy_chaos";
        let tsv = root.join("partial.tsv");

        {
            let engine = open_engine(&root);
            engine
                .register_table(
                    TableSchema::new(table, "id", vec![]).with_columns(vec![
                        ColumnSpec::new("id", "BIGINT"),
                        ColumnSpec::new("name", "TEXT"),
                    ]),
                )
                .unwrap();
            let mut session = SessionState::new(Arc::clone(&engine));

            let mut body = String::new();
            for i in 1..=100 {
                body.push_str(&format!("{i}\trow{i}\n"));
            }
            fs::write(&tsv, &body).unwrap();

            // Simulate mid-COPY: load first 40 rows then hard-crash.
            let text = fs::read_to_string(&tsv).unwrap();
            for line in text.lines().take(40) {
                let fields: Vec<&str> = line.split('\t').collect();
                session
                    .execute_sql(&format!(
                        "INSERT INTO {table} (id, name) VALUES ({}, '{}')",
                        fields[0], fields[1]
                    ))
                    .unwrap();
            }
            engine.abandon_for_crash_test().unwrap();
            std::mem::forget(session);
            std::mem::forget(engine);
        }

        let engine = Arc::new(
            TakyonicEngine::open(
                Config::default()
                    .data_dir(&data_dir)
                    .wal_dir(&wal_dir)
                    .memtable_size_bytes(8 * 1024 * 1024)
                    .l0_rapid_pool_threads(1)
                    .ln_haul_pool_threads(1)
                    .compaction_write_bytes_per_sec(1024 * 1024 * 1024),
            )
            .unwrap(),
        );
        let mut session = SessionState::new(engine);
        let rows = session
            .execute_sql(&format!("SELECT id FROM {table} ORDER BY id"))
            .unwrap();
        assert_eq!(
            rows.rows.len(),
            40,
            "committed COPY prefix must survive abandon"
        );
        assert_eq!(rows.rows[0].get("id"), Some("1"));
        assert_eq!(rows.rows[39].get("id"), Some("40"));

        let mut rest = String::new();
        for i in 41..=100 {
            rest.push_str(&format!("{i}\trow{i}\n"));
        }
        let rest_path = root.join("rest.tsv");
        fs::write(&rest_path, rest).unwrap();
        let path_s = rest_path.to_string_lossy();
        session
            .execute_sql(&format!("COPY {table} FROM '{path_s}'"))
            .unwrap();
        let full = session
            .execute_sql(&format!("SELECT id FROM {table}"))
            .unwrap();
        assert_eq!(full.rows.len(), 100, "resume COPY completes load");
        session.engine().close().unwrap();
        let _ = fs::remove_dir_all(root);
    }

    /// Faz Z: transient remote dispatch failure retries once (reconnect).
    #[test]
    fn waa_mpp_transient_dispatch_retries_once() {
        let root = temp_root("mpp");
        let engine = open_engine(&root);
        engine
            .register_table(
                TableSchema::new("employees", "id", vec![]).with_columns(vec![
                    ColumnSpec::new("id", "BIGINT"),
                    ColumnSpec::new("department", "TEXT"),
                    ColumnSpec::new("salary", "BIGINT"),
                ]),
            )
            .unwrap();
        let mut session = SessionState::new(Arc::clone(&engine));
        session
            .execute_sql(
                "INSERT INTO employees (id, department, salary) VALUES \
                 (1, 'Eng', 100), (2, 'Eng', 200), (3, 'Sales', 50)",
            )
            .unwrap();

        let shuffle = Arc::new(ShuffleManager::new(
            32,
            Some(Arc::clone(engine.metrics())),
        ));
        let worker = Worker::new(
            Arc::clone(&engine),
            Arc::clone(&shuffle),
            Arc::clone(engine.metrics()),
        );
        let endpoints = vec![WorkerEndpoint {
            node_id: 1,
            address: "local".into(),
            slot: 0,
        }];
        let coord = Coordinator::local(Arc::clone(&engine), shuffle, endpoints);

        struct FlakyOnce {
            inner: Worker,
            attempts: AtomicU64,
        }
        impl FragmentDispatcher for FlakyOnce {
            fn execute_remote(&self, _node_id: u64, fragment: &FragmentSpec) -> Result<Vec<Record>> {
                if self.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                    return Err(TakyonicError::Engine(
                        "connection unavailable (injected)".into(),
                    ));
                }
                self.inner.execute_fragment(fragment)
            }
        }
        let dispatcher = Arc::new(FlakyOnce {
            inner: worker,
            attempts: AtomicU64::new(0),
        });
        let attempts = Arc::clone(&dispatcher);
        coord.set_dispatcher(dispatcher);

        let rows = coord
            .execute_distributed_aggregate(
                "employees",
                "department",
                DistAggKind::Sum("salary".into()),
            )
            .expect("transient failure should be retried");
        assert!(
            attempts.attempts.load(Ordering::SeqCst) >= 2,
            "expected fail-then-retry (≥2 dispatch attempts)"
        );
        let mut map = StdHashMap::new();
        for r in &rows {
            map.insert(
                r.get("department").unwrap().to_string(),
                r.get("SUM(salary)").unwrap().parse::<i64>().unwrap(),
            );
        }
        assert_eq!(map.get("Eng"), Some(&300));
        assert_eq!(map.get("Sales"), Some(&50));
        engine.close().unwrap();
        let _ = fs::remove_dir_all(root);
    }

    /// Compose: 3-shard 2PC crash-after-decide + recover, then MPP SUM over
    /// engine-visible data written in a follow-up local path.
    #[test]
    fn waa_twopc_three_shard_then_mpp_aggregate() {
        let root = temp_root("tc3-mpp");
        let a = LocalShard::new(1);
        let b = LocalShard::new(2);
        let c = LocalShard::new(3);
        {
            let tc = TransactionCoordinator::open(&root, None).unwrap();
            tc.register_shard(a.clone());
            tc.register_shard(b.clone());
            tc.register_shard(c.clone());
            tc.inject_crash_after_decide(true);
            let err = tc
                .execute(DistTxnRequest {
                    read_ts: 0,
                    branches: vec![
                        put_branch(1, key("acct:A"), val("100")),
                        put_branch(2, key("acct:B"), val("50")),
                        put_branch(3, key("acct:C"), val("25")),
                    ],
                })
                .expect_err("injected crash");
            assert!(
                err.to_string().contains("crash after durable"),
                "{err}"
            );
        }
        let tc2 = TransactionCoordinator::open(&root, None).unwrap();
        tc2.register_shard(a.clone());
        tc2.register_shard(b.clone());
        tc2.register_shard(c.clone());
        let _ = tc2.recover_participant(a.as_ref()).unwrap();
        let _ = tc2.recover_participant(b.as_ref()).unwrap();
        let _ = tc2.recover_participant(c.as_ref()).unwrap();
        assert_eq!(a.get(&key("acct:A")).unwrap().as_bytes(), b"100");
        assert_eq!(b.get(&key("acct:B")).unwrap().as_bytes(), b"50");
        assert_eq!(c.get(&key("acct:C")).unwrap().as_bytes(), b"25");

        // Mixed load: engine MPP agg under admission-friendly small config.
        let engine = open_engine(&root);
        engine
            .register_table(
                TableSchema::new("employees", "id", vec![]).with_columns(vec![
                    ColumnSpec::new("id", "TEXT"),
                    ColumnSpec::new("department", "TEXT"),
                    ColumnSpec::new("salary", "INT"),
                ]),
            )
            .unwrap();
        {
            let mut session = SessionState::new(Arc::clone(&engine));
            for (id, dept, sal) in [("1", "Eng", "100"), ("2", "Eng", "200"), ("3", "Sales", "50")]
            {
                session
                    .execute_sql(&format!(
                        "INSERT INTO employees (id, department, salary) VALUES ('{id}', '{dept}', '{sal}')"
                    ))
                    .unwrap();
            }
        }
        let shuffle = Arc::new(ShuffleManager::new(
            32,
            Some(Arc::clone(engine.metrics())),
        ));
        let workers: Vec<_> = (0..3u32)
            .map(|slot| WorkerEndpoint {
                node_id: u64::from(slot) + 1,
                address: format!("local:{slot}"),
                slot,
            })
            .collect();
        let coord = Coordinator::local(Arc::clone(&engine), Arc::clone(&shuffle), workers);
        struct LocalDispatch(Worker);
        impl FragmentDispatcher for LocalDispatch {
            fn execute_remote(&self, _node_id: u64, fragment: &FragmentSpec) -> Result<Vec<Record>> {
                self.0.execute_fragment(fragment)
            }
        }
        coord.set_dispatcher(Arc::new(LocalDispatch(Worker::new(
            Arc::clone(&engine),
            Arc::clone(&shuffle),
            Arc::clone(engine.metrics()),
        ))));
        let rows = coord
            .execute_distributed_aggregate(
                "employees",
                "department",
                DistAggKind::Sum("salary".into()),
            )
            .expect("mpp after 2PC recover");
        let mut map = StdHashMap::new();
        for r in &rows {
            map.insert(
                r.get("department").unwrap().to_string(),
                r.get("SUM(salary)").unwrap().parse::<i64>().unwrap(),
            );
        }
        assert_eq!(map.get("Eng"), Some(&300));
        assert_eq!(map.get("Sales"), Some(&50));
        engine.close().unwrap();
        let _ = fs::remove_dir_all(root);
    }

    /// Faz 3.4: concurrent 2PC + MPP + engine writes under tight L0 / admission —
    /// must complete without deadlock (watchdog timeout).
    #[test]
    fn waa_mixed_mpp_twopc_under_l0_pressure() {
        use std::sync::mpsc;
        use std::thread;
        use std::time::Duration;

        let root = temp_root("mixed-l0");
        let config = Config::default()
            .data_dir(root.join("data"))
            .wal_dir(root.join("wal"))
            .memtable_size_bytes(64 * 1024) // force frequent flushes → L0 pressure
            .block_size_bytes(64)
            .l0_soft_limit(2)
            .l0_hard_limit(4)
            .l0_rapid_pool_threads(1)
            .ln_haul_pool_threads(1)
            .compaction_write_bytes_per_sec(4 * 1024 * 1024)
            .write_admission_ops_per_sec(2_000)
            .write_admission_min_ops_per_sec(100)
            .write_admission_burst(200)
            .mpp_enabled(true)
            .metrics_enabled(true)
            .metrics_bind("127.0.0.1:0");
        let engine = Arc::new(TakyonicEngine::open(config).unwrap());
        engine
            .register_table(
                TableSchema::new("employees", "id", vec![]).with_columns(vec![
                    ColumnSpec::new("id", "TEXT"),
                    ColumnSpec::new("department", "TEXT"),
                    ColumnSpec::new("salary", "INT"),
                ]),
            )
            .unwrap();

        let (done_tx, done_rx) = mpsc::channel::<&'static str>();

        // Writer: engine INSERTs (admission + L0 backpressure).
        let eng_w = Arc::clone(&engine);
        let tx_w = done_tx.clone();
        thread::spawn(move || {
            let mut session = SessionState::new(eng_w);
            for i in 0..80 {
                let dept = if i % 2 == 0 { "Eng" } else { "Sales" };
                let _ = session.execute_sql(&format!(
                    "INSERT INTO employees (id, department, salary) VALUES ('w{i}', '{dept}', '{i}')"
                ));
            }
            let _ = tx_w.send("writer");
        });

        // 2PC: LocalShards crash-free path under concurrent load.
        let tc_root = root.join("tc");
        let _ = fs::create_dir_all(&tc_root);
        let tx_tc = done_tx.clone();
        thread::spawn(move || {
            let a = LocalShard::new(1);
            let b = LocalShard::new(2);
            let tc = TransactionCoordinator::open(&tc_root, None).unwrap();
            tc.register_shard(a.clone());
            tc.register_shard(b.clone());
            for i in 0..40 {
                let _ = tc.execute(DistTxnRequest {
                    read_ts: 0,
                    branches: vec![
                        put_branch(1, key(&format!("k:a:{i}")), val(&format!("{i}"))),
                        put_branch(2, key(&format!("k:b:{i}")), val(&format!("{i}"))),
                    ],
                });
            }
            let _ = tx_tc.send("twopc");
        });

        // MPP reader: distributed SUM while writers stress L0.
        let eng_r = Arc::clone(&engine);
        let tx_r = done_tx;
        thread::spawn(move || {
            let shuffle = Arc::new(ShuffleManager::new(
                32,
                Some(Arc::clone(eng_r.metrics())),
            ));
            let workers: Vec<_> = (0..3u32)
                .map(|slot| WorkerEndpoint {
                    node_id: u64::from(slot) + 1,
                    address: format!("local:{slot}"),
                    slot,
                })
                .collect();
            let coord = Coordinator::local(Arc::clone(&eng_r), Arc::clone(&shuffle), workers);
            struct LocalDispatch(Worker);
            impl FragmentDispatcher for LocalDispatch {
                fn execute_remote(
                    &self,
                    _node_id: u64,
                    fragment: &FragmentSpec,
                ) -> Result<Vec<Record>> {
                    self.0.execute_fragment(fragment)
                }
            }
            coord.set_dispatcher(Arc::new(LocalDispatch(Worker::new(
                Arc::clone(&eng_r),
                Arc::clone(&shuffle),
                Arc::clone(eng_r.metrics()),
            ))));
            for _ in 0..20 {
                let _ = coord.execute_distributed_aggregate(
                    "employees",
                    "department",
                    DistAggKind::Sum("salary".into()),
                );
                thread::sleep(Duration::from_millis(5));
            }
            let _ = tx_r.send("mpp");
        });

        let mut finished = StdHashMap::new();
        let deadline = Duration::from_secs(45);
        let start = std::time::Instant::now();
        while finished.len() < 3 {
            let remain = deadline.saturating_sub(start.elapsed());
            match done_rx.recv_timeout(remain) {
                Ok(tag) => {
                    finished.insert(tag, ());
                }
                Err(_) => panic!(
                    "mixed MPP+2PC+writer under L0 pressure timed out after {:?} \
                     (finished={:?}) — possible admission/compaction deadlock",
                    start.elapsed(),
                    finished.keys().collect::<Vec<_>>()
                ),
            }
        }

        // Final MPP agg must still succeed after pressure.
        let shuffle = Arc::new(ShuffleManager::new(
            32,
            Some(Arc::clone(engine.metrics())),
        ));
        let workers: Vec<_> = (0..3u32)
            .map(|slot| WorkerEndpoint {
                node_id: u64::from(slot) + 1,
                address: format!("local:{slot}"),
                slot,
            })
            .collect();
        let coord = Coordinator::local(Arc::clone(&engine), Arc::clone(&shuffle), workers);
        struct LocalDispatch2(Worker);
        impl FragmentDispatcher for LocalDispatch2 {
            fn execute_remote(&self, _node_id: u64, fragment: &FragmentSpec) -> Result<Vec<Record>> {
                self.0.execute_fragment(fragment)
            }
        }
        coord.set_dispatcher(Arc::new(LocalDispatch2(Worker::new(
            Arc::clone(&engine),
            Arc::clone(&shuffle),
            Arc::clone(engine.metrics()),
        ))));
        let rows = coord
            .execute_distributed_aggregate(
                "employees",
                "department",
                DistAggKind::Sum("salary".into()),
            )
            .expect("post-pressure MPP");
        assert!(!rows.is_empty() || engine.metrics().mpp_fragments() > 0);

        engine.close().unwrap();
        let _ = fs::remove_dir_all(root);
    }
}
