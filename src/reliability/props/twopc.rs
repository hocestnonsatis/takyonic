//! Distributed 2PC atomicity / recovery properties.

use proptest::prelude::*;

use crate::dtxn::{
    DistTxnOutcome, DistTxnRequest, LocalShard, ShardParticipant, TransactionCoordinator,
    put_branch,
};
use crate::types::{Key, Value};

fn key(s: &str) -> Key {
    Key::new(s.as_bytes().to_vec())
}
fn val(s: &str) -> Value {
    Value::new(s.as_bytes().to_vec())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]

    #[test]
    fn prop_cross_shard_all_or_nothing(
        a_val in "[0-9]{1,4}",
        b_val in "[0-9]{1,4}",
        fail_b in proptest::bool::ANY,
    ) {
        let tc = TransactionCoordinator::new(None);
        let a = LocalShard::new(1);
        let b = LocalShard::new(2);
        tc.register_shard(a.clone());
        tc.register_shard(b.clone());

        // Seed committed baseline.
        let _ = tc.execute(DistTxnRequest {
            read_ts: 0,
            branches: vec![
                put_branch(1, key("x"), val("0")),
                put_branch(2, key("y"), val("0")),
            ],
        });

        if fail_b {
            b.inject_prepare_failure(true);
        }
        let out = tc
            .execute(DistTxnRequest {
                read_ts: 0,
                branches: vec![
                    put_branch(1, key("x"), val(&a_val)),
                    put_branch(2, key("y"), val(&b_val)),
                ],
            })
            .unwrap();

        match out {
            DistTxnOutcome::Committed { .. } => {
                prop_assert!(!fail_b);
                let ax = a.get(&key("x")).unwrap();
                let by = b.get(&key("y")).unwrap();
                prop_assert_eq!(ax.as_bytes(), a_val.as_bytes());
                prop_assert_eq!(by.as_bytes(), b_val.as_bytes());
            }
            DistTxnOutcome::Aborted { .. } => {
                prop_assert!(fail_b);
                let ax = a.get(&key("x")).unwrap();
                let by = b.get(&key("y")).unwrap();
                prop_assert_eq!(ax.as_bytes(), b"0");
                prop_assert_eq!(by.as_bytes(), b"0");
            }
        }
        prop_assert!(a.orphaned_prepared().is_empty());
        prop_assert!(b.orphaned_prepared().is_empty());
    }

    #[test]
    fn prop_crash_after_prepare_then_recover(payload in "[a-z]{1,6}") {
        let tc = TransactionCoordinator::new(None);
        let a = LocalShard::new(1);
        let b = LocalShard::new(2);
        tc.register_shard(a.clone());
        tc.register_shard(b.clone());

        // Baseline so abort leaves a visible prior value on shard A.
        tc.execute(DistTxnRequest {
            read_ts: 0,
            branches: vec![put_branch(1, key("p"), val("base"))],
        })
        .unwrap();

        b.inject_crash_after_prepare(true);
        let out = tc
            .execute(DistTxnRequest {
                read_ts: 0,
                branches: vec![
                    put_branch(1, key("p"), val(&payload)),
                    put_branch(2, key("q"), val(&payload)),
                ],
            })
            .unwrap();
        let aborted = matches!(out, DistTxnOutcome::Aborted { .. });
        prop_assert!(aborted, "expected abort after crash injection");

        b.inject_crash_after_prepare(false);
        let resolved = tc.recover_participant(b.as_ref()).unwrap();
        prop_assert!(resolved >= 1);
        prop_assert!(b.orphaned_prepared().is_empty(), "orphans after recover");
        prop_assert!(b.get(&key("q")).is_none());
        let ap = a.get(&key("p")).unwrap();
        prop_assert_eq!(ap.as_bytes(), b"base");
    }
}
