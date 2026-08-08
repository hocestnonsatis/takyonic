//! Step 15: MVCC bank transfer crucible (snapshot isolation + OCC).
//!
//! 100 accounts × $1000 = $100_000 invariant under concurrent transfers.
//! Clients use begin/get/put/commit with retry on Conflict.
//!
//! Usage:
//!   cargo run --release --example mvcc_bank

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use takyonic::{Config, Key, TakyonicEngine, TakyonicError, Value};

const ACCOUNTS: u64 = 100;
const INITIAL: i64 = 1000;
const TARGET_SUM: i64 = ACCOUNTS as i64 * INITIAL;
const TRANSFERS_PER_WORKER: u64 = 500;
const WORKERS: usize = 8;

fn account_key(id: u64) -> Key {
    Key::new(format!("acct-{id:03}").into_bytes())
}

fn encode_balance(n: i64) -> Value {
    Value::new(n.to_le_bytes().to_vec())
}

fn decode_balance(v: &Value) -> i64 {
    let b = v.as_bytes();
    assert_eq!(b.len(), 8, "balance encoding");
    i64::from_le_bytes(b.try_into().unwrap())
}

fn transfer(engine: &Arc<TakyonicEngine>, from: u64, to: u64, amount: i64) -> Result<(), TakyonicError> {
    let mut txn = engine.begin()?;
    let from_key = account_key(from);
    let to_key = account_key(to);
    let from_bal = txn
        .get(from_key.clone())?
        .map(|v| decode_balance(&v))
        .unwrap_or(0);
    let to_bal = txn
        .get(to_key.clone())?
        .map(|v| decode_balance(&v))
        .unwrap_or(0);
    if from_bal < amount {
        txn.abort();
        return Ok(());
    }
    txn.put(from_key, encode_balance(from_bal - amount))?;
    txn.put(to_key, encode_balance(to_bal + amount))?;
    txn.commit()?;
    Ok(())
}

fn sum_all(engine: &TakyonicEngine) -> Result<i64, TakyonicError> {
    let mut sum = 0i64;
    for id in 0..ACCOUNTS {
        let v = engine.get(&account_key(id))?;
        sum += v.map(|x| decode_balance(&x)).unwrap_or(0);
    }
    Ok(sum)
}

fn main() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("takyonic=warn")
        .try_init();

    let root = std::env::temp_dir().join(format!("takyonic-mvcc-bank-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();

    let engine = Arc::new(
        TakyonicEngine::open(
            Config::default()
                .data_dir(root.join("data"))
                .wal_dir(root.join("wal"))
                .memtable_size_bytes(256 * 1024)
                .l0_soft_limit(16)
                .l0_hard_limit(48)
                .l0_rapid_pool_threads(1)
                .ln_haul_pool_threads(1)
                .compaction_write_bytes_per_sec(64 * 1024 * 1024)
                .write_admission_ops_per_sec(500_000)
                .write_admission_min_ops_per_sec(10_000)
                .write_admission_burst(50_000),
        )
        .expect("open engine"),
    );

    println!("== Takyonic MVCC bank crucible ==");
    println!("accounts={ACCOUNTS} initial={INITIAL} invariant={TARGET_SUM}");

    // Seed accounts (each in its own txn for clarity).
    for id in 0..ACCOUNTS {
        let mut txn = engine.begin().unwrap();
        txn.put(account_key(id), encode_balance(INITIAL)).unwrap();
        txn.commit().unwrap();
    }
    let seeded = sum_all(&engine).unwrap();
    assert_eq!(seeded, TARGET_SUM, "seed sum");
    println!("phase0: seeded sum={seeded}");

    let ok = Arc::new(AtomicU64::new(0));
    let conflicts = Arc::new(AtomicU64::new(0));
    let other_err = Arc::new(AtomicU64::new(0));
    let start = Instant::now();

    let mut handles = Vec::new();
    for w in 0..WORKERS {
        let engine = Arc::clone(&engine);
        let ok = Arc::clone(&ok);
        let conflicts = Arc::clone(&conflicts);
        let other_err = Arc::clone(&other_err);
        handles.push(std::thread::spawn(move || {
            let mut rng = w as u64 + 1;
            for _ in 0..TRANSFERS_PER_WORKER {
                // xorshift
                rng ^= rng << 13;
                rng ^= rng >> 7;
                rng ^= rng << 17;
                let from = rng % ACCOUNTS;
                let to = (rng / ACCOUNTS) % ACCOUNTS;
                if from == to {
                    continue;
                }
                let amount = 1 + (rng % 50) as i64;
                // Retry loop on OCC conflict.
                loop {
                    match transfer(&engine, from, to, amount) {
                        Ok(()) => {
                            ok.fetch_add(1, Ordering::Relaxed);
                            break;
                        }
                        Err(TakyonicError::Conflict(_)) => {
                            conflicts.fetch_add(1, Ordering::Relaxed);
                            std::thread::yield_now();
                        }
                        Err(e) => {
                            other_err.fetch_add(1, Ordering::Relaxed);
                            eprintln!("worker {w} error: {e}");
                            break;
                        }
                    }
                }
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }
    let elapsed = start.elapsed();

    // Let compaction settle briefly.
    std::thread::sleep(Duration::from_millis(200));

    let final_sum = sum_all(&engine).unwrap();
    let committed = ok.load(Ordering::Relaxed);
    let aborted = conflicts.load(Ordering::Relaxed);
    let errors = other_err.load(Ordering::Relaxed);

    println!(
        "phase1: transfers committed={committed} occ_aborts={aborted} other_err={errors} elapsed={elapsed:?}"
    );
    println!(
        "phase1: abort_rate={:.1}%",
        100.0 * aborted as f64 / (committed + aborted).max(1) as f64
    );
    println!("phase2: final sum={final_sum} (expected {TARGET_SUM})");

    engine.close().unwrap();
    let _ = std::fs::remove_dir_all(&root);

    assert_eq!(errors, 0, "unexpected errors");
    assert_eq!(
        final_sum, TARGET_SUM,
        "bank invariant violated: sum={final_sum}"
    );
    println!("VERDICT: PASS — MVCC snapshot isolation held the bank invariant");
}
