//! Concurrent MVCC bank soak.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::config::Config;
use crate::engine::TakyonicEngine;
use crate::error::TakyonicError;
use crate::pg::SessionState;
use crate::reliability::ReliabilityReport;
use crate::schema::{Record, TableSchema};

const INITIAL_BALANCE: i64 = 1000;
const MAX_RETRIES: usize = 64;

/// Configuration for [`run_mvcc_soak`].
pub struct MvccSoakConfig {
    /// RNG seed for worker RNGs.
    pub seed: u64,
    /// Concurrent transfer workers.
    pub writers: usize,
    /// Concurrent sum readers.
    pub readers: usize,
    /// Wall-clock duration.
    pub duration: Duration,
    /// Number of accounts to seed.
    pub accounts: usize,
}

fn open_engine() -> (Arc<TakyonicEngine>, std::path::PathBuf) {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = std::env::temp_dir().join(format!("takyonic-mvcc-soak-{nanos}"));
    let config = Config::default()
        .data_dir(root.join("data"))
        .wal_dir(root.join("wal"))
        .memtable_size_bytes(64 * 1024 * 1024)
        .block_size_bytes(64)
        .l0_soft_limit(8)
        .l0_hard_limit(32)
        .l0_rapid_pool_threads(1)
        .ln_haul_pool_threads(1)
        .compaction_write_bytes_per_sec(1024 * 1024 * 1024)
        .write_admission_ops_per_sec(100_000)
        .write_admission_min_ops_per_sec(1_000)
        .write_admission_burst(10_000);
    let engine = Arc::new(TakyonicEngine::open(config).unwrap());
    (engine, root)
}

fn account_record(id: usize, balance: i64) -> Record {
    Record::new()
        .set("id", id.to_string())
        .set("balance", balance.to_string())
        .set("owner", format!("u{}", id % 8))
}

fn parse_balance(rec: &Record) -> i64 {
    rec.get("balance")
        .and_then(|s| s.parse().ok())
        .unwrap_or(0)
}

fn xorshift(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    x
}

fn transfer_once(
    engine: &Arc<TakyonicEngine>,
    from: usize,
    to: usize,
    amount: i64,
) -> Result<(), TakyonicError> {
    if from == to || amount <= 0 {
        return Ok(());
    }
    let mut txn = engine.begin()?;
    let from_pk = from.to_string();
    let to_pk = to.to_string();
    let from_rec = txn
        .get_record("accounts", &from_pk)?
        .ok_or_else(|| TakyonicError::Sql(format!("missing account {from}")))?;
    let to_rec = txn
        .get_record("accounts", &to_pk)?
        .ok_or_else(|| TakyonicError::Sql(format!("missing account {to}")))?;
    let from_bal = parse_balance(&from_rec);
    let to_bal = parse_balance(&to_rec);
    if from_bal < amount {
        txn.abort();
        return Ok(());
    }
    txn.put_record(
        "accounts",
        account_record(from, from_bal - amount),
    )?;
    txn.put_record("accounts", account_record(to, to_bal + amount))?;
    txn.commit()?;
    Ok(())
}

fn sum_balances(engine: &Arc<TakyonicEngine>) -> Result<i64, TakyonicError> {
    let mut txn = engine.begin()?;
    let rows = txn.scan_table_records("accounts")?;
    let sum = rows.iter().map(parse_balance).sum();
    txn.abort();
    Ok(sum)
}

/// Run a concurrent bank soak; bank sum and SI visibility must hold.
pub fn run_mvcc_soak(cfg: MvccSoakConfig) -> ReliabilityReport {
    let mut report = ReliabilityReport::new(cfg.seed);
    let expected_sum = cfg.accounts as i64 * INITIAL_BALANCE;
    let (engine, root) = open_engine();
    if let Err(e) = engine.register_table(TableSchema::new("accounts", "id", vec![])) {
        report.fail(format!("register_table: {e}"));
        let _ = std::fs::remove_dir_all(&root);
        return report;
    }

    for id in 0..cfg.accounts {
        let mut txn = match engine.begin() {
            Ok(t) => t,
            Err(e) => {
                report.fail(format!("seed begin: {e}"));
                let _ = std::fs::remove_dir_all(&root);
                return report;
            }
        };
        if let Err(e) = txn.put_record("accounts", account_record(id, INITIAL_BALANCE)) {
            report.fail(format!("seed put: {e}"));
            let _ = std::fs::remove_dir_all(&root);
            return report;
        }
        if let Err(e) = txn.commit() {
            report.fail(format!("seed commit: {e}"));
            let _ = std::fs::remove_dir_all(&root);
            return report;
        }
    }

    match sum_balances(&engine) {
        Ok(s) if s == expected_sum => {}
        Ok(s) => {
            report.fail(format!("seed sum {s} != {expected_sum}"));
            let _ = std::fs::remove_dir_all(&root);
            return report;
        }
        Err(e) => {
            report.fail(format!("seed sum: {e}"));
            let _ = std::fs::remove_dir_all(&root);
            return report;
        }
    }

    let stop = Arc::new(AtomicBool::new(false));
    let ops = Arc::new(AtomicU64::new(0));
    let violations: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let start = Instant::now();

    let mut handles = Vec::new();
    for w in 0..cfg.writers {
        let engine = Arc::clone(&engine);
        let stop = Arc::clone(&stop);
        let ops = Arc::clone(&ops);
        let violations = Arc::clone(&violations);
        let accounts = cfg.accounts;
        let mut rng = cfg.seed ^ ((w as u64 + 1) * 0x9E37_79B9_7F4A_7C15);
        handles.push(std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                let from = (xorshift(&mut rng) as usize) % accounts;
                let to = (xorshift(&mut rng) as usize) % accounts;
                let amount = (xorshift(&mut rng) % 50) as i64 + 1;
                let mut attempt = 0;
                loop {
                    match transfer_once(&engine, from, to, amount) {
                        Ok(()) => {
                            ops.fetch_add(1, Ordering::Relaxed);
                            break;
                        }
                        Err(TakyonicError::Conflict(_)) => {
                            attempt += 1;
                            if attempt >= MAX_RETRIES {
                                if let Ok(mut v) = violations.lock() {
                                    v.push(format!(
                                        "writer {w} exhausted OCC retries from={from} to={to}"
                                    ));
                                }
                                stop.store(true, Ordering::Relaxed);
                                return;
                            }
                        }
                        Err(e) => {
                            if let Ok(mut v) = violations.lock() {
                                v.push(format!("writer {w}: {e}"));
                            }
                            stop.store(true, Ordering::Relaxed);
                            return;
                        }
                    }
                }
            }
        }));
    }

    for r in 0..cfg.readers {
        let engine = Arc::clone(&engine);
        let stop = Arc::clone(&stop);
        let violations = Arc::clone(&violations);
        handles.push(std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                match sum_balances(&engine) {
                    Ok(sum) if sum == expected_sum => {}
                    Ok(sum) => {
                        if let Ok(mut v) = violations.lock() {
                            v.push(format!(
                                "reader {r}: sum {sum} != expected {expected_sum}"
                            ));
                        }
                        stop.store(true, Ordering::Relaxed);
                        return;
                    }
                    Err(e) => {
                        if let Ok(mut v) = violations.lock() {
                            v.push(format!("reader {r}: {e}"));
                        }
                        stop.store(true, Ordering::Relaxed);
                        return;
                    }
                }
                std::thread::sleep(Duration::from_millis(5));
            }
        }));
    }

    while start.elapsed() < cfg.duration && !stop.load(Ordering::Relaxed) {
        std::thread::sleep(Duration::from_millis(20));
    }
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        let _ = h.join();
    }

    report.ops = ops.load(Ordering::Relaxed);
    if let Ok(v) = violations.lock() {
        for msg in v.iter() {
            report.fail(msg.clone());
        }
    }

    match sum_balances(&engine) {
        Ok(sum) if sum == expected_sum => {}
        Ok(sum) => report.fail(format!("final sum {sum} != {expected_sum}")),
        Err(e) => report.fail(format!("final sum: {e}")),
    }

    // VACUUM after writers/readers stop so the watermark is idle; sum must hold.
    if report.ok() {
        let mut session = SessionState::new(Arc::clone(&engine));
        if let Err(e) = session.execute_sql("VACUUM accounts") {
            report.fail(format!("VACUUM: {e}"));
        } else {
            match sum_balances(&engine) {
                Ok(sum) if sum == expected_sum => {}
                Ok(sum) => report.fail(format!("post-VACUUM sum {sum} != {expected_sum}")),
                Err(e) => report.fail(format!("post-VACUUM sum: {e}")),
            }
        }
    }

    let _ = engine.close();
    let _ = std::fs::remove_dir_all(&root);
    report
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mvcc_soak_short_preserves_bank_sum() {
        let cfg = MvccSoakConfig {
            seed: 7,
            writers: 4,
            readers: 2,
            duration: Duration::from_secs(crate::reliability::env_u64(
                "TAKYONIC_SOAK_SECS",
                5,
            )),
            accounts: 32,
        };
        let report = run_mvcc_soak(cfg);
        assert!(report.ok(), "{}", report.summary());
        assert!(report.ops > 0);
    }
}
