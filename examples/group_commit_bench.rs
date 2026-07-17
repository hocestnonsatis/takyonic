//! Step 9: local group-commit throughput benchmark (no network).
//!
//! Compares against the Step 8 Termux ceiling of ~7.3k ops/sec (per-op fsync).
//! Large memtable + high admission limits isolate the WAL group-commit path.
//!
//! Usage:
//!   cargo run --release --example group_commit_bench -- \
//!       [writers] [total_ops] [value_bytes] [max_secs]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use takyonic::{Config, TakyonicEngine};

struct XorShift(u64);

impl XorShift {
    fn new(seed: u64) -> Self {
        Self(seed | 1)
    }

    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }
}

fn arg<T: std::str::FromStr>(n: usize, default: T) -> T {
    std::env::args()
        .nth(n)
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn fmt_micros(us: u64) -> String {
    if us >= 1_000_000 {
        format!("{:.2}s", us as f64 / 1e6)
    } else if us >= 1_000 {
        format!("{:.1}ms", us as f64 / 1e3)
    } else {
        format!("{us}µs")
    }
}

fn main() {
    let writers: usize = arg(1, 16);
    let total_ops: u64 = arg(2, 500_000);
    let value_bytes: usize = arg(3, 96);
    let max_secs: u64 = arg(4, 120);

    let root = std::env::temp_dir().join(format!("takyonic-gc-bench-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);

    // Isolate group-commit: big memtable, high L0 limits, generous admission.
    let config = Config::default()
        .data_dir(root.join("data"))
        .wal_dir(root.join("wal"))
        .memtable_size_bytes(64 * 1024 * 1024)
        .block_size_bytes(4 * 1024)
        .l0_soft_limit(64)
        .l0_hard_limit(128)
        .l0_rapid_pool_threads(2)
        .ln_haul_pool_threads(1)
        .compaction_write_bytes_per_sec(64 * 1024 * 1024)
        .write_admission_ops_per_sec(1_000_000)
        .write_admission_min_ops_per_sec(100_000)
        .write_admission_burst(100_000);

    println!("== Takyonic group-commit bench ==");
    println!(
        "writers={writers} total_ops={total_ops} value_bytes={value_bytes} max_secs={max_secs}"
    );
    println!("baseline (Step 8 per-op fsync): ~7.3k ops/sec on this host");

    let engine = Arc::new(TakyonicEngine::open(config).expect("open"));
    let next_op = Arc::new(AtomicU64::new(0));
    let ok = Arc::new(AtomicU64::new(0));
    let err = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let started = Instant::now();

    let monitor = {
        let engine = Arc::clone(&engine);
        let stop = Arc::clone(&stop);
        let ok = Arc::clone(&ok);
        std::thread::spawn(move || {
            let metrics = Arc::clone(engine.metrics());
            let mut last = 0u64;
            println!(
                "{:>6} {:>9} {:>10} {:>8} {:>9} {:>9} {:>8}",
                "t(s)", "ops/s", "total", "batch", "wal_p50", "wal_p99", "gc_n"
            );
            while !stop.load(Ordering::Relaxed) {
                std::thread::sleep(Duration::from_secs(1));
                let now = ok.load(Ordering::Relaxed);
                let hist = metrics.wal_sync_snapshot();
                let p = |q| {
                    hist.percentile_micros(q)
                        .map(fmt_micros)
                        .unwrap_or_else(|| "-".into())
                };
                println!(
                    "{:>6.1} {:>9} {:>10} {:>8.1} {:>9} {:>9} {:>8}",
                    started.elapsed().as_secs_f64(),
                    now.saturating_sub(last),
                    now,
                    metrics.avg_group_batch_size(),
                    p(0.50),
                    p(0.99),
                    metrics.group_commits(),
                );
                last = now;
            }
        })
    };

    let mut handles = Vec::with_capacity(writers);
    for t in 0..writers {
        let engine = Arc::clone(&engine);
        let next_op = Arc::clone(&next_op);
        let ok = Arc::clone(&ok);
        let err = Arc::clone(&err);
        let stop = Arc::clone(&stop);
        handles.push(std::thread::spawn(move || {
            let mut rng = XorShift::new(0x9e37_79b9_7f4a_7c15 ^ (t as u64) << 32);
            let mut value = vec![0u8; value_bytes];
            loop {
                if stop.load(Ordering::Relaxed) {
                    break;
                }
                let op = next_op.fetch_add(1, Ordering::Relaxed);
                if op >= total_ops {
                    break;
                }
                let k = rng.next();
                let key = format!("k-{k:016x}");
                for (i, b) in value.iter_mut().enumerate() {
                    *b = (k as usize).wrapping_add(i) as u8;
                }
                match engine.put(key.into_bytes(), value.clone()) {
                    Ok(()) => {
                        ok.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => {
                        err.fetch_add(1, Ordering::Relaxed);
                        eprintln!("writer {t}: {e}");
                    }
                }
            }
        }));
    }

    let deadline = started + Duration::from_secs(max_secs);
    loop {
        if handles.iter().all(|h| h.is_finished()) {
            break;
        }
        if Instant::now() >= deadline {
            println!("-- wall-clock cap; stopping --");
            stop.store(true, Ordering::Relaxed);
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    for h in handles {
        h.join().expect("writer panic");
    }
    stop.store(true, Ordering::Relaxed);
    monitor.join().unwrap();

    let elapsed = started.elapsed();
    let applied = ok.load(Ordering::Relaxed);
    let errors = err.load(Ordering::Relaxed);
    let metrics = Arc::clone(engine.metrics());
    let hist = metrics.wal_sync_snapshot();

    engine.close().expect("close");

    let ops_per_sec = applied as f64 / elapsed.as_secs_f64();
    let speedup = ops_per_sec / 7300.0;

    println!("== SUMMARY ==");
    println!("wall time           : {:.2}s", elapsed.as_secs_f64());
    println!("puts applied        : {applied}");
    println!("errors              : {errors}");
    println!("sustained ops/sec   : {ops_per_sec:.0}");
    println!("vs Step 8 ceiling   : {speedup:.1}x (7.3k baseline)");
    println!(
        "avg group batch     : {:.1}",
        metrics.avg_group_batch_size()
    );
    println!("group-commit flushes: {}", metrics.group_commits());
    for q in [0.50, 0.90, 0.99] {
        if let Some(us) = hist.percentile_micros(q) {
            println!(
                "wal sync p{:<4}       : {}  (per batch, not per op)",
                q * 100.0,
                fmt_micros(us)
            );
        }
    }
    let verdict = errors == 0 && ops_per_sec > 7300.0;
    println!(
        "verdict             : {}",
        if verdict {
            "SHATTERED the 7.3k ceiling"
        } else if errors == 0 {
            "COMPLETED (did not beat 7.3k — check coalescing)"
        } else {
            "FAILED"
        }
    );
    let _ = std::fs::remove_dir_all(&root);
    if errors > 0 {
        std::process::exit(1);
    }
}
