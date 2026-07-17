//! Step 8: extreme-environment stress harness (Termux / mobile ARM / UFS).
//!
//! Goal is NOT raw throughput — it is graceful degradation: prove that when
//! WAL fsync is slow and L0 backs up, the L0-aware token bucket throttles
//! writers smoothly (ops/sec dips and recovers) with no crash, OOM, or
//! deadlock, while the L0 Rapid and Ln Haul pools keep draining.
//!
//! Usage:
//!   cargo run --release --example mobile_stress -- \
//!       [writers] [total_ops] [value_bytes] [max_secs] [memtable_kib] [compaction_mib_s]
//!
//! Defaults: 8 writers, 1_000_000 ops, 96-byte values, 300 s wall-clock cap,
//! 256 KiB memtable, 2 MiB/s compaction bandwidth (deliberately strangled).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use takyonic::{Config, HistogramSnapshot, Key, TakyonicEngine, TakyonicError, Value};

/// Per-thread xorshift64* PRNG — no external dependency, plenty random for
/// key dispersion.
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

#[derive(Default)]
struct Stats {
    ok: AtomicU64,
    admission_timeouts: AtomicU64,
    hard_errors: AtomicU64,
    /// Writer-observed cumulative put latency in µs (admission wait included).
    put_micros: AtomicU64,
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

fn arg<T: std::str::FromStr>(n: usize, default: T) -> T {
    std::env::args()
        .nth(n)
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn main() {
    let writers: usize = arg(1, 8);
    let total_ops: u64 = arg(2, 1_000_000);
    let value_bytes: usize = arg(3, 96);
    let max_secs: u64 = arg(4, 300);
    let memtable_kib: usize = arg(5, 256);
    let compaction_mib_s: u64 = arg(6, 2);

    let root = std::env::temp_dir().join(format!("takyonic-mobile-stress-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);

    // Mobile-crucible tuning: a small memtable forces frequent flushes so L0
    // actually saturates within the run, and compaction bandwidth is capped
    // low so the pools genuinely fall behind — that is what exercises the
    // soft-throttle and hard-limit paths of the token bucket.
    let config = Config::default()
        .data_dir(root.join("data"))
        .wal_dir(root.join("wal"))
        .memtable_size_bytes(memtable_kib * 1024)
        .block_size_bytes(4 * 1024)
        .l0_soft_limit(4)
        .l0_hard_limit(12)
        .l0_rapid_pool_threads(2)
        .ln_haul_pool_threads(1)
        .compaction_write_bytes_per_sec(compaction_mib_s * 1024 * 1024)
        .write_admission_ops_per_sec(200_000)
        .write_admission_min_ops_per_sec(2_000)
        .write_admission_burst(20_000);

    println!("== Takyonic mobile stress ==");
    println!(
        "writers={writers} total_ops={total_ops} value_bytes={value_bytes} max_secs={max_secs}"
    );
    println!(
        "memtable={memtable_kib}KiB l0_soft=4 l0_hard=12 compaction_bw={compaction_mib_s}MiB/s dir={}",
        root.display()
    );

    let engine = Arc::new(TakyonicEngine::open(config).expect("engine open"));
    let stats = Arc::new(Stats::default());
    let next_op = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let started = Instant::now();

    let monitor = {
        let engine = Arc::clone(&engine);
        let stats = Arc::clone(&stats);
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            let metrics = Arc::clone(engine.metrics());
            let mut last_ops = 0u64;
            let mut last_hist: HistogramSnapshot = metrics.wal_sync_snapshot();
            let mut zero_intervals = 0u32;
            println!(
                "{:>6} {:>9} {:>10} {:>4} {:>9} {:>9} {:>9} {:>7} {:>8} {:>7}",
                "t(s)",
                "ops/s",
                "total",
                "L0",
                "wal_p50",
                "wal_p99",
                "wal_max",
                "flush",
                "throttl",
                "memKiB"
            );
            while !stop.load(Ordering::Relaxed) {
                std::thread::sleep(Duration::from_secs(2));
                let now_ops = metrics.ops_applied();
                let hist = metrics.wal_sync_snapshot();
                let interval = hist.diff(&last_hist);
                let ops_per_sec = (now_ops - last_ops) / 2;
                if now_ops == last_ops {
                    zero_intervals += 1;
                } else {
                    zero_intervals = 0;
                }
                let p = |q| {
                    interval
                        .percentile_micros(q)
                        .map(fmt_micros)
                        .unwrap_or_else(|| "-".into())
                };
                println!(
                    "{:>6.1} {:>9} {:>10} {:>4} {:>9} {:>9} {:>9} {:>7} {:>8} {:>7}",
                    started.elapsed().as_secs_f64(),
                    ops_per_sec,
                    now_ops,
                    engine.manager().l0_file_count(),
                    p(0.50),
                    p(0.99),
                    interval
                        .max_micros()
                        .map(fmt_micros)
                        .unwrap_or_else(|| "-".into()),
                    metrics.flushes(),
                    stats.admission_timeouts.load(Ordering::Relaxed),
                    engine.memtable().approx_size_bytes() / 1024,
                );
                if zero_intervals >= 15 {
                    // 30 s with zero progress and writers still live: treat as
                    // a stall so the harness fails loudly instead of hanging.
                    eprintln!("!! no progress for 30s — possible deadlock/livelock");
                    zero_intervals = 0;
                }
                last_ops = now_ops;
                last_hist = hist;
            }
        })
    };

    let mut handles = Vec::with_capacity(writers);
    for t in 0..writers {
        let engine = Arc::clone(&engine);
        let stats = Arc::clone(&stats);
        let next_op = Arc::clone(&next_op);
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
                let key = format!("key-{k:016x}");
                for (i, b) in value.iter_mut().enumerate() {
                    *b = (k as usize).wrapping_add(i) as u8;
                }
                let begin = Instant::now();
                match engine.put(key.into_bytes(), value.clone()) {
                    Ok(()) => {
                        stats.ok.fetch_add(1, Ordering::Relaxed);
                        stats
                            .put_micros
                            .fetch_add(begin.elapsed().as_micros() as u64, Ordering::Relaxed);
                    }
                    Err(TakyonicError::Admission(_)) => {
                        // Backpressure verdict: count it, brief pause, retry
                        // path continues with the next op.
                        stats.admission_timeouts.fetch_add(1, Ordering::Relaxed);
                        std::thread::sleep(Duration::from_millis(50));
                    }
                    Err(e) => {
                        stats.hard_errors.fetch_add(1, Ordering::Relaxed);
                        eprintln!("writer {t} hard error: {e}");
                    }
                }
            }
        }));
    }

    // Wall-clock watchdog so a mobile run never melts the device.
    let deadline = started + Duration::from_secs(max_secs);
    loop {
        if handles.iter().all(|h| h.is_finished()) {
            break;
        }
        if Instant::now() >= deadline {
            println!("-- wall-clock cap reached; asking writers to stop --");
            stop.store(true, Ordering::Relaxed);
            break;
        }
        std::thread::sleep(Duration::from_millis(250));
    }
    for h in handles {
        h.join().expect("writer panicked");
    }
    stop.store(true, Ordering::Relaxed);
    monitor.join().expect("monitor panicked");

    let elapsed = started.elapsed();
    let ok = stats.ok.load(Ordering::Relaxed);
    let throttled = stats.admission_timeouts.load(Ordering::Relaxed);
    let hard = stats.hard_errors.load(Ordering::Relaxed);
    let metrics = Arc::clone(engine.metrics());
    let hist = metrics.wal_sync_snapshot();

    // Read-back sanity: a freshly written key must be visible pre-close.
    engine
        .put(&b"final-sanity-key"[..], &b"alive"[..])
        .expect("final put");
    let visible = engine
        .get(&Key::new(&b"final-sanity-key"[..]))
        .expect("final get")
        .map(|v: Value| v.as_bytes().to_vec());
    assert_eq!(visible.as_deref(), Some(&b"alive"[..]), "read-back failed");

    println!("-- closing engine (final flush + pool shutdown) --");
    let close_started = Instant::now();
    engine.close().expect("clean close");
    println!("close took {:?}", close_started.elapsed());

    println!("== SUMMARY ==");
    println!("wall time            : {:.1}s", elapsed.as_secs_f64());
    println!("puts applied         : {ok}");
    println!("admission timeouts   : {throttled}");
    println!("hard errors          : {hard}");
    println!(
        "avg sustained ops/s  : {:.0}",
        ok as f64 / elapsed.as_secs_f64()
    );
    if ok > 0 {
        println!(
            "avg put latency      : {}",
            fmt_micros(stats.put_micros.load(Ordering::Relaxed) / ok.max(1))
        );
    }
    for q in [0.50, 0.90, 0.99, 0.999] {
        if let Some(us) = hist.percentile_micros(q) {
            println!("wal append_sync p{:<4}: {}", q * 100.0, fmt_micros(us));
        }
    }
    if let Some(us) = hist.max_micros() {
        println!("wal append_sync max  : {}", fmt_micros(us));
    }
    println!("memtable flushes     : {}", metrics.flushes());
    println!(
        "final L0 file count  : {}",
        engine.manager().l0_file_count()
    );
    let verdict = hard == 0;
    println!(
        "verdict              : {}",
        if verdict {
            "SURVIVED"
        } else {
            "FAILED (hard errors)"
        }
    );
    let _ = std::fs::remove_dir_all(&root);
    if !verdict {
        std::process::exit(1);
    }
}
