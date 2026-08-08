//! Concurrent BPM microbench: hit-heavy and eviction-heavy workloads.
//!
//! Usage:
//!   cargo run --release --example bpm_bench
//!   TAKYONIC_BPM_BENCH_THREADS=16 TAKYONIC_BPM_BENCH_ITERS=50000 cargo run --release --example bpm_bench

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use takyonic::{BufferPoolManager, DEFAULT_LRU_K, DiskManager, DEFAULT_PAGE_SIZE};

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn temp_root() -> std::path::PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = std::env::temp_dir().join(format!("takyonic-bpm-bench-{nanos}"));
    std::fs::create_dir_all(&root).unwrap();
    root
}

fn run_workload(label: &str, pool: usize, pages: usize, threads: usize, iters: u64) {
    let root = temp_root();
    let disk = Arc::new(DiskManager::open(&root, DEFAULT_PAGE_SIZE).unwrap());
    let bpm = BufferPoolManager::new(disk, pool, DEFAULT_LRU_K).unwrap();

    let mut ids = Vec::with_capacity(pages);
    for i in 0..pages {
        let g = bpm.new_page().unwrap();
        g.write(|d| {
            d[0] = (i % 256) as u8;
            d[1] = 0x5A;
        });
        ids.push(g.page_id());
        drop(g);
    }

    let errs = Arc::new(AtomicU64::new(0));
    let barrier = Arc::new(std::sync::Barrier::new(threads));
    let started = Arc::new(std::sync::Barrier::new(threads + 1));
    let mut handles = Vec::with_capacity(threads);

    for t in 0..threads {
        let bpm = Arc::clone(&bpm);
        let ids = ids.clone();
        let errs = Arc::clone(&errs);
        let barrier = Arc::clone(&barrier);
        let started = Arc::clone(&started);
        handles.push(std::thread::spawn(move || {
            barrier.wait();
            started.wait();
            for i in 0..iters {
                let idx = (t.wrapping_mul(31).wrapping_add(i as usize)) % ids.len();
                let id = ids[idx];
                match bpm.fetch_page(id) {
                    Ok(g) => {
                        let (a, b) = g.read(|d| (d[0], d[1]));
                        if b != 0x5A || a != (idx % 256) as u8 {
                            errs.fetch_add(1, Ordering::Relaxed);
                        }
                        drop(g);
                    }
                    Err(_) => {
                        errs.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }));
    }

    started.wait();
    let t0 = Instant::now();
    for h in handles {
        h.join().expect("worker panicked");
    }
    let elapsed = t0.elapsed();
    let total = threads as u64 * iters;
    let ops = total as f64 / elapsed.as_secs_f64();
    let stats = bpm.stats();
    println!(
        "{label}: pool={pool} pages={pages} threads={threads} iters/thread={iters} \
         elapsed_ms={:.1} ops_per_s={:.0} errors={} hits={} misses={} evictions={}",
        elapsed.as_secs_f64() * 1000.0,
        ops,
        errs.load(Ordering::Relaxed),
        stats.hits,
        stats.misses,
        stats.evictions,
    );
    let _ = std::fs::remove_dir_all(root);
}

fn main() {
    let threads = env_u64("TAKYONIC_BPM_BENCH_THREADS", 16) as usize;
    let iters = env_u64("TAKYONIC_BPM_BENCH_ITERS", 40_000);
    println!("== Takyonic BPM concurrent microbench (fixed concurrency path) ==");
    // Hit-heavy: pool fits working set → exercises locked hit path.
    run_workload("hit_heavy", 256, 128, threads, iters);
    // Eviction-heavy: pool << pages → exercises allocate/evict + miss publish.
    run_workload("evict_heavy", 32, 256, threads, iters / 2);
}
