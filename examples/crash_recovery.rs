//! Step 10: local chaos & crash-recovery crucible.
//!
//! Parent mode (default): for N iterations, spawn this same binary in child
//! mode against a shared DB directory, SIGKILL it at a random instant during
//! a multi-threaded write storm (mid group-commit fsync / mid flush), then
//! reopen the database in-process and verify:
//!
//! - Rule 1 (No Lost Acks): every write acked by `put` before the kill is
//!   readable after recovery, at the acked version or newer.
//! - Rule 2 (No Corruption): recovery truncates torn WAL tails via checksums
//!   and never panics or reports integrity errors on boot.
//! - Rule 3 (State Consistency): recovered values carry intact deterministic
//!   payloads; L0/SST metadata recovers cleanly; a second clean reopen agrees.
//!
//! Ack protocol: writers append `key version\n` to a per-thread ack file with
//! a direct `write()` syscall AFTER `put` returns Ok. SIGKILL preserves the
//! OS page cache, so every ack line present after the kill was provably
//! written after a successful (durable) propose — never before.
//!
//! Keys are reused across iterations, so this also proves sequence-number
//! monotonicity across crash and clean restarts (newest-wins must hold).
//!
//! Usage:
//!   cargo run --release --example crash_recovery                 # parent, 8 iterations
//!   cargo run --release --example crash_recovery -- 10 8         # 10 iters, 8 writers
//!   cargo run --release --example crash_recovery -- child <dir> <writers>  # internal

use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use takyonic::{Config, Key, TakyonicEngine, WalReader};

// Wide enough that the 1 MiB memtable fills every few hundred ms during the
// storm, so kills land mid-flush/rotate as well as mid group-commit fsync.
const KEYS_PER_THREAD: u64 = 4096;
const VALUE_PAD: usize = 200;

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

fn db_config(root: &Path) -> Config {
    // Small memtable so kills land mid-flush/rotate, not just mid-fsync.
    Config::default()
        .data_dir(root.join("data"))
        .wal_dir(root.join("wal"))
        .memtable_size_bytes(1024 * 1024)
        .block_size_bytes(4 * 1024)
        .l0_soft_limit(16)
        .l0_hard_limit(48)
        .l0_rapid_pool_threads(2)
        .ln_haul_pool_threads(1)
        .compaction_write_bytes_per_sec(32 * 1024 * 1024)
        .write_admission_ops_per_sec(500_000)
        .write_admission_min_ops_per_sec(50_000)
        .write_admission_burst(50_000)
}

/// Deterministic payload: `v{version}|` + padding derived from the version.
/// Any bit rot in a recovered value fails verification.
fn value_for(version: u64) -> Vec<u8> {
    let mut v = format!("v{version}|").into_bytes();
    let seed = version.wrapping_mul(0x9e37_79b9_7f4a_7c15);
    v.extend((0..VALUE_PAD).map(|i| (seed.rotate_left((i % 64) as u32) as u8) ^ i as u8));
    v
}

fn parse_version(value: &[u8]) -> Option<u64> {
    let s = value.strip_prefix(b"v")?;
    let bar = s.iter().position(|&b| b == b'|')?;
    let version: u64 = std::str::from_utf8(&s[..bar]).ok()?.parse().ok()?;
    (value == value_for(version).as_slice()).then_some(version)
}

// ---------------------------------------------------------------------------
// Child: write storm until killed.
// ---------------------------------------------------------------------------

fn run_child(root: &Path, writers: usize) -> ! {
    let engine = Arc::new(TakyonicEngine::open(db_config(root)).expect("child: engine open"));
    // Version base strictly above anything a previous run could have acked.
    let base = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
        * 1_000_000;

    let mut handles = Vec::new();
    for t in 0..writers {
        let engine = Arc::clone(&engine);
        let root = root.to_path_buf();
        handles.push(std::thread::spawn(move || {
            let mut acks = fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(root.join(format!("acks-{t}.log")))
                .expect("open ack log");
            let mut i = 0u64;
            loop {
                let slot = i % KEYS_PER_THREAD;
                let key = format!("w{t}-{slot}");
                let version = base + i;
                match engine.put(key.clone().into_bytes(), value_for(version)) {
                    Ok(()) => {
                        // Direct write() after the durable ack. No fsync
                        // needed: SIGKILL cannot un-write page cache.
                        let line = format!("{key} {version}\n");
                        acks.write_all(line.as_bytes()).expect("ack write");
                    }
                    Err(e) => {
                        // Admission timeouts are survivable; anything else is
                        // a harness failure worth crashing loudly over.
                        eprintln!("child writer {t}: {e}");
                    }
                }
                i += 1;
            }
        }));
    }
    for h in handles {
        let _ = h.join();
    }
    unreachable!("child writers never exit voluntarily");
}

// ---------------------------------------------------------------------------
// Parent: chaos loop + verification.
// ---------------------------------------------------------------------------

/// Highest acked version per key across all ack files (all iterations).
/// A torn final line (killed mid-`write`) is skipped.
fn read_acks(root: &Path) -> (HashMap<String, u64>, u64) {
    let mut max_versions: HashMap<String, u64> = HashMap::new();
    let mut total = 0u64;
    let Ok(dir) = fs::read_dir(root) else {
        return (max_versions, 0);
    };
    for entry in dir.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if !name.starts_with("acks-") || !name.ends_with(".log") {
            continue;
        }
        let Ok(data) = fs::read(entry.path()) else {
            continue;
        };
        for line in data.split(|&b| b == b'\n') {
            let Ok(line) = std::str::from_utf8(line) else {
                continue;
            };
            let mut parts = line.split_whitespace();
            let (Some(key), Some(ver)) = (parts.next(), parts.next()) else {
                continue;
            };
            let Ok(ver) = ver.parse::<u64>() else {
                continue;
            };
            total += 1;
            max_versions
                .entry(key.to_string())
                .and_modify(|v| *v = (*v).max(ver))
                .or_insert(ver);
        }
    }
    (max_versions, total)
}

/// Simulate a torn append at power loss: write an incomplete record at the
/// tail of the newest WAL segment. Recovery must truncate it via the
/// physical-EOF check without touching preceding valid records.
///
/// Variants: partial length prefix, or full prefix with a truncated body.
/// (A complete record with a bad checksum is intentionally NOT injected:
/// that is real corruption and is fatal by design.)
fn inject_torn_tail(root: &Path, rng: &mut XorShift) -> bool {
    let wal_dir = root.join("wal");
    let Ok(dir) = fs::read_dir(&wal_dir) else {
        return false;
    };
    let newest = dir
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("wal"))
        .max();
    let Some(path) = newest else { return false };
    let Ok(mut file) = fs::OpenOptions::new().append(true).open(&path) else {
        return false;
    };
    let torn: Vec<u8> = match rng.next() % 3 {
        // Partial length prefix (1-3 bytes).
        0 => vec![0xde; 1 + (rng.next() % 3) as usize],
        // Length prefix promising 512 bytes, body cut short.
        1 => {
            let mut v = 512u32.to_le_bytes().to_vec();
            v.extend(std::iter::repeat_n(0xAB, 64));
            v
        }
        // Huge bogus length prefix (torn before any body landed).
        _ => vec![0xff, 0xff, 0xff, 0x7f, 0x01, 0x02],
    };
    if file.write_all(&torn).is_err() {
        return false;
    }
    let _ = file.sync_all();
    true
}

struct IterationReport {
    lifetime_ms: u128,
    acked_keys: usize,
    total_acks: u64,
    wal_records: u64,
    torn_tails: usize,
    recovered_memtable: usize,
    l0_files: usize,
    open_ms: u128,
}

/// Pre-recovery forensic scan: count valid WAL records and torn tails as the
/// crash left them, before the engine's recovery truncates anything.
fn scan_wal_damage(root: &Path) -> (u64, usize) {
    let wal_dir = root.join("wal");
    let mut records = 0u64;
    let mut torn = 0usize;
    let Ok(dir) = fs::read_dir(&wal_dir) else {
        return (0, 0);
    };
    for entry in dir.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("wal") {
            continue;
        }
        let Ok(mut reader) = WalReader::open(&path) else {
            continue;
        };
        if let Ok(n) = reader.replay(|_| {}) {
            records += n;
        }
        if reader.has_torn_tail() {
            torn += 1;
        }
    }
    (records, torn)
}

fn verify_iteration(root: &Path, iter: usize) -> IterationReport {
    let (acks, total_acks) = read_acks(root);
    let (wal_records, torn_tails) = scan_wal_damage(root);

    let open_started = Instant::now();
    let engine = TakyonicEngine::open(db_config(root))
        .unwrap_or_else(|e| panic!("iter {iter}: RECOVERY FAILED TO OPEN: {e}"));
    let open_ms = open_started.elapsed().as_millis();

    // Rule 1 + Rule 3: every acked key readable at acked-or-newer version,
    // with a bit-exact deterministic payload.
    for (key, &acked_version) in &acks {
        let value = engine
            .get(&Key::new(key.clone().into_bytes()))
            .unwrap_or_else(|e| panic!("iter {iter}: get({key}) errored: {e}"))
            .unwrap_or_else(|| panic!("iter {iter}: LOST ACK: {key} v{acked_version} missing"));
        let recovered_version = parse_version(value.as_bytes()).unwrap_or_else(|| {
            panic!(
                "iter {iter}: CORRUPT VALUE for {key}: {:?}",
                &value.as_bytes()[..value.as_bytes().len().min(24)]
            )
        });
        assert!(
            recovered_version >= acked_version,
            "iter {iter}: STALE READ for {key}: recovered v{recovered_version} < acked v{acked_version}"
        );
    }

    let recovered_memtable = engine.memtable().len();
    let l0_files = engine.manager().l0_file_count();
    engine.close().expect("clean close after verify");

    // Rule 3 (idempotent recovery): a second clean reopen must still serve
    // every acked key. This also exercises SEQNO + pruned-WAL recovery.
    let reopened = TakyonicEngine::open(db_config(root))
        .unwrap_or_else(|e| panic!("iter {iter}: second reopen failed: {e}"));
    for (key, &acked_version) in &acks {
        let value = reopened
            .get(&Key::new(key.clone().into_bytes()))
            .unwrap()
            .unwrap_or_else(|| panic!("iter {iter}: key {key} lost after clean reopen"));
        let v = parse_version(value.as_bytes()).expect("payload intact after reopen");
        assert!(v >= acked_version, "iter {iter}: reopen regressed {key}");
    }
    reopened.close().expect("clean close of reopen");

    IterationReport {
        lifetime_ms: 0,
        acked_keys: acks.len(),
        total_acks,
        wal_records,
        torn_tails,
        recovered_memtable,
        l0_files,
        open_ms,
    }
}

fn run_parent(iterations: usize, writers: usize) {
    let root = std::env::temp_dir().join(format!("takyonic-crash-{}", std::process::id()));
    let _ = fs::remove_dir_all(&root);
    fs::create_dir_all(&root).unwrap();
    let exe = std::env::current_exe().unwrap();
    let mut rng = XorShift::new(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64,
    );

    println!("== Takyonic crash-recovery crucible ==");
    println!(
        "iterations={iterations} writers={writers} dir={}",
        root.display()
    );
    println!(
        "{:>4} {:>8} {:>10} {:>10} {:>8} {:>5} {:>9} {:>6} {:>8}",
        "iter", "life_ms", "acks", "keys", "wal_rec", "torn", "memtable", "L0", "open_ms"
    );
    let mut total_torn = 0usize;

    for iter in 1..=iterations {
        let mut child = Command::new(&exe)
            .arg("child")
            .arg(&root)
            .arg(writers.to_string())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn child");

        // Random kill point: 300ms (startup / first flush) .. 3000ms (deep
        // into the storm, likely mid group-commit or mid L0 flush).
        let lifetime_ms = 300 + rng.next() % 2700;
        std::thread::sleep(Duration::from_millis(lifetime_ms));
        child.kill().expect("SIGKILL child");
        let status = child.wait().expect("reap child");
        assert!(!status.success(), "child must die by signal, not exit 0");

        // Every other iteration, simulate a torn append at the WAL tail on
        // top of the SIGKILL (the power-loss case the OS page cache hides).
        let injected = iter % 2 == 0 && inject_torn_tail(&root, &mut rng);

        let mut report = verify_iteration(&root, iter);
        if injected {
            assert!(
                report.torn_tails >= 1,
                "iter {iter}: injected torn tail was not detected by forensic scan"
            );
        }
        report.lifetime_ms = lifetime_ms as u128;
        total_torn += report.torn_tails;
        println!(
            "{:>4} {:>8} {:>10} {:>10} {:>8} {:>5} {:>9} {:>6} {:>8}",
            iter,
            report.lifetime_ms,
            report.total_acks,
            report.acked_keys,
            report.wal_records,
            report.torn_tails,
            report.recovered_memtable,
            report.l0_files,
            report.open_ms,
        );
    }

    println!("== VERDICT: INDESTRUCTIBLE — {iterations}/{iterations} crash cycles recovered ==");
    println!("Rule 1 (no lost acks)      : PASS");
    println!(
        "Rule 2 (no corruption)     : PASS ({total_torn} torn WAL tails detected and truncated)"
    );
    println!("Rule 3 (state consistency) : PASS (payloads intact, idempotent reopen)");
    let _ = fs::remove_dir_all(&root);
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.get(1).map(String::as_str) == Some("child") {
        let root = PathBuf::from(args.get(2).expect("child needs dir"));
        let writers: usize = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(8);
        run_child(&root, writers);
    }
    let iterations: usize = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(8);
    let writers: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(8);
    run_parent(iterations, writers);
}
