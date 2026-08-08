//! Continuous multi-round chaos soak (crash / HA / mobile / reliability).

use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use super::{ReliabilityReport, env_secs, env_u64};

/// Knob set for [`run_continuous_chaos`] / CI smoke.
#[derive(Clone, Debug)]
pub struct ContinuousChaosConfig {
    /// Seed printed on failure (forwarded to child examples when supported).
    pub seed: u64,
    /// Wall-clock budget for the whole continuous run.
    pub duration: Duration,
    /// `crash_recovery` parent iterations per crash round.
    pub crash_iters_per_round: u64,
    /// Writer threads for crash / mobile children.
    pub writers: u64,
    /// Run `mobile_stress` rounds.
    pub include_mobile: bool,
    /// Run `ha_soak` rounds.
    pub include_ha: bool,
    /// Run `reliability_soak` rounds.
    pub include_reliability: bool,
}

impl ContinuousChaosConfig {
    /// Read knobs from env (see `docs/RELIABILITY.md`).
    pub fn from_env() -> Self {
        Self {
            seed: env_u64("TAKYONIC_FUZZ_SEED", 1),
            duration: env_secs("TAKYONIC_CONTINUOUS_SECS", 120),
            crash_iters_per_round: env_u64("TAKYONIC_CRASH_ITERS", 4),
            writers: env_u64("TAKYONIC_CHAOS_WRITERS", 4),
            include_mobile: env_u64("TAKYONIC_INCLUDE_MOBILE", 1) != 0,
            include_ha: env_u64("TAKYONIC_INCLUDE_HA", 1) != 0,
            include_reliability: env_u64("TAKYONIC_INCLUDE_RELIABILITY", 1) != 0,
        }
    }
}

/// One completed chaos round.
#[derive(Clone, Debug)]
pub struct RoundResult {
    /// Round label (`crash_recovery`, `raft_chaos`, …).
    pub name: &'static str,
    /// Child exit success / harness ok.
    pub ok: bool,
    /// stdout/stderr summary snippet.
    pub detail: String,
    /// Wall time for the round.
    pub elapsed: Duration,
}

/// Append a timestamped heartbeat line (creates parent dirs if needed).
pub fn append_heartbeat(path: &Path, line: &str) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut f = OpenOptions::new().create(true).append(true).open(path)?;
    writeln!(f, "{line}")?;
    Ok(())
}

/// Spawn `program` with `args`; kill on `timeout`. Captures stderr on failure.
///
/// stdout/stderr are drained on background threads so a chatty child (e.g.
/// `raft_chaos` with tracing) cannot deadlock on a full OS pipe buffer.
pub fn run_command_round(
    name: &'static str,
    program: &str,
    args: &[&str],
    env: &[(&str, String)],
    timeout: Duration,
) -> RoundResult {
    use std::sync::{Arc, Mutex};
    use std::thread;

    let started = Instant::now();
    let mut cmd = Command::new(program);
    cmd.args(args).stdout(Stdio::piped()).stderr(Stdio::piped());
    for (k, v) in env {
        cmd.env(k, v);
    }
    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => {
            return RoundResult {
                name,
                ok: false,
                detail: format!("spawn {program}: {e}"),
                elapsed: started.elapsed(),
            };
        }
    };

    let stdout_buf = Arc::new(Mutex::new(Vec::new()));
    let stderr_buf = Arc::new(Mutex::new(Vec::new()));
    let stdout_pipe = child.stdout.take();
    let stderr_pipe = child.stderr.take();
    let so = Arc::clone(&stdout_buf);
    let se = Arc::clone(&stderr_buf);
    let t_out = thread::spawn(move || {
        if let Some(mut r) = stdout_pipe {
            let _ = r.read_to_end(&mut *so.lock().unwrap());
        }
    });
    let t_err = thread::spawn(move || {
        if let Some(mut r) = stderr_pipe {
            let _ = r.read_to_end(&mut *se.lock().unwrap());
        }
    });

    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) if started.elapsed() >= timeout => {
                let _ = child.kill();
                let _ = child.wait();
                let _ = t_out.join();
                let _ = t_err.join();
                return RoundResult {
                    name,
                    ok: false,
                    detail: format!("timeout after {timeout:?}"),
                    elapsed: started.elapsed(),
                };
            }
            Ok(None) => std::thread::sleep(Duration::from_millis(50)),
            Err(e) => {
                let _ = child.kill();
                let _ = t_out.join();
                let _ = t_err.join();
                return RoundResult {
                    name,
                    ok: false,
                    detail: format!("try_wait: {e}"),
                    elapsed: started.elapsed(),
                };
            }
        }
    };

    let _ = t_out.join();
    let _ = t_err.join();
    let stdout = String::from_utf8_lossy(&stdout_buf.lock().unwrap()).into_owned();
    let stderr = String::from_utf8_lossy(&stderr_buf.lock().unwrap()).into_owned();
    let ok = status.success();
    let detail = if ok {
        stdout.chars().take(500).collect()
    } else {
        format!(
            "exit={status}; stderr={}",
            stderr.chars().take(800).collect::<String>()
        )
    };
    RoundResult {
        name,
        ok,
        detail,
        elapsed: started.elapsed(),
    }
}

fn cargo_bin_example(example: &str) -> String {
    // Prefer CARGO_BIN_EXE_* when available (integration tests / harnesses).
    if let Ok(path) = std::env::var(format!(
        "CARGO_BIN_EXE_{}",
        example.replace('-', "_")
    )) {
        return path;
    }
    // Next: already-built release example next to this process / under CARGO_TARGET_DIR.
    let candidates = [
        format!("target/release/examples/{example}"),
        format!(
            "{}/examples/{example}",
            std::env::var("CARGO_TARGET_DIR").unwrap_or_else(|_| "target".into())
        ),
    ];
    for c in candidates {
        let p = std::path::Path::new(&c);
        if p.is_file() {
            if let Ok(abs) = std::fs::canonicalize(p) {
                return abs.display().to_string();
            }
            return c;
        }
    }
    // Last resort: `cargo run --example` (slow; can contend on the package lock).
    "cargo".into()
}

/// Run `crash_recovery` parent for `cfg.crash_iters_per_round` iterations.
pub fn round_crash_recovery(cfg: &ContinuousChaosConfig, timeout: Duration) -> RoundResult {
    let iters = cfg.crash_iters_per_round.to_string();
    let writers = cfg.writers.to_string();
    let bin = cargo_bin_example("crash_recovery");
    if bin == "cargo" {
        run_command_round(
            "crash_recovery",
            "cargo",
            &[
                "run",
                "--release",
                "--example",
                "crash_recovery",
                "--",
                &iters,
                &writers,
            ],
            &[],
            timeout,
        )
    } else {
        run_command_round("crash_recovery", &bin, &[&iters, &writers], &[], timeout)
    }
}

/// Run `raft_chaos` example (fixed short scripted chaos).
pub fn round_raft_chaos(_cfg: &ContinuousChaosConfig, timeout: Duration) -> RoundResult {
    let bin = cargo_bin_example("raft_chaos");
    if bin == "cargo" {
        run_command_round(
            "raft_chaos",
            "cargo",
            &["run", "--release", "--example", "raft_chaos"],
            &[],
            timeout,
        )
    } else {
        run_command_round("raft_chaos", &bin, &[], &[], timeout)
    }
}

/// Run `mobile_stress` example.
pub fn round_mobile_stress(_cfg: &ContinuousChaosConfig, timeout: Duration) -> RoundResult {
    let bin = cargo_bin_example("mobile_stress");
    if bin == "cargo" {
        run_command_round(
            "mobile_stress",
            "cargo",
            &["run", "--release", "--example", "mobile_stress"],
            &[],
            timeout,
        )
    } else {
        run_command_round("mobile_stress", &bin, &[], &[], timeout)
    }
}

/// Run a short `ha_soak` round.
pub fn round_ha_soak(cfg: &ContinuousChaosConfig, timeout: Duration) -> RoundResult {
    let env = [
        ("TAKYONIC_HA_SECS", "30".to_string()),
        ("TAKYONIC_FUZZ_SEED", cfg.seed.to_string()),
    ];
    let bin = cargo_bin_example("ha_soak");
    if bin == "cargo" {
        run_command_round(
            "ha_soak",
            "cargo",
            &["run", "--release", "--example", "ha_soak"],
            &env,
            timeout,
        )
    } else {
        run_command_round("ha_soak", &bin, &[], &env, timeout)
    }
}

/// Run a short `reliability_soak` round.
pub fn round_reliability_soak(cfg: &ContinuousChaosConfig, timeout: Duration) -> RoundResult {
    let env = [
        ("TAKYONIC_SOAK_SECS", "30".to_string()),
        ("TAKYONIC_FUZZ_ITERS", "200".to_string()),
        ("TAKYONIC_FUZZ_SEED", cfg.seed.to_string()),
    ];
    let bin = cargo_bin_example("reliability_soak");
    if bin == "cargo" {
        run_command_round(
            "reliability_soak",
            "cargo",
            &["run", "--release", "--example", "reliability_soak"],
            &env,
            timeout,
        )
    } else {
        run_command_round("reliability_soak", &bin, &[], &env, timeout)
    }
}

fn dry_run_round(name: &'static str) -> RoundResult {
    RoundResult {
        name,
        ok: true,
        detail: "dry-run".into(),
        elapsed: Duration::from_millis(1),
    }
}

/// Run chaos rounds until `cfg.duration` elapses; stop early on first failure.
pub fn run_continuous_chaos(
    cfg: ContinuousChaosConfig,
    heartbeat: Option<&Path>,
) -> ReliabilityReport {
    let mut report = ReliabilityReport::new(cfg.seed);
    let deadline = Instant::now() + cfg.duration;
    let dry = env_u64("TAKYONIC_CONTINUOUS_DRY_RUN", 0) != 0;
    let mut round_idx: u64 = 0;

    while Instant::now() < deadline {
        round_idx += 1;
        let remaining = deadline.saturating_duration_since(Instant::now());
        // Don't start a real schedule if leftover budget cannot finish crash_recovery.
        // Dry-run rounds are instantaneous, so allow any positive remaining there.
        if !dry && remaining < Duration::from_secs(45) {
            break;
        }
        if remaining.is_zero() {
            break;
        }
        // Per-round timeout = min(remaining, 30m) so one hung child cannot eat the budget.
        let per = remaining.min(Duration::from_secs(30 * 60));

        let schedule: Vec<RoundResult> = if dry {
            vec![
                dry_run_round("crash_recovery"),
                dry_run_round("raft_chaos"),
            ]
        } else {
            let mut v = vec![
                round_crash_recovery(&cfg, per),
                round_raft_chaos(&cfg, per),
            ];
            if cfg.include_mobile {
                v.push(round_mobile_stress(&cfg, per));
            }
            if cfg.include_reliability {
                v.push(round_reliability_soak(&cfg, per));
            }
            if cfg.include_ha {
                v.push(round_ha_soak(&cfg, per));
            }
            v
        };

        for r in schedule {
            report.record_op();
            let line = format!(
                "ts_ns={} round={} name={} ok={} elapsed_ms={} detail={}",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_nanos())
                    .unwrap_or(0),
                round_idx,
                r.name,
                r.ok,
                r.elapsed.as_millis(),
                r.detail.replace('\n', " "),
            );
            if let Some(path) = heartbeat {
                let _ = append_heartbeat(path, &line);
            }
            if !r.ok {
                report.fail(format!("{} failed: {}", r.name, r.detail));
                return report;
            }
        }
    }
    report
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use std::time::{SystemTime, UNIX_EPOCH};

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn lock_env() -> std::sync::MutexGuard<'static, ()> {
        ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner())
    }

    #[test]
    fn from_env_defaults_are_sane() {
        let _g = lock_env();
        // SAFETY: unique keys, test-only, serialized via ENV_LOCK.
        unsafe {
            std::env::remove_var("TAKYONIC_CONTINUOUS_SECS");
            std::env::remove_var("TAKYONIC_CRASH_ITERS");
            std::env::remove_var("TAKYONIC_CONTINUOUS_DRY_RUN");
            std::env::remove_var("TAKYONIC_INCLUDE_MOBILE");
        }
        let c = ContinuousChaosConfig::from_env();
        assert_eq!(c.duration, Duration::from_secs(120));
        assert_eq!(c.crash_iters_per_round, 4);
        assert!(c.include_mobile);
    }

    #[test]
    fn append_heartbeat_writes_line() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("takyonic-hb-{nanos}.log"));
        append_heartbeat(&path, "round=1 ok=true").unwrap();
        let s = std::fs::read_to_string(&path).unwrap();
        assert!(s.contains("round=1 ok=true"), "{s}");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn run_external_command_round_reports_exit() {
        let ok = run_command_round("true_cmd", "/bin/true", &[], &[], Duration::from_secs(5));
        assert!(ok.ok, "{:?}", ok.detail);
        let bad = run_command_round("false_cmd", "/bin/false", &[], &[], Duration::from_secs(5));
        assert!(!bad.ok, "false must fail");
    }

    #[test]
    fn continuous_chaos_smoke_completes_tiny_budget() {
        let _g = lock_env();
        // SAFETY: test-only env isolation, serialized via ENV_LOCK.
        unsafe {
            std::env::set_var("TAKYONIC_CONTINUOUS_DRY_RUN", "1");
            std::env::set_var("TAKYONIC_CONTINUOUS_SECS", "2");
            std::env::set_var("TAKYONIC_INCLUDE_MOBILE", "0");
            std::env::set_var("TAKYONIC_INCLUDE_HA", "0");
            std::env::set_var("TAKYONIC_INCLUDE_RELIABILITY", "0");
        }
        let cfg = ContinuousChaosConfig::from_env();
        let report = run_continuous_chaos(cfg, None);
        unsafe {
            std::env::remove_var("TAKYONIC_CONTINUOUS_DRY_RUN");
            std::env::remove_var("TAKYONIC_CONTINUOUS_SECS");
            std::env::remove_var("TAKYONIC_INCLUDE_MOBILE");
            std::env::remove_var("TAKYONIC_INCLUDE_HA");
            std::env::remove_var("TAKYONIC_INCLUDE_RELIABILITY");
        }
        assert!(report.ok(), "{}", report.summary());
        assert!(report.ops >= 1, "expected at least one dry-run round");
    }
}
