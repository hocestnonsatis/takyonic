//! Continuous multi-round chaos soak (crash / HA / mobile / reliability).

use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use super::{env_secs, env_u64};

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
pub fn run_command_round(
    name: &'static str,
    program: &str,
    args: &[&str],
    env: &[(&str, String)],
    timeout: Duration,
) -> RoundResult {
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
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                let mut stderr = String::new();
                if let Some(mut err) = child.stderr.take() {
                    let _ = err.read_to_string(&mut stderr);
                }
                let mut stdout = String::new();
                if let Some(mut out) = child.stdout.take() {
                    let _ = out.read_to_string(&mut stdout);
                }
                let ok = status.success();
                let detail = if ok {
                    stdout.chars().take(500).collect()
                } else {
                    format!(
                        "exit={status}; stderr={}",
                        stderr.chars().take(800).collect::<String>()
                    )
                };
                return RoundResult {
                    name,
                    ok,
                    detail,
                    elapsed: started.elapsed(),
                };
            }
            Ok(None) if started.elapsed() >= timeout => {
                let _ = child.kill();
                let _ = child.wait();
                return RoundResult {
                    name,
                    ok: false,
                    detail: format!("timeout after {timeout:?}"),
                    elapsed: started.elapsed(),
                };
            }
            Ok(None) => std::thread::sleep(Duration::from_millis(50)),
            Err(e) => {
                return RoundResult {
                    name,
                    ok: false,
                    detail: format!("try_wait: {e}"),
                    elapsed: started.elapsed(),
                };
            }
        }
    }
}

fn cargo_bin_example(example: &str) -> String {
    // Prefer CARGO_BIN_EXE_* when available; else `cargo run --example`.
    std::env::var(format!(
        "CARGO_BIN_EXE_{}",
        example.replace('-', "_")
    ))
    .unwrap_or_else(|_| "cargo".into())
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn from_env_defaults_are_sane() {
        // SAFETY: unique keys, test-only.
        unsafe {
            std::env::remove_var("TAKYONIC_CONTINUOUS_SECS");
            std::env::remove_var("TAKYONIC_CRASH_ITERS");
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
}
