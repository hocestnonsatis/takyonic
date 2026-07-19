//! Continuous multi-round chaos soak (crash / HA / mobile / reliability).

use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;
use std::time::Duration;

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
}
