//! Reliability suite: SQL grammar fuzz, MVCC soak, HA soak, W–AA chaos.
pub mod continuous;
pub mod ha_soak;
pub mod mvcc_soak;
pub mod sql_fuzzer;
pub mod waa_chaos;

#[cfg(test)]
pub mod props;

use std::time::Duration;

/// Aggregate outcome of a reliability run (fuzz / soak / HA).
#[derive(Debug, Default)]
pub struct ReliabilityReport {
    /// RNG / harness seed for reproduction.
    pub seed: u64,
    /// Successful or expected-error operations counted.
    pub ops: u64,
    /// Invariant or unexpected-error messages; empty means pass.
    pub violations: Vec<String>,
}

impl ReliabilityReport {
    /// Create an empty passing report for `seed`.
    pub fn new(seed: u64) -> Self {
        Self {
            seed,
            ops: 0,
            violations: Vec::new(),
        }
    }

    /// True when no violations were recorded.
    pub fn ok(&self) -> bool {
        self.violations.is_empty()
    }

    /// Record a failure message.
    pub fn fail(&mut self, msg: impl Into<String>) {
        self.violations.push(msg.into());
    }

    /// Increment the operation counter.
    pub fn record_op(&mut self) {
        self.ops = self.ops.saturating_add(1);
    }

    /// Human-readable one-line summary (includes seed and violations).
    pub fn summary(&self) -> String {
        format!(
            "ReliabilityReport seed={} ops={} ok={} violations={:?}",
            self.seed,
            self.ops,
            self.ok(),
            self.violations
        )
    }
}

/// Parse `name` as `u64`, or return `default` if unset/invalid.
pub fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

/// Parse `name` as seconds into a [`Duration`], or `default` seconds.
pub fn env_secs(name: &str, default: u64) -> Duration {
    Duration::from_secs(env_u64(name, default))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn report_ok_until_violation() {
        let mut r = ReliabilityReport::new(42);
        r.record_op();
        assert!(r.ok());
        assert_eq!(r.ops, 1);
        r.fail("boom");
        assert!(!r.ok());
        let s = r.summary();
        assert!(s.contains("seed=42"), "{s}");
        assert!(s.contains("boom"), "{s}");
    }

    #[test]
    fn env_u64_reads_or_default() {
        // SAFETY: test-only isolation for a unique env key.
        unsafe {
            std::env::remove_var("TAKYONIC_TEST_ENV_U64");
        }
        assert_eq!(env_u64("TAKYONIC_TEST_ENV_U64", 7), 7);
        unsafe {
            std::env::set_var("TAKYONIC_TEST_ENV_U64", "99");
        }
        assert_eq!(env_u64("TAKYONIC_TEST_ENV_U64", 7), 99);
        unsafe {
            std::env::remove_var("TAKYONIC_TEST_ENV_U64");
        }
    }
}
