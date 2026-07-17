//! L0-aware token-bucket write admission control.
//!
//! Below the soft limit the bucket refills at the configured normal rate.
//! Between soft and hard limits the rate decreases linearly. At the hard limit,
//! nonblocking admission rejects immediately and blocking admission waits for an
//! L0 catalog change, which compaction signals through [`SstManager`].

use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;

use crate::compaction::SstManager;
use crate::config::Config;
use crate::error::{Result, TakyonicError};

#[derive(Debug)]
struct BucketState {
    tokens: f64,
    last_refill: Instant,
    refill_rate: f64,
}

/// Immediate result from a token-bucket admission attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AdmissionDecision {
    /// Tokens were consumed and the write may proceed.
    Admitted,
    /// The soft-pressure rate has insufficient tokens right now.
    Throttled {
        /// Earliest useful retry interval at the current refill rate.
        retry_after: Duration,
    },
    /// L0 reached the hard limit; no new writes should enter ingestion.
    HardLimit,
}

/// Result from a blocking, deadline-bounded admission attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AdmissionOutcome {
    /// Tokens were acquired before the deadline.
    Acquired,
    /// The deadline elapsed under token or hard-limit pressure.
    TimedOut,
}

/// Concurrent token bucket whose refill rate follows live L0 pressure.
pub struct AdmissionController {
    manager: Arc<SstManager>,
    state: Mutex<BucketState>,
    normal_rate: u64,
    minimum_rate: u64,
    burst: u64,
    soft_limit: usize,
    hard_limit: usize,
}

impl AdmissionController {
    /// Create an admission controller from validated engine configuration.
    pub fn new(manager: Arc<SstManager>, config: &Config) -> Result<Self> {
        config.validate()?;
        let l0_files = manager.l0_file_count();
        let refill_rate = effective_rate_for(
            l0_files,
            config.l0_soft_limit,
            config.l0_hard_limit,
            config.write_admission_ops_per_sec,
            config.write_admission_min_ops_per_sec,
        ) as f64;
        Ok(Self {
            manager,
            state: Mutex::new(BucketState {
                tokens: config.write_admission_burst as f64,
                last_refill: Instant::now(),
                refill_rate,
            }),
            normal_rate: config.write_admission_ops_per_sec,
            minimum_rate: config.write_admission_min_ops_per_sec,
            burst: config.write_admission_burst,
            soft_limit: config.l0_soft_limit,
            hard_limit: config.l0_hard_limit,
        })
    }

    /// Current refill rate derived from the live L0 file count.
    pub fn effective_rate(&self) -> u64 {
        effective_rate_for(
            self.manager.l0_file_count(),
            self.soft_limit,
            self.hard_limit,
            self.normal_rate,
            self.minimum_rate,
        )
    }

    /// Attempt to consume `permits` write-operation tokens without blocking.
    pub fn try_acquire(&self, permits: u64) -> Result<AdmissionDecision> {
        if permits == 0 {
            return Err(TakyonicError::Admission(
                "admission permits must be > 0".into(),
            ));
        }
        if permits > self.burst {
            return Err(TakyonicError::Admission(format!(
                "requested permits {permits} exceed burst capacity {}",
                self.burst
            )));
        }

        let l0_files = self.manager.l0_file_count();
        let new_rate = effective_rate_for(
            l0_files,
            self.soft_limit,
            self.hard_limit,
            self.normal_rate,
            self.minimum_rate,
        );
        let now = Instant::now();
        let mut state = self.state.lock();
        // Never credit elapsed time at a rate higher than either side of an
        // observed pressure transition; that would retroactively over-admit.
        state.refill_rate = state.refill_rate.min(new_rate as f64);
        refill(&mut state, now, self.burst);
        state.refill_rate = new_rate as f64;

        if l0_files >= self.hard_limit {
            // Do not release a stale full-bucket burst immediately after L0
            // recovers; refill gradually at the newly relieved rate.
            state.tokens = 0.0;
            return Ok(AdmissionDecision::HardLimit);
        }
        if state.tokens >= permits as f64 {
            state.tokens -= permits as f64;
            return Ok(AdmissionDecision::Admitted);
        }

        let deficit = permits as f64 - state.tokens;
        let seconds = deficit / state.refill_rate;
        Ok(AdmissionDecision::Throttled {
            retry_after: Duration::from_secs_f64(seconds.max(0.000_001)),
        })
    }

    /// Wait for tokens or compaction relief, bounded by `timeout`.
    ///
    /// Hard-limit waits use the manager's L0 generation condition variable, so
    /// compaction installation wakes writers immediately rather than polling.
    pub fn acquire_timeout(&self, permits: u64, timeout: Duration) -> Result<AdmissionOutcome> {
        let deadline = Instant::now()
            .checked_add(timeout)
            .unwrap_or_else(Instant::now);
        let mut generation = self.manager.l0_generation();
        loop {
            let decision = self.try_acquire(permits)?;
            if decision == AdmissionDecision::Admitted {
                return Ok(AdmissionOutcome::Acquired);
            }

            let now = Instant::now();
            if now >= deadline {
                return Ok(AdmissionOutcome::TimedOut);
            }
            let remaining = deadline - now;
            let wait = match decision {
                AdmissionDecision::Throttled { retry_after } => retry_after.min(remaining),
                AdmissionDecision::HardLimit => remaining,
                AdmissionDecision::Admitted => unreachable!(),
            };
            generation = self.manager.wait_for_l0_change(generation, wait);
        }
    }

    /// Approximate currently accumulated permits, primarily for observability.
    pub fn available_tokens(&self) -> f64 {
        let new_rate = self.effective_rate();
        let now = Instant::now();
        let mut state = self.state.lock();
        state.refill_rate = state.refill_rate.min(new_rate as f64);
        refill(&mut state, now, self.burst);
        state.refill_rate = new_rate as f64;
        state.tokens
    }
}

fn refill(state: &mut BucketState, now: Instant, burst: u64) {
    let elapsed = now.saturating_duration_since(state.last_refill);
    state.tokens = (state.tokens + elapsed.as_secs_f64() * state.refill_rate).min(burst as f64);
    state.last_refill = now;
}

fn effective_rate_for(
    l0_files: usize,
    soft_limit: usize,
    hard_limit: usize,
    normal_rate: u64,
    minimum_rate: u64,
) -> u64 {
    if l0_files >= hard_limit {
        return 0;
    }
    if l0_files <= soft_limit {
        return normal_rate;
    }
    let span = (hard_limit - soft_limit) as u128;
    let remaining = (hard_limit - l0_files) as u128;
    let range = (normal_rate - minimum_rate) as u128;
    minimum_rate + (range * remaining / span) as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compaction::{CompactionEngine, SstMeta};
    use crate::sst::{SstRegistry, SstWriter};
    use crate::types::Entry;
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("takyonic-admission-{name}-{nanos}"));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    fn setup(dir: &Path, config: &Config) -> Arc<SstManager> {
        Arc::new(
            SstManager::new(
                Arc::new(SstRegistry::new()),
                dir,
                config.block_size_bytes,
                4,
                100,
            )
            .unwrap(),
        )
    }

    fn add_l0(manager: &SstManager, dir: &Path, id: u64, key: &'static [u8], seq: u64) {
        let entries = vec![Entry::put(key, &b"value"[..], seq)];
        let path = dir.join(format!("admission-{id}.sst"));
        let info = SstWriter::write(id, path, &entries, 64).unwrap();
        let meta =
            SstMeta::from_info(0, info, entries[0].key.clone(), entries[0].key.clone()).unwrap();
        manager.add_sst(meta).unwrap();
    }

    fn test_config(dir: &Path) -> Config {
        Config::default()
            .data_dir(dir)
            .l0_soft_limit(1)
            .l0_hard_limit(3)
            .write_admission_ops_per_sec(1_000)
            .write_admission_min_ops_per_sec(100)
            .write_admission_burst(2)
            .l0_rapid_pool_threads(1)
            .ln_haul_pool_threads(1)
            .compaction_write_bytes_per_sec(1024 * 1024 * 1024)
    }

    #[test]
    fn normal_rate_and_token_shortage() {
        let dir = temp_dir("normal");
        let config = test_config(&dir);
        let manager = setup(&dir, &config);
        let admission = AdmissionController::new(manager, &config).unwrap();
        assert_eq!(admission.effective_rate(), 1_000);
        assert_eq!(
            admission.try_acquire(2).unwrap(),
            AdmissionDecision::Admitted
        );
        assert!(matches!(
            admission.try_acquire(1).unwrap(),
            AdmissionDecision::Throttled { .. }
        ));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn refill_rate_tracks_soft_and_hard_l0_pressure() {
        let dir = temp_dir("pressure");
        let config = test_config(&dir);
        let manager = setup(&dir, &config);
        let admission = AdmissionController::new(Arc::clone(&manager), &config).unwrap();
        add_l0(&manager, &dir, 1, b"a", 1);
        assert_eq!(admission.effective_rate(), 1_000);
        add_l0(&manager, &dir, 2, b"m", 2);
        assert!(admission.effective_rate() < 1_000);
        assert!(admission.effective_rate() >= 100);
        add_l0(&manager, &dir, 3, b"z", 3);
        assert_eq!(admission.effective_rate(), 0);
        assert_eq!(
            admission.try_acquire(1).unwrap(),
            AdmissionDecision::HardLimit
        );
        drop(admission);
        drop(manager);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn hard_limit_waiter_wakes_after_l0_compaction() {
        let dir = temp_dir("wake");
        let config = test_config(&dir).l0_hard_limit(2);
        let manager = setup(&dir, &config);
        // Overlapping keys force both L0 files into one OCC plan.
        add_l0(&manager, &dir, 10, b"k", 1);
        add_l0(&manager, &dir, 11, b"k", 2);
        let admission = Arc::new(AdmissionController::new(Arc::clone(&manager), &config).unwrap());
        assert_eq!(
            admission.try_acquire(1).unwrap(),
            AdmissionDecision::HardLimit
        );

        let waiting = Arc::clone(&admission);
        let waiter =
            std::thread::spawn(move || waiting.acquire_timeout(1, Duration::from_secs(2)).unwrap());
        let engine = CompactionEngine::new(Arc::clone(&manager), &config).unwrap();
        engine.submit_l0().unwrap().unwrap().wait().unwrap();
        assert_eq!(waiter.join().unwrap(), AdmissionOutcome::Acquired);
        assert_eq!(manager.l0_file_count(), 0);

        drop(engine);
        drop(admission);
        drop(manager);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
