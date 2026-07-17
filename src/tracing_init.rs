//! Tracing bootstrap helpers.
//!
//! Production binaries should call [`init_tracing`] once at process start.
//! The subscriber is only linked from tests / examples via `tracing-subscriber`
//! in `[dev-dependencies]`; the helper itself is gated so the library does not
//! force a subscriber on embedders.

/// Initialize a default `tracing` subscriber from `RUST_LOG` (falls back to `info`).
///
/// Safe to call multiple times: subsequent calls are no-ops if a global
/// subscriber is already set.
///
/// Available when building tests/examples (dev-dependency) or with the
/// `tracing-init` feature.
#[cfg(any(test, feature = "tracing-init"))]
pub fn init_tracing() {
    use tracing_subscriber::EnvFilter;

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    // Ignore AlreadyExists — common when tests share a process.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .try_init();
}

/// No-op stub when the init feature is not enabled (library consumers).
#[cfg(not(any(test, feature = "tracing-init")))]
pub fn init_tracing() {
    // Embedders install their own subscriber; library code only emits spans/events.
}
