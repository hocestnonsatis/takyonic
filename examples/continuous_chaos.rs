//! Multi-round continuous chaos soak (hours → days via env).
//!
//! ```bash
//! TAKYONIC_CONTINUOUS_SECS=86400 cargo run --release --example continuous_chaos
//! ```

use std::path::PathBuf;

fn main() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("takyonic=warn")
        .try_init();

    let cfg = takyonic::reliability::continuous::ContinuousChaosConfig::from_env();
    let hb = std::env::var("TAKYONIC_HEARTBEAT_PATH")
        .ok()
        .map(PathBuf::from);
    println!(
        "== Takyonic continuous chaos == seed={} duration_secs={}",
        cfg.seed,
        cfg.duration.as_secs()
    );
    let report =
        takyonic::reliability::continuous::run_continuous_chaos(cfg, hb.as_deref());
    println!("{}", report.summary());
    if !report.ok() {
        std::process::exit(1);
    }
}
