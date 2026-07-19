//! Long HA failover soak (leader kill / resurrect cycles).
//!
//! ```bash
//! TAKYONIC_HA_SECS=3600 cargo run --release --example ha_soak
//! ```

fn main() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("takyonic=warn")
        .try_init();

    let cfg = takyonic::reliability::ha_soak::HaSoakConfig::from_env();
    println!(
        "== Takyonic HA soak == duration={:?} kill_every={:?}",
        cfg.duration, cfg.kill_every
    );
    let report = takyonic::reliability::ha_soak::run_ha_soak(cfg);
    println!("{}", report.summary());
    if !report.ok() {
        std::process::exit(1);
    }
}
