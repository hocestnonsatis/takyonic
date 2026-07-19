//! Long SQL fuzz + MVCC soak entrypoint.
//!
//! ```bash
//! TAKYONIC_SOAK_SECS=3600 TAKYONIC_FUZZ_ITERS=100000 \
//!   cargo run --release --example reliability_soak
//! ```

fn main() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("takyonic=warn")
        .try_init();

    let seed = takyonic::reliability::env_u64("TAKYONIC_FUZZ_SEED", 1);
    let iters = takyonic::reliability::env_u64("TAKYONIC_FUZZ_ITERS", 10_000);
    println!("== Takyonic reliability soak == seed={seed} fuzz_iters={iters}");

    let fuzz = takyonic::reliability::sql_fuzzer::run_sql_fuzz(seed, iters);
    println!("fuzz: {}", fuzz.summary());
    if !fuzz.ok() {
        std::process::exit(1);
    }

    let soak = takyonic::reliability::mvcc_soak::run_mvcc_soak(
        takyonic::reliability::mvcc_soak::MvccSoakConfig {
            seed,
            writers: 8,
            readers: 4,
            duration: takyonic::reliability::env_secs("TAKYONIC_SOAK_SECS", 600),
            accounts: 64,
        },
    );
    println!("soak: {}", soak.summary());
    if !soak.ok() {
        std::process::exit(1);
    }
}
