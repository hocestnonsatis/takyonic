# Reliability Suite

Grammar-guided SQL fuzz, MVCC bank soak, and optional HA failover soak for
Takyonic. Design: [superpowers/specs/2026-07-19-reliability-suite-design.md](superpowers/specs/2026-07-19-reliability-suite-design.md).

## CI smoke

Included in `cargo test --release` (module `reliability`):

| Test | Env knobs | Default |
|------|-----------|---------|
| SQL grammar fuzz | `TAKYONIC_FUZZ_ITERS`, `TAKYONIC_FUZZ_SEED` | 200 iters, seed `1` |
| MVCC soak | `TAKYONIC_SOAK_SECS` | 5 seconds |

Failures print a `ReliabilityReport` with `seed`, `ops`, and `violations`. SQL
fuzz includes a recent-statement transcript on unexpected errors.

## Local long soak

```bash
TAKYONIC_SOAK_SECS=3600 TAKYONIC_FUZZ_ITERS=100000 TAKYONIC_FUZZ_SEED=1 \
  cargo run --release --example reliability_soak
```

## HA soak

```bash
TAKYONIC_HA_SECS=3600 cargo run --release --example ha_soak
```

Short ignored lib smoke (optional):

```bash
cargo test --release --lib reliability::ha_soak::tests::ha_soak_short_ignored -- --ignored
```

## GitHub Actions

`.github/workflows/reliability.yml` runs on `workflow_dispatch` only (not on every
PR). Provide `soak_secs` / `ha_secs` inputs when starting the workflow.

## Interpreting failures

- **SQL fuzz:** unexpected errors (not `Sql` / `Conflict` / `PermissionDenied`) or
  panic. Re-run with the printed `seed`.
- **MVCC soak:** bank sum drift, OCC retry exhaustion, or post-`VACUUM` sum mismatch.
- **HA soak:** failover election failure, split-brain (≠1 leader), or missing keys
  after resurrection.
