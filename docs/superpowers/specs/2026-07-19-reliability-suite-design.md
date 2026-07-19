# Reliability Suite Design

**Date:** 2026-07-19  
**Status:** Approved — implemented  
**Related backlog:** Growth phase item (3) soak/fuzz; precedes remaining SQL completeness (window / UPSERT / deeper correlate)

## Summary

Ship an in-tree **Reliability Suite** that stress-tests the supported SQL surface and MVCC invariants with a grammar-guided fuzzer and concurrent soak harnesses. CI runs a short smoke budget; multi-hour HA soak is manual or `workflow_dispatch` only.

## Goals

- Catch panic, hang, silent corruption, and MVCC visibility bugs on the **currently supported** SQL + DML + SI path.
- Keep PR CI fast: ~30–60s SQL fuzz + ~2 min MVCC soak inside `cargo test --release`.
- Make long runs reproducible via seed + env duration knobs.
- Reuse patterns from existing harnesses (`mvcc_bank`, `raft_chaos`, `crash_recovery`) without introducing `cargo-fuzz` / libFuzzer in v1.

## Non-goals (v1)

- Window functions, `MERGE` / `ON CONFLICT`, `LATERAL`, vector/`<->`, RBAC DDL fuzz.
- libFuzzer / coverage-guided byte fuzzing.
- Backup / PITR / production deploy guides (separate growth items).
- TPC-H/TPC-C competitive benchmarks (separate).
- Putting multi-hour HA soak on every PR.

## Decisions (locked)

| Decision | Choice |
|----------|--------|
| Packaging | Reliability suite: SQL fuzz + MVCC soak primary; HA separate overnight/manual |
| CI gate | Short smoke in `cargo test`; long soak/HA via example + optional `workflow_dispatch` |
| SQL generation | Grammar-guided over supported subset (not mutation / not cargo-fuzz) |
| Implementation shape | In-tree `src/reliability/` + examples; no separate fuzz crate |

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│  ReliabilityReport (seed, ops, violations, exit code)   │
└───────────────────────────┬─────────────────────────────┘
                            │
     ┌──────────────────────┼──────────────────────┐
     ▼                      ▼                      ▼
 SqlGrammarFuzzer     MvccSoakHarness         HaSoakHarness
 (SessionState SQL)   (bank + snapshots)      (3-node failover)
```

### Components

| Component | Responsibility |
|-----------|----------------|
| `SqlGrammarFuzzer` | Deterministic-seed weighted SQL generator over a fixed schema |
| `MvccSoakHarness` | Concurrent writers/readers; bank sum + SI visibility invariants |
| `HaSoakHarness` | 3-node cluster; periodic leader kill/resurrect; ack spot-check |
| `ReliabilityReport` | Stdout summary; non-zero exit on violation |

### Execution modes

| Mode | Entry | Budget |
|------|--------|--------|
| CI smoke | `cargo test --release` (`reliability::*`) | ~45s fuzz + ~2 min MVCC |
| Long SQL+MVCC | `cargo run --release --example reliability_soak` | `TAKYONIC_SOAK_SECS` (hours) |
| Long HA | `cargo run --release --example ha_soak` | `TAKYONIC_HA_SECS` |
| Optional CI | `.github/workflows/reliability.yml` | `workflow_dispatch` only |

## SQL grammar (v1)

### Fixed schema

- `accounts(id PK, balance, owner)`
- `orders(id PK, account_id, amount)`

Generator may create secondary indexes rarely under a superuser session.

### Produced statements

- **DQL:** `SELECT` with projection, `WHERE` (`AND`/`OR`), equi-`JOIN`, `GROUP BY` + aggregates, `HAVING`, `ORDER BY`/`LIMIT`, `WITH` CTEs, uncorrelated and correlated `IN` / `EXISTS` / scalar subqueries.
- **DML:** `INSERT … VALUES`, `UPDATE … SET … WHERE`, `DELETE … WHERE` (PK or simple predicates).
- **Txn:** interleaved `BEGIN` / `COMMIT` / `ROLLBACK`.
- **DDL (rare):** `CREATE INDEX` / `DROP INDEX`.

### Never produced (v1)

`OVER` / window functions, `MERGE`, `ON CONFLICT`, `UPDATE … JOIN`, `LATERAL`, vector distance ops, `CREATE USER` / `GRANT` / `REVOKE`.

### Pass / fail rules

**Pass (expected):**

- Successful execution, or
- `TakyonicError::Sql`, `PermissionDenied`, or `Conflict` (OCC: retry with backoff or continue),
- Process stays alive; worker threads join by deadline.

**Fail (violation):**

- Panic, abort, or hang past wall-clock timeout,
- Unexpected `Internal` / corruption / I/O class errors on the happy path,
- MVCC invariant break (see below),
- Reproducible crash for a printed `(seed, last N statements)` transcript.

## MVCC soak

- **Writers:** transfer-style paired balance updates; occasional insert/delete of accounts/orders.
- **Readers:** concurrent snapshot transactions; `SUM(balance)` and point lookups.
- **Invariants:**
  1. Global balance sum remains constant across committed state (bank invariant).
  2. Snapshot isolation: uncommitted writes are invisible to other txns.
  3. After `VACUUM`, all committed rows that should remain visible are still readable.
- **OCC:** `Conflict` is not a failure; exhausting a retry budget is a failure.

CI default: ~2 minutes wall time, small thread counts (e.g. 4 writers / 2 readers). Long mode scales via env.

## HA soak (v1)

- Bootstrap 3-node Raft + SQL write storm (smart client or direct leader writes).
- Periodically kill leader process; wait for new leader; resume writes; resurrect old leader as follower.
- **Invariants:** exactly one leader when stable; every acknowledged key readable on surviving nodes; zero missing spot-check keys after catch-up.
- **v1.1 (optional, out of v1 required scope):** membership AddNode/RemoveNode churn.

Not run on PR CI.

## File layout

| Path | Role |
|------|------|
| `src/reliability/mod.rs` | Module root, public suite entry, re-exports |
| `src/reliability/sql_fuzzer.rs` | Grammar generator + SQL smoke runner |
| `src/reliability/mvcc_soak.rs` | Bank/soak harness |
| `src/reliability/ha_soak.rs` | Cluster failover soak helpers |
| `src/lib.rs` | `mod reliability;` (test-visible; public API minimal) |
| `examples/reliability_soak.rs` | Long SQL fuzz + MVCC |
| `examples/ha_soak.rs` | Long HA |
| `.github/workflows/reliability.yml` | `workflow_dispatch` long jobs |
| `docs/RELIABILITY.md` | Operator/dev commands, budgets, interpreting failures |
| `CONTRIBUTING.md` | Link to reliability docs + note that smoke is in `cargo test` |

Existing examples (`mvcc_bank`, `raft_chaos`) remain; new suite may call shared helpers but should not break their CLI contracts.

## Configuration

| Env var | Meaning | CI default (approx.) |
|---------|---------|----------------------|
| `TAKYONIC_FUZZ_SEED` | RNG seed (printed on failure) | fixed + one random |
| `TAKYONIC_FUZZ_ITERS` | Statement count / budget | ~200 or ~45s wall |
| `TAKYONIC_SOAK_SECS` | MVCC soak duration | ~120 |
| `TAKYONIC_HA_SECS` | HA soak duration | N/A in PR CI |

## CI integration

- **No new required job** in `.github/workflows/ci.yml` beyond what `cargo test --release` already runs.
- Reliability smoke tests live under `src/reliability/` with hard wall-clock budgets so they cannot balloon CI.
- `.github/workflows/reliability.yml`: `on: workflow_dispatch` with inputs for duration; runs release examples; uploads logs on failure.

## Testing strategy

1. Unit: generator produces only allowed statement shapes; seed reproducibility (`same seed → same SQL stream`).
2. Integration smoke: fuzz loop against `SessionState` on temp engine; no panic.
3. Integration soak: bank invariant under concurrency for CI budget.
4. Manual/dispatch: HA soak matches spirit of `raft_chaos` with longer duration and SQL traffic.

## Success criteria

- [ ] `cargo test --release` includes reliability smoke and stays within existing ~30 min CI job timeout with comfortable margin.
- [ ] Documented commands in `docs/RELIABILITY.md` for local long soak and HA.
- [ ] Failure output always includes seed + recent SQL transcript (fuzz) or last acknowledged op range (soak/HA).
- [ ] No dependency on `cargo-fuzz` / libFuzzer for v1.

## Follow-on (explicitly deferred)

After this suite lands, growth phase continues with:

1. Remaining SQL completeness: window functions, UPSERT/MERGE, deeper correlated subquery (SELECT-list / decorrelation).
2. User-triggered backup/restore & PITR on top of Step 13 snapshots.
3. Production deployment guide + sample Grafana dashboard (Prometheus metrics already exist).
4. TPC-H/TPC-C style competitive benchmarks.

## Open questions (none blocking v1)

- Exact thread counts for CI soak (implementer may tune to stay ~2 min on `ubuntu-latest`).
- Whether HA soak drives traffic via pgwire or Smart Client only (prefer Smart Client for NotLeader retries).
