# Design: Embed 2PC into the Real Write Path

**Date:** 2026-07-20 (status refresh 2026-08-03)  
**Status:** Phases 1A–1B + Client B3–B4 + Session B5 + durable TC log (1D) **DONE**.

## Goal

Cross-shard `COMMIT` uses `TransactionCoordinator` → real Raft `TxnPrepare` / `TxnCommit` / `TxnAbort` on Engine-backed shards, reachable from SQL/Client without new SQL syntax.

## Decisions (unchanged)

1. **API surface:** `put`/`get`/single-shard `Transaction::commit` unchanged. Cross-shard only at **COMMIT**.
2. **Trigger:** Partition write-set via `PartitionRouter`. 1 shard → OCC `txn_batch`; ≥2 → `TransactionCoordinator`.
3. **Participant:** `EngineShard` (+ `RemoteShard` over `TwopcService` on `serve_node`).
4. **Prepared state:** Engine/Raft durable prepared map (done for Engine path).
5. **TC placement:** Leader/session node runs TC; peers via `RemoteShard`.

## Call path (target)

```
SessionState::run_plan(Commit)
  → partition_txn_branches(writes, reads, catalog, router)
  → TransactionCoordinator::execute(DistTxnRequest)
  → EngineShard|RemoteShard::prepare
  → engine.twopc_prepare → RaftCommand::TxnPrepare
```

## Remaining gaps (2026-08-07)

| Gap | Notes |
|-----|--------|
| Durable TC decisions on disk | **DONE** — `data_dir/TC_DECISIONS` + `TransactionCoordinator::open`; Session uses `engine.txn_coordinator()` |
| Session SQL COMMIT | **DONE** (B5) — partition + shared TC |
| Multipart S3 | Non-goal; PutObject ≥5 GiB refused |

## Non-goals

- New SQL syntax for distributed txns
- Changing single-shard OCC semantics
