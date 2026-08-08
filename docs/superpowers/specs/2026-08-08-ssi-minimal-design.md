# Minimal SSI (Faz 4B)

**Date:** 2026-08-08  
**Status:** Done

## Goal

Accept `SET transaction_isolation TO 'serializable'` and abort the classic
SI write-skew anomaly that Snapshot Isolation + OCC allows.

## Non-goals

- Full Cahill SSI with commit ordering / safe snapshots
- Changing `repeatable read` / `read committed` (still SI+OCC)
- Distributed SSI across 2PC shards (local engine first)

## Mechanism

1. Each SERIALIZABLE txn registers in `SsiRegistry` at begin.
2. Every snapshot `get` adds the key to that txn's SSI read-set.
3. At successful OCC commit of txn T: for every other **active** SSI txn U
   whose read-set intersects T's write-set, mark U **doomed**.
4. A doomed txn fails its next commit with `Conflict` (SSI rw-antidependency).
5. Abort / end unregisters the txn.

Write skew (T1 reads A, T2 reads B, T1 writes B, T2 writes A): the first
committer dooms the second; the second's COMMIT returns an SSI-labelled
conflict. Under `repeatable read`, the same pattern still aborts via OCC
read-set validation (no SSI registry / no "SSI" error text).

## Surfaces

- [`normalize_transaction_isolation`]: allow `serializable`
- [`Transaction`] + [`TakyonicEngine::begin_with_isolation`]
- Session `BEGIN` / auto-commit honor the GUC
