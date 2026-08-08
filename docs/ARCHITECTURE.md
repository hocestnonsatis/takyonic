# Takyonic Architecture

Takyonic is a shared-nothing NewSQL database whose storage, transaction, query,
and consensus layers are implemented in Rust. Every voting node owns an LSM
state machine; Raft determines the single ordered command stream applied to
those replicas.

## System overview

```text
psql / Smart Client
        |
        v
PostgreSQL wire / gRPC
        |
        v
SQL AST -> Logical Plan -> CBO
        |
        v
Raft Leader -> grouped durable Raft log -> quorum
        |
        v
MVCC state-machine apply -> Memtable -> SSTables
```

The PostgreSQL endpoint and Smart Client share the same Raft / MVCC apply path
beneath the protocol boundary. Followers reject mutations with a leader hint,
allowing the Smart Client to rediscover the leader and retry without embedding
topology logic in the application.

## SQL surfaces (Smart Client vs pgwire)

| Path | Role |
|------|------|
| **pgwire / `SessionState`** | Full local Volcano SQL: JOIN, aggregates, `ORDER BY`/`LIMIT`, session `BEGIN`/`COMMIT`, DDL, CTE/subquery, `UNION`, GUCs (`search_path`, `transaction_isolation`, `TimeZone`), … |
| **Smart Client `execute_session_sql`** | Same Volcano path on the Raft leader (ephemeral `SessionState` per RPC) |
| **Smart Client `execute_sql`** | Narrow RPC façade: `INSERT`; single-table filtered `SELECT`; `UPDATE`/`DELETE` with `pk = literal` |
| **Smart Client `execute_txn` / `execute_dist_txn`** | Programmatic OCC / 2PC (not SQL) |

Unsupported narrow `execute_sql` plans fail with a stable **`pgwire only`** error
(`takyonic::client::PGWIRE_ONLY_HINT`). Rich SQL on gRPC uses
`execute_session_sql` (or psql); it is not re-implemented as a second planner.

`AT TIME ZONE` / `TIMEZONE()` accept fixed offsets (`UTC`/`GMT`/`±HH[:MM]`) and
IANA names (embedded `tzdb`, DST-aware). Session `SET TimeZone` defaults to
`UTC`; `LOCALTIMESTAMP` renders wall clock in the session zone.

## Write request lifecycle

An SQL write travels through the following stages:

1. **Client and pgwire** — `psql` sends a simple-query message. The pgwire
   backend authenticates the demo connection, strips protocol-level framing,
   and passes SQL to the SQL engine. Native clients enter through the gRPC Smart
   Client service instead.
2. **AST and logical planning** — `sqlparser` produces an AST. Takyonic lowers
   it into a logical `Insert` or `Select` plan. The cost-based optimizer (CBO)
   chooses among eligible secondary indexes for filtered reads; writes are
   normalized into primary-data and secondary-index records.
3. **Leader routing** — The Smart Client sends the operation to its cached Raft
   leader. A follower responds with `not_leader` and an address hint; the client
   invalidates its cache, discovers the current leader, and retries.
4. **MVCC workspace and OCC validation** — A transaction reads at
   `last_applied` and buffers its write set. Default isolation is Snapshot
   Isolation + OCC (PostgreSQL-equivalent `repeatable read`). `SET
   transaction_isolation` accepts `read committed` (still SI), `repeatable
   read`, and `serializable` (minimal SSI: concurrent readers of committed
   write keys are doomed to abort — blocks classic write-skew). `read
   uncommitted` is rejected. At commit, the leader serializes validation with
   proposal submission. It rejects the transaction if any key in the read or
   write set committed after the transaction's `read_ts`. Admission control is
   checked before the command enters consensus.
5. **Raft proposal and group commit** — The leader coalesces parked proposals
   into one durable Raft-log append, then replicates a multi-entry
   `AppendEntries` batch. This amortizes both local sync and network round trips.
   The proposal becomes committed only after the active membership reaches a
   quorum.
6. **State-machine apply** — Each node decodes committed entries in strict index
   order and applies the MVCC versions to its ordered memtable. The Raft log is
   the distributed path's durability source, so apply does not append the same
   command to a second engine WAL. `last_applied` advances only after the batch
   has been published successfully.
7. **Conflict metadata and flush** — Apply refreshes the per-key latest-commit
   index on every replica, preserving OCC correctness after failover. When the
   memtable reaches its configured threshold, Takyonic emits an immutable L0
   SSTable and schedules background compaction.
8. **Acknowledgement** — The leader wakes proposal waiters after quorum commit
   and local apply. The result flows back through the Smart Client and pgwire as
   a PostgreSQL command-complete response.

OCC validation deliberately occurs before Raft proposal and memtable
publication. Applying a write before conflict validation would make an aborted
transaction visible. The post-apply OCC step updates conflict metadata; it is
not validation of the already committed transaction.

### MVCC representation

Each logical key is encoded as an internal key containing the user key and
commit timestamp. Versions sort by user key ascending and timestamp descending,
which makes the newest visible value cheap to locate while retaining older
versions for active snapshots. Compaction uses the minimum active transaction
timestamp as a garbage-collection watermark.

Secondary indexes are ordinary transactional records. A record write and its
index projections are carried in one `TxnBatch`, so replicas cannot observe a
base row without its corresponding index update.

## LSM storage and compaction

The mutable state is an ordered memtable. Flushes create checksummed SSTables
containing data blocks, a key-range index, and a Bloom filter. Readers access
SSTables through memory maps held by reference-counted pins; retired files are
not unlinked until the final reader releases its pin.

Compaction has two independent bounded worker pools:

- **L0 Rapid** resolves overlapping L0 files into L1 before write amplification
  stalls ingestion.
- **Ln Haul** performs longer-running compactions through the lower levels.

Planning reserves overlapping files under the catalog lock, while merge I/O
runs outside it. Both pools share a write-rate pacer so compaction cannot starve
Raft/WAL sync latency.

## Raft log compaction and SST-backed snapshots

An ever-growing Raft log is unnecessary once every command through an index is
represented by the state machine. When the configured snapshot threshold is
reached, a node:

1. forces the memtable to immutable SSTables;
2. records the applied index and its term in `SNAPSHOT.meta`;
3. packages the active leveled SST files and cluster membership in a versioned
   snapshot payload; and
4. rewrites the Raft log, retaining only entries after the snapshot boundary.

If a follower's `next_index` falls behind that boundary, the leader streams the
snapshot in chunks with `InstallSnapshot` instead of replaying the compacted
prefix. The follower installs the packaged SST set, restores the included
membership, advances its commit/apply boundary, and resumes normal
`AppendEntries` catch-up. Snapshot files are synced and installed atomically so
a crash cannot expose a partially replaced LSM.

## Dynamic membership

Membership is part of the replicated state and consists of voter IDs plus their
advertised endpoints. Takyonic supports `AddNode` and `RemoveNode` using Raft's
one-at-a-time, single-server configuration-change rule:

- only one uncommitted configuration change may exist;
- a new membership and quorum take effect as soon as the entry is appended;
- truncating an uncommitted suffix reconstructs the previous membership;
- committed membership is persisted and included in snapshots; and
- a newly elected leader appends a no-op entry so prior-term configuration
  entries can become committed safely.

A joining node starts with empty membership and cannot campaign. `AddNode`
creates a snapshot boundary when needed, seeds replication cursors, and catches
the joiner up with `InstallSnapshot` before normal log replication continues.
When `RemoveNode(self)` commits, that node steps down and remains a passive
learner rather than forming an independent cluster. Peer connection caches track
membership generations so endpoint additions and removals propagate without a
restart.

This design intentionally implements single-server changes rather than joint
consensus. Operational tooling must therefore wait for one membership change to
commit before proposing the next.

## Recovery guarantees

Raft and engine records are checksummed. Recovery truncates only physically
incomplete trailing records; a checksum mismatch in the middle of a file is
treated as corruption, not as a repairable crash tail. SST creation follows
temp-write, file-sync, atomic-rename, and parent-directory-sync ordering.
Together with quorum commit and ordered apply, these boundaries ensure an
acknowledged write is recoverable after process termination.

Cross-shard 2PC decisions are appended to `data_dir/TC_DECISIONS` and
`sync_data`'d before Phase-2 participant apply. On coordinator reopen,
orphaned `PREPARED` shards are resolved from that log (presumed abort only
when no decision was recorded). Use `TransactionCoordinator::open` (not
`::new`) when decisions must survive process restart.

Prometheus counters (when `metrics_enabled`):
`takyonic_distributed_txn_{prepared,committed,aborted}_total` — scrape
`/metrics` for a minimal 2PC dashboard (prepare vs decide outcome rates).

## MPP honesty

With `mpp_enabled` / `--mpp`, distributed aggregates that are a single
`GROUP BY` column plus one of `SUM`/`COUNT`/`MIN`/`MAX`/`AVG` run through the
Coordinator + shuffle path (`DistAggKind`). Simple equi `DistributedJoin`
remote-scans both sides then finishes with a local HashJoin (NLJ fallback
for non-equi). Other aggregates fall back locally so EXPLAIN stays honest.
Remote fragment dispatch retries once on transient transport errors.
`REBALANCE TABLE name` measures per-node row load and applies one hot→cold
partition-map move, persisting `PMAP` in the catalog.

## Object-store pages

BPM pages use ChunkV2 (default 64 MiB). Checkpoint `flush_all` coalesces dirty
pages so each touched chunk is uploaded once (RMW overlay), not once per page.
SST uploads use a single PutObject when under 5 GiB (`max_sst_bytes` still
defaults to 1 GiB). Payloads at/above the AWS PutObject limit are uploaded via
**multipart** (`CreateMultipartUpload` / `UploadPart` / `Complete`). Local and
in-memory backends accept large objects without the AWS cap; the in-memory mock
exposes multipart part counters for tests.

## Sequences and schema product surface

`SERIAL` / `CREATE SEQUENCE` state persists in `data_dir/SEQUENCES` across
engine reopen. Column `DEFAULT`, `NOT NULL`, and single-column `UNIQUE`
constraints are enforced on INSERT. File-based `COPY table FROM|TO 'path'`
loads or dumps tab-separated text. `COPY … FROM STDIN` / `TO STDOUT` use the
PostgreSQL copy protocol (pgwire `CopyHandler`); session helpers
`SessionState::copy_from_tsv` / `copy_to_tsv` cover the same TSV format.

Describe / `information_schema.columns` report UUID, BYTEA, NUMERIC, and
TIMESTAMPTZ with matching `udt_name` tokens for basic ORM introspection.
