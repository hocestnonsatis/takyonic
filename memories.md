# Takyonic — Persistent Context

Güncelleme: 2026-07-17

## Proje Durumu
- Public GitHub repository: `https://github.com/hocestnonsatis/takyonic` (`main`)
- Step 1 (Foundation & Types): completed 2026-07-17 — types, Config, errors, tracing-init
- Step 2 (WAL & Memtable): completed 2026-07-17 — WalWriter/WalReader (xxh3 + sync_data), Memtable (RwLock+BTreeMap)
- Step 3 (SST & mmap Pinning): completed 2026-07-17 — block/index/Bloom layout, mmap reader, strict deferred deletion
- Step 4 (Dual-Pool Compaction): completed 2026-07-17 — leveled catalog, OCC reservations, streaming merge, physical worker pools
- Step 5 (Admission Control): completed 2026-07-17 — L0-aware token bucket, soft pacing, hard-limit waits
- Step 6 (Raft Integration): completed 2026-07-17 — command codec, ordered group commit, recovery/repair
- Step 7 (Engine Orchestration): completed 2026-07-17 — TakyonicEngine facade, flush, dual-pool lifecycle, close
- Step 8 (Mobile Stress Test): completed 2026-07-17 — telemetry module + mobile_stress harness; SURVIVED both runs
- Step 9 (Group Commit + Raft Wiring): completed 2026-07-17 — GroupCommitWal, LocalRaftNode, engine propose→apply; bench ~15k ops/s (2.1× Step 8)
- Step 10 (Crash Recovery Crucible): completed 2026-07-17 — SIGKILL chaos harness + torn-tail injection; 28/28 cycles recovered, zero lost acks
- Step 11 (Distributed Raft + tonic): completed 2026-07-17 — proto/gRPC transport, RaftConsensus, 3-node cluster elects leader and replicates 500 keys identically
- Step 12 (Network Batching + HA): completed 2026-07-17 — batched AppendEntries + propose coalescing; bench ~10.5k ops/s (35× Step 11); chaos assassination/failover/resurrection PASS
- Step 13 (Log Compaction + Snapshot): completed 2026-07-17 — SST-backed snapshots, Raft log truncate, InstallSnapshot catch-up; comatose follower crucible PASS
- Step 14 (Dynamic Membership): completed 2026-07-17 — single-server AddNode/RemoveNode, immediate-effect quorum, InstallSnapshot joiner catch-up; topology mutation crucible PASS
- Step 15 (MVCC + Snapshot Isolation): completed 2026-07-17 — InternalKey versions, Transaction OCC, watermark GC; bank invariant PASS
- Step 16 (Secondary Indexes + CBO): completed 2026-07-17 — Data_/Idx_ projection, TableStats NDV, cost-based planner; cbo_planner PASS
- Step 17 (Smart Client SDK): completed 2026-07-17 — TakyonicClient topology/routing + execute_txn OCC backoff; leader-kill bank crucible PASS
- Step 18 (SQL Parser + Logical Planner): completed 2026-07-17 — sqlparser GenericDialect → CBO/MVCC; execute_sql crucible PASS
- Step 19 (PostgreSQL Wire Protocol): completed 2026-07-17 — pgwire on :5433; psql INSERT/SELECT PASS
- Step 20 (V1.0 Repository Polish): completed 2026-07-17 — README, architecture guide, dual license, contributing guide, GitHub CI; all checks PASS
- Step 21 (Automated Release CI/CD): completed 2026-07-17 — `.github/workflows/release.yml`, tag-triggered (`v*`) cross-platform build matrix + GitHub Release publish
- Step 22 (Cloud-Native Deployment): completed 2026-07-17 — multi-stage `Dockerfile` (distroless), 3-node `docker-compose.yml`, cluster-aware `takyonic-server`, GHCR `docker-publish` CI job
- GitHub Community Standards completed 2026-07-17 — Code of Conduct, issue forms, PR template, security and support policies; Discussions and private vulnerability reporting enabled
- Package metadata is v1.0.2 with MSRV 1.85; initial six-step roadmap and Steps 7–22 are complete

## Ingestion Path (Step 2)
- WAL record: `[u32 len][body][u64 xxh3]`; body = flags|seq|key|value
- Durability: `File::sync_data` (fdatasync-style); WAL lives under `wal_dir`
- Memtable: ordered `parking_lot::RwLock<BTreeMap>` (not DashMap — flush needs key order)
- Newer seq wins; tombstones hide values on get

## SST Safety (Step 3)
- Immutable SSTs are temp-written, `sync_all`'d, atomically renamed, and parent-directory synced
- Layout: checksummed data blocks + checksummed key-range index + checksummed Bloom filter + fixed footer
- `SstReader::open` is private; mmap creation only occurs through `SstRegistry::register`
- `SstPin` owns an `Arc` to mmap state; retirement blocks new pins and defers unlink until all pins drop
- Never truncate SSTs; `reap` drops the final mmap before unlink

## Compaction (Step 4)
- `SstManager` owns levels and `HashSet<SstId>` reservations
- Protocol is strict: pick/reserve under lock → pin and block-stream merge unlocked → verify/install under lock
- L0 overlap closure plus target overlaps are reserved; non-overlapping plans can run concurrently
- Two bounded `crossbeam-channel` queues and long-lived physical pools: L0 Rapid and Ln Haul
- K-way merge holds one decoded block per input, chooses highest sequence per key, and preserves tombstones
- Both pools share an aggregate write pacer (`compaction_write_bytes_per_sec`) to protect WAL/Raft fsync latency
- Compaction input pins remain alive through install; external pins defer physical deletion through `SstRegistry`

## Write Admission (Step 5)
- `AdmissionController` consumes operation permits from a concurrent token bucket
- Refill is full rate through the L0 soft limit, then decreases linearly toward the configured minimum
- At the L0 hard limit, `try_acquire` returns `HardLimit` and clears stale burst credit
- `acquire_timeout` blocks with a deadline and uses `SstManager` L0 generation notifications
- L0 compaction install wakes blocked writers immediately; elapsed time is conservatively credited across rate changes

## Raft State Machine (Step 6)
- `RaftStateMachineApi` accepts strictly ordered committed batches of `CommittedEntry`
- `RaftCommand` uses a versioned `Bytes` codec for Put/Delete log payloads
- Apply path: validate contiguous indices → append entire batch → one `sync_data` → publish to memtable
- Proposal admission is separate; committed entries never pass through or fail L0 admission control
- `last_applied` is release/acquire published only after WAL durability and memtable apply
- Recovery replays checksummed WAL records and truncates only incomplete trailing records
- Checksum mismatches remain fatal and are never treated as repairable torn tails

## Engine Orchestration (Step 7 → 9)
- `TakyonicEngine` is the public facade: `open` / `put` / `get` / `delete` / `close`
- Write path: admit → Raft `propose` → group-commit WAL → apply hook → memtable → flush-to-L0
- Memtable remains ordered `RwLock<BTreeMap>` (not DashMap) for SST emission order
- `open` recovers on-disk SSTs into `SstManager`, replays WAL segments, starts dual pools + WAL flusher
- Flush drains memtable to L0 SST, rotates WAL (keeps prior segment), kicks L0 Rapid + Ln Haul
- `close` / `Drop`: flush residual memtable, stop group-commit flusher, prune WALs, shut down compaction pools
- Compaction I/O stays on paced pools; WAL sync never shares those worker locks

## Telemetry & Stress (Step 8)
- `src/telemetry.rs`: lock-free log-scale `LatencyHistogram` (256 buckets, 8 sub-buckets/octave, µs), `EngineMetrics` (ops, flushes, WAL sync histogram)
- Engine exposes `metrics()`; WAL `append_sync` duration recorded on every write
- `examples/mobile_stress.rs`: args = writers, total_ops, value_bytes, max_secs, memtable_kib, compaction_mib_s
- Termux/proot results: WAL fsync p50 ≈ 90µs, p99 ≈ 220µs, max ≈ 11ms; single-writer-lock ceiling ≈ 7.3k ops/s
- Strangled run (2MiB/s compaction): L0 pinned at hard limit 12, writers park in `acquire_timeout`, ops dip to 0 and recover on each compaction install — no timeouts (30s budget always met)
- Balanced run (16 writers, 16MiB/s): 1M ops in 156s, avg 6.4k ops/s, L0 oscillates 5–12, zero hard errors
- Both verdicts: SURVIVED — no panic, no deadlock, no OOM; close() drains pools cleanly (8.7s / 21.5s under backlog)

## Group Commit & Raft Wiring (Step 9)
- `GroupCommitWal`: dedicated flusher drains pending queue, one `append_batch_sync` per batch, Condvar wake; optional `ApplyHook` publishes to memtable after durability
- `LocalRaftNode`: single-node Raft stand-in — `propose(RaftCommand)` → group-commit log → apply hook → memtable; `RaftStateMachineApi` gained `snapshot` / `apply_snapshot` (`Bytes` payloads)
- Engine write path: admit → `LocalRaftNode::propose` (no per-op fsync) → maybe flush; memtable never updated before WAL durability
- Flush uses `Memtable::drain_entries`; WAL rotate keeps prior segment until `close` prunes
- Bench (`examples/group_commit_bench.rs`): 16 writers → ~15.2k ops/s (batch≈15); 32 writers → ~15.8k ops/s (batch≈31); **2.1–2.2×** over Step 8's 7.3k ceiling
- Per-batch wal sync p50 ≈ 191–287µs (amortized across batch); zero errors

## Crash Recovery (Step 10)
- `examples/crash_recovery.rs`: parent spawns itself as child (write storm), SIGKILLs at random 300–3000ms, verifies in-process, repeats
- Ack protocol: writer does raw `write()` of `key version` line AFTER `put` Ok — SIGKILL preserves page cache, so present ack ⇒ propose returned before crash
- Verification: every acked key readable at acked-or-newer version with bit-exact deterministic payload; second clean reopen must agree (idempotent recovery)
- Torn-tail injection every other iteration (partial len prefix / truncated body / bogus huge len) on top of SIGKILL; forensic pre-scan counts WAL records + torn tails
- SEQNO marker fix: `close()` durably writes `wal_dir/SEQNO` (temp+rename+dirsync) BEFORE pruning WAL; `open()` takes max(replay, SEQNO) — prevents seq regression → stale newest-wins vs SSTs
- Results: 10+10+8 = 28/28 cycles recovered; 9 injected torn tails truncated; zero lost acks, zero boot panics; recovery open ≤ ~120ms for ~35k-record WALs
- Known design property: checksum mismatch mid-file stays FATAL (real corruption); only physical EOF-truncated tails are repaired

## Distributed Raft (Step 11)
- Stack: `tonic` 0.12 + `prost` 0.13 + `tokio`; `proto/takyonic.proto` defines AppendEntries / RequestVote / InstallSnapshot
- `RaftLog`: durable group-commit shadow of Raft entries (term|command in value); apply hook absent — commit drives SM apply
- `RaftConsensus`: Follower/Candidate/Leader; election timeout 300–600ms; heartbeat/replication ~50ms; quorum = n/2+1
- `network`: tonic server + `PeerClients`; protobuf `bytes` ↔ `bytes::Bytes` at boundary
- `TakyonicNode`: engine + consensus + gRPC; `put` only on leader → propose → quorum → `engine.apply_committed`
- `examples/raft_cluster.rs`: 3 nodes on :15001–15003; elects leader; 500 puts; all nodes apply=500, 0 mismatches
- Throughput ~300 ops/s on loopback (per-propose quorum RTT); batching AppendEntries is the next lever

## Network Group Commit & HA (Step 12)
- Leader `propose()` parks writers in a pending queue; replication loop drains all pending into one Raft-log durable batch, then one multi-entry `AppendEntries` (cap 2048)
- Quorum ack advances `commit_index`; all parked writers wake together (network group commit)
- Dead-peer connect timeout 50ms + client cache invalidate on RPC failure (avoids 500ms join_all stalls after assassination)
- Bench (`examples/raft_bench.rs`): 32 writers / 5k ops → **~10.5k ops/s** (~35× vs Step 11's ~300 ops/s); all 3 nodes applied=5000
- Chaos (`examples/raft_chaos.rs`): kill leader mid write-storm → survivors elect new leader (term++) → writes resume → resurrect old leader as Follower, sync commit/applied, 0 missing spot-check keys; split-brain check (exactly one leader) PASS

## Raft Log Compaction & Snapshots (Step 13)
- Threshold `Config::raft_snapshot_threshold` (default 10k; harness uses 5k): after apply, if in-memory Raft log length ≥ threshold → `force_flush` memtable → persist `SNAPSHOT.meta` → rewrite `raft.log` retaining only `index > last_included`
- Snapshot payload (`src/snapshot.rs`, magic `TKYS`): packages live leveled SST file bytes via `bytes`; InstallSnapshot streams 256KiB chunks
- Leader detects gap (`next_index <= snapshot_index`) or unreachable peer → `InstallSnapshot` instead of AppendEntries; follower wipes LSM, installs SSTs, sets applied/commit to `last_included_*`
- Harness (`examples/raft_snapshot.rs`): kill node-3 → 50k write storm → compaction to snapshot_index≈20k → resurrect node-3 → InstallSnapshot + AE catch-up → 3/3 applied match, 0 spot-check mismatches

## Dynamic Membership (Step 14)
- Single-server ConfigChange (`RaftCommand::AddNode` / `RemoveNode`) with Ongaro immediate-effect: quorum uses new membership as soon as the entry is appended; truncate rebuilds membership from `base_membership` + remaining log
- Only one uncommitted ConfigChange at a time; leader appends a `Noop` on `become_leader` (Raft §5.4.2) so prior-term config entries can commit
- Joiner API: `TakyonicNode::open_joining` starts with empty membership (no elections); AddNode triggers snapshot boundary + InstallSnapshot; membership travels inside snapshot payload v2
- RemoveNode(self) committed → step down / `is_removed` passive learner; PeerClients syncs endpoints from `ClusterMembership` on every membership generation bump
- InstallSnapshot attempts cooldown (2s) + gentle next_index backoff so dead peers don't starve replication with SST-export storms
- Harness (`examples/raft_dynamic.rs`): 3→4 AddNode (quorum 2→3) via InstallSnapshot → kill node-1 → RemoveNode(1) (quorum 3→2 on {2,3,4}) → 7988/8000 acks, 0 mismatches, single leader PASS

## MVCC & Snapshot Isolation (Step 15)
- `InternalKey(UserKey, CommitTs)` + `ValueType`; memtable keeps all versions (BTreeMap by user ASC, ts DESC); SST blocks never split a user-key version chain
- `Transaction`: `begin` → `read_ts = last_applied`; `get`/`put` buffer write-set + track read-set; `commit` OCC-validates then proposes `RaftCommand::TxnBatch` (shared commit_ts)
- OCC: conflict if any read/write-set key has `last_commit_ts > read_ts` → `TakyonicError::Conflict` (client retries)
- Compaction GC watermark = min active txn `read_ts`; merge keeps versions `>= watermark` plus newest below; drops shadowed older versions
- Harness (`examples/mvcc_bank.rs`): 100×$1000, 8 workers × 500 transfers → sum=$100_000; ~4k commits, ~16% OCC abort rate, 0 lost dollars PASS

## Secondary Indexes & CBO (Step 16)
- Projection: `Data_<table>_<pk>` → record; `Idx_<table>_<index>_<value>_<pk>` → empty; `Transaction::put_record` writes both atomically under MVCC
- `TableStats`: `row_count` + per-index NDV; updated on commit via `StatsEdit` (zero read-path cost)
- CBO: `engine.query(table).filter(...).filter(...)` → pick min `eq_cost = row_count/NDV` index → IndexScan → PK fetch → residual filters
- Harness (`examples/cbo_planner.rs`): 10k users (9k active, 50 city=X, 200 cities) → EXPLAIN chooses `city` (est=50) over `status` (est=5000); returns 50 rows PASS

## Smart Client SDK (Step 17)
- Proto `ClientService`: Ping / Get / Put / BeginTxn / TxnGet / TxnPut / TxnCommit / TxnAbort multiplexed with Raft on the same port
- Followers return gRPC `FailedPrecondition` `not_leader` + metadata `x-takyonic-leader-address`
- `TakyonicClient`: seed ping → cache leader channel → transparent NotLeader invalidate/rediscover/retry
- `execute_txn(|txn| async { ... })`: auto-commit; on Conflict exponential backoff+jitter (10/25/50ms…); on NotLeader/network re-discover and re-run closure
- Leader sessions hold MVCC workspace; commit proposes `RaftCommand::TxnBatch` via `RaftConsensus` (serialized OCC + propose)
- `apply_committed` updates OCC `last_commit` on every replica (failover-safe SI)
- Harness (`examples/smart_client.rs`): 3-node bank storm → kill leader → post-failover transfers → sum invariant held, 0 app-visible errors PASS

## SQL Parser & Logical Planner (Step 18)
- Dep: `sqlparser` 0.62 (`default-features = false`, `std` only — avoids `stacker`/C toolchain on this host)
- `src/sql.rs`: `LogicalPlanner` / `SqlEngine` — GenericDialect AST → `LogicalPlan::Select { filters }` / `Insert { records }`
- WHERE: flatten AND of BinaryOps into CBO `Filter`s; INSERT VALUES → `Record` maps
- Proto: `RegisterTable`, `ExecuteQuery`, `TxnPutRecord`; leader runs CBO + returns EXPLAIN; put_record updates StatsEdit
- `TakyonicClient::execute_sql` / `explain_sql` / `register_table` / `ClientTxn::put_record`
- Harness (`examples/sql_interface.rs`): 1k SQL INSERTs (10 Ankara) → `SELECT ... status='active' AND city='Ankara'` → IndexScan(city) est=10, 10 rows PASS

## PostgreSQL Wire Protocol (Step 19)
- Dep: `pgwire` pinned to `=0.36.3` (`server-api` only) + `async-trait`; this is the latest line compatible with the project's Rust 1.85 MSRV (`pgwire` 0.37+ requires Rust 1.89)
- `src/pg.rs`: `TakyonicPgBackend` (SimpleQueryHandler) → `execute_sql`; `AcceptAnyCleartext` auth (any user/pass)
- Result mapping: `FieldInfo` + `DataRowEncoder` (INT8 / VARCHAR); INSERT → `CommandComplete` `INSERT 0 N`
- Binary `takyonic-server`: single-node Raft + pgwire on `127.0.0.1:5433` (Raft gRPC `:15433`)
- Single-node election fix: `run_election` promotes immediately when `peers.is_empty()` && quorum≤1
- Crucible: `PGPASSWORD=any psql -h 127.0.0.1 -p 5433 -U admin -d postgres` → INSERT 0 1 + tabular SELECT PASS
- Host note (2026-07-17): Ryzen 9 / x86_64 — `.cargo/config.toml` uses `CC=gcc` + gcc-16 lib path

## Release Pipeline (Step 21)
- `.github/workflows/release.yml`: triggers on `push` tags `v*`; `permissions: contents: write`
- Build matrix: `ubuntu-latest`/x86_64-unknown-linux-gnu, `windows-latest`/x86_64-pc-windows-msvc, `macos-14`/aarch64-apple-darwin
- protoc via `arduino/setup-protoc@v3` (cross-platform, needed for tonic-build); toolchain via `dtolnay/rust-toolchain@stable` with `targets`
- Packages: `.tar.gz` (Linux/macOS, bash), `.zip` (Windows, pwsh Compress-Archive); bundles binary + README + both LICENSE files
- Artifacts uploaded (upload-artifact@v4) then `release` job downloads (merge-multiple) and publishes via `softprops/action-gh-release@v2` with `generate_release_notes`

## Cloud-Native Deployment (Step 22)
- `takyonic-server` is now cluster-aware: CLI flags `--node-id`, `--peers id:host:port,...`, `--raft-port` (5001), `--pg-port` (5433), `--bind-host` (0.0.0.0), `--data-dir`; env fallbacks `TAKYONIC_NODE_ID/PEERS/RAFT_PORT/PG_PORT/BIND_HOST/DATA`
- Self binds `0.0.0.0:<raft-port>` (self endpoint only used to bind); peers keep advertised `host:port`; `leader_hint` resolves leader from each node's own membership so cross-node redirects use real hostnames
- Binary no longer wipes data dir (defaults `/data`); pgwire client seeds = loopback + all peers, retries `connect()` up to 60s for leader election; SIGTERM/Ctrl-C → `node.close()` flush
- `Dockerfile`: builder `rust:1.85-slim-bookworm` (protobuf-compiler + libprotobuf-dev for tonic-build) → cargo-chef-lite dep-cache layer (stub lib/bin build, then real src) → distroless `gcr.io/distroless/cc-debian12`; strips binary; EXPOSE 5001 5433; `/data` pre-created chown 65532 (nonroot) so named volume is writable; ENTRYPOINT `["/takyonic-server"]`
- `.dockerignore` trims context to manifests+src+proto (Cargo.lock IS tracked/committed — required by build)
- `docker-compose.yml`: 3 services node-1/2/3 on `takyonet` bridge, `image: ghcr.io/hocestnonsatis/takyonic:latest` + `build: .`; node-1 pgwire `5433:5433` (node-2→5434, node-3→5435); named volumes node{1,2,3}-data; peers via service DNS `--peers`
- Image name is bare product brand `ghcr.io/<owner>/takyonic` (no `-server` suffix, postgres/mysql-style)
- CI: `release.yml` gained `permissions: packages: write`, `IMAGE_NAME=ghcr.io/${{ github.repository }}`, and `docker-publish` job (`needs: build`) — buildx + docker/login-action (GITHUB_TOKEN) + metadata-action (tags: latest, semver {{version}}/{{major}}.{{minor}}, raw `v*` tag) + build-push-action@v6 with gha cache
- NOT verified with a live `docker build` here — sandbox has no docker daemon access (no passwordless sudo, not in docker group); validated via `cargo check/clippy -D warnings/fmt --check` and `docker compose config`

## Ortam Notları
- Host is Ryzen 9 9950X workstation (x86_64); `.cargo/config.toml` sets `CC=gcc`
- Legacy note: older Termux/proot aarch64 setups needed `aarch64-linux-gnu-gcc`

## Mimari Kısıtlar (DO NOT VIOLATE)
- Compaction I/O must NEVER starve WAL or Raft fsyncs
- mmap SST reads require Pin/Unpin refcounting (no truncate/unlink while pinned)
- Dual-pool compaction: L0 Rapid (L0→L1) + Ln Haul (L1→L2+) via crossbeam-channel
- OCC compaction: pick under lock, merge unlocked, install under lock; HashSet<SstId> reservations
- No RocksDB / Sled / existing DB engines

## Tech Stack
- parking_lot, crossbeam-channel, dashmap
- bytes, memmap2, xxhash-rust (xxh3/xxh64)
- tracing, thiserror, anyhow

## Tercihler
- Step-by-step roadmap; do not scaffold the entire engine at once
- Work on main branch; avoid creating new branches
