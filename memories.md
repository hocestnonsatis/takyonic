# Takyonic — Persistent Context

Güncelleme: 2026-07-19

## Proje Durumu
- Public GitHub repository: `https://github.com/hocestnonsatis/takyonic` (`main`)
- MPP scaffolding: `LogicalPlan::{DistributedAggregate,DistributedJoin}` + `src/shuffle.rs` (`Distribution`); optimizer lowers both to local Aggregate / HashJoin|NestedLoopJoin (2026-07-19)
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
- Step 18 (SQL Parser + Logical Planner): completed 2026-07-17 — sqlparser → CBO/MVCC; now `PostgreSqlDialect` (Step 41) for `<->` / `ARRAY`
- Step 19 (PostgreSQL Wire Protocol): completed 2026-07-17 — pgwire on :5433; psql INSERT/SELECT PASS
- Step 20 (V1.0 Repository Polish): completed 2026-07-17 — README, architecture guide, dual license, contributing guide, GitHub CI; all checks PASS
- Step 21 (Automated Release CI/CD): completed 2026-07-17 — `.github/workflows/release.yml`, tag-triggered (`v*`) cross-platform build matrix + GitHub Release publish
- Step 22 (Cloud-Native Deployment): completed 2026-07-17 — multi-stage `Dockerfile` (distroless), 3-node `docker-compose.yml`, cluster-aware `takyonic-server`, GHCR `docker-publish` CI job
- Step 23 (Prepared Statements + Join Infrastructure): scaffolding 2026-07-19 — pgwire Extended Query (`SessionState` Parse/Bind/Execute/Sync), `LogicalPlan::Join` + INNER JOIN ON parse, Volcano `NestedLoopJoin` + unit test (3 matching rows)
- Step 24 (Parameter Substitution & Binding): completed 2026-07-19 — `Expression::Parameter`, `sql::Value`, `ExecutionContext`, Bind byte→Value decode, Volcano `Filter` with `$1`; tests: `age > $1` + `Int(25)` → Ada/Di
- Step 25 (TableScan ↔ MVCC Storage): completed 2026-07-19 — `Transaction::scan_table_records`, `TableScanExec` via `open_executor_with_txn`, catalog PK check + `record_to_sql_values`; integration tests PASS
- Step 26 (DML INSERT/UPDATE/DELETE): completed 2026-07-19 — expression-based `LogicalPlan::{Insert,Update,Delete}`, Volcano `InsertExec`/`UpdateExec`/`DeleteExec`, implicit `Transaction::commit` after DML; tests PASS (Ada age=31 only)
- Step 27 (PK IndexScan + CBO heuristic): completed 2026-07-19 — `PhysicalPlan::IndexScan` + `Transaction::get_record`; optimizer rewrites `pk = lit|$n` → IndexScan; Update/Delete inherit; `pk_equality_optimizes_to_index_scan` PASS
- Step 28 (Explicit txn BEGIN/COMMIT/ROLLBACK): completed 2026-07-19 — `LogicalPlan::{Begin,Commit,Rollback}`; `Transaction` owns `Arc<Engine>`; `SessionState.active_txn` Idle/InTransaction; isolation+rollback test PASS
- Step 29 (HashJoin equi-join): completed 2026-07-19 — `PhysicalPlan::HashJoin` + `HashJoinExec` build/probe; optimizer rewrites `col=col` → HashJoin (else NestedLoop); `hash_join_users_orders_via_sql` PASS (3 rows)
- Step 30 (SCRAM-SHA-256 SASL auth): completed 2026-07-19 — pgwire `server-api-ring` + `SASLAuthStartupHandler`/`ScramAuth`; bootstrap `postgres`/`password`; psql login PASS / wrong pass FAIL
- Step 31 (Aggregations + GROUP BY): completed 2026-07-19 — `Expression::AggregateFunction`, `LogicalPlan::Aggregate`, `PhysicalPlan::Aggregate` + `AggregateExec` (Count/Sum/Avg/Min/Max); `group_by_count_sum_via_sql` PASS
- Step 32 (Sort / Limit / TopN): completed 2026-07-19 — `LogicalPlan::{Sort,Limit}`, `SortExec`/`LimitExec`, CBO fuses Sort→Limit into `TopNExec` (bounded heap); `group_by_order_by_limit_topn_e2e` PASS
- Step 33 (Secondary Index DDL + Volcano IndexScan + CBO): completed 2026-07-19 — `CREATE/DROP INDEX`, durable `CATALOG`, secondary `IndexScanExec`, IndexSelectionRule; EXPLAIN chooses `IndexScan(idx_dept)` PASS
- Step 34 (ARIES Txn WAL + Crash Redo): completed 2026-07-19 — `WalManager`/`WalRecord` (Insert/Update/Delete/Commit), WAL-before-apply on OCC commit, Redo on `Engine::open`; crash-abandon SQL recovery PASS
- Step 35 (CTE + Subquery + SemiJoin unnesting): completed 2026-07-19 — `WITH` CTEs, `IN`/`EXISTS`/scalar subqueries, `HashSemiJoin` SubqueryUnnestingRule; E2E EXPLAIN + SELECT PASS
- Step 36 (ANALYZE + Table Statistics + CBO selectivity): completed 2026-07-19 — `ANALYZE <table>`, durable `STATS`, HyperLogLog/reservoir, IndexSelection + HashJoin build-side; EXPLAIN skew plan switch PASS
- Step 37 (MVCC VACUUM / Garbage Collection): completed 2026-07-19 — `VACUUM <table>`, EpochManager watermark, VacuumExec + engine GC, index cleanup; SI pin + 10k reclaim PASS
- Step 38 (Buffer Pool Manager + Direct I/O + LRU-K): completed 2026-07-19 — Page/DiskManager(O_DIRECT)/BPM, SST data blocks via BPM, dirty flush + scan-resistance; 123 lib tests PASS
- Step 39 (Raft HA / Quorum OCC): completed 2026-07-19 — `RaftNode` alias, engine↔Raft attach, follower `NotLeader`, quorum propose before apply; mock election + 3-node SQL INSERT + leader-crash tests; **126** lib tests PASS
- Step 40 (JIT Query Compiler / Cranelift): completed 2026-07-19 — `JitCompiler`, expression codegen + Volcano fallback, HyPer-style `JitExec` push pipeline, CBO attach; arith/`Value::Float`; E2E + bench; **132** lib tests PASS
- Step 41 (Vector Search + HNSW): completed 2026-07-19 — `VectorValue`/SIMD distance, HNSW graph + durable `HNSW_<name>`, `CREATE VECTOR INDEX`, `VectorIndexScanExec`, CBO VectorIndexSelectionRule, VACUUM prune; **139** lib tests PASS
- Step 42 (RBAC / Security): completed 2026-07-19 — AUTH catalog, Argon2id + SCRAM, `CREATE USER/ROLE`, `GRANT`/`REVOKE`, `AuthorizationManager`, SessionState `current_user`; **145** lib tests PASS
- Step 43 (Telemetry / Observability): completed 2026-07-19 — `MetricsManager`, Prometheus `/metrics`, BPM/JIT/Raft/Txn instrumentation, `metrics_enabled`; **191** lib tests PASS
- Step 44 (MPP Distributed Query): completed 2026-07-19 — Exchange/Shuffle, Fragmenter, Coordinator/Worker, DistributedAggregate/Join, 3-node agg+shuffle tests; **156** lib tests PASS
- Step 45 (Multi-Engine LSM + B-Tree): completed 2026-07-19 — `LSMStorage`/`LSMReader`/`BTreeStorage`/`StorageManager`, per-table engine, K-way merge, VACUUM/ANALYZE + throughput bench; **164** lib tests PASS
- Step 46 (Partitioning + Partition Pruning): completed 2026-07-19 — Hash/Range strategy, catalog PMAP, Router, PartitionPruningRule, Coordinator::execute_insert, Rebalancer; **170** lib tests PASS
- Step 47 (Storage–Compute Decoupling): completed 2026-07-19 — ObjectStorage, LocalFile/S3-mock/optional aws-sdk-s3, ManifestManager, DiskManager+BPM two-tier cache; Engine::open loads shared manifest; **190** lib tests PASS
- Step 48 (SIMD JIT Vectorization): completed 2026-07-19 — VectorBatch, AVX2/AVX-512 kernels, Cranelift `F64X2` batch JIT, VectorizedExec, JITVectorizationRule; **191** lib tests PASS
- Step 49 (Distributed 2PC Coordinator): completed 2026-07-19 — `TransactionCoordinator`, Raft `TxnPrepare`/`TxnCommit`/`TxnAbort`, global SI clock, recovery; **191** lib tests PASS
- GitHub Community Standards completed 2026-07-17 — Code of Conduct, issue forms, PR template, security and support policies; Discussions and private vulnerability reporting enabled
- Package metadata is v1.0.2 with MSRV 1.85; Steps 7–49 complete (Merge join + Smart Client UPDATE/DELETE RPC still pending)

## Step 49 — Distributed Transaction Coordinator / 2PC (2026-07-19)
- `src/dtxn.rs`: `TransactionCoordinator` + `TwopcState::{Preparing,Prepared,Committed,Aborted}`; `LocalShard` participants with exclusive prepare locks + OCC
- Raft SM: `RaftCommand::{TxnPrepare,TxnCommit,TxnAbort}` (opcodes 7–9); Prepare/Abort are `is_meta` (durable log, no LSM apply); Commit applies write-set at global `commit_ts`
- Phase 1 prepare → all ACK → TC durable decision → Phase 2 commit; any prepare failure → abort all; crash-after-PREPARED → query TC on recover (`recover_participant`, presumed abort)
- `GlobalClock` issues cross-shard snapshot `read_ts` + commit timestamps for SI
- Metrics: `takyonic_distributed_txn_{prepared,committed,aborted}_total`
- Tests: cross-shard atomicity, prepare-failure rollback, crash recovery, concurrent stress (no leftover prepared), SI snapshot, Prometheus names; **191** lib tests PASS

## Step 48 — SIMD-Optimized JIT Vectorization (2026-07-19)
- `src/vectorized.rs`: `VectorBatch` (N=1024), `SimdKernels` (AVX-512 zmm / AVX2 ymm / portable), bitmask filters, `VectorizedScanExec` / `VectorizedAggregateExec`
- Cranelift: `JitCompiler::compile_batch_arith` emits packed `F64X2` load/op/store loops (`JitBatchBinOpFn`)
- CBO `JITVectorizationRule`: `PhysicalPlan::VectorizedExec` when estimated rows ≥ 256 and exprs are SIMD-lowerable; else scalar `JitExec`
- EXPLAIN shows `VectorizedExec(simd=avx512|avx2|sse2|scalar)`; results match scalar interpreter
- Tests: scalar vs SIMD mul/sum, masked BETWEEN, TPC-H Q6-style 100k-row throughput, Cranelift F64X2 batch, CBO rule; **191** lib tests PASS

## Step 47 — Storage–Compute Decoupling (2026-07-19)
- `src/object_store.rs`: `ObjectStorage` trait (`read`/`write`/`list`/`delete`); `LocalFileBackend`; `InMemoryObjectStore` (shared S3/MinIO mock); `S3Backend` (+ `--features s3` → aws-sdk-s3)
- `src/manifest.rs`: versioned JSON `StorageManifest` + `ManifestManager` (`manifest/CURRENT.json`) for SST/B-Tree/pages source of truth
- `DiskManager::open_with_remote`: Tier-1 local PAGES cache, Tier-2 remote hydrate/write-through; `BufferPoolManager` tracks `remote_fetches`
- Engine: `Config::object_store_root`, `open_with_object_storage`, loads manifest on open, `publish_storage_manifest` on close/checkpoint
- Tests: S3-mock cross-node, cold-start hydrate, 3-node restart integrity, engine manifest open + concurrent readers; **190** lib tests PASS

## Step 46 — Partitioning & Partition Pruning (2026-07-19)
- `src/partition.rs`: `PartitioningStrategy::{Hash,Range}`, `PartitionMap`, `PartitionRouter`, `PartitionPruningRule`, `Rebalancer`
- Catalog: `PARTITION HASH|RANGE …` + `PMAP node…`; `TableSchema::with_partitioning` / `with_partition_map`
- CBO: partitioned tables lower to `PhysicalPlan::DistributedScan` with pruned `RemoteWorker(node, partition)` list
- MPP: `Fragmenter::fragment_aggregate_pruned`, `Coordinator::execute_insert` routes to owning shard (no broadcast)
- Tests: hash spread across 3 nodes, EXPLAIN single RemoteWorker, INSERT ownership routing, rebalancer hot→cold; **170** lib tests PASS

## Step 45 — Multi-Engine LSM + B-Tree Storage (2026-07-19)
- Existing LSM path (Memtable → L0 SST → leveled compaction + Bloom) exposed as `LSMStorage` / `LSMReader` / `CompactionManager`
- `BTreeStorage`: in-memory MVCC B-Tree for read-friendly tables
- `StorageManager` + `StorageEngineKind::{Lsm,BTree}`; catalog `TABLE name pk [LSM|BTREE]`; `TableSchema::with_engine`
- Engine: `router_only` StorageManager; B-Tree tables mirrored on OCC commit; get/scan/VACUUM/ANALYZE routed by engine kind
- Public `KWayMergeIterator::from_sorted_runs` for SST merge / latest-version selection
- Tests: SST flush roundtrip, K-way latest version, multi-flush LSMReader, BTree vs LSM write throughput (50k debug / 1M release), VACUUM+ANALYZE across engines; **164** lib tests PASS

## Step 44 — MPP Distributed Query Execution (2026-07-19)
- `src/shuffle.rs`: `Distribution` (Hash/Range/RoundRobin), `ShuffleManager` (bounded channels + backpressure), `ExchangeExec` Volcano operator
- `src/mpp.rs`: `FragmentSpec`/`Fragmenter`, `Worker`/`Coordinator`, `maybe_distribute` → `LogicalPlan::{DistributedAggregate,DistributedJoin}`, virtual `hash(pk)%N` shards
- `src/shuffle_service.rs` + proto `ShuffleService` (Push/Fetch/ExecuteFragment/Close) on every node gRPC server
- Config: `mpp_enabled`
- Metrics: `takyonic_mpp_shuffle_rows_{sent,recv}_total`, `takyonic_mpp_fragments_total`
- Tests: exchange roundtrip, distributed agg (virtual 3 workers), INSERT…SELECT hash shuffle, `three_node_cluster_distributed_aggregate_and_shuffle_metrics`; **156** lib tests PASS

## Step 43 — Telemetry & Observability (2026-07-19)
- `src/telemetry.rs`: expanded `EngineMetrics` (BPM, JIT, Raft, txn/VACUUM atomics + histograms), `MetricsManager` OS-thread HTTP scrape server, Prometheus text format
- Config: `metrics_enabled` / `metrics_bind` (default `127.0.0.1:9090`, use `:0` in tests)
- Instrumentation: BPM dual-write, `JitCompiler`/`JitPipelineExec`, Raft election/append/heartbeat, OCC commit + active txn gauges, VACUUM cycle latency
- Engine: starts `MetricsManager` on open when enabled; `metrics_bind_addr()` / `render_metrics()`
- Tests: concurrent atomics, Prometheus text, HTTP scrape, overhead (<1% release / <10% debug), `metrics_http_scrape_reflects_jit_bpm_and_txn`; **191** lib tests PASS

## Step 42 — Role-Based Access Control & Security (2026-07-19)
- `src/rbac.rs`: `AuthCatalog` (`data_dir/AUTH`), `RoleDef`, `Privilege` (SELECT/INSERT/UPDATE/DELETE), Argon2id PHC + SCRAM credentials, `AuthorizationManager` / AccessControlRule
- DDL: `CREATE USER … WITH PASSWORD` (preprocess → `CREATE ROLE … LOGIN`), `CREATE ROLE`, `DROP ROLE/USER`, `GRANT`/`REVOKE` on tables, `GRANT role TO member`
- `SessionState`: `current_user` / `AuthContext`; `as_user`; authorize before every `run_plan`
- SUPERUSER-only: `VACUUM`, `ANALYZE`, `CREATE/DROP INDEX`, role/grant DDL
- PgWire: `TakyonicAuthSource` reads engine AUTH catalog (SCRAM-SHA-256); bootstrap `postgres`/`password` seeded on open
- Error: `TakyonicError::PermissionDenied`
- Tests: `rbac::*`, `parses_create_user_grant_revoke`, `session_rbac_analyst_select_ok_delete_denied`

## Step 41 — Vector Search + HNSW Indexing (2026-07-19)
- `src/vector.rs`: `VectorValue` (f32), `DistanceMetric` (Euclidean/Cosine), `euclidean_simd` (AVX2/SSE), `VectorIndexSpec`
- `src/hnsw.rs`: thread-safe `HnswIndex` (insert/delete/search_knn/prune); exact kNN for ≤256 nodes; snapshot `data_dir/HNSW_<name>`
- DDL: preprocess `CREATE VECTOR INDEX` → `CREATE INDEX … WITH (DIMENSION=…, TYPE=HNSW)`; parser uses `PostgreSqlDialect` (`<->`, `ARRAY[]`)
- Catalog: `VINDEX table name col dim metric type`; `IndexDef.vector: Option<VectorIndexSpec>`
- Engine: HNSW registry; OCC `StatsEdit::{VectorUpsert,VectorDelete}`; VACUUM `retain_pks`; save on create/close/vacuum
- Exec: `PhysicalPlan::VectorIndexScan` / `VectorIndexScanExec`; CBO rewrites `ORDER BY col <-> query LIMIT k`
- Distance eval uses SIMD Euclidean for `<->`
- Tests: `hnsw_2d_*`, `hnsw_3d_*`, `simd_euclidean_vs_scalar_throughput`, `session_vector_index_hnsw_explain_and_knn`

## Step 40 — JIT Query Compiler (Cranelift) (2026-07-19)
- Deps: `cranelift` / `cranelift-jit` / `cranelift-module` / `cranelift-codegen` 0.133
- `src/jit.rs`: `JitCompiler` owns `JITModule`; `JitIrType` maps Int→I64, Float→F64, Bool→I64, String→fallback
- Expression: `ArithOp` / `Expression::Arith` / `Expression::Or`; SQL `+ - * /` and `OR`; `Value::Float`
- Compile predicates + scalar arith to `extern "C" fn(*const i64, i64) -> i64`; string/unsupported → interpreter
- `PhysicalPlan::JitExec` — single push loop Scan→Filter→Aggregate (no Volcano vcalls between); CBO `maybe_attach_jit` when compilable
- `optimize_without_jit` for baseline benches; EXPLAIN prints `JitExec(agg|filter)`
- Tests: `jit_*` unit, `jit_sum_salary_times_tax_e2e`, `session_jit_olap_sum_filter_via_sql`, `jit_benchmark_*`

## Step 39 — Raft Distributed Consensus & Quorum OCC (2026-07-19)
- `RaftNode` = `RaftConsensus` (role, term, `voted_for`, `commit_index`); gRPC `AppendEntries` / `RequestVote` via `network`; membership in `ClusterMembership`
- `TakyonicEngine::attach_raft_node` (Weak, no cycle): cluster nodes gate local put/OCC through networked Raft
- Follower OCC/DML → `TakyonicError::NotLeader { leader_address }`; leader `block_in_place` + quorum `propose` after ARIES `log_txn_wal`
- Vote safety: `RaftLog::is_up_to_date` in `handle_request_vote`; randomized election timeout unchanged
- Tests: `mock_election_follower_candidate_leader`, `three_node_election_and_sql_insert_replicates`, `leader_crash_triggers_reelection_and_safe_writes`

## Step 23–38 Extended Query + Volcano + … + Buffer Pool Manager (2026-07-19)
- `SessionState` owns `Arc<TakyonicEngine>` + `active_txn: Option<Transaction>` (Idle vs InTransaction)
- `SessionState::run_plan` / `execute_sql`: BEGIN opens txn; COMMIT/ROLLBACK consume it; DQL/DML reuse workspace or auto-commit
- `Transaction` holds `Arc<TakyonicEngine>` (no lifetime) so sessions can store it; `Engine::begin(self: &Arc<Self>)`
- `LogicalPlan::{Begin,Commit,Rollback}` from sqlparser `StartTransaction` / `Commit` / `Rollback`
- PgWire Simple/Extended Query route through SessionState (local Volcano); factory takes `(client, engine)`
- Isolation: uncommitted INSERT invisible to a second `engine.begin()` snapshot; visible after COMMIT; ROLLBACK discards workspace
- `PhysicalPlan::IndexScan { table, index, index_column, key_value }` — PK (`index=None`) or secondary two-step lookup
- Optimizer: PK equality → IndexScan(pk); indexed `col = lit|$n` via IndexSelection (pre-ANALYZE: `eq_cost < rows`; post-ANALYZE: MCV/NDV ≤ 5% selectivity)
- `PhysicalPlan::HashJoin { left, right, left_key, right_key }` — build `HashMap<Value, Vec<Record>>`, probe right
- Equi-join rewrite: `Column == Column` → HashJoin (schema hints assign sides); `<`/`>`/`!=` → NestedLoopJoin
- SCRAM-SHA-256: `pgwire` feature `server-api-ring`; `TakyonicAuthSource` seeds `postgres`/`password` (fixed salt + PBKDF2 SaltedPassword); per-connection `SASLAuthStartupHandler` (never share SASL state); `AuthStage` documents handshake; SessionState tests bypass wire auth
- Connect: `PGPASSWORD=password psql -h 127.0.0.1 -p 5433 -U postgres -d postgres`
- Aggregates: `Expression::AggregateFunction` + `LogicalPlan::Aggregate { input, group_exprs, aggr_exprs }`; COUNT/SUM/AVG/MIN/MAX
- `PhysicalPlan::Aggregate` → `AggregateExec` drains child, `HashMap<Vec<Value>, Accumulators>`, emits group keys + aggregate columns (sorted by group key)
- Accumulators: `CountAccumulator`, `SumAccumulator`, `AvgAccumulator` (int div), `MinAccumulator`, `MaxAccumulator`
- Sort/Limit: `SortExpr` + `LogicalPlan::{Sort,Limit}`; chain Scan→Filter→Aggregate→Sort→Limit
- `ORDER BY SUM(x)` rewrites to column `sum(x)` after Aggregate; `SortExec` (full drain+sort), `LimitExec` (streaming skip/fetch)
- Top-N CBO: `Limit(Sort(_))` with fetch → `PhysicalPlan::TopN` / `TopNExec` (BinaryHeap size skip+fetch)
- Secondary indexes (Step 33):
  - DDL: `LogicalPlan::{CreateIndex,DropIndex,Explain}`; `CREATE INDEX name ON table(col)` / `DROP INDEX [IF EXISTS] name` / `EXPLAIN <stmt>`
  - Catalog: `data_dir/CATALOG` (`TABLE` / `INDEX` lines); load on `Engine::open`; save on register/create/drop
  - Storage: non-unique `Idx_<table>_<index>_<value>_<pk>` keys; `put_record`/`delete_record` maintain all secondary B-Trees in the same OCC txn; `create_index` backfills + `on_index_backfill` NDV
  - Volcano: `Transaction::lookup_by_index` → `IndexScanExec` two-step; `explain_physical` prints `IndexScan(idx_dept) on table.col`
  - Tests: catalog reopen, MVCC index key maintain, `session_create_index_explain_index_scan` (EXPLAIN + SELECT Engineering)
- ARIES transactional WAL (Step 34):
  - `src/txn_wal.rs`: `WalRecord::{Insert,Update,Delete,Commit}` + `WalManager` (`data_dir/TXN_WAL`, xxh3 framing, `sync_data`)
  - OCC commit: `log_txn_wal` (ops + Commit + fsync) **before** Raft/memtable apply; also on gRPC `txn_commit`
  - `Engine::open`: Redo committed batches → memtable, handoff into LSM WAL, truncate TXN_WAL; incomplete write-sets without Commit discarded
  - `abandon_for_crash_test`: hard crash without SST flush; tests `aries_wal_recovers_*` + `crash_abandon_recovers_committed_sql_from_wal`
- CTE / subqueries (Step 35):
  - `WITH alias AS (query)` → CTE map; `FROM alias` → `LogicalPlan::SubqueryAlias` (inline view)
  - Expressions: `InSubquery` / `Exists` / `ScalarSubquery` / `InList`; `LogicalPlan::Filter` for WHERE over non-base scans
  - Uncorrelated `IN (SELECT …)` → CBO `SubqueryUnnestingRule` → `HashJoin` with `JoinType::Semi` (EXPLAIN: `HashSemiJoin`); `NOT IN` → Anti
  - Residual / scalar / EXISTS: `rewrite_uncorrelated_subqueries` at Filter open (materialize once into InList/Literal)
  - Correlation flag + best-effort rewrite; true OuterRef Apply still pending
  - Tests: parser WITH/IN/EXISTS/scalar; `in_list_and_scalar_subquery_filter`; `cte_in_subquery_unnests_to_hash_semi_join`
- ANALYZE / table statistics (Step 36):
  - SQL: `LogicalPlan::Analyze` from sqlparser `ANALYZE <table>`; Volcano `PhysicalPlan::Analyze` / `AnalyzeExec`
  - Stats: `ColumnStats` (null_frac, NDV via HyperLogLog, min/max, MCV, equal-height histogram) + `TableStats.page_count`
  - Persistence: `data_dir/STATS`; load on `Engine::open`; `apply_analyzed_stats` after ANALYZE
  - Algorithms: full scan + reservoir (`ANALYZE_SAMPLE_SIZE`) for MCV/histogram; HLL for large-table NDV
  - CBO IndexSelection: after ANALYZE, IndexScan only if `eq_rows <= 5%` of table (MCV-aware); else Filter(TableScan)
  - CBO HashJoin: Inner join builds the smaller side by estimated cardinality
  - Tests: catalog reopen, AnalyzeExec NDV/min/max, `session_analyze_explain_switches_plan_on_skew`
- VACUUM / MVCC GC (Step 37):
  - `EpochManager` (`src/epoch.rs`): active txn epochs; watermark = oldest `read_ts`; idle → `last_applied+1`
  - Visibility: keep `commit_ts >= watermark` + newest below (snapshot floor); older shadowed versions are dead
  - SQL: `LogicalPlan::Vacuum` / `PhysicalPlan::Vacuum` / `VacuumExec`; SessionState runs Vacuum **without** an open snapshot
  - Engine: `vacuum_table` — classify dead Data_/Idx_ versions, memtable prefix GC, flush, drain compaction; dangling Idx_ purge pass
  - Tests: epoch watermark unit tests; `vacuum_respects_long_running_snapshot`; `session_vacuum_reclaims_dead_versions_after_updates` (10k)
- Buffer Pool Manager (Step 38):
  - `Page` / `DiskManager` (`src/page.rs`, `src/disk.rs`): fixed-size aligned frames; Linux `O_DIRECT` with buffered fallback; primary `data_dir/PAGES`
  - `BufferPoolManager` (`src/bpm.rs`): pre-allocated pool, pin/unpin, dirty flush on eviction + `flush_all` checkpoint; **LRU-K** (K=2) scan-resistant
  - Config: `bpm_pool_size` / `bpm_page_size` / `bpm_lru_k` (default 1024×4KiB, K=2; `0` disables)
  - SST integration: registry attaches BPM; data blocks read via page-aligned Direct I/O cache (`fetch_file_page`); mmap kept for index/Bloom
  - Engine: owns BPM, `checkpoint_buffer_pool` after memtable→L0 flush; txn mutations mark pages dirty via `PageGuard::write`
  - Tests: eviction+dirty flush, pin protection, LRU-K scan resistance, `session_bpm_caches_sst_reads_after_flush`
- Not yet wired: (none — backlog from 2026-07-19 cleared)
- Done: per-connection SessionState (pid-keyed DashMap + SCRAM user bind) — 2026-07-19
- Done: persistent role catalog reopen e2e (`create_user_grant_survives_engine_reopen`) — 2026-07-19
- Done: TxnDeleteRecord RPC + Smart Client UPDATE/DELETE (PK equality) — 2026-07-19
- Done: HAVING clause (Filter over Aggregate) — 2026-07-19
- Done: MergeJoin for sorted equi-join inputs — 2026-07-19
- Done: correlated OuterRef Apply (per-row EXISTS/IN/scalar) — 2026-07-19
- Implementation plan: `docs/superpowers/plans/2026-07-19-not-yet-wired-gaps.md` (complete)

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
- Dep: `pgwire` pinned to `=0.36.3` (`server-api-ring` for SCRAM) + `async-trait`; latest line compatible with Rust 1.85 MSRV (`pgwire` 0.37+ requires Rust 1.89)
- `src/pg.rs`: `TakyonicPgBackend` + SCRAM via `SASLAuthStartupHandler` / `TakyonicAuthSource` (bootstrap `postgres`/`password`)
- Result mapping: `FieldInfo` + `DataRowEncoder` (INT8 / VARCHAR); INSERT → `CommandComplete` `INSERT 0 N`
- Binary `takyonic-server`: single-node Raft + pgwire on `127.0.0.1:5433` (Raft gRPC `:15433`)
- Single-node election fix: `run_election` promotes immediately when `peers.is_empty()` && quorum≤1
- Crucible: `PGPASSWORD=password psql -h 127.0.0.1 -p 5433 -U postgres -d postgres` → SCRAM ok; wrong password → FATAL auth failed
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
- Live verify 2026-07-17: `docker compose build` + `up` 3-node cluster PASS — node-1 elected leader; INSERT via :5433, SELECT via follower :5434, INSERT via :5435 all replicated; psql INSERT 0 N + tabular SELECT OK
- GHCR pull still 403 for anonymous/`gh` token (no `read:packages`); package visibility is private — make `ghcr.io/hocestnonsatis/takyonic` public under Package settings → Change visibility, then `docker pull` works without auth

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
