//! Takyonic — embedded high-performance LSM-Tree key-value engine.
//!
//! Step 23/24 adds PostgreSQL extended-query scaffolding with parameter
//! substitution ([`pg::SessionState`], [`executor::ExecutionContext`]) and
//! Volcano join / filter infrastructure.

#![warn(missing_docs)]

pub mod admission;
pub mod bpm;
pub mod btree_storage;
pub mod catalog;
pub mod client;
pub mod client_service;
pub mod cluster;
pub mod compaction;
pub mod config;
pub mod consensus;
pub mod demo_bootstrap;
pub mod disk;
pub mod dtxn;
pub mod engine;
pub mod epoch;
pub mod error;
pub mod executor;
pub mod group_commit;
pub mod hnsw;
pub mod jit;
pub mod lsm_storage;
pub mod membership;
pub mod memtable;
pub mod manifest;
pub mod mpp;
pub mod network;
pub mod object_store;
pub mod oid;
pub mod page;
pub mod partition;
pub mod pg;
pub mod pg_catalog;
pub mod query;
pub mod raft;
pub mod raft_log;
pub mod rbac;
pub mod reliability;
pub mod schema;
pub mod shuffle;
pub mod shuffle_service;
pub mod snapshot;
pub mod sql;
pub mod sst;
pub mod stats;
pub mod storage;
pub mod telemetry;
pub mod tc_log;
pub mod tracing_init;
pub mod txn;
pub mod txn_wal;
pub mod twopc_service;
pub mod types;
pub mod vacuum;
pub mod vector;
pub mod vectorized;
pub mod wal;

pub use admission::{AdmissionController, AdmissionDecision, AdmissionOutcome};
pub use client::{
    ClientTxn, PGWIRE_ONLY_HINT, SessionSqlResult, TakyonicClient, pgwire_only_sql,
};
pub use client_service::{ClientGrpcService, LEADER_ADDR_META, NOT_LEADER_MSG};
pub use cluster::{TakyonicNode, wait_for_leader};
pub use compaction::{
    CompactionEngine, CompactionPool, CompactionResult, CompactionTicket, DEFAULT_MAX_SST_BYTES,
    KWayMergeIterator, SstManager, SstMeta, split_entries_by_max_bytes, sst_object_key,
};
pub use config::Config;
pub use consensus::{RaftConsensus, RaftNode, Role};
pub use dtxn::{
    CoordinatorDecision, DistTxnId, DistTxnOutcome, DistTxnRequest, EngineShard, GlobalClock,
    LocalShard, ParticipantLogRecord, ShardBranch, ShardId, ShardParticipant,
    TransactionCoordinator, TwopcState, partition_txn_branches, put_branch,
};
pub use twopc_service::{
    RemoteShard, TwopcGrpcService, bind_ephemeral, ephemeral_addr, serve_twopc_shard,
    serve_twopc_shard_listener,
};
pub use engine::{COMMENTS_FILE, TakyonicEngine, encode_comments, load_comments, parse_comments};
pub use error::{Result, TakyonicError};
pub use executor::{
    Accumulator, AggregateExec, AnalyzeExec, AvgAccumulator, CountAccumulator, DeleteExec,
    ExecutionContext, Executor, HashJoinExec, IndexScanExec, InsertExec, LimitExec, MaxAccumulator,
    MergeJoinExec, MinAccumulator, NestedLoopJoin, PhysicalPlan, SortExec, SumAccumulator,
    TableScanExec, TopNExec, UpdateExec, VacuumExec, affected_row_count, collect_rows, evaluate,
    evaluate_bool, execute_plan, execute_plan_autocommit, explain_physical, is_dml_plan,
    materialize_insert_records, open_executor, open_executor_with_txn, optimize as optimize_physical,
    optimize_with_catalog, optimize_without_jit, record_to_sql_values,
};
pub use jit::{
    CompiledFn, JitBatchBinOpFn, JitCompiler, JitIrType, collect_jit_columns, is_jit_compilable,
};
pub use group_commit::{ApplyHook, GroupCommitWal};
pub use membership::ClusterMembership;
pub use memtable::Memtable;
pub use manifest::{
    DEFAULT_PAGES_CHUNK_BYTES, DEFAULT_PAGES_PREFIX, MANIFEST_CURRENT_KEY, ManifestManager,
    ManifestSst, PagesLayout, StorageManifest,
};
pub use mpp::{
    Coordinator, DistAggKind, FragmentDispatcher, FragmentSpec, Fragmenter, Worker,
    WorkerEndpoint, extract_simple_agg, maybe_distribute,
};
pub use object_store::{
    AWS_S3_MULTIPART_MIN_PART_BYTES, AWS_S3_PUT_OBJECT_MAX_BYTES, DEFAULT_MULTIPART_PART_BYTES,
    InMemoryObjectStore, LocalFileBackend, ObjectStorage, S3Backend, assert_put_object_size,
    multipart_part_ranges, prefer_multipart,
};
pub use partition::{
    PartitionMap, PartitionPruningRule, PartitionRouter, PartitioningStrategy, RebalanceMove,
    Rebalancer, extract_partition_eq, hash_key,
};
pub use shuffle::{Distribution, ExchangeExec, ShuffleKey, ShuffleManager, hash_partition};
pub use shuffle_service::{GrpcFragmentDispatcher, RemoteShuffleClient, ShuffleGrpcService};
pub use pg::{
    AuthStage, BOOTSTRAP_PASSWORD, BOOTSTRAP_USER, BoundPlan, DEFAULT_DATABASE, ScramCredential,
    SessionResult, SessionState, SessionTxnMode, TakyonicAuthSource, TakyonicPgBackend,
    TakyonicPgFactory, TakyonicQueryParser, database_allowed, net_info_from_endpoints,
};
pub use query::{ExecutionPlan, Filter, FilterOp, IndexCandidate, Query};
pub use raft::{
    ApplyStatus, BatchApplyResult, CommittedEntry, LocalRaftNode, RaftCommand, RaftSnapshot,
    RaftStateMachine, RaftStateMachineApi,
};
pub use raft_log::{RaftLog, RaftLogEntry};
pub use rbac::{
    AuthCatalog, AuthContext, AuthorizationManager, ColumnGrantSpec, ColumnPrivilege,
    DatabasePrivilege, FunctionPrivilege, Privilege, RoleDef, RolePrivilege, SchemaPrivilege,
    SharedAuthCatalog, TypePrivilege,
};
pub use schema::{ColumnSpec, IndexDef, Record, TableSchema, data_key, data_table_prefix, index_key};
pub use snapshot::{SnapshotMeta, SnapshotPayload, SnapshotSst};
pub use sql::{
    AlterTableOp, ArithOp, CastTarget, CopyIoTarget, Expression, JoinType, LogicalPlan,
    LogicalPlanner, SetOpKind, SortExpr, SqlEngine, Value as SqlValue, aggregate_result_column,
    cast_sql_value, normalize_transaction_isolation, sql_like_match,
};
pub use sst::{DeleteStatus, SstId, SstInfo, SstPin, SstReader, SstRegistry, SstWriter};
pub use bpm::{BpmStats, BufferPoolManager, DEFAULT_LRU_K, PageGuard};
pub use btree_storage::BTreeStorage;
pub use demo_bootstrap::{
    DemoSeedOutcome, demo_users_schema, ensure_demo_users, should_seed_demo_users,
};
pub use disk::{
    DiskManager, PAGE_FILE_NAME, PAGES_V2_PREFIX, REMOTE_PAGES_KEY, file_page_id, is_file_cache_page,
    pages_chunk_key,
};
pub use epoch::{EpochManager, dead_versions_for_key, survivors_for_key};
pub use lsm_storage::{CompactionManager, LSMReader, LSMStorage};
pub use page::{DEFAULT_PAGE_SIZE, INVALID_PAGE_ID, Page, PageId};
pub use storage::{StorageEngineKind, StorageManager};
pub use stats::{ColumnStats, StatsCatalog, TableStats, compute_table_stats};
pub use vacuum::VacuumStats;
pub use vector::{DistanceMetric, VectorIndexSpec, VectorValue, euclidean_simd};
pub use vectorized::{
    SIMD_WIDTH, VECTOR_BATCH_SIZE, SimdKernels, VectorBatch, VectorizedAggregateExec,
    VectorizedScanExec, host_simd_level, is_vectorizable, rows_per_ms, vectorized_exec_count,
};
pub use telemetry::{EngineMetrics, HistogramSnapshot, LatencyHistogram, MetricsManager};
pub use tracing_init::init_tracing;
pub use txn::{IsolationLevel, Transaction, TxnTracker, WriteOp};
pub use txn_wal::{WalManager, WalRecord};
pub use types::{CommitTs, Entry, InternalKey, Key, SequenceNumber, Value, ValueType};
pub use wal::{WalReader, WalWriter};

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn public_surface_smoke() {
        init_tracing();
        let cfg = Config::new()
            .data_dir("/tmp/takyonic-data")
            .wal_dir("/tmp/takyonic-wal");
        cfg.validate().expect("valid config");

        let entry = Entry::put(Key::new(&b"hello"[..]), Value::new(&b"world"[..]), 1);
        assert!(!entry.is_tombstone());
        tracing::info!(key = ?entry.key, "step1 smoke ok");
    }

    #[test]
    fn ingestion_path_wal_then_memtable() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("takyonic-ingest-{nanos}"));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("000001.wal");

        {
            let mut wal = WalWriter::create(&path).unwrap();
            wal.append_sync(&Entry::put(&b"k"[..], &b"v"[..], 1))
                .unwrap();
        }

        let mt = Memtable::new();
        let mut reader = WalReader::open(&path).unwrap();
        reader.replay(|e| mt.apply(e)).unwrap();
        assert_eq!(mt.get(&Key::new(&b"k"[..])).unwrap().as_bytes(), b"v");

        let _ = std::fs::remove_dir_all(&dir);
    }
}
