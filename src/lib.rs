//! Takyonic — embedded high-performance LSM-Tree key-value engine.
//!
//! Step 11 adds distributed Raft consensus over tonic/gRPC.

#![warn(missing_docs)]

pub mod admission;
pub mod cluster;
pub mod compaction;
pub mod config;
pub mod consensus;
pub mod engine;
pub mod error;
pub mod group_commit;
pub mod membership;
pub mod memtable;
pub mod network;
pub mod query;
pub mod raft;
pub mod raft_log;
pub mod schema;
pub mod snapshot;
pub mod sst;
pub mod stats;
pub mod telemetry;
pub mod tracing_init;
pub mod txn;
pub mod types;
pub mod wal;

pub use admission::{AdmissionController, AdmissionDecision, AdmissionOutcome};
pub use cluster::{TakyonicNode, wait_for_leader};
pub use compaction::{
    CompactionEngine, CompactionPool, CompactionResult, CompactionTicket, SstManager, SstMeta,
};
pub use config::Config;
pub use consensus::{RaftConsensus, Role};
pub use engine::TakyonicEngine;
pub use error::{Result, TakyonicError};
pub use group_commit::{ApplyHook, GroupCommitWal};
pub use membership::ClusterMembership;
pub use memtable::Memtable;
pub use network::{PeerClients, RaftGrpcService};
pub use query::{ExecutionPlan, Filter, FilterOp, IndexCandidate, Query};
pub use raft::{
    ApplyStatus, BatchApplyResult, CommittedEntry, LocalRaftNode, RaftCommand, RaftSnapshot,
    RaftStateMachine, RaftStateMachineApi,
};
pub use raft_log::{RaftLog, RaftLogEntry};
pub use schema::{IndexDef, Record, TableSchema, data_key, index_key};
pub use snapshot::{SnapshotMeta, SnapshotPayload, SnapshotSst};
pub use sst::{DeleteStatus, SstId, SstInfo, SstPin, SstReader, SstRegistry, SstWriter};
pub use stats::{StatsCatalog, TableStats};
pub use telemetry::{EngineMetrics, HistogramSnapshot, LatencyHistogram};
pub use tracing_init::init_tracing;
pub use txn::{Transaction, TxnTracker, WriteOp};
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
