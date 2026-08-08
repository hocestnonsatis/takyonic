//! MVCC VACUUM: reclaim dead tuple versions below the epoch watermark.
//!
//! [`VacuumStats`] summarizes one `VACUUM <table>` run. Physical removal happens
//! in the memtable ([`crate::memtable::Memtable::gc_below_watermark`]) and during
//! subsequent flush / leveled compaction (MergeIterator GC). Secondary index
//! keys under `Idx_<table>_` are vacuumed with the same watermark rule so
//! dangling index versions disappear with their heap counterparts.

use crate::types::CommitTs;

/// Result of a [`crate::engine::TakyonicEngine::vacuum_table`] invocation.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct VacuumStats {
    /// Target table.
    pub table: String,
    /// Watermark used for this run (oldest active epoch, or last-applied).
    pub watermark: CommitTs,
    /// Versioned entries removed from the memtable during GC.
    pub memtable_removed: u64,
    /// Approximate Data_/Idx_ version count before Vacuum (memtable + SSTs).
    pub versions_before: u64,
    /// Approximate Data_/Idx_ version count after flush + compaction GC.
    pub versions_after: u64,
    /// Sum of SST `file_size` before Vacuum.
    pub sst_bytes_before: u64,
    /// Sum of SST `file_size` after Vacuum.
    pub sst_bytes_after: u64,
    /// Dead heap (Data_) versions identified before GC.
    pub dead_heap_versions: u64,
    /// Dead secondary-index (Idx_) versions identified before GC.
    pub dead_index_versions: u64,
}

impl VacuumStats {
    /// Versions reclaimed across storage (best-effort; compaction may still be async).
    pub fn versions_reclaimed(&self) -> u64 {
        self.versions_before.saturating_sub(self.versions_after)
    }
}
