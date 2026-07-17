//! Table statistics for the cost-based optimizer.
//!
//! Tracks row counts and per-index distinct-value cardinality. Updated
//! incrementally on record put/delete (zero overhead on the read path) and
//! optionally refreshed during L0 flush.

use std::collections::{BTreeMap, HashMap, HashSet};

use parking_lot::RwLock;

use crate::schema::TableSchema;

/// Statistics for one table.
#[derive(Clone, Debug, Default)]
pub struct TableStats {
    /// Approximate live row count.
    pub row_count: u64,
    /// Distinct indexed values per index name.
    pub distinct: BTreeMap<String, u64>,
}

impl TableStats {
    /// Estimated rows matching an equality probe on `index`.
    ///
    /// Uses `row_count / NDV` (uniform assumption). Returns `row_count` when
    /// NDV is unknown or zero.
    pub fn eq_cost(&self, index: &str) -> u64 {
        let ndv = self.distinct.get(index).copied().unwrap_or(0).max(1);
        (self.row_count / ndv).max(1)
    }

    /// Selectivity of an equality predicate (`1/NDV`), clamped to `(0, 1]`.
    pub fn eq_selectivity(&self, index: &str) -> f64 {
        let ndv = self.distinct.get(index).copied().unwrap_or(1).max(1) as f64;
        (1.0 / ndv).clamp(f64::MIN_POSITIVE, 1.0)
    }
}

/// In-memory distinct-value sets used to maintain accurate NDV.
#[derive(Default)]
struct DistinctTracker {
    /// index_name → set of indexed values currently present.
    values: HashMap<String, HashSet<String>>,
    /// index_name → value → reference count (multiplicity across rows).
    refs: HashMap<String, HashMap<String, u64>>,
}

impl DistinctTracker {
    fn observe_insert(&mut self, index: &str, value: &str) {
        let refs = self.refs.entry(index.to_string()).or_default();
        let n = refs.entry(value.to_string()).or_insert(0);
        *n += 1;
        if *n == 1 {
            self.values
                .entry(index.to_string())
                .or_default()
                .insert(value.to_string());
        }
    }

    fn observe_delete(&mut self, index: &str, value: &str) {
        let Some(refs) = self.refs.get_mut(index) else {
            return;
        };
        let Some(n) = refs.get_mut(value) else {
            return;
        };
        *n = n.saturating_sub(1);
        if *n == 0 {
            refs.remove(value);
            if let Some(set) = self.values.get_mut(index) {
                set.remove(value);
            }
        }
    }

    fn ndv(&self, index: &str) -> u64 {
        self.values.get(index).map(|s| s.len() as u64).unwrap_or(0)
    }
}

/// Catalog of per-table statistics + distinct trackers.
#[derive(Default)]
pub struct StatsCatalog {
    stats: RwLock<BTreeMap<String, TableStats>>,
    trackers: RwLock<BTreeMap<String, DistinctTracker>>,
}

impl StatsCatalog {
    /// Create an empty stats catalog.
    pub fn new() -> Self {
        Self::default()
    }

    /// Ensure a table has a stats entry (call on schema register).
    pub fn register_table(&self, schema: &TableSchema) {
        let mut stats = self.stats.write();
        let entry = stats.entry(schema.name.clone()).or_default();
        for idx in &schema.indexes {
            entry.distinct.entry(idx.name.clone()).or_insert(0);
        }
        let mut trackers = self.trackers.write();
        trackers.entry(schema.name.clone()).or_default();
    }

    /// Snapshot stats for `table`.
    pub fn get(&self, table: &str) -> TableStats {
        self.stats.read().get(table).cloned().unwrap_or_default()
    }

    /// Record an inserted row (and its indexed column values).
    pub fn on_insert(&self, table: &str, index_values: &[(String, String)]) {
        let mut trackers = self.trackers.write();
        let tracker = trackers.entry(table.to_string()).or_default();
        for (index, value) in index_values {
            tracker.observe_insert(index, value);
        }
        let mut stats = self.stats.write();
        let st = stats.entry(table.to_string()).or_default();
        st.row_count = st.row_count.saturating_add(1);
        for (index, _) in index_values {
            st.distinct.insert(index.clone(), tracker.ndv(index));
        }
    }

    /// Record a deleted row.
    pub fn on_delete(&self, table: &str, index_values: &[(String, String)]) {
        let mut trackers = self.trackers.write();
        let tracker = trackers.entry(table.to_string()).or_default();
        for (index, value) in index_values {
            tracker.observe_delete(index, value);
        }
        let mut stats = self.stats.write();
        let st = stats.entry(table.to_string()).or_default();
        st.row_count = st.row_count.saturating_sub(1);
        for (index, _) in index_values {
            st.distinct.insert(index.clone(), tracker.ndv(index));
        }
    }

    /// Replace stats after a full rebuild (e.g. flush-time recount).
    pub fn replace(&self, table: &str, new_stats: TableStats) {
        self.stats.write().insert(table.to_string(), new_stats);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{IndexDef, TableSchema};

    #[test]
    fn eq_cost_prefers_high_ndv() {
        let mut st = TableStats {
            row_count: 10_000,
            distinct: BTreeMap::new(),
        };
        st.distinct.insert("status".into(), 2);
        st.distinct.insert("city".into(), 200);
        assert!(st.eq_cost("city") < st.eq_cost("status"));
        assert_eq!(st.eq_cost("city"), 50);
        assert_eq!(st.eq_cost("status"), 5_000);
    }

    #[test]
    fn tracker_updates_ndv() {
        let cat = StatsCatalog::new();
        let schema = TableSchema::new("users", "id", vec![IndexDef::new("city", "city")]);
        cat.register_table(&schema);
        cat.on_insert("users", &[("city".into(), "A".into())]);
        cat.on_insert("users", &[("city".into(), "A".into())]);
        cat.on_insert("users", &[("city".into(), "B".into())]);
        let st = cat.get("users");
        assert_eq!(st.row_count, 3);
        assert_eq!(st.distinct.get("city").copied(), Some(2));
        cat.on_delete("users", &[("city".into(), "A".into())]);
        let st = cat.get("users");
        assert_eq!(st.row_count, 2);
        assert_eq!(st.distinct.get("city").copied(), Some(2));
        cat.on_delete("users", &[("city".into(), "A".into())]);
        assert_eq!(cat.get("users").distinct.get("city").copied(), Some(1));
    }
}
