//! Horizontal table partitioning (Hash / Range) and partition pruning.
//!
//! Tables declare a [`PartitioningStrategy`] and a [`PartitionMap`] that assigns
//! each bucket / range slice to a cluster node. The [`PartitionRouter`] maps
//! partition-key values to owning node(s); [`PartitionPruningRule`] strips
//! unrelated worker fragments from distributed plans when the predicate pins
//! the partition key. A lightweight [`Rebalancer`] monitors per-node load and
//! proposes fragment moves to cool hotspots.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use parking_lot::RwLock;
use tracing::{debug, info};
use xxhash_rust::xxh3::xxh3_64;

use crate::error::{Result, TakyonicError};
use crate::mpp::{FragmentSpec, WorkerEndpoint};
use crate::query::FilterOp;
use crate::schema::TableSchema;
use crate::shuffle::hash_partition;
use crate::sql::Expression;

/// How a table's rows are horizontally sharded across the cluster.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub enum PartitioningStrategy {
    /// No horizontal partitioning (replicated / single-node semantics).
    #[default]
    None,
    /// `hash(column) % bucket_count` → partition id.
    Hash {
        /// Partition key column (often the primary key).
        column: String,
        /// Number of hash buckets (typically == worker count).
        bucket_count: u32,
    },
    /// Lexicographic ranges over `column` with inclusive lower bounds.
    Range {
        /// Partition key column.
        column: String,
        /// Lower bounds per partition (`bounds.len() == partition_count`).
        /// Partition `i` owns `[bounds[i], bounds[i+1])` (last unbounded above).
        bounds: Vec<String>,
    },
}

impl PartitioningStrategy {
    /// Partition-key column, if any.
    pub fn column(&self) -> Option<&str> {
        match self {
            Self::None => None,
            Self::Hash { column, .. } | Self::Range { column, .. } => Some(column.as_str()),
        }
    }

    /// Number of partitions / buckets.
    pub fn partition_count(&self) -> u32 {
        match self {
            Self::None => 1,
            Self::Hash { bucket_count, .. } => (*bucket_count).max(1),
            Self::Range { bounds, .. } => bounds.len().max(1) as u32,
        }
    }

    /// Catalog token (`HASH` / `RANGE` / `NONE`).
    pub fn kind_str(&self) -> &'static str {
        match self {
            Self::None => "NONE",
            Self::Hash { .. } => "HASH",
            Self::Range { .. } => "RANGE",
        }
    }

    /// Map a partition-key value to a partition id in `0..partition_count`.
    pub fn partition_id(&self, key_value: &str) -> u32 {
        match self {
            Self::None => 0,
            Self::Hash { bucket_count, .. } => hash_partition(key_value, *bucket_count),
            Self::Range { bounds, .. } => {
                let n = bounds.len().max(1) as u32;
                if bounds.is_empty() {
                    return 0;
                }
                for (i, b) in bounds.iter().enumerate().skip(1) {
                    if key_value < b.as_str() {
                        return (i as u32 - 1).min(n - 1);
                    }
                }
                n - 1
            }
        }
    }
}

/// Node assignment for each partition id (`assignments[i]` = owning `node_id`).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PartitionMap {
    /// Owning cluster node id per partition slot.
    pub assignments: Vec<u64>,
}

impl PartitionMap {
    /// Build a round-robin map over `node_ids` for `partition_count` slots.
    pub fn round_robin(node_ids: &[u64], partition_count: u32) -> Self {
        let n = partition_count.max(1) as usize;
        if node_ids.is_empty() {
            return Self {
                assignments: (0..n).map(|i| i as u64 + 1).collect(),
            };
        }
        Self {
            assignments: (0..n)
                .map(|i| node_ids[i % node_ids.len()])
                .collect(),
        }
    }

    /// Owning node for `partition_id` (falls back to slot+1).
    pub fn node_for(&self, partition_id: u32) -> u64 {
        self.assignments
            .get(partition_id as usize)
            .copied()
            .unwrap_or(u64::from(partition_id) + 1)
    }

    /// All distinct node ids in the map.
    pub fn nodes(&self) -> Vec<u64> {
        let mut v = self.assignments.clone();
        v.sort_unstable();
        v.dedup();
        v
    }
}

/// Routes partition-key values to owning cluster node(s).
#[derive(Clone, Debug)]
pub struct PartitionRouter {
    workers: Vec<WorkerEndpoint>,
}

impl PartitionRouter {
    /// Build a router from the current worker directory.
    pub fn new(workers: Vec<WorkerEndpoint>) -> Self {
        Self { workers }
    }

    /// Worker endpoints (slot order).
    pub fn workers(&self) -> &[WorkerEndpoint] {
        &self.workers
    }

    /// Resolve the owning node for a concrete partition-key value.
    pub fn route_key(&self, schema: &TableSchema, key_value: &str) -> Result<(u32, u64)> {
        let strategy = &schema.partitioning;
        if matches!(strategy, PartitioningStrategy::None) {
            let node = self.workers.first().map(|w| w.node_id).unwrap_or(1);
            return Ok((0, node));
        }
        let pid = strategy.partition_id(key_value);
        let node = if !schema.partition_map.assignments.is_empty() {
            schema.partition_map.node_for(pid)
        } else if let Some(w) = self.workers.iter().find(|w| w.slot == pid) {
            w.node_id
        } else if let Some(w) = self.workers.get(pid as usize) {
            w.node_id
        } else {
            u64::from(pid) + 1
        };
        Ok((pid, node))
    }

    /// Nodes that must receive a fragment given an optional predicate.
    ///
    /// When the predicate equality-binds the partition key, returns a singleton
    /// (or empty on contradiction). Otherwise returns every mapped node.
    pub fn route_predicate(
        &self,
        schema: &TableSchema,
        predicate: Option<&Expression>,
    ) -> Result<Vec<(u32, u64)>> {
        if matches!(schema.partitioning, PartitioningStrategy::None) {
            return Ok(self
                .workers
                .iter()
                .map(|w| (w.slot, w.node_id))
                .collect());
        }
        if let Some(pred) = predicate {
            if let Some(val) = extract_partition_eq(pred, schema.partitioning.column().unwrap_or(""))
            {
                let (pid, node) = self.route_key(schema, &val)?;
                return Ok(vec![(pid, node)]);
            }
            if let Some(vals) =
                extract_partition_in(pred, schema.partitioning.column().unwrap_or(""))
            {
                let mut out = Vec::new();
                for v in vals {
                    let (pid, node) = self.route_key(schema, &v)?;
                    if !out.iter().any(|(p, n)| *p == pid && *n == node) {
                        out.push((pid, node));
                    }
                }
                return Ok(out);
            }
        }
        // Cluster-wide: one fragment per partition assignment.
        let n = schema.partitioning.partition_count();
        let mut out = Vec::with_capacity(n as usize);
        for pid in 0..n {
            let node = if !schema.partition_map.assignments.is_empty() {
                schema.partition_map.node_for(pid)
            } else if let Some(w) = self.workers.iter().find(|w| w.slot == pid) {
                w.node_id
            } else {
                u64::from(pid) + 1
            };
            out.push((pid, node));
        }
        Ok(out)
    }
}

/// CBO rule: strip worker fragments that cannot contain matching rows.
pub struct PartitionPruningRule;

impl PartitionPruningRule {
    /// Prune `(node_id, FragmentSpec)` list using the table's partition key
    /// equality (if present in `predicate`).
    pub fn prune_fragments(
        schema: &TableSchema,
        predicate: Option<&Expression>,
        fragments: Vec<(u64, FragmentSpec)>,
        router: &PartitionRouter,
    ) -> Result<Vec<(u64, FragmentSpec)>> {
        if matches!(schema.partitioning, PartitioningStrategy::None) {
            return Ok(fragments);
        }
        let keep = router.route_predicate(schema, predicate)?;
        let keep_set: std::collections::HashSet<(u32, u64)> = keep.into_iter().collect();
        let pruned: Vec<_> = fragments
            .into_iter()
            .filter(|(node_id, spec)| {
                let pid = fragment_partition_id(spec);
                keep_set.contains(&(pid, *node_id))
                    || keep_set.iter().any(|(p, n)| *p == pid && *n == *node_id)
            })
            .collect();
        debug!(
            table = %schema.name,
            kept = pruned.len(),
            "PartitionPruningRule applied"
        );
        Ok(pruned)
    }

    /// Build the pruned remote-worker list for EXPLAIN / fragment graphs.
    pub fn prune_workers(
        schema: &TableSchema,
        predicate: Option<&Expression>,
        router: &PartitionRouter,
    ) -> Result<Vec<(u64, u32)>> {
        let targets = router.route_predicate(schema, predicate)?;
        Ok(targets
            .into_iter()
            .map(|(pid, node)| (node, pid))
            .collect())
    }
}

fn fragment_partition_id(spec: &FragmentSpec) -> u32 {
    match spec {
        FragmentSpec::PartitionedScan { partition_id, .. }
        | FragmentSpec::PartialAggregate { partition_id, .. } => *partition_id,
        _ => 0,
    }
}

/// Extract `col = literal` (or `literal = col`) for the partition column.
pub fn extract_partition_eq(expr: &Expression, column: &str) -> Option<String> {
    match expr {
        Expression::BinaryOp {
            left,
            op: FilterOp::Eq,
            right,
        } => match (left.as_ref(), right.as_ref()) {
            (Expression::Column(c), Expression::Literal(v)) if c == column => Some(v.clone()),
            (Expression::Literal(v), Expression::Column(c)) if c == column => Some(v.clone()),
            _ => None,
        },
        Expression::And { left, right } => {
            extract_partition_eq(left, column).or_else(|| extract_partition_eq(right, column))
        }
        _ => None,
    }
}

fn extract_partition_in(expr: &Expression, column: &str) -> Option<Vec<String>> {
    // Best-effort: OR of equalities on the same column.
    match expr {
        Expression::Or { left, right } => {
            let mut out = Vec::new();
            if let Some(v) = extract_partition_eq(left, column) {
                out.push(v);
            } else if let Some(mut vs) = extract_partition_in(left, column) {
                out.append(&mut vs);
            } else {
                return None;
            }
            if let Some(v) = extract_partition_eq(right, column) {
                out.push(v);
            } else if let Some(mut vs) = extract_partition_in(right, column) {
                out.append(&mut vs);
            } else {
                return None;
            }
            Some(out)
        }
        _ => None,
    }
}

/// Stable hash used for insert routing (same as shuffle `hash_partition`).
pub fn hash_key(key: &str) -> u64 {
    xxh3_64(key.as_bytes())
}

/// Proposed move of one partition fragment between nodes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RebalanceMove {
    /// Partition id to relocate.
    pub partition_id: u32,
    /// Current owner.
    pub from_node: u64,
    /// Destination owner.
    pub to_node: u64,
}

/// Background task: watch per-node row counts and propose rebalance moves.
pub struct Rebalancer {
    loads: Arc<RwLock<HashMap<u64, AtomicU64>>>,
    map: Arc<RwLock<PartitionMap>>,
    stop: Arc<AtomicBool>,
    handle: RwLock<Option<JoinHandle<()>>>,
    /// Moves applied (for tests / metrics).
    applied: AtomicU64,
}

impl Rebalancer {
    /// Create an idle rebalancer over `initial` partition map.
    pub fn new(initial: PartitionMap) -> Self {
        Self {
            loads: Arc::new(RwLock::new(HashMap::new())),
            map: Arc::new(RwLock::new(initial)),
            stop: Arc::new(AtomicBool::new(false)),
            handle: RwLock::new(None),
            applied: AtomicU64::new(0),
        }
    }

    /// Current partition → node map.
    pub fn partition_map(&self) -> PartitionMap {
        self.map.read().clone()
    }

    /// Record that `node_id` stores `delta` additional rows (can be negative).
    pub fn observe_load(&self, node_id: u64, delta: i64) {
        let loads = self.loads.read();
        if let Some(c) = loads.get(&node_id) {
            if delta >= 0 {
                c.fetch_add(delta as u64, Ordering::Relaxed);
            } else {
                let sub = (-delta) as u64;
                let _ = c.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                    Some(v.saturating_sub(sub))
                });
            }
            return;
        }
        drop(loads);
        let mut loads = self.loads.write();
        loads
            .entry(node_id)
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(delta.max(0) as u64, Ordering::Relaxed);
    }

    /// Snapshot of node → approximate row counts.
    pub fn load_snapshot(&self) -> HashMap<u64, u64> {
        self.loads
            .read()
            .iter()
            .map(|(k, v)| (*k, v.load(Ordering::Relaxed)))
            .collect()
    }

    /// Number of rebalance moves applied.
    pub fn moves_applied(&self) -> u64 {
        self.applied.load(Ordering::Relaxed)
    }

    /// Compute a single move if the hottest node is > 2× the coldest (and both exist).
    pub fn plan_move(&self) -> Option<RebalanceMove> {
        let snap = self.load_snapshot();
        if snap.len() < 2 {
            return None;
        }
        let (hot_node, hot_load) = snap.iter().max_by_key(|(_, v)| *v)?;
        let (cold_node, cold_load) = snap.iter().min_by_key(|(_, v)| *v)?;
        if *hot_node == *cold_node || *hot_load < 2 || *hot_load < cold_load.saturating_mul(2) {
            return None;
        }
        let map = self.map.read();
        let pid = map
            .assignments
            .iter()
            .enumerate()
            .find(|(_, n)| **n == *hot_node)
            .map(|(i, _)| i as u32)?;
        Some(RebalanceMove {
            partition_id: pid,
            from_node: *hot_node,
            to_node: *cold_node,
        })
    }

    /// Apply `mv` to the in-memory partition map and adjust load counters.
    pub fn apply_move(&self, mv: &RebalanceMove) -> Result<()> {
        {
            let mut map = self.map.write();
            if let Some(slot) = map.assignments.get_mut(mv.partition_id as usize) {
                if *slot != mv.from_node {
                    return Err(TakyonicError::Engine(format!(
                        "rebalance: partition {} owned by {}, expected {}",
                        mv.partition_id, slot, mv.from_node
                    )));
                }
                *slot = mv.to_node;
            } else {
                return Err(TakyonicError::Engine(format!(
                    "rebalance: unknown partition {}",
                    mv.partition_id
                )));
            }
        }
        // Transfer a chunk of load estimate.
        let half = self
            .loads
            .read()
            .get(&mv.from_node)
            .map(|c| c.load(Ordering::Relaxed) / 2)
            .unwrap_or(0);
        self.observe_load(mv.from_node, -(half as i64));
        self.observe_load(mv.to_node, half as i64);
        self.applied.fetch_add(1, Ordering::Relaxed);
        info!(
            partition = mv.partition_id,
            from = mv.from_node,
            to = mv.to_node,
            "rebalancer moved partition fragment"
        );
        Ok(())
    }

    /// Run one planning + apply cycle (tests / foreground).
    pub fn tick(&self) -> Result<Option<RebalanceMove>> {
        let Some(mv) = self.plan_move() else {
            return Ok(None);
        };
        self.apply_move(&mv)?;
        Ok(Some(mv))
    }

    /// Spawn a background loop that ticks every `interval`.
    pub fn start_background(self: &Arc<Self>, interval: Duration) {
        let stop = Arc::clone(&self.stop);
        let this = Arc::clone(self);
        let handle = thread::Builder::new()
            .name("takyonic-rebalancer".into())
            .spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    thread::sleep(interval);
                    if stop.load(Ordering::Relaxed) {
                        break;
                    }
                    let _ = this.tick();
                }
            })
            .expect("spawn rebalancer");
        *self.handle.write() = Some(handle);
    }

    /// Stop the background thread.
    pub fn stop(&self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(h) = self.handle.write().take() {
            let _ = h.join();
        }
    }
}

impl Drop for Rebalancer {
    fn drop(&mut self) {
        self.stop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::TableSchema;

    fn hash_schema(buckets: u32, nodes: &[u64]) -> TableSchema {
        TableSchema::new("users", "user_id", vec![])
            .with_partitioning(PartitioningStrategy::Hash {
                column: "user_id".into(),
                bucket_count: buckets,
            })
            .with_partition_map(PartitionMap::round_robin(nodes, buckets))
    }

    #[test]
    fn hash_router_spreads_keys_across_nodes() {
        let schema = hash_schema(3, &[1, 2, 3]);
        let router = PartitionRouter::new(vec![
            WorkerEndpoint {
                node_id: 1,
                address: "a".into(),
                slot: 0,
            },
            WorkerEndpoint {
                node_id: 2,
                address: "b".into(),
                slot: 1,
            },
            WorkerEndpoint {
                node_id: 3,
                address: "c".into(),
                slot: 2,
            },
        ]);
        let mut by_node: HashMap<u64, u32> = HashMap::new();
        for i in 0..300 {
            let (_pid, node) = router.route_key(&schema, &i.to_string()).unwrap();
            *by_node.entry(node).or_default() += 1;
        }
        assert_eq!(by_node.len(), 3, "expected all 3 nodes, got {by_node:?}");
        for c in by_node.values() {
            assert!(*c > 50, "skewed distribution {by_node:?}");
        }
    }

    #[test]
    fn pruning_equality_keeps_single_worker() {
        let schema = hash_schema(3, &[10, 20, 30]);
        let router = PartitionRouter::new(Vec::new());
        let pred = Expression::BinaryOp {
            left: Box::new(Expression::Column("user_id".into())),
            op: FilterOp::Eq,
            right: Box::new(Expression::Literal("42".into())),
        };
        let workers = PartitionPruningRule::prune_workers(&schema, Some(&pred), &router).unwrap();
        assert_eq!(workers.len(), 1, "expected single RemoteWorker, got {workers:?}");
        let (pid, _) = router.route_key(&schema, "42").unwrap();
        assert_eq!(workers[0].1, pid);
    }

    #[test]
    fn rebalancer_moves_from_hot_to_cold() {
        let map = PartitionMap::round_robin(&[1, 2, 3], 3);
        let rb = Rebalancer::new(map);
        rb.observe_load(1, 1000);
        rb.observe_load(2, 10);
        rb.observe_load(3, 10);
        let mv = rb.tick().unwrap().expect("should move");
        assert_eq!(mv.from_node, 1);
        assert!(mv.to_node == 2 || mv.to_node == 3);
        assert_eq!(rb.partition_map().node_for(mv.partition_id), mv.to_node);
        assert_eq!(rb.moves_applied(), 1);
    }
}
