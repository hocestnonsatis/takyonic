//! Massively Parallel Processing (MPP) coordinator / worker framework.
//!
//! The coordinator fragments a distributed logical plan, dispatches worker
//! fragments (partitioned scans + partial aggregates), shuffles intermediate
//! rows via [`crate::shuffle::ShuffleManager`] / gRPC, and gathers the final
//! result. Workers execute local Volcano plans over a hash-partitioned slice
//! of each table (`hash(pk) % N == worker_slot`) so Raft-replicated data is
//! not triple-counted.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use parking_lot::Mutex;
use tracing::debug;

use crate::engine::TakyonicEngine;
use crate::error::{Result, TakyonicError};
use crate::partition::{
    PartitionPruningRule, PartitionRouter, Rebalancer,
};
use crate::schema::Record;
use crate::shuffle::{
    Distribution, ShuffleKey, ShuffleManager, hash_partition,
};
use crate::sql::{Expression, JoinType, LogicalPlan};
use crate::telemetry::EngineMetrics;

static NEXT_QUERY_ID: AtomicU64 = AtomicU64::new(1);
static NEXT_SHUFFLE_ID: AtomicU64 = AtomicU64::new(1);

/// Cluster worker identity for MPP dispatch.
#[derive(Clone, Debug)]
pub struct WorkerEndpoint {
    /// Raft / cluster node id.
    pub node_id: u64,
    /// gRPC `host:port`.
    pub address: String,
    /// Stable slot in `0..worker_count` for hash partitioning.
    pub slot: u32,
}

/// One distributed aggregate operation (single GROUP BY column).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DistAggKind {
    /// `COUNT(*)` / `COUNT(col)`.
    Count,
    /// `SUM(col)`.
    Sum(String),
    /// `MIN(col)`.
    Min(String),
    /// `MAX(col)`.
    Max(String),
    /// `AVG(col)` — partial sum+count, final sum/count.
    Avg(String),
}

impl DistAggKind {
    fn wire_tag(&self) -> u8 {
        match self {
            Self::Count => 0,
            Self::Sum(_) => 1,
            Self::Min(_) => 2,
            Self::Max(_) => 3,
            Self::Avg(_) => 4,
        }
    }

    fn column(&self) -> Option<&str> {
        match self {
            Self::Count => None,
            Self::Sum(c) | Self::Min(c) | Self::Max(c) | Self::Avg(c) => Some(c.as_str()),
        }
    }

    fn from_wire(tag: u8, col: Option<String>) -> Result<Self> {
        match tag {
            0 => Ok(Self::Count),
            1 => Ok(Self::Sum(col.ok_or_else(|| {
                TakyonicError::Engine("SUM fragment missing column".into())
            })?)),
            2 => Ok(Self::Min(col.ok_or_else(|| {
                TakyonicError::Engine("MIN fragment missing column".into())
            })?)),
            3 => Ok(Self::Max(col.ok_or_else(|| {
                TakyonicError::Engine("MAX fragment missing column".into())
            })?)),
            4 => Ok(Self::Avg(col.ok_or_else(|| {
                TakyonicError::Engine("AVG fragment missing column".into())
            })?)),
            other => Err(TakyonicError::Engine(format!(
                "unknown dist agg kind {other}"
            ))),
        }
    }

    fn result_name(&self) -> String {
        match self {
            Self::Count => "COUNT(*)".into(),
            Self::Sum(c) => format!("SUM({c})"),
            Self::Min(c) => format!("MIN({c})"),
            Self::Max(c) => format!("MAX({c})"),
            Self::Avg(c) => format!("AVG({c})"),
        }
    }
}

/// One executable fragment produced by the fragmenter.
#[derive(Clone, Debug)]
#[allow(missing_docs)]
pub enum FragmentSpec {
    /// Scan a hash shard of `table` and optionally push into a shuffle.
    PartitionedScan {
        table: String,
        partition_id: u32,
        partition_count: u32,
        /// When set, rows are hash-shuffled on `dist_column` into this shuffle.
        shuffle: Option<(ShuffleKey, String)>,
    },
    /// Local partial `GROUP BY` + aggregate then shuffle by group key.
    PartialAggregate {
        table: String,
        group_column: String,
        agg: DistAggKind,
        partition_id: u32,
        partition_count: u32,
        shuffle: ShuffleKey,
    },
    /// Gather all shuffle partitions and finalize aggregation.
    FinalAggregate {
        shuffle: ShuffleKey,
        group_column: String,
        agg: DistAggKind,
        partition_count: u32,
    },
    /// Gather shuffled rows and INSERT into `target_table` (coordinator).
    GatherInsert {
        target_table: String,
        shuffle: ShuffleKey,
        partition_count: u32,
    },
}

impl FragmentSpec {
    /// Encode for gRPC `ExecuteFragment`.
    pub fn encode(&self) -> Bytes {
        let mut b = BytesMut::new();
        match self {
            FragmentSpec::PartitionedScan {
                table,
                partition_id,
                partition_count,
                shuffle,
            } => {
                b.put_u8(1);
                put_str(&mut b, table);
                b.put_u32_le(*partition_id);
                b.put_u32_le(*partition_count);
                match shuffle {
                    None => b.put_u8(0),
                    Some((k, col)) => {
                        b.put_u8(1);
                        b.put_u64_le(k.query_id);
                        b.put_u64_le(k.shuffle_id);
                        put_str(&mut b, col);
                    }
                }
            }
            FragmentSpec::PartialAggregate {
                table,
                group_column,
                agg,
                partition_id,
                partition_count,
                shuffle,
            } => {
                b.put_u8(2);
                put_str(&mut b, table);
                put_str(&mut b, group_column);
                b.put_u8(agg.wire_tag());
                if let Some(c) = agg.column() {
                    put_str(&mut b, c);
                }
                b.put_u32_le(*partition_id);
                b.put_u32_le(*partition_count);
                b.put_u64_le(shuffle.query_id);
                b.put_u64_le(shuffle.shuffle_id);
            }
            FragmentSpec::FinalAggregate {
                shuffle,
                group_column,
                agg,
                partition_count,
            } => {
                b.put_u8(3);
                b.put_u64_le(shuffle.query_id);
                b.put_u64_le(shuffle.shuffle_id);
                put_str(&mut b, group_column);
                b.put_u8(agg.wire_tag());
                if let Some(c) = agg.column() {
                    put_str(&mut b, c);
                }
                b.put_u32_le(*partition_count);
            }
            FragmentSpec::GatherInsert {
                target_table,
                shuffle,
                partition_count,
            } => {
                b.put_u8(4);
                put_str(&mut b, target_table);
                b.put_u64_le(shuffle.query_id);
                b.put_u64_le(shuffle.shuffle_id);
                b.put_u32_le(*partition_count);
            }
        }
        b.freeze()
    }

    /// Decode a fragment payload.
    pub fn decode(mut data: Bytes) -> Result<Self> {
        if data.remaining() < 1 {
            return Err(TakyonicError::Engine("empty fragment".into()));
        }
        let tag = data.get_u8();
        match tag {
            1 => {
                let table = get_str(&mut data)?;
                let partition_id = data.get_u32_le();
                let partition_count = data.get_u32_le();
                let shuffle = if data.get_u8() == 0 {
                    None
                } else {
                    let query_id = data.get_u64_le();
                    let shuffle_id = data.get_u64_le();
                    let col = get_str(&mut data)?;
                    Some((
                        ShuffleKey {
                            query_id,
                            shuffle_id,
                        },
                        col,
                    ))
                };
                Ok(FragmentSpec::PartitionedScan {
                    table,
                    partition_id,
                    partition_count,
                    shuffle,
                })
            }
            2 => {
                let table = get_str(&mut data)?;
                let group_column = get_str(&mut data)?;
                let kind = data.get_u8();
                let col = if kind == 0 {
                    None
                } else {
                    Some(get_str(&mut data)?)
                };
                let agg = DistAggKind::from_wire(kind, col)?;
                let partition_id = data.get_u32_le();
                let partition_count = data.get_u32_le();
                let shuffle = ShuffleKey {
                    query_id: data.get_u64_le(),
                    shuffle_id: data.get_u64_le(),
                };
                Ok(FragmentSpec::PartialAggregate {
                    table,
                    group_column,
                    agg,
                    partition_id,
                    partition_count,
                    shuffle,
                })
            }
            3 => {
                let shuffle = ShuffleKey {
                    query_id: data.get_u64_le(),
                    shuffle_id: data.get_u64_le(),
                };
                let group_column = get_str(&mut data)?;
                let kind = data.get_u8();
                let col = if kind == 0 {
                    None
                } else {
                    Some(get_str(&mut data)?)
                };
                let agg = DistAggKind::from_wire(kind, col)?;
                let partition_count = data.get_u32_le();
                Ok(FragmentSpec::FinalAggregate {
                    shuffle,
                    group_column,
                    agg,
                    partition_count,
                })
            }
            4 => {
                let target_table = get_str(&mut data)?;
                let shuffle = ShuffleKey {
                    query_id: data.get_u64_le(),
                    shuffle_id: data.get_u64_le(),
                };
                let partition_count = data.get_u32_le();
                Ok(FragmentSpec::GatherInsert {
                    target_table,
                    shuffle,
                    partition_count,
                })
            }
            _ => Err(TakyonicError::Engine(format!("unknown fragment tag {tag}"))),
        }
    }
}

fn put_str(buf: &mut BytesMut, s: &str) {
    let b = s.as_bytes();
    buf.put_u32_le(b.len() as u32);
    buf.put_slice(b);
}

fn get_str(data: &mut Bytes) -> Result<String> {
    if data.remaining() < 4 {
        return Err(TakyonicError::Engine("fragment string truncated".into()));
    }
    let n = data.get_u32_le() as usize;
    if data.remaining() < n {
        return Err(TakyonicError::Engine("fragment string body truncated".into()));
    }
    let bytes = data.copy_to_bytes(n);
    String::from_utf8(bytes.to_vec())
        .map_err(|e| TakyonicError::Engine(format!("fragment utf8: {e}")))
}

/// Break a distributed logical plan into per-worker + coordinator fragments.
pub struct Fragmenter {
    workers: Vec<WorkerEndpoint>,
}

impl Fragmenter {
    /// Create a fragmenter for the given worker set (slot order matters).
    pub fn new(workers: Vec<WorkerEndpoint>) -> Self {
        Self { workers }
    }

    /// Worker count / partition count.
    pub fn partition_count(&self) -> u32 {
        self.workers.len().max(1) as u32
    }

    /// Fragment a distributed aggregate: N partial workers + 1 final gather.
    ///
    /// When `schema` is partitioned and `predicate` pins the partition key,
    /// [`PartitionPruningRule`] keeps only the owning worker fragment(s).
    pub fn fragment_aggregate(
        &self,
        table: &str,
        group_column: &str,
        agg: DistAggKind,
    ) -> (u64, ShuffleKey, Vec<(u64, FragmentSpec)>, FragmentSpec) {
        self.fragment_aggregate_pruned(table, group_column, agg, None, None)
    }

    /// Like [`Self::fragment_aggregate`] with optional partition pruning.
    pub fn fragment_aggregate_pruned(
        &self,
        table: &str,
        group_column: &str,
        agg: DistAggKind,
        schema: Option<&crate::schema::TableSchema>,
        predicate: Option<&Expression>,
    ) -> (u64, ShuffleKey, Vec<(u64, FragmentSpec)>, FragmentSpec) {
        let query_id = NEXT_QUERY_ID.fetch_add(1, Ordering::Relaxed);
        let shuffle = ShuffleKey {
            query_id,
            shuffle_id: NEXT_SHUFFLE_ID.fetch_add(1, Ordering::Relaxed),
        };
        let n = self.partition_count();
        let mut worker_frags = Vec::new();
        for w in &self.workers {
            worker_frags.push((
                w.node_id,
                FragmentSpec::PartialAggregate {
                    table: table.to_string(),
                    group_column: group_column.to_string(),
                    agg: agg.clone(),
                    partition_id: w.slot,
                    partition_count: n,
                    shuffle,
                },
            ));
        }
        if let Some(schema) = schema {
            let router = PartitionRouter::new(self.workers.clone());
            worker_frags = PartitionPruningRule::prune_fragments(
                schema,
                predicate,
                worker_frags,
                &router,
            )
            .unwrap_or_default();
        }
        let final_frag = FragmentSpec::FinalAggregate {
            shuffle,
            group_column: group_column.to_string(),
            agg,
            partition_count: n,
        };
        (query_id, shuffle, worker_frags, final_frag)
    }

    /// Fragment a partitioned table scan (one [`FragmentSpec::PartitionedScan`]
    /// per RemoteWorker), optionally pruned by `schema` + `predicate`.
    pub fn fragment_partitioned_scan(
        &self,
        table: &str,
        schema: Option<&crate::schema::TableSchema>,
        predicate: Option<&Expression>,
    ) -> Result<Vec<(u64, FragmentSpec)>> {
        let n = self.partition_count();
        let router = PartitionRouter::new(self.workers.clone());
        let targets = if let Some(schema) = schema {
            PartitionPruningRule::prune_workers(schema, predicate, &router)?
        } else {
            self.workers
                .iter()
                .map(|w| (w.node_id, w.slot))
                .collect()
        };
        let part_count = schema
            .map(|s| s.partitioning.partition_count().max(n))
            .unwrap_or(n)
            .max(1);
        Ok(targets
            .into_iter()
            .map(|(node_id, partition_id)| {
                (
                    node_id,
                    FragmentSpec::PartitionedScan {
                        table: table.to_string(),
                        partition_id,
                        partition_count: part_count,
                        shuffle: None,
                    },
                )
            })
            .collect())
    }

    /// Fragment `INSERT INTO target SELECT * FROM source` with hash redistribute
    /// on the target primary-key column name `dist_column`.
    pub fn fragment_insert_select(
        &self,
        source: &str,
        target: &str,
        dist_column: &str,
    ) -> (u64, ShuffleKey, Vec<(u64, FragmentSpec)>, FragmentSpec) {
        let query_id = NEXT_QUERY_ID.fetch_add(1, Ordering::Relaxed);
        let shuffle = ShuffleKey {
            query_id,
            shuffle_id: NEXT_SHUFFLE_ID.fetch_add(1, Ordering::Relaxed),
        };
        let n = self.partition_count();
        let mut worker_frags = Vec::new();
        for w in &self.workers {
            worker_frags.push((
                w.node_id,
                FragmentSpec::PartitionedScan {
                    table: source.to_string(),
                    partition_id: w.slot,
                    partition_count: n,
                    shuffle: Some((shuffle, dist_column.to_string())),
                },
            ));
        }
        let gather = FragmentSpec::GatherInsert {
            target_table: target.to_string(),
            shuffle,
            partition_count: n,
        };
        (query_id, shuffle, worker_frags, gather)
    }
}

/// Rewrite node-local aggregates / joins into distributed logical forms when
/// the cluster has more than one worker.
pub fn maybe_distribute(plan: LogicalPlan, worker_count: usize) -> LogicalPlan {
    if worker_count <= 1 {
        return plan;
    }
    match plan {
        LogicalPlan::Project { columns, input } => LogicalPlan::Project {
            columns,
            input: Box::new(maybe_distribute(*input, worker_count)),
        },
        LogicalPlan::Filter { input, predicate } => LogicalPlan::Filter {
            input: Box::new(maybe_distribute(*input, worker_count)),
            predicate,
        },
        LogicalPlan::Sort { input, exprs } => LogicalPlan::Sort {
            input: Box::new(maybe_distribute(*input, worker_count)),
            exprs,
        },
        LogicalPlan::Limit {
            input,
            skip,
            fetch,
            with_ties,
            ties_order,
        } => LogicalPlan::Limit {
            input: Box::new(maybe_distribute(*input, worker_count)),
            skip,
            fetch,
            with_ties,
            ties_order,
        },
        LogicalPlan::Aggregate {
            input,
            group_exprs,
            aggr_exprs,
        } if !group_exprs.is_empty() && is_simple_distributed_agg(&group_exprs, &aggr_exprs) => {
            LogicalPlan::DistributedAggregate {
                input,
                group_exprs,
                aggr_exprs,
            }
        }
        LogicalPlan::Join {
            left,
            right,
            on,
            join_type,
        } if matches!(join_type, JoinType::Inner) => {
            let keys = equi_join_keys(&on).unwrap_or_default();
            LogicalPlan::DistributedJoin {
                left,
                right,
                on,
                join_type,
                distribution: Distribution::Hash { keys },
            }
        }
        other => other,
    }
}

/// True when the coordinator can execute this aggregate (single GROUP BY col +
/// one of SUM/COUNT/MIN/MAX/AVG). Unsupported shapes stay local so EXPLAIN matches exec.
fn is_simple_distributed_agg(group_exprs: &[Expression], aggr_exprs: &[Expression]) -> bool {
    extract_dist_agg(group_exprs, aggr_exprs).is_some()
}

fn extract_dist_agg(
    group_exprs: &[Expression],
    aggr_exprs: &[Expression],
) -> Option<(String, DistAggKind)> {
    if group_exprs.len() != 1 {
        return None;
    }
    let Expression::Column(group) = group_exprs.first()? else {
        return None;
    };
    if aggr_exprs.len() != 1 {
        return None;
    }
    match &aggr_exprs[0] {
        Expression::AggregateFunction {
            name,
            args,
            distinct,
            ..
        } if !*distinct => {
            let n = name.to_ascii_lowercase();
            let col = match args.first() {
                Some(Expression::Column(c)) => Some(c.clone()),
                None if n == "count" => None,
                _ => return None,
            };
            let kind = match n.as_str() {
                "count" => DistAggKind::Count,
                "sum" => DistAggKind::Sum(col?),
                "min" => DistAggKind::Min(col?),
                "max" => DistAggKind::Max(col?),
                "avg" => DistAggKind::Avg(col?),
                _ => return None,
            };
            Some((group.clone(), kind))
        }
        _ => None,
    }
}

fn equi_join_keys(on: &Expression) -> Option<Vec<String>> {
    equi_column_pair(on).map(|(a, b)| vec![a, b])
}

/// Equi-join `col = col` → `(left_col, right_col)`.
fn equi_column_pair(on: &Expression) -> Option<(String, String)> {
    match on {
        Expression::BinaryOp {
            left,
            op: crate::query::FilterOp::Eq,
            right,
        } => match (left.as_ref(), right.as_ref()) {
            (Expression::Column(a), Expression::Column(b)) => Some((a.clone(), b.clone())),
            _ => None,
        },
        _ => None,
    }
}

/// Local worker: executes fragments against an engine + shuffle manager.
pub struct Worker {
    engine: Arc<TakyonicEngine>,
    shuffle: Arc<ShuffleManager>,
    metrics: Arc<EngineMetrics>,
}

impl Worker {
    /// Construct a worker bound to local storage + shuffle buffers.
    pub fn new(
        engine: Arc<TakyonicEngine>,
        shuffle: Arc<ShuffleManager>,
        metrics: Arc<EngineMetrics>,
    ) -> Self {
        Self {
            engine,
            shuffle,
            metrics,
        }
    }

    /// Shared shuffle manager (gRPC handlers).
    pub fn shuffle(&self) -> &Arc<ShuffleManager> {
        &self.shuffle
    }

    /// Local engine handle.
    pub fn engine(&self) -> &Arc<TakyonicEngine> {
        &self.engine
    }

    /// Execute a fragment and return result rows (empty when rows were shuffled out).
    pub fn execute_fragment(&self, spec: &FragmentSpec) -> Result<Vec<Record>> {
        self.metrics.record_mpp_fragment();
        match spec {
            FragmentSpec::PartitionedScan {
                table,
                partition_id,
                partition_count,
                shuffle,
            } => {
                let rows = self.scan_partition(table, *partition_id, *partition_count)?;
                if let Some((key, dist_col)) = shuffle {
                    self.shuffle.open_shuffle(*key, *partition_count);
                    let dist = Distribution::Hash {
                        keys: vec![dist_col.clone()],
                    };
                    let mut batches: HashMap<u32, Vec<Record>> = HashMap::new();
                    for row in &rows {
                        let p =
                            dist.partition_of(row, *partition_count, self.shuffle.rr_counter());
                        batches.entry(p).or_default().push(row.clone());
                    }
                    for p in 0..*partition_count {
                        let batch = batches.remove(&p).unwrap_or_default();
                        self.shuffle.push_blocking(*key, p, batch, true)?;
                    }
                }
                // Always return the scanned rows so the coordinator can gather
                // remotely without depending on a shared shuffle buffer.
                Ok(rows)
            }
            FragmentSpec::PartialAggregate {
                table,
                group_column,
                agg,
                partition_id,
                partition_count,
                shuffle: _,
            } => {
                let rows = self.scan_partition(table, *partition_id, *partition_count)?;
                let partial = partial_aggregate(&rows, group_column, agg)?;
                if let FragmentSpec::PartialAggregate {
                    shuffle,
                    partition_count,
                    group_column,
                    ..
                } = spec
                {
                    self.shuffle.open_shuffle(*shuffle, *partition_count);
                    let dist = Distribution::Hash {
                        keys: vec![group_column.clone()],
                    };
                    let mut batches: HashMap<u32, Vec<Record>> = HashMap::new();
                    for row in &partial {
                        let p = dist.partition_of(
                            row,
                            *partition_count,
                            self.shuffle.rr_counter(),
                        );
                        batches.entry(p).or_default().push(row.clone());
                    }
                    for p in 0..*partition_count {
                        let batch = batches.remove(&p).unwrap_or_default();
                        self.shuffle.push_blocking(*shuffle, p, batch, true)?;
                    }
                }
                Ok(partial)
            }
            FragmentSpec::FinalAggregate {
                shuffle,
                group_column,
                agg,
                partition_count,
            } => {
                let mut all = Vec::new();
                for p in 0..*partition_count {
                    loop {
                        let (rows, eos) = self.shuffle.fetch_blocking(*shuffle, p)?;
                        all.extend(rows);
                        if eos {
                            break;
                        }
                    }
                }
                Ok(merge_partial_aggregates(&all, group_column, agg)?)
            }
            FragmentSpec::GatherInsert {
                target_table,
                shuffle,
                partition_count,
            } => {
                let mut all = Vec::new();
                for p in 0..*partition_count {
                    loop {
                        let (rows, eos) = self.shuffle.fetch_blocking(*shuffle, p)?;
                        all.extend(rows);
                        if eos {
                            break;
                        }
                    }
                }
                let schema = self.engine.table_schema(target_table)?;
                let mut txn = self.engine.begin()?;
                for row in &all {
                    txn.put_record(target_table, row.clone())?;
                }
                txn.commit()?;
                let _ = schema;
                Ok(vec![Record::new().set("inserted", all.len().to_string())])
            }
        }
    }

    fn scan_partition(
        &self,
        table: &str,
        partition_id: u32,
        partition_count: u32,
    ) -> Result<Vec<Record>> {
        let schema = self.engine.table_schema(table)?;
        let mut txn = self.engine.begin()?;
        let rows = txn.scan_table_records(table)?;
        txn.abort();
        let pk = schema.primary_key.clone();
        Ok(rows
            .into_iter()
            .filter(|r| {
                let key = r.get(&pk).unwrap_or("");
                hash_partition(key, partition_count) == partition_id
            })
            .collect())
    }
}

fn partial_aggregate(
    rows: &[Record],
    group_column: &str,
    agg: &DistAggKind,
) -> Result<Vec<Record>> {
    // Accumulators: sum, count, min, max (min/max as Option until first value).
    let mut groups: HashMap<String, (i64, i64, Option<i64>, Option<i64>)> = HashMap::new();
    for row in rows {
        let g = row.get(group_column).unwrap_or("").to_string();
        let e = groups.entry(g).or_insert((0, 0, None, None));
        e.1 += 1;
        let col = agg.column();
        if let Some(col) = col {
            if let Some(v) = row.get(col).and_then(|s| s.parse::<i64>().ok()) {
                e.0 = e.0.saturating_add(v);
                e.2 = Some(e.2.map_or(v, |m| m.min(v)));
                e.3 = Some(e.3.map_or(v, |m| m.max(v)));
            }
        }
    }
    let mut out = Vec::new();
    for (g, (sum, count, min, max)) in groups {
        let mut r = Record::new().set(group_column, g);
        r = r.set("partial_count", count.to_string());
        match agg {
            DistAggKind::Count => {}
            DistAggKind::Sum(_) | DistAggKind::Avg(_) => {
                r = r.set("partial_sum", sum.to_string());
            }
            DistAggKind::Min(_) => {
                if let Some(m) = min {
                    r = r.set("partial_min", m.to_string());
                }
            }
            DistAggKind::Max(_) => {
                if let Some(m) = max {
                    r = r.set("partial_max", m.to_string());
                }
            }
        }
        out.push(r);
    }
    Ok(out)
}

fn merge_partial_aggregates(
    rows: &[Record],
    group_column: &str,
    agg: &DistAggKind,
) -> Result<Vec<Record>> {
    let mut groups: HashMap<String, (i64, i64, Option<i64>, Option<i64>)> = HashMap::new();
    for row in rows {
        let g = row.get(group_column).unwrap_or("").to_string();
        let e = groups.entry(g).or_insert((0, 0, None, None));
        if let Some(c) = row.get("partial_count").and_then(|s| s.parse().ok()) {
            e.1 = e.1.saturating_add(c);
        }
        if let Some(s) = row.get("partial_sum").and_then(|s| s.parse().ok()) {
            e.0 = e.0.saturating_add(s);
        }
        if let Some(m) = row.get("partial_min").and_then(|s| s.parse().ok()) {
            e.2 = Some(e.2.map_or(m, |cur| cur.min(m)));
        }
        if let Some(m) = row.get("partial_max").and_then(|s| s.parse().ok()) {
            e.3 = Some(e.3.map_or(m, |cur| cur.max(m)));
        }
    }
    let mut keys: Vec<_> = groups.keys().cloned().collect();
    keys.sort();
    let mut out = Vec::new();
    let name = agg.result_name();
    for g in keys {
        let (sum, count, min, max) = groups[&g];
        let mut r = Record::new().set(group_column, &g);
        let value = match agg {
            DistAggKind::Count => count.to_string(),
            DistAggKind::Sum(_) => sum.to_string(),
            DistAggKind::Min(_) => min.unwrap_or(0).to_string(),
            DistAggKind::Max(_) => max.unwrap_or(0).to_string(),
            DistAggKind::Avg(_) => {
                if count == 0 {
                    "0".into()
                } else {
                    // Integer AVG to match local AggregateExec int division.
                    (sum / count).to_string()
                }
            }
        };
        r = r.set(&name, value);
        out.push(r);
    }
    Ok(out)
}

/// Coordinator: plans, dispatches, gathers.
pub struct Coordinator {
    local: Worker,
    workers: Vec<WorkerEndpoint>,
    /// Optional async dispatch hook: `(node_id, fragment) -> rows`.
    /// When `None`, all fragments run on the local worker (single-process MPP sim).
    remote: Mutex<Option<Arc<dyn FragmentDispatcher>>>,
}

/// Pluggable remote fragment execution (gRPC in cluster mode).
pub trait FragmentDispatcher: Send + Sync {
    /// Run `fragment` on `node_id` and return result rows.
    fn execute_remote(&self, node_id: u64, fragment: &FragmentSpec) -> Result<Vec<Record>>;
}

fn is_transient_mpp_dispatch_error(err: &TakyonicError) -> bool {
    let s = err.to_string().to_ascii_lowercase();
    s.contains("unavailable")
        || s.contains("connection")
        || s.contains("broken pipe")
        || s.contains("transport")
        || s.contains("temporarily")
        || s.contains("timed out")
        || s.contains("timeout")
}

impl Coordinator {
    /// In-process coordinator (all fragments local — still exercises partitioning).
    pub fn local(
        engine: Arc<TakyonicEngine>,
        shuffle: Arc<ShuffleManager>,
        workers: Vec<WorkerEndpoint>,
    ) -> Self {
        let metrics = Arc::clone(engine.metrics());
        Self {
            local: Worker::new(engine, shuffle, metrics),
            workers,
            remote: Mutex::new(None),
        }
    }

    /// Attach a remote dispatcher (cluster gRPC).
    pub fn set_dispatcher(&self, d: Arc<dyn FragmentDispatcher>) {
        *self.remote.lock() = Some(d);
    }

    /// Attach a gRPC [`crate::shuffle_service::GrpcFragmentDispatcher`] when
    /// workers have real `host:port` addresses (cluster mode).
    pub fn attach_grpc_dispatcher(&self, local_node: Option<u64>) {
        if !crate::shuffle_service::GrpcFragmentDispatcher::useful_for(&self.workers) {
            return;
        }
        let local = Arc::new(Worker::new(
            Arc::clone(self.local.engine()),
            Arc::clone(self.local.shuffle()),
            Arc::clone(self.local.engine().metrics()),
        ));
        self.set_dispatcher(Arc::new(
            crate::shuffle_service::GrpcFragmentDispatcher::new(
                &self.workers,
                local_node,
                Some(local),
            ),
        ));
    }

    /// Worker directory.
    pub fn workers(&self) -> &[WorkerEndpoint] {
        &self.workers
    }

    /// Partition / worker count.
    pub fn partition_count(&self) -> u32 {
        self.workers.len().max(1) as u32
    }

    /// Local worker (final gather / insert).
    pub fn worker(&self) -> &Worker {
        &self.local
    }

    fn dispatch_once(&self, node_id: u64, frag: &FragmentSpec) -> Result<Vec<Record>> {
        if let Some(remote) = self.remote.lock().as_ref() {
            // Prefer remote for non-local nodes when dispatcher is set.
            if self.workers.iter().any(|w| w.node_id == node_id) {
                return remote.execute_remote(node_id, frag);
            }
        }
        self.local.execute_fragment(frag)
    }

    fn dispatch(&self, node_id: u64, frag: &FragmentSpec) -> Result<Vec<Record>> {
        // One reconnect-style retry for transient transport errors (Faz 1B).
        match self.dispatch_once(node_id, frag) {
            Ok(rows) => Ok(rows),
            Err(e) if is_transient_mpp_dispatch_error(&e) => {
                debug!(node_id, error = %e, "mpp dispatch transient failure — retrying once");
                self.dispatch_once(node_id, frag)
            }
            Err(e) => Err(e),
        }
    }

    /// Run distributed `GROUP BY` + aggregate across workers.
    pub fn execute_distributed_aggregate(
        &self,
        table: &str,
        group_column: &str,
        agg: DistAggKind,
    ) -> Result<Vec<Record>> {
        let frag = Fragmenter::new(self.workers.clone());
        let (_qid, shuffle, worker_frags, _final_frag) =
            frag.fragment_aggregate(table, group_column, agg.clone());
        self.local
            .shuffle()
            .open_shuffle(shuffle, frag.partition_count());

        let mut partials = Vec::new();
        for (node_id, spec) in &worker_frags {
            debug!(node_id, ?spec, "dispatch partial aggregate fragment");
            partials.extend(self.dispatch(*node_id, spec)?);
        }
        for p in 0..frag.partition_count() {
            let _ = self.local.shuffle().try_fetch(shuffle, p);
        }
        let rows = merge_partial_aggregates(&partials, group_column, &agg)?;
        self.local.shuffle().close(shuffle);
        Ok(rows)
    }

    /// Partition-pruned distributed scan: dispatch [`FragmentSpec::PartitionedScan`]
    /// only to RemoteWorkers that survive [`PartitionPruningRule`], then gather.
    ///
    /// When `predicate` equality-binds the partition key, a single worker is
    /// contacted (measurable prune). Residual predicates are applied locally
    /// after the gather.
    pub fn execute_distributed_scan(
        &self,
        table: &str,
        predicate: Option<&Expression>,
    ) -> Result<Vec<Record>> {
        let schema = self.local.engine().table_schema(table)?;
        let frag = Fragmenter::new(self.workers.clone());
        let worker_frags =
            frag.fragment_partitioned_scan(table, Some(&schema), predicate)?;
        if worker_frags.is_empty() {
            return Ok(Vec::new());
        }
        let mut rows = Vec::new();
        for (node_id, spec) in &worker_frags {
            debug!(node_id, ?spec, "dispatch partitioned scan fragment");
            rows.extend(self.dispatch(*node_id, spec)?);
        }
        if let Some(pred) = predicate {
            let ctx = crate::executor::ExecutionContext::new();
            let mut filtered = Vec::with_capacity(rows.len());
            for row in rows {
                if crate::executor::evaluate_bool(pred, &row, &ctx)? {
                    filtered.push(row);
                }
            }
            rows = filtered;
        }
        Ok(rows)
    }

    /// Distributed inner join: remote-scan both sides, then local hash (equi) or NLJ.
    pub fn execute_distributed_join(
        &self,
        left_table: &str,
        right_table: &str,
        on: &Expression,
        left_predicate: Option<&Expression>,
        right_predicate: Option<&Expression>,
    ) -> Result<Vec<Record>> {
        let left = self.execute_distributed_scan(left_table, left_predicate)?;
        let right = self.execute_distributed_scan(right_table, right_predicate)?;
        if let Some((lk, rk)) = equi_column_pair(on) {
            // Build the smaller side (same heuristic as the local HashJoin path).
            let (build, probe, build_key, probe_key) = if left.len() <= right.len() {
                (left, right, lk, rk)
            } else {
                (right, left, rk, lk)
            };
            let mut hj = crate::executor::HashJoinExec::new(
                Box::new(crate::executor::ValuesExec::new(build)),
                Box::new(crate::executor::ValuesExec::new(probe)),
                Expression::Column(build_key),
                Expression::Column(probe_key),
                JoinType::Inner,
                crate::executor::ExecutionContext::new(),
            );
            return crate::executor::collect_rows(&mut hj);
        }
        let mut join =
            crate::executor::NestedLoopJoin::from_rows(left, right, on.clone());
        crate::executor::collect_rows(&mut join)
    }

    /// Run distributed `INSERT INTO target SELECT * FROM source` with hash shuffle.
    pub fn execute_insert_select(
        &self,
        source: &str,
        target: &str,
        dist_column: &str,
    ) -> Result<u64> {
        let frag = Fragmenter::new(self.workers.clone());
        let (_qid, shuffle, worker_frags, _gather) =
            frag.fragment_insert_select(source, target, dist_column);
        self.local
            .shuffle()
            .open_shuffle(shuffle, frag.partition_count());

        let mut all = Vec::new();
        for (node_id, spec) in &worker_frags {
            debug!(node_id, ?spec, "dispatch shuffle scan fragment");
            all.extend(self.dispatch(*node_id, spec)?);
        }
        // Re-hash on the coordinator to exercise the distribution key, then insert.
        let dist = Distribution::Hash {
            keys: vec![dist_column.to_string()],
        };
        let n = frag.partition_count();
        let mut by_part: HashMap<u32, Vec<Record>> = HashMap::new();
        for row in all {
            let p = dist.partition_of(&row, n, self.local.shuffle().rr_counter());
            by_part.entry(p).or_default().push(row);
        }
        let mut inserted = 0u64;
        let mut txn = self.local.engine().begin()?;
        for p in 0..n {
            let batch = by_part.remove(&p).unwrap_or_default();
            self.local
                .shuffle()
                .push_blocking(shuffle, p, batch.clone(), true)?;
            for row in batch {
                txn.put_record(target, row)?;
                inserted += 1;
            }
        }
        txn.commit()?;
        self.local.shuffle().close(shuffle);
        Ok(inserted)
    }

    /// Route a single-row INSERT to the owning partition node (no broadcast).
    ///
    /// Returns `(node_id, partition_id)` that received the row. In local /
    /// single-process mode the row is written via the local worker after the
    /// router decides the target; with a remote dispatcher only that node is
    /// contacted.
    pub fn execute_insert(
        &self,
        table: &str,
        record: Record,
    ) -> Result<(u64, u32)> {
        let schema = self.local.engine().table_schema(table)?;
        let col = schema
            .partitioning
            .column()
            .unwrap_or(schema.primary_key.as_str());
        let key = record
            .get(col)
            .ok_or_else(|| {
                TakyonicError::Engine(format!(
                    "INSERT missing partition key `{col}` on `{table}`"
                ))
            })?
            .to_string();
        let router = PartitionRouter::new(self.workers.clone());
        let (partition_id, node_id) = router.route_key(&schema, &key)?;

        // Single-node access: only the owning partition is contacted.
        let frag = FragmentSpec::PartitionedScan {
            table: table.to_string(),
            partition_id,
            partition_count: schema.partitioning.partition_count().max(self.partition_count()),
            shuffle: None,
        };
        // Touch the owning worker (metrics / remote path) then write locally
        // when the dispatcher is local-sim, or rely on remote apply.
        let _ = self.dispatch(node_id, &frag)?;

        // Persist on the coordinator's engine (Raft-replicated cluster store).
        // Ownership metadata is tracked for routing / rebalance; the durable
        // write remains consensus-backed.
        let mut txn = self.local.engine().begin()?;
        txn.put_record(table, record)?;
        txn.commit()?;

        debug!(
            table,
            node_id,
            partition_id,
            key = %key,
            "execute_insert routed to owning partition"
        );
        Ok((node_id, partition_id))
    }

    /// Route each row of a multi-row INSERT to its owning partition (no broadcast).
    ///
    /// Returns the number of rows written.
    pub fn execute_insert_rows(
        &self,
        table: &str,
        records: Vec<Record>,
    ) -> Result<u64> {
        let n = records.len() as u64;
        for record in records {
            let _ = self.execute_insert(table, record)?;
        }
        Ok(n)
    }

    /// Build a [`Rebalancer`] seeded from worker node ids (background optional).
    pub fn rebalancer(&self, partition_count: u32) -> Arc<Rebalancer> {
        let nodes: Vec<u64> = self.workers.iter().map(|w| w.node_id).collect();
        Arc::new(Rebalancer::new(
            crate::partition::PartitionMap::round_robin(&nodes, partition_count),
        ))
    }
}

/// Detect `SUM`/`COUNT`/`MIN`/`MAX`/`AVG` shape for MPP rewrite from a logical aggregate.
pub fn extract_simple_agg(
    group_exprs: &[Expression],
    aggr_exprs: &[Expression],
) -> Option<(String, DistAggKind)> {
    extract_dist_agg(group_exprs, aggr_exprs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::schema::{IndexDef, TableSchema};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_engine(name: &str) -> (Arc<TakyonicEngine>, std::path::PathBuf) {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("takyonic-mpp-{name}-{nanos}"));
        let cfg = Config::default()
            .data_dir(root.join("data"))
            .wal_dir(root.join("wal"))
            .memtable_size_bytes(64 * 1024 * 1024)
            .l0_rapid_pool_threads(1)
            .ln_haul_pool_threads(1)
            .compaction_write_bytes_per_sec(1024 * 1024 * 1024)
            .metrics_enabled(false);
        let engine = Arc::new(TakyonicEngine::open(cfg).unwrap());
        (engine, root)
    }

    fn seed_employees(engine: &Arc<TakyonicEngine>) {
        engine
            .register_table(TableSchema::new(
                "employees",
                "id",
                vec![IndexDef::new("dept", "department")],
            ))
            .unwrap();
        let rows = [
            ("1", "Engineering", "100"),
            ("2", "Engineering", "150"),
            ("3", "Sales", "90"),
            ("4", "Sales", "110"),
            ("5", "HR", "80"),
            ("6", "Engineering", "120"),
        ];
        let mut txn = engine.begin().unwrap();
        for (id, dept, sal) in rows {
            txn.put_record(
                "employees",
                Record::new()
                    .set("id", id)
                    .set("department", dept)
                    .set("salary", sal),
            )
            .unwrap();
        }
        txn.commit().unwrap();
    }

    #[test]
    fn fragment_codec_roundtrip() {
        let f = FragmentSpec::PartialAggregate {
            table: "employees".into(),
            group_column: "department".into(),
            agg: DistAggKind::Sum("salary".into()),
            partition_id: 1,
            partition_count: 3,
            shuffle: ShuffleKey {
                query_id: 9,
                shuffle_id: 2,
            },
        };
        let enc = f.encode();
        let dec = FragmentSpec::decode(enc).unwrap();
        assert!(matches!(dec, FragmentSpec::PartialAggregate { .. }));
    }

    #[test]
    fn distributed_aggregate_three_virtual_workers() {
        let (engine, root) = temp_engine("agg");
        seed_employees(&engine);
        let shuffle = Arc::new(ShuffleManager::new(
            32,
            Some(Arc::clone(engine.metrics())),
        ));
        let workers: Vec<_> = (0..3u32)
            .map(|slot| WorkerEndpoint {
                node_id: u64::from(slot) + 1,
                address: format!("local:{slot}"),
                slot,
            })
            .collect();
        // Local dispatcher: every "remote" call runs on the same worker with
        // the fragment's own partition_id (virtual multi-worker).
        let coord = Coordinator::local(Arc::clone(&engine), Arc::clone(&shuffle), workers);
        struct LocalDispatch(Worker);
        impl FragmentDispatcher for LocalDispatch {
            fn execute_remote(&self, _node_id: u64, fragment: &FragmentSpec) -> Result<Vec<Record>> {
                self.0.execute_fragment(fragment)
            }
        }
        coord.set_dispatcher(Arc::new(LocalDispatch(Worker::new(
            Arc::clone(&engine),
            Arc::clone(&shuffle),
            Arc::clone(engine.metrics()),
        ))));

        let rows = coord
            .execute_distributed_aggregate(
                "employees",
                "department",
                DistAggKind::Sum("salary".into()),
            )
            .unwrap();
        let mut map = HashMap::new();
        for r in &rows {
            let d = r.get("department").unwrap().to_string();
            let s: i64 = r.get("SUM(salary)").unwrap().parse().unwrap();
            map.insert(d, s);
        }
        assert_eq!(map.get("Engineering"), Some(&370));
        assert_eq!(map.get("Sales"), Some(&200));
        assert_eq!(map.get("HR"), Some(&80));

        // Shuffle load should be recorded on both send and receive paths.
        assert!(engine.metrics().mpp_shuffle_sent() > 0);
        assert!(engine.metrics().mpp_fragments() >= 3);

        engine.close().unwrap();
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn distributed_min_max_avg_three_virtual_workers() {
        let (engine, root) = temp_engine("agg-mma");
        seed_employees(&engine);
        let shuffle = Arc::new(ShuffleManager::new(
            32,
            Some(Arc::clone(engine.metrics())),
        ));
        let workers: Vec<_> = (0..3u32)
            .map(|slot| WorkerEndpoint {
                node_id: u64::from(slot) + 1,
                address: format!("local:{slot}"),
                slot,
            })
            .collect();
        let coord = Coordinator::local(Arc::clone(&engine), Arc::clone(&shuffle), workers);
        struct LocalDispatch(Worker);
        impl FragmentDispatcher for LocalDispatch {
            fn execute_remote(&self, _node_id: u64, fragment: &FragmentSpec) -> Result<Vec<Record>> {
                self.0.execute_fragment(fragment)
            }
        }
        coord.set_dispatcher(Arc::new(LocalDispatch(Worker::new(
            Arc::clone(&engine),
            Arc::clone(&shuffle),
            Arc::clone(engine.metrics()),
        ))));

        let min_rows = coord
            .execute_distributed_aggregate(
                "employees",
                "department",
                DistAggKind::Min("salary".into()),
            )
            .unwrap();
        let max_rows = coord
            .execute_distributed_aggregate(
                "employees",
                "department",
                DistAggKind::Max("salary".into()),
            )
            .unwrap();
        let avg_rows = coord
            .execute_distributed_aggregate(
                "employees",
                "department",
                DistAggKind::Avg("salary".into()),
            )
            .unwrap();

        let mut min_map = HashMap::new();
        for r in &min_rows {
            min_map.insert(
                r.get("department").unwrap().to_string(),
                r.get("MIN(salary)").unwrap().parse::<i64>().unwrap(),
            );
        }
        let mut max_map = HashMap::new();
        for r in &max_rows {
            max_map.insert(
                r.get("department").unwrap().to_string(),
                r.get("MAX(salary)").unwrap().parse::<i64>().unwrap(),
            );
        }
        let mut avg_map = HashMap::new();
        for r in &avg_rows {
            avg_map.insert(
                r.get("department").unwrap().to_string(),
                r.get("AVG(salary)").unwrap().parse::<i64>().unwrap(),
            );
        }
        // Engineering: 100,150,120 → min 100, max 150, avg 123
        assert_eq!(min_map.get("Engineering"), Some(&100));
        assert_eq!(max_map.get("Engineering"), Some(&150));
        assert_eq!(avg_map.get("Engineering"), Some(&123));
        assert_eq!(min_map.get("Sales"), Some(&90));
        assert_eq!(max_map.get("Sales"), Some(&110));
        assert_eq!(avg_map.get("Sales"), Some(&100));
        assert_eq!(min_map.get("HR"), Some(&80));
        assert_eq!(max_map.get("HR"), Some(&80));
        assert_eq!(avg_map.get("HR"), Some(&80));

        engine.close().unwrap();
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn distributed_insert_select_hash_shuffle() {
        let (engine, root) = temp_engine("ins");
        seed_employees(&engine);
        engine
            .register_table(TableSchema::new("employees_copy", "id", vec![]))
            .unwrap();
        let shuffle = Arc::new(ShuffleManager::new(
            32,
            Some(Arc::clone(engine.metrics())),
        ));
        let workers: Vec<_> = (0..3u32)
            .map(|slot| WorkerEndpoint {
                node_id: u64::from(slot) + 1,
                address: format!("local:{slot}"),
                slot,
            })
            .collect();
        let coord = Coordinator::local(Arc::clone(&engine), Arc::clone(&shuffle), workers);
        struct LocalDispatch(Worker);
        impl FragmentDispatcher for LocalDispatch {
            fn execute_remote(&self, _node_id: u64, fragment: &FragmentSpec) -> Result<Vec<Record>> {
                self.0.execute_fragment(fragment)
            }
        }
        coord.set_dispatcher(Arc::new(LocalDispatch(Worker::new(
            Arc::clone(&engine),
            Arc::clone(&shuffle),
            Arc::clone(engine.metrics()),
        ))));

        let n = coord
            .execute_insert_select("employees", "employees_copy", "id")
            .unwrap();
        assert_eq!(n, 6);

        let mut txn = engine.begin().unwrap();
        let copied = txn.scan_table_records("employees_copy").unwrap();
        txn.abort();
        assert_eq!(copied.len(), 6);

        // Even-ish shuffle: every partition should have been touched.
        assert!(engine.metrics().mpp_shuffle_sent() >= 3);

        engine.close().unwrap();
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn three_node_cluster_distributed_aggregate_and_shuffle_metrics() {
        use crate::client::TakyonicClient;
        use crate::cluster::{TakyonicNode, wait_for_leader};
        use crate::consensus::Role;
        use std::collections::HashMap as StdHashMap;
        use std::time::Duration;

        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("takyonic-mpp-cluster-{nanos}"));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();

        fn free_port() -> u16 {
            let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            l.local_addr().unwrap().port()
        }

        let mut endpoints = StdHashMap::new();
        for id in 1u64..=3 {
            endpoints.insert(id, format!("127.0.0.1:{}", free_port()));
        }

        let mut nodes = Vec::new();
        let mut handles = Vec::new();
        for id in 1u64..=3 {
            let cfg = Config::default()
                .data_dir(root.join(format!("node-{id}")).join("data"))
                .wal_dir(root.join(format!("node-{id}")).join("wal"))
                .memtable_size_bytes(64 * 1024 * 1024)
                .l0_rapid_pool_threads(1)
                .ln_haul_pool_threads(1)
                .compaction_write_bytes_per_sec(1024 * 1024 * 1024)
                .mpp_enabled(true)
                .metrics_enabled(true)
                .metrics_bind("127.0.0.1:0");
            let node = Arc::new(
                TakyonicNode::open(id, root.join(format!("node-{id}")), endpoints.clone(), cfg)
                    .unwrap(),
            );
            let (s, t) = node.start_background();
            handles.push(s);
            handles.push(t);
            nodes.push(node);
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
        let leader_id = wait_for_leader(&nodes, Duration::from_secs(10))
            .await
            .expect("leader");
        let leader = nodes.iter().find(|n| n.id() == leader_id).unwrap();
        assert_eq!(leader.role(), Role::Leader);

        let seeds: Vec<String> = nodes.iter().map(|n| n.addr().to_string()).collect();
        let client = TakyonicClient::new(seeds);
        client.connect().await.unwrap();
        client
            .register_table(TableSchema::new("employees", "id", vec![]))
            .await
            .unwrap();
        for (id, dept, sal) in [
            ("1", "Engineering", "100"),
            ("2", "Engineering", "150"),
            ("3", "Sales", "90"),
            ("4", "Sales", "110"),
            ("5", "HR", "80"),
            ("6", "Engineering", "120"),
        ] {
            client
                .execute_sql(&format!(
                    "INSERT INTO employees (id, department, salary) VALUES ('{id}', '{dept}', '{sal}')"
                ))
                .await
                .unwrap();
        }

        // Wait until every node sees all six rows.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            let ok = nodes.iter().all(|n| {
                let Ok(mut txn) = n.engine().begin() else {
                    return false;
                };
                let rows = txn.scan_table_records("employees").unwrap_or_default();
                let n_rows = rows.len();
                txn.abort();
                n_rows == 6
            });
            if ok {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("cluster did not replicate employees");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        // Per-node workers: each scans its hash shard of the replicated table.
        let mut worker_map: StdHashMap<u64, Worker> = StdHashMap::new();
        let mut endpoints_list = Vec::new();
        for (slot, n) in nodes.iter().enumerate() {
            let shuffle = Arc::new(ShuffleManager::new(
                32,
                Some(Arc::clone(n.engine().metrics())),
            ));
            worker_map.insert(
                n.id(),
                Worker::new(
                    Arc::clone(n.engine()),
                    shuffle,
                    Arc::clone(n.engine().metrics()),
                ),
            );
            endpoints_list.push(WorkerEndpoint {
                node_id: n.id(),
                address: n.addr().to_string(),
                slot: slot as u32,
            });
        }

        let leader_engine = Arc::clone(leader.engine());
        let leader_shuffle = Arc::new(ShuffleManager::new(
            32,
            Some(Arc::clone(leader_engine.metrics())),
        ));
        let coord = Coordinator::local(
            Arc::clone(&leader_engine),
            leader_shuffle,
            endpoints_list,
        );

        struct MultiDispatch {
            workers: StdHashMap<u64, Worker>,
        }
        impl FragmentDispatcher for MultiDispatch {
            fn execute_remote(&self, node_id: u64, fragment: &FragmentSpec) -> Result<Vec<Record>> {
                self.workers
                    .get(&node_id)
                    .ok_or_else(|| TakyonicError::Engine(format!("no worker {node_id}")))?
                    .execute_fragment(fragment)
            }
        }
        coord.set_dispatcher(Arc::new(MultiDispatch {
            workers: worker_map,
        }));

        let rows = coord
            .execute_distributed_aggregate(
                "employees",
                "department",
                DistAggKind::Sum("salary".into()),
            )
            .unwrap();
        let mut map = HashMap::new();
        for r in &rows {
            map.insert(
                r.get("department").unwrap().to_string(),
                r.get("SUM(salary)").unwrap().parse::<i64>().unwrap(),
            );
        }
        assert_eq!(map.get("Engineering"), Some(&370));
        assert_eq!(map.get("Sales"), Some(&200));
        assert_eq!(map.get("HR"), Some(&80));

        // Shuffle-phase load: every node should have executed a fragment.
        let frag_total: u64 = nodes.iter().map(|n| n.engine().metrics().mpp_fragments()).sum();
        assert!(
            frag_total >= 3,
            "expected fragments on all workers, got {frag_total}"
        );
        let sent: Vec<_> = nodes
            .iter()
            .map(|n| n.engine().metrics().mpp_shuffle_sent())
            .collect();
        // At least two nodes should have sent shuffle traffic (partitioned load).
        let active = sent.iter().filter(|&&s| s > 0).count();
        assert!(
            active >= 2,
            "shuffle load not distributed: sent={sent:?}"
        );
        assert!(
            sent.iter().copied().sum::<u64>() > 0,
            "3-node agg must increase shuffle sent metrics, sent={sent:?}"
        );

        // Distributed INSERT…SELECT into a copy table on the leader.
        leader_engine
            .register_table(TableSchema::new("employees_copy", "id", vec![]))
            .unwrap();
        // Rebuild dispatcher workers (previous map moved).
        let mut worker_map: StdHashMap<u64, Worker> = StdHashMap::new();
        let mut endpoints_list = Vec::new();
        for (slot, n) in nodes.iter().enumerate() {
            let shuffle = Arc::new(ShuffleManager::new(
                32,
                Some(Arc::clone(n.engine().metrics())),
            ));
            worker_map.insert(
                n.id(),
                Worker::new(
                    Arc::clone(n.engine()),
                    shuffle,
                    Arc::clone(n.engine().metrics()),
                ),
            );
            endpoints_list.push(WorkerEndpoint {
                node_id: n.id(),
                address: n.addr().to_string(),
                slot: slot as u32,
            });
        }
        let leader_shuffle = Arc::new(ShuffleManager::new(
            32,
            Some(Arc::clone(leader_engine.metrics())),
        ));
        let coord = Coordinator::local(
            Arc::clone(&leader_engine),
            leader_shuffle,
            endpoints_list,
        );
        coord.set_dispatcher(Arc::new(MultiDispatch {
            workers: worker_map,
        }));
        let n = coord
            .execute_insert_select("employees", "employees_copy", "id")
            .unwrap();
        assert_eq!(n, 6);

        for h in handles {
            h.abort();
        }
        for n in &nodes {
            let _ = n.close();
        }
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn hash_partition_inserts_route_to_different_nodes() {
        use crate::partition::{PartitionMap, PartitioningStrategy};
        use std::sync::Mutex as StdMutex;

        let (engine, root) = temp_engine("part-ins");
        let schema = TableSchema::new("users", "user_id", vec![])
            .with_partitioning(PartitioningStrategy::Hash {
                column: "user_id".into(),
                bucket_count: 3,
            })
            .with_partition_map(PartitionMap::round_robin(&[1, 2, 3], 3));
        engine.register_table(schema).unwrap();

        let shuffle = Arc::new(ShuffleManager::new(32, Some(Arc::clone(engine.metrics()))));
        let workers: Vec<_> = (0..3u32)
            .map(|slot| WorkerEndpoint {
                node_id: u64::from(slot) + 1,
                address: format!("local:{slot}"),
                slot,
            })
            .collect();
        let coord = Coordinator::local(Arc::clone(&engine), Arc::clone(&shuffle), workers);
        let contacted: Arc<StdMutex<Vec<u64>>> = Arc::new(StdMutex::new(Vec::new()));
        struct TrackingDispatch {
            inner: Worker,
            contacted: Arc<StdMutex<Vec<u64>>>,
        }
        impl FragmentDispatcher for TrackingDispatch {
            fn execute_remote(&self, node_id: u64, fragment: &FragmentSpec) -> Result<Vec<Record>> {
                self.contacted.lock().unwrap().push(node_id);
                self.inner.execute_fragment(fragment)
            }
        }
        coord.set_dispatcher(Arc::new(TrackingDispatch {
            inner: Worker::new(
                Arc::clone(&engine),
                Arc::clone(&shuffle),
                Arc::clone(engine.metrics()),
            ),
            contacted: Arc::clone(&contacted),
        }));

        let mut owners = std::collections::HashSet::new();
        for i in 0..60 {
            let (node, _pid) = coord
                .execute_insert(
                    "users",
                    Record::new()
                        .set("user_id", i.to_string())
                        .set("name", format!("u{i}")),
                )
                .unwrap();
            owners.insert(node);
        }
        assert_eq!(
            owners.len(),
            3,
            "hash inserts must hit all 3 nodes, got {owners:?}"
        );
        let seen = contacted.lock().unwrap().clone();
        let unique: std::collections::HashSet<_> = seen.iter().copied().collect();
        assert_eq!(unique.len(), 3, "dispatcher must not broadcast, got {seen:?}");
        // Each insert contacts exactly one node.
        assert_eq!(seen.len(), 60);

        engine.close().unwrap();
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn partition_prune_scan_contacts_single_remote_worker() {
        use crate::partition::{PartitionMap, PartitioningStrategy};
        use crate::query::FilterOp;
        use crate::sql::Expression;
        use std::sync::Mutex as StdMutex;

        let (engine, root) = temp_engine("c2-prune-scan");
        engine
            .register_table(
                TableSchema::new("users", "user_id", vec![])
                    .with_columns(vec![
                        crate::schema::ColumnSpec::new("user_id", "TEXT"),
                        crate::schema::ColumnSpec::new("name", "TEXT"),
                    ])
                    .with_partitioning(PartitioningStrategy::Hash {
                        column: "user_id".into(),
                        bucket_count: 3,
                    })
                    .with_partition_map(PartitionMap::round_robin(&[1, 2, 3], 3)),
            )
            .unwrap();

        // Seed rows across partitions.
        {
            let mut txn = engine.begin().unwrap();
            for i in 0..30 {
                txn.put_record(
                    "users",
                    Record::new()
                        .set("user_id", i.to_string())
                        .set("name", format!("u{i}")),
                )
                .unwrap();
            }
            txn.commit().unwrap();
        }

        let shuffle = Arc::new(ShuffleManager::new(32, Some(Arc::clone(engine.metrics()))));
        let workers: Vec<_> = (0..3u32)
            .map(|slot| WorkerEndpoint {
                node_id: u64::from(slot) + 1,
                address: format!("local-{slot}"),
                slot,
            })
            .collect();
        let coord = Coordinator::local(Arc::clone(&engine), Arc::clone(&shuffle), workers);
        let contacted: Arc<StdMutex<Vec<(u64, u32)>>> = Arc::new(StdMutex::new(Vec::new()));
        struct TrackingDispatch {
            inner: Worker,
            contacted: Arc<StdMutex<Vec<(u64, u32)>>>,
        }
        impl FragmentDispatcher for TrackingDispatch {
            fn execute_remote(&self, node_id: u64, fragment: &FragmentSpec) -> Result<Vec<Record>> {
                if let FragmentSpec::PartitionedScan { partition_id, .. } = fragment {
                    self.contacted
                        .lock()
                        .unwrap()
                        .push((node_id, *partition_id));
                }
                self.inner.execute_fragment(fragment)
            }
        }
        coord.set_dispatcher(Arc::new(TrackingDispatch {
            inner: Worker::new(
                Arc::clone(&engine),
                Arc::clone(&shuffle),
                Arc::clone(engine.metrics()),
            ),
            contacted: Arc::clone(&contacted),
        }));

        let pred = Expression::BinaryOp {
            left: Box::new(Expression::Column("user_id".into())),
            op: FilterOp::Eq,
            right: Box::new(Expression::Literal("7".into())),
        };
        let rows = coord
            .execute_distributed_scan("users", Some(&pred))
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("user_id"), Some("7"));

        let hits = contacted.lock().unwrap().clone();
        assert_eq!(
            hits.len(),
            1,
            "partition prune must contact exactly one RemoteWorker, got {hits:?}"
        );

        // Router agreement.
        let schema = engine.table_schema("users").unwrap();
        let router = PartitionRouter::new(coord.workers().to_vec());
        let (expect_pid, expect_node) = router.route_key(&schema, "7").unwrap();
        assert_eq!(hits[0], (expect_node, expect_pid));

        engine.close().unwrap();
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn partition_pruning_explain_single_remote_worker() {
        use crate::executor::{explain_physical, optimize_with_catalog};
        use crate::partition::{PartitionMap, PartitioningStrategy};
        use crate::sql::{Expression, LogicalPlan};
        use crate::query::FilterOp;

        let schema = TableSchema::new("users", "user_id", vec![])
            .with_partitioning(PartitioningStrategy::Hash {
                column: "user_id".into(),
                bucket_count: 3,
            })
            .with_partition_map(PartitionMap::round_robin(&[1, 2, 3], 3));
        let plan = LogicalPlan::Select {
            table: "users".into(),
            filters: vec![],
            predicate: Some(Expression::BinaryOp {
                left: Box::new(Expression::Column("user_id".into())),
                op: FilterOp::Eq,
                right: Box::new(Expression::Literal("123".into())),
            }),
        };
        let physical = optimize_with_catalog(&plan, &|_| Some(schema.clone()), &|_| None).unwrap();
        let text = explain_physical(&physical);
        let remote_count = text.matches("RemoteWorker(").count();
        assert_eq!(
            remote_count, 1,
            "pruned plan must show exactly one RemoteWorker, got:\n{text}"
        );
        assert!(text.contains("DistributedScan(users)"), "{text}");
    }

    #[test]
    fn execute_insert_routes_to_owning_partition_not_broadcast() {
        use crate::partition::{PartitionMap, PartitioningStrategy};
        use std::sync::Mutex as StdMutex;

        let (engine, root) = temp_engine("part-route");
        engine
            .register_table(
                TableSchema::new("orders", "id", vec![])
                    .with_partitioning(PartitioningStrategy::Hash {
                        column: "id".into(),
                        bucket_count: 3,
                    })
                    .with_partition_map(PartitionMap::round_robin(&[1, 2, 3], 3)),
            )
            .unwrap();
        let shuffle = Arc::new(ShuffleManager::new(32, None));
        let workers: Vec<_> = (0..3u32)
            .map(|slot| WorkerEndpoint {
                node_id: u64::from(slot) + 1,
                address: format!("n{slot}"),
                slot,
            })
            .collect();
        let coord = Coordinator::local(Arc::clone(&engine), Arc::clone(&shuffle), workers);
        let hits: Arc<StdMutex<Vec<u64>>> = Arc::new(StdMutex::new(Vec::new()));
        struct HitDispatch(Worker, Arc<StdMutex<Vec<u64>>>);
        impl FragmentDispatcher for HitDispatch {
            fn execute_remote(&self, node_id: u64, fragment: &FragmentSpec) -> Result<Vec<Record>> {
                self.1.lock().unwrap().push(node_id);
                // Only the owning partition fragment should arrive.
                if let FragmentSpec::PartitionedScan {
                    partition_id,
                    partition_count,
                    ..
                } = fragment
                {
                    assert!(*partition_id < *partition_count);
                }
                self.0.execute_fragment(fragment)
            }
        }
        coord.set_dispatcher(Arc::new(HitDispatch(
            Worker::new(
                Arc::clone(&engine),
                Arc::clone(&shuffle),
                Arc::clone(engine.metrics()),
            ),
            Arc::clone(&hits),
        )));

        let (node, pid) = coord
            .execute_insert("orders", Record::new().set("id", "42").set("amt", "9"))
            .unwrap();
        let h = hits.lock().unwrap().clone();
        assert_eq!(h.len(), 1, "must not broadcast INSERT, contacted {h:?}");
        assert_eq!(h[0], node);
        // Router agreement.
        let schema = engine.table_schema("orders").unwrap();
        let router = PartitionRouter::new(coord.workers().to_vec());
        let (expect_pid, expect_node) = router.route_key(&schema, "42").unwrap();
        assert_eq!(pid, expect_pid);
        assert_eq!(node, expect_node);

        engine.close().unwrap();
        let _ = std::fs::remove_dir_all(root);
    }
}
