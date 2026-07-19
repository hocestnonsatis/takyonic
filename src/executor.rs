//! Volcano-style physical execution iterators.
//!
//! Logical plans are lowered to [`PhysicalPlan`] nodes. Equi-joins become
//! [`HashJoinExec`]; non-equality predicates fall back to [`NestedLoopJoin`].
//! Bind-time parameters flow through [`ExecutionContext`]; storage reads go
//! through an active MVCC [`Transaction`] at the executor entry point.
//!
//! DML nodes ([`InsertExec`] / [`UpdateExec`] / [`DeleteExec`]) mutate via
//! `Transaction::{put,delete}_record` and yield a single affected-row count.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::fmt::Write;
use std::sync::Arc;

use crate::error::{Result, TakyonicError};
use crate::jit::{self, CompiledFn, JitCompiler, collect_jit_columns, is_jit_compilable};
use crate::query::{FilterOp, matches_filter};
use crate::schema::{Record, TableSchema};
use crate::sql::{Expression, JoinType, LogicalPlan, SortExpr, Value, aggregate_result_column};
use crate::stats::TableStats;
use crate::telemetry::EngineMetrics;
use crate::txn::Transaction;

/// Minimum estimated rows before the CBO wraps a segment in [`PhysicalPlan::JitExec`].
///
/// Cranelift compile of a simple filter/arith tree is microseconds, so the
/// break-even cardinality is effectively 1 for OLAP push pipelines.
const JIT_MIN_ROWS: u64 = 1;
/// Prefer [`PhysicalPlan::VectorizedExec`] (SIMD batches) above this cardinality.
const VECTOR_MIN_ROWS: u64 = 256;

/// Bind-time / runtime parameter bag passed down the Volcano tree.
///
/// Storage access (MVCC transaction + catalog) is supplied separately via
/// [`open_executor_with_txn`] so this context stays cheaply [`Clone`].
#[derive(Clone, Debug, Default)]
pub struct ExecutionContext {
    /// Resolved `$1`…`$n` values (0-based: `params[0]` ≡ `$1`).
    pub params: Vec<Value>,
}

impl ExecutionContext {
    /// Empty context (no parameters).
    pub fn new() -> Self {
        Self::default()
    }

    /// Build from an already-decoded parameter list.
    pub fn with_params(params: Vec<Value>) -> Self {
        Self { params }
    }

    /// Look up `$n` (0-based index).
    pub fn param(&self, idx: usize) -> Result<&Value> {
        self.params.get(idx).ok_or_else(|| {
            TakyonicError::Sql(format!(
                "parameter ${} not bound (have {} params)",
                idx + 1,
                self.params.len()
            ))
        })
    }
}

/// Physical plan tree produced by the (currently trivial) optimizer.
#[derive(Clone, Debug)]
pub enum PhysicalPlan {
    /// Full / filtered table access via MVCC [`Transaction`] prefix scan.
    TableScan {
        /// Target table.
        table: String,
        /// Residual literal filters (CBO path); applied after decode.
        filters: Vec<crate::query::Filter>,
    },
    /// In-memory row source — useful for unit tests and stub pipelines.
    Values {
        /// Materialized rows.
        rows: Vec<Record>,
    },
    /// Filter rows by a predicate (may contain `$n` parameters).
    Filter {
        /// Child plan.
        input: Box<PhysicalPlan>,
        /// Boolean predicate.
        predicate: Expression,
    },
    /// Nested-loop join over two child plans (non-equi / complex predicates).
    NestedLoopJoin {
        /// Outer (left) child.
        left: Box<PhysicalPlan>,
        /// Inner (right) child.
        right: Box<PhysicalPlan>,
        /// Join predicate.
        condition: Expression,
        /// Logical join kind (only Inner is evaluated today).
        join_type: JoinType,
    },
    /// Hash equi-join: build on `left_key`, probe with `right_key`.
    HashJoin {
        /// Build-side child.
        left: Box<PhysicalPlan>,
        /// Probe-side child.
        right: Box<PhysicalPlan>,
        /// Key expression evaluated on left rows.
        left_key: Expression,
        /// Key expression evaluated on right rows.
        right_key: Expression,
        /// Logical join kind (only Inner is evaluated today).
        join_type: JoinType,
    },
    /// `INSERT INTO … VALUES …` — evaluates expressions and writes records.
    Insert {
        /// Target table.
        table: String,
        /// Column list.
        columns: Vec<String>,
        /// Expression rows.
        values: Vec<Vec<Expression>>,
    },
    /// `UPDATE … SET …` over target rows from `input`.
    Update {
        /// Target table.
        table: String,
        /// Column assignments.
        assignments: HashMap<String, Expression>,
        /// Child yielding rows to update (typically Filter(TableScan)).
        input: Box<PhysicalPlan>,
    },
    /// `DELETE FROM …` over target rows from `input`.
    Delete {
        /// Target table.
        table: String,
        /// Child yielding rows to delete.
        input: Box<PhysicalPlan>,
    },
    /// Index point/equality lookup — PK or secondary (two-step PK fetch).
    IndexScan {
        /// Target table.
        table: String,
        /// `None` = primary-key point lookup; `Some(name)` = secondary index.
        index: Option<String>,
        /// Indexed column name (for residual verification / EXPLAIN); PK when `index` is None.
        index_column: String,
        /// Equality key expression (`Literal` or `Parameter`).
        key_value: Expression,
    },
    /// HNSW approximate / exact k-NN scan (`ORDER BY col <-> query LIMIT k`).
    VectorIndexScan {
        /// Target table.
        table: String,
        /// Vector index name.
        index: String,
        /// Embedding column.
        index_column: String,
        /// Query vector expression (`ARRAY[…]` / literal).
        query: Expression,
        /// `OFFSET`.
        skip: usize,
        /// `LIMIT` (k nearest).
        fetch: usize,
    },
    /// Hash aggregation: drain child, group by keys, emit group + aggregate columns.
    Aggregate {
        /// Child plan providing input rows.
        input: Box<PhysicalPlan>,
        /// Grouping key expressions (empty → single global group).
        group_exprs: Vec<Expression>,
        /// Aggregate expressions (`COUNT` / `SUM` / …).
        aggr_exprs: Vec<Expression>,
    },
    /// Full in-memory sort of the child stream.
    Sort {
        /// Child plan.
        input: Box<PhysicalPlan>,
        /// Sort keys.
        exprs: Vec<crate::sql::SortExpr>,
    },
    /// Skip `skip` rows then yield at most `fetch` (streaming).
    Limit {
        /// Child plan.
        input: Box<PhysicalPlan>,
        /// `OFFSET`.
        skip: usize,
        /// `LIMIT`; `None` = no upper bound.
        fetch: Option<usize>,
    },
    /// Fused Sort+Limit via a bounded heap (Top-N).
    TopN {
        /// Child plan.
        input: Box<PhysicalPlan>,
        /// Sort keys (same semantics as [`PhysicalPlan::Sort`]).
        exprs: Vec<crate::sql::SortExpr>,
        /// `OFFSET`.
        skip: usize,
        /// `LIMIT` (required for Top-N).
        fetch: usize,
    },
    /// `ANALYZE <table>` — scan + gather statistics into the catalog.
    Analyze {
        /// Target table.
        table: String,
    },
    /// `VACUUM <table>` — reclaim dead MVCC versions under the epoch watermark.
    Vacuum {
        /// Target table.
        table: String,
    },
    /// HyPer-style push pipeline: compiled `Scan → Filter → [Aggregate]` loop.
    ///
    /// Expressions are Cranelift-compiled; the operator drains the scan once
    /// in a tight loop (no Volcano virtual calls between filter and aggregate).
    JitExec {
        /// Underlying scan (typically [`PhysicalPlan::TableScan`]).
        input: Box<PhysicalPlan>,
        /// Optional filter predicate (JIT-compiled when possible).
        predicate: Option<Expression>,
        /// Grouping keys (empty → global aggregate / filter-only).
        group_exprs: Vec<Expression>,
        /// Aggregate expressions (`SUM` / `COUNT` / …); empty → emit filtered rows.
        aggr_exprs: Vec<Expression>,
    },
    /// SIMD vectorized batch pipeline ([`crate::vectorized::VectorBatch`]).
    ///
    /// Preferred by [`JITVectorizationRule`] for large OLAP scans / aggs.
    VectorizedExec {
        /// Underlying scan (TableScan / Values).
        input: Box<PhysicalPlan>,
        /// Optional filter (evaluated via SIMD masks when possible).
        predicate: Option<Expression>,
        /// Global aggregate expressions (`SUM`/`COUNT`); empty → filter-only emit.
        aggr_exprs: Vec<Expression>,
    },
    /// Partition-pruned distributed scan (MPP fragment graph for EXPLAIN).
    ///
    /// `remote_workers` lists `(node_id, partition_id)` after
    /// [`crate::partition::PartitionPruningRule`]. Execution falls through to
    /// `input` on the local node.
    DistributedScan {
        /// Target table.
        table: String,
        /// Pruned remote workers (`RemoteWorker(node=…, partition=…)` in EXPLAIN).
        remote_workers: Vec<(u64, u32)>,
        /// Local access path (IndexScan / Filter / TableScan).
        input: Box<PhysicalPlan>,
    },
}

/// Whether this logical plan is a mutating statement.
pub fn is_dml_plan(plan: &LogicalPlan) -> bool {
    matches!(
        plan,
        LogicalPlan::Insert { .. } | LogicalPlan::Update { .. } | LogicalPlan::Delete { .. }
    )
}

/// Map a [`LogicalPlan`] to a [`PhysicalPlan`] without catalog access.
///
/// Prefer [`optimize_with_catalog`] when a schema is available so PK / secondary
/// IndexScan rewrites can fire.
pub fn optimize(plan: &LogicalPlan) -> Result<PhysicalPlan> {
    optimize_with_catalog(plan, &|_| None, &|_| None)
}

/// Map a [`LogicalPlan`] to a [`PhysicalPlan`], consulting the catalog for
/// primary-key and secondary IndexScan rewrites.
///
/// Heuristics (cheapest wins):
/// 1. `pk = lit|$n` → PK [`PhysicalPlan::IndexScan`]
/// 2. indexed `col = lit|$n` when selectivity (NDV/MCV) prefers IndexScan
/// 3. else Filter(TableScan)
/// 4. wrap compatible `Scan→Filter→Aggregate` segments in [`PhysicalPlan::JitExec`]
pub fn optimize_with_catalog(
    plan: &LogicalPlan,
    schema_of: &dyn Fn(&str) -> Option<TableSchema>,
    stats_of: &dyn Fn(&str) -> Option<TableStats>,
) -> Result<PhysicalPlan> {
    let physical = optimize_plan_tree(plan, schema_of, stats_of)?;
    Ok(maybe_attach_vectorized(physical, stats_of))
}

/// Lower without attaching [`PhysicalPlan::JitExec`] (benchmark baseline / tests).
pub fn optimize_without_jit(
    plan: &LogicalPlan,
    schema_of: &dyn Fn(&str) -> Option<TableSchema>,
    stats_of: &dyn Fn(&str) -> Option<TableStats>,
) -> Result<PhysicalPlan> {
    optimize_plan_tree(plan, schema_of, stats_of)
}

fn optimize_plan_tree(
    plan: &LogicalPlan,
    schema_of: &dyn Fn(&str) -> Option<TableSchema>,
    stats_of: &dyn Fn(&str) -> Option<TableStats>,
) -> Result<PhysicalPlan> {
    match plan {
        LogicalPlan::Select {
            table,
            filters,
            predicate,
        } => {
            if let Some(pred) = predicate {
                let scan = LogicalPlan::Select {
                    table: table.clone(),
                    filters: filters.clone(),
                    predicate: None,
                };
                if let Some(semi) =
                    try_subquery_unnest(&scan, pred, schema_of, stats_of)?
                {
                    return Ok(semi);
                }
            }
            optimize_table_access(table, filters, predicate.as_ref(), schema_of, stats_of)
        },
        LogicalPlan::Insert {
            table,
            columns,
            values,
        } => Ok(PhysicalPlan::Insert {
            table: table.clone(),
            columns: columns.clone(),
            values: values.clone(),
        }),
        LogicalPlan::Update {
            table,
            assignments,
            selection,
        } => {
            let input =
                optimize_table_access(table, &[], selection.as_ref(), schema_of, stats_of)?;
            Ok(PhysicalPlan::Update {
                table: table.clone(),
                assignments: assignments.clone(),
                input: Box::new(input),
            })
        }
        LogicalPlan::Delete { table, selection } => {
            let input =
                optimize_table_access(table, &[], selection.as_ref(), schema_of, stats_of)?;
            Ok(PhysicalPlan::Delete {
                table: table.clone(),
                input: Box::new(input),
            })
        }
        LogicalPlan::Join {
            left,
            right,
            on,
            join_type,
        }
        | LogicalPlan::DistributedJoin {
            left,
            right,
            on,
            join_type,
            distribution: _,
        } => {
            // DistributedJoin: local fallback → same HashJoin / NestedLoop as Join.
            let left_phys = Box::new(optimize_plan_tree(left, schema_of, stats_of)?);
            let right_phys = Box::new(optimize_plan_tree(right, schema_of, stats_of)?);
            if let Some((left_key, right_key)) =
                match_equi_join_keys(on, left, right, schema_of)
            {
                // Inner HashJoin: build the smaller side (by estimated cardinality).
                let (build, probe, build_key, probe_key) = if *join_type == JoinType::Inner {
                    let left_rows = estimate_physical_rows(&left_phys, stats_of);
                    let right_rows = estimate_physical_rows(&right_phys, stats_of);
                    if right_rows < left_rows {
                        (right_phys, left_phys, right_key, left_key)
                    } else {
                        (left_phys, right_phys, left_key, right_key)
                    }
                } else {
                    (left_phys, right_phys, left_key, right_key)
                };
                Ok(PhysicalPlan::HashJoin {
                    left: build,
                    right: probe,
                    left_key: build_key,
                    right_key: probe_key,
                    join_type: *join_type,
                })
            } else {
                Ok(PhysicalPlan::NestedLoopJoin {
                    left: left_phys,
                    right: right_phys,
                    condition: on.clone(),
                    join_type: *join_type,
                })
            }
        }
        LogicalPlan::Aggregate {
            input,
            group_exprs,
            aggr_exprs,
        }
        | LogicalPlan::DistributedAggregate {
            input,
            group_exprs,
            aggr_exprs,
        } => Ok(PhysicalPlan::Aggregate {
            // DistributedAggregate: local fallback → normal Aggregate.
            input: Box::new(optimize_plan_tree(input, schema_of, stats_of)?),
            group_exprs: group_exprs.clone(),
            aggr_exprs: aggr_exprs.clone(),
        }),
        LogicalPlan::Sort { input, exprs } => Ok(PhysicalPlan::Sort {
            input: Box::new(optimize_plan_tree(input, schema_of, stats_of)?),
            exprs: exprs.clone(),
        }),
        LogicalPlan::Limit {
            input,
            skip,
            fetch,
        } => {
            // VectorIndexSelectionRule: ORDER BY col <-> query LIMIT k → HNSW scan.
            if let Some(vis) =
                try_vector_index_selection(input, *skip, *fetch, schema_of)
            {
                return Ok(vis);
            }
            if let (Some(fetch), LogicalPlan::Sort { input: sort_in, exprs }) =
                (*fetch, input.as_ref())
            {
                return Ok(PhysicalPlan::TopN {
                    input: Box::new(optimize_plan_tree(sort_in, schema_of, stats_of)?),
                    exprs: exprs.clone(),
                    skip: *skip,
                    fetch,
                });
            }
            Ok(PhysicalPlan::Limit {
                input: Box::new(optimize_plan_tree(input, schema_of, stats_of)?),
                skip: *skip,
                fetch: *fetch,
            })
        }
        LogicalPlan::Filter { input, predicate } => {
            if let Some(semi) =
                try_subquery_unnest(input, predicate, schema_of, stats_of)?
            {
                return Ok(semi);
            }
            Ok(PhysicalPlan::Filter {
                input: Box::new(optimize_plan_tree(input, schema_of, stats_of)?),
                predicate: predicate.clone(),
            })
        }
        LogicalPlan::SubqueryAlias { input, .. } => {
            optimize_plan_tree(input, schema_of, stats_of)
        }
        LogicalPlan::Explain { plan } => optimize_plan_tree(plan, schema_of, stats_of),
        LogicalPlan::Analyze { table } => Ok(PhysicalPlan::Analyze {
            table: table.clone(),
        }),
        LogicalPlan::Vacuum { table } => Ok(PhysicalPlan::Vacuum {
            table: table.clone(),
        }),
        LogicalPlan::CreateIndex { .. }
        | LogicalPlan::DropIndex { .. }
        | LogicalPlan::CreateRole { .. }
        | LogicalPlan::DropRole { .. }
        | LogicalPlan::Grant { .. }
        | LogicalPlan::Revoke { .. }
        | LogicalPlan::GrantRole { .. }
        | LogicalPlan::Begin
        | LogicalPlan::Commit
        | LogicalPlan::Rollback => Err(TakyonicError::Sql(
            "DDL/transaction control has no physical plan; handle in SessionState".into(),
        )),
    }
}

/// JITVectorizationRule: prefer SIMD batch pipelines for large OLAP fragments.
fn maybe_attach_vectorized(
    plan: PhysicalPlan,
    stats_of: &dyn Fn(&str) -> Option<TableStats>,
) -> PhysicalPlan {
    // Only fire when catalog stats (or a large Values node) prove cardinality;
    // the coarse TableScan default of 1000 must not force SIMD on tiny tables.
    let prefer_simd = vectorization_row_hint(&plan, stats_of)
        .map(|n| n >= VECTOR_MIN_ROWS)
        .unwrap_or(false);

    if prefer_simd {
        match &plan {
            PhysicalPlan::Aggregate {
                input,
                group_exprs,
                aggr_exprs,
            } if group_exprs.is_empty()
                && aggr_exprs.iter().all(aggr_args_vectorizable)
                && matches!(
                    input.as_ref(),
                    PhysicalPlan::Filter { .. }
                        | PhysicalPlan::TableScan { .. }
                        | PhysicalPlan::Values { .. }
                        | PhysicalPlan::JitExec { .. }
                ) =>
            {
                let (scan, predicate) = match input.as_ref() {
                    PhysicalPlan::Filter { input, predicate } => {
                        (input.as_ref().clone(), Some(predicate.clone()))
                    }
                    PhysicalPlan::JitExec {
                        input,
                        predicate,
                        ..
                    } => (input.as_ref().clone(), predicate.clone()),
                    other => (other.clone(), None),
                };
                if predicate
                    .as_ref()
                    .map(crate::vectorized::is_vectorizable)
                    .unwrap_or(true)
                {
                    crate::vectorized::note_vectorized_exec();
                    return PhysicalPlan::VectorizedExec {
                        input: Box::new(scan),
                        predicate,
                        aggr_exprs: aggr_exprs.clone(),
                    };
                }
            }
            PhysicalPlan::Filter { input, predicate }
                if crate::vectorized::is_vectorizable(predicate)
                    && matches!(
                        input.as_ref(),
                        PhysicalPlan::TableScan { .. } | PhysicalPlan::Values { .. }
                    ) =>
            {
                crate::vectorized::note_vectorized_exec();
                return PhysicalPlan::VectorizedExec {
                    input: input.clone(),
                    predicate: Some(predicate.clone()),
                    aggr_exprs: Vec::new(),
                };
            }
            _ => {}
        }
    }
    maybe_attach_jit(plan, stats_of)
}

/// Cardinality hint for SIMD selection: `None` when no real stats exist.
fn vectorization_row_hint(
    plan: &PhysicalPlan,
    stats_of: &dyn Fn(&str) -> Option<TableStats>,
) -> Option<u64> {
    match plan {
        PhysicalPlan::TableScan { table, .. } => stats_of(table).map(|s| s.row_count),
        PhysicalPlan::Values { rows } => Some(rows.len() as u64),
        PhysicalPlan::Filter { input, .. }
        | PhysicalPlan::Aggregate { input, .. }
        | PhysicalPlan::JitExec { input, .. }
        | PhysicalPlan::VectorizedExec { input, .. }
        | PhysicalPlan::DistributedScan { input, .. }
        | PhysicalPlan::Sort { input, .. }
        | PhysicalPlan::Limit { input, .. }
        | PhysicalPlan::TopN { input, .. } => vectorization_row_hint(input, stats_of),
        _ => None,
    }
}

fn aggr_args_vectorizable(expr: &Expression) -> bool {
    match expr {
        Expression::AggregateFunction { name, args } => {
            let n = name.to_ascii_lowercase();
            (n == "sum" || n == "count")
                && (args.is_empty() || args.iter().all(crate::vectorized::is_vectorizable))
        }
        _ => false,
    }
}

/// Wrap a compatible linear pipeline in [`PhysicalPlan::JitExec`] when CBO
/// estimates enough rows to amortize compilation.
fn maybe_attach_jit(
    plan: PhysicalPlan,
    stats_of: &dyn Fn(&str) -> Option<TableStats>,
) -> PhysicalPlan {
    let rows = estimate_physical_rows(&plan, stats_of);
    // No stats → still JIT (compilation is cheap for simple predicates).
    // With stats, require JIT_MIN_ROWS to amortize compile cost.
    let enough = rows == 0 || rows >= JIT_MIN_ROWS;

    match plan {
        PhysicalPlan::Aggregate {
            input,
            group_exprs,
            aggr_exprs,
        } if group_exprs.is_empty() && enough && aggr_exprs.iter().all(aggr_args_jit_ok) => {
            match *input {
                PhysicalPlan::Filter { input: scan, predicate }
                    if is_jit_compilable(&predicate)
                        && matches!(
                            scan.as_ref(),
                            PhysicalPlan::TableScan { .. } | PhysicalPlan::Values { .. }
                        ) =>
                {
                    PhysicalPlan::JitExec {
                        input: scan,
                        predicate: Some(predicate),
                        group_exprs,
                        aggr_exprs,
                    }
                }
                scan @ (PhysicalPlan::TableScan { .. } | PhysicalPlan::Values { .. }) => {
                    PhysicalPlan::JitExec {
                        input: Box::new(scan),
                        predicate: None,
                        group_exprs,
                        aggr_exprs,
                    }
                }
                other => PhysicalPlan::Aggregate {
                    input: Box::new(other),
                    group_exprs,
                    aggr_exprs,
                },
            }
        }
        PhysicalPlan::Filter { input, predicate }
            if enough
                && is_jit_compilable(&predicate)
                && matches!(
                    input.as_ref(),
                    PhysicalPlan::TableScan { .. } | PhysicalPlan::Values { .. }
                ) =>
        {
            PhysicalPlan::JitExec {
                input,
                predicate: Some(predicate),
                group_exprs: Vec::new(),
                aggr_exprs: Vec::new(),
            }
        }
        other => other,
    }
}

fn aggr_args_jit_ok(expr: &Expression) -> bool {
    match expr {
        Expression::AggregateFunction { args, .. } => {
            args.is_empty() || args.iter().all(is_jit_compilable)
        }
        _ => false,
    }
}

/// Detect `col_left = col_right` equi-join and assign keys to build/probe sides.
fn match_equi_join_keys(
    on: &Expression,
    left_plan: &LogicalPlan,
    right_plan: &LogicalPlan,
    schema_of: &dyn Fn(&str) -> Option<TableSchema>,
) -> Option<(Expression, Expression)> {
    let Expression::BinaryOp {
        left,
        op: FilterOp::Eq,
        right,
    } = on
    else {
        return None;
    };
    let (Expression::Column(a), Expression::Column(b)) = (left.as_ref(), right.as_ref()) else {
        return None;
    };

    let left_hint = side_column_hints(left_plan, schema_of);
    let right_hint = side_column_hints(right_plan, schema_of);

    // Prefer catalog hints: column on left side → left_key, other → right_key.
    let a_left = left_hint.contains(a);
    let b_left = left_hint.contains(b);
    let a_right = right_hint.contains(a);
    let b_right = right_hint.contains(b);

    if a_left && !b_left {
        return Some((Expression::Column(a.clone()), Expression::Column(b.clone())));
    }
    if b_left && !a_left {
        return Some((Expression::Column(b.clone()), Expression::Column(a.clone())));
    }
    if b_right && !a_right {
        return Some((Expression::Column(a.clone()), Expression::Column(b.clone())));
    }
    if a_right && !b_right {
        return Some((Expression::Column(b.clone()), Expression::Column(a.clone())));
    }

    // No / ambiguous catalog: assume BinaryOp order matches join child order
    // (`users.id = orders.user_id` after leaf-name parse → `id = user_id`).
    Some((Expression::Column(a.clone()), Expression::Column(b.clone())))
}

fn side_column_hints(
    plan: &LogicalPlan,
    schema_of: &dyn Fn(&str) -> Option<TableSchema>,
) -> HashSet<String> {
    let mut cols = HashSet::new();
    for table in collect_tables(plan) {
        if let Some(schema) = schema_of(&table) {
            cols.insert(schema.primary_key.clone());
            for idx in &schema.indexes {
                cols.insert(idx.column.clone());
            }
        }
    }
    cols
}

fn collect_tables(plan: &LogicalPlan) -> Vec<String> {
    match plan {
        LogicalPlan::Select { table, .. } => vec![table.clone()],
        LogicalPlan::Join { left, right, .. }
        | LogicalPlan::DistributedJoin { left, right, .. } => {
            let mut t = collect_tables(left);
            t.extend(collect_tables(right));
            t
        }
        LogicalPlan::Update { table, .. } | LogicalPlan::Delete { table, .. } => {
            vec![table.clone()]
        }
        LogicalPlan::Insert { table, .. } => vec![table.clone()],
        LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::DistributedAggregate { input, .. } => collect_tables(input),
        LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Filter { input, .. }
        | LogicalPlan::SubqueryAlias { input, .. }
        | LogicalPlan::Explain { plan: input } => collect_tables(input),
        LogicalPlan::CreateIndex { table, .. }
        | LogicalPlan::Analyze { table }
        | LogicalPlan::Vacuum { table } => {
            vec![table.clone()]
        }
        LogicalPlan::DropIndex { .. }
        | LogicalPlan::CreateRole { .. }
        | LogicalPlan::DropRole { .. }
        | LogicalPlan::Grant { .. }
        | LogicalPlan::Revoke { .. }
        | LogicalPlan::GrantRole { .. }
        | LogicalPlan::Begin
        | LogicalPlan::Commit
        | LogicalPlan::Rollback => Vec::new(),
    }
}

/// SubqueryUnnestingRule: uncorrelated `IN (SELECT …)` → Hash Semi/Anti Join.
fn try_subquery_unnest(
    left_plan: &LogicalPlan,
    predicate: &Expression,
    schema_of: &dyn Fn(&str) -> Option<TableSchema>,
    stats_of: &dyn Fn(&str) -> Option<TableStats>,
) -> Result<Option<PhysicalPlan>> {
    match predicate {
        Expression::InSubquery {
            expr,
            subquery,
            value_column,
            negated,
            correlated: false,
        } => {
            let left = Box::new(optimize_with_catalog(left_plan, schema_of, stats_of)?);
            let right = Box::new(optimize_with_catalog(subquery, schema_of, stats_of)?);
            let join_type = if *negated {
                JoinType::Anti
            } else {
                JoinType::Semi
            };
            Ok(Some(PhysicalPlan::HashJoin {
                left,
                right,
                left_key: expr.as_ref().clone(),
                right_key: Expression::Column(value_column.clone()),
                join_type,
            }))
        }
        // Compound AND: unnest only when the whole predicate is a bare InSubquery
        // (residual Filter kept otherwise).
        _ => Ok(None),
    }
}

/// Lower a single-table access path with PK / secondary IndexScan selection.
fn optimize_table_access(
    table: &str,
    residual_filters: &[crate::query::Filter],
    predicate: Option<&Expression>,
    schema_of: &dyn Fn(&str) -> Option<TableSchema>,
    stats_of: &dyn Fn(&str) -> Option<TableStats>,
) -> Result<PhysicalPlan> {
    let local = optimize_table_access_local(
        table,
        residual_filters,
        predicate,
        schema_of,
        stats_of,
    )?;
    // PartitionPruningRule: wrap with DistributedScan when the table is sharded.
    if let Some(schema) = schema_of(table) {
        if !matches!(
            schema.partitioning,
            crate::partition::PartitioningStrategy::None
        ) {
            let router = crate::partition::PartitionRouter::new(Vec::new());
            let remote_workers =
                crate::partition::PartitionPruningRule::prune_workers(
                    &schema, predicate, &router,
                )
                .unwrap_or_default();
            return Ok(PhysicalPlan::DistributedScan {
                table: table.to_string(),
                remote_workers,
                input: Box::new(local),
            });
        }
    }
    Ok(local)
}

fn optimize_table_access_local(
    table: &str,
    residual_filters: &[crate::query::Filter],
    predicate: Option<&Expression>,
    schema_of: &dyn Fn(&str) -> Option<TableSchema>,
    stats_of: &dyn Fn(&str) -> Option<TableStats>,
) -> Result<PhysicalPlan> {
    if let Some(pred) = predicate {
        if let Some(schema) = schema_of(table) {
            // 1. Primary-key equality → O(1) point lookup.
            if let Some(key_value) = match_pk_equality(pred, &schema.primary_key) {
                return Ok(PhysicalPlan::IndexScan {
                    table: table.to_string(),
                    index: None,
                    index_column: schema.primary_key.clone(),
                    key_value,
                });
            }
            // 2. Secondary-index equality when cheaper than a full scan.
            if let Some((idx_name, col, key_value)) =
                match_secondary_index_equality(pred, &schema, stats_of(table).as_ref())
            {
                let scan = PhysicalPlan::IndexScan {
                    table: table.to_string(),
                    index: Some(idx_name),
                    index_column: col.clone(),
                    key_value,
                };
                // Keep a Filter when the predicate is compound (AND / residual).
                if !is_bare_column_equality(pred, &col) {
                    return Ok(PhysicalPlan::Filter {
                        input: Box::new(scan),
                        predicate: pred.clone(),
                    });
                }
                return Ok(scan);
            }
        }
        Ok(PhysicalPlan::Filter {
            input: Box::new(PhysicalPlan::TableScan {
                table: table.to_string(),
                filters: residual_filters.to_vec(),
            }),
            predicate: pred.clone(),
        })
    } else {
        Ok(PhysicalPlan::TableScan {
            table: table.to_string(),
            filters: residual_filters.to_vec(),
        })
    }
}

/// VectorIndexSelectionRule: `ORDER BY col <-> query [ASC] LIMIT k` → HNSW scan.
fn try_vector_index_selection(
    input: &LogicalPlan,
    skip: usize,
    fetch: Option<usize>,
    schema_of: &dyn Fn(&str) -> Option<TableSchema>,
) -> Option<PhysicalPlan> {
    let fetch = fetch?;
    let LogicalPlan::Sort { input: sort_in, exprs } = input else {
        return None;
    };
    if exprs.len() != 1 || !exprs[0].asc {
        return None;
    }
    let Expression::VectorDistance {
        left,
        right,
        metric: _,
    } = &exprs[0].expr
    else {
        return None;
    };
    let Expression::Column(col) = left.as_ref() else {
        return None;
    };
    // Unwrap optional Filter-free Select (or Select with empty filters).
    let table = match sort_in.as_ref() {
        LogicalPlan::Select {
            table,
            filters,
            predicate,
        } if filters.is_empty() && predicate.is_none() => table.clone(),
        LogicalPlan::Filter { .. } => return None,
        _ => return None,
    };
    let schema = schema_of(&table)?;
    let idx = schema
        .indexes
        .iter()
        .find(|i| i.is_vector() && i.column == *col)?;
    Some(PhysicalPlan::VectorIndexScan {
        table,
        index: idx.name.clone(),
        index_column: col.clone(),
        query: right.as_ref().clone(),
        skip,
        fetch,
    })
}

/// Detect `pk = literal|param` (either operand order) → the key-value expression.
fn match_pk_equality(expr: &Expression, primary_key: &str) -> Option<Expression> {
    match_column_equality(expr, primary_key)
}

/// Pick the cheapest secondary index for an equality predicate when cheaper than a full scan.
///
/// After `ANALYZE`, uses MCV / NDV selectivity ([`TableStats::prefer_index_scan`]).
/// Without column stats, falls back to `eq_cost(index) < row_count`.
fn match_secondary_index_equality(
    expr: &Expression,
    schema: &TableSchema,
    stats: Option<&TableStats>,
) -> Option<(String, String, Expression)> {
    let mut best: Option<(u64, String, String, Expression)> = None;
    for idx in &schema.indexes {
        if idx.is_vector() {
            continue;
        }
        if let Some(key_value) = match_column_equality(expr, &idx.column) {
            let literal = literal_text(&key_value);
            let (prefer, cost) = match stats {
                Some(st) if !st.columns.is_empty() => {
                    let prefer = st.prefer_index_scan(&idx.column, literal.as_deref());
                    let cost = st.eq_rows_for_column(&idx.column, literal.as_deref());
                    (prefer, cost)
                }
                Some(st) => {
                    let cost = st.eq_cost(&idx.name);
                    let prefer = cost < st.row_count.max(1);
                    (prefer, cost)
                }
                // No stats yet — still prefer an indexed equality over a full scan.
                None => (true, 1),
            };
            if prefer {
                let replace = best
                    .as_ref()
                    .map(|(c, _, _, _)| cost < *c)
                    .unwrap_or(true);
                if replace {
                    best = Some((cost, idx.name.clone(), idx.column.clone(), key_value));
                }
            }
        }
    }
    best.map(|(_, name, col, kv)| (name, col, kv))
}

fn literal_text(expr: &Expression) -> Option<String> {
    match expr {
        Expression::Literal(s) => Some(s.clone()),
        _ => None,
    }
}

/// Coarse cardinality estimate for HashJoin build-side selection.
fn estimate_physical_rows(
    plan: &PhysicalPlan,
    stats_of: &dyn Fn(&str) -> Option<TableStats>,
) -> u64 {
    match plan {
        PhysicalPlan::TableScan { table, .. } => stats_of(table)
            .map(|s| s.row_count.max(1))
            .unwrap_or(1_000),
        PhysicalPlan::IndexScan { table, index_column, key_value, .. } => {
            let lit = literal_text(key_value);
            stats_of(table)
                .map(|s| s.eq_rows_for_column(index_column, lit.as_deref()))
                .unwrap_or(1)
        }
        PhysicalPlan::VectorIndexScan { fetch, .. } => *fetch as u64,
        PhysicalPlan::Filter { input, .. } => {
            (estimate_physical_rows(input, stats_of) / 3).max(1)
        }
        PhysicalPlan::Values { rows } => rows.len() as u64,
        PhysicalPlan::NestedLoopJoin { left, right, .. }
        | PhysicalPlan::HashJoin { left, right, .. } => estimate_physical_rows(left, stats_of)
            .saturating_mul(estimate_physical_rows(right, stats_of))
            .max(1),
        PhysicalPlan::Aggregate { input, .. }
        | PhysicalPlan::Sort { input, .. }
        | PhysicalPlan::Limit { input, .. }
        | PhysicalPlan::TopN { input, .. }
        | PhysicalPlan::Update { input, .. }
        | PhysicalPlan::Delete { input, .. } => estimate_physical_rows(input, stats_of),
        PhysicalPlan::Insert { values, .. } => values.len() as u64,
        PhysicalPlan::Analyze { .. } | PhysicalPlan::Vacuum { .. } => 0,
        PhysicalPlan::JitExec { input, .. } => estimate_physical_rows(input, stats_of),
        PhysicalPlan::VectorizedExec { input, .. } => estimate_physical_rows(input, stats_of),
        PhysicalPlan::DistributedScan { input, .. } => estimate_physical_rows(input, stats_of),
    }
}

fn match_column_equality(expr: &Expression, column: &str) -> Option<Expression> {
    match expr {
        Expression::BinaryOp {
            left,
            op: FilterOp::Eq,
            right,
        } => match (left.as_ref(), right.as_ref()) {
            (Expression::Column(col), Expression::Literal(_))
            | (Expression::Column(col), Expression::Parameter(_))
                if col == column =>
            {
                Some((**right).clone())
            }
            (Expression::Literal(_), Expression::Column(col))
            | (Expression::Parameter(_), Expression::Column(col))
                if col == column =>
            {
                Some((**left).clone())
            }
            _ => None,
        },
        // AND: prefer left then right (first matching equality).
        Expression::And { left, right } => match_column_equality(left, column)
            .or_else(|| match_column_equality(right, column)),
        _ => None,
    }
}

fn is_bare_column_equality(expr: &Expression, column: &str) -> bool {
    match expr {
        Expression::BinaryOp {
            left,
            op: FilterOp::Eq,
            right,
        } => match (left.as_ref(), right.as_ref()) {
            (Expression::Column(c), Expression::Literal(_))
            | (Expression::Column(c), Expression::Parameter(_))
            | (Expression::Literal(_), Expression::Column(c))
            | (Expression::Parameter(_), Expression::Column(c)) => c == column,
            _ => false,
        },
        _ => false,
    }
}

/// Build a physical plan that filters an in-memory row set with the Select predicate.
///
/// Used by unit tests and by [`crate::pg::SessionState::execute_with_rows`].
pub fn optimize_with_values(plan: &LogicalPlan, rows: Vec<Record>) -> Result<PhysicalPlan> {
    match plan {
        LogicalPlan::Select { predicate, .. } => {
            let values = PhysicalPlan::Values { rows };
            if let Some(pred) = predicate {
                Ok(PhysicalPlan::Filter {
                    input: Box::new(values),
                    predicate: pred.clone(),
                })
            } else {
                Ok(values)
            }
        }
        LogicalPlan::Aggregate {
            group_exprs,
            aggr_exprs,
            ..
        }
        | LogicalPlan::DistributedAggregate {
            group_exprs,
            aggr_exprs,
            ..
        } => Ok(PhysicalPlan::Aggregate {
            input: Box::new(PhysicalPlan::Values { rows }),
            group_exprs: group_exprs.clone(),
            aggr_exprs: aggr_exprs.clone(),
        }),
        LogicalPlan::Sort { exprs, .. } => Ok(PhysicalPlan::Sort {
            input: Box::new(PhysicalPlan::Values { rows }),
            exprs: exprs.clone(),
        }),
        LogicalPlan::Limit {
            skip, fetch, ..
        } => Ok(PhysicalPlan::Limit {
            input: Box::new(PhysicalPlan::Values { rows }),
            skip: *skip,
            fetch: *fetch,
        }),
        other => Err(TakyonicError::Sql(format!(
            "optimize_with_values expects Select/Aggregate/Sort/Limit, got {other:?}"
        ))),
    }
}

/// Execute a logical plan against an active transaction (Volcano pull).
///
/// Does **not** commit — callers should [`Transaction::commit`] after DML
/// (see [`execute_plan_autocommit`]) or abort after reads.
///
/// Uses the transaction's catalog so PK equality can become an IndexScan.
pub fn execute_plan(
    plan: &LogicalPlan,
    ctx: &ExecutionContext,
    txn: &mut Transaction,
) -> Result<Vec<Record>> {
    let physical = optimize_with_catalog(
        plan,
        &|t| txn.table_schema(t).ok(),
        &|t| Some(txn.engine().table_stats(t)),
    )?;
    let mut exec = open_executor_with_txn(physical, ctx, txn)?;
    collect_rows(exec.as_mut())
}

/// Execute a plan; if it is DML, [`Transaction::commit`] immediately after.
///
/// SELECT/JOIN leave the transaction aborted (snapshot read only).
pub fn execute_plan_autocommit(
    plan: &LogicalPlan,
    ctx: &ExecutionContext,
    mut txn: Transaction,
) -> Result<Vec<Record>> {
    let rows = execute_plan(plan, ctx, &mut txn)?;
    if is_dml_plan(plan) {
        txn.commit()?;
    } else {
        txn.abort();
    }
    Ok(rows)
}

/// Affected-row count from a DML executor's single output row.
pub fn affected_row_count(rows: &[Record]) -> u64 {
    rows.first()
        .and_then(|r| r.get("rows"))
        .and_then(|s| s.parse().ok())
        .unwrap_or(0)
}

/// Convert a SQL [`Value`] into a storage field string.
pub fn value_to_field(v: &Value) -> String {
    match v {
        Value::Null => String::new(),
        Value::Int(n) => n.to_string(),
        Value::Float(f) => f.to_string(),
        Value::String(s) => s.clone(),
        Value::Bool(b) => b.to_string(),
    }
}

/// Materialize INSERT expression rows into [`Record`]s (Smart Client path).
pub fn materialize_insert_records(
    columns: &[String],
    values: &[Vec<Expression>],
    ctx: &ExecutionContext,
) -> Result<Vec<Record>> {
    let empty = Record::new();
    let mut records = Vec::with_capacity(values.len());
    for row in values {
        if row.len() != columns.len() {
            return Err(TakyonicError::Sql(format!(
                "INSERT row has {} values for {} columns",
                row.len(),
                columns.len()
            )));
        }
        let mut record = Record::new();
        for (col, expr) in columns.iter().zip(row.iter()) {
            let v = evaluate(expr, &empty, ctx)?;
            record = record.set(col.clone(), value_to_field(&v));
        }
        records.push(record);
    }
    Ok(records)
}

/// Volcano-style pull iterator over [`Record`]s.
pub trait Executor: Send {
    /// Pull the next row, or `Ok(None)` at end-of-stream.
    fn next_row(&mut self) -> Result<Option<Record>>;

    /// Drain the iterator into a vector.
    fn collect(mut self) -> Result<Vec<Record>>
    where
        Self: Sized,
    {
        let mut out = Vec::new();
        while let Some(row) = self.next_row()? {
            out.push(row);
        }
        Ok(out)
    }
}

/// Open a physical plan without storage — only [`PhysicalPlan::Values`] (and
/// filters/joins over values) can run; [`PhysicalPlan::TableScan`] errors.
pub fn open_executor(
    plan: PhysicalPlan,
    ctx: &ExecutionContext,
) -> Result<Box<dyn Executor>> {
    open_executor_with_storage(plan, ctx, None)
}

/// Open a physical plan with an active MVCC [`Transaction`].
///
/// All [`PhysicalPlan::TableScan`] nodes read through `txn` (snapshot isolation
/// + workspace overlay). Catalog lookups use `txn.table_schema`.
pub fn open_executor_with_txn(
    plan: PhysicalPlan,
    ctx: &ExecutionContext,
    txn: &mut Transaction,
) -> Result<Box<dyn Executor>> {
    open_executor_with_storage(plan, ctx, Some(txn))
}

fn open_executor_with_storage(
    plan: PhysicalPlan,
    ctx: &ExecutionContext,
    mut storage: Option<&mut Transaction>,
) -> Result<Box<dyn Executor>> {
    match plan {
        PhysicalPlan::Values { rows } => Ok(Box::new(ValuesExec { rows, idx: 0 })),
        PhysicalPlan::TableScan { table, filters } => {
            let txn = storage.as_deref_mut().ok_or_else(|| {
                TakyonicError::Sql(format!(
                    "TableScan({table}) requires an active MVCC transaction"
                ))
            })?;
            Ok(Box::new(TableScanExec::open(txn, &table, &filters)?))
        }
        PhysicalPlan::IndexScan {
            table,
            index,
            index_column: _,
            key_value,
        } => {
            let txn = storage.as_deref_mut().ok_or_else(|| {
                TakyonicError::Sql(format!(
                    "IndexScan({table}) requires an active MVCC transaction"
                ))
            })?;
            Ok(Box::new(IndexScanExec::open(
                txn, &table, index.as_deref(), &key_value, ctx,
            )?))
        }
        PhysicalPlan::VectorIndexScan {
            table,
            index,
            index_column: _,
            query,
            skip,
            fetch,
        } => {
            let txn = storage.as_deref_mut().ok_or_else(|| {
                TakyonicError::Sql(format!(
                    "VectorIndexScan({table}) requires an active MVCC transaction"
                ))
            })?;
            Ok(Box::new(VectorIndexScanExec::open(
                txn, &table, &index, &query, skip, fetch, ctx,
            )?))
        }
        PhysicalPlan::NestedLoopJoin {
            left,
            right,
            condition,
            join_type,
        } => {
            if join_type != JoinType::Inner {
                return Err(TakyonicError::Sql(format!(
                    "only INNER nested-loop join is implemented; got {join_type:?}"
                )));
            }
            let left_exec = open_executor_with_storage(*left, ctx, storage.as_deref_mut())?;
            let mut right_exec = open_executor_with_storage(*right, ctx, storage.as_deref_mut())?;
            let right_rows = drain_executor(right_exec.as_mut())?;
            Ok(Box::new(NestedLoopJoin::new(
                left_exec,
                right_rows,
                condition,
                ctx.clone(),
            )))
        }
        PhysicalPlan::HashJoin {
            left,
            right,
            left_key,
            right_key,
            join_type,
        } => {
            match join_type {
                JoinType::Inner | JoinType::Semi | JoinType::Anti => {}
                other => {
                    return Err(TakyonicError::Sql(format!(
                        "hash join supports Inner/Semi/Anti; got {other:?}"
                    )));
                }
            }
            // Semi/Anti: build hash set on the **right** (subquery), probe with left.
            // Inner: build on left, probe with right (historical convention).
            let (build, probe, build_key, probe_key) = if matches!(
                join_type,
                JoinType::Semi | JoinType::Anti
            ) {
                (
                    open_executor_with_storage(*right, ctx, storage.as_deref_mut())?,
                    open_executor_with_storage(*left, ctx, storage)?,
                    right_key,
                    left_key,
                )
            } else {
                (
                    open_executor_with_storage(*left, ctx, storage.as_deref_mut())?,
                    open_executor_with_storage(*right, ctx, storage)?,
                    left_key,
                    right_key,
                )
            };
            Ok(Box::new(HashJoinExec::new(
                build,
                probe,
                build_key,
                probe_key,
                join_type,
                ctx.clone(),
            )))
        }
        PhysicalPlan::Filter { input, predicate } => {
            let predicate = if let Some(txn) = storage.as_deref_mut() {
                rewrite_uncorrelated_subqueries(predicate, ctx, txn)?
            } else {
                predicate
            };
            let child = open_executor_with_storage(*input, ctx, storage)?;
            Ok(Box::new(FilterExec {
                input: child,
                predicate,
                ctx: ctx.clone(),
            }))
        }
        PhysicalPlan::Insert {
            table,
            columns,
            values,
        } => {
            let txn = storage.as_deref_mut().ok_or_else(|| {
                TakyonicError::Sql("INSERT requires an active MVCC transaction".into())
            })?;
            Ok(Box::new(InsertExec::run(txn, &table, &columns, &values, ctx)?))
        }
        PhysicalPlan::Update {
            table,
            assignments,
            input,
        } => {
            // Child scan buffers rows and releases the txn borrow before we mutate.
            let mut child = open_executor_with_storage(*input, ctx, storage.as_deref_mut())?;
            let targets = drain_executor(child.as_mut())?;
            let txn = storage.as_deref_mut().ok_or_else(|| {
                TakyonicError::Sql("UPDATE requires an active MVCC transaction".into())
            })?;
            Ok(Box::new(UpdateExec::run(
                txn,
                &table,
                &assignments,
                targets,
                ctx,
            )?))
        }
        PhysicalPlan::Delete { table, input } => {
            let mut child = open_executor_with_storage(*input, ctx, storage.as_deref_mut())?;
            let targets = drain_executor(child.as_mut())?;
            let txn = storage.as_deref_mut().ok_or_else(|| {
                TakyonicError::Sql("DELETE requires an active MVCC transaction".into())
            })?;
            Ok(Box::new(DeleteExec::run(txn, &table, targets)?))
        }
        PhysicalPlan::Aggregate {
            input,
            group_exprs,
            aggr_exprs,
        } => {
            let child = open_executor_with_storage(*input, ctx, storage)?;
            Ok(Box::new(AggregateExec::new(
                child,
                group_exprs,
                aggr_exprs,
                ctx.clone(),
            )?))
        }
        PhysicalPlan::Sort { input, exprs } => {
            let child = open_executor_with_storage(*input, ctx, storage)?;
            Ok(Box::new(SortExec::new(child, exprs, ctx.clone())))
        }
        PhysicalPlan::Limit {
            input,
            skip,
            fetch,
        } => {
            let child = open_executor_with_storage(*input, ctx, storage)?;
            Ok(Box::new(LimitExec::new(child, skip, fetch)))
        }
        PhysicalPlan::TopN {
            input,
            exprs,
            skip,
            fetch,
        } => {
            let child = open_executor_with_storage(*input, ctx, storage)?;
            Ok(Box::new(TopNExec::new(
                child,
                exprs,
                skip,
                fetch,
                ctx.clone(),
            )))
        }
        PhysicalPlan::Analyze { table } => {
            let Some(txn) = storage.as_deref_mut() else {
                return Err(TakyonicError::Sql(
                    "ANALYZE requires an active MVCC transaction".into(),
                ));
            };
            Ok(Box::new(AnalyzeExec::open(table, txn)?))
        }
        PhysicalPlan::Vacuum { table } => {
            let Some(txn) = storage.as_deref_mut() else {
                return Err(TakyonicError::Sql(
                    "VACUUM requires an active MVCC transaction".into(),
                ));
            };
            Ok(Box::new(VacuumExec::open(table, txn)?))
        }
        PhysicalPlan::JitExec {
            input,
            predicate,
            group_exprs,
            aggr_exprs,
        } => {
            let metrics = storage
                .as_ref()
                .map(|t| Arc::clone(t.engine().metrics()));
            let child = open_executor_with_storage(*input, ctx, storage)?;
            Ok(Box::new(JitPipelineExec::try_new(
                child,
                predicate,
                group_exprs,
                aggr_exprs,
                ctx.clone(),
                metrics,
            )?))
        }
        PhysicalPlan::VectorizedExec {
            input,
            predicate,
            aggr_exprs,
        } => {
            let metrics = storage
                .as_ref()
                .map(|t| Arc::clone(t.engine().metrics()));
            let child = open_executor_with_storage(*input, ctx, storage)?;
            Ok(Box::new(VectorizedPipelineExec::try_new(
                child,
                predicate,
                aggr_exprs,
                metrics,
            )?))
        }
        PhysicalPlan::DistributedScan { input, .. } => {
            // Local execution uses the pruned access path; remote dispatch is
            // handled by the MPP coordinator.
            open_executor_with_storage(*input, ctx, storage)
        }
    }
}

/// Drain any executor (including `Box<dyn Executor>`) into a vector.
pub fn collect_rows(exec: &mut dyn Executor) -> Result<Vec<Record>> {
    drain_executor(exec)
}

fn drain_executor(exec: &mut dyn Executor) -> Result<Vec<Record>> {
    let mut out = Vec::new();
    while let Some(row) = exec.next_row()? {
        out.push(row);
    }
    Ok(out)
}

fn affected_rows_record(count: u64) -> Record {
    Record::new().set("rows", count.to_string())
}

/// Yields a single `{ rows: N }` record then ends.
pub struct AffectedRowsExec {
    row: Option<Record>,
}

impl AffectedRowsExec {
    fn new(count: u64) -> Self {
        Self {
            row: Some(affected_rows_record(count)),
        }
    }
}

impl Executor for AffectedRowsExec {
    fn next_row(&mut self) -> Result<Option<Record>> {
        Ok(self.row.take())
    }
}

/// Evaluate INSERT VALUES and write via [`Transaction::put_record`].
pub struct InsertExec;

impl InsertExec {
    /// Run the insert and return an executor that yields the affected-row count.
    pub fn run(
        txn: &mut Transaction,
        table: &str,
        columns: &[String],
        values: &[Vec<Expression>],
        ctx: &ExecutionContext,
    ) -> Result<AffectedRowsExec> {
        let schema = txn.table_schema(table)?;
        let records = materialize_insert_records(columns, values, ctx)?;
        for record in &records {
            validate_record_against_catalog(record, &schema)?;
            txn.put_record(table, record.clone())?;
        }
        Ok(AffectedRowsExec::new(records.len() as u64))
    }
}

/// Apply SET assignments to target rows and rewrite via [`Transaction::put_record`].
pub struct UpdateExec;

impl UpdateExec {
    /// Run the update and return an executor that yields the affected-row count.
    pub fn run(
        txn: &mut Transaction,
        table: &str,
        assignments: &HashMap<String, Expression>,
        targets: Vec<Record>,
        ctx: &ExecutionContext,
    ) -> Result<AffectedRowsExec> {
        let schema = txn.table_schema(table)?;
        let mut count = 0u64;
        for mut row in targets {
            for (col, expr) in assignments {
                let v = evaluate(expr, &row, ctx)?;
                row = row.set(col.clone(), value_to_field(&v));
            }
            validate_record_against_catalog(&row, &schema)?;
            txn.put_record(table, row)?;
            count += 1;
        }
        Ok(AffectedRowsExec::new(count))
    }
}

/// Delete target rows via [`Transaction::delete_record`] (tombstones).
pub struct DeleteExec;

impl DeleteExec {
    /// Run the delete and return an executor that yields the affected-row count.
    pub fn run(
        txn: &mut Transaction,
        table: &str,
        targets: Vec<Record>,
    ) -> Result<AffectedRowsExec> {
        let schema = txn.table_schema(table)?;
        let mut count = 0u64;
        for row in targets {
            let pk = row.get(&schema.primary_key).ok_or_else(|| {
                TakyonicError::Sql(format!(
                    "DELETE row missing primary key `{}`",
                    schema.primary_key
                ))
            })?;
            txn.delete_record(table, pk)?;
            count += 1;
        }
        Ok(AffectedRowsExec::new(count))
    }
}

fn validate_record_against_catalog(record: &Record, schema: &TableSchema) -> Result<()> {
    if record.get(&schema.primary_key).is_none() {
        return Err(TakyonicError::Sql(format!(
            "record missing primary key `{}`",
            schema.primary_key
        )));
    }
    for idx in &schema.indexes {
        if record.get(&idx.column).is_none() {
            return Err(TakyonicError::Sql(format!(
                "record missing indexed column `{}`",
                idx.column
            )));
        }
    }
    Ok(())
}

/// `ANALYZE` physical operator: scan the table, compute stats, persist to catalog.
pub struct AnalyzeExec {
    result: Option<Record>,
}

impl AnalyzeExec {
    /// Scan `table` under `txn`, write stats via the engine, emit one summary row.
    pub fn open(table: String, txn: &mut Transaction) -> Result<Self> {
        let schema = txn.table_schema(&table)?;
        let records = txn.scan_table_records(&table)?;
        let stats = crate::stats::compute_table_stats(&schema, &records);
        txn.engine()
            .apply_analyzed_stats(&table, stats.clone())?;
        let row = Record::new()
            .set("table", table)
            .set("rows", stats.row_count.to_string())
            .set("pages", stats.page_count.to_string());
        Ok(Self {
            result: Some(row),
        })
    }
}

impl Executor for AnalyzeExec {
    fn next_row(&mut self) -> Result<Option<Record>> {
        Ok(self.result.take())
    }
}

/// `VACUUM` physical operator: reclaim dead MVCC versions under the watermark.
pub struct VacuumExec {
    result: Option<Record>,
}

impl VacuumExec {
    /// Run engine Vacuum for `table` and emit one summary row.
    pub fn open(table: String, txn: &mut Transaction) -> Result<Self> {
        let stats = txn.vacuum_table(&table)?;
        let row = Record::new()
            .set("table", table)
            .set("watermark", stats.watermark.to_string())
            .set("removed", stats.memtable_removed.to_string())
            .set("versions_before", stats.versions_before.to_string())
            .set("versions_after", stats.versions_after.to_string())
            .set("dead_heap", stats.dead_heap_versions.to_string())
            .set("dead_index", stats.dead_index_versions.to_string());
        Ok(Self {
            result: Some(row),
        })
    }
}

impl Executor for VacuumExec {
    fn next_row(&mut self) -> Result<Option<Record>> {
        Ok(self.result.take())
    }
}

/// Materialized in-memory scan.
pub(crate) struct ValuesExec {
    rows: Vec<Record>,
    idx: usize,
}

impl ValuesExec {
    pub(crate) fn new(rows: Vec<Record>) -> Self {
        Self { rows, idx: 0 }
    }
}

impl Executor for ValuesExec {
    fn next_row(&mut self) -> Result<Option<Record>> {
        if self.idx >= self.rows.len() {
            return Ok(None);
        }
        let row = self.rows[self.idx].clone();
        self.idx += 1;
        Ok(Some(row))
    }
}

/// Storage-backed table scan: MVCC prefix walk → deserialize → residual filters.
///
/// Rows are buffered at open (LSM prefix scan + per-key `Transaction::get`) so
/// the rest of the Volcano tree can stay free of `&mut Transaction` lifetimes.
pub struct TableScanExec {
    rows: Vec<Record>,
    idx: usize,
}

impl TableScanExec {
    /// Open a scan for `table` through `txn`, applying residual literal filters.
    pub fn open(
        txn: &mut Transaction,
        table: &str,
        filters: &[crate::query::Filter],
    ) -> Result<Self> {
        let schema = txn.table_schema(table)?;
        let mut rows = txn.scan_table_records(table)?;
        // Schema-aware view: ensure PK is present; coerce field display via catalog.
        rows = rows
            .into_iter()
            .filter_map(|r| match deserialize_storage_row(&r, &schema) {
                Ok(row) => Some(row),
                Err(_) => None,
            })
            .filter(|r| filters.iter().all(|f| matches_filter(r, f)))
            .collect();
        Ok(Self { rows, idx: 0 })
    }
}

impl Executor for TableScanExec {
    fn next_row(&mut self) -> Result<Option<Record>> {
        if self.idx >= self.rows.len() {
            return Ok(None);
        }
        let row = self.rows[self.idx].clone();
        self.idx += 1;
        Ok(Some(row))
    }
}

/// Index lookup via PK point-get or secondary index → PK → data (two-step).
///
/// Key expression is evaluated at open (bind params resolved). Secondary scans
/// may yield multiple rows; PK scans yield at most one.
pub struct IndexScanExec {
    rows: Vec<Record>,
    idx: usize,
}

impl IndexScanExec {
    /// Evaluate `key_value` and materialize matching rows.
    pub fn open(
        txn: &mut Transaction,
        table: &str,
        index: Option<&str>,
        key_value: &Expression,
        ctx: &ExecutionContext,
    ) -> Result<Self> {
        let schema = txn.table_schema(table)?;
        let empty = Record::new();
        let key_val = evaluate(key_value, &empty, ctx)?;
        let key_text = value_to_field(&key_val);
        let rows = match index {
            None => {
                // Primary-key point lookup.
                match txn.get_record(table, &key_text)? {
                    Some(record) => deserialize_storage_row(&record, &schema)
                        .ok()
                        .into_iter()
                        .collect(),
                    None => Vec::new(),
                }
            }
            Some(index_name) => {
                let mut found = txn.lookup_by_index(table, index_name, &key_text)?;
                for row in &mut found {
                    *row = deserialize_storage_row(row, &schema)?;
                }
                found
            }
        };
        Ok(Self { rows, idx: 0 })
    }
}

impl Executor for IndexScanExec {
    fn next_row(&mut self) -> Result<Option<Record>> {
        if self.idx >= self.rows.len() {
            return Ok(None);
        }
        let row = self.rows[self.idx].clone();
        self.idx += 1;
        Ok(Some(row))
    }
}

/// HNSW k-NN scan: graph search → PK fetch → ordered rows.
pub struct VectorIndexScanExec {
    rows: Vec<Record>,
    idx: usize,
}

impl VectorIndexScanExec {
    /// Search the HNSW index and materialize the top-`(skip+fetch)` neighbours.
    pub fn open(
        txn: &mut Transaction,
        table: &str,
        index: &str,
        query: &Expression,
        skip: usize,
        fetch: usize,
        ctx: &ExecutionContext,
    ) -> Result<Self> {
        let schema = txn.table_schema(table)?;
        let empty = Record::new();
        let qv = evaluate_as_vector(query, &empty, ctx)?;
        let need = skip.saturating_add(fetch);
        let hits = txn.engine().hnsw_search(index, &qv, need)?;
        let mut rows = Vec::new();
        for (_dist, pk) in hits.into_iter().skip(skip).take(fetch) {
            if let Some(record) = txn.get_record(table, &pk)? {
                rows.push(deserialize_storage_row(&record, &schema)?);
            }
        }
        Ok(Self { rows, idx: 0 })
    }
}

impl Executor for VectorIndexScanExec {
    fn next_row(&mut self) -> Result<Option<Record>> {
        if self.idx >= self.rows.len() {
            return Ok(None);
        }
        let row = self.rows[self.idx].clone();
        self.idx += 1;
        Ok(Some(row))
    }
}

/// Human-readable physical plan tree for `EXPLAIN`.
pub fn explain_physical(plan: &PhysicalPlan) -> String {
    fn walk(plan: &PhysicalPlan, indent: usize, out: &mut String) {
        let pad = "  ".repeat(indent);
        match plan {
            PhysicalPlan::TableScan { table, .. } => {
                let _ = writeln!(out, "{pad}TableScan({table})");
            }
            PhysicalPlan::IndexScan {
                table,
                index,
                index_column,
                ..
            } => match index {
                Some(idx) => {
                    let _ = writeln!(
                        out,
                        "{pad}IndexScan({idx}) on {table}.{index_column}"
                    );
                }
                None => {
                    let _ = writeln!(out, "{pad}IndexScan(pk) on {table}.{index_column}");
                }
            },
            PhysicalPlan::VectorIndexScan {
                table,
                index,
                index_column,
                fetch,
                skip,
                ..
            } => {
                let _ = writeln!(
                    out,
                    "{pad}VectorIndexScanExec({index}) on {table}.{index_column} k={fetch} skip={skip}"
                );
            },
            PhysicalPlan::Filter { input, .. } => {
                let _ = writeln!(out, "{pad}Filter");
                walk(input, indent + 1, out);
            }
            PhysicalPlan::Values { rows } => {
                let _ = writeln!(out, "{pad}Values(rows={})", rows.len());
            }
            PhysicalPlan::NestedLoopJoin { left, right, .. } => {
                let _ = writeln!(out, "{pad}NestedLoopJoin");
                walk(left, indent + 1, out);
                walk(right, indent + 1, out);
            }
            PhysicalPlan::HashJoin {
                left,
                right,
                join_type,
                ..
            } => {
                let label = match join_type {
                    JoinType::Semi => "HashSemiJoin",
                    JoinType::Anti => "HashAntiJoin",
                    _ => "HashJoin",
                };
                let _ = writeln!(out, "{pad}{label}");
                walk(left, indent + 1, out);
                walk(right, indent + 1, out);
            }
            PhysicalPlan::Aggregate { input, .. } => {
                let _ = writeln!(out, "{pad}Aggregate");
                walk(input, indent + 1, out);
            }
            PhysicalPlan::Sort { input, .. } => {
                let _ = writeln!(out, "{pad}Sort");
                walk(input, indent + 1, out);
            }
            PhysicalPlan::Limit {
                input, skip, fetch, ..
            } => {
                let _ = writeln!(out, "{pad}Limit(skip={skip}, fetch={fetch:?})");
                walk(input, indent + 1, out);
            }
            PhysicalPlan::TopN {
                input, skip, fetch, ..
            } => {
                let _ = writeln!(out, "{pad}TopN(skip={skip}, fetch={fetch})");
                walk(input, indent + 1, out);
            }
            PhysicalPlan::Insert { table, .. } => {
                let _ = writeln!(out, "{pad}Insert({table})");
            }
            PhysicalPlan::Update { table, input, .. } => {
                let _ = writeln!(out, "{pad}Update({table})");
                walk(input, indent + 1, out);
            }
            PhysicalPlan::Delete { table, input } => {
                let _ = writeln!(out, "{pad}Delete({table})");
                walk(input, indent + 1, out);
            }
            PhysicalPlan::Analyze { table } => {
                let _ = writeln!(out, "{pad}Analyze({table})");
            }
            PhysicalPlan::Vacuum { table } => {
                let _ = writeln!(out, "{pad}Vacuum({table})");
            }
            PhysicalPlan::JitExec {
                input,
                predicate,
                aggr_exprs,
                ..
            } => {
                let kind = if aggr_exprs.is_empty() {
                    "JitExec(filter)"
                } else {
                    "JitExec(agg)"
                };
                let _ = writeln!(
                    out,
                    "{pad}{kind} pred={} aggr={}",
                    predicate.is_some(),
                    aggr_exprs.len()
                );
                walk(input, indent + 1, out);
            }
            PhysicalPlan::VectorizedExec {
                input,
                predicate,
                aggr_exprs,
            } => {
                let _ = writeln!(
                    out,
                    "{pad}VectorizedExec(simd={}) pred={} aggr={}",
                    crate::vectorized::host_simd_level(),
                    predicate.is_some(),
                    aggr_exprs.len()
                );
                walk(input, indent + 1, out);
            }
            PhysicalPlan::DistributedScan {
                table,
                remote_workers,
                input,
            } => {
                let _ = writeln!(
                    out,
                    "{pad}DistributedScan({table}) workers={}",
                    remote_workers.len()
                );
                for (node, part) in remote_workers {
                    let _ = writeln!(
                        out,
                        "{pad}  RemoteWorker(node={node}, partition={part})"
                    );
                }
                walk(input, indent + 1, out);
            }
        }
    }
    let mut out = String::new();
    walk(plan, 0, &mut out);
    out
}

/// Deserialize a storage [`Record`] into a SQL-oriented row projection.
///
/// Field strings are kept in [`Record`]; callers can project to [`Value`]s via
/// [`record_to_sql_values`]. Schema validates the primary key is present.
pub fn deserialize_storage_row(record: &Record, schema: &TableSchema) -> Result<Record> {
    if record.get(&schema.primary_key).is_none() {
        return Err(TakyonicError::Sql(format!(
            "scanned row missing primary key `{}`",
            schema.primary_key
        )));
    }
    Ok(record.clone())
}

/// Project a [`Record`] into `(column, sql::Value)` pairs for the Volcano/SQL layer.
pub fn record_to_sql_values(record: &Record) -> Vec<(String, Value)> {
    record
        .fields
        .iter()
        .map(|(k, v)| (k.clone(), Value::from_text(v)))
        .collect()
}

/// Filter iterator: yield rows for which `predicate` evaluates to true.
struct FilterExec {
    input: Box<dyn Executor>,
    predicate: Expression,
    ctx: ExecutionContext,
}

impl Executor for FilterExec {
    fn next_row(&mut self) -> Result<Option<Record>> {
        while let Some(row) = self.input.next_row()? {
            if evaluate_bool(&self.predicate, &row, &self.ctx)? {
                return Ok(Some(row));
            }
        }
        Ok(None)
    }
}

/// Push-based JIT pipeline: one tight loop over the scan with compiled filter/aggregate.
///
/// Keeps Cranelift-compiled predicates / scalar projections in registers via an
/// `i64` column buffer — no Volcano virtual calls between filter and aggregate.
struct JitPipelineExec {
    /// Kept alive for the lifetime of `pred_fn` / `proj_fns` pointers.
    _compiler: JitCompiler,
    pending: Option<Vec<Record>>,
    emit_idx: usize,
}

/// SIMD batch pipeline: drains the scan into [`crate::vectorized::VectorBatch`]s.
struct VectorizedPipelineExec {
    pending: Option<Vec<Record>>,
    emit_idx: usize,
}

impl VectorizedPipelineExec {
    fn try_new(
        input: Box<dyn Executor>,
        predicate: Option<Expression>,
        aggr_exprs: Vec<Expression>,
        metrics: Option<Arc<EngineMetrics>>,
    ) -> Result<Self> {
        use crate::vectorized::{
            VectorizedAggregateExec, collect_vector_columns, is_vectorizable,
        };

        // Filter-only: run masked batch scan and emit compacted rows.
        if aggr_exprs.is_empty() {
            let mut cols = Vec::new();
            if let Some(p) = &predicate {
                cols.extend(collect_vector_columns(p));
            }
            let mut scan = crate::vectorized::VectorizedScanExec::new(input, cols);
            let mut out = Vec::new();
            while let Some(mut batch) = scan.next_batch()? {
                if let Some(pred) = &predicate {
                    let bits = crate::vectorized::eval_predicate_mask(pred, &batch)?;
                    batch.apply_mask_bits(&bits);
                }
                out.extend(batch.compact().to_records());
            }
            if let Some(m) = &metrics {
                m.record_jit_execution();
            }
            return Ok(Self {
                pending: Some(out),
                emit_idx: 0,
            });
        }

        // Single SUM / COUNT global aggregate.
        if aggr_exprs.len() != 1 {
            // Fall back: materialize via interpreted AggregateExec path.
            let mut agg = AggregateExec::new(
                input,
                Vec::new(),
                aggr_exprs,
                ExecutionContext::new(),
            )?;
            let mut rows = Vec::new();
            while let Some(r) = agg.next_row()? {
                rows.push(r);
            }
            return Ok(Self {
                pending: Some(rows),
                emit_idx: 0,
            });
        }

        let aggr = &aggr_exprs[0];
        let (sum_expr, result_name, cols) = match aggr {
            Expression::AggregateFunction { name, args } => {
                let n = name.to_ascii_lowercase();
                let result_name = aggr_output_name(aggr);
                if n == "count" && args.is_empty() {
                    let mut cols = Vec::new();
                    if let Some(p) = &predicate {
                        cols.extend(collect_vector_columns(p));
                    }
                    (None, result_name, cols)
                } else if n == "sum" && args.len() == 1 && is_vectorizable(&args[0]) {
                    let mut cols = collect_vector_columns(&args[0]);
                    if let Some(p) = &predicate {
                        cols.extend(collect_vector_columns(p));
                    }
                    cols.sort();
                    cols.dedup();
                    (Some(args[0].clone()), result_name, cols)
                } else {
                    let mut agg = AggregateExec::new(
                        input,
                        Vec::new(),
                        aggr_exprs,
                        ExecutionContext::new(),
                    )?;
                    let mut rows = Vec::new();
                    while let Some(r) = agg.next_row()? {
                        rows.push(r);
                    }
                    return Ok(Self {
                        pending: Some(rows),
                        emit_idx: 0,
                    });
                }
            }
            _ => {
                return Err(TakyonicError::Sql(
                    "VectorizedExec expects SUM/COUNT aggregate".into(),
                ));
            }
        };

        let mut vex = VectorizedAggregateExec::new(
            input,
            cols,
            predicate,
            sum_expr,
            result_name,
            metrics,
        );
        let rec = vex.run()?;
        Ok(Self {
            pending: Some(vec![rec]),
            emit_idx: 0,
        })
    }
}

impl Executor for VectorizedPipelineExec {
    fn next_row(&mut self) -> Result<Option<Record>> {
        let rows = self.pending.as_ref().expect("populated in try_new");
        if self.emit_idx >= rows.len() {
            return Ok(None);
        }
        let row = rows[self.emit_idx].clone();
        self.emit_idx += 1;
        Ok(Some(row))
    }
}

impl JitPipelineExec {
    fn try_new(
        mut input: Box<dyn Executor>,
        predicate: Option<Expression>,
        group_exprs: Vec<Expression>,
        aggr_exprs: Vec<Expression>,
        ctx: ExecutionContext,
        metrics: Option<Arc<EngineMetrics>>,
    ) -> Result<Self> {
        if !group_exprs.is_empty() {
            // Grouped aggregates stay on AggregateExec for now.
            let child = input;
            let mut agg = AggregateExec::new(child, group_exprs, aggr_exprs, ctx)?;
            let mut rows = Vec::new();
            while let Some(r) = agg.next_row()? {
                rows.push(r);
            }
            return Ok(Self {
                _compiler: JitCompiler::new_with_metrics(metrics)?,
                pending: Some(rows),
                emit_idx: 0,
            });
        }

        let compiler = JitCompiler::new_with_metrics(metrics.clone())?;
        let pred_compiled = match &predicate {
            Some(p) => {
                let cols = collect_jit_columns(p);
                compiler.compile_predicate(p, &cols)?
            }
            None => None,
        };

        // Compile each aggregate argument expression (e.g. salary * tax_rate).
        let mut proj_compiled: Vec<(Expression, Option<CompiledFn>)> = Vec::new();
        for aggr in &aggr_exprs {
            if let Expression::AggregateFunction { args, .. } = aggr {
                for a in args {
                    let cols = collect_jit_columns(a);
                    let cf = compiler.compile_scalar(a, &cols)?;
                    proj_compiled.push((a.clone(), cf));
                }
            }
        }

        let metrics_ref = metrics.as_deref();
        let mut scratch = Vec::new();
        let mut filtered_rows = Vec::new();
        let mut accs = if aggr_exprs.is_empty() {
            None
        } else {
            Some(fresh_accumulators(&aggr_exprs)?)
        };

        // Single push loop — HyPer-style data-centric evaluation.
        while let Some(row) = input.next_row()? {
            let pass = match (&predicate, &pred_compiled) {
                (None, _) => true,
                (Some(p), compiled) => jit::evaluate_bool_jit_or_interp_metrics(
                    compiled.as_ref(),
                    p,
                    &row,
                    &ctx,
                    &mut scratch,
                    metrics_ref,
                )?,
            };
            if !pass {
                continue;
            }
            if let Some(accs) = accs.as_mut() {
                let mut arg_idx = 0;
                for (acc, expr) in accs.iter_mut().zip(aggr_exprs.iter()) {
                    let args = match expr {
                        Expression::AggregateFunction { args, .. } => {
                            let mut vals = Vec::with_capacity(args.len());
                            for _ in args {
                                let (a_expr, compiled) = &proj_compiled[arg_idx];
                                arg_idx += 1;
                                vals.push(jit::evaluate_jit_or_interp_metrics(
                                    compiled.as_ref(),
                                    a_expr,
                                    &row,
                                    &ctx,
                                    &mut scratch,
                                    metrics_ref,
                                )?);
                            }
                            vals
                        }
                        _ => Vec::new(),
                    };
                    acc.update(&args)?;
                }
            } else {
                filtered_rows.push(row);
            }
        }

        let pending = if let Some(accs) = accs {
            let mut record = Record::new();
            for (expr, acc) in aggr_exprs.iter().zip(accs.iter()) {
                let name = aggr_output_name(expr);
                let val = acc.evaluate()?;
                record = record.set(&name, value_to_field(&val));
            }
            vec![record]
        } else {
            filtered_rows
        };

        Ok(Self {
            _compiler: compiler,
            pending: Some(pending),
            emit_idx: 0,
        })
    }
}

impl Executor for JitPipelineExec {
    fn next_row(&mut self) -> Result<Option<Record>> {
        let rows = self.pending.as_ref().expect("populated in try_new");
        if self.emit_idx >= rows.len() {
            return Ok(None);
        }
        let row = rows[self.emit_idx].clone();
        self.emit_idx += 1;
        Ok(Some(row))
    }
}

/// Nested-loop join: for every left row, rescan the (materialized) right side.
pub struct NestedLoopJoin {
    left: Box<dyn Executor>,
    right_rows: Vec<Record>,
    condition: Expression,
    ctx: ExecutionContext,
    current_left: Option<Record>,
    right_idx: usize,
}

impl NestedLoopJoin {
    /// Build a nested-loop join over a left pull-iterator and a materialized right.
    pub fn new(
        left: Box<dyn Executor>,
        right_rows: Vec<Record>,
        condition: Expression,
        ctx: ExecutionContext,
    ) -> Self {
        Self {
            left,
            right_rows,
            condition,
            ctx,
            current_left: None,
            right_idx: 0,
        }
    }

    /// Convenience: wrap two in-memory tables (empty parameter context).
    pub fn from_rows(
        left_rows: Vec<Record>,
        right_rows: Vec<Record>,
        condition: Expression,
    ) -> Self {
        Self::new(
            Box::new(ValuesExec {
                rows: left_rows,
                idx: 0,
            }),
            right_rows,
            condition,
            ExecutionContext::new(),
        )
    }
}

impl Executor for NestedLoopJoin {
    fn next_row(&mut self) -> Result<Option<Record>> {
        loop {
            if self.current_left.is_none() {
                self.current_left = self.left.next_row()?;
                self.right_idx = 0;
                if self.current_left.is_none() {
                    return Ok(None);
                }
            }

            let left_row = self
                .current_left
                .as_ref()
                .expect("current_left set above");

            while self.right_idx < self.right_rows.len() {
                let right_row = &self.right_rows[self.right_idx];
                self.right_idx += 1;
                let combined = combine_rows(left_row, right_row);
                if evaluate_bool(&self.condition, &combined, &self.ctx)? {
                    return Ok(Some(combined));
                }
            }

            self.current_left = None;
        }
    }
}

/// Build / probe phase for [`HashJoinExec`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HashJoinPhase {
    /// Still need to drain the build (left) side into the hash table.
    Build,
    /// Probing the right side against the built table.
    Probe,
}

/// Hash equi-join / semi-join / anti-join.
///
/// * [`JoinType::Inner`] — build left, probe right, emit combined rows.
/// * [`JoinType::Semi`] / [`JoinType::Anti`] — build right (subquery keys), probe
///   left, emit left rows that match / do not match (SubqueryUnnestingRule).
pub struct HashJoinExec {
    /// Build-side iterator (taken / drained during the Build phase).
    build: Option<Box<dyn Executor>>,
    /// Probe-side iterator.
    probe: Box<dyn Executor>,
    build_key: Expression,
    probe_key: Expression,
    join_type: JoinType,
    ctx: ExecutionContext,
    phase: HashJoinPhase,
    /// Inner join: build-side hash table.
    hash_table: HashMap<Value, Vec<Record>>,
    /// Semi/Anti: build-side key set.
    hash_set: HashSet<Value>,
    /// Current probe row (Inner only).
    current_probe: Option<Record>,
    /// Matching build rows for `current_probe` (Inner only).
    matches: Vec<Record>,
    /// Index into `matches`.
    match_idx: usize,
}

impl HashJoinExec {
    /// Construct a hash join over build + probe child executors.
    pub fn new(
        build: Box<dyn Executor>,
        probe: Box<dyn Executor>,
        build_key: Expression,
        probe_key: Expression,
        join_type: JoinType,
        ctx: ExecutionContext,
    ) -> Self {
        Self {
            build: Some(build),
            probe,
            build_key,
            probe_key,
            join_type,
            ctx,
            phase: HashJoinPhase::Build,
            hash_table: HashMap::new(),
            hash_set: HashSet::new(),
            current_probe: None,
            matches: Vec::new(),
            match_idx: 0,
        }
    }

    fn build_side(&mut self) -> Result<()> {
        let mut build = self
            .build
            .take()
            .ok_or_else(|| TakyonicError::Sql("hash join build already completed".into()))?;
        while let Some(row) = build.next_row()? {
            let key = evaluate(&self.build_key, &row, &self.ctx)?;
            if matches!(key, Value::Null) {
                continue;
            }
            match self.join_type {
                JoinType::Semi | JoinType::Anti => {
                    self.hash_set.insert(key);
                }
                _ => {
                    self.hash_table.entry(key).or_default().push(row);
                }
            }
        }
        Ok(())
    }
}

impl Executor for HashJoinExec {
    fn next_row(&mut self) -> Result<Option<Record>> {
        if self.phase == HashJoinPhase::Build {
            self.build_side()?;
            self.phase = HashJoinPhase::Probe;
        }

        match self.join_type {
            JoinType::Semi | JoinType::Anti => {
                while let Some(row) = self.probe.next_row()? {
                    let key = evaluate(&self.probe_key, &row, &self.ctx)?;
                    if matches!(key, Value::Null) {
                        continue;
                    }
                    let found = self.hash_set.contains(&key);
                    let keep = match self.join_type {
                        JoinType::Semi => found,
                        JoinType::Anti => !found,
                        _ => unreachable!(),
                    };
                    if keep {
                        return Ok(Some(row));
                    }
                }
                Ok(None)
            }
            _ => loop {
                if self.match_idx < self.matches.len() {
                    let build_row = &self.matches[self.match_idx];
                    self.match_idx += 1;
                    let probe_row = self
                        .current_probe
                        .as_ref()
                        .expect("current_probe set when matches non-empty");
                    return Ok(Some(combine_rows(build_row, probe_row)));
                }

                let Some(probe_row) = self.probe.next_row()? else {
                    return Ok(None);
                };
                let key = evaluate(&self.probe_key, &probe_row, &self.ctx)?;
                self.matches = if matches!(key, Value::Null) {
                    Vec::new()
                } else {
                    self.hash_table.get(&key).cloned().unwrap_or_default()
                };
                self.match_idx = 0;
                self.current_probe = Some(probe_row);
            },
        }
    }
}

/// Stateful aggregator updated row-by-row inside [`AggregateExec`].
pub trait Accumulator: Send {
    /// Fold one row's evaluated argument values into internal state.
    fn update(&mut self, values: &[Value]) -> Result<()>;
    /// Finalize the aggregate value for emission.
    fn evaluate(&self) -> Result<Value>;
}

/// `COUNT(*)` / `COUNT(expr)` — increments for each non-null input (or every row for `*`).
#[derive(Debug, Default)]
pub struct CountAccumulator {
    count: i64,
    /// When true, count every row (COUNT(*)); otherwise skip NULL args.
    star: bool,
}

impl CountAccumulator {
    fn new(star: bool) -> Self {
        Self { count: 0, star }
    }
}

impl Accumulator for CountAccumulator {
    fn update(&mut self, values: &[Value]) -> Result<()> {
        if self.star {
            self.count += 1;
            return Ok(());
        }
        let v = values.first().unwrap_or(&Value::Null);
        if !matches!(v, Value::Null) {
            self.count += 1;
        }
        Ok(())
    }

    fn evaluate(&self) -> Result<Value> {
        Ok(Value::Int(self.count))
    }
}

/// `SUM(expr)` over integer-coercible values.
#[derive(Debug, Default)]
pub struct SumAccumulator {
    sum: i64,
    seen: bool,
}

impl Accumulator for SumAccumulator {
    fn update(&mut self, values: &[Value]) -> Result<()> {
        let Some(v) = values.first() else {
            return Ok(());
        };
        if matches!(v, Value::Null) {
            return Ok(());
        }
        let n = value_as_i64(v)?;
        self.sum = self.sum.saturating_add(n);
        self.seen = true;
        Ok(())
    }

    fn evaluate(&self) -> Result<Value> {
        if self.seen {
            Ok(Value::Int(self.sum))
        } else {
            Ok(Value::Null)
        }
    }
}

/// `AVG(expr)` — tracks sum + count; evaluates to integer `sum / count`.
#[derive(Debug, Default)]
pub struct AvgAccumulator {
    sum: i64,
    count: i64,
}

impl Accumulator for AvgAccumulator {
    fn update(&mut self, values: &[Value]) -> Result<()> {
        let Some(v) = values.first() else {
            return Ok(());
        };
        if matches!(v, Value::Null) {
            return Ok(());
        }
        let n = value_as_i64(v)?;
        self.sum = self.sum.saturating_add(n);
        self.count += 1;
        Ok(())
    }

    fn evaluate(&self) -> Result<Value> {
        if self.count == 0 {
            Ok(Value::Null)
        } else {
            Ok(Value::Int(self.sum / self.count))
        }
    }
}

/// `MIN(expr)` over comparable values.
#[derive(Debug, Default)]
pub struct MinAccumulator {
    current: Option<Value>,
}

impl Accumulator for MinAccumulator {
    fn update(&mut self, values: &[Value]) -> Result<()> {
        let Some(v) = values.first() else {
            return Ok(());
        };
        if matches!(v, Value::Null) {
            return Ok(());
        }
        match &self.current {
            None => self.current = Some(v.clone()),
            Some(cur) if value_less(v, cur) => self.current = Some(v.clone()),
            _ => {}
        }
        Ok(())
    }

    fn evaluate(&self) -> Result<Value> {
        Ok(self.current.clone().unwrap_or(Value::Null))
    }
}

/// `MAX(expr)` over comparable values.
#[derive(Debug, Default)]
pub struct MaxAccumulator {
    current: Option<Value>,
}

impl Accumulator for MaxAccumulator {
    fn update(&mut self, values: &[Value]) -> Result<()> {
        let Some(v) = values.first() else {
            return Ok(());
        };
        if matches!(v, Value::Null) {
            return Ok(());
        }
        match &self.current {
            None => self.current = Some(v.clone()),
            Some(cur) if value_less(cur, v) => self.current = Some(v.clone()),
            _ => {}
        }
        Ok(())
    }

    fn evaluate(&self) -> Result<Value> {
        Ok(self.current.clone().unwrap_or(Value::Null))
    }
}

fn value_as_i64(v: &Value) -> Result<i64> {
    match v {
        Value::Int(n) => Ok(*n),
        Value::Float(f) => Ok(*f as i64),
        Value::String(s) => s
            .parse()
            .map_err(|_| TakyonicError::Sql(format!("cannot cast `{s}` to integer for aggregate"))),
        Value::Bool(b) => Ok(i64::from(*b)),
        Value::Null => Err(TakyonicError::Sql(
            "NULL cannot be coerced to integer".into(),
        )),
    }
}

fn eval_arith(left: &Value, op: crate::sql::ArithOp, right: &Value) -> Result<Value> {
    use crate::sql::ArithOp;
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }
    let use_float = matches!(left, Value::Float(_)) || matches!(right, Value::Float(_));
    if use_float {
        let a = left.as_f64().ok_or_else(|| {
            TakyonicError::Sql(format!("cannot coerce {:?} for arithmetic", left))
        })?;
        let b = right.as_f64().ok_or_else(|| {
            TakyonicError::Sql(format!("cannot coerce {:?} for arithmetic", right))
        })?;
        let r = match op {
            ArithOp::Add => a + b,
            ArithOp::Sub => a - b,
            ArithOp::Mul => a * b,
            ArithOp::Div => {
                if b == 0.0 {
                    return Err(TakyonicError::Sql("division by zero".into()));
                }
                a / b
            }
        };
        return Ok(Value::Float(r));
    }
    let a = value_as_i64(left)?;
    let b = value_as_i64(right)?;
    let r = match op {
        ArithOp::Add => a.saturating_add(b),
        ArithOp::Sub => a.saturating_sub(b),
        ArithOp::Mul => a.saturating_mul(b),
        ArithOp::Div => {
            if b == 0 {
                return Err(TakyonicError::Sql("division by zero".into()));
            }
            a / b
        }
    };
    Ok(Value::Int(r))
}

fn value_less(a: &Value, b: &Value) -> bool {
    compare_sql_values(a, FilterOp::Lt, b)
}

fn new_accumulator(expr: &Expression) -> Result<Box<dyn Accumulator>> {
    let Expression::AggregateFunction { name, args } = expr else {
        return Err(TakyonicError::Sql(format!(
            "expected AggregateFunction, got {expr:?}"
        )));
    };
    match name.as_str() {
        "COUNT" => Ok(Box::new(CountAccumulator::new(args.is_empty()))),
        "SUM" => Ok(Box::new(SumAccumulator::default())),
        "AVG" => Ok(Box::new(AvgAccumulator::default())),
        "MIN" => Ok(Box::new(MinAccumulator::default())),
        "MAX" => Ok(Box::new(MaxAccumulator::default())),
        other => Err(TakyonicError::Sql(format!(
            "unsupported aggregate function `{other}`"
        ))),
    }
}

fn group_output_name(expr: &Expression, idx: usize) -> String {
    match expr {
        Expression::Column(c) => c.clone(),
        _ => format!("group_{idx}"),
    }
}

fn aggr_output_name(expr: &Expression) -> String {
    aggregate_result_column(expr).unwrap_or_else(|| "aggr".into())
}

/// Build a fresh accumulator vector matching `aggr_exprs`.
fn fresh_accumulators(aggr_exprs: &[Expression]) -> Result<Vec<Box<dyn Accumulator>>> {
    aggr_exprs.iter().map(new_accumulator).collect()
}

/// Hash-aggregate physical operator (blocking / pipeline-breaking).
///
/// First [`Executor::next_row`] drains the child completely, then subsequent
/// calls emit one output row per group (or a single global aggregate row).
pub struct AggregateExec {
    input: Box<dyn Executor>,
    group_exprs: Vec<Expression>,
    aggr_exprs: Vec<Expression>,
    ctx: ExecutionContext,
    /// `None` until accumulation finishes; then remaining rows to emit.
    pending: Option<Vec<Record>>,
    emit_idx: usize,
}

impl AggregateExec {
    /// Construct an aggregate executor over `input`.
    pub fn new(
        input: Box<dyn Executor>,
        group_exprs: Vec<Expression>,
        aggr_exprs: Vec<Expression>,
        ctx: ExecutionContext,
    ) -> Result<Self> {
        for expr in &aggr_exprs {
            let _ = new_accumulator(expr)?;
        }
        Ok(Self {
            input,
            group_exprs,
            aggr_exprs,
            ctx,
            pending: None,
            emit_idx: 0,
        })
    }

    fn accumulate(&mut self) -> Result<Vec<Record>> {
        let mut groups: HashMap<Vec<Value>, Vec<Box<dyn Accumulator>>> = HashMap::new();
        let global_key: Vec<Value> = Vec::new();

        if self.group_exprs.is_empty() {
            groups.insert(global_key.clone(), fresh_accumulators(&self.aggr_exprs)?);
        }

        while let Some(row) = self.input.next_row()? {
            let key = if self.group_exprs.is_empty() {
                global_key.clone()
            } else {
                let mut key = Vec::with_capacity(self.group_exprs.len());
                for g in &self.group_exprs {
                    key.push(evaluate(g, &row, &self.ctx)?);
                }
                key
            };
            if !groups.contains_key(&key) {
                groups.insert(key.clone(), fresh_accumulators(&self.aggr_exprs)?);
            }
            let accs = groups.get_mut(&key).expect("just inserted");
            for (acc, expr) in accs.iter_mut().zip(self.aggr_exprs.iter()) {
                let args = match expr {
                    Expression::AggregateFunction { args, .. } => {
                        let mut vals = Vec::with_capacity(args.len());
                        for a in args {
                            vals.push(evaluate(a, &row, &self.ctx)?);
                        }
                        vals
                    }
                    _ => Vec::new(),
                };
                acc.update(&args)?;
            }
        }

        let mut entries: Vec<(Vec<Value>, Vec<Box<dyn Accumulator>>)> =
            groups.into_iter().collect();
        entries.sort_by(|a, b| {
            let sa: Vec<String> = a.0.iter().map(|v| v.to_display()).collect();
            let sb: Vec<String> = b.0.iter().map(|v| v.to_display()).collect();
            sa.cmp(&sb)
        });

        let mut out = Vec::with_capacity(entries.len());
        for (key, accs) in entries {
            let mut record = Record::new();
            for (i, g) in self.group_exprs.iter().enumerate() {
                let name = group_output_name(g, i);
                let val = key.get(i).cloned().unwrap_or(Value::Null);
                record = record.set(&name, value_to_field(&val));
            }
            for (expr, acc) in self.aggr_exprs.iter().zip(accs.iter()) {
                let name = aggr_output_name(expr);
                let val = acc.evaluate()?;
                record = record.set(&name, value_to_field(&val));
            }
            out.push(record);
        }
        Ok(out)
    }
}

impl Executor for AggregateExec {
    fn next_row(&mut self) -> Result<Option<Record>> {
        if self.pending.is_none() {
            self.pending = Some(self.accumulate()?);
            self.emit_idx = 0;
        }
        let rows = self.pending.as_ref().expect("just set");
        if self.emit_idx >= rows.len() {
            return Ok(None);
        }
        let row = rows[self.emit_idx].clone();
        self.emit_idx += 1;
        Ok(Some(row))
    }
}

/// Compare two SQL values for ordering (NULL sorts as empty / lowest).
fn value_ord(a: &Value, b: &Value) -> Ordering {
    if compare_sql_values(a, FilterOp::Lt, b) {
        Ordering::Less
    } else if compare_sql_values(a, FilterOp::Gt, b) {
        Ordering::Greater
    } else {
        Ordering::Equal
    }
}

/// Multi-key sort comparison respecting ASC/DESC per [`SortExpr`].
fn cmp_sort_keys(a: &[Value], b: &[Value], exprs: &[SortExpr]) -> Ordering {
    for (i, se) in exprs.iter().enumerate() {
        let av = a.get(i).unwrap_or(&Value::Null);
        let bv = b.get(i).unwrap_or(&Value::Null);
        let mut c = value_ord(av, bv);
        if !se.asc {
            c = c.reverse();
        }
        if c != Ordering::Equal {
            return c;
        }
    }
    Ordering::Equal
}

fn eval_sort_keys(
    row: &Record,
    exprs: &[SortExpr],
    ctx: &ExecutionContext,
) -> Result<Vec<Value>> {
    let mut keys = Vec::with_capacity(exprs.len());
    for se in exprs {
        keys.push(evaluate(&se.expr, row, ctx)?);
    }
    Ok(keys)
}

/// Blocking sort: drain child, sort in memory, emit.
pub struct SortExec {
    input: Box<dyn Executor>,
    exprs: Vec<SortExpr>,
    ctx: ExecutionContext,
    pending: Option<Vec<Record>>,
    emit_idx: usize,
}

impl SortExec {
    /// Construct a sort executor.
    pub fn new(input: Box<dyn Executor>, exprs: Vec<SortExpr>, ctx: ExecutionContext) -> Self {
        Self {
            input,
            exprs,
            ctx,
            pending: None,
            emit_idx: 0,
        }
    }

    fn materialize(&mut self) -> Result<Vec<Record>> {
        let mut keyed: Vec<(Vec<Value>, Record)> = Vec::new();
        while let Some(row) = self.input.next_row()? {
            let keys = eval_sort_keys(&row, &self.exprs, &self.ctx)?;
            keyed.push((keys, row));
        }
        let exprs = &self.exprs;
        keyed.sort_by(|(a, _), (b, _)| cmp_sort_keys(a, b, exprs));
        Ok(keyed.into_iter().map(|(_, r)| r).collect())
    }
}

impl Executor for SortExec {
    fn next_row(&mut self) -> Result<Option<Record>> {
        if self.pending.is_none() {
            self.pending = Some(self.materialize()?);
            self.emit_idx = 0;
        }
        let rows = self.pending.as_ref().expect("just set");
        if self.emit_idx >= rows.len() {
            return Ok(None);
        }
        let row = rows[self.emit_idx].clone();
        self.emit_idx += 1;
        Ok(Some(row))
    }
}

/// Streaming LIMIT / OFFSET: drop `skip`, then yield at most `fetch`.
pub struct LimitExec {
    input: Box<dyn Executor>,
    skip: usize,
    fetch: Option<usize>,
    skipped: usize,
    yielded: usize,
}

impl LimitExec {
    /// Construct a limit/offset executor.
    pub fn new(input: Box<dyn Executor>, skip: usize, fetch: Option<usize>) -> Self {
        Self {
            input,
            skip,
            fetch,
            skipped: 0,
            yielded: 0,
        }
    }
}

impl Executor for LimitExec {
    fn next_row(&mut self) -> Result<Option<Record>> {
        while self.skipped < self.skip {
            match self.input.next_row()? {
                Some(_) => self.skipped += 1,
                None => return Ok(None),
            }
        }
        if let Some(fetch) = self.fetch {
            if self.yielded >= fetch {
                return Ok(None);
            }
        }
        match self.input.next_row()? {
            Some(row) => {
                self.yielded += 1;
                Ok(Some(row))
            }
            None => Ok(None),
        }
    }
}

/// Heap entry for Top-N: max-heap ordered so the *worst* (last in sort order) is on top.
#[derive(Clone)]
struct TopNEntry {
    keys: Vec<Value>,
    ascending: Vec<bool>,
    row: Record,
}

impl PartialEq for TopNEntry {
    fn eq(&self, other: &Self) -> bool {
        self.keys == other.keys
    }
}

impl Eq for TopNEntry {}

impl PartialOrd for TopNEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TopNEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Match SortExpr ASC/DESC via ascending flags.
        for (i, asc) in self.ascending.iter().enumerate() {
            let av = self.keys.get(i).unwrap_or(&Value::Null);
            let bv = other.keys.get(i).unwrap_or(&Value::Null);
            let mut c = value_ord(av, bv);
            if !asc {
                c = c.reverse();
            }
            if c != Ordering::Equal {
                return c;
            }
        }
        Ordering::Equal
    }
}

/// Fused Sort+Limit: keep only `skip + fetch` best rows in a bounded heap.
pub struct TopNExec {
    input: Box<dyn Executor>,
    exprs: Vec<SortExpr>,
    skip: usize,
    fetch: usize,
    ctx: ExecutionContext,
    pending: Option<Vec<Record>>,
    emit_idx: usize,
}

impl TopNExec {
    /// Construct a Top-N executor.
    pub fn new(
        input: Box<dyn Executor>,
        exprs: Vec<SortExpr>,
        skip: usize,
        fetch: usize,
        ctx: ExecutionContext,
    ) -> Self {
        Self {
            input,
            exprs,
            skip,
            fetch,
            ctx,
            pending: None,
            emit_idx: 0,
        }
    }

    fn materialize(&mut self) -> Result<Vec<Record>> {
        let k = self.skip.saturating_add(self.fetch);
        if k == 0 {
            return Ok(Vec::new());
        }
        let ascending: Vec<bool> = self.exprs.iter().map(|e| e.asc).collect();
        let mut heap: BinaryHeap<TopNEntry> = BinaryHeap::new();

        while let Some(row) = self.input.next_row()? {
            let keys = eval_sort_keys(&row, &self.exprs, &self.ctx)?;
            heap.push(TopNEntry {
                keys,
                ascending: ascending.clone(),
                row,
            });
            if heap.len() > k {
                heap.pop(); // evict worst (last in sort order)
            }
        }

        // into_sorted_vec: least → greatest = best → worst in our Ord.
        let mut entries = heap.into_sorted_vec();
        if self.skip >= entries.len() {
            return Ok(Vec::new());
        }
        entries.drain(0..self.skip);
        entries.truncate(self.fetch);
        Ok(entries.into_iter().map(|e| e.row).collect())
    }
}

impl Executor for TopNExec {
    fn next_row(&mut self) -> Result<Option<Record>> {
        if self.pending.is_none() {
            self.pending = Some(self.materialize()?);
            self.emit_idx = 0;
        }
        let rows = self.pending.as_ref().expect("just set");
        if self.emit_idx >= rows.len() {
            return Ok(None);
        }
        let row = rows[self.emit_idx].clone();
        self.emit_idx += 1;
        Ok(Some(row))
    }
}

fn combine_rows(left: &Record, right: &Record) -> Record {
    let mut out = Record::new();
    for (k, v) in &left.fields {
        out = out.set(k.clone(), v.clone());
    }
    for (k, v) in &right.fields {
        out = out.set(k.clone(), v.clone());
    }
    out
}

/// Evaluate an expression against a row + bind context → [`Value`].
pub fn evaluate(expr: &Expression, row: &Record, ctx: &ExecutionContext) -> Result<Value> {
    match expr {
        Expression::Column(name) => row
            .get(name)
            .map(Value::from_text)
            .ok_or_else(|| TakyonicError::Sql(format!("column `{name}` not found"))),
        Expression::Literal(s) => Ok(Value::from_text(s)),
        Expression::Parameter(idx) => Ok(ctx.param(*idx)?.clone()),
        Expression::BinaryOp { left, op, right } => {
            let lv = evaluate(left, row, ctx)?;
            let rv = evaluate(right, row, ctx)?;
            Ok(Value::Bool(compare_sql_values(&lv, *op, &rv)))
        }
        Expression::And { left, right } => {
            let l = evaluate_bool(left, row, ctx)?;
            let r = evaluate_bool(right, row, ctx)?;
            Ok(Value::Bool(l && r))
        }
        Expression::Or { left, right } => {
            let l = evaluate_bool(left, row, ctx)?;
            let r = evaluate_bool(right, row, ctx)?;
            Ok(Value::Bool(l || r))
        }
        Expression::Arith { left, op, right } => {
            let lv = evaluate(left, row, ctx)?;
            let rv = evaluate(right, row, ctx)?;
            Ok(eval_arith(&lv, *op, &rv)?)
        }
        Expression::InList {
            expr: inner,
            list,
            negated,
        } => {
            let v = evaluate(inner, row, ctx)?;
            if matches!(v, Value::Null) {
                return Ok(Value::Bool(false));
            }
            let found = list.iter().any(|x| values_equal(&v, x));
            Ok(Value::Bool(if *negated { !found } else { found }))
        }
        Expression::InSubquery { .. }
        | Expression::Exists { .. }
        | Expression::ScalarSubquery { .. } => Err(TakyonicError::Sql(
            "subquery expression must be rewritten before scalar evaluate \
             (uncorrelated → InList/Literal; use Filter open / SemiJoin)"
                .into(),
        )),
        Expression::AggregateFunction { name, .. } => Err(TakyonicError::Sql(format!(
            "aggregate `{name}` cannot be evaluated as a scalar expression"
        ))),
        Expression::Array(items) => {
            let mut parts = Vec::with_capacity(items.len());
            for item in items {
                let v = evaluate(item, row, ctx)?;
                parts.push(value_to_field(&v));
            }
            Ok(Value::String(format!("[{}]", parts.join(","))))
        }
        Expression::VectorDistance { left, right, metric } => {
            let lv = evaluate_as_vector(left, row, ctx)?;
            let rv = evaluate_as_vector(right, row, ctx)?;
            // Prefer SIMD Euclidean for the `<->` operator path.
            let d = match metric {
                crate::vector::DistanceMetric::Euclidean => {
                    crate::vector::euclidean_simd(lv.as_slice(), rv.as_slice())
                }
                crate::vector::DistanceMetric::Cosine => lv.cosine_distance(&rv)?,
            };
            Ok(Value::Float(d as f64))
        }
    }
}

/// Evaluate an expression to a [`crate::vector::VectorValue`].
fn evaluate_as_vector(
    expr: &Expression,
    row: &Record,
    ctx: &ExecutionContext,
) -> Result<crate::vector::VectorValue> {
    match expr {
        Expression::Array(_) => {
            let v = evaluate(expr, row, ctx)?;
            let text = match v {
                Value::String(s) => s,
                other => value_to_field(&other),
            };
            crate::vector::VectorValue::from_text(&text)
        }
        Expression::Literal(s) => crate::vector::VectorValue::from_text(s),
        Expression::Column(name) => {
            let text = row.get(name).ok_or_else(|| {
                TakyonicError::Sql(format!("column `{name}` not found"))
            })?;
            crate::vector::VectorValue::from_text(text)
        }
        Expression::Parameter(idx) => {
            let v = ctx.param(*idx)?;
            crate::vector::VectorValue::from_text(&value_to_field(v))
        }
        other => {
            let v = evaluate(other, row, ctx)?;
            crate::vector::VectorValue::from_text(&value_to_field(&v))
        }
    }
}

fn values_equal(a: &Value, b: &Value) -> bool {
    compare_sql_values(a, FilterOp::Eq, b)
}

/// Materialize uncorrelated subqueries inside a predicate into literals / IN-lists.
fn rewrite_uncorrelated_subqueries(
    expr: Expression,
    ctx: &ExecutionContext,
    txn: &mut Transaction,
) -> Result<Expression> {
    match expr {
        Expression::And { left, right } => Ok(Expression::And {
            left: Box::new(rewrite_uncorrelated_subqueries(*left, ctx, txn)?),
            right: Box::new(rewrite_uncorrelated_subqueries(*right, ctx, txn)?),
        }),
        Expression::Or { left, right } => Ok(Expression::Or {
            left: Box::new(rewrite_uncorrelated_subqueries(*left, ctx, txn)?),
            right: Box::new(rewrite_uncorrelated_subqueries(*right, ctx, txn)?),
        }),
        Expression::Arith { left, op, right } => Ok(Expression::Arith {
            left: Box::new(rewrite_uncorrelated_subqueries(*left, ctx, txn)?),
            op,
            right: Box::new(rewrite_uncorrelated_subqueries(*right, ctx, txn)?),
        }),
        Expression::BinaryOp { left, op, right } => Ok(Expression::BinaryOp {
            left: Box::new(rewrite_uncorrelated_subqueries(*left, ctx, txn)?),
            op,
            right: Box::new(rewrite_uncorrelated_subqueries(*right, ctx, txn)?),
        }),
        Expression::InList {
            expr: inner,
            list,
            negated,
        } => Ok(Expression::InList {
            expr: Box::new(rewrite_uncorrelated_subqueries(*inner, ctx, txn)?),
            list,
            negated,
        }),
        Expression::InSubquery {
            expr: inner,
            subquery,
            value_column,
            negated,
            correlated: false,
        } => {
            let list = execute_subquery_column(&subquery, &value_column, ctx, txn)?;
            Ok(Expression::InList {
                expr: inner,
                list,
                negated,
            })
        }
        Expression::Exists {
            subquery,
            negated,
            correlated: false,
        } => {
            let rows = execute_subquery_rows(&subquery, ctx, txn)?;
            let exists = !rows.is_empty();
            let flag = if negated { !exists } else { exists };
            Ok(Expression::Literal(if flag { "true" } else { "false" }.into()))
        }
        Expression::ScalarSubquery {
            subquery,
            value_column,
            correlated: false,
        } => {
            let list = execute_subquery_column(&subquery, &value_column, ctx, txn)?;
            if list.len() > 1 {
                return Err(TakyonicError::Sql(
                    "scalar subquery returned more than one row".into(),
                ));
            }
            let v = list.into_iter().next().unwrap_or(Value::Null);
            Ok(Expression::Literal(v.to_display()))
        }
        // Correlated: Nested-loop Apply — re-evaluate subquery per outer row at filter time.
        Expression::InSubquery {
            expr: inner,
            subquery,
            value_column,
            negated,
            correlated: true,
        } => {
            // Defer: store as-is; FilterExec can't re-run without txn per row.
            // Materialize once using current outer-unaware plan (best-effort), then
            // fall through to InList — true correlation needs OuterRef substitution.
            let list = execute_subquery_column(&subquery, &value_column, ctx, txn)?;
            Ok(Expression::InList {
                expr: inner,
                list,
                negated,
            })
        }
        Expression::Exists {
            subquery,
            negated,
            correlated: true,
        } => {
            let rows = execute_subquery_rows(&subquery, ctx, txn)?;
            let exists = !rows.is_empty();
            let flag = if negated { !exists } else { exists };
            Ok(Expression::Literal(if flag { "true" } else { "false" }.into()))
        }
        Expression::ScalarSubquery {
            subquery,
            value_column,
            correlated: true,
        } => {
            let list = execute_subquery_column(&subquery, &value_column, ctx, txn)?;
            if list.len() > 1 {
                return Err(TakyonicError::Sql(
                    "scalar subquery returned more than one row".into(),
                ));
            }
            let v = list.into_iter().next().unwrap_or(Value::Null);
            Ok(Expression::Literal(v.to_display()))
        }
        other => Ok(other),
    }
}

fn execute_subquery_rows(
    plan: &LogicalPlan,
    ctx: &ExecutionContext,
    txn: &mut Transaction,
) -> Result<Vec<Record>> {
    let physical = optimize_with_catalog(
        plan,
        &|t| txn.table_schema(t).ok(),
        &|_| None,
    )?;
    let mut exec = open_executor_with_txn(physical, ctx, txn)?;
    collect_rows(exec.as_mut())
}

fn execute_subquery_column(
    plan: &LogicalPlan,
    column: &str,
    ctx: &ExecutionContext,
    txn: &mut Transaction,
) -> Result<Vec<Value>> {
    let rows = execute_subquery_rows(plan, ctx, txn)?;
    let mut out = Vec::with_capacity(rows.len());
    for r in rows {
        match r.get(column) {
            Some(v) => out.push(Value::from_text(v)),
            None => {
                // Fall back to first field if column name missing (SELECT alias quirks).
                if let Some((_, v)) = r.fields.iter().next() {
                    out.push(Value::from_text(v));
                }
            }
        }
    }
    Ok(out)
}

/// Evaluate an expression as a boolean predicate.
pub fn evaluate_bool(expr: &Expression, row: &Record, ctx: &ExecutionContext) -> Result<bool> {
    Ok(evaluate(expr, row, ctx)?.is_truthy())
}

/// Evaluate a join predicate (legacy helper; prefers combined-row eval).
pub fn eval_join_predicate(
    expr: &Expression,
    left: &Record,
    right: &Record,
    ctx: &ExecutionContext,
) -> Result<bool> {
    let combined = combine_rows(left, right);
    evaluate_bool(expr, &combined, ctx)
}

fn compare_sql_values(left: &Value, op: FilterOp, right: &Value) -> bool {
    // Numeric path (Int / Float / Bool / numeric String).
    if let (Some(a), Some(b)) = (left.as_f64(), right.as_f64()) {
        let both_numeric = !matches!(left, Value::String(_)) || left.to_display().parse::<f64>().is_ok();
        let both_numeric = both_numeric
            && (!matches!(right, Value::String(_)) || right.to_display().parse::<f64>().is_ok());
        if both_numeric
            && !matches!((left, right), (Value::String(_), Value::String(_)))
        {
            return match op {
                FilterOp::Eq => (a - b).abs() < f64::EPSILON,
                FilterOp::Ne => (a - b).abs() >= f64::EPSILON,
                FilterOp::Gt => a > b,
                FilterOp::Gte => a >= b,
                FilterOp::Lt => a < b,
                FilterOp::Lte => a <= b,
            };
        }
    }
    let ls = left.to_display();
    let rs = right.to_display();
    match op {
        FilterOp::Eq => ls == rs,
        FilterOp::Ne => ls != rs,
        FilterOp::Gt => ls > rs,
        FilterOp::Gte => ls >= rs,
        FilterOp::Lt => ls < rs,
        FilterOp::Lte => ls <= rs,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::engine::TakyonicEngine;
    use crate::query::FilterOp;
    use crate::schema::IndexDef;
    use crate::sql::{Expression, LogicalPlan, LogicalPlanner};
    use std::fs;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_engine(name: &str) -> (Arc<TakyonicEngine>, std::path::PathBuf) {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("takyonic-exec-{name}-{nanos}"));
        let config = Config::default()
            .data_dir(root.join("data"))
            .wal_dir(root.join("wal"))
            .memtable_size_bytes(64 * 1024 * 1024)
            .block_size_bytes(64)
            .l0_soft_limit(8)
            .l0_hard_limit(32)
            .l0_rapid_pool_threads(1)
            .ln_haul_pool_threads(1)
            .compaction_write_bytes_per_sec(1024 * 1024 * 1024)
            .write_admission_ops_per_sec(100_000)
            .write_admission_min_ops_per_sec(1_000)
            .write_admission_burst(10_000);
        let engine = Arc::new(TakyonicEngine::open(config).unwrap());
        (engine, root)
    }

    fn register_users(engine: &TakyonicEngine) {
        engine
            .register_table(TableSchema::new(
                "users",
                "id",
                vec![IndexDef::new("age", "age")],
            ))
            .unwrap();
    }

    fn run_sql(engine: &Arc<TakyonicEngine>, sql: &str) -> Vec<Record> {
        let plan = LogicalPlanner::plan(sql).unwrap();
        let ctx = ExecutionContext::new();
        execute_plan_autocommit(&plan, &ctx, engine.begin().unwrap()).unwrap()
    }

    #[test]
    fn nested_loop_join_counts_matching_rows() {
        let users = vec![
            Record::new().set("id", "1").set("name", "Ada"),
            Record::new().set("id", "2").set("name", "Bob"),
            Record::new().set("id", "3").set("name", "Cy"),
        ];
        let orders = vec![
            Record::new().set("order_id", "10").set("user_id", "1"),
            Record::new().set("order_id", "11").set("user_id", "1"),
            Record::new().set("order_id", "20").set("user_id", "2"),
        ];

        let condition = Expression::BinaryOp {
            left: Box::new(Expression::Column("id".into())),
            op: FilterOp::Eq,
            right: Box::new(Expression::Column("user_id".into())),
        };

        let mut join = NestedLoopJoin::from_rows(users, orders, condition);
        let mut count = 0usize;
        while join.next_row().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 3);
    }

    #[test]
    fn equi_join_optimizes_to_hash_join() {
        let plan = LogicalPlan::Join {
            left: Box::new(LogicalPlan::Select {
                table: "users".into(),
                filters: vec![],
                predicate: None,
            }),
            right: Box::new(LogicalPlan::Select {
                table: "orders".into(),
                filters: vec![],
                predicate: None,
            }),
            on: Expression::BinaryOp {
                left: Box::new(Expression::Column("id".into())),
                op: FilterOp::Eq,
                right: Box::new(Expression::Column("user_id".into())),
            },
            join_type: JoinType::Inner,
        };
        let physical = optimize(&plan).unwrap();
        match physical {
            PhysicalPlan::HashJoin {
                left_key,
                right_key,
                join_type,
                ..
            } => {
                assert_eq!(join_type, JoinType::Inner);
                assert_eq!(left_key, Expression::Column("id".into()));
                assert_eq!(right_key, Expression::Column("user_id".into()));
            }
            other => panic!("expected HashJoin, got {other:?}"),
        }
    }

    #[test]
    fn non_equi_join_falls_back_to_nested_loop() {
        let plan = LogicalPlan::Join {
            left: Box::new(LogicalPlan::Select {
                table: "users".into(),
                filters: vec![],
                predicate: None,
            }),
            right: Box::new(LogicalPlan::Select {
                table: "orders".into(),
                filters: vec![],
                predicate: None,
            }),
            on: Expression::BinaryOp {
                left: Box::new(Expression::Column("id".into())),
                op: FilterOp::Gt,
                right: Box::new(Expression::Column("user_id".into())),
            },
            join_type: JoinType::Inner,
        };
        let physical = optimize(&plan).unwrap();
        match physical {
            PhysicalPlan::NestedLoopJoin { join_type, .. } => {
                assert_eq!(join_type, JoinType::Inner);
            }
            other => panic!("expected NestedLoopJoin, got {other:?}"),
        }
    }

    #[test]
    fn hash_join_users_orders_via_sql() {
        let (engine, root) = temp_engine("hashjoin");
        register_users(&engine);
        engine
            .register_table(TableSchema::new(
                "orders",
                "order_id",
                vec![IndexDef::new("user_id", "user_id")],
            ))
            .unwrap();
        let ctx = ExecutionContext::new();

        execute_plan_autocommit(
            &LogicalPlanner::plan(
                "INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30), (2, 'Bob', 20), (3, 'Cy', 25)",
            )
            .unwrap(),
            &ctx,
            engine.begin().unwrap(),
        )
        .unwrap();
        execute_plan_autocommit(
            &LogicalPlanner::plan(
                "INSERT INTO orders (order_id, user_id) VALUES (10, 1), (11, 1), (20, 2)",
            )
            .unwrap(),
            &ctx,
            engine.begin().unwrap(),
        )
        .unwrap();

        let sql = "SELECT * FROM users JOIN orders ON users.id = orders.user_id";
        let plan = LogicalPlanner::plan(sql).unwrap();
        let users_schema = engine.table_schema("users").unwrap().clone();
        let orders_schema = engine.table_schema("orders").unwrap().clone();
        let physical = optimize_with_catalog(
            &plan,
            &|t| match t {
                "users" => Some(users_schema.clone()),
                "orders" => Some(orders_schema.clone()),
                _ => None,
            },
            &|_| None,
        )
        .unwrap();
        assert!(
            matches!(physical, PhysicalPlan::HashJoin { .. }),
            "expected HashJoin, got {physical:?}"
        );
        assert!(
            !matches!(physical, PhysicalPlan::NestedLoopJoin { .. }),
            "must not use NestedLoopJoin for equi-join"
        );

        let mut txn = engine.begin().unwrap();
        let out = execute_plan(&plan, &ctx, &mut txn).unwrap();
        txn.abort();

        // Ada×2 + Bob×1 = 3 joined rows.
        assert_eq!(out.len(), 3);
        let mut pairs: Vec<(String, String)> = out
            .iter()
            .map(|r| {
                (
                    r.get("name").unwrap().to_string(),
                    r.get("order_id").unwrap().to_string(),
                )
            })
            .collect();
        pairs.sort();
        assert_eq!(
            pairs,
            vec![
                ("Ada".into(), "10".into()),
                ("Ada".into(), "11".into()),
                ("Bob".into(), "20".into()),
            ]
        );

        engine.close().unwrap();
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn parameterized_filter_age_gt_bind_param() {
        let plan = LogicalPlanner::plan("SELECT * FROM users WHERE age > $1").unwrap();
        let ctx = ExecutionContext::with_params(vec![Value::Int(25)]);
        let rows = vec![
            Record::new().set("name", "Ada").set("age", "30"),
            Record::new().set("name", "Bob").set("age", "20"),
            Record::new().set("name", "Cy").set("age", "25"),
            Record::new().set("name", "Di").set("age", "40"),
        ];
        let physical = optimize_with_values(&plan, rows).unwrap();
        let mut exec = open_executor(physical, &ctx).unwrap();
        let out = collect_rows(exec.as_mut()).unwrap();

        assert_eq!(out.len(), 2);
        let names: Vec<_> = out.iter().map(|r| r.get("name").unwrap()).collect();
        assert_eq!(names, vec!["Ada", "Di"]);
    }

    #[test]
    fn tablescan_reads_committed_rows_via_mvcc_txn() {
        let (engine, root) = temp_engine("tablescan");
        register_users(&engine);

        let inserted = run_sql(
            &engine,
            "INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30), (2, 'Bob', 20), (3, 'Cy', 25), (4, 'Di', 40)",
        );
        assert_eq!(affected_row_count(&inserted), 4);

        let plan = LogicalPlanner::plan("SELECT * FROM users WHERE age > $1").unwrap();
        let physical = optimize(&plan).unwrap();
        match &physical {
            PhysicalPlan::Filter { input, .. } => {
                assert!(matches!(input.as_ref(), PhysicalPlan::TableScan { .. }));
            }
            other => panic!("expected Filter(TableScan), got {other:?}"),
        }

        let ctx = ExecutionContext::with_params(vec![Value::Int(25)]);
        let mut txn = engine.begin().unwrap();
        let mut exec = open_executor_with_txn(physical, &ctx, &mut txn).unwrap();
        let out = collect_rows(exec.as_mut()).unwrap();
        txn.abort();

        assert_eq!(out.len(), 2);
        let mut names: Vec<_> = out
            .iter()
            .map(|r| r.get("name").unwrap().to_string())
            .collect();
        names.sort();
        assert_eq!(names, vec!["Ada".to_string(), "Di".to_string()]);

        let projected = record_to_sql_values(&out[0]);
        assert!(projected.iter().any(|(k, _)| k == "age"));

        engine.close().unwrap();
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn tablescan_sees_uncommitted_workspace_inserts() {
        let (engine, root) = temp_engine("workspace");
        register_users(&engine);

        let mut txn = engine.begin().unwrap();
        let plan =
            LogicalPlanner::plan("INSERT INTO users (id, name, age) VALUES (1, 'Workspace', 1)")
                .unwrap();
        let ctx = ExecutionContext::new();
        let rows = execute_plan(&plan, &ctx, &mut txn).unwrap();
        assert_eq!(affected_row_count(&rows), 1);

        let plan = LogicalPlanner::plan("SELECT * FROM users").unwrap();
        let physical = optimize(&plan).unwrap();
        let mut exec = open_executor_with_txn(physical, &ctx, &mut txn).unwrap();
        let out = collect_rows(exec.as_mut()).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].get("name"), Some("Workspace"));
        txn.abort();

        engine.close().unwrap();
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn pk_equality_optimizes_to_index_scan() {
        let (engine, root) = temp_engine("indexscan");
        register_users(&engine);
        let ctx = ExecutionContext::new();

        // Seed via SQL DML (no put_record backdoor).
        let insert = LogicalPlanner::plan(
            "INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30), (2, 'Bob', 20), (3, 'Cy', 25)",
        )
        .unwrap();
        execute_plan_autocommit(&insert, &ctx, engine.begin().unwrap()).unwrap();

        // EXPLAIN-style: physical plan for `WHERE id = $1` must be IndexScan.
        let plan = LogicalPlanner::plan("SELECT * FROM users WHERE id = $1").unwrap();
        let schema = engine.table_schema("users").unwrap().clone();
        let physical = optimize_with_catalog(
            &plan,
            &|t| {
                if t == "users" {
                    Some(schema.clone())
                } else {
                    None
                }
            },
            &|_| None,
        )
        .unwrap();
        match &physical {
            PhysicalPlan::IndexScan {
                table,
                index,
                key_value,
                ..
            } => {
                assert_eq!(table, "users");
                assert!(index.is_none(), "PK IndexScan has no secondary name");
                assert_eq!(*key_value, Expression::Parameter(0));
            }
            other => panic!("expected IndexScan, got {other:?}"),
        }

        // Non-PK predicate must NOT become IndexScan.
        let age_plan = LogicalPlanner::plan("SELECT * FROM users WHERE age > $1").unwrap();
        let age_physical = optimize_with_catalog(
            &age_plan,
            &|t| {
                if t == "users" {
                    Some(schema.clone())
                } else {
                    None
                }
            },
            &|_| None,
        )
        .unwrap();
        match &age_physical {
            PhysicalPlan::Filter { input, .. } => {
                assert!(
                    matches!(input.as_ref(), PhysicalPlan::TableScan { .. }),
                    "expected TableScan child, got {input:?}"
                );
            }
            other => panic!("age filter should remain Filter(TableScan), got {other:?}"),
        }

        // Execute IndexScan point lookup.
        let bind_ctx = ExecutionContext::with_params(vec![Value::Int(1)]);
        let mut txn = engine.begin().unwrap();
        let mut exec = open_executor_with_txn(physical, &bind_ctx, &mut txn).unwrap();
        let out = collect_rows(exec.as_mut()).unwrap();
        txn.abort();

        assert_eq!(out.len(), 1);
        assert_eq!(out[0].get("name"), Some("Ada"));
        assert_eq!(out[0].get("id"), Some("1"));

        // UPDATE/DELETE with PK equality also get IndexScan children.
        let update = LogicalPlanner::plan("UPDATE users SET age = 99 WHERE id = 2").unwrap();
        let update_phys = optimize_with_catalog(
            &update,
            &|t| {
                if t == "users" {
                    Some(schema.clone())
                } else {
                    None
                }
            },
            &|_| None,
        )
        .unwrap();
        match update_phys {
            PhysicalPlan::Update { input, .. } => {
                assert!(
                    matches!(input.as_ref(), PhysicalPlan::IndexScan { .. }),
                    "UPDATE WHERE id= should use IndexScan, got {input:?}"
                );
            }
            other => panic!("expected Update, got {other:?}"),
        }

        let delete = LogicalPlanner::plan("DELETE FROM users WHERE id = 3").unwrap();
        let delete_phys = optimize_with_catalog(
            &delete,
            &|t| {
                if t == "users" {
                    Some(schema.clone())
                } else {
                    None
                }
            },
            &|_| None,
        )
        .unwrap();
        match delete_phys {
            PhysicalPlan::Delete { input, .. } => {
                assert!(
                    matches!(input.as_ref(), PhysicalPlan::IndexScan { .. }),
                    "DELETE WHERE id= should use IndexScan, got {input:?}"
                );
            }
            other => panic!("expected Delete, got {other:?}"),
        }

        engine.close().unwrap();
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn dml_insert_update_delete_then_select() {
        let (engine, root) = temp_engine("dml");
        register_users(&engine);
        let ctx = ExecutionContext::new();

        // 1. INSERT via SQL (no direct put_record).
        let insert = LogicalPlanner::plan(
            "INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30), (2, 'Bob', 20)",
        )
        .unwrap();
        let rows = execute_plan_autocommit(&insert, &ctx, engine.begin().unwrap()).unwrap();
        assert_eq!(affected_row_count(&rows), 2);

        // 2. UPDATE Ada's age.
        let update =
            LogicalPlanner::plan("UPDATE users SET age = 31 WHERE name = 'Ada'").unwrap();
        let rows = execute_plan_autocommit(&update, &ctx, engine.begin().unwrap()).unwrap();
        assert_eq!(affected_row_count(&rows), 1);

        // 3. DELETE Bob (age < 25).
        let delete = LogicalPlanner::plan("DELETE FROM users WHERE age < 25").unwrap();
        let rows = execute_plan_autocommit(&delete, &ctx, engine.begin().unwrap()).unwrap();
        assert_eq!(affected_row_count(&rows), 1);

        // 4. SELECT — only Ada (31) remains.
        let select = LogicalPlanner::plan("SELECT * FROM users").unwrap();
        let out = execute_plan_autocommit(&select, &ctx, engine.begin().unwrap()).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].get("name"), Some("Ada"));
        assert_eq!(out[0].get("age"), Some("31"));

        engine.close().unwrap();
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn group_by_count_sum_via_sql() {
        let (engine, root) = temp_engine("agg");
        engine
            .register_table(TableSchema::new("employees", "id", vec![
                IndexDef::new("department", "department"),
                IndexDef::new("salary", "salary"),
            ]))
            .unwrap();
        let ctx = ExecutionContext::new();

        execute_plan_autocommit(
            &LogicalPlanner::plan(
                "INSERT INTO employees (id, department, salary) VALUES \
                 (1, 'Sales', 5000), (2, 'Sales', 7000), (3, 'Engineering', 9000)",
            )
            .unwrap(),
            &ctx,
            engine.begin().unwrap(),
        )
        .unwrap();

        let sql =
            "SELECT department, COUNT(id), SUM(salary) FROM employees GROUP BY department";
        let plan = LogicalPlanner::plan(sql).unwrap();
        match &plan {
            LogicalPlan::Aggregate {
                group_exprs,
                aggr_exprs,
                ..
            } => {
                assert_eq!(group_exprs, &vec![Expression::Column("department".into())]);
                assert_eq!(aggr_exprs.len(), 2);
                assert!(matches!(
                    &aggr_exprs[0],
                    Expression::AggregateFunction { name, .. } if name == "COUNT"
                ));
                assert!(matches!(
                    &aggr_exprs[1],
                    Expression::AggregateFunction { name, .. } if name == "SUM"
                ));
            }
            other => panic!("expected Aggregate plan, got {other:?}"),
        }

        let schema = engine.table_schema("employees").unwrap().clone();
        let physical = optimize_with_catalog(
            &plan,
            &|t| {
                if t == "employees" {
                    Some(schema.clone())
                } else {
                    None
                }
            },
            &|_| None,
        )
        .unwrap();
        assert!(
            matches!(physical, PhysicalPlan::Aggregate { .. }),
            "expected Aggregate physical plan, got {physical:?}"
        );

        let mut txn = engine.begin().unwrap();
        let out = execute_plan(&plan, &ctx, &mut txn).unwrap();
        txn.abort();

        assert_eq!(out.len(), 2, "expected 2 groups, got {out:?}");
        // Sorted by department: Engineering then Sales.
        assert_eq!(out[0].get("department"), Some("Engineering"));
        assert_eq!(out[0].get("count(id)"), Some("1"));
        assert_eq!(out[0].get("sum(salary)"), Some("9000"));
        assert_eq!(out[1].get("department"), Some("Sales"));
        assert_eq!(out[1].get("count(id)"), Some("2"));
        assert_eq!(out[1].get("sum(salary)"), Some("12000"));

        engine.close().unwrap();
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn group_by_having_filters_groups_via_sql() {
        let (engine, root) = temp_engine("having");
        engine
            .register_table(TableSchema::new(
                "employees",
                "id",
                vec![IndexDef::new("department", "department")],
            ))
            .unwrap();
        let ctx = ExecutionContext::new();

        execute_plan_autocommit(
            &LogicalPlanner::plan(
                "INSERT INTO employees (id, department, salary) VALUES \
                 (1, 'Sales', 5000), (2, 'Sales', 7000), (3, 'Engineering', 9000)",
            )
            .unwrap(),
            &ctx,
            engine.begin().unwrap(),
        )
        .unwrap();

        let sql = "SELECT department, COUNT(*) FROM employees GROUP BY department HAVING COUNT(*) > 1";
        let plan = LogicalPlanner::plan(sql).unwrap();
        assert!(
            matches!(plan, LogicalPlan::Filter { .. }),
            "expected Filter(Aggregate), got {plan:?}"
        );

        let mut txn = engine.begin().unwrap();
        let out = execute_plan(&plan, &ctx, &mut txn).unwrap();
        txn.abort();

        assert_eq!(out.len(), 1, "expected only Sales group, got {out:?}");
        assert_eq!(out[0].get("department"), Some("Sales"));
        assert_eq!(out[0].get("count(*)"), Some("2"));

        engine.close().unwrap();
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn sort_exec_orders_rows_asc_desc() {
        let rows = vec![
            Record::new().set("name", "Cy").set("age", "25"),
            Record::new().set("name", "Ada").set("age", "30"),
            Record::new().set("name", "Bob").set("age", "20"),
        ];
        let ctx = ExecutionContext::new();
        let values = open_executor(
            PhysicalPlan::Values { rows: rows.clone() },
            &ctx,
        )
        .unwrap();
        let mut sort = SortExec::new(
            values,
            vec![SortExpr::asc(Expression::Column("name".into()))],
            ctx.clone(),
        );
        let out = collect_rows(&mut sort).unwrap();
        assert_eq!(
            out.iter().map(|r| r.get("name").unwrap()).collect::<Vec<_>>(),
            vec!["Ada", "Bob", "Cy"]
        );

        let values = open_executor(PhysicalPlan::Values { rows }, &ctx).unwrap();
        let mut sort = SortExec::new(
            values,
            vec![SortExpr::desc(Expression::Column("age".into()))],
            ctx,
        );
        let out = collect_rows(&mut sort).unwrap();
        assert_eq!(
            out.iter().map(|r| r.get("age").unwrap()).collect::<Vec<_>>(),
            vec!["30", "25", "20"]
        );
    }

    #[test]
    fn limit_exec_skip_and_fetch() {
        let rows: Vec<_> = (1..=10)
            .map(|i| Record::new().set("id", i.to_string()))
            .collect();
        let ctx = ExecutionContext::new();
        let values = open_executor(PhysicalPlan::Values { rows }, &ctx).unwrap();
        let mut lim = LimitExec::new(values, 3, Some(2));
        let out = collect_rows(&mut lim).unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].get("id"), Some("4"));
        assert_eq!(out[1].get("id"), Some("5"));
    }

    #[test]
    fn top_n_optimizer_fuses_sort_limit() {
        let plan = LogicalPlanner::plan(
            "SELECT department, SUM(salary) FROM employees GROUP BY department \
             ORDER BY SUM(salary) DESC LIMIT 1",
        )
        .unwrap();
        let physical = optimize(&plan).unwrap();
        match physical {
            PhysicalPlan::TopN {
                skip: 0,
                fetch: 1,
                exprs,
                ..
            } => {
                assert_eq!(exprs.len(), 1);
                assert!(!exprs[0].asc);
                assert_eq!(exprs[0].expr, Expression::Column("sum(salary)".into()));
            }
            other => panic!("expected TopN fusion, got {other:?}"),
        }
    }

    #[test]
    fn group_by_order_by_limit_topn_e2e() {
        let (engine, root) = temp_engine("topn");
        engine
            .register_table(TableSchema::new(
                "employees",
                "id",
                vec![
                    IndexDef::new("department", "department"),
                    IndexDef::new("salary", "salary"),
                ],
            ))
            .unwrap();
        let ctx = ExecutionContext::new();

        execute_plan_autocommit(
            &LogicalPlanner::plan(
                "INSERT INTO employees (id, department, salary) VALUES \
                 (1, 'Sales', 5000), (2, 'Sales', 7000), (3, 'Engineering', 9000)",
            )
            .unwrap(),
            &ctx,
            engine.begin().unwrap(),
        )
        .unwrap();

        let sql = "SELECT department, SUM(salary) FROM employees GROUP BY department \
                   ORDER BY SUM(salary) DESC LIMIT 1";
        let plan = LogicalPlanner::plan(sql).unwrap();
        let physical =
            optimize_with_catalog(&plan, &|t| engine.table_schema(t).ok(), &|_| None).unwrap();
        assert!(
            matches!(physical, PhysicalPlan::TopN { fetch: 1, skip: 0, .. }),
            "CBO must fuse Sort+Limit into TopN, got {physical:?}"
        );

        let mut txn = engine.begin().unwrap();
        let out = execute_plan(&plan, &ctx, &mut txn).unwrap();
        txn.abort();

        assert_eq!(out.len(), 1);
        assert_eq!(out[0].get("department"), Some("Sales"));
        assert_eq!(out[0].get("sum(salary)"), Some("12000"));

        engine.close().unwrap();
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn secondary_index_equality_optimizes_to_index_scan() {
        let (engine, root) = temp_engine("sec-idx-cbo");
        engine
            .register_table(TableSchema::new("employees", "id", vec![]))
            .unwrap();
        let ctx = ExecutionContext::new();

        execute_plan_autocommit(
            &LogicalPlanner::plan(
                "INSERT INTO employees (id, department, salary) VALUES \
                 (1, 'Sales', 5000), (2, 'Sales', 7000), (3, 'Engineering', 9000)",
            )
            .unwrap(),
            &ctx,
            engine.begin().unwrap(),
        )
        .unwrap();

        engine
            .create_index("idx_dept", "employees", "department", false)
            .unwrap();

        let plan =
            LogicalPlanner::plan("SELECT * FROM employees WHERE department = 'Engineering'")
                .unwrap();
        let physical = optimize_with_catalog(
            &plan,
            &|t| engine.table_schema(t).ok(),
            &|t| Some(engine.table_stats(t)),
        )
        .unwrap();
        match &physical {
            PhysicalPlan::IndexScan {
                table,
                index,
                index_column,
                key_value,
            } => {
                assert_eq!(table, "employees");
                assert_eq!(index.as_deref(), Some("idx_dept"));
                assert_eq!(index_column, "department");
                assert_eq!(
                    *key_value,
                    Expression::Literal("Engineering".into())
                );
            }
            other => panic!("expected secondary IndexScan, got {other:?}"),
        }

        let explain = explain_physical(&physical);
        assert!(
            explain.contains("IndexScan(idx_dept)"),
            "EXPLAIN must name the secondary index, got:\n{explain}"
        );

        let mut txn = engine.begin().unwrap();
        let out = execute_plan(&plan, &ctx, &mut txn).unwrap();
        txn.abort();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].get("id"), Some("3"));
        assert_eq!(out[0].get("department"), Some("Engineering"));

        engine.close().unwrap();
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn in_list_and_scalar_subquery_filter() {
        let (engine, root) = temp_engine("subq-filter");
        engine
            .register_table(TableSchema::new("employees", "id", vec![]))
            .unwrap();
        let ctx = ExecutionContext::new();
        execute_plan_autocommit(
            &LogicalPlanner::plan(
                "INSERT INTO employees (id, department, salary) VALUES \
                 (1, 'Sales', 5000), (2, 'Engineering', 9000), (3, 'Sales', 7000)",
            )
            .unwrap(),
            &ctx,
            engine.begin().unwrap(),
        )
        .unwrap();

        // Uncorrelated IN → SemiJoin (or InList rewrite).
        let plan = LogicalPlanner::plan(
            "SELECT * FROM employees WHERE department IN \
             (SELECT department FROM employees WHERE id = 2)",
        )
        .unwrap();
        let physical = optimize_with_catalog(
            &plan,
            &|t| engine.table_schema(t).ok(),
            &|_| None,
        )
        .unwrap();
        assert!(
            matches!(
                physical,
                PhysicalPlan::HashJoin {
                    join_type: JoinType::Semi,
                    ..
                }
            ),
            "expected HashSemiJoin, got {physical:?}"
        );
        let explain = explain_physical(&physical);
        assert!(explain.contains("HashSemiJoin"), "{explain}");

        let mut txn = engine.begin().unwrap();
        let out = execute_plan(&plan, &ctx, &mut txn).unwrap();
        txn.abort();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].get("department"), Some("Engineering"));

        // Scalar subquery equality.
        let scalar = LogicalPlanner::plan(
            "SELECT * FROM employees WHERE salary = (SELECT salary FROM employees WHERE id = 2)",
        )
        .unwrap();
        let mut txn = engine.begin().unwrap();
        let out = execute_plan(&scalar, &ctx, &mut txn).unwrap();
        txn.abort();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].get("id"), Some("2"));

        engine.close().unwrap();
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn analyze_exec_computes_ndv_and_minmax() {
        let (engine, root) = temp_engine("analyze-exec");
        engine
            .register_table(TableSchema::new(
                "employees",
                "id",
                vec![IndexDef::new("idx_dept", "department")],
            ))
            .unwrap();
        let ctx = ExecutionContext::new();
        execute_plan_autocommit(
            &LogicalPlanner::plan(
                "INSERT INTO employees (id, department, salary) VALUES \
                 (1, 'Sales', 100), (2, 'Sales', 200), (3, 'Engineering', 300), \
                 (4, 'Sales', 150)",
            )
            .unwrap(),
            &ctx,
            engine.begin().unwrap(),
        )
        .unwrap();

        let plan = LogicalPlanner::plan("ANALYZE employees").unwrap();
        assert!(matches!(plan, LogicalPlan::Analyze { .. }));
        let physical = optimize_with_catalog(
            &plan,
            &|t| engine.table_schema(t).ok(),
            &|t| Some(engine.table_stats(t)),
        )
        .unwrap();
        assert!(matches!(physical, PhysicalPlan::Analyze { .. }));

        let rows = execute_plan_autocommit(&plan, &ctx, engine.begin().unwrap()).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("rows"), Some("4"));

        let st = engine.table_stats("employees");
        assert_eq!(st.row_count, 4);
        let dept = st.columns.get("department").unwrap();
        assert_eq!(dept.ndv, 2);
        assert_eq!(dept.min.as_deref(), Some("Engineering"));
        assert_eq!(dept.max.as_deref(), Some("Sales"));

        engine.close().unwrap();
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn analyze_selectivity_switches_index_to_seq_scan() {
        let (engine, root) = temp_engine("analyze-cbo");
        engine
            .register_table(TableSchema::new("employees", "id", vec![]))
            .unwrap();
        let ctx = ExecutionContext::new();

        // 1000 rows: 960 Sales (frequent), 40 Engineering (rare).
        let mut values = String::from(
            "INSERT INTO employees (id, department, salary) VALUES ",
        );
        for i in 1..=1000 {
            if i > 1 {
                values.push_str(", ");
            }
            let dept = if i <= 960 { "Sales" } else { "Engineering" };
            values.push_str(&format!("({i}, '{dept}', {})", i * 10));
        }
        execute_plan_autocommit(
            &LogicalPlanner::plan(&values).unwrap(),
            &ctx,
            engine.begin().unwrap(),
        )
        .unwrap();
        engine
            .create_index("idx_dept", "employees", "department", false)
            .unwrap();

        // Before ANALYZE: incremental NDV heuristic still prefers IndexScan.
        let before = optimize_with_catalog(
            &LogicalPlanner::plan(
                "SELECT * FROM employees WHERE department = 'Sales'",
            )
            .unwrap(),
            &|t| engine.table_schema(t).ok(),
            &|t| Some(engine.table_stats(t)),
        )
        .unwrap();
        assert!(
            matches!(before, PhysicalPlan::IndexScan { .. }),
            "pre-ANALYZE expected IndexScan, got {before:?}"
        );

        execute_plan_autocommit(
            &LogicalPlanner::plan("ANALYZE employees").unwrap(),
            &ctx,
            engine.begin().unwrap(),
        )
        .unwrap();

        let frequent = optimize_with_catalog(
            &LogicalPlanner::plan(
                "SELECT * FROM employees WHERE department = 'Sales'",
            )
            .unwrap(),
            &|t| engine.table_schema(t).ok(),
            &|t| Some(engine.table_stats(t)),
        )
        .unwrap();
        let freq_text = explain_physical(&frequent);
        assert!(
            freq_text.contains("TableScan(employees)"),
            "frequent value must use seq scan after ANALYZE, got:\n{freq_text}"
        );
        assert!(
            !freq_text.contains("IndexScan(idx_dept)"),
            "frequent value must not use IndexScan, got:\n{freq_text}"
        );

        let rare = optimize_with_catalog(
            &LogicalPlanner::plan(
                "SELECT * FROM employees WHERE department = 'Engineering'",
            )
            .unwrap(),
            &|t| engine.table_schema(t).ok(),
            &|t| Some(engine.table_stats(t)),
        )
        .unwrap();
        let rare_text = explain_physical(&rare);
        assert!(
            rare_text.contains("IndexScan(idx_dept)"),
            "rare value must keep IndexScan after ANALYZE, got:\n{rare_text}"
        );

        engine.close().unwrap();
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn jit_sum_salary_times_tax_e2e() {
        let (engine, root) = temp_engine("jit-olap");
        engine
            .register_table(TableSchema::new("employees", "id", Vec::new()))
            .unwrap();
        let mut session_txn = engine.begin().unwrap();
        // ages: 25,35,40,28,50 — filter age > 30 keeps 35,40,50
        // salary * tax_rate: 100*2 + 200*3 + 150*2 = 200+600+300 = 1100
        let rows = [
            ("1", "25", "90", "1"),
            ("2", "35", "100", "2"),
            ("3", "40", "200", "3"),
            ("4", "28", "80", "1"),
            ("5", "50", "150", "2"),
        ];
        for (id, age, sal, tax) in rows {
            session_txn
                .put_record(
                    "employees",
                    Record::new()
                        .set("id", id)
                        .set("age", age)
                        .set("salary", sal)
                        .set("tax_rate", tax),
                )
                .unwrap();
        }
        session_txn.commit().unwrap();

        let sql =
            "SELECT SUM(salary * tax_rate) FROM employees WHERE age > 30";
        let plan = LogicalPlanner::plan(sql).unwrap();
        let physical = optimize_with_catalog(
            &plan,
            &|t| engine.table_schema(t).ok(),
            &|_| None,
        )
        .unwrap();
        let explain = explain_physical(&physical);
        assert!(
            explain.contains("JitExec"),
            "CBO must attach JitExec for compilable OLAP segment, got:\n{explain}"
        );

        let mut txn = engine.begin().unwrap();
        let mut exec =
            open_executor_with_txn(physical, &ExecutionContext::new(), &mut txn).unwrap();
        let rows = collect_rows(exec.as_mut()).unwrap();
        assert_eq!(rows.len(), 1);
        let sum_col = rows[0]
            .fields
            .keys()
            .find(|k| k.starts_with("sum("))
            .expect("sum column");
        let sum = rows[0].get(sum_col).unwrap();
        assert_eq!(sum, "1100");

        // Interpreter path must agree.
        let baseline = optimize_without_jit(
            &plan,
            &|t| engine.table_schema(t).ok(),
            &|_| None,
        )
        .unwrap();
        assert!(
            !explain_physical(&baseline).contains("JitExec"),
            "optimize_without_jit must stay Volcano"
        );
        let mut txn2 = engine.begin().unwrap();
        let mut exec2 =
            open_executor_with_txn(baseline, &ExecutionContext::new(), &mut txn2).unwrap();
        let rows2 = collect_rows(exec2.as_mut()).unwrap();
        assert_eq!(rows2[0].get(sum_col).unwrap(), "1100");

        engine.close().unwrap();
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn jit_benchmark_beats_interpreter_on_large_filter_agg() {
        let (engine, root) = temp_engine("jit-bench");
        engine
            .register_table(TableSchema::new("employees", "id", Vec::new()))
            .unwrap();
        const N: i64 = 8_000;
        let mut txn = engine.begin().unwrap();
        for i in 0..N {
            txn.put_record(
                "employees",
                Record::new()
                    .set("id", i.to_string())
                    .set("age", (20 + (i % 50)).to_string())
                    .set("salary", (50_000 + i * 3).to_string())
                    .set("tax_rate", ((i % 5) + 1).to_string()),
            )
            .unwrap();
        }
        txn.commit().unwrap();

        let sql = "SELECT SUM(salary * tax_rate) FROM employees WHERE age > 30";
        let plan = LogicalPlanner::plan(sql).unwrap();

        let jit_plan = optimize_with_catalog(
            &plan,
            &|t| engine.table_schema(t).ok(),
            &|_| None,
        )
        .unwrap();
        assert!(explain_physical(&jit_plan).contains("JitExec"));

        let interp_plan = optimize_without_jit(
            &plan,
            &|t| engine.table_schema(t).ok(),
            &|_| None,
        )
        .unwrap();

        // Warm-up
        for _ in 0..2 {
            let mut t = engine.begin().unwrap();
            let mut e =
                open_executor_with_txn(jit_plan.clone(), &ExecutionContext::new(), &mut t).unwrap();
            let _ = collect_rows(e.as_mut()).unwrap();
            let mut t = engine.begin().unwrap();
            let mut e =
                open_executor_with_txn(interp_plan.clone(), &ExecutionContext::new(), &mut t)
                    .unwrap();
            let _ = collect_rows(e.as_mut()).unwrap();
        }

        let t0 = std::time::Instant::now();
        let mut jit_sum = String::new();
        for _ in 0..8 {
            let mut t = engine.begin().unwrap();
            let mut e =
                open_executor_with_txn(jit_plan.clone(), &ExecutionContext::new(), &mut t).unwrap();
            let rows = collect_rows(e.as_mut()).unwrap();
            let col = rows[0].fields.keys().find(|k| k.starts_with("sum(")).unwrap();
            jit_sum = rows[0].get(col).unwrap().to_string();
        }
        let jit_ns = t0.elapsed().as_nanos() as f64;

        let t1 = std::time::Instant::now();
        let mut interp_sum = String::new();
        for _ in 0..8 {
            let mut t = engine.begin().unwrap();
            let mut e =
                open_executor_with_txn(interp_plan.clone(), &ExecutionContext::new(), &mut t)
                    .unwrap();
            let rows = collect_rows(e.as_mut()).unwrap();
            let col = rows[0].fields.keys().find(|k| k.starts_with("sum(")).unwrap();
            interp_sum = rows[0].get(col).unwrap().to_string();
        }
        let interp_ns = t1.elapsed().as_nanos() as f64;

        assert_eq!(jit_sum, interp_sum, "JIT and interpreter must agree");
        let speedup = interp_ns / jit_ns;
        eprintln!(
            "JIT bench: jit={jit_ns:.0}ns interp={interp_ns:.0}ns speedup={speedup:.2}x sum={jit_sum}"
        );
        assert!(
            speedup > 0.5,
            "JIT unexpectedly much slower than interpreter ({speedup:.2}x)"
        );

        engine.close().unwrap();
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn jit_vectorization_rule_prefers_simd_for_large_olap() {
        use crate::stats::TableStats;

        let (engine, root) = temp_engine("vec-cbo");
        engine
            .register_table(TableSchema::new("lineitem", "id", vec![]))
            .unwrap();
        let sql = "SELECT SUM(price * discount) FROM lineitem WHERE qty < 24";
        let plan = LogicalPlanner::plan(sql).unwrap();

        // Small cardinality → scalar JitExec (or Aggregate).
        let small = optimize_with_catalog(
            &plan,
            &|t| engine.table_schema(t).ok(),
            &|_| {
                Some(TableStats {
                    row_count: 10,
                    page_count: 1,
                    ..TableStats::default()
                })
            },
        )
        .unwrap();
        let small_text = explain_physical(&small);
        assert!(
            !small_text.contains("VectorizedExec"),
            "small table must not use VectorizedExec, got:\n{small_text}"
        );

        // Large OLAP → VectorizedExec via JITVectorizationRule.
        let large = optimize_with_catalog(
            &plan,
            &|t| engine.table_schema(t).ok(),
            &|_| {
                Some(TableStats {
                    row_count: 100_000,
                    page_count: 1000,
                    ..TableStats::default()
                })
            },
        )
        .unwrap();
        let text = explain_physical(&large);
        assert!(
            text.contains("VectorizedExec"),
            "CBO must pick VectorizedExec for large OLAP, got:\n{text}"
        );

        engine.close().unwrap();
        let _ = fs::remove_dir_all(root);
    }
}
