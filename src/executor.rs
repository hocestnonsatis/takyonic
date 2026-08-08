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
#[derive(Clone, Debug)]
pub struct ExecutionContext {
    /// Resolved `$1`…`$n` values (0-based: `params[0]` ≡ `$1`).
    pub params: Vec<Value>,
    /// Frozen wall clock at statement start (`STATEMENT_TIMESTAMP` / `NOW`).
    pub statement_timestamp: String,
    /// Session `current_user` / `session_user` / `user`.
    pub current_user: String,
    /// First schema on `search_path` (`current_schema()`).
    pub current_schema: String,
    /// Full `search_path` GUC (`current_schemas(…)`).
    pub search_path: String,
    /// Database / catalog name (`current_catalog`).
    pub current_catalog: String,
    /// Statement-scoped synthetic xid (`txid_current` / `pg_current_xact_id`).
    pub txid: u64,
    /// `transaction_isolation` GUC.
    pub transaction_isolation: String,
    /// `TimeZone` GUC (IANA name or fixed offset; default `UTC`).
    pub timezone: String,
    /// True when the session has an open explicit transaction (`SET LOCAL` / `set_config(..., true)`).
    pub in_transaction: bool,
    /// Session auth context for privilege helpers (`has_table_privilege`).
    pub auth: Option<crate::rbac::AuthContext>,
    /// Shared AUTH catalog for privilege helpers.
    pub auth_catalog: Option<crate::rbac::SharedAuthCatalog>,
    /// Server TCP address for `inet_server_addr` (`None` → NULL, like Unix socket).
    pub inet_server_addr: Option<String>,
    /// Server TCP port for `inet_server_port`.
    pub inet_server_port: Option<i64>,
    /// Client TCP address for `inet_client_addr`.
    pub inet_client_addr: Option<String>,
    /// Client TCP port for `inet_client_port`.
    pub inet_client_port: Option<i64>,
    /// Shared `COMMENT ON` map for `obj_description` / `col_description`.
    pub comments: Option<std::sync::Arc<parking_lot::RwLock<std::collections::BTreeMap<String, String>>>>,
    /// Synthetic relation OIDs for `to_regclass` / OID-form descriptions.
    pub relation_catalog: Option<std::sync::Arc<crate::oid::RelationCatalog>>,
    /// Approximate relation byte sizes for `pg_relation_size` / `pg_table_size`.
    pub relation_sizes: Option<std::sync::Arc<crate::oid::RelationSizeCatalog>>,
    /// Secondary indexes for `pg_get_indexdef`.
    pub index_catalog: Option<std::sync::Arc<crate::oid::IndexCatalog>>,
    /// Session id for advisory locks (`pg_try_advisory_lock`, …).
    pub session_id: u64,
    /// Text of the currently executing SQL (`current_query()`), if known.
    pub current_query: Option<String>,
    /// Session `LISTEN` channels for `pg_listening_channels()`.
    pub listening_channels: Vec<String>,
}

impl Default for ExecutionContext {
    fn default() -> Self {
        Self {
            params: Vec::new(),
            statement_timestamp: crate::sql::utc_now_timestamp(),
            current_user: "postgres".into(),
            current_schema: "public".into(),
            search_path: "public".into(),
            current_catalog: "postgres".into(),
            txid: crate::sql::next_txid(),
            transaction_isolation: "repeatable read".into(),
            timezone: "UTC".into(),
            in_transaction: false,
            auth: None,
            auth_catalog: None,
            inet_server_addr: None,
            inet_server_port: None,
            inet_client_addr: None,
            inet_client_port: None,
            comments: None,
            relation_catalog: None,
            relation_sizes: None,
            index_catalog: None,
            session_id: 0,
            current_query: None,
            listening_channels: Vec::new(),
        }
    }
}

impl ExecutionContext {
    /// Empty context (no parameters); captures statement timestamp.
    pub fn new() -> Self {
        Self::default()
    }

    /// Build from an already-decoded parameter list.
    pub fn with_params(params: Vec<Value>) -> Self {
        Self {
            params,
            statement_timestamp: crate::sql::utc_now_timestamp(),
            current_user: "postgres".into(),
            current_schema: "public".into(),
            search_path: "public".into(),
            current_catalog: "postgres".into(),
            txid: crate::sql::next_txid(),
            transaction_isolation: "repeatable read".into(),
            timezone: "UTC".into(),
            in_transaction: false,
            auth: None,
            auth_catalog: None,
            inet_server_addr: None,
            inet_server_port: None,
            inet_client_addr: None,
            inet_client_port: None,
            comments: None,
            relation_catalog: None,
            relation_sizes: None,
            index_catalog: None,
            session_id: 0,
            current_query: None,
            listening_channels: Vec::new(),
        }
    }

    /// Session-aware context (user + search_path + catalog + isolation + RBAC).
    pub fn for_session(
        params: Vec<Value>,
        auth: crate::rbac::AuthContext,
        auth_catalog: crate::rbac::SharedAuthCatalog,
        search_path: &str,
        current_catalog: impl Into<String>,
        transaction_isolation: impl Into<String>,
        timezone: impl Into<String>,
        in_transaction: bool,
        inet_server_addr: Option<String>,
        inet_server_port: Option<i64>,
        inet_client_addr: Option<String>,
        inet_client_port: Option<i64>,
        comments: Option<
            std::sync::Arc<parking_lot::RwLock<std::collections::BTreeMap<String, String>>>,
        >,
    ) -> Self {
        Self {
            params,
            statement_timestamp: crate::sql::utc_now_timestamp(),
            current_user: auth.user.clone(),
            current_schema: first_search_path_schema(search_path),
            search_path: search_path.to_string(),
            current_catalog: current_catalog.into(),
            txid: crate::sql::next_txid(),
            transaction_isolation: transaction_isolation.into(),
            timezone: timezone.into(),
            in_transaction,
            auth: Some(auth),
            auth_catalog: Some(auth_catalog),
            inet_server_addr,
            inet_server_port,
            inet_client_addr,
            inet_client_port,
            comments,
            relation_catalog: None,
            relation_sizes: None,
            index_catalog: None,
            session_id: 0,
            current_query: None,
            listening_channels: Vec::new(),
        }
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

/// First schema name from a comma-separated `search_path` GUC.
pub fn first_search_path_schema(search_path: &str) -> String {
    search_path
        .split(',')
        .map(|s| s.trim().trim_matches('"'))
        .find(|s| !s.is_empty())
        .unwrap_or("public")
        .to_string()
}

/// Schema names from `search_path` as a Takyonic array literal (`[a,b]`).
pub fn current_schemas_array(search_path: &str, include_implicit: bool) -> String {
    let mut parts: Vec<String> = search_path
        .split(',')
        .map(|s| s.trim().trim_matches('"').to_string())
        .filter(|s| !s.is_empty())
        .collect();
    if include_implicit
        && !parts
            .iter()
            .any(|s| s.eq_ignore_ascii_case("pg_catalog"))
    {
        parts.insert(0, "pg_catalog".into());
    }
    if parts.is_empty() {
        parts.push("public".into());
    }
    format!("[{}]", parts.join(","))
}

/// Which correlated regexp table function to expand.
#[derive(Clone, Debug)]
pub enum LateralRegexpSrfKind {
    /// `regexp_split_to_table`.
    SplitToTable,
    /// `regexp_matches`.
    Matches,
}

/// Physical plan tree produced by the (currently trivial) optimizer.
#[derive(Clone, Debug)]
pub enum LateralJsonSrfKind {
    /// `jsonb_array_elements` / `_text`.
    ArrayElements {
        /// Output column name.
        column: String,
        /// Emit text scalars when true.
        as_text: bool,
        /// Optional `WITH ORDINALITY` column.
        ordinality_column: Option<String>,
    },
    /// `json_each` / `jsonb_each` / `*_text`.
    Each {
        /// Key column name.
        key_column: String,
        /// Value column name.
        value_column: String,
        /// Emit values as text when true.
        as_text: bool,
        /// Optional `WITH ORDINALITY` column.
        ordinality_column: Option<String>,
    },
    /// `jsonb_object_keys` / `json_object_keys`.
    ObjectKeys {
        /// Output column name.
        column: String,
        /// Optional `WITH ORDINALITY` column.
        ordinality_column: Option<String>,
    },
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
    /// Window functions (`ROW_NUMBER() OVER …`) — blocking; adds output columns.
    Window {
        /// Child plan.
        input: Box<PhysicalPlan>,
        /// Window calls to evaluate.
        calls: Vec<crate::sql::WindowCall>,
    },
    /// Correlated Apply: for each outer row, bind [`Expression::OuterRef`] and
    /// re-evaluate IN/EXISTS/scalar subqueries (nested-loop dependent join).
    ///
    /// Chosen by the CBO when a [`LogicalPlan::Filter`] predicate still contains
    /// correlated subqueries after SemiJoin unnesting.
    Apply {
        /// Outer input (driven row-by-row).
        input: Box<PhysicalPlan>,
        /// Predicate with correlated IN/EXISTS/scalar subqueries.
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
    /// Correlated `LATERAL` JSON SRF — expand `doc` per left row.
    LateralJsonSrf {
        /// Outer (left) input.
        left: Box<PhysicalPlan>,
        /// JSON document expression evaluated on each outer row.
        doc: Expression,
        /// Which JSON SRF to expand.
        kind: LateralJsonSrfKind,
    },
    /// Correlated `LATERAL unnest(array)` — expand array per left row.
    LateralUnnest {
        /// Outer (left) input.
        left: Box<PhysicalPlan>,
        /// Array expression evaluated on each outer row.
        array: Expression,
        /// Output column name.
        column: String,
        /// Optional `WITH ORDINALITY` / `WITH OFFSET` column.
        ordinality_column: Option<String>,
        /// When true, ordinality/offset is 0-based.
        zero_based_ordinality: bool,
    },
    /// Correlated `LATERAL regexp_split_to_table` / `regexp_matches`.
    LateralRegexpSrf {
        /// Outer (left) input.
        left: Box<PhysicalPlan>,
        /// Subject string expression.
        string: Expression,
        /// Regex pattern expression.
        pattern: Expression,
        /// Optional flags expression.
        flags: Option<Expression>,
        /// Output column name.
        column: String,
        /// Optional `WITH ORDINALITY` column.
        ordinality_column: Option<String>,
        /// Which regexp SRF to expand.
        kind: LateralRegexpSrfKind,
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
    /// Sort-merge equi-join: both children sorted ascending on join keys.
    MergeJoin {
        /// Left (sorted) child.
        left: Box<PhysicalPlan>,
        /// Right (sorted) child.
        right: Box<PhysicalPlan>,
        /// Key expression evaluated on left rows.
        left_key: Expression,
        /// Key expression evaluated on right rows.
        right_key: Expression,
        /// Logical join kind (Inner only).
        join_type: JoinType,
    },
    /// `INSERT INTO … VALUES …` or `INSERT … SELECT …`.
    Insert {
        /// Target table.
        table: String,
        /// Column list.
        columns: Vec<String>,
        /// Expression rows (`VALUES`); empty when `input` is set.
        values: Vec<Vec<Expression>>,
        /// Child plan for `INSERT … SELECT` (`None` for VALUES).
        input: Option<Box<PhysicalPlan>>,
        /// Output column names of `input` (positional map onto `columns`).
        source_columns: Vec<String>,
        /// Optional `ON CONFLICT` action.
        on_conflict: Option<crate::sql::OnConflict>,
        /// Optional `RETURNING` projection.
        returning: Option<crate::sql::Returning>,
    },
    /// `UPDATE … SET …` over target rows from `input`.
    Update {
        /// Target table.
        table: String,
        /// Column assignments.
        assignments: HashMap<String, Expression>,
        /// Child yielding rows to update (typically Filter(TableScan)).
        input: Box<PhysicalPlan>,
        /// Optional `RETURNING` projection.
        returning: Option<crate::sql::Returning>,
    },
    /// `DELETE FROM …` over target rows from `input`.
    Delete {
        /// Target table.
        table: String,
        /// Child yielding rows to delete.
        input: Box<PhysicalPlan>,
        /// Optional `RETURNING` projection.
        returning: Option<crate::sql::Returning>,
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
        /// `LIMIT` / `FETCH`; `None` = no upper bound.
        fetch: Option<usize>,
        /// `FETCH … WITH TIES`.
        with_ties: bool,
        /// ORDER BY keys for WITH TIES peer expansion.
        ties_order: Vec<crate::sql::SortExpr>,
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
    /// Column projection (`SELECT a, b AS x`).
    Project {
        /// Child plan.
        input: Box<PhysicalPlan>,
        /// `(output_name, source_expr)` pairs.
        columns: Vec<(String, Expression)>,
    },
    /// `UNION` / `UNION ALL`.
    Union {
        /// Left operand.
        left: Box<PhysicalPlan>,
        /// Right operand.
        right: Box<PhysicalPlan>,
        /// `UNION` / `INTERSECT` / `EXCEPT`.
        op: crate::sql::SetOpKind,
        /// `true` keeps duplicates (`… ALL`).
        all: bool,
    },
    /// `SELECT DISTINCT` — hash-deduplicate child rows.
    Distinct {
        /// Child plan.
        input: Box<PhysicalPlan>,
    },
    /// `SELECT DISTINCT ON (exprs)` — keep first row per ON-key group.
    DistinctOn {
        /// Child plan.
        input: Box<PhysicalPlan>,
        /// DISTINCT ON key expressions.
        exprs: Vec<Expression>,
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
    /// [`crate::partition::PartitionPruningRule`]. When `mpp_enabled`, Session
    /// executes via [`crate::mpp::Coordinator::execute_distributed_scan`]
    /// (RemoteWorker fetch); otherwise volcano falls through to `input`.
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
        LogicalPlan::Insert { .. }
            | LogicalPlan::Update { .. }
            | LogicalPlan::Delete { .. }
            | LogicalPlan::Truncate { .. }
            | LogicalPlan::Copy { .. }
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
            query,
            on_conflict,
            returning,
        } => {
            if let Some(q) = query {
                let mut source_columns = crate::sql::ctas_output_columns(q).unwrap_or_default();
                if source_columns.is_empty() {
                    if let LogicalPlan::Select { table: src, .. } = q.as_ref() {
                        if let Some(schema) = schema_of(src) {
                            source_columns = if schema.columns.is_empty() {
                                vec![schema.primary_key.clone()]
                            } else {
                                schema.columns.iter().map(|c| c.name.clone()).collect()
                            };
                        }
                    }
                }
                let input = optimize_plan_tree(q, schema_of, stats_of)?;
                Ok(PhysicalPlan::Insert {
                    table: table.clone(),
                    columns: columns.clone(),
                    values: Vec::new(),
                    input: Some(Box::new(input)),
                    source_columns,
                    on_conflict: on_conflict.clone(),
                    returning: returning.clone(),
                })
            } else {
                Ok(PhysicalPlan::Insert {
                    table: table.clone(),
                    columns: columns.clone(),
                    values: values.clone(),
                    input: None,
                    source_columns: Vec::new(),
                    on_conflict: on_conflict.clone(),
                    returning: returning.clone(),
                })
            }
        }
        LogicalPlan::Update {
            table,
            assignments,
            selection,
            returning,
        } => {
            let input =
                optimize_table_access(table, &[], selection.as_ref(), schema_of, stats_of)?;
            Ok(PhysicalPlan::Update {
                table: table.clone(),
                assignments: assignments.clone(),
                input: Box::new(input),
                returning: returning.clone(),
            })
        }
        LogicalPlan::Delete {
            table,
            selection,
            returning,
        } => {
            let input =
                optimize_table_access(table, &[], selection.as_ref(), schema_of, stats_of)?;
            Ok(PhysicalPlan::Delete {
                table: table.clone(),
                input: Box::new(input),
                returning: returning.clone(),
            })
        }
        LogicalPlan::Truncate { table, if_exists } => {
            if schema_of(table).is_none() {
                if *if_exists {
                    return Ok(PhysicalPlan::Values { rows: Vec::new() });
                }
                return Err(TakyonicError::Sql(format!(
                    "table `{table}` does not exist"
                )));
            }
            let input = optimize_table_access(table, &[], None, schema_of, stats_of)?;
            Ok(PhysicalPlan::Delete {
                table: table.clone(),
                input: Box::new(input),
                returning: None,
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
            // Correlated LATERAL JSON SRF → per-outer-row expand.
            match right.as_ref() {
                LogicalPlan::JsonArrayElements {
                    doc,
                    column,
                    as_text,
                    ordinality_column,
                } if crate::sql::expr_needs_row_eval(doc) => {
                    let left_phys = Box::new(optimize_plan_tree(left, schema_of, stats_of)?);
                    return Ok(PhysicalPlan::LateralJsonSrf {
                        left: left_phys,
                        doc: doc.clone(),
                        kind: LateralJsonSrfKind::ArrayElements {
                            column: column.clone(),
                            as_text: *as_text,
                            ordinality_column: ordinality_column.clone(),
                        },
                    });
                }
                LogicalPlan::JsonEach {
                    doc,
                    key_column,
                    value_column,
                    as_text,
                    ordinality_column,
                } if crate::sql::expr_needs_row_eval(doc) => {
                    let left_phys = Box::new(optimize_plan_tree(left, schema_of, stats_of)?);
                    return Ok(PhysicalPlan::LateralJsonSrf {
                        left: left_phys,
                        doc: doc.clone(),
                        kind: LateralJsonSrfKind::Each {
                            key_column: key_column.clone(),
                            value_column: value_column.clone(),
                            as_text: *as_text,
                            ordinality_column: ordinality_column.clone(),
                        },
                    });
                }
                LogicalPlan::JsonObjectKeys {
                    doc,
                    column,
                    ordinality_column,
                } if crate::sql::expr_needs_row_eval(doc) => {
                    let left_phys = Box::new(optimize_plan_tree(left, schema_of, stats_of)?);
                    return Ok(PhysicalPlan::LateralJsonSrf {
                        left: left_phys,
                        doc: doc.clone(),
                        kind: LateralJsonSrfKind::ObjectKeys {
                            column: column.clone(),
                            ordinality_column: ordinality_column.clone(),
                        },
                    });
                }
                LogicalPlan::Unnest {
                    array,
                    column,
                    ordinality_column,
                    zero_based_ordinality,
                } if crate::sql::expr_needs_row_eval(array) => {
                    let left_phys = Box::new(optimize_plan_tree(left, schema_of, stats_of)?);
                    return Ok(PhysicalPlan::LateralUnnest {
                        left: left_phys,
                        array: array.clone(),
                        column: column.clone(),
                        ordinality_column: ordinality_column.clone(),
                        zero_based_ordinality: *zero_based_ordinality,
                    });
                }
                LogicalPlan::RegexpSplitToTable {
                    string,
                    pattern,
                    flags,
                    column,
                    ordinality_column,
                } if regexp_srf_needs_row(string, pattern, flags) => {
                    let left_phys = Box::new(optimize_plan_tree(left, schema_of, stats_of)?);
                    return Ok(PhysicalPlan::LateralRegexpSrf {
                        left: left_phys,
                        string: string.clone(),
                        pattern: pattern.clone(),
                        flags: flags.clone(),
                        column: column.clone(),
                        ordinality_column: ordinality_column.clone(),
                        kind: LateralRegexpSrfKind::SplitToTable,
                    });
                }
                LogicalPlan::RegexpMatches {
                    string,
                    pattern,
                    flags,
                    column,
                    ordinality_column,
                } if regexp_srf_needs_row(string, pattern, flags) => {
                    let left_phys = Box::new(optimize_plan_tree(left, schema_of, stats_of)?);
                    return Ok(PhysicalPlan::LateralRegexpSrf {
                        left: left_phys,
                        string: string.clone(),
                        pattern: pattern.clone(),
                        flags: flags.clone(),
                        column: column.clone(),
                        ordinality_column: ordinality_column.clone(),
                        kind: LateralRegexpSrfKind::Matches,
                    });
                }
                _ => {}
            }
            // DistributedJoin: local fallback → same HashJoin / NestedLoop as Join.
            let left_phys = Box::new(optimize_plan_tree(left, schema_of, stats_of)?);
            let right_phys = Box::new(optimize_plan_tree(right, schema_of, stats_of)?);
            if let Some((left_key, right_key)) =
                match_equi_join_keys(on, left, right, schema_of)
            {
                // Prefer MergeJoin when both sides are already sorted on the join key.
                if *join_type == JoinType::Inner
                    && is_sorted_on(&left_phys, &left_key)
                    && is_sorted_on(&right_phys, &right_key)
                {
                    return Ok(PhysicalPlan::MergeJoin {
                        left: left_phys,
                        right: right_phys,
                        left_key,
                        right_key,
                        join_type: *join_type,
                    });
                }
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
            with_ties,
            ties_order,
        } => {
            // VectorIndexSelectionRule: ORDER BY col <-> query LIMIT k → HNSW scan.
            if !*with_ties {
                if let Some(vis) =
                    try_vector_index_selection(input, *skip, *fetch, schema_of)
                {
                    return Ok(vis);
                }
            }
            if let (false, Some(fetch), LogicalPlan::Sort { input: sort_in, exprs }) =
                (*with_ties, *fetch, input.as_ref())
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
                with_ties: *with_ties,
                ties_order: ties_order.clone(),
            })
        }
        LogicalPlan::Project { input, columns } => Ok(PhysicalPlan::Project {
            input: Box::new(optimize_plan_tree(input, schema_of, stats_of)?),
            columns: columns.clone(),
        }),
        LogicalPlan::Window { input, calls } => Ok(PhysicalPlan::Window {
            input: Box::new(optimize_plan_tree(input, schema_of, stats_of)?),
            calls: calls.clone(),
        }),
        LogicalPlan::Union {
            left,
            right,
            op,
            all,
        } => Ok(PhysicalPlan::Union {
            left: Box::new(optimize_plan_tree(left, schema_of, stats_of)?),
            right: Box::new(optimize_plan_tree(right, schema_of, stats_of)?),
            op: *op,
            all: *all,
        }),
        LogicalPlan::Distinct { input } => Ok(PhysicalPlan::Distinct {
            input: Box::new(optimize_plan_tree(input, schema_of, stats_of)?),
        }),
        LogicalPlan::DistinctOn { input, exprs } => Ok(PhysicalPlan::DistinctOn {
            input: Box::new(optimize_plan_tree(input, schema_of, stats_of)?),
            exprs: exprs.clone(),
        }),
        LogicalPlan::Filter { input, predicate } => {
            if let Some(semi) =
                try_subquery_unnest(input, predicate, schema_of, stats_of)?
            {
                return Ok(semi);
            }
            let input_phys = Box::new(optimize_plan_tree(input, schema_of, stats_of)?);
            Ok(filter_or_apply(input_phys, predicate.clone()))
        }
        LogicalPlan::SubqueryAlias { input, .. } => {
            optimize_plan_tree(input, schema_of, stats_of)
        }
        LogicalPlan::Values { columns, rows } => {
            let ctx = ExecutionContext::new();
            let empty = Record::new();
            let mut out = Vec::with_capacity(rows.len());
            for row in rows {
                if row.len() != columns.len() {
                    return Err(TakyonicError::Sql(
                        "VALUES row width does not match column list".into(),
                    ));
                }
                let mut rec = Record::new();
                for (col, expr) in columns.iter().zip(row.iter()) {
                    let v = evaluate(expr, &empty, &ctx)?;
                    rec = rec.set(col, value_to_field(&v));
                }
                out.push(rec);
            }
            Ok(PhysicalPlan::Values { rows: out })
        }
        LogicalPlan::GenerateSeries {
            start,
            stop,
            step,
            column,
            ordinality_column,
            as_timestamp,
            date_only,
        } => Ok(PhysicalPlan::Values {
            rows: crate::sql::materialize_generate_series(
                *start,
                *stop,
                *step,
                column,
                ordinality_column.as_deref(),
                *as_timestamp,
                *date_only,
            )?,
        }),
        LogicalPlan::Unnest {
            array,
            column,
            ordinality_column,
            zero_based_ordinality,
        } => {
            if crate::sql::expr_needs_row_eval(array) {
                return Err(TakyonicError::Sql(
                    "correlated unnest requires CROSS JOIN LATERAL".into(),
                ));
            }
            Ok(PhysicalPlan::Values {
                rows: crate::sql::materialize_unnest(
                    array,
                    column,
                    ordinality_column.as_deref(),
                    *zero_based_ordinality,
                )?,
            })
        }
        LogicalPlan::JsonArrayElements {
            doc,
            column,
            as_text,
            ordinality_column,
        } => {
            if crate::sql::expr_needs_row_eval(doc) {
                return Err(TakyonicError::Sql(
                    "correlated jsonb_array_elements requires CROSS JOIN LATERAL".into(),
                ));
            }
            let doc_text = const_json_doc_text(doc)?;
            Ok(PhysicalPlan::Values {
                rows: crate::sql::materialize_json_array_elements(
                    &doc_text,
                    column,
                    *as_text,
                    ordinality_column.as_deref(),
                )?,
            })
        }
        LogicalPlan::JsonEach {
            doc,
            key_column,
            value_column,
            as_text,
            ordinality_column,
        } => {
            if crate::sql::expr_needs_row_eval(doc) {
                return Err(TakyonicError::Sql(
                    "correlated json_each requires CROSS JOIN LATERAL".into(),
                ));
            }
            let doc_text = const_json_doc_text(doc)?;
            Ok(PhysicalPlan::Values {
                rows: crate::sql::materialize_json_each(
                    &doc_text,
                    key_column,
                    value_column,
                    *as_text,
                    ordinality_column.as_deref(),
                )?,
            })
        }
        LogicalPlan::JsonObjectKeys {
            doc,
            column,
            ordinality_column,
        } => {
            if crate::sql::expr_needs_row_eval(doc) {
                return Err(TakyonicError::Sql(
                    "correlated jsonb_object_keys requires CROSS JOIN LATERAL".into(),
                ));
            }
            let doc_text = const_json_doc_text(doc)?;
            Ok(PhysicalPlan::Values {
                rows: crate::sql::materialize_json_object_keys(
                    &doc_text,
                    column,
                    ordinality_column.as_deref(),
                )?,
            })
        }
        LogicalPlan::RegexpSplitToTable {
            string,
            pattern,
            flags,
            column,
            ordinality_column,
        } => {
            if regexp_srf_needs_row(string, pattern, flags) {
                return Err(TakyonicError::Sql(
                    "correlated regexp_split_to_table requires CROSS JOIN LATERAL".into(),
                ));
            }
            let (s, p, f) = fold_regexp_srf_args(string, pattern, flags)?;
            Ok(PhysicalPlan::Values {
                rows: crate::sql::materialize_regexp_split_to_table(
                    &s,
                    &p,
                    f.as_deref(),
                    column,
                    ordinality_column.as_deref(),
                )?,
            })
        }
        LogicalPlan::RegexpMatches {
            string,
            pattern,
            flags,
            column,
            ordinality_column,
        } => {
            if regexp_srf_needs_row(string, pattern, flags) {
                return Err(TakyonicError::Sql(
                    "correlated regexp_matches requires CROSS JOIN LATERAL".into(),
                ));
            }
            let (s, p, f) = fold_regexp_srf_args(string, pattern, flags)?;
            Ok(PhysicalPlan::Values {
                rows: crate::sql::materialize_regexp_matches(
                    &s,
                    &p,
                    f.as_deref(),
                    column,
                    ordinality_column.as_deref(),
                )?,
            })
        }
        LogicalPlan::Explain { plan } => optimize_plan_tree(plan, schema_of, stats_of),
        LogicalPlan::Analyze { table } => Ok(PhysicalPlan::Analyze {
            table: table.clone(),
        }),
        LogicalPlan::Vacuum { table } => Ok(PhysicalPlan::Vacuum {
            table: table.clone(),
        }),
        LogicalPlan::Rebalance { .. }
        | LogicalPlan::CreateIndex { .. }
        | LogicalPlan::DropIndex { .. }
        | LogicalPlan::CreateTable { .. }
        | LogicalPlan::CreateTableAs { .. }
        | LogicalPlan::AlterTable { .. }
        | LogicalPlan::DropTable { .. }
        | LogicalPlan::CreateRole { .. }
        | LogicalPlan::DropRole { .. }
        | LogicalPlan::Grant { .. }
        | LogicalPlan::Revoke { .. }
        | LogicalPlan::GrantSchema { .. }
        | LogicalPlan::RevokeSchema { .. }
        | LogicalPlan::GrantColumn { .. }
        | LogicalPlan::RevokeColumn { .. }
        | LogicalPlan::GrantRole { .. }
        | LogicalPlan::Begin
        | LogicalPlan::Commit
        | LogicalPlan::Rollback
        | LogicalPlan::Set { .. }
        | LogicalPlan::Show { .. }
        | LogicalPlan::Comment { .. }
        | LogicalPlan::Listen { .. }
        | LogicalPlan::Unlisten { .. }
        | LogicalPlan::Notify { .. }
        | LogicalPlan::CreateSequence { .. }
        | LogicalPlan::DropSequence { .. }
        | LogicalPlan::AlterSequence { .. }
        | LogicalPlan::Copy { .. } => Err(TakyonicError::Sql(
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
        | PhysicalPlan::TopN { input, .. }
        | PhysicalPlan::Project { input, .. }
        | PhysicalPlan::Apply { input, .. } => vectorization_row_hint(input, stats_of),
        _ => None,
    }
}

fn aggr_args_vectorizable(expr: &Expression) -> bool {
    match expr {
        Expression::AggregateFunction {
            name,
            args,
            filter,
            distinct,
            order_by,
        } => {
            if filter.is_some() || *distinct || !order_by.is_empty() {
                return false;
            }
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
        Expression::AggregateFunction {
            name,
            args,
            filter,
            distinct,
            order_by,
        } => {
            // JSON aggregates often fold text/JSON values; keep them on AggregateExec
            // until JIT scalar eval covers non-numeric columns reliably.
            let n = name.to_ascii_uppercase();
            if filter.is_some() || *distinct || !order_by.is_empty() {
                return false;
            }
            if matches!(
                n.as_str(),
                "JSON_AGG"
                    | "JSONB_AGG"
                    | "JSON_OBJECT_AGG"
                    | "JSONB_OBJECT_AGG"
                    | "STRING_AGG"
                    | "ARRAY_AGG"
                    | "BOOL_AND"
                    | "BOOL_OR"
                    | "EVERY"
                    | "BIT_AND"
                    | "BIT_OR"
                    | "STDDEV"
                    | "STDDEV_POP"
                    | "STDDEV_SAMP"
                    | "VARIANCE"
                    | "VAR_POP"
                    | "VAR_SAMP"
                    | "CORR"
                    | "COVAR_POP"
                    | "COVAR_SAMP"
                    | "REGR_SLOPE"
                    | "REGR_INTERCEPT"
                    | "REGR_R2"
                    | "REGR_COUNT"
                    | "REGR_AVGX"
                    | "REGR_AVGY"
                    | "REGR_SXX"
                    | "REGR_SYY"
                    | "REGR_SXY"
                    | "MODE"
                    | "PERCENTILE_CONT"
                    | "PERCENTILE_DISC"
            ) {
                return false;
            }
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
        LogicalPlan::Update { table, .. } | LogicalPlan::Delete { table, .. }
        | LogicalPlan::Truncate { table, .. }
        | LogicalPlan::Copy { table, .. } => {
            vec![table.clone()]
        }
        LogicalPlan::Insert { table, .. } => vec![table.clone()],
        LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::DistributedAggregate { input, .. } => collect_tables(input),
        LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Filter { input, .. }
        | LogicalPlan::Project { input, .. }
        | LogicalPlan::Window { input, .. }
        | LogicalPlan::Distinct { input, .. }
        | LogicalPlan::DistinctOn { input, .. }
        | LogicalPlan::SubqueryAlias { input, .. }
        | LogicalPlan::Explain { plan: input } => collect_tables(input),
        LogicalPlan::Union { left, right, .. } => {
            let mut t = collect_tables(left);
            t.extend(collect_tables(right));
            t
        },
        LogicalPlan::CreateIndex { table, .. }
        | LogicalPlan::CreateTable { name: table, .. }
        | LogicalPlan::CreateTableAs { name: table, .. }
        | LogicalPlan::AlterTable { name: table, .. }
        | LogicalPlan::DropTable { name: table, .. }
        | LogicalPlan::Analyze { table }
        | LogicalPlan::Vacuum { table }
        | LogicalPlan::Rebalance { table } => {
            vec![table.clone()]
        }
        LogicalPlan::DropIndex { .. }
        | LogicalPlan::CreateRole { .. }
        | LogicalPlan::DropRole { .. }
        | LogicalPlan::Grant { .. }
        | LogicalPlan::Revoke { .. }
        | LogicalPlan::GrantSchema { .. }
        | LogicalPlan::RevokeSchema { .. }
        | LogicalPlan::GrantColumn { .. }
        | LogicalPlan::RevokeColumn { .. }
        | LogicalPlan::GrantRole { .. }
        | LogicalPlan::Begin
        | LogicalPlan::Commit
        | LogicalPlan::Rollback
        | LogicalPlan::Set { .. }
        | LogicalPlan::Show { .. }
        | LogicalPlan::Comment { .. }
        | LogicalPlan::Listen { .. }
        | LogicalPlan::Unlisten { .. }
        | LogicalPlan::Notify { .. }
        | LogicalPlan::CreateSequence { .. }
        | LogicalPlan::DropSequence { .. }
        | LogicalPlan::AlterSequence { .. }
        | LogicalPlan::Values { .. }
        | LogicalPlan::GenerateSeries { .. }
        | LogicalPlan::Unnest { .. }
        | LogicalPlan::JsonArrayElements { .. }
        | LogicalPlan::JsonEach { .. }
        | LogicalPlan::JsonObjectKeys { .. }
        | LogicalPlan::RegexpSplitToTable { .. }
        | LogicalPlan::RegexpMatches { .. } => Vec::new(),
    }
}

/// SubqueryUnnestingRule: uncorrelated `IN` → Hash Semi/Anti; correlated equi
/// `EXISTS` / matching equi-`IN` → Hash Semi/Anti (OuterRef becomes join key).
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
        Expression::Exists {
            subquery,
            negated,
            correlated: true,
        } => {
            let Some((outer_col, inner_col, inner_plan)) =
                extract_single_equi_outer_ref(subquery)
            else {
                return Ok(None);
            };
            let left = Box::new(optimize_with_catalog(left_plan, schema_of, stats_of)?);
            let right = Box::new(optimize_with_catalog(&inner_plan, schema_of, stats_of)?);
            let join_type = if *negated {
                JoinType::Anti
            } else {
                JoinType::Semi
            };
            Ok(Some(PhysicalPlan::HashJoin {
                left,
                right,
                left_key: Expression::Column(outer_col),
                right_key: Expression::Column(inner_col),
                join_type,
            }))
        }
        Expression::InSubquery {
            expr,
            subquery,
            value_column,
            negated,
            correlated: true,
        } => {
            // Unnest when the correlation equi is on the same columns as the IN
            // probe (`outer.c IN (SELECT inner.c … WHERE inner.c = OuterRef(c))`)
            // or when OuterRef column matches the IN probe column name.
            let Some((outer_col, inner_col, inner_plan)) =
                extract_single_equi_outer_ref(subquery)
            else {
                return Ok(None);
            };
            let probe_is_outer = matches!(
                expr.as_ref(),
                Expression::Column(c) | Expression::OuterRef(c) if c == &outer_col
            );
            if !probe_is_outer || value_column != &inner_col {
                return Ok(None);
            }
            let left = Box::new(optimize_with_catalog(left_plan, schema_of, stats_of)?);
            let right = Box::new(optimize_with_catalog(&inner_plan, schema_of, stats_of)?);
            let join_type = if *negated {
                JoinType::Anti
            } else {
                JoinType::Semi
            };
            Ok(Some(PhysicalPlan::HashJoin {
                left,
                right,
                left_key: Expression::Column(outer_col),
                right_key: Expression::Column(inner_col),
                join_type,
            }))
        }
        _ => Ok(None),
    }
}

/// Pull a single `inner.col = OuterRef(outer.col)` (either order) out of a
/// correlated subquery and return the residual plan without that predicate.
fn extract_single_equi_outer_ref(
    subquery: &LogicalPlan,
) -> Option<(String, String, LogicalPlan)> {
    match subquery {
        LogicalPlan::Project { input, .. } | LogicalPlan::SubqueryAlias { input, .. } => {
            extract_single_equi_outer_ref(input)
        }
        LogicalPlan::Select {
            table,
            filters,
            predicate: Some(pred),
        } if filters.is_empty() => {
            let (outer_col, inner_col, residual) = split_equi_outer_ref(pred)?;
            let plan = LogicalPlan::Select {
                table: table.clone(),
                filters: Vec::new(),
                predicate: residual,
            };
            Some((outer_col, inner_col, plan))
        }
        LogicalPlan::Filter {
            input,
            predicate,
        } => {
            let (outer_col, inner_col, residual) = split_equi_outer_ref(predicate)?;
            let plan = match residual {
                Some(p) => LogicalPlan::Filter {
                    input: input.clone(),
                    predicate: p,
                },
                None => input.as_ref().clone(),
            };
            Some((outer_col, inner_col, plan))
        }
        _ => None,
    }
}

fn split_equi_outer_ref(
    pred: &Expression,
) -> Option<(String, String, Option<Expression>)> {
    match pred {
        Expression::BinaryOp {
            left,
            op: crate::query::FilterOp::Eq,
            right,
        } => match (left.as_ref(), right.as_ref()) {
            (Expression::Column(inner), Expression::OuterRef(outer))
            | (Expression::OuterRef(outer), Expression::Column(inner)) => {
                Some((outer.clone(), inner.clone(), None))
            }
            _ => None,
        },
        Expression::And { left, right } => {
            if let Some((o, i, _)) = split_equi_outer_ref(left) {
                return Some((o, i, Some(right.as_ref().clone())));
            }
            if let Some((o, i, _)) = split_equi_outer_ref(right) {
                return Some((o, i, Some(left.as_ref().clone())));
            }
            None
        }
        _ => None,
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
                // Keep a Filter/Apply when the predicate is compound (AND / residual).
                if !is_bare_column_equality(pred, &col) {
                    return Ok(filter_or_apply(Box::new(scan), pred.clone()));
                }
                return Ok(scan);
            }
        }
        Ok(filter_or_apply(
            Box::new(PhysicalPlan::TableScan {
                table: table.to_string(),
                filters: residual_filters.to_vec(),
            }),
            pred.clone(),
        ))
    } else {
        Ok(PhysicalPlan::TableScan {
            table: table.to_string(),
            filters: residual_filters.to_vec(),
        })
    }
}

/// [`PhysicalPlan::Apply`] when `predicate` still has correlated subqueries; else Filter.
fn filter_or_apply(input: Box<PhysicalPlan>, predicate: Expression) -> PhysicalPlan {
    if predicate_has_correlated(&predicate) {
        PhysicalPlan::Apply { input, predicate }
    } else {
        PhysicalPlan::Filter { input, predicate }
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
        PhysicalPlan::LateralJsonSrf { left, .. }
        | PhysicalPlan::LateralUnnest { left, .. }
        | PhysicalPlan::LateralRegexpSrf { left, .. } => {
            estimate_physical_rows(left, stats_of).saturating_mul(4).max(1)
        }
        PhysicalPlan::NestedLoopJoin { left, right, .. }
        | PhysicalPlan::HashJoin { left, right, .. }
        | PhysicalPlan::MergeJoin { left, right, .. } => estimate_physical_rows(left, stats_of)
            .saturating_mul(estimate_physical_rows(right, stats_of))
            .max(1),
        PhysicalPlan::Aggregate { input, .. }
        | PhysicalPlan::Sort { input, .. }
        | PhysicalPlan::Limit { input, .. }
        | PhysicalPlan::TopN { input, .. }
        | PhysicalPlan::Project { input, .. }
        | PhysicalPlan::Window { input, .. }
        | PhysicalPlan::Distinct { input, .. }
        | PhysicalPlan::DistinctOn { input, .. }
        | PhysicalPlan::Apply { input, .. }
        | PhysicalPlan::Update { input, .. }
        | PhysicalPlan::Delete { input, .. } => estimate_physical_rows(input, stats_of),
        PhysicalPlan::Union {
            left,
            right,
            op,
            all,
        } => {
            let n_left = estimate_physical_rows(left, stats_of);
            let n_right = estimate_physical_rows(right, stats_of);
            match op {
                crate::sql::SetOpKind::Union => {
                    let n = n_left.saturating_add(n_right);
                    if *all { n } else { n.max(1) / 2 + 1 }
                }
                crate::sql::SetOpKind::Intersect => n_left.min(n_right).max(1) / 2 + 1,
                crate::sql::SetOpKind::Except => n_left.saturating_mul(3) / 4 + 1,
            }
        }
        PhysicalPlan::Insert {
            values,
            input,
            ..
        } => {
            if let Some(child) = input {
                estimate_physical_rows(child, stats_of)
            } else {
                values.len() as u64
            }
        }
        PhysicalPlan::Analyze { .. } | PhysicalPlan::Vacuum { .. } => 0,
        PhysicalPlan::JitExec { input, .. } => estimate_physical_rows(input, stats_of),
        PhysicalPlan::VectorizedExec { input, .. } => estimate_physical_rows(input, stats_of),
        PhysicalPlan::DistributedScan { input, .. } => estimate_physical_rows(input, stats_of),
    }
}

/// True when `plan` is (or wraps) an ascending Sort whose leading key equals `key`.
fn is_sorted_on(plan: &PhysicalPlan, key: &Expression) -> bool {
    match plan {
        PhysicalPlan::Sort { exprs, .. } => exprs
            .first()
            .is_some_and(|se| se.asc && &se.expr == key),
        PhysicalPlan::Limit { input, .. } | PhysicalPlan::Filter { input, .. } => {
            is_sorted_on(input, key)
        }
        _ => false,
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
            skip, fetch, with_ties, ties_order, ..
        } => Ok(PhysicalPlan::Limit {
            input: Box::new(PhysicalPlan::Values { rows }),
            skip: *skip,
            fetch: *fetch,
            with_ties: *with_ties,
            ties_order: ties_order.clone(),
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
        Value::String(s) => {
            if let Some(secs) = crate::sql::decode_interval_secs(s) {
                crate::sql::format_interval_secs(secs)
            } else {
                s.clone()
            }
        }
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

/// Raw session txn pointer for streaming Apply (`Executor: Send` erasure).
///
/// SAFETY: plans are driven on a single thread; the pointed-to [`Transaction`]
/// outlives the executor and is not concurrently mutated elsewhere.
struct ApplyTxnPtr(*mut Transaction);
// SAFETY: see struct docs — single-threaded Volcano drive.
unsafe impl Send for ApplyTxnPtr {}

/// Correlated Apply: nested-loop dependent join over an outer pull iterator.
///
/// Emits matching outer rows on demand (streaming). Peak memory is one outer
/// row plus subquery scratch, not the full kept result set.
struct ApplyExec {
    outer: Box<dyn Executor>,
    predicate: Expression,
    ctx: ExecutionContext,
    /// Caller-owned MVCC txn; must outlive this executor (see `open_apply_executor`).
    txn: ApplyTxnPtr,
}

impl Executor for ApplyExec {
    fn next_row(&mut self) -> Result<Option<Record>> {
        // SAFETY: pointer set in `open_apply_executor` from the active execute_plan txn.
        let txn = unsafe { &mut *self.txn.0 };
        while let Some(row) = self.outer.next_row()? {
            if evaluate_bool_correlated(&self.predicate, &row, &self.ctx, txn)? {
                return Ok(Some(row));
            }
        }
        Ok(None)
    }
}

/// Drive an outer input through a correlated predicate (Apply / nested-loop dependent join).
///
/// Streams matching rows on `next_row` (pull outer until a match). Subquery
/// evaluation borrows the caller's MVCC [`Transaction`] for the lifetime of the
/// executor (see SAFETY on [`ApplyExec`]).
fn open_apply_executor(
    input: PhysicalPlan,
    predicate: Expression,
    ctx: &ExecutionContext,
    storage: Option<&mut Transaction>,
) -> Result<Box<dyn Executor>> {
    let txn = storage.ok_or_else(|| {
        TakyonicError::Sql(
            "Apply (correlated subquery) requires an active MVCC transaction".into(),
        )
    })?;
    // SAFETY: `txn` is borrowed from `execute_plan` / open_executor_with_txn for
    // the full lifetime of the returned executor. ApplyExec only touches it from
    // `next_row` while the outer child does not hold a concurrent &mut.
    let txn_ptr = ApplyTxnPtr(txn as *mut Transaction);
    let outer = open_executor_with_storage(input, ctx, Some(unsafe { &mut *txn_ptr.0 }))?;
    Ok(Box::new(ApplyExec {
        outer,
        predicate,
        ctx: ctx.clone(),
        txn: txn_ptr,
    }))
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
            match join_type {
                JoinType::Inner | JoinType::Left | JoinType::Full => {
                    let left_exec =
                        open_executor_with_storage(*left, ctx, storage.as_deref_mut())?;
                    let mut right_exec =
                        open_executor_with_storage(*right, ctx, storage.as_deref_mut())?;
                    let right_rows = drain_executor(right_exec.as_mut())?;
                    Ok(Box::new(NestedLoopJoin::new(
                        left_exec,
                        right_rows,
                        condition,
                        join_type,
                        ctx.clone(),
                    )))
                }
                JoinType::Right => {
                    // Stream SQL-right as outer; materialize SQL-left for null-pad.
                    let right_exec =
                        open_executor_with_storage(*right, ctx, storage.as_deref_mut())?;
                    let mut left_exec =
                        open_executor_with_storage(*left, ctx, storage.as_deref_mut())?;
                    let left_rows = drain_executor(left_exec.as_mut())?;
                    Ok(Box::new(NestedLoopJoin::new_right_outer(
                        right_exec,
                        left_rows,
                        condition,
                        ctx.clone(),
                    )))
                }
                other => {
                    return Err(TakyonicError::Sql(format!(
                        "nested-loop join supports Inner/Left/Right/Full; got {other:?}"
                    )));
                }
            }
        }
        PhysicalPlan::LateralJsonSrf {
            left,
            doc,
            kind,
        } => {
            let left_exec = open_executor_with_storage(*left, ctx, storage)?;
            Ok(Box::new(LateralJsonSrfExec {
                left: left_exec,
                doc,
                kind,
                ctx: ctx.clone(),
                pending: Vec::new(),
                pending_idx: 0,
            }))
        }
        PhysicalPlan::LateralUnnest {
            left,
            array,
            column,
            ordinality_column,
            zero_based_ordinality,
        } => {
            let left_exec = open_executor_with_storage(*left, ctx, storage)?;
            Ok(Box::new(LateralUnnestExec {
                left: left_exec,
                array,
                column,
                ordinality_column,
                zero_based_ordinality,
                ctx: ctx.clone(),
                pending: Vec::new(),
                pending_idx: 0,
            }))
        }
        PhysicalPlan::LateralRegexpSrf {
            left,
            string,
            pattern,
            flags,
            column,
            ordinality_column,
            kind,
        } => {
            let left_exec = open_executor_with_storage(*left, ctx, storage)?;
            Ok(Box::new(LateralRegexpSrfExec {
                left: left_exec,
                string,
                pattern,
                flags,
                column,
                ordinality_column,
                kind,
                ctx: ctx.clone(),
                pending: Vec::new(),
                pending_idx: 0,
            }))
        }
        PhysicalPlan::HashJoin {
            left,
            right,
            left_key,
            right_key,
            join_type,
        } => {
            match join_type {
                JoinType::Inner
                | JoinType::Semi
                | JoinType::Anti
                | JoinType::Left
                | JoinType::Right
                | JoinType::Full => {}
            }
            // Semi/Anti/Left/Full: build hash on the **right**, probe with left.
            // Right: build hash on the **left**, probe with right.
            // Inner: build/probe as stored by the optimizer (may swap for size).
            let (build, probe, build_key, probe_key) = if matches!(
                join_type,
                JoinType::Semi | JoinType::Anti | JoinType::Left | JoinType::Full
            ) {
                (
                    open_executor_with_storage(*right, ctx, storage.as_deref_mut())?,
                    open_executor_with_storage(*left, ctx, storage)?,
                    right_key,
                    left_key,
                )
            } else if join_type == JoinType::Right {
                (
                    open_executor_with_storage(*left, ctx, storage.as_deref_mut())?,
                    open_executor_with_storage(*right, ctx, storage)?,
                    left_key,
                    right_key,
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
        PhysicalPlan::MergeJoin {
            left,
            right,
            left_key,
            right_key,
            join_type,
        } => {
            if join_type != JoinType::Inner {
                return Err(TakyonicError::Sql(format!(
                    "only INNER merge join is implemented; got {join_type:?}"
                )));
            }
            let left_exec = open_executor_with_storage(*left, ctx, storage.as_deref_mut())?;
            let right_exec = open_executor_with_storage(*right, ctx, storage)?;
            Ok(Box::new(MergeJoinExec::new(
                left_exec,
                right_exec,
                left_key,
                right_key,
                ctx.clone(),
            )))
        }
        PhysicalPlan::Filter { input, predicate } => {
            let predicate = if let Some(txn) = storage.as_deref_mut() {
                rewrite_uncorrelated_subqueries(predicate, ctx, txn)?
            } else {
                predicate
            };
            // Uncorrelated rewrite may clear the last correlated node — fall back
            // to a plain Filter when nothing correlated remains.
            if predicate_has_correlated(&predicate) {
                return open_apply_executor(*input, predicate, ctx, storage);
            }
            let child = open_executor_with_storage(*input, ctx, storage)?;
            Ok(Box::new(FilterExec {
                input: child,
                predicate,
                ctx: ctx.clone(),
            }))
        }
        PhysicalPlan::Apply { input, predicate } => {
            let predicate = if let Some(txn) = storage.as_deref_mut() {
                rewrite_uncorrelated_subqueries(predicate, ctx, txn)?
            } else {
                predicate
            };
            if !predicate_has_correlated(&predicate) {
                let child = open_executor_with_storage(*input, ctx, storage)?;
                return Ok(Box::new(FilterExec {
                    input: child,
                    predicate,
                    ctx: ctx.clone(),
                }));
            }
            open_apply_executor(*input, predicate, ctx, storage)
        }
        PhysicalPlan::Insert {
            table,
            columns,
            values,
            input,
            source_columns,
            on_conflict,
            returning,
        } => {
            let records = if let Some(child) = input {
                let mut child_exec =
                    open_executor_with_storage(*child, ctx, storage.as_deref_mut())?;
                let rows = drain_executor(child_exec.as_mut())?;
                remap_insert_select_rows(&rows, &source_columns, &columns)?
            } else {
                materialize_insert_records(&columns, &values, ctx)?
            };
            let txn = storage.as_deref_mut().ok_or_else(|| {
                TakyonicError::Sql("INSERT requires an active MVCC transaction".into())
            })?;
            InsertExec::run(
                txn,
                &table,
                records,
                on_conflict,
                returning.as_ref(),
                ctx,
            )
        }
        PhysicalPlan::Update {
            table,
            assignments,
            input,
            returning,
        } => {
            // Child scan buffers rows and releases the txn borrow before we mutate.
            let mut child = open_executor_with_storage(*input, ctx, storage.as_deref_mut())?;
            let targets = drain_executor(child.as_mut())?;
            let txn = storage.as_deref_mut().ok_or_else(|| {
                TakyonicError::Sql("UPDATE requires an active MVCC transaction".into())
            })?;
            UpdateExec::run(
                txn,
                &table,
                &assignments,
                targets,
                returning.as_ref(),
                ctx,
            )
        }
        PhysicalPlan::Delete {
            table,
            input,
            returning,
        } => {
            let mut child = open_executor_with_storage(*input, ctx, storage.as_deref_mut())?;
            let targets = drain_executor(child.as_mut())?;
            let txn = storage.as_deref_mut().ok_or_else(|| {
                TakyonicError::Sql("DELETE requires an active MVCC transaction".into())
            })?;
            DeleteExec::run(txn, &table, targets, returning.as_ref(), ctx)
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
            with_ties,
            ties_order,
        } => {
            let child = open_executor_with_storage(*input, ctx, storage)?;
            Ok(Box::new(LimitExec::new(
                child,
                skip,
                fetch,
                with_ties,
                ties_order,
                ctx.clone(),
            )))
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
        PhysicalPlan::Project { input, columns } => {
            let child = open_executor_with_storage(*input, ctx, storage)?;
            Ok(Box::new(ProjectExec {
                input: child,
                columns,
                ctx: ctx.clone(),
            }))
        }
        PhysicalPlan::Window { input, calls } => {
            let child = open_executor_with_storage(*input, ctx, storage)?;
            Ok(Box::new(WindowExec::new(child, calls, ctx.clone())))
        }
        PhysicalPlan::Union {
            left,
            right,
            op,
            all,
        } => {
            let left = open_executor_with_storage(*left, ctx, storage.as_deref_mut())?;
            let right = open_executor_with_storage(*right, ctx, storage)?;
            Ok(Box::new(UnionExec::new(left, right, op, all)))
        }
        PhysicalPlan::Distinct { input } => {
            let child = open_executor_with_storage(*input, ctx, storage)?;
            Ok(Box::new(DistinctExec::new(child)))
        }
        PhysicalPlan::DistinctOn { input, exprs } => {
            let child = open_executor_with_storage(*input, ctx, storage)?;
            Ok(Box::new(DistinctOnExec::new(child, exprs, ctx.clone())))
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
            // Without an MPP Session path, volcano uses the pruned local access
            // path; Coordinator::execute_distributed_scan handles RemoteWorker
            // fetch when mpp_enabled.
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

/// Map SELECT output rows onto INSERT target column names (positional).
pub fn remap_insert_select_rows(
    rows: &[Record],
    source_names: &[String],
    dest_names: &[String],
) -> Result<Vec<Record>> {
    if source_names.len() != dest_names.len() {
        return Err(TakyonicError::Sql(format!(
            "INSERT SELECT column count mismatch: {} target columns for {} query columns",
            dest_names.len(),
            source_names.len()
        )));
    }
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let mut record = Record::new();
        for (src, dst) in source_names.iter().zip(dest_names.iter()) {
            let val = row.get(src).unwrap_or("");
            record = record.set(dst.clone(), val);
        }
        out.push(record);
    }
    Ok(out)
}

impl InsertExec {
    /// Run the insert; yields RETURNING rows or a single affected-row count.
    pub fn run(
        txn: &mut Transaction,
        table: &str,
        records: Vec<Record>,
        on_conflict: Option<crate::sql::OnConflict>,
        returning: Option<&crate::sql::Returning>,
        ctx: &ExecutionContext,
    ) -> Result<Box<dyn Executor>> {
        let schema = txn.table_schema(table)?;
        let mut out = Vec::new();
        let mut affected = 0u64;
        for record in &records {
            let mut record = record.clone();
            let col_names: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
            crate::sql::fill_serial_defaults(ctx.session_id, table, &col_names, &mut record)?;
            crate::sql::fill_column_defaults(ctx.session_id, &schema, &mut record, ctx)?;
            validate_record_against_catalog(&record, &schema)?;
            enforce_unique_constraints(txn, table, &schema, &record, None)?;
            let pk = record.get(&schema.primary_key).ok_or_else(|| {
                TakyonicError::Sql(format!(
                    "INSERT row missing primary key `{}`",
                    schema.primary_key
                ))
            })?;
            if let Some(existing) = txn.get_record(table, pk)? {
                match &on_conflict {
                    None => {
                        // Default: overwrite via put_record (legacy upsert).
                        txn.put_record(table, record.clone())?;
                        affected += 1;
                        if let Some(ret) = returning {
                            out.push(project_returning_row(ret, &record, ctx)?);
                        }
                    }
                    Some(crate::sql::OnConflict::DoNothing) => {
                        // Skip existing PK.
                    }
                    Some(crate::sql::OnConflict::DoUpdate {
                        assignments,
                        selection,
                    }) => {
                        let mut eval_row = existing.clone();
                        for (k, v) in &record.fields {
                            eval_row = eval_row.set(
                                format!("{}{k}", crate::sql::EXCLUDED_FIELD_PREFIX),
                                v.clone(),
                            );
                        }
                        if let Some(pred) = selection {
                            if !evaluate_bool(pred, &eval_row, ctx)? {
                                continue;
                            }
                        }
                        let mut updated = existing;
                        for (col, expr) in assignments {
                            let v = evaluate(expr, &eval_row, ctx)?;
                            updated = updated.set(col.clone(), value_to_field(&v));
                        }
                        validate_record_against_catalog(&updated, &schema)?;
                        txn.put_record(table, updated.clone())?;
                        affected += 1;
                        if let Some(ret) = returning {
                            out.push(project_returning_row(ret, &updated, ctx)?);
                        }
                    }
                }
            } else {
                txn.put_record(table, record.clone())?;
                affected += 1;
                if let Some(ret) = returning {
                    out.push(project_returning_row(ret, &record, ctx)?);
                }
            }
        }
        if returning.is_some() {
            Ok(Box::new(ValuesExec::new(out)))
        } else {
            Ok(Box::new(AffectedRowsExec::new(affected)))
        }
    }
}

/// Apply SET assignments to target rows and rewrite via [`Transaction::put_record`].
pub struct UpdateExec;

impl UpdateExec {
    /// Run the update; yields RETURNING rows or a single affected-row count.
    pub fn run(
        txn: &mut Transaction,
        table: &str,
        assignments: &HashMap<String, Expression>,
        targets: Vec<Record>,
        returning: Option<&crate::sql::Returning>,
        ctx: &ExecutionContext,
    ) -> Result<Box<dyn Executor>> {
        let schema = txn.table_schema(table)?;
        let mut count = 0u64;
        let mut out = Vec::new();
        for mut row in targets {
            for (col, expr) in assignments {
                let v = evaluate(expr, &row, ctx)?;
                row = row.set(col.clone(), value_to_field(&v));
            }
            validate_record_against_catalog(&row, &schema)?;
            txn.put_record(table, row.clone())?;
            count += 1;
            if let Some(ret) = returning {
                out.push(project_returning_row(ret, &row, ctx)?);
            }
        }
        if returning.is_some() {
            Ok(Box::new(ValuesExec::new(out)))
        } else {
            Ok(Box::new(AffectedRowsExec::new(count)))
        }
    }
}

/// Delete target rows via [`Transaction::delete_record`] (tombstones).
pub struct DeleteExec;

impl DeleteExec {
    /// Run the delete; yields RETURNING rows or a single affected-row count.
    pub fn run(
        txn: &mut Transaction,
        table: &str,
        targets: Vec<Record>,
        returning: Option<&crate::sql::Returning>,
        ctx: &ExecutionContext,
    ) -> Result<Box<dyn Executor>> {
        let schema = txn.table_schema(table)?;
        let mut count = 0u64;
        let mut out = Vec::new();
        for row in targets {
            let pk = row.get(&schema.primary_key).ok_or_else(|| {
                TakyonicError::Sql(format!(
                    "DELETE row missing primary key `{}`",
                    schema.primary_key
                ))
            })?;
            if let Some(ret) = returning {
                out.push(project_returning_row(ret, &row, ctx)?);
            }
            txn.delete_record(table, pk)?;
            count += 1;
        }
        if returning.is_some() {
            Ok(Box::new(ValuesExec::new(out)))
        } else {
            Ok(Box::new(AffectedRowsExec::new(count)))
        }
    }
}

fn project_returning_row(
    returning: &crate::sql::Returning,
    row: &Record,
    ctx: &ExecutionContext,
) -> Result<Record> {
    match returning {
        crate::sql::Returning::Star => Ok(row.clone()),
        crate::sql::Returning::List(cols) => {
            let mut out = Record::new();
            for (name, expr) in cols {
                let v = evaluate(expr, row, ctx)?;
                out = out.set(name.clone(), value_to_field(&v));
            }
            Ok(out)
        }
    }
}

fn validate_record_against_catalog(record: &Record, schema: &TableSchema) -> Result<()> {
    if record.get(&schema.primary_key).is_none() {
        return Err(TakyonicError::Sql(format!(
            "record missing primary key `{}`",
            schema.primary_key
        )));
    }
    for col in &schema.columns {
        let val = record.get(&col.name);
        let is_null = match val {
            None => true,
            Some(s) => s.is_empty(),
        };
        if !col.nullable && is_null {
            return Err(TakyonicError::Sql(format!(
                "null value in column \"{}\" of relation \"{}\" violates not-null constraint",
                col.name, schema.name
            )));
        }
        if let Some(s) = val.filter(|s| !s.is_empty()) {
            coerce_column_value(&col.data_type, s).map_err(|e| {
                TakyonicError::Sql(format!(
                    "column \"{}\": {e}",
                    col.name
                ))
            })?;
        }
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

/// Soft coerce / validate catalog type tokens for INSERT values.
fn coerce_column_value(data_type: &str, value: &str) -> Result<()> {
    let t = data_type.to_ascii_uppercase();
    let t = t.split('(').next().unwrap_or(&t);
    match t {
        "UUID" => {
            if !looks_like_uuid(value) {
                return Err(TakyonicError::Sql(format!(
                    "invalid input syntax for type uuid: \"{value}\""
                )));
            }
        }
        "BYTEA" => {
            // Accept hex `\x…` or plain text (stored as UTF-8 string).
            if value.starts_with("\\x") || value.starts_with("\\X") {
                let hex = &value[2..];
                if hex.len() % 2 != 0 || !hex.chars().all(|c| c.is_ascii_hexdigit()) {
                    return Err(TakyonicError::Sql(
                        "invalid bytea hex encoding".into(),
                    ));
                }
            }
        }
        "NUMERIC" | "DECIMAL" => {
            if value.parse::<f64>().is_err() {
                return Err(TakyonicError::Sql(format!(
                    "invalid input syntax for type numeric: \"{value}\""
                )));
            }
        }
        "TIMESTAMPTZ" | "TIMESTAMP_WITH_TIME_ZONE" => {
            // Accept anything NOW()-shaped or ISO-ish; reject empty already handled.
            if value.len() < 4 {
                return Err(TakyonicError::Sql(format!(
                    "invalid input syntax for type timestamptz: \"{value}\""
                )));
            }
        }
        _ => {}
    }
    Ok(())
}

fn looks_like_uuid(s: &str) -> bool {
    let s = s.trim();
    if s.len() != 36 {
        return false;
    }
    let b = s.as_bytes();
    b[8] == b'-'
        && b[13] == b'-'
        && b[18] == b'-'
        && b[23] == b'-'
        && s.chars()
            .enumerate()
            .all(|(i, c)| matches!(i, 8 | 13 | 18 | 23) || c.is_ascii_hexdigit())
}

/// Reject INSERT/UPDATE when a UNIQUE column collides with another live row.
fn enforce_unique_constraints(
    txn: &mut Transaction,
    table: &str,
    schema: &TableSchema,
    record: &Record,
    exclude_pk: Option<&str>,
) -> Result<()> {
    for col in &schema.columns {
        if !col.unique || col.name == schema.primary_key {
            continue;
        }
        let Some(val) = record.get(&col.name) else {
            continue;
        };
        if val.is_empty() {
            continue; // NULL is distinct for UNIQUE in PG
        }
        // Prefer unique index named uq_<table>_<col> when present.
        let idx_name = format!("uq_{table}_{}", col.name);
        if schema.indexes.iter().any(|i| i.name == idx_name) {
            let hits = txn.lookup_by_index(table, &idx_name, val)?;
            for other in hits {
                let other_pk = other.get(&schema.primary_key).unwrap_or("");
                if exclude_pk.is_some_and(|e| e == other_pk) {
                    continue;
                }
                if other.get(&col.name) == Some(val) {
                    return Err(TakyonicError::Sql(format!(
                        "duplicate key value violates unique constraint on \"{}\".\"{}\"",
                        table, col.name
                    )));
                }
            }
        } else {
            for other in txn.scan_table_records(table)? {
                let other_pk = other.get(&schema.primary_key).unwrap_or("");
                if exclude_pk.is_some_and(|e| e == other_pk) {
                    continue;
                }
                if other.get(&col.name) == Some(val) {
                    return Err(TakyonicError::Sql(format!(
                        "duplicate key value violates unique constraint on \"{}\".\"{}\"",
                        table, col.name
                    )));
                }
            }
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
            PhysicalPlan::Window { input, calls } => {
                let names: Vec<_> = calls.iter().map(|c| c.output_column.as_str()).collect();
                let _ = writeln!(out, "{pad}Window({})", names.join(", "));
                walk(input, indent + 1, out);
            }
            PhysicalPlan::Apply { input, .. } => {
                let _ = writeln!(out, "{pad}Apply");
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
            PhysicalPlan::LateralJsonSrf { left, kind, .. } => {
                let label = match kind {
                    LateralJsonSrfKind::ArrayElements { column, .. } => {
                        format!("LateralJsonArrayElements({column})")
                    }
                    LateralJsonSrfKind::Each {
                        key_column,
                        value_column,
                        ..
                    } => format!("LateralJsonEach({key_column},{value_column})"),
                    LateralJsonSrfKind::ObjectKeys { column, .. } => {
                        format!("LateralJsonObjectKeys({column})")
                    }
                };
                let _ = writeln!(out, "{pad}{label}");
                walk(left, indent + 1, out);
            }
            PhysicalPlan::LateralUnnest { left, column, .. } => {
                let _ = writeln!(out, "{pad}LateralUnnest({column})");
                walk(left, indent + 1, out);
            }
            PhysicalPlan::LateralRegexpSrf {
                left,
                column,
                kind,
                ..
            } => {
                let label = match kind {
                    LateralRegexpSrfKind::SplitToTable => {
                        format!("LateralRegexpSplitToTable({column})")
                    }
                    LateralRegexpSrfKind::Matches => format!("LateralRegexpMatches({column})"),
                };
                let _ = writeln!(out, "{pad}{label}");
                walk(left, indent + 1, out);
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
            PhysicalPlan::MergeJoin { left, right, .. } => {
                let _ = writeln!(out, "{pad}MergeJoin");
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
            PhysicalPlan::Project { input, columns } => {
                let names: Vec<&str> = columns.iter().map(|(n, _)| n.as_str()).collect();
                let _ = writeln!(out, "{pad}Project({})", names.join(", "));
                walk(input, indent + 1, out);
            }
            PhysicalPlan::Union {
                left,
                right,
                op,
                all,
            } => {
                let _ = writeln!(
                    out,
                    "{pad}{}({})",
                    op.sql_name(),
                    if *all { "ALL" } else { "DISTINCT" }
                );
                walk(left, indent + 1, out);
                walk(right, indent + 1, out);
            }
            PhysicalPlan::Distinct { input } => {
                let _ = writeln!(out, "{pad}Distinct");
                walk(input, indent + 1, out);
            }
            PhysicalPlan::DistinctOn { input, exprs } => {
                let _ = writeln!(out, "{pad}DistinctOn({})", exprs.len());
                walk(input, indent + 1, out);
            }
            PhysicalPlan::Insert { table, .. } => {
                let _ = writeln!(out, "{pad}Insert({table})");
            }
            PhysicalPlan::Update { table, input, .. } => {
                let _ = writeln!(out, "{pad}Update({table})");
                walk(input, indent + 1, out);
            }
            PhysicalPlan::Delete { table, input, .. } => {
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

/// Project iterator: evaluate SELECT-list expressions into a new [`Record`].
struct ProjectExec {
    input: Box<dyn Executor>,
    columns: Vec<(String, Expression)>,
    ctx: ExecutionContext,
}

impl Executor for ProjectExec {
    fn next_row(&mut self) -> Result<Option<Record>> {
        let Some(row) = self.input.next_row()? else {
            return Ok(None);
        };
        let mut out = Record::new();
        for (name, expr) in &self.columns {
            let val = evaluate(expr, &row, &self.ctx)?;
            out = out.set(name, value_to_field(&val));
        }
        Ok(Some(out))
    }
}

/// Blocking window operator (`ROW_NUMBER() OVER …`).
struct WindowExec {
    input: Box<dyn Executor>,
    calls: Vec<crate::sql::WindowCall>,
    ctx: ExecutionContext,
    pending: Option<Vec<Record>>,
    emit_idx: usize,
}

impl WindowExec {
    fn new(
        input: Box<dyn Executor>,
        calls: Vec<crate::sql::WindowCall>,
        ctx: ExecutionContext,
    ) -> Self {
        Self {
            input,
            calls,
            ctx,
            pending: None,
            emit_idx: 0,
        }
    }

    fn materialize(&mut self) -> Result<Vec<Record>> {
        let mut rows = Vec::new();
        while let Some(row) = self.input.next_row()? {
            rows.push(row);
        }
        for call in &self.calls {
            // Sort by PARTITION BY keys, then ORDER BY keys.
            if !call.partition_by.is_empty() || !call.order_by.is_empty() {
                let parts = call.partition_by.clone();
                let order = call.order_by.clone();
                let ctx = self.ctx.clone();
                rows.sort_by(|a, b| {
                    for p in &parts {
                        let av = evaluate(p, a, &ctx).unwrap_or(Value::Null);
                        let bv = evaluate(p, b, &ctx).unwrap_or(Value::Null);
                        let c = value_ord(&av, &bv);
                        if c != Ordering::Equal {
                            return c;
                        }
                    }
                    if order.is_empty() {
                        return Ordering::Equal;
                    }
                    let mut ka = Vec::with_capacity(order.len());
                    let mut kb = Vec::with_capacity(order.len());
                    for s in &order {
                        ka.push(evaluate(&s.expr, a, &ctx).unwrap_or(Value::Null));
                        kb.push(evaluate(&s.expr, b, &ctx).unwrap_or(Value::Null));
                    }
                    cmp_sort_keys(&ka, &kb, &order)
                });
            }

            let n = rows.len();
            let mut out_cells = vec![String::new(); n];
            let mut part_start = 0usize;
            // Precompute partition ends for LAG/LEAD lookups.
            let mut part_ends = vec![n; n];
            if !call.partition_by.is_empty() {
                let mut starts = Vec::new();
                let mut s = 0usize;
                for i in 1..n {
                    let mut changed = false;
                    for p in &call.partition_by {
                        let av = evaluate(p, &rows[i - 1], &self.ctx)?;
                        let bv = evaluate(p, &rows[i], &self.ctx)?;
                        if value_ord(&av, &bv) != Ordering::Equal {
                            changed = true;
                            break;
                        }
                    }
                    if changed {
                        starts.push((s, i));
                        s = i;
                    }
                }
                starts.push((s, n));
                for (s, e) in starts {
                    for i in s..e {
                        part_ends[i] = e;
                    }
                }
            }

            for i in 0..n {
                if i > 0 {
                    let new_part = if call.partition_by.is_empty() {
                        false
                    } else {
                        let mut changed = false;
                        for p in &call.partition_by {
                            let av = evaluate(p, &rows[i - 1], &self.ctx)?;
                            let bv = evaluate(p, &rows[i], &self.ctx)?;
                            if value_ord(&av, &bv) != Ordering::Equal {
                                changed = true;
                                break;
                            }
                        }
                        changed
                    };
                    if new_part {
                        part_start = i;
                    }
                }
                let part_end = part_ends[i];
                let local_i = i - part_start;
                match call.kind {
                    crate::sql::WindowKind::RowNumber => {
                        out_cells[i] = (local_i as i64 + 1).to_string();
                    }
                    crate::sql::WindowKind::Rank | crate::sql::WindowKind::DenseRank => {
                        if local_i == 0 {
                            out_cells[i] = "1".into();
                        } else {
                            let same = if call.order_by.is_empty() {
                                true
                            } else {
                                let order = &call.order_by;
                                let mut ka = Vec::with_capacity(order.len());
                                let mut kb = Vec::with_capacity(order.len());
                                for s in order {
                                    ka.push(evaluate(&s.expr, &rows[i - 1], &self.ctx)?);
                                    kb.push(evaluate(&s.expr, &rows[i], &self.ctx)?);
                                }
                                cmp_sort_keys(&ka, &kb, order) == Ordering::Equal
                            };
                            if same {
                                out_cells[i] = out_cells[i - 1].clone();
                            } else if call.kind == crate::sql::WindowKind::Rank {
                                out_cells[i] = (local_i as i64 + 1).to_string();
                            } else {
                                let prev: i64 = out_cells[i - 1].parse().unwrap_or(1);
                                out_cells[i] = (prev + 1).to_string();
                            }
                        }
                    }
                    crate::sql::WindowKind::Lag | crate::sql::WindowKind::Lead => {
                        let value_expr = call.value.as_ref().ok_or_else(|| {
                            TakyonicError::Sql("LAG/LEAD missing value expression".into())
                        })?;
                        let off = call.offset.max(1) as usize;
                        let found = if call.ignore_nulls {
                            // Count only non-NULL value steps within the partition.
                            let mut seen = 0usize;
                            if call.kind == crate::sql::WindowKind::Lag {
                                let mut j = i;
                                let mut hit = None;
                                while j > part_start {
                                    j -= 1;
                                    let v = evaluate(value_expr, &rows[j], &self.ctx)?;
                                    if v.is_null() {
                                        continue;
                                    }
                                    seen += 1;
                                    if seen == off {
                                        hit = Some(v);
                                        break;
                                    }
                                }
                                hit
                            } else {
                                let mut j = i + 1;
                                let mut hit = None;
                                while j < part_end {
                                    let v = evaluate(value_expr, &rows[j], &self.ctx)?;
                                    j += 1;
                                    if v.is_null() {
                                        continue;
                                    }
                                    seen += 1;
                                    if seen == off {
                                        hit = Some(v);
                                        break;
                                    }
                                }
                                hit
                            }
                        } else {
                            let target = if call.kind == crate::sql::WindowKind::Lag {
                                i.checked_sub(off)
                            } else {
                                Some(i + off)
                            };
                            match target {
                                Some(t) if t >= part_start && t < part_end => {
                                    Some(evaluate(value_expr, &rows[t], &self.ctx)?)
                                }
                                _ => None,
                            }
                        };
                        if let Some(v) = found {
                            out_cells[i] = value_to_field(&v);
                        } else if let Some(def) = &call.default_value {
                            let v = evaluate(def, &rows[i], &self.ctx)?;
                            out_cells[i] = value_to_field(&v);
                        } else {
                            out_cells[i] = String::new(); // NULL sentinel
                        }
                    }
                    crate::sql::WindowKind::Ntile => {
                        let buckets = call.offset.max(1) as usize;
                        let part_len = part_end - part_start;
                        out_cells[i] = ntile_bucket(local_i, part_len, buckets).to_string();
                    }
                    crate::sql::WindowKind::FirstValue | crate::sql::WindowKind::LastValue => {
                        let value_expr = call.value.as_ref().ok_or_else(|| {
                            TakyonicError::Sql(
                                "FIRST_VALUE/LAST_VALUE missing value expression".into(),
                            )
                        })?;
                        let members = window_frame_members(
                            i,
                            part_start,
                            part_end,
                            call.frame.as_ref(),
                            &call.order_by,
                            &rows,
                            &self.ctx,
                        )?;
                        if members.is_empty() {
                            out_cells[i] = String::new();
                        } else if call.ignore_nulls {
                            let mut hit = None;
                            if call.kind == crate::sql::WindowKind::FirstValue {
                                for &t in &members {
                                    let v = evaluate(value_expr, &rows[t], &self.ctx)?;
                                    if !v.is_null() {
                                        hit = Some(v);
                                        break;
                                    }
                                }
                            } else {
                                for &t in members.iter().rev() {
                                    let v = evaluate(value_expr, &rows[t], &self.ctx)?;
                                    if !v.is_null() {
                                        hit = Some(v);
                                        break;
                                    }
                                }
                            }
                            out_cells[i] = hit
                                .map(|v| value_to_field(&v))
                                .unwrap_or_default();
                        } else {
                            let target = if call.kind == crate::sql::WindowKind::FirstValue {
                                members[0]
                            } else {
                                *members.last().unwrap()
                            };
                            let v = evaluate(value_expr, &rows[target], &self.ctx)?;
                            out_cells[i] = value_to_field(&v);
                        }
                    }
                    crate::sql::WindowKind::NthValue => {
                        let value_expr = call.value.as_ref().ok_or_else(|| {
                            TakyonicError::Sql("NTH_VALUE missing value expression".into())
                        })?;
                        let members = window_frame_members(
                            i,
                            part_start,
                            part_end,
                            call.frame.as_ref(),
                            &call.order_by,
                            &rows,
                            &self.ctx,
                        )?;
                        let n = call.offset.max(1) as usize;
                        if call.ignore_nulls {
                            let mut seen = 0usize;
                            let mut hit = None;
                            for &t in &members {
                                let v = evaluate(value_expr, &rows[t], &self.ctx)?;
                                if v.is_null() {
                                    continue;
                                }
                                seen += 1;
                                if seen == n {
                                    hit = Some(v);
                                    break;
                                }
                            }
                            out_cells[i] = hit
                                .map(|v| value_to_field(&v))
                                .unwrap_or_default();
                        } else if n > 0 && n <= members.len() {
                            let v = evaluate(value_expr, &rows[members[n - 1]], &self.ctx)?;
                            out_cells[i] = value_to_field(&v);
                        } else {
                            out_cells[i] = String::new(); // NULL sentinel
                        }
                    }
                    crate::sql::WindowKind::PercentRank | crate::sql::WindowKind::CumeDist => {
                        let part_len = part_end - part_start;
                        let same_order = |a: usize, b: usize| -> Result<bool> {
                            if call.order_by.is_empty() {
                                return Ok(true);
                            }
                            let order = &call.order_by;
                            let mut ka = Vec::with_capacity(order.len());
                            let mut kb = Vec::with_capacity(order.len());
                            for s in order {
                                ka.push(evaluate(&s.expr, &rows[a], &self.ctx)?);
                                kb.push(evaluate(&s.expr, &rows[b], &self.ctx)?);
                            }
                            Ok(cmp_sort_keys(&ka, &kb, order) == Ordering::Equal)
                        };
                        if call.kind == crate::sql::WindowKind::PercentRank {
                            // RANK semantics: ties share the first peer's 1-based position.
                            let rank = if local_i == 0 {
                                1i64
                            } else if same_order(i - 1, i)? {
                                let mut j = i;
                                while j > part_start && same_order(j - 1, j)? {
                                    j -= 1;
                                }
                                (j - part_start) as i64 + 1
                            } else {
                                local_i as i64 + 1
                            };
                            let pr = if part_len <= 1 {
                                0.0
                            } else {
                                (rank - 1) as f64 / (part_len - 1) as f64
                            };
                            out_cells[i] = pr.to_string();
                        } else {
                            // CUME_DIST: fraction of rows with order key ≤ current (end of peer group).
                            let mut peer_end = i + 1;
                            while peer_end < part_end && same_order(i, peer_end)? {
                                peer_end += 1;
                            }
                            let cd = (peer_end - part_start) as f64 / part_len as f64;
                            out_cells[i] = cd.to_string();
                        }
                    }
                    crate::sql::WindowKind::Sum
                    | crate::sql::WindowKind::Avg
                    | crate::sql::WindowKind::Count
                    | crate::sql::WindowKind::Min
                    | crate::sql::WindowKind::Max
                    | crate::sql::WindowKind::StringAgg
                    | crate::sql::WindowKind::ArrayAgg
                    | crate::sql::WindowKind::BoolAnd
                    | crate::sql::WindowKind::BoolOr
                    | crate::sql::WindowKind::JsonAgg
                    | crate::sql::WindowKind::JsonbAgg
                    | crate::sql::WindowKind::StddevSamp
                    | crate::sql::WindowKind::StddevPop
                    | crate::sql::WindowKind::VarSamp
                    | crate::sql::WindowKind::VarPop
                    | crate::sql::WindowKind::Corr
                    | crate::sql::WindowKind::CovarPop
                    | crate::sql::WindowKind::CovarSamp
                    | crate::sql::WindowKind::RegrSlope
                    | crate::sql::WindowKind::RegrIntercept
                    | crate::sql::WindowKind::RegrR2
                    | crate::sql::WindowKind::RegrCount
                    | crate::sql::WindowKind::RegrAvgX
                    | crate::sql::WindowKind::RegrAvgY
                    | crate::sql::WindowKind::RegrSxx
                    | crate::sql::WindowKind::RegrSyy
                    | crate::sql::WindowKind::RegrSxy
                    | crate::sql::WindowKind::BitAnd
                    | crate::sql::WindowKind::BitOr
                    | crate::sql::WindowKind::Mode
                    | crate::sql::WindowKind::JsonObjectAgg
                    | crate::sql::WindowKind::JsonbObjectAgg => {
                        let members =
                            window_frame_members(
                                i,
                                part_start,
                                part_end,
                                call.frame.as_ref(),
                                &call.order_by,
                                &rows,
                                &self.ctx,
                            )?;
                        let acc_name = match call.kind {
                            crate::sql::WindowKind::Sum => "SUM",
                            crate::sql::WindowKind::Avg => "AVG",
                            crate::sql::WindowKind::Min => "MIN",
                            crate::sql::WindowKind::Max => "MAX",
                            crate::sql::WindowKind::Count if call.value.is_none() => "COUNT_STAR",
                            crate::sql::WindowKind::Count => "COUNT",
                            crate::sql::WindowKind::StringAgg => "STRING_AGG",
                            crate::sql::WindowKind::ArrayAgg => "ARRAY_AGG",
                            crate::sql::WindowKind::BoolAnd => "BOOL_AND",
                            crate::sql::WindowKind::BoolOr => "BOOL_OR",
                            crate::sql::WindowKind::JsonAgg => "JSON_AGG",
                            crate::sql::WindowKind::JsonbAgg => "JSONB_AGG",
                            crate::sql::WindowKind::StddevSamp => "STDDEV_SAMP",
                            crate::sql::WindowKind::StddevPop => "STDDEV_POP",
                            crate::sql::WindowKind::VarSamp => "VAR_SAMP",
                            crate::sql::WindowKind::VarPop => "VAR_POP",
                            crate::sql::WindowKind::Corr => "CORR",
                            crate::sql::WindowKind::CovarPop => "COVAR_POP",
                            crate::sql::WindowKind::CovarSamp => "COVAR_SAMP",
                            crate::sql::WindowKind::RegrSlope => "REGR_SLOPE",
                            crate::sql::WindowKind::RegrIntercept => "REGR_INTERCEPT",
                            crate::sql::WindowKind::RegrR2 => "REGR_R2",
                            crate::sql::WindowKind::RegrCount => "REGR_COUNT",
                            crate::sql::WindowKind::RegrAvgX => "REGR_AVGX",
                            crate::sql::WindowKind::RegrAvgY => "REGR_AVGY",
                            crate::sql::WindowKind::RegrSxx => "REGR_SXX",
                            crate::sql::WindowKind::RegrSyy => "REGR_SYY",
                            crate::sql::WindowKind::RegrSxy => "REGR_SXY",
                            crate::sql::WindowKind::BitAnd => "BIT_AND",
                            crate::sql::WindowKind::BitOr => "BIT_OR",
                            crate::sql::WindowKind::Mode => "MODE",
                            crate::sql::WindowKind::JsonObjectAgg => "JSON_OBJECT_AGG",
                            crate::sql::WindowKind::JsonbObjectAgg => "JSONB_OBJECT_AGG",
                            _ => unreachable!(),
                        };
                        let mut acc = new_base_accumulator(acc_name)?;
                        for &idx in &members {
                            let row = &rows[idx];
                            if let Some(pred) = &call.filter {
                                if !evaluate_bool(pred, row, &self.ctx)? {
                                    continue;
                                }
                            }
                            let vals = match call.kind {
                                crate::sql::WindowKind::StringAgg
                                | crate::sql::WindowKind::JsonObjectAgg
                                | crate::sql::WindowKind::JsonbObjectAgg
                                | crate::sql::WindowKind::Corr
                                | crate::sql::WindowKind::CovarPop
                                | crate::sql::WindowKind::CovarSamp
                                | crate::sql::WindowKind::RegrSlope
                                | crate::sql::WindowKind::RegrIntercept
                                | crate::sql::WindowKind::RegrR2
                                | crate::sql::WindowKind::RegrCount
                                | crate::sql::WindowKind::RegrAvgX
                                | crate::sql::WindowKind::RegrAvgY
                                | crate::sql::WindowKind::RegrSxx
                                | crate::sql::WindowKind::RegrSyy
                                | crate::sql::WindowKind::RegrSxy => {
                                    let y = call.value.as_ref().ok_or_else(|| {
                                        TakyonicError::Sql(format!(
                                            "{acc_name} missing first argument"
                                        ))
                                    })?;
                                    let x = call.default_value.as_ref().ok_or_else(|| {
                                        TakyonicError::Sql(format!(
                                            "{acc_name} missing second argument"
                                        ))
                                    })?;
                                    vec![
                                        evaluate(y, row, &self.ctx)?,
                                        evaluate(x, row, &self.ctx)?,
                                    ]
                                }
                                _ => match &call.value {
                                    None => Vec::new(),
                                    Some(e) => vec![evaluate(e, row, &self.ctx)?],
                                },
                            };
                            acc.update(&vals)?;
                        }
                        out_cells[i] = value_to_field(&acc.evaluate()?);
                    }
                }
            }
            for (i, row) in rows.iter_mut().enumerate() {
                *row = std::mem::take(row).set(&call.output_column, out_cells[i].clone());
            }
        }
        Ok(rows)
    }
}

/// Assign `NTILE(buckets)` for 0-based `local_i` within a partition of `part_len`.
fn ntile_bucket(local_i: usize, part_len: usize, buckets: usize) -> i64 {
    if part_len == 0 || buckets == 0 {
        return 1;
    }
    if buckets >= part_len {
        return (local_i + 1) as i64;
    }
    let size = part_len / buckets;
    let rem = part_len % buckets;
    if local_i < rem * (size + 1) {
        (local_i / (size + 1) + 1) as i64
    } else {
        (rem + (local_i - rem * (size + 1)) / size + 1) as i64
    }
}

/// Resolve a window frame to an end-exclusive `[start, end)` within the partition.
///
/// When `frame` is `None`, the full partition is used.
/// `RANGE` treats `CURRENT ROW` as the full peer group of equal `ORDER BY` keys.
fn window_frame_range(
    i: usize,
    part_start: usize,
    part_end: usize,
    frame: Option<&crate::sql::WindowRowsFrame>,
    order_by: &[crate::sql::SortExpr],
    rows: &[Record],
    ctx: &ExecutionContext,
) -> Result<(usize, usize)> {
    let Some(frame) = frame else {
        return Ok((part_start, part_end));
    };
    match frame.units {
        crate::sql::FrameUnits::Rows => {
            let start = resolve_rows_bound(frame.start, i, part_start, part_end, false);
            let end = resolve_rows_bound(frame.end, i, part_start, part_end, true);
            Ok(if start >= end {
                (start.min(part_end), start.min(part_end))
            } else {
                (start, end)
            })
        }
        crate::sql::FrameUnits::Range => {
            let needs_offset = matches!(
                frame.start,
                crate::sql::FrameBound::Preceding(_) | crate::sql::FrameBound::Following(_)
            ) || matches!(
                frame.end,
                crate::sql::FrameBound::Preceding(_) | crate::sql::FrameBound::Following(_)
            );
            if needs_offset {
                return range_value_offset_frame(
                    i, part_start, part_end, frame, order_by, rows, ctx,
                );
            }
            let (peer_start, peer_end) =
                peer_group_range(i, part_start, part_end, order_by, rows, ctx)?;
            let start = match frame.start {
                crate::sql::FrameBound::UnboundedPreceding => part_start,
                crate::sql::FrameBound::CurrentRow => peer_start,
                crate::sql::FrameBound::UnboundedFollowing => peer_end,
                crate::sql::FrameBound::Preceding(_) | crate::sql::FrameBound::Following(_) => {
                    unreachable!("handled above")
                }
            };
            let end = match frame.end {
                crate::sql::FrameBound::UnboundedFollowing => part_end,
                crate::sql::FrameBound::CurrentRow => peer_end,
                crate::sql::FrameBound::UnboundedPreceding => peer_start,
                crate::sql::FrameBound::Preceding(_) | crate::sql::FrameBound::Following(_) => {
                    unreachable!("handled above")
                }
            };
            Ok(if start >= end {
                (start.min(part_end), start.min(part_end))
            } else {
                (start, end)
            })
        }
        crate::sql::FrameUnits::Groups => {
            groups_frame_range(i, part_start, part_end, frame, order_by, rows, ctx)
        }
    }
}

fn frame_excludes_row(
    j: usize,
    cur: usize,
    exclude: crate::sql::FrameExclude,
    peer_start: usize,
    peer_end: usize,
) -> bool {
    use crate::sql::FrameExclude::*;
    match exclude {
        NoOthers => false,
        CurrentRow => j == cur,
        Group => j >= peer_start && j < peer_end,
        Ties => j != cur && j >= peer_start && j < peer_end,
    }
}

/// Frame row indices after applying `EXCLUDE`.
fn window_frame_members(
    i: usize,
    part_start: usize,
    part_end: usize,
    frame: Option<&crate::sql::WindowRowsFrame>,
    order_by: &[crate::sql::SortExpr],
    rows: &[Record],
    ctx: &ExecutionContext,
) -> Result<Vec<usize>> {
    let (fs, fe) = window_frame_range(i, part_start, part_end, frame, order_by, rows, ctx)?;
    let exclude = frame
        .map(|f| f.exclude)
        .unwrap_or(crate::sql::FrameExclude::NoOthers);
    if exclude == crate::sql::FrameExclude::NoOthers {
        return Ok((fs..fe).collect());
    }
    let (peer_s, peer_e) = peer_group_range(i, part_start, part_end, order_by, rows, ctx)?;
    Ok((fs..fe)
        .filter(|&j| !frame_excludes_row(j, i, exclude, peer_s, peer_e))
        .collect())
}

/// `GROUPS n PRECEDING/FOLLOWING` — offsets count peer groups, not rows/values.
fn groups_frame_range(
    i: usize,
    part_start: usize,
    part_end: usize,
    frame: &crate::sql::WindowRowsFrame,
    order_by: &[crate::sql::SortExpr],
    rows: &[Record],
    ctx: &ExecutionContext,
) -> Result<(usize, usize)> {
    if order_by.is_empty() {
        return Err(TakyonicError::Sql(
            "GROUPS frames require ORDER BY".into(),
        ));
    }
    let groups = build_peer_groups(part_start, part_end, order_by, rows, ctx)?;
    if groups.is_empty() {
        return Ok((part_start, part_end));
    }
    let cur_gi = groups
        .iter()
        .position(|&(s, e)| i >= s && i < e)
        .ok_or_else(|| TakyonicError::Sql("GROUPS: row not in any peer group".into()))?;
    let n_groups = groups.len();

    let start_gi = match frame.start {
        crate::sql::FrameBound::UnboundedPreceding => 0,
        crate::sql::FrameBound::CurrentRow => cur_gi,
        crate::sql::FrameBound::Preceding(n) => cur_gi.saturating_sub(n as usize),
        crate::sql::FrameBound::Following(n) => (cur_gi + n as usize).min(n_groups),
        crate::sql::FrameBound::UnboundedFollowing => n_groups,
    };
    let end_gi = match frame.end {
        crate::sql::FrameBound::UnboundedFollowing => n_groups,
        crate::sql::FrameBound::CurrentRow => cur_gi + 1,
        crate::sql::FrameBound::Following(n) => (cur_gi + n as usize + 1).min(n_groups),
        crate::sql::FrameBound::Preceding(n) => {
            if (n as usize) > cur_gi {
                0
            } else {
                cur_gi - n as usize + 1
            }
        }
        crate::sql::FrameBound::UnboundedPreceding => 0,
    };
    if start_gi >= end_gi || start_gi >= n_groups {
        return Ok((i, i));
    }
    let start = groups[start_gi].0;
    let end = groups[end_gi - 1].1;
    Ok((start, end))
}

fn build_peer_groups(
    part_start: usize,
    part_end: usize,
    order_by: &[crate::sql::SortExpr],
    rows: &[Record],
    ctx: &ExecutionContext,
) -> Result<Vec<(usize, usize)>> {
    let mut groups = Vec::new();
    let mut s = part_start;
    while s < part_end {
        let (_, e) = peer_group_range(s, part_start, part_end, order_by, rows, ctx)?;
        if e <= s {
            break;
        }
        groups.push((s, e));
        s = e;
    }
    Ok(groups)
}

/// `RANGE n PRECEDING/FOLLOWING` with a single numeric `ORDER BY` key.
fn range_value_offset_frame(
    i: usize,
    part_start: usize,
    part_end: usize,
    frame: &crate::sql::WindowRowsFrame,
    order_by: &[crate::sql::SortExpr],
    rows: &[Record],
    ctx: &ExecutionContext,
) -> Result<(usize, usize)> {
    if order_by.len() != 1 {
        return Err(TakyonicError::Sql(
            "RANGE value offsets require exactly one ORDER BY expression".into(),
        ));
    }
    let sort = &order_by[0];
    let cur_v = evaluate(&sort.expr, &rows[i], ctx)?;
    let cur = value_as_i64(&cur_v)?;
    let asc = sort.asc;

    let start_val = range_bound_value(cur, frame.start, asc, /*toward_start*/ true)?;
    let end_val = range_bound_value(cur, frame.end, asc, /*toward_start*/ false)?;

    // Collect contiguous rows whose order key lies between the value bounds (inclusive),
    // expanding to full peer groups at the edges.
    let mut first = None;
    let mut last = None;
    for j in part_start..part_end {
        let v = value_as_i64(&evaluate(&sort.expr, &rows[j], ctx)?)?;
        let in_range = if asc {
            match (start_val, end_val) {
                (None, None) => true,
                (Some(lo), None) => v >= lo,
                (None, Some(hi)) => v <= hi,
                (Some(lo), Some(hi)) => v >= lo && v <= hi,
            }
        } else {
            match (start_val, end_val) {
                (None, None) => true,
                (Some(hi), None) => v <= hi, // toward start of DESC = larger values
                (None, Some(lo)) => v >= lo,
                (Some(hi), Some(lo)) => v <= hi && v >= lo,
            }
        };
        if in_range {
            if first.is_none() {
                first = Some(j);
            }
            last = Some(j);
        } else if first.is_some() {
            // Contiguous frame in sorted partition — stop after leaving the range.
            break;
        }
    }
    match (first, last) {
        (Some(s), Some(e)) => {
            // Expand end to full peer group of last included value.
            let (_, peer_end) = peer_group_range(e, part_start, part_end, order_by, rows, ctx)?;
            let (peer_start, _) = peer_group_range(s, part_start, part_end, order_by, rows, ctx)?;
            Ok((peer_start, peer_end))
        }
        _ => Ok((i, i)), // empty
    }
}

/// Map a RANGE bound to an inclusive numeric threshold (`None` = unbounded that side).
fn range_bound_value(
    cur: i64,
    bound: crate::sql::FrameBound,
    asc: bool,
    toward_start: bool,
) -> Result<Option<i64>> {
    use crate::sql::FrameBound;
    Ok(match bound {
        FrameBound::UnboundedPreceding if toward_start => None,
        FrameBound::UnboundedFollowing if !toward_start => None,
        FrameBound::UnboundedPreceding => Some(cur), // unusual as end
        FrameBound::UnboundedFollowing => Some(cur), // unusual as start
        FrameBound::CurrentRow => Some(cur),
        FrameBound::Preceding(n) => {
            let n = n as i64;
            Some(if asc {
                cur.saturating_sub(n)
            } else {
                cur.saturating_add(n)
            })
        }
        FrameBound::Following(n) => {
            let n = n as i64;
            Some(if asc {
                cur.saturating_add(n)
            } else {
                cur.saturating_sub(n)
            })
        }
    })
}

/// Inclusive peer-group start and exclusive end for row `i` within the partition.
fn peer_group_range(
    i: usize,
    part_start: usize,
    part_end: usize,
    order_by: &[crate::sql::SortExpr],
    rows: &[Record],
    ctx: &ExecutionContext,
) -> Result<(usize, usize)> {
    if order_by.is_empty() {
        return Ok((part_start, part_end));
    }
    let same = |a: usize, b: usize| -> Result<bool> {
        let mut ka = Vec::with_capacity(order_by.len());
        let mut kb = Vec::with_capacity(order_by.len());
        for s in order_by {
            ka.push(evaluate(&s.expr, &rows[a], ctx)?);
            kb.push(evaluate(&s.expr, &rows[b], ctx)?);
        }
        Ok(cmp_sort_keys(&ka, &kb, order_by) == Ordering::Equal)
    };
    let mut peer_start = i;
    while peer_start > part_start && same(peer_start - 1, i)? {
        peer_start -= 1;
    }
    let mut peer_end = i + 1;
    while peer_end < part_end && same(i, peer_end)? {
        peer_end += 1;
    }
    Ok((peer_start, peer_end))
}

fn resolve_rows_bound(
    bound: crate::sql::FrameBound,
    i: usize,
    part_start: usize,
    part_end: usize,
    as_end_exclusive: bool,
) -> usize {
    use crate::sql::FrameBound;
    if part_start >= part_end {
        return part_start;
    }
    let inclusive = match bound {
        FrameBound::UnboundedPreceding => part_start,
        FrameBound::UnboundedFollowing => part_end - 1,
        FrameBound::CurrentRow => i,
        FrameBound::Preceding(n) => i.saturating_sub(n as usize).max(part_start),
        FrameBound::Following(n) => (i + n as usize).min(part_end - 1),
    };
    if as_end_exclusive {
        match bound {
            FrameBound::UnboundedFollowing => part_end,
            _ => (inclusive + 1).clamp(part_start, part_end),
        }
    } else {
        match bound {
            FrameBound::UnboundedFollowing => part_end,
            _ => inclusive.clamp(part_start, part_end),
        }
    }
}

impl Executor for WindowExec {
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

/// Concatenate two inputs; optionally hash-deduplicate (`UNION` vs `UNION ALL`).
struct UnionExec {
    left: Option<Box<dyn Executor>>,
    right: Option<Box<dyn Executor>>,
    op: crate::sql::SetOpKind,
    all: bool,
    /// After drain: rows to emit.
    pending: Option<Vec<Record>>,
    emit_idx: usize,
}

impl UnionExec {
    fn new(
        left: Box<dyn Executor>,
        right: Box<dyn Executor>,
        op: crate::sql::SetOpKind,
        all: bool,
    ) -> Self {
        Self {
            left: Some(left),
            right: Some(right),
            op,
            all,
            pending: None,
            emit_idx: 0,
        }
    }

    fn materialize(&mut self) -> Result<()> {
        let mut left = self
            .left
            .take()
            .ok_or_else(|| TakyonicError::Sql("set-op left already drained".into()))?;
        let mut right = self
            .right
            .take()
            .ok_or_else(|| TakyonicError::Sql("set-op right already drained".into()))?;
        let left_rows = drain_executor(left.as_mut())?;
        let right_rows = drain_executor(right.as_mut())?;
        let rows = apply_set_op(left_rows, right_rows, self.op, self.all);
        self.pending = Some(rows);
        self.emit_idx = 0;
        Ok(())
    }
}

impl Executor for UnionExec {
    fn next_row(&mut self) -> Result<Option<Record>> {
        if self.pending.is_none() {
            self.materialize()?;
        }
        let rows = self.pending.as_ref().expect("materialized");
        if self.emit_idx >= rows.len() {
            return Ok(None);
        }
        let row = rows[self.emit_idx].clone();
        self.emit_idx += 1;
        Ok(Some(row))
    }
}

fn apply_set_op(
    left: Vec<Record>,
    right: Vec<Record>,
    op: crate::sql::SetOpKind,
    all: bool,
) -> Vec<Record> {
    use crate::sql::SetOpKind;
    match (op, all) {
        (SetOpKind::Union, true) => {
            let mut rows = left;
            rows.extend(right);
            rows
        }
        (SetOpKind::Union, false) => {
            let mut rows = left;
            rows.extend(right);
            dedupe_records(rows)
        }
        (SetOpKind::Intersect, false) => {
            let right_set: HashSet<Record> = right.into_iter().collect();
            dedupe_records(
                left.into_iter()
                    .filter(|r| right_set.contains(r))
                    .collect(),
            )
        }
        (SetOpKind::Intersect, true) => {
            let mut right_counts: HashMap<Record, usize> = HashMap::new();
            for r in right {
                *right_counts.entry(r).or_default() += 1;
            }
            let mut out = Vec::new();
            for r in left {
                if let Some(c) = right_counts.get_mut(&r) {
                    if *c > 0 {
                        *c -= 1;
                        out.push(r);
                    }
                }
            }
            out
        }
        (SetOpKind::Except, false) => {
            let right_set: HashSet<Record> = right.into_iter().collect();
            dedupe_records(
                left.into_iter()
                    .filter(|r| !right_set.contains(r))
                    .collect(),
            )
        }
        (SetOpKind::Except, true) => {
            let mut right_counts: HashMap<Record, usize> = HashMap::new();
            for r in right {
                *right_counts.entry(r).or_default() += 1;
            }
            let mut out = Vec::new();
            for r in left {
                match right_counts.get_mut(&r) {
                    Some(c) if *c > 0 => *c -= 1,
                    _ => out.push(r),
                }
            }
            out
        }
    }
}

/// Hash-deduplicate rows (`SELECT DISTINCT`).
struct DistinctExec {
    input: Option<Box<dyn Executor>>,
    pending: Option<Vec<Record>>,
    emit_idx: usize,
}

impl DistinctExec {
    fn new(input: Box<dyn Executor>) -> Self {
        Self {
            input: Some(input),
            pending: None,
            emit_idx: 0,
        }
    }
}

impl Executor for DistinctExec {
    fn next_row(&mut self) -> Result<Option<Record>> {
        if self.pending.is_none() {
            let mut child = self
                .input
                .take()
                .ok_or_else(|| TakyonicError::Sql("distinct input already drained".into()))?;
            let rows = dedupe_records(drain_executor(child.as_mut())?);
            self.pending = Some(rows);
            self.emit_idx = 0;
        }
        let rows = self.pending.as_ref().expect("materialized");
        if self.emit_idx >= rows.len() {
            return Ok(None);
        }
        let row = rows[self.emit_idx].clone();
        self.emit_idx += 1;
        Ok(Some(row))
    }
}

fn dedupe_records(rows: Vec<Record>) -> Vec<Record> {
    let mut seen = std::collections::HashSet::new();
    let mut out = Vec::new();
    for row in rows {
        if seen.insert(row.clone()) {
            out.push(row);
        }
    }
    out
}

/// `SELECT DISTINCT ON (exprs)` — keep the first row for each distinct ON-key.
struct DistinctOnExec {
    input: Box<dyn Executor>,
    exprs: Vec<Expression>,
    ctx: ExecutionContext,
    seen: std::collections::HashSet<Vec<Value>>,
}

impl DistinctOnExec {
    fn new(input: Box<dyn Executor>, exprs: Vec<Expression>, ctx: ExecutionContext) -> Self {
        Self {
            input,
            exprs,
            ctx,
            seen: std::collections::HashSet::new(),
        }
    }

    fn on_key(&self, row: &Record) -> Result<Vec<Value>> {
        let mut keys = Vec::with_capacity(self.exprs.len());
        for e in &self.exprs {
            keys.push(evaluate(e, row, &self.ctx)?);
        }
        Ok(keys)
    }
}

impl Executor for DistinctOnExec {
    fn next_row(&mut self) -> Result<Option<Record>> {
        while let Some(row) = self.input.next_row()? {
            let key = self.on_key(&row)?;
            if self.seen.insert(key) {
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
            Expression::AggregateFunction { name, args, .. } => {
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
    join_type: JoinType,
    ctx: ExecutionContext,
    current_left: Option<Record>,
    right_idx: usize,
    /// True once the current outer row produced ≥1 match (LEFT/RIGHT outer tracking).
    left_matched: bool,
    /// Column names from the right side for LEFT/FULL null-padding.
    right_null_template: Record,
    /// Column names from the left side for RIGHT/FULL null-padding.
    left_null_template: Record,
    /// When true, `left` streams the SQL-right side and `right_rows` is SQL-left.
    right_outer: bool,
    /// FULL OUTER: which materialized right rows already matched a left row.
    right_matched: Vec<bool>,
    /// FULL OUTER: emitting unmatched right rows after left is exhausted.
    full_dangling: bool,
    full_dangling_idx: usize,
}

impl NestedLoopJoin {
    /// Build a nested-loop join over a left pull-iterator and a materialized right.
    pub fn new(
        left: Box<dyn Executor>,
        right_rows: Vec<Record>,
        condition: Expression,
        join_type: JoinType,
        ctx: ExecutionContext,
    ) -> Self {
        let right_null_template = null_template_from_rows(&right_rows);
        let right_matched = vec![false; right_rows.len()];
        Self {
            left,
            right_rows,
            condition,
            join_type,
            ctx,
            current_left: None,
            right_idx: 0,
            left_matched: false,
            right_null_template,
            left_null_template: Record::new(),
            right_outer: false,
            right_matched,
            full_dangling: false,
            full_dangling_idx: 0,
        }
    }

    /// RIGHT OUTER: stream SQL-right as outer; `left_rows` is the SQL-left side.
    pub fn new_right_outer(
        right_stream: Box<dyn Executor>,
        left_rows: Vec<Record>,
        condition: Expression,
        ctx: ExecutionContext,
    ) -> Self {
        let left_null_template = null_template_from_rows(&left_rows);
        Self {
            left: right_stream,
            right_rows: left_rows,
            condition,
            join_type: JoinType::Right,
            ctx,
            current_left: None,
            right_idx: 0,
            left_matched: false,
            right_null_template: Record::new(),
            left_null_template,
            right_outer: true,
            right_matched: Vec::new(),
            full_dangling: false,
            full_dangling_idx: 0,
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
            JoinType::Inner,
            ExecutionContext::new(),
        )
    }

    /// Like [`Self::from_rows`] with an explicit join kind (tests).
    pub fn from_rows_with_type(
        left_rows: Vec<Record>,
        right_rows: Vec<Record>,
        condition: Expression,
        join_type: JoinType,
    ) -> Self {
        match join_type {
            JoinType::Right => Self::new_right_outer(
                Box::new(ValuesExec {
                    rows: right_rows,
                    idx: 0,
                }),
                left_rows,
                condition,
                ExecutionContext::new(),
            ),
            other => Self::new(
                Box::new(ValuesExec {
                    rows: left_rows,
                    idx: 0,
                }),
                right_rows,
                condition,
                other,
                ExecutionContext::new(),
            ),
        }
    }
}

impl Executor for NestedLoopJoin {
    fn next_row(&mut self) -> Result<Option<Record>> {
        if self.full_dangling {
            while self.full_dangling_idx < self.right_rows.len() {
                let i = self.full_dangling_idx;
                self.full_dangling_idx += 1;
                if !self.right_matched[i] {
                    return Ok(Some(combine_rows(
                        &self.left_null_template,
                        &self.right_rows[i],
                    )));
                }
            }
            return Ok(None);
        }

        loop {
            if self.current_left.is_none() {
                self.current_left = self.left.next_row()?;
                self.right_idx = 0;
                self.left_matched = false;
                if self.current_left.is_none() {
                    if self.join_type == JoinType::Full {
                        // Capture left null-pad from any left row we saw, or empty.
                        if self.left_null_template.fields.is_empty() {
                            // No left rows: still emit all rights with empty left pad.
                        }
                        self.full_dangling = true;
                        self.full_dangling_idx = 0;
                        return self.next_row();
                    }
                    return Ok(None);
                }
                // Seed left null template for FULL from the first left row's keys.
                if self.join_type == JoinType::Full && self.left_null_template.fields.is_empty() {
                    if let Some(row) = self.current_left.as_ref() {
                        self.left_null_template =
                            null_template_from_keys(row.fields.keys());
                    }
                }
            }

            let outer_row = self
                .current_left
                .as_ref()
                .expect("current_left set above");

            while self.right_idx < self.right_rows.len() {
                let ri = self.right_idx;
                let inner_row = &self.right_rows[ri];
                self.right_idx += 1;
                let combined = if self.right_outer {
                    combine_rows(inner_row, outer_row)
                } else {
                    combine_rows(outer_row, inner_row)
                };
                if evaluate_bool(&self.condition, &combined, &self.ctx)? {
                    self.left_matched = true;
                    if self.join_type == JoinType::Full {
                        self.right_matched[ri] = true;
                    }
                    return Ok(Some(combined));
                }
            }

            // Exhausted inner side for this outer row.
            if !self.left_matched {
                let outer = self.current_left.take().expect("current_left set");
                if self.join_type == JoinType::Left || self.join_type == JoinType::Full {
                    return Ok(Some(combine_rows(&outer, &self.right_null_template)));
                }
                if self.join_type == JoinType::Right {
                    return Ok(Some(combine_rows(&self.left_null_template, &outer)));
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
    /// FULL OUTER: emit unmatched build (right) rows after probe completes.
    FullDangling,
}

/// Hash equi-join / semi-join / anti-join / left / right / full outer join.
///
/// * [`JoinType::Inner`] — build left, probe right, emit combined rows.
/// * [`JoinType::Semi`] / [`JoinType::Anti`] — build right (subquery keys), probe
///   left, emit left rows that match / do not match (SubqueryUnnestingRule).
/// * [`JoinType::Left`] — build right, probe left, emit left+right matches; unmatched
///   left rows get null-padded right columns.
/// * [`JoinType::Right`] — build left, probe right, emit left+right matches; unmatched
///   right rows get null-padded left columns.
/// * [`JoinType::Full`] — like Left, then emit unmatched right rows with null left.
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
    /// Inner / Left / Right join: build-side hash table.
    hash_table: HashMap<Value, Vec<Record>>,
    /// FULL OUTER: build (right) rows with match flags.
    full_table: HashMap<Value, Vec<(Record, bool)>>,
    /// Semi/Anti: build-side key set.
    hash_set: HashSet<Value>,
    /// Current probe row (Inner / Left / Right / Full).
    current_probe: Option<Record>,
    /// Matching build rows for `current_probe`.
    matches: Vec<Record>,
    /// FULL: indices into `full_table` buckets for marking matches.
    full_match_keys: Vec<(Value, usize)>,
    /// Index into `matches`.
    match_idx: usize,
    /// Right-side null pad for unmatched LEFT/FULL probe rows.
    right_null_template: Record,
    /// Left-side null pad for unmatched RIGHT/FULL build rows.
    left_null_template: Record,
    /// FULL dangling iteration state.
    full_dangling_keys: Vec<Value>,
    full_dangling_key_idx: usize,
    full_dangling_row_idx: usize,
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
            full_table: HashMap::new(),
            hash_set: HashSet::new(),
            current_probe: None,
            matches: Vec::new(),
            full_match_keys: Vec::new(),
            match_idx: 0,
            right_null_template: Record::new(),
            left_null_template: Record::new(),
            full_dangling_keys: Vec::new(),
            full_dangling_key_idx: 0,
            full_dangling_row_idx: 0,
        }
    }

    fn build_side(&mut self) -> Result<()> {
        let mut build = self
            .build
            .take()
            .ok_or_else(|| TakyonicError::Sql("hash join build already completed".into()))?;
        let mut pad_keys = std::collections::BTreeSet::new();
        while let Some(row) = build.next_row()? {
            if matches!(
                self.join_type,
                JoinType::Left | JoinType::Right | JoinType::Full
            ) {
                for k in row.fields.keys() {
                    pad_keys.insert(k.clone());
                }
            }
            let key = evaluate(&self.build_key, &row, &self.ctx)?;
            if matches!(key, Value::Null) {
                // FULL: null-keyed right rows never match — keep for dangling emit.
                if self.join_type == JoinType::Full {
                    self.full_table
                        .entry(Value::Null)
                        .or_default()
                        .push((row, false));
                }
                continue;
            }
            match self.join_type {
                JoinType::Semi | JoinType::Anti => {
                    self.hash_set.insert(key);
                }
                JoinType::Full => {
                    self.full_table.entry(key).or_default().push((row, false));
                }
                _ => {
                    self.hash_table.entry(key).or_default().push(row);
                }
            }
        }
        match self.join_type {
            JoinType::Left | JoinType::Full => {
                self.right_null_template = null_template_from_keys(pad_keys.iter());
            }
            JoinType::Right => {
                self.left_null_template = null_template_from_keys(pad_keys.iter());
            }
            _ => {}
        }
        Ok(())
    }

    fn next_full_dangling(&mut self) -> Option<Record> {
        loop {
            while self.full_dangling_key_idx < self.full_dangling_keys.len() {
                let key = &self.full_dangling_keys[self.full_dangling_key_idx];
                if let Some(rows) = self.full_table.get(key) {
                    while self.full_dangling_row_idx < rows.len() {
                        let i = self.full_dangling_row_idx;
                        self.full_dangling_row_idx += 1;
                        if !rows[i].1 {
                            return Some(combine_rows(
                                &self.left_null_template,
                                &rows[i].0,
                            ));
                        }
                    }
                }
                self.full_dangling_key_idx += 1;
                self.full_dangling_row_idx = 0;
            }
            return None;
        }
    }
}

impl Executor for HashJoinExec {
    fn next_row(&mut self) -> Result<Option<Record>> {
        if self.phase == HashJoinPhase::Build {
            self.build_side()?;
            self.phase = HashJoinPhase::Probe;
        }
        if self.phase == HashJoinPhase::FullDangling {
            return Ok(self.next_full_dangling());
        }

        match self.join_type {
            JoinType::Semi | JoinType::Anti => {
                while let Some(row) = self.probe.next_row()? {
                    let key = evaluate(&self.probe_key, &row, &self.ctx)?;
                    // NULL probe key → UNKNOWN for IN/NOT IN; exclude from Semi and Anti.
                    if key.is_null() {
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
            JoinType::Left => loop {
                if self.match_idx < self.matches.len() {
                    let build_row = &self.matches[self.match_idx];
                    self.match_idx += 1;
                    let probe_row = self
                        .current_probe
                        .as_ref()
                        .expect("current_probe set when matches non-empty");
                    return Ok(Some(combine_rows(probe_row, build_row)));
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
                if self.matches.is_empty() {
                    return Ok(Some(combine_rows(
                        &probe_row,
                        &self.right_null_template,
                    )));
                }
                self.current_probe = Some(probe_row);
            },
            JoinType::Full => loop {
                if self.match_idx < self.matches.len() {
                    let build_row = self.matches[self.match_idx].clone();
                    let (key, bi) = self.full_match_keys[self.match_idx].clone();
                    self.match_idx += 1;
                    if let Some(bucket) = self.full_table.get_mut(&key) {
                        if bi < bucket.len() {
                            bucket[bi].1 = true;
                        }
                    }
                    let probe_row = self
                        .current_probe
                        .as_ref()
                        .expect("current_probe set when matches non-empty");
                    return Ok(Some(combine_rows(probe_row, &build_row)));
                }

                let Some(probe_row) = self.probe.next_row()? else {
                    if self.left_null_template.fields.is_empty() {
                        // No left rows probed — left pad stays empty.
                    }
                    self.full_dangling_keys = self.full_table.keys().cloned().collect();
                    self.full_dangling_key_idx = 0;
                    self.full_dangling_row_idx = 0;
                    self.phase = HashJoinPhase::FullDangling;
                    return Ok(self.next_full_dangling());
                };
                if self.left_null_template.fields.is_empty() {
                    self.left_null_template =
                        null_template_from_keys(probe_row.fields.keys());
                }
                let key = evaluate(&self.probe_key, &probe_row, &self.ctx)?;
                self.matches.clear();
                self.full_match_keys.clear();
                if !matches!(key, Value::Null) {
                    if let Some(bucket) = self.full_table.get(&key) {
                        for (i, (row, _)) in bucket.iter().enumerate() {
                            self.matches.push(row.clone());
                            self.full_match_keys.push((key.clone(), i));
                        }
                    }
                }
                self.match_idx = 0;
                if self.matches.is_empty() {
                    return Ok(Some(combine_rows(
                        &probe_row,
                        &self.right_null_template,
                    )));
                }
                self.current_probe = Some(probe_row);
            },
            JoinType::Right => loop {
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
                if self.matches.is_empty() {
                    return Ok(Some(combine_rows(
                        &self.left_null_template,
                        &probe_row,
                    )));
                }
                self.current_probe = Some(probe_row);
            },
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

/// Sort-merge equi-join over two inputs already sorted ascending on join keys.
pub struct MergeJoinExec {
    left: Box<dyn Executor>,
    right: Box<dyn Executor>,
    left_key: Expression,
    right_key: Expression,
    ctx: ExecutionContext,
    left_cur: Option<Record>,
    right_cur: Option<Record>,
    left_run: Vec<Record>,
    right_run: Vec<Record>,
    li: usize,
    ri: usize,
    primed: bool,
}

impl MergeJoinExec {
    /// Construct a merge join over sorted children.
    pub fn new(
        left: Box<dyn Executor>,
        right: Box<dyn Executor>,
        left_key: Expression,
        right_key: Expression,
        ctx: ExecutionContext,
    ) -> Self {
        Self {
            left,
            right,
            left_key,
            right_key,
            ctx,
            left_cur: None,
            right_cur: None,
            left_run: Vec::new(),
            right_run: Vec::new(),
            li: 0,
            ri: 0,
            primed: false,
        }
    }

    /// Convenience: wrap two in-memory sorted tables.
    pub fn from_sorted_rows(
        left_rows: Vec<Record>,
        right_rows: Vec<Record>,
        left_key: Expression,
        right_key: Expression,
    ) -> Self {
        Self::new(
            Box::new(ValuesExec {
                rows: left_rows,
                idx: 0,
            }),
            Box::new(ValuesExec {
                rows: right_rows,
                idx: 0,
            }),
            left_key,
            right_key,
            ExecutionContext::new(),
        )
    }

    fn key_of(&self, side: bool, row: &Record) -> Result<Value> {
        let expr = if side {
            &self.left_key
        } else {
            &self.right_key
        };
        evaluate(expr, row, &self.ctx)
    }

    fn load_left_run(&mut self) -> Result<()> {
        self.left_run.clear();
        let Some(first) = self.left_cur.take() else {
            return Ok(());
        };
        let run_key = self.key_of(true, &first)?;
        self.left_run.push(first);
        if matches!(run_key, Value::Null) {
            self.left_cur = self.left.next_row()?;
            return Ok(());
        }
        loop {
            let Some(next) = self.left.next_row()? else {
                self.left_cur = None;
                break;
            };
            let k = self.key_of(true, &next)?;
            if k == run_key {
                self.left_run.push(next);
            } else {
                self.left_cur = Some(next);
                break;
            }
        }
        Ok(())
    }

    fn load_right_run(&mut self) -> Result<()> {
        self.right_run.clear();
        let Some(first) = self.right_cur.take() else {
            return Ok(());
        };
        let run_key = self.key_of(false, &first)?;
        self.right_run.push(first);
        if matches!(run_key, Value::Null) {
            self.right_cur = self.right.next_row()?;
            return Ok(());
        }
        loop {
            let Some(next) = self.right.next_row()? else {
                self.right_cur = None;
                break;
            };
            let k = self.key_of(false, &next)?;
            if k == run_key {
                self.right_run.push(next);
            } else {
                self.right_cur = Some(next);
                break;
            }
        }
        Ok(())
    }
}

impl Executor for MergeJoinExec {
    fn next_row(&mut self) -> Result<Option<Record>> {
        if !self.primed {
            self.left_cur = self.left.next_row()?;
            self.right_cur = self.right.next_row()?;
            self.primed = true;
        }

        loop {
            // Emit remaining cross-product of current equal-key runs.
            if self.li < self.left_run.len() && self.ri < self.right_run.len() {
                let combined = combine_rows(&self.left_run[self.li], &self.right_run[self.ri]);
                self.ri += 1;
                if self.ri >= self.right_run.len() {
                    self.ri = 0;
                    self.li += 1;
                }
                return Ok(Some(combined));
            }
            self.left_run.clear();
            self.right_run.clear();
            self.li = 0;
            self.ri = 0;

            let (Some(lref), Some(rref)) = (&self.left_cur, &self.right_cur) else {
                return Ok(None);
            };
            let lk = self.key_of(true, lref)?;
            let rk = self.key_of(false, rref)?;
            if matches!(lk, Value::Null) {
                self.left_cur = self.left.next_row()?;
                continue;
            }
            if matches!(rk, Value::Null) {
                self.right_cur = self.right.next_row()?;
                continue;
            }
            match value_ord(&lk, &rk) {
                Ordering::Less => {
                    self.left_cur = self.left.next_row()?;
                }
                Ordering::Greater => {
                    self.right_cur = self.right.next_row()?;
                }
                Ordering::Equal => {
                    self.load_left_run()?;
                    self.load_right_run()?;
                    // loop will emit cross-product
                }
            }
        }
    }
}

/// Stateful aggregator updated row-by-row inside [`AggregateExec`].
pub trait Accumulator: Send {
    /// Fold one row's evaluated argument values into internal state.
    fn update(&mut self, values: &[Value]) -> Result<()>;
    /// Like [`update`](Self::update), with optional `ORDER BY` keys for ordered aggs.
    fn update_ordered(&mut self, values: &[Value], _order_keys: &[Value]) -> Result<()> {
        self.update(values)
    }
    /// Finalize the aggregate value for emission.
    fn evaluate(&self) -> Result<Value>;
}

/// Wraps an accumulator so each distinct argument tuple is applied at most once.
struct DistinctAccumulator {
    inner: Box<dyn Accumulator>,
    seen: HashSet<Vec<Value>>,
}

impl Accumulator for DistinctAccumulator {
    fn update(&mut self, values: &[Value]) -> Result<()> {
        // Skip NULLs for DISTINCT (PG: NULL is not counted as a distinct value for COUNT).
        if values.iter().any(|v| v.is_null()) {
            return Ok(());
        }
        let key: Vec<Value> = values.to_vec();
        if !self.seen.insert(key) {
            return Ok(());
        }
        self.inner.update(values)
    }

    fn evaluate(&self) -> Result<Value> {
        self.inner.evaluate()
    }
}

/// Buffers inputs and feeds an inner accumulator in `ORDER BY` order on evaluate.
struct OrderedAccumulator {
    /// Aggregate name for recreating the inner accumulator.
    name: String,
    /// Whether to wrap the recreated inner with DISTINCT.
    distinct: bool,
    /// ASC flags parallel to each order key.
    order_asc: Vec<bool>,
    /// `(order_keys, arg_values)` rows.
    buffered: Vec<(Vec<Value>, Vec<Value>)>,
}

impl Accumulator for OrderedAccumulator {
    fn update(&mut self, values: &[Value]) -> Result<()> {
        self.update_ordered(values, &[])
    }

    fn update_ordered(&mut self, values: &[Value], order_keys: &[Value]) -> Result<()> {
        self.buffered
            .push((order_keys.to_vec(), values.to_vec()));
        Ok(())
    }

    fn evaluate(&self) -> Result<Value> {
        let mut rows = self.buffered.clone();
        let asc = &self.order_asc;
        rows.sort_by(|(ka, _), (kb, _)| {
            for i in 0..asc.len().max(ka.len()).max(kb.len()) {
                let a = ka.get(i).cloned().unwrap_or(Value::Null);
                let b = kb.get(i).cloned().unwrap_or(Value::Null);
                let cmp = value_ord(&a, &b);
                if cmp != Ordering::Equal {
                    return if asc.get(i).copied().unwrap_or(true) {
                        cmp
                    } else {
                        cmp.reverse()
                    };
                }
            }
            Ordering::Equal
        });
        let mut inner = new_base_accumulator(&self.name)?;
        if self.distinct {
            inner = Box::new(DistinctAccumulator {
                inner,
                seen: HashSet::new(),
            });
        }
        for (_, args) in rows {
            inner.update(&args)?;
        }
        inner.evaluate()
    }
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

/// `JSON_AGG` / `JSONB_AGG` — collect values into a JSON array.
#[derive(Debug, Default)]
pub struct JsonAggAccumulator {
    items: Vec<serde_json::Value>,
    seen: bool,
}

impl Accumulator for JsonAggAccumulator {
    fn update(&mut self, values: &[Value]) -> Result<()> {
        self.seen = true;
        let v = values.first().unwrap_or(&Value::Null);
        self.items.push(crate::sql::value_to_json(v));
        Ok(())
    }

    fn evaluate(&self) -> Result<Value> {
        if !self.seen {
            return Ok(Value::Null);
        }
        Ok(Value::String(
            serde_json::Value::Array(self.items.clone()).to_string(),
        ))
    }
}

/// `JSON_OBJECT_AGG` / `JSONB_OBJECT_AGG` — build a JSON object from key/value pairs.
#[derive(Debug, Default)]
pub struct JsonObjectAggAccumulator {
    map: serde_json::Map<String, serde_json::Value>,
    seen: bool,
}

impl Accumulator for JsonObjectAggAccumulator {
    fn update(&mut self, values: &[Value]) -> Result<()> {
        if values.len() < 2 {
            return Err(TakyonicError::Sql(
                "json_object_agg requires key and value".into(),
            ));
        }
        let key = &values[0];
        if key.is_null() {
            return Err(TakyonicError::Sql(
                "json_object_agg key must not be NULL".into(),
            ));
        }
        self.seen = true;
        self.map
            .insert(key.to_display(), crate::sql::value_to_json(&values[1]));
        Ok(())
    }

    fn evaluate(&self) -> Result<Value> {
        if !self.seen {
            return Ok(Value::Null);
        }
        Ok(Value::String(
            serde_json::Value::Object(self.map.clone()).to_string(),
        ))
    }
}

/// `STRING_AGG(expr, delimiter)` — concatenate non-NULL values with a delimiter.
#[derive(Debug, Default)]
pub struct StringAggAccumulator {
    parts: Vec<String>,
    delim: Option<String>,
    null_delim: bool,
}

impl Accumulator for StringAggAccumulator {
    fn update(&mut self, values: &[Value]) -> Result<()> {
        if values.len() < 2 {
            return Err(TakyonicError::Sql(
                "string_agg requires expression and delimiter".into(),
            ));
        }
        if values[1].is_null() {
            self.null_delim = true;
            return Ok(());
        }
        if self.delim.is_none() {
            self.delim = Some(values[1].to_display());
        }
        if values[0].is_null() {
            return Ok(());
        }
        self.parts.push(values[0].to_display());
        Ok(())
    }

    fn evaluate(&self) -> Result<Value> {
        if self.null_delim {
            return Ok(Value::Null);
        }
        if self.parts.is_empty() {
            return Ok(Value::Null);
        }
        let delim = self.delim.as_deref().unwrap_or("");
        Ok(Value::String(self.parts.join(delim)))
    }
}

/// `ARRAY_AGG(expr)` — collect values into a text array display `[a,b,…]`.
#[derive(Debug, Default)]
pub struct ArrayAggAccumulator {
    items: Vec<String>,
    seen: bool,
}

impl Accumulator for ArrayAggAccumulator {
    fn update(&mut self, values: &[Value]) -> Result<()> {
        self.seen = true;
        let v = values.first().unwrap_or(&Value::Null);
        if v.is_null() {
            self.items.push(String::new());
        } else {
            self.items.push(v.to_display());
        }
        Ok(())
    }

    fn evaluate(&self) -> Result<Value> {
        if !self.seen {
            return Ok(Value::Null);
        }
        Ok(Value::String(format!("[{}]", self.items.join(","))))
    }
}

/// `BOOL_AND` / `EVERY` — true iff every non-NULL input is true.
#[derive(Debug, Default)]
pub struct BoolAndAccumulator {
    result: Option<bool>,
}

impl Accumulator for BoolAndAccumulator {
    fn update(&mut self, values: &[Value]) -> Result<()> {
        let Some(v) = values.first() else {
            return Ok(());
        };
        if v.is_null() {
            return Ok(());
        }
        let b = value_as_bool(v)?;
        self.result = Some(self.result.unwrap_or(true) && b);
        Ok(())
    }

    fn evaluate(&self) -> Result<Value> {
        match self.result {
            Some(b) => Ok(Value::Bool(b)),
            None => Ok(Value::Null),
        }
    }
}

/// `BOOL_OR` — true if any non-NULL input is true.
#[derive(Debug, Default)]
pub struct BoolOrAccumulator {
    result: Option<bool>,
}

impl Accumulator for BoolOrAccumulator {
    fn update(&mut self, values: &[Value]) -> Result<()> {
        let Some(v) = values.first() else {
            return Ok(());
        };
        if v.is_null() {
            return Ok(());
        }
        let b = value_as_bool(v)?;
        self.result = Some(self.result.unwrap_or(false) || b);
        Ok(())
    }

    fn evaluate(&self) -> Result<Value> {
        match self.result {
            Some(b) => Ok(Value::Bool(b)),
            None => Ok(Value::Null),
        }
    }
}

/// `BIT_AND` — bitwise AND of all non-NULL integer inputs.
#[derive(Debug, Default)]
pub struct BitAndAccumulator {
    result: Option<i64>,
}

impl Accumulator for BitAndAccumulator {
    fn update(&mut self, values: &[Value]) -> Result<()> {
        let Some(v) = values.first() else {
            return Ok(());
        };
        if v.is_null() {
            return Ok(());
        }
        let n = value_as_i64(v)?;
        self.result = Some(match self.result {
            Some(cur) => cur & n,
            None => n,
        });
        Ok(())
    }

    fn evaluate(&self) -> Result<Value> {
        match self.result {
            Some(n) => Ok(Value::Int(n)),
            None => Ok(Value::Null),
        }
    }
}

/// `BIT_OR` — bitwise OR of all non-NULL integer inputs.
#[derive(Debug, Default)]
pub struct BitOrAccumulator {
    result: Option<i64>,
}

impl Accumulator for BitOrAccumulator {
    fn update(&mut self, values: &[Value]) -> Result<()> {
        let Some(v) = values.first() else {
            return Ok(());
        };
        if v.is_null() {
            return Ok(());
        }
        let n = value_as_i64(v)?;
        self.result = Some(match self.result {
            Some(cur) => cur | n,
            None => n,
        });
        Ok(())
    }

    fn evaluate(&self) -> Result<Value> {
        match self.result {
            Some(n) => Ok(Value::Int(n)),
            None => Ok(Value::Null),
        }
    }
}

/// `MODE` — most frequent non-NULL value (ties: first in ASC/DESC sort order).
#[derive(Debug)]
pub struct ModeAccumulator {
    counts: HashMap<Value, u64>,
    /// Tie-break direction (`WITHIN GROUP ORDER BY … [ASC|DESC]`).
    tie_asc: bool,
}

impl ModeAccumulator {
    fn new(tie_asc: bool) -> Self {
        Self {
            counts: HashMap::new(),
            tie_asc,
        }
    }
}

impl Accumulator for ModeAccumulator {
    fn update(&mut self, values: &[Value]) -> Result<()> {
        let Some(v) = values.first() else {
            return Ok(());
        };
        if v.is_null() {
            return Ok(());
        }
        *self.counts.entry(v.clone()).or_insert(0) += 1;
        Ok(())
    }

    fn evaluate(&self) -> Result<Value> {
        if self.counts.is_empty() {
            return Ok(Value::Null);
        }
        let max = self.counts.values().copied().max().unwrap_or(0);
        let mut best: Option<&Value> = None;
        for (val, cnt) in &self.counts {
            if *cnt != max {
                continue;
            }
            best = Some(match best {
                None => val,
                Some(cur) => {
                    let cmp = value_ord(val, cur);
                    let prefer_new = if self.tie_asc {
                        cmp == Ordering::Less
                    } else {
                        cmp == Ordering::Greater
                    };
                    if prefer_new { val } else { cur }
                }
            });
        }
        Ok(best.cloned().unwrap_or(Value::Null))
    }
}

/// `PERCENTILE_CONT` / `PERCENTILE_DISC` — ordered-set percentile over numeric samples.
#[derive(Debug)]
pub struct PercentileAccumulator {
    fraction: f64,
    continuous: bool,
    /// Sort direction from `WITHIN GROUP (ORDER BY …)`.
    tie_asc: bool,
    samples: Vec<f64>,
}

impl PercentileAccumulator {
    fn new(fraction: f64, continuous: bool, tie_asc: bool) -> Self {
        Self {
            fraction,
            continuous,
            tie_asc,
            samples: Vec::new(),
        }
    }
}

impl Accumulator for PercentileAccumulator {
    fn update(&mut self, values: &[Value]) -> Result<()> {
        // Planned as [fraction, value]; sample is the last arg.
        let Some(v) = values.last() else {
            return Ok(());
        };
        if v.is_null() {
            return Ok(());
        }
        let x = v.as_f64().ok_or_else(|| {
            TakyonicError::Sql(format!(
                "percentile input must be numeric, got {}",
                v.to_display()
            ))
        })?;
        self.samples.push(x);
        Ok(())
    }

    fn evaluate(&self) -> Result<Value> {
        if self.samples.is_empty() {
            return Ok(Value::Null);
        }
        let mut sorted = self.samples.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
        if !self.tie_asc {
            sorted.reverse();
        }
        let n = sorted.len();
        let f = self.fraction.clamp(0.0, 1.0);
        let result = if self.continuous {
            if n == 1 {
                sorted[0]
            } else {
                let rn = 1.0 + f * ((n - 1) as f64);
                let i = rn.floor() as usize; // 1-based index into sorted
                let h = rn - (i as f64);
                let lo = sorted[i.saturating_sub(1)];
                if h == 0.0 || i >= n {
                    lo
                } else {
                    let hi = sorted[i];
                    (1.0 - h) * lo + h * hi
                }
            }
        } else if f <= 0.0 {
            sorted[0]
        } else {
            let idx = ((n as f64) * f).ceil() as usize - 1;
            sorted[idx.min(n - 1)]
        };
        Ok(Value::Float(result))
    }
}

/// Population / sample variance (and stddev via `as_stddev`).
#[derive(Debug)]
pub struct VarianceAccumulator {
    count: i64,
    mean: f64,
    m2: f64,
    /// When true, divide by n (population); else n-1 (sample).
    population: bool,
    /// When true, return sqrt(variance).
    as_stddev: bool,
}

impl VarianceAccumulator {
    fn new(population: bool, as_stddev: bool) -> Self {
        Self {
            count: 0,
            mean: 0.0,
            m2: 0.0,
            population,
            as_stddev,
        }
    }
}

impl Accumulator for VarianceAccumulator {
    fn update(&mut self, values: &[Value]) -> Result<()> {
        let Some(v) = values.first() else {
            return Ok(());
        };
        if v.is_null() {
            return Ok(());
        }
        let x = v.as_f64().ok_or_else(|| {
            TakyonicError::Sql(format!(
                "cannot cast `{}` to float for variance/stddev",
                v.to_display()
            ))
        })?;
        self.count += 1;
        let delta = x - self.mean;
        self.mean += delta / self.count as f64;
        let delta2 = x - self.mean;
        self.m2 += delta * delta2;
        Ok(())
    }

    fn evaluate(&self) -> Result<Value> {
        let n = self.count;
        if n == 0 {
            return Ok(Value::Null);
        }
        if !self.population && n < 2 {
            return Ok(Value::Null);
        }
        let denom = if self.population {
            n as f64
        } else {
            (n - 1) as f64
        };
        let var = self.m2 / denom;
        if self.as_stddev {
            Ok(Value::Float(var.sqrt()))
        } else {
            Ok(Value::Float(var))
        }
    }
}

/// Kind of bivariate statistical aggregate.
#[derive(Debug, Clone, Copy)]
enum BivarStatKind {
    Corr,
    CovarPop,
    CovarSamp,
    RegrSlope,
    RegrIntercept,
    RegrR2,
    RegrCount,
    RegrAvgX,
    RegrAvgY,
    RegrSxx,
    RegrSyy,
    RegrSxy,
}

/// `CORR` / `COVAR_POP` / `COVAR_SAMP` — two-argument online stats.
#[derive(Debug)]
pub struct BivarStatAccumulator {
    kind: BivarStatKind,
    count: i64,
    mean_y: f64,
    mean_x: f64,
    c: f64,
    m2_y: f64,
    m2_x: f64,
}

impl BivarStatAccumulator {
    fn new(kind: BivarStatKind) -> Self {
        Self {
            kind,
            count: 0,
            mean_y: 0.0,
            mean_x: 0.0,
            c: 0.0,
            m2_y: 0.0,
            m2_x: 0.0,
        }
    }
}

impl Accumulator for BivarStatAccumulator {
    fn update(&mut self, values: &[Value]) -> Result<()> {
        if values.len() < 2 {
            return Err(TakyonicError::Sql(
                "corr/covar requires y and x arguments".into(),
            ));
        }
        if values[0].is_null() || values[1].is_null() {
            return Ok(());
        }
        let y = values[0].as_f64().ok_or_else(|| {
            TakyonicError::Sql(format!(
                "cannot cast `{}` to float for corr/covar",
                values[0].to_display()
            ))
        })?;
        let x = values[1].as_f64().ok_or_else(|| {
            TakyonicError::Sql(format!(
                "cannot cast `{}` to float for corr/covar",
                values[1].to_display()
            ))
        })?;
        self.count += 1;
        let n = self.count as f64;
        let dy = y - self.mean_y;
        let dx = x - self.mean_x;
        self.mean_y += dy / n;
        self.mean_x += dx / n;
        self.c += dy * (x - self.mean_x);
        self.m2_y += dy * (y - self.mean_y);
        self.m2_x += dx * (x - self.mean_x);
        Ok(())
    }

    fn evaluate(&self) -> Result<Value> {
        let n = self.count;
        if n == 0 {
            return Ok(Value::Null);
        }
        match self.kind {
            BivarStatKind::CovarPop => Ok(Value::Float(self.c / n as f64)),
            BivarStatKind::CovarSamp => {
                if n < 2 {
                    Ok(Value::Null)
                } else {
                    Ok(Value::Float(self.c / (n - 1) as f64))
                }
            }
            BivarStatKind::Corr => {
                if n < 2 || self.m2_y == 0.0 || self.m2_x == 0.0 {
                    Ok(Value::Null)
                } else {
                    Ok(Value::Float(self.c / (self.m2_y * self.m2_x).sqrt()))
                }
            }
            BivarStatKind::RegrSlope => {
                if n < 2 || self.m2_x == 0.0 {
                    Ok(Value::Null)
                } else {
                    Ok(Value::Float(self.c / self.m2_x))
                }
            }
            BivarStatKind::RegrIntercept => {
                if n < 2 || self.m2_x == 0.0 {
                    Ok(Value::Null)
                } else {
                    let slope = self.c / self.m2_x;
                    Ok(Value::Float(self.mean_y - slope * self.mean_x))
                }
            }
            BivarStatKind::RegrR2 => {
                if n < 2 || self.m2_y == 0.0 || self.m2_x == 0.0 {
                    Ok(Value::Null)
                } else {
                    let r = self.c / (self.m2_y * self.m2_x).sqrt();
                    Ok(Value::Float(r * r))
                }
            }
            BivarStatKind::RegrCount => Ok(Value::Int(n)),
            BivarStatKind::RegrAvgX => Ok(Value::Float(self.mean_x)),
            BivarStatKind::RegrAvgY => Ok(Value::Float(self.mean_y)),
            BivarStatKind::RegrSxx => {
                if n < 2 {
                    Ok(Value::Null)
                } else {
                    Ok(Value::Float(self.m2_x))
                }
            }
            BivarStatKind::RegrSyy => {
                if n < 2 {
                    Ok(Value::Null)
                } else {
                    Ok(Value::Float(self.m2_y))
                }
            }
            BivarStatKind::RegrSxy => {
                if n < 2 {
                    Ok(Value::Null)
                } else {
                    Ok(Value::Float(self.c))
                }
            }
        }
    }
}

fn value_as_bool(v: &Value) -> Result<bool> {
    match v {
        Value::Bool(b) => Ok(*b),
        Value::Int(n) => Ok(*n != 0),
        Value::Float(f) => Ok(*f != 0.0),
        Value::String(s) => match s.to_ascii_lowercase().as_str() {
            "t" | "true" | "1" | "yes" | "y" | "on" => Ok(true),
            "f" | "false" | "0" | "no" | "n" | "off" => Ok(false),
            other => Err(TakyonicError::Sql(format!(
                "cannot cast `{other}` to boolean for aggregate"
            ))),
        },
        Value::Null => Err(TakyonicError::Sql(
            "NULL cannot be coerced to boolean".into(),
        )),
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

fn advisory_lock_key_from_args(vals: &[Value]) -> Result<i64> {
    match vals.len() {
        1 => value_as_i64(&vals[0]),
        2 => {
            let k1 = i32::try_from(value_as_i64(&vals[0])?).map_err(|_| {
                TakyonicError::Sql("advisory lock key1 out of int32 range".into())
            })?;
            let k2 = i32::try_from(value_as_i64(&vals[1])?).map_err(|_| {
                TakyonicError::Sql("advisory lock key2 out of int32 range".into())
            })?;
            Ok(crate::sql::advisory_lock_key_pair(k1, k2))
        }
        _ => Err(TakyonicError::Sql(
            "advisory lock requires 1 or 2 key arguments".into(),
        )),
    }
}

fn eval_arith(left: &Value, op: crate::sql::ArithOp, right: &Value) -> Result<Value> {
    use crate::sql::ArithOp;
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }

    let left_ivl = match left {
        Value::String(s) => crate::sql::decode_interval_secs(s),
        _ => None,
    };
    let right_ivl = match right {
        Value::String(s) => crate::sql::decode_interval_secs(s),
        _ => None,
    };

    match (left_ivl, right_ivl, op) {
        (Some(a), Some(b), ArithOp::Add) => {
            return Ok(Value::String(crate::sql::encode_interval_secs(
                a.saturating_add(b),
            )));
        }
        (Some(a), Some(b), ArithOp::Sub) => {
            return Ok(Value::String(crate::sql::encode_interval_secs(
                a.saturating_sub(b),
            )));
        }
        (Some(_), Some(_), _) => {
            return Err(TakyonicError::Sql(
                "INTERVAL * / INTERVAL is not supported".into(),
            ));
        }
        (Some(a), None, ArithOp::Mul) | (None, Some(a), ArithOp::Mul) => {
            let n = if left_ivl.is_some() {
                right.as_f64()
            } else {
                left.as_f64()
            }
            .ok_or_else(|| TakyonicError::Sql("INTERVAL * requires a number".into()))?;
            return Ok(Value::String(crate::sql::encode_interval_secs(
                (a as f64 * n).round() as i64,
            )));
        }
        (Some(a), None, ArithOp::Div) => {
            let n = right
                .as_f64()
                .ok_or_else(|| TakyonicError::Sql("INTERVAL / requires a number".into()))?;
            if n == 0.0 {
                return Err(TakyonicError::Sql("division by zero".into()));
            }
            return Ok(Value::String(crate::sql::encode_interval_secs(
                (a as f64 / n).round() as i64,
            )));
        }
        (Some(delta), None, ArithOp::Add) | (None, Some(delta), ArithOp::Add) => {
            let ts = if left_ivl.is_some() {
                right.to_display()
            } else {
                left.to_display()
            };
            return Ok(Value::String(crate::sql::add_secs_to_timestamp_text(
                &ts, delta,
            )?));
        }
        (None, Some(delta), ArithOp::Sub) => {
            let ts = left.to_display();
            return Ok(Value::String(crate::sql::add_secs_to_timestamp_text(
                &ts,
                -delta,
            )?));
        }
        (Some(_), None, ArithOp::Sub) => {
            return Err(TakyonicError::Sql(
                "INTERVAL - timestamp is not supported (use timestamp - INTERVAL)".into(),
            ));
        }
        _ => {}
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

fn new_base_accumulator(name: &str) -> Result<Box<dyn Accumulator>> {
    Ok(match name {
        "COUNT" => Box::new(CountAccumulator::new(false)),
        "COUNT_STAR" => Box::new(CountAccumulator::new(true)),
        "SUM" => Box::new(SumAccumulator::default()),
        "AVG" => Box::new(AvgAccumulator::default()),
        "MIN" => Box::new(MinAccumulator::default()),
        "MAX" => Box::new(MaxAccumulator::default()),
        "JSON_AGG" | "JSONB_AGG" => Box::new(JsonAggAccumulator::default()),
        "JSON_OBJECT_AGG" | "JSONB_OBJECT_AGG" => {
            Box::new(JsonObjectAggAccumulator::default())
        }
        "STRING_AGG" => Box::new(StringAggAccumulator::default()),
        "ARRAY_AGG" => Box::new(ArrayAggAccumulator::default()),
        "BOOL_AND" | "EVERY" => Box::new(BoolAndAccumulator::default()),
        "BOOL_OR" => Box::new(BoolOrAccumulator::default()),
        "BIT_AND" => Box::new(BitAndAccumulator::default()),
        "BIT_OR" => Box::new(BitOrAccumulator::default()),
        "VAR_POP" => Box::new(VarianceAccumulator::new(true, false)),
        "VAR_SAMP" => Box::new(VarianceAccumulator::new(false, false)),
        "STDDEV_POP" => Box::new(VarianceAccumulator::new(true, true)),
        "STDDEV_SAMP" => Box::new(VarianceAccumulator::new(false, true)),
        "CORR" => Box::new(BivarStatAccumulator::new(BivarStatKind::Corr)),
        "COVAR_POP" => Box::new(BivarStatAccumulator::new(BivarStatKind::CovarPop)),
        "COVAR_SAMP" => Box::new(BivarStatAccumulator::new(BivarStatKind::CovarSamp)),
        "REGR_SLOPE" => Box::new(BivarStatAccumulator::new(BivarStatKind::RegrSlope)),
        "REGR_INTERCEPT" => Box::new(BivarStatAccumulator::new(BivarStatKind::RegrIntercept)),
        "REGR_R2" => Box::new(BivarStatAccumulator::new(BivarStatKind::RegrR2)),
        "REGR_COUNT" => Box::new(BivarStatAccumulator::new(BivarStatKind::RegrCount)),
        "REGR_AVGX" => Box::new(BivarStatAccumulator::new(BivarStatKind::RegrAvgX)),
        "REGR_AVGY" => Box::new(BivarStatAccumulator::new(BivarStatKind::RegrAvgY)),
        "REGR_SXX" => Box::new(BivarStatAccumulator::new(BivarStatKind::RegrSxx)),
        "REGR_SYY" => Box::new(BivarStatAccumulator::new(BivarStatKind::RegrSyy)),
        "REGR_SXY" => Box::new(BivarStatAccumulator::new(BivarStatKind::RegrSxy)),
        "MODE" => Box::new(ModeAccumulator::new(true)),
        "PERCENTILE_CONT" => Box::new(PercentileAccumulator::new(0.5, true, true)),
        "PERCENTILE_DISC" => Box::new(PercentileAccumulator::new(0.5, false, true)),
        other => {
            return Err(TakyonicError::Sql(format!(
                "unsupported aggregate function `{other}`"
            )));
        }
    })
}

fn new_accumulator(expr: &Expression) -> Result<Box<dyn Accumulator>> {
    let Expression::AggregateFunction {
        name,
        args,
        distinct,
        order_by,
        ..
    } = expr
    else {
        return Err(TakyonicError::Sql(format!(
            "expected AggregateFunction, got {expr:?}"
        )));
    };
    let mode_tie_asc = order_by.first().map(|s| s.asc).unwrap_or(true);
    let percentile_frac = if matches!(name.as_str(), "PERCENTILE_CONT" | "PERCENTILE_DISC") {
        Some(crate::sql::expr_as_fraction_literal(&args[0])?)
    } else {
        None
    };
    let mut inner: Box<dyn Accumulator> = match name.as_str() {
        "COUNT" => Box::new(CountAccumulator::new(args.is_empty())),
        "SUM" => Box::new(SumAccumulator::default()),
        "AVG" => Box::new(AvgAccumulator::default()),
        "MIN" => Box::new(MinAccumulator::default()),
        "MAX" => Box::new(MaxAccumulator::default()),
        "JSON_AGG" | "JSONB_AGG" => Box::new(JsonAggAccumulator::default()),
        "JSON_OBJECT_AGG" | "JSONB_OBJECT_AGG" => {
            Box::new(JsonObjectAggAccumulator::default())
        }
        "STRING_AGG" => Box::new(StringAggAccumulator::default()),
        "ARRAY_AGG" => Box::new(ArrayAggAccumulator::default()),
        "BOOL_AND" | "EVERY" => Box::new(BoolAndAccumulator::default()),
        "BOOL_OR" => Box::new(BoolOrAccumulator::default()),
        "BIT_AND" => Box::new(BitAndAccumulator::default()),
        "BIT_OR" => Box::new(BitOrAccumulator::default()),
        "VAR_POP" => Box::new(VarianceAccumulator::new(true, false)),
        "VAR_SAMP" => Box::new(VarianceAccumulator::new(false, false)),
        "STDDEV_POP" => Box::new(VarianceAccumulator::new(true, true)),
        "STDDEV_SAMP" => Box::new(VarianceAccumulator::new(false, true)),
        "CORR" => Box::new(BivarStatAccumulator::new(BivarStatKind::Corr)),
        "COVAR_POP" => Box::new(BivarStatAccumulator::new(BivarStatKind::CovarPop)),
        "COVAR_SAMP" => Box::new(BivarStatAccumulator::new(BivarStatKind::CovarSamp)),
        "REGR_SLOPE" => Box::new(BivarStatAccumulator::new(BivarStatKind::RegrSlope)),
        "REGR_INTERCEPT" => Box::new(BivarStatAccumulator::new(BivarStatKind::RegrIntercept)),
        "REGR_R2" => Box::new(BivarStatAccumulator::new(BivarStatKind::RegrR2)),
        "REGR_COUNT" => Box::new(BivarStatAccumulator::new(BivarStatKind::RegrCount)),
        "REGR_AVGX" => Box::new(BivarStatAccumulator::new(BivarStatKind::RegrAvgX)),
        "REGR_AVGY" => Box::new(BivarStatAccumulator::new(BivarStatKind::RegrAvgY)),
        "REGR_SXX" => Box::new(BivarStatAccumulator::new(BivarStatKind::RegrSxx)),
        "REGR_SYY" => Box::new(BivarStatAccumulator::new(BivarStatKind::RegrSyy)),
        "REGR_SXY" => Box::new(BivarStatAccumulator::new(BivarStatKind::RegrSxy)),
        "MODE" => Box::new(ModeAccumulator::new(mode_tie_asc)),
        "PERCENTILE_CONT" => Box::new(PercentileAccumulator::new(
            percentile_frac.unwrap(),
            true,
            mode_tie_asc,
        )),
        "PERCENTILE_DISC" => Box::new(PercentileAccumulator::new(
            percentile_frac.unwrap(),
            false,
            mode_tie_asc,
        )),
        other => {
            return Err(TakyonicError::Sql(format!(
                "unsupported aggregate function `{other}`"
            )));
        }
    };
    if *distinct && order_by.is_empty() {
        inner = Box::new(DistinctAccumulator {
            inner,
            seen: HashSet::new(),
        });
    }
    // MODE / PERCENTILE_* use order_by for WITHIN GROUP direction, not OrderedAccumulator.
    if !order_by.is_empty() && name != "MODE" && name != "PERCENTILE_CONT" && name != "PERCENTILE_DISC"
    {
        let star_count = name == "COUNT" && args.is_empty();
        Ok(Box::new(OrderedAccumulator {
            name: if star_count {
                "COUNT_STAR".into()
            } else {
                name.clone()
            },
            distinct: *distinct,
            order_asc: order_by.iter().map(|s| s.asc).collect(),
            buffered: Vec::new(),
        }))
    } else {
        Ok(inner)
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
                let (args, filter, order_by) = match expr {
                    Expression::AggregateFunction {
                        args,
                        filter,
                        order_by,
                        ..
                    } => (args, filter, order_by),
                    _ => {
                        acc.update(&[])?;
                        continue;
                    }
                };
                if let Some(pred) = filter {
                    let ok = evaluate(pred, &row, &self.ctx)?;
                    if !ok.is_truthy() {
                        continue;
                    }
                }
                let mut vals = Vec::with_capacity(args.len());
                for a in args {
                    vals.push(evaluate(a, &row, &self.ctx)?);
                }
                if order_by.is_empty() {
                    acc.update(&vals)?;
                } else {
                    let mut keys = Vec::with_capacity(order_by.len());
                    for s in order_by {
                        keys.push(evaluate(&s.expr, &row, &self.ctx)?);
                    }
                    acc.update_ordered(&vals, &keys)?;
                }
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

/// Multi-key sort comparison respecting ASC/DESC and NULLS FIRST/LAST per [`SortExpr`].
fn cmp_sort_keys(a: &[Value], b: &[Value], exprs: &[SortExpr]) -> Ordering {
    for (i, se) in exprs.iter().enumerate() {
        let av = a.get(i).unwrap_or(&Value::Null);
        let bv = b.get(i).unwrap_or(&Value::Null);
        let a_null = av.is_null();
        let b_null = bv.is_null();
        if a_null || b_null {
            if a_null && b_null {
                continue;
            }
            // NULLS FIRST/LAST is independent of ASC/DESC (PostgreSQL).
            let c = if a_null {
                if se.nulls_first {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            } else if se.nulls_first {
                Ordering::Greater
            } else {
                Ordering::Less
            };
            return c;
        }
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

/// Streaming LIMIT / OFFSET / FETCH [WITH TIES].
pub struct LimitExec {
    input: Box<dyn Executor>,
    skip: usize,
    fetch: Option<usize>,
    with_ties: bool,
    ties_order: Vec<SortExpr>,
    ctx: ExecutionContext,
    skipped: usize,
    yielded: usize,
    boundary_keys: Option<Vec<Value>>,
    done: bool,
}

impl LimitExec {
    /// Construct a limit/offset[/with ties] executor.
    pub fn new(
        input: Box<dyn Executor>,
        skip: usize,
        fetch: Option<usize>,
        with_ties: bool,
        ties_order: Vec<SortExpr>,
        ctx: ExecutionContext,
    ) -> Self {
        Self {
            input,
            skip,
            fetch,
            with_ties,
            ties_order,
            ctx,
            skipped: 0,
            yielded: 0,
            boundary_keys: None,
            done: false,
        }
    }
}

impl Executor for LimitExec {
    fn next_row(&mut self) -> Result<Option<Record>> {
        if self.done {
            return Ok(None);
        }
        while self.skipped < self.skip {
            match self.input.next_row()? {
                Some(_) => self.skipped += 1,
                None => {
                    self.done = true;
                    return Ok(None);
                }
            }
        }
        match self.input.next_row()? {
            None => {
                self.done = true;
                Ok(None)
            }
            Some(row) => {
                if let Some(fetch) = self.fetch {
                    if fetch == 0 {
                        self.done = true;
                        return Ok(None);
                    }
                    if self.yielded < fetch {
                        if self.with_ties && self.yielded + 1 == fetch {
                            self.boundary_keys =
                                Some(eval_sort_keys(&row, &self.ties_order, &self.ctx)?);
                        }
                        self.yielded += 1;
                        return Ok(Some(row));
                    }
                    if self.with_ties {
                        let keys = eval_sort_keys(&row, &self.ties_order, &self.ctx)?;
                        let boundary = self.boundary_keys.as_ref().expect("set at fetch boundary");
                        if cmp_sort_keys(&keys, boundary, &self.ties_order) == Ordering::Equal {
                            self.yielded += 1;
                            return Ok(Some(row));
                        }
                        self.done = true;
                        return Ok(None);
                    }
                    self.done = true;
                    return Ok(None);
                }
                self.yielded += 1;
                Ok(Some(row))
            }
        }
    }
}

/// Heap entry for Top-N: max-heap ordered so the *worst* (last in sort order) is on top.
#[derive(Clone)]
struct TopNEntry {
    keys: Vec<Value>,
    exprs: Vec<SortExpr>,
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
        cmp_sort_keys(&self.keys, &other.keys, &self.exprs)
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
        let exprs = self.exprs.clone();
        let mut heap: BinaryHeap<TopNEntry> = BinaryHeap::new();

        while let Some(row) = self.input.next_row()? {
            let keys = eval_sort_keys(&row, &self.exprs, &self.ctx)?;
            heap.push(TopNEntry {
                keys,
                exprs: exprs.clone(),
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

/// Fold a non-row-dependent JSON document expression to compact text.
fn const_json_doc_text(expr: &Expression) -> Result<String> {
    let v = evaluate(expr, &Record::new(), &ExecutionContext::new())?;
    if v.is_null() {
        return Err(TakyonicError::Sql(
            "JSON SRF requires a JSON document".into(),
        ));
    }
    let s = v.to_display();
    let parsed: serde_json::Value = serde_json::from_str(s.trim()).map_err(|e| {
        TakyonicError::Sql(format!("JSON SRF: invalid JSON: {e}"))
    })?;
    Ok(parsed.to_string())
}

/// Correlated `LATERAL` JSON SRF — expand `doc` for each outer row.
struct LateralJsonSrfExec {
    left: Box<dyn Executor>,
    doc: Expression,
    kind: LateralJsonSrfKind,
    ctx: ExecutionContext,
    pending: Vec<Record>,
    pending_idx: usize,
}

impl Executor for LateralJsonSrfExec {
    fn next_row(&mut self) -> Result<Option<Record>> {
        loop {
            if self.pending_idx < self.pending.len() {
                let row = self.pending[self.pending_idx].clone();
                self.pending_idx += 1;
                return Ok(Some(row));
            }
            let Some(outer) = self.left.next_row()? else {
                return Ok(None);
            };
            let doc_val = evaluate(&self.doc, &outer, &self.ctx)?;
            if doc_val.is_null() {
                self.pending.clear();
                self.pending_idx = 0;
                continue;
            }
            let doc_text = doc_val.to_display();
            let elems = match &self.kind {
                LateralJsonSrfKind::ArrayElements {
                    column,
                    as_text,
                    ordinality_column,
                } => crate::sql::materialize_json_array_elements(
                    &doc_text,
                    column,
                    *as_text,
                    ordinality_column.as_deref(),
                )?,
                LateralJsonSrfKind::Each {
                    key_column,
                    value_column,
                    as_text,
                    ordinality_column,
                } => crate::sql::materialize_json_each(
                    &doc_text,
                    key_column,
                    value_column,
                    *as_text,
                    ordinality_column.as_deref(),
                )?,
                LateralJsonSrfKind::ObjectKeys {
                    column,
                    ordinality_column,
                } => crate::sql::materialize_json_object_keys(
                    &doc_text,
                    column,
                    ordinality_column.as_deref(),
                )?,
            };
            self.pending = elems
                .into_iter()
                .map(|e| combine_rows(&outer, &e))
                .collect();
            self.pending_idx = 0;
        }
    }
}

/// Correlated `LATERAL unnest(array)` — expand array for each outer row.
struct LateralUnnestExec {
    left: Box<dyn Executor>,
    array: Expression,
    column: String,
    ordinality_column: Option<String>,
    zero_based_ordinality: bool,
    ctx: ExecutionContext,
    pending: Vec<Record>,
    pending_idx: usize,
}

impl Executor for LateralUnnestExec {
    fn next_row(&mut self) -> Result<Option<Record>> {
        loop {
            if self.pending_idx < self.pending.len() {
                let row = self.pending[self.pending_idx].clone();
                self.pending_idx += 1;
                return Ok(Some(row));
            }
            let Some(outer) = self.left.next_row()? else {
                return Ok(None);
            };
            let elems = eval_array_elements(&self.array, &outer, &self.ctx)?;
            self.pending = elems
                .into_iter()
                .enumerate()
                .map(|(i, v)| {
                    let mut inner =
                        Record::new().set(self.column.clone(), value_to_field(&v));
                    if let Some(ord) = &self.ordinality_column {
                        let n = if self.zero_based_ordinality {
                            i
                        } else {
                            i + 1
                        };
                        inner = inner.set(ord.clone(), n.to_string());
                    }
                    combine_rows(&outer, &inner)
                })
                .collect();
            self.pending_idx = 0;
        }
    }
}

fn regexp_srf_needs_row(
    string: &Expression,
    pattern: &Expression,
    flags: &Option<Expression>,
) -> bool {
    crate::sql::expr_needs_row_eval(string)
        || crate::sql::expr_needs_row_eval(pattern)
        || flags
            .as_ref()
            .is_some_and(crate::sql::expr_needs_row_eval)
}

fn fold_regexp_srf_args(
    string: &Expression,
    pattern: &Expression,
    flags: &Option<Expression>,
) -> Result<(String, String, Option<String>)> {
    let ctx = ExecutionContext::new();
    let row = Record::new();
    let s = evaluate(string, &row, &ctx)?.to_display();
    let p = evaluate(pattern, &row, &ctx)?.to_display();
    let f = match flags {
        None => None,
        Some(e) => {
            let v = evaluate(e, &row, &ctx)?;
            if v.is_null() {
                None
            } else {
                Some(v.to_display())
            }
        }
    };
    Ok((s, p, f))
}

/// Correlated `LATERAL` regexp SRF — expand for each outer row.
struct LateralRegexpSrfExec {
    left: Box<dyn Executor>,
    string: Expression,
    pattern: Expression,
    flags: Option<Expression>,
    column: String,
    ordinality_column: Option<String>,
    kind: LateralRegexpSrfKind,
    ctx: ExecutionContext,
    pending: Vec<Record>,
    pending_idx: usize,
}

impl Executor for LateralRegexpSrfExec {
    fn next_row(&mut self) -> Result<Option<Record>> {
        loop {
            if self.pending_idx < self.pending.len() {
                let row = self.pending[self.pending_idx].clone();
                self.pending_idx += 1;
                return Ok(Some(row));
            }
            let Some(outer) = self.left.next_row()? else {
                return Ok(None);
            };
            let s_val = evaluate(&self.string, &outer, &self.ctx)?;
            if s_val.is_null() {
                self.pending.clear();
                self.pending_idx = 0;
                continue;
            }
            let p_val = evaluate(&self.pattern, &outer, &self.ctx)?;
            if p_val.is_null() {
                self.pending.clear();
                self.pending_idx = 0;
                continue;
            }
            let flags = match &self.flags {
                None => None,
                Some(e) => {
                    let v = evaluate(e, &outer, &self.ctx)?;
                    if v.is_null() {
                        None
                    } else {
                        Some(v.to_display())
                    }
                }
            };
            let s = s_val.to_display();
            let p = p_val.to_display();
            let elems = match self.kind {
                LateralRegexpSrfKind::SplitToTable => {
                    crate::sql::materialize_regexp_split_to_table(
                        &s,
                        &p,
                        flags.as_deref(),
                        &self.column,
                        self.ordinality_column.as_deref(),
                    )?
                }
                LateralRegexpSrfKind::Matches => crate::sql::materialize_regexp_matches(
                    &s,
                    &p,
                    flags.as_deref(),
                    &self.column,
                    self.ordinality_column.as_deref(),
                )?,
            };
            self.pending = elems
                .into_iter()
                .map(|e| combine_rows(&outer, &e))
                .collect();
            self.pending_idx = 0;
        }
    }
}

fn null_template_from_rows(rows: &[Record]) -> Record {
    let mut keys = std::collections::BTreeSet::new();
    for row in rows {
        for k in row.fields.keys() {
            keys.insert(k.clone());
        }
    }
    null_template_from_keys(keys.iter())
}

fn null_template_from_keys<'a>(keys: impl Iterator<Item = &'a String>) -> Record {
    let mut out = Record::new();
    for k in keys {
        // Empty string matches `value_to_field(Value::Null)` — projects as blank/NULL-ish.
        out = out.set(k.clone(), "");
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
        Expression::OuterRef(name) => row
            .get(name)
            .map(Value::from_text)
            .ok_or_else(|| {
                TakyonicError::Sql(format!(
                    "outer reference `{name}` not found on current row"
                ))
            }),
        Expression::Literal(s) => Ok(Value::from_text(s)),
        Expression::Parameter(idx) => Ok(ctx.param(*idx)?.clone()),
        Expression::BinaryOp { left, op, right } => {
            let lv = evaluate(left, row, ctx)?;
            let rv = evaluate(right, row, ctx)?;
            // SQL three-valued logic: any NULL operand → UNKNOWN (NULL), not Bool(false).
            if lv.is_null() || rv.is_null() {
                return Ok(Value::Null);
            }
            Ok(Value::Bool(compare_sql_values(&lv, *op, &rv)))
        }
        Expression::And { left, right } => {
            let l = evaluate(left, row, ctx)?;
            let r = evaluate(right, row, ctx)?;
            Ok(sql_and_3vl(&l, &r))
        }
        Expression::Or { left, right } => {
            let l = evaluate(left, row, ctx)?;
            let r = evaluate(right, row, ctx)?;
            Ok(sql_or_3vl(&l, &r))
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
            if v.is_null() {
                return Ok(Value::Null);
            }
            let mut saw_null = false;
            let mut found = false;
            for x in list {
                if x.is_null() {
                    saw_null = true;
                    continue;
                }
                if values_equal(&v, x) {
                    found = true;
                    break;
                }
            }
            if found {
                Ok(Value::Bool(!*negated))
            } else if saw_null {
                Ok(Value::Null)
            } else {
                Ok(Value::Bool(*negated))
            }
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
        Expression::ArrayIndex { array, index } => {
            let elems = eval_array_elements(array, row, ctx)?;
            let idx_v = evaluate(index, row, ctx)?;
            if idx_v.is_null() {
                return Ok(Value::Null);
            }
            let idx = idx_v.as_f64().ok_or_else(|| {
                TakyonicError::Sql("array subscript must be numeric".into())
            })? as i64;
            if idx < 1 || idx as usize > elems.len() {
                return Ok(Value::Null);
            }
            Ok(elems[(idx as usize) - 1].clone())
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
        Expression::Like {
            expr: inner,
            pattern,
            case_insensitive,
            negated,
            any,
            escape,
        } => {
            let lv = evaluate(inner, row, ctx)?;
            if lv.is_null() {
                return Ok(Value::Null);
            }
            let text = value_to_field(&lv);
            if *any {
                let elems = eval_array_elements(pattern, row, ctx)?;
                let matched = eval_like_any(&text, &elems, *case_insensitive, *escape);
                let result = match matched {
                    Some(true) => Value::Bool(true),
                    Some(false) => Value::Bool(false),
                    None => Value::Null,
                };
                return Ok(if *negated {
                    match result {
                        Value::Bool(b) => Value::Bool(!b),
                        other => other,
                    }
                } else {
                    result
                });
            }
            let pv = evaluate(pattern, row, ctx)?;
            if pv.is_null() {
                return Ok(Value::Null);
            }
            let pat = value_to_field(&pv);
            let matched = crate::sql::sql_like_match(&text, &pat, *case_insensitive, *escape);
            Ok(Value::Bool(if *negated { !matched } else { matched }))
        }
        Expression::SimilarTo {
            expr: inner,
            pattern,
            negated,
            escape,
        } => {
            let lv = evaluate(inner, row, ctx)?;
            let pv = evaluate(pattern, row, ctx)?;
            if lv.is_null() || pv.is_null() {
                return Ok(Value::Null);
            }
            let text = value_to_field(&lv);
            let pat = value_to_field(&pv);
            let matched = crate::sql::sql_similar_to_match(&text, &pat, *escape)?;
            Ok(Value::Bool(if *negated { !matched } else { matched }))
        }
        Expression::RegexMatch {
            expr: inner,
            pattern,
            case_insensitive,
            negated,
        } => {
            let lv = evaluate(inner, row, ctx)?;
            let pv = evaluate(pattern, row, ctx)?;
            if lv.is_null() || pv.is_null() {
                return Ok(Value::Null);
            }
            let text = value_to_field(&lv);
            let pat = value_to_field(&pv);
            let flags = if *case_insensitive { Some("i") } else { None };
            let matched = crate::sql::regexp_like(&text, &pat, flags)?;
            Ok(Value::Bool(if *negated { !matched } else { matched }))
        }
        Expression::AtTimeZone {
            timestamp,
            time_zone,
        } => {
            let tv = evaluate(timestamp, row, ctx)?;
            let zv = evaluate(time_zone, row, ctx)?;
            if tv.is_null() || zv.is_null() {
                return Ok(Value::Null);
            }
            Ok(Value::String(crate::sql::at_time_zone(
                &value_to_field(&tv),
                &value_to_field(&zv),
            )?))
        }
        Expression::Case {
            when_then,
            else_result,
        } => {
            for (cond, result) in when_then {
                if evaluate_bool(cond, row, ctx)? {
                    return evaluate(result, row, ctx);
                }
            }
            match else_result {
                Some(e) => evaluate(e, row, ctx),
                None => Ok(Value::Null),
            }
        }
        Expression::IsNull { expr, negated } => {
            let v = evaluate(expr, row, ctx)?;
            let is_null = v.is_null();
            Ok(Value::Bool(if *negated { !is_null } else { is_null }))
        }
        Expression::IsBoolTest {
            expr,
            test,
            negated,
        } => {
            let v = evaluate(expr, row, ctx)?;
            let known = if v.is_null() {
                None
            } else {
                Some(v.is_truthy())
            };
            let matched = match test {
                crate::sql::BoolTest::True => known == Some(true),
                crate::sql::BoolTest::False => known == Some(false),
                crate::sql::BoolTest::Unknown => known.is_none(),
            };
            Ok(Value::Bool(if *negated { !matched } else { matched }))
        }
        Expression::IsDistinctFrom {
            left,
            right,
            negated,
        } => {
            let lv = evaluate(left, row, ctx)?;
            let rv = evaluate(right, row, ctx)?;
            let distinct = match (lv.is_null(), rv.is_null()) {
                (true, true) => false,
                (true, false) | (false, true) => true,
                (false, false) => !values_equal(&lv, &rv),
            };
            Ok(Value::Bool(if *negated { !distinct } else { distinct }))
        }
        Expression::QuantifiedCmp {
            left,
            op,
            right,
            quantifier,
        } => {
            let lv = evaluate(left, row, ctx)?;
            let elems = eval_array_elements(right, row, ctx)?;
            Ok(eval_quantified_cmp(&lv, *op, &elems, *quantifier))
        }
        Expression::Not { expr } => {
            let v = evaluate(expr, row, ctx)?;
            if v.is_null() {
                Ok(Value::Null)
            } else {
                Ok(Value::Bool(!v.is_truthy()))
            }
        }
        Expression::Coalesce(args) => {
            for arg in args {
                let v = evaluate(arg, row, ctx)?;
                if !v.is_null() {
                    return Ok(v);
                }
            }
            Ok(Value::Null)
        }
        Expression::Cast {
            expr,
            target,
            try_cast,
        } => {
            let v = evaluate(expr, row, ctx)?;
            crate::sql::cast_sql_value(&v, *target, *try_cast)
        }
        Expression::NullIf { left, right } => {
            let lv = evaluate(left, row, ctx)?;
            let rv = evaluate(right, row, ctx)?;
            if values_equal(&lv, &rv) {
                Ok(Value::Null)
            } else {
                Ok(lv)
            }
        }
        Expression::ScalarFunction { name, args } => {
            eval_scalar_function(name, args, row, ctx)
        }
    }
}

fn eval_scalar_function(
    name: &str,
    args: &[Expression],
    row: &Record,
    ctx: &ExecutionContext,
) -> Result<Value> {
    if name == "ROW_TO_JSON" {
        return eval_row_to_json(args, row, ctx);
    }
    let mut vals = Vec::with_capacity(args.len());
    for a in args {
        vals.push(evaluate(a, row, ctx)?);
    }
    match name {
        "LOWER" => {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            Ok(Value::String(vals[0].to_display().to_lowercase()))
        }
        "UPPER" => {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            Ok(Value::String(vals[0].to_display().to_uppercase()))
        }
        "LENGTH" | "CHAR_LENGTH" | "CHARACTER_LENGTH" => {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            Ok(Value::Int(vals[0].to_display().chars().count() as i64))
        }
        "OCTET_LENGTH" => {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            Ok(Value::Int(vals[0].to_display().len() as i64))
        }
        "BIT_LENGTH" => {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            Ok(Value::Int((vals[0].to_display().len() * 8) as i64))
        }
        "TRIM" => {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            let s = vals[0].to_display();
            let side = vals
                .get(1)
                .map(|v| v.to_display().to_ascii_uppercase())
                .unwrap_or_else(|| "BOTH".into());
            let chars = match vals.get(2) {
                None => None,
                Some(v) if v.is_null() => None,
                Some(v) => Some(v.to_display()),
            };
            let out = match side.as_str() {
                "LEADING" => crate::sql::ltrim(&s, chars.as_deref()),
                "TRAILING" => crate::sql::rtrim(&s, chars.as_deref()),
                _ => crate::sql::btrim(&s, chars.as_deref()),
            };
            Ok(Value::String(out))
        }
        "BTRIM" | "LTRIM" | "RTRIM" => {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            let chars = match vals.get(1) {
                None => None,
                Some(v) if v.is_null() => None,
                Some(v) => Some(v.to_display()),
            };
            let out = match name {
                "BTRIM" => crate::sql::btrim(&vals[0].to_display(), chars.as_deref()),
                "LTRIM" => crate::sql::ltrim(&vals[0].to_display(), chars.as_deref()),
                _ => crate::sql::rtrim(&vals[0].to_display(), chars.as_deref()),
            };
            Ok(Value::String(out))
        }
        "TRANSLATE" => {
            if vals[0].is_null() || vals[1].is_null() || vals[2].is_null() {
                return Ok(Value::Null);
            }
            Ok(Value::String(crate::sql::translate(
                &vals[0].to_display(),
                &vals[1].to_display(),
                &vals[2].to_display(),
            )))
        }
        "SUBSTRING" | "SUBSTR" => {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            let s = vals[0].to_display();
            let start = vals
                .get(1)
                .and_then(|v| v.as_f64())
                .map(|f| f as i64)
                .unwrap_or(1);
            let chars: Vec<char> = s.chars().collect();
            if start == 0 {
                return Ok(Value::String(String::new()));
            }
            let from = if start > 0 {
                (start as usize).saturating_sub(1)
            } else {
                chars.len().saturating_sub((-start) as usize)
            };
            if from >= chars.len() {
                return Ok(Value::String(String::new()));
            }
            let slice = if let Some(len_v) = vals.get(2) {
                if len_v.is_null() {
                    return Ok(Value::Null);
                }
                let len = len_v.as_f64().map(|f| f as i64).unwrap_or(0);
                if len <= 0 {
                    String::new()
                } else {
                    chars[from..].iter().take(len as usize).collect()
                }
            } else {
                chars[from..].iter().collect()
            };
            Ok(Value::String(slice))
        }
        "CONCAT" => {
            // PostgreSQL CONCAT: NULL args become empty strings.
            let mut out = String::new();
            for v in &vals {
                if !v.is_null() {
                    out.push_str(&v.to_display());
                }
            }
            Ok(Value::String(out))
        }
        "CONCAT_WS" => {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            Ok(Value::String(crate::sql::concat_ws(
                &vals[0].to_display(),
                &vals[1..],
            )))
        }
        "FORMAT" => {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            Ok(Value::String(crate::sql::format_sql(
                &vals[0].to_display(),
                &vals[1..],
            )?))
        }
        "QUOTE_IDENT" => {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            Ok(Value::String(crate::sql::quote_ident(
                &vals[0].to_display(),
            )))
        }
        "QUOTE_LITERAL" => {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            Ok(Value::String(crate::sql::quote_literal(
                &vals[0].to_display(),
            )))
        }
        "QUOTE_NULLABLE" => Ok(Value::String(crate::sql::quote_nullable(&vals[0]))),
        "WIDTH_BUCKET" => {
            if vals.iter().any(|v| v.is_null()) {
                return Ok(Value::Null);
            }
            let operand = vals[0].as_f64().ok_or_else(|| {
                TakyonicError::Sql("width_bucket operand must be numeric".into())
            })?;
            let low = vals[1].as_f64().ok_or_else(|| {
                TakyonicError::Sql("width_bucket low must be numeric".into())
            })?;
            let high = vals[2].as_f64().ok_or_else(|| {
                TakyonicError::Sql("width_bucket high must be numeric".into())
            })?;
            let count = vals[3].as_f64().ok_or_else(|| {
                TakyonicError::Sql("width_bucket count must be numeric".into())
            })? as i64;
            Ok(Value::Int(crate::sql::width_bucket(
                operand, low, high, count,
            )?))
        }
        "REPLACE" => {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            let hay = vals[0].to_display();
            let from = if vals[1].is_null() {
                String::new()
            } else {
                vals[1].to_display()
            };
            let to = if vals[2].is_null() {
                String::new()
            } else {
                vals[2].to_display()
            };
            if from.is_empty() {
                return Ok(Value::String(hay));
            }
            Ok(Value::String(hay.replace(&from, &to)))
        }
        "REGEXP_REPLACE" => {
            if vals[0].is_null() || vals[1].is_null() || vals[2].is_null() {
                return Ok(Value::Null);
            }
            let flags = match vals.get(3) {
                None => None,
                Some(v) if v.is_null() => None,
                Some(v) => Some(v.to_display()),
            };
            Ok(Value::String(crate::sql::regexp_replace(
                &vals[0].to_display(),
                &vals[1].to_display(),
                &vals[2].to_display(),
                flags.as_deref(),
            )?))
        }
        "REGEXP_LIKE" => {
            if vals[0].is_null() || vals[1].is_null() {
                return Ok(Value::Null);
            }
            let flags = match vals.get(2) {
                None => None,
                Some(v) if v.is_null() => None,
                Some(v) => Some(v.to_display()),
            };
            Ok(Value::Bool(crate::sql::regexp_like(
                &vals[0].to_display(),
                &vals[1].to_display(),
                flags.as_deref(),
            )?))
        }
        "LPAD" | "RPAD" => {
            if vals[0].is_null() || vals[1].is_null() {
                return Ok(Value::Null);
            }
            let length = vals[1].as_f64().ok_or_else(|| {
                TakyonicError::Sql(format!("{} length must be numeric", name.to_ascii_lowercase()))
            })? as i64;
            let fill = match vals.get(2) {
                None => " ".to_string(),
                Some(v) if v.is_null() => " ".to_string(),
                Some(v) => v.to_display(),
            };
            let out = if name == "LPAD" {
                crate::sql::lpad(&vals[0].to_display(), length, &fill)?
            } else {
                crate::sql::rpad(&vals[0].to_display(), length, &fill)?
            };
            Ok(Value::String(out))
        }
        "REPEAT" => {
            if vals[0].is_null() || vals[1].is_null() {
                return Ok(Value::Null);
            }
            let count = vals[1].as_f64().ok_or_else(|| {
                TakyonicError::Sql("repeat count must be numeric".into())
            })? as i64;
            Ok(Value::String(crate::sql::repeat(
                &vals[0].to_display(),
                count,
            )?))
        }
        "LEFT" | "RIGHT" => {
            if vals[0].is_null() || vals[1].is_null() {
                return Ok(Value::Null);
            }
            let n = vals[1].as_f64().ok_or_else(|| {
                TakyonicError::Sql(format!(
                    "{} length must be numeric",
                    name.to_ascii_lowercase()
                ))
            })? as i64;
            let out = if name == "LEFT" {
                crate::sql::left(&vals[0].to_display(), n)
            } else {
                crate::sql::right(&vals[0].to_display(), n)
            };
            Ok(Value::String(out))
        }
        "REVERSE" => {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            Ok(Value::String(crate::sql::reverse(&vals[0].to_display())))
        }
        "INITCAP" => {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            Ok(Value::String(crate::sql::initcap(&vals[0].to_display())))
        }
        "ASCII" => {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            Ok(Value::Int(crate::sql::ascii(&vals[0].to_display())))
        }
        "CHR" => {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            let n = vals[0].as_f64().ok_or_else(|| {
                TakyonicError::Sql("chr argument must be numeric".into())
            })? as i64;
            Ok(Value::String(crate::sql::chr(n)?))
        }
        "MD5" => {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            Ok(Value::String(crate::sql::md5_hex(&vals[0].to_display())))
        }
        "ENCODE" => {
            if vals[0].is_null() || vals[1].is_null() {
                return Ok(Value::Null);
            }
            Ok(Value::String(crate::sql::encode_bytes(
                &vals[0].to_display(),
                &vals[1].to_display(),
            )?))
        }
        "DECODE" => {
            if vals[0].is_null() || vals[1].is_null() {
                return Ok(Value::Null);
            }
            Ok(Value::String(crate::sql::decode_bytes(
                &vals[0].to_display(),
                &vals[1].to_display(),
            )?))
        }
        "STARTS_WITH" => {
            if vals[0].is_null() || vals[1].is_null() {
                return Ok(Value::Null);
            }
            Ok(Value::Bool(crate::sql::starts_with(
                &vals[0].to_display(),
                &vals[1].to_display(),
            )))
        }
        "ENDS_WITH" => {
            if vals[0].is_null() || vals[1].is_null() {
                return Ok(Value::Null);
            }
            Ok(Value::Bool(crate::sql::ends_with(
                &vals[0].to_display(),
                &vals[1].to_display(),
            )))
        }
        "OVERLAY" => {
            if vals[0].is_null() || vals[1].is_null() || vals[2].is_null() {
                return Ok(Value::Null);
            }
            let from = vals[2].as_f64().ok_or_else(|| {
                TakyonicError::Sql("overlay FROM must be numeric".into())
            })? as i64;
            let for_count = match vals.get(3) {
                None => None,
                Some(v) if v.is_null() => None,
                Some(v) => Some(v.as_f64().ok_or_else(|| {
                    TakyonicError::Sql("overlay FOR must be numeric".into())
                })? as i64),
            };
            Ok(Value::String(crate::sql::overlay(
                &vals[0].to_display(),
                &vals[1].to_display(),
                from,
                for_count,
            )?))
        }
        "STRPOS" | "POSITION" => {
            // STRPOS(haystack, needle) — 1-based; 0 if missing. NULL → NULL.
            if vals[0].is_null() || vals[1].is_null() {
                return Ok(Value::Null);
            }
            let hay = vals[0].to_display();
            let needle = vals[1].to_display();
            if needle.is_empty() {
                return Ok(Value::Int(1));
            }
            match hay.find(&needle) {
                Some(byte_idx) => {
                    let pos = hay[..byte_idx].chars().count() as i64 + 1;
                    Ok(Value::Int(pos))
                }
                None => Ok(Value::Int(0)),
            }
        }
        "ABS" => {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            match &vals[0] {
                Value::Int(n) => Ok(Value::Int(n.saturating_abs())),
                other => {
                    let f = other.as_f64().ok_or_else(|| {
                        TakyonicError::Sql(format!("ABS requires a number, got {other:?}"))
                    })?;
                    Ok(Value::Float(f.abs()))
                }
            }
        }
        "NEGATE" => {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            match &vals[0] {
                Value::Int(n) => Ok(Value::Int(-n)),
                other => {
                    let f = other.as_f64().ok_or_else(|| {
                        TakyonicError::Sql(format!("unary minus requires a number, got {other:?}"))
                    })?;
                    Ok(Value::Float(-f))
                }
            }
        }
        "CEIL" | "CEILING" => {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            let f = vals[0].as_f64().ok_or_else(|| {
                TakyonicError::Sql("CEIL requires a number".into())
            })?;
            Ok(Value::Float(f.ceil()))
        }
        "FLOOR" => {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            let f = vals[0].as_f64().ok_or_else(|| {
                TakyonicError::Sql("FLOOR requires a number".into())
            })?;
            Ok(Value::Float(f.floor()))
        }
        "ROUND" => {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            let f = vals[0].as_f64().ok_or_else(|| {
                TakyonicError::Sql("ROUND requires a number".into())
            })?;
            let digits = vals
                .get(1)
                .map(|v| {
                    if v.is_null() {
                        Ok(0i64)
                    } else {
                        v.as_f64()
                            .map(|x| x as i64)
                            .ok_or_else(|| TakyonicError::Sql("ROUND digits must be numeric".into()))
                    }
                })
                .transpose()?
                .unwrap_or(0);
            let factor = 10f64.powi(digits as i32);
            Ok(Value::Float((f * factor).round() / factor))
        }
        "TRUNC" => {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            let f = vals[0].as_f64().ok_or_else(|| {
                TakyonicError::Sql("TRUNC requires a number".into())
            })?;
            let digits = vals
                .get(1)
                .map(|v| {
                    if v.is_null() {
                        Ok(0i64)
                    } else {
                        v.as_f64()
                            .map(|x| x as i64)
                            .ok_or_else(|| TakyonicError::Sql("TRUNC digits must be numeric".into()))
                    }
                })
                .transpose()?
                .unwrap_or(0);
            Ok(Value::Float(crate::sql::trunc_num(f, digits)))
        }
        "SIGN" => {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            let f = vals[0].as_f64().ok_or_else(|| {
                TakyonicError::Sql("SIGN requires a number".into())
            })?;
            Ok(Value::Float(crate::sql::sign(f)))
        }
        "MOD" => {
            if vals[0].is_null() || vals[1].is_null() {
                return Ok(Value::Null);
            }
            let a = vals[0].as_f64().ok_or_else(|| {
                TakyonicError::Sql("MOD requires numbers".into())
            })?;
            let b = vals[1].as_f64().ok_or_else(|| {
                TakyonicError::Sql("MOD requires numbers".into())
            })?;
            if b == 0.0 {
                return Err(TakyonicError::Sql("division by zero in MOD".into()));
            }
            if matches!((&vals[0], &vals[1]), (Value::Int(_), Value::Int(_))) {
                Ok(Value::Int((a as i64) % (b as i64)))
            } else {
                Ok(Value::Float(a % b))
            }
        }
        "DIV" => {
            if vals[0].is_null() || vals[1].is_null() {
                return Ok(Value::Null);
            }
            let y = vals[0].as_f64().ok_or_else(|| {
                TakyonicError::Sql("DIV requires numbers".into())
            })?;
            let x = vals[1].as_f64().ok_or_else(|| {
                TakyonicError::Sql("DIV requires numbers".into())
            })?;
            Ok(Value::Int(crate::sql::div_int(y, x)?))
        }
        "PI" => Ok(Value::Float(std::f64::consts::PI)),
        "SQRT" => {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            let f = vals[0].as_f64().ok_or_else(|| {
                TakyonicError::Sql("SQRT requires a number".into())
            })?;
            if f < 0.0 {
                return Err(TakyonicError::Sql(
                    "cannot take square root of a negative number".into(),
                ));
            }
            Ok(Value::Float(f.sqrt()))
        }
        "CBRT" => {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            let f = vals[0].as_f64().ok_or_else(|| {
                TakyonicError::Sql("CBRT requires a number".into())
            })?;
            Ok(Value::Float(f.cbrt()))
        }
        "LN" => {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            let f = vals[0].as_f64().ok_or_else(|| {
                TakyonicError::Sql("LN requires a number".into())
            })?;
            if f <= 0.0 {
                return Err(TakyonicError::Sql(
                    "cannot take logarithm of a non-positive number".into(),
                ));
            }
            Ok(Value::Float(f.ln()))
        }
        "LOG" => {
            if vals.iter().any(|v| v.is_null()) {
                return Ok(Value::Null);
            }
            let nums: Result<Vec<f64>> = vals
                .iter()
                .map(|v| {
                    v.as_f64()
                        .ok_or_else(|| TakyonicError::Sql("LOG requires numbers".into()))
                })
                .collect();
            Ok(Value::Float(crate::sql::log_num(&nums?)?))
        }
        "EXP" => {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            let f = vals[0].as_f64().ok_or_else(|| {
                TakyonicError::Sql("EXP requires a number".into())
            })?;
            Ok(Value::Float(f.exp()))
        }
        "SIN" | "COS" | "TAN" | "ASIN" | "ACOS" | "ATAN" | "RADIANS" | "DEGREES" => {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            let f = vals[0].as_f64().ok_or_else(|| {
                TakyonicError::Sql(format!("{name} requires a number"))
            })?;
            let out = match name {
                "SIN" => f.sin(),
                "COS" => f.cos(),
                "TAN" => f.tan(),
                "ASIN" => {
                    if !(-1.0..=1.0).contains(&f) {
                        return Err(TakyonicError::Sql(
                            "ASIN input out of range [-1, 1]".into(),
                        ));
                    }
                    f.asin()
                }
                "ACOS" => {
                    if !(-1.0..=1.0).contains(&f) {
                        return Err(TakyonicError::Sql(
                            "ACOS input out of range [-1, 1]".into(),
                        ));
                    }
                    f.acos()
                }
                "ATAN" => f.atan(),
                "RADIANS" => f.to_radians(),
                "DEGREES" => f.to_degrees(),
                _ => unreachable!(),
            };
            Ok(Value::Float(out))
        }
        "ATAN2" => {
            if vals[0].is_null() || vals[1].is_null() {
                return Ok(Value::Null);
            }
            let y = vals[0].as_f64().ok_or_else(|| {
                TakyonicError::Sql("ATAN2 requires numbers".into())
            })?;
            let x = vals[1].as_f64().ok_or_else(|| {
                TakyonicError::Sql("ATAN2 requires numbers".into())
            })?;
            Ok(Value::Float(y.atan2(x)))
        }
        "POWER" | "POW" => {
            if vals[0].is_null() || vals[1].is_null() {
                return Ok(Value::Null);
            }
            let a = vals[0].as_f64().ok_or_else(|| {
                TakyonicError::Sql("POWER requires numbers".into())
            })?;
            let b = vals[1].as_f64().ok_or_else(|| {
                TakyonicError::Sql("POWER requires numbers".into())
            })?;
            Ok(Value::Float(a.powf(b)))
        }
        "NOW" | "CURRENT_TIMESTAMP" | "STATEMENT_TIMESTAMP"
        | "TRANSACTION_TIMESTAMP" => {
            Ok(Value::String(ctx.statement_timestamp.clone()))
        }
        "LOCALTIMESTAMP" => {
            // Wall clock in session TimeZone (no offset suffix), matching PG LOCALTIMESTAMP.
            let local = crate::sql::at_time_zone(&ctx.statement_timestamp, &ctx.timezone)?;
            Ok(Value::String(local))
        }
        "CLOCK_TIMESTAMP" => Ok(Value::String(crate::sql::utc_now_timestamp())),
        "TIMEOFDAY" => Ok(Value::String(crate::sql::timeofday_now())),
        "CURRENT_USER" | "SESSION_USER" | "USER" | "CURRENT_ROLE" => {
            Ok(Value::String(ctx.current_user.clone()))
        }
        "CURRENT_SCHEMA" => Ok(Value::String(ctx.current_schema.clone())),
        "CURRENT_SCHEMAS" => {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            let include = vals[0].is_truthy();
            Ok(Value::String(current_schemas_array(
                &ctx.search_path,
                include,
            )))
        }
        "CURRENT_CATALOG" => Ok(Value::String(ctx.current_catalog.clone())),
        "VERSION" => Ok(Value::String(crate::sql::version_text())),
        "PG_BACKEND_PID" => Ok(Value::Int(std::process::id() as i64)),
        "PG_IS_IN_RECOVERY" => Ok(Value::Bool(false)),
        "PG_JIT_AVAILABLE" => Ok(Value::Bool(true)),
        "PG_RELOAD_CONF" => Ok(Value::Bool(crate::sql::pg_reload_conf())),
        "PG_ROTATE_LOGFILE" => Ok(Value::Bool(crate::sql::pg_rotate_logfile())),
        "PG_CONF_LOAD_TIME" => Ok(Value::String(crate::sql::pg_conf_load_time())),
        "NEXTVAL" => {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            Ok(Value::Int(crate::sql::nextval(
                ctx.session_id,
                &vals[0].to_display(),
            )?))
        }
        "CURRVAL" => {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            Ok(Value::Int(crate::sql::currval(
                ctx.session_id,
                &vals[0].to_display(),
            )?))
        }
        "LASTVAL" => Ok(Value::Int(crate::sql::lastval(ctx.session_id)?)),
        "SETVAL" => {
            if vals[0].is_null() || vals[1].is_null() {
                return Ok(Value::Null);
            }
            let value = match &vals[1] {
                Value::Int(i) => *i,
                Value::Float(f) => *f as i64,
                other => other.to_display().parse::<i64>().map_err(|_| {
                    TakyonicError::Sql("SETVAL value must be an integer".into())
                })?,
            };
            let is_called = vals.get(2).map(|v| v.is_truthy()).unwrap_or(true);
            Ok(Value::Int(crate::sql::setval(
                ctx.session_id,
                &vals[0].to_display(),
                value,
                is_called,
            )?))
        }
        "PG_GET_SERIAL_SEQUENCE" => {
            if vals.iter().any(|v| v.is_null()) {
                return Ok(Value::Null);
            }
            match crate::sql::pg_get_serial_sequence(
                &vals[0].to_display(),
                &vals[1].to_display(),
            ) {
                Some(s) => Ok(Value::String(s)),
                None => Ok(Value::Null),
            }
        }
        "PG_SEQUENCE_LAST_VALUE" => {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            match crate::sql::pg_sequence_last_value(&vals[0].to_display())? {
                Some(v) => Ok(Value::Int(v)),
                None => Ok(Value::Null),
            }
        }
        "PG_CURRENT_WAL_LSN" | "PG_CURRENT_WAL_INSERT_LSN" | "PG_CURRENT_WAL_FLUSH_LSN" => {
            Ok(Value::String(crate::sql::pg_current_wal_lsn()))
        }
        "PG_SWITCH_WAL" | "PG_SWITCH_XLOG" => {
            Ok(Value::String(crate::sql::pg_switch_wal()))
        }
        "PG_LAST_WAL_RECEIVE_LSN" | "PG_LAST_WAL_REPLAY_LSN"
        | "PG_LAST_XACT_REPLAY_TIMESTAMP" => Ok(Value::Null),
        "PG_IS_WAL_REPLAY_PAUSED" => {
            Ok(Value::Bool(crate::sql::pg_is_wal_replay_paused()))
        }
        "PG_WAL_REPLAY_PAUSE" => {
            crate::sql::pg_wal_replay_pause();
            Ok(Value::Null)
        }
        "PG_WAL_REPLAY_RESUME" => {
            crate::sql::pg_wal_replay_resume();
            Ok(Value::Null)
        }
        "PG_IS_IN_BACKUP" => Ok(Value::Bool(crate::sql::pg_is_in_backup())),
        "PG_BACKUP_START_TIME" => match crate::sql::pg_backup_start_time() {
            Some(t) => Ok(Value::String(t)),
            None => Ok(Value::Null),
        },
        "PG_BACKUP_START" | "PG_START_BACKUP" => {
            if vals.is_empty() || vals[0].is_null() {
                return Ok(Value::Null);
            }
            // Extra args (fast/exclusive) accepted and ignored in this stub.
            Ok(Value::String(crate::sql::pg_backup_start(
                &vals[0].to_display(),
            )?))
        }
        "PG_BACKUP_STOP" | "PG_STOP_BACKUP" => {
            Ok(Value::String(crate::sql::pg_backup_stop()?))
        }
        "PG_CREATE_RESTORE_POINT" => {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            Ok(Value::String(crate::sql::pg_create_restore_point(
                &vals[0].to_display(),
            )?))
        }
        "PG_PROMOTE" => {
            // Optional wait / wait_seconds args are accepted and ignored.
            Ok(Value::Bool(crate::sql::pg_promote()))
        }
        "PG_WAL_LSN_DIFF" => {
            if vals.iter().any(|v| v.is_null()) {
                return Ok(Value::Null);
            }
            match crate::sql::pg_wal_lsn_diff(&vals[0].to_display(), &vals[1].to_display()) {
                Some(n) => Ok(Value::Int(n)),
                None => Ok(Value::Null),
            }
        }
        "PG_WALFILE_NAME" => {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            match crate::sql::pg_walfile_name(&vals[0].to_display()) {
                Some(s) => Ok(Value::String(s)),
                None => Ok(Value::Null),
            }
        }
        "PG_WALFILE_NAME_OFFSET" => {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            match crate::sql::pg_walfile_name_offset(&vals[0].to_display()) {
                Some(s) => Ok(Value::String(s)),
                None => Ok(Value::Null),
            }
        }
        "PG_CANCEL_BACKEND" | "PG_TERMINATE_BACKEND" => {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            let pid = match &vals[0] {
                Value::Int(i) => *i,
                Value::Float(f) => *f as i64,
                other => other.to_display().parse::<i64>().map_err(|_| {
                    TakyonicError::Sql(format!("{name} requires an integer pid argument"))
                })?,
            };
            Ok(Value::Bool(crate::sql::pg_signal_backend(pid)))
        }
        "CURRENT_QUERY" => match &ctx.current_query {
            Some(q) => Ok(Value::String(q.clone())),
            None => Ok(Value::Null),
        },
        "PG_TRY_ADVISORY_LOCK" => {
            if vals.iter().any(|v| v.is_null()) {
                return Ok(Value::Null);
            }
            let key = advisory_lock_key_from_args(&vals)?;
            Ok(Value::Bool(crate::sql::pg_try_advisory_lock(
                ctx.session_id,
                key,
            )))
        }
        "PG_ADVISORY_LOCK" => {
            if vals.iter().any(|v| v.is_null()) {
                return Ok(Value::Null);
            }
            let key = advisory_lock_key_from_args(&vals)?;
            crate::sql::pg_advisory_lock(ctx.session_id, key)?;
            Ok(Value::Null)
        }
        "PG_ADVISORY_UNLOCK" => {
            if vals.iter().any(|v| v.is_null()) {
                return Ok(Value::Null);
            }
            let key = advisory_lock_key_from_args(&vals)?;
            Ok(Value::Bool(crate::sql::pg_advisory_unlock(
                ctx.session_id,
                key,
            )))
        }
        "PG_TRY_ADVISORY_LOCK_SHARED" => {
            if vals.iter().any(|v| v.is_null()) {
                return Ok(Value::Null);
            }
            let key = advisory_lock_key_from_args(&vals)?;
            Ok(Value::Bool(crate::sql::pg_try_advisory_lock_shared(
                ctx.session_id,
                key,
            )))
        }
        "PG_ADVISORY_LOCK_SHARED" => {
            if vals.iter().any(|v| v.is_null()) {
                return Ok(Value::Null);
            }
            let key = advisory_lock_key_from_args(&vals)?;
            crate::sql::pg_advisory_lock_shared(ctx.session_id, key)?;
            Ok(Value::Null)
        }
        "PG_ADVISORY_UNLOCK_SHARED" => {
            if vals.iter().any(|v| v.is_null()) {
                return Ok(Value::Null);
            }
            let key = advisory_lock_key_from_args(&vals)?;
            Ok(Value::Bool(crate::sql::pg_advisory_unlock_shared(
                ctx.session_id,
                key,
            )))
        }
        "PG_TRY_ADVISORY_XACT_LOCK" => {
            if vals.iter().any(|v| v.is_null()) {
                return Ok(Value::Null);
            }
            let key = advisory_lock_key_from_args(&vals)?;
            Ok(Value::Bool(crate::sql::pg_try_advisory_xact_lock(
                ctx.session_id,
                key,
            )))
        }
        "PG_ADVISORY_XACT_LOCK" => {
            if vals.iter().any(|v| v.is_null()) {
                return Ok(Value::Null);
            }
            let key = advisory_lock_key_from_args(&vals)?;
            crate::sql::pg_advisory_xact_lock(ctx.session_id, key)?;
            Ok(Value::Null)
        }
        "PG_TRY_ADVISORY_XACT_LOCK_SHARED" => {
            if vals.iter().any(|v| v.is_null()) {
                return Ok(Value::Null);
            }
            let key = advisory_lock_key_from_args(&vals)?;
            Ok(Value::Bool(crate::sql::pg_try_advisory_xact_lock_shared(
                ctx.session_id,
                key,
            )))
        }
        "PG_ADVISORY_XACT_LOCK_SHARED" => {
            if vals.iter().any(|v| v.is_null()) {
                return Ok(Value::Null);
            }
            let key = advisory_lock_key_from_args(&vals)?;
            crate::sql::pg_advisory_xact_lock_shared(ctx.session_id, key)?;
            Ok(Value::Null)
        }
        "PG_ADVISORY_UNLOCK_ALL" => {
            let _ = crate::sql::pg_advisory_unlock_all(ctx.session_id);
            Ok(Value::Null)
        }
        "GETDATABASEENCODING" | "PG_CLIENT_ENCODING" => {
            Ok(Value::String(crate::sql::database_encoding().into()))
        }
        "PG_ENCODING_TO_CHAR" => {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            let n = match &vals[0] {
                Value::Int(i) => *i,
                Value::Float(f) => *f as i64,
                other => other.to_display().parse::<i64>().map_err(|_| {
                    TakyonicError::Sql("PG_ENCODING_TO_CHAR requires an integer argument".into())
                })?,
            };
            Ok(Value::String(crate::sql::pg_encoding_to_char(n).into()))
        }
        "PG_CHAR_TO_ENCODING" => {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            Ok(Value::Int(crate::sql::pg_char_to_encoding(
                &vals[0].to_display(),
            )))
        }
        "PG_TABLE_IS_VISIBLE" => {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            let Some(cat) = ctx.relation_catalog.as_ref() else {
                return Ok(Value::Bool(false));
            };
            let visible = match &vals[0] {
                Value::Int(n) => {
                    let Ok(oid) = u32::try_from(*n) else {
                        return Ok(Value::Bool(false));
                    };
                    crate::oid::pg_table_is_visible_oid(&ctx.search_path, cat, oid)
                }
                other => {
                    crate::oid::pg_table_is_visible_name(&ctx.search_path, cat, &other.to_display())
                }
            };
            Ok(Value::Bool(visible))
        }
        "PG_TYPE_IS_VISIBLE" => {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            let key = match &vals[0] {
                Value::Int(n) => n.to_string(),
                other => other.to_display(),
            };
            Ok(Value::Bool(crate::oid::pg_type_is_visible(&key)))
        }
        "TO_REGPROC" | "TO_REGPROCEDURE" => {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            match crate::sql::to_regproc(&vals[0].to_display()) {
                Some(oid) => Ok(Value::Int(i64::from(oid))),
                None => Ok(Value::Null),
            }
        }
        "PG_FUNCTION_IS_VISIBLE" => {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            let visible = match &vals[0] {
                Value::Int(n) => {
                    let Ok(oid) = u32::try_from(*n) else {
                        return Ok(Value::Bool(false));
                    };
                    crate::sql::pg_function_is_visible_oid(oid)
                }
                other => crate::sql::pg_function_is_visible_name(&other.to_display()),
            };
            Ok(Value::Bool(visible))
        }
        "TO_REGOPER" | "TO_REGOPERATOR" => {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            match crate::sql::to_regoper(&vals[0].to_display()) {
                Some(oid) => Ok(Value::Int(i64::from(oid))),
                None => Ok(Value::Null),
            }
        }
        "PG_OPERATOR_IS_VISIBLE" => {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            let visible = match &vals[0] {
                Value::Int(n) => {
                    let Ok(oid) = u32::try_from(*n) else {
                        return Ok(Value::Bool(false));
                    };
                    crate::sql::pg_operator_is_visible_oid(oid)
                }
                other => crate::sql::pg_operator_is_visible_name(&other.to_display()),
            };
            Ok(Value::Bool(visible))
        }
        "TO_REGCOLLATION" => {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            match crate::sql::to_regcollation(&vals[0].to_display()) {
                Some(oid) => Ok(Value::Int(i64::from(oid))),
                None => Ok(Value::Null),
            }
        }
        "PG_COLLATION_IS_VISIBLE" => {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            let visible = match &vals[0] {
                Value::Int(n) => {
                    let Ok(oid) = u32::try_from(*n) else {
                        return Ok(Value::Bool(false));
                    };
                    crate::sql::pg_collation_is_visible_oid(oid)
                }
                other => crate::sql::pg_collation_is_visible_name(&other.to_display()),
            };
            Ok(Value::Bool(visible))
        }
        "PG_RELATION_IS_UPDATABLE" => {
            if vals[0].is_null() || vals[1].is_null() {
                return Ok(Value::Null);
            }
            let include_triggers = value_as_bool(&vals[1])?;
            let Some(cat) = ctx.relation_catalog.as_ref() else {
                return Ok(Value::Int(0));
            };
            let bits = match &vals[0] {
                Value::Int(n) => {
                    let Ok(oid) = u32::try_from(*n) else {
                        return Ok(Value::Int(0));
                    };
                    crate::oid::pg_relation_is_updatable(
                        cat,
                        crate::oid::NameOrOid::Oid(oid),
                        include_triggers,
                    )
                }
                other => crate::oid::pg_relation_is_updatable(
                    cat,
                    crate::oid::NameOrOid::Name(&other.to_display()),
                    include_triggers,
                ),
            };
            Ok(Value::Int(bits))
        }
        "PG_COLUMN_IS_UPDATABLE" => {
            if vals[0].is_null() || vals[1].is_null() || vals[2].is_null() {
                return Ok(Value::Null);
            }
            let include_triggers = value_as_bool(&vals[2])?;
            let Some(cat) = ctx.relation_catalog.as_ref() else {
                return Ok(Value::Bool(false));
            };
            let table_owned;
            let table_ref = match &vals[0] {
                Value::Int(n) => {
                    let Ok(oid) = u32::try_from(*n) else {
                        return Ok(Value::Bool(false));
                    };
                    crate::oid::NameOrOid::Oid(oid)
                }
                other => {
                    table_owned = other.to_display();
                    crate::oid::NameOrOid::Name(table_owned.as_str())
                }
            };
            let col_owned;
            let column = match &vals[1] {
                Value::Int(n) => crate::oid::ColumnRef::Attnum(*n),
                other => {
                    col_owned = other.to_display();
                    crate::oid::ColumnRef::Name(col_owned.as_str())
                }
            };
            Ok(Value::Bool(crate::oid::pg_column_is_updatable(
                cat,
                table_ref,
                column,
                include_triggers,
            )))
        }
        "PG_GET_INDEXDEF" => {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            // Extra args (column_no, pretty) accepted for PG signature compatibility.
            let Some(cat) = ctx.index_catalog.as_ref() else {
                return Ok(Value::Null);
            };
            let entry = match &vals[0] {
                Value::Int(n) => {
                    let Ok(oid) = u32::try_from(*n) else {
                        return Ok(Value::Null);
                    };
                    cat.by_oid(oid)
                }
                other => cat.by_name(&other.to_display()),
            };
            match entry {
                Some(e) => Ok(Value::String(crate::oid::pg_get_indexdef(e))),
                None => Ok(Value::Null),
            }
        }
        "PG_DESCRIBE_OBJECT" => {
            if vals[0].is_null() || vals[1].is_null() || vals[2].is_null() {
                return Ok(Value::Null);
            }
            let parse_u32 = |v: &Value, label: &str| -> Result<u32> {
                match v {
                    Value::Int(n) => u32::try_from(*n).map_err(|_| {
                        TakyonicError::Sql(format!("PG_DESCRIBE_OBJECT {label} out of range"))
                    }),
                    other => other.to_display().parse::<u32>().map_err(|_| {
                        TakyonicError::Sql(format!(
                            "PG_DESCRIBE_OBJECT {label} must be an OID"
                        ))
                    }),
                }
            };
            let classid = parse_u32(&vals[0], "classid")?;
            let objid = parse_u32(&vals[1], "objid")?;
            let objsubid = match &vals[2] {
                Value::Int(n) => i32::try_from(*n).unwrap_or(0),
                other => other.to_display().parse::<i32>().unwrap_or(0),
            };
            let role_names: Vec<String> = ctx
                .auth_catalog
                .as_ref()
                .map(|c| c.read().role_names().map(str::to_string).collect())
                .unwrap_or_default();
            let proc_name = if classid == crate::oid::CLASS_PG_PROC {
                crate::sql::regproc_name_for_oid(objid)
            } else {
                None
            };
            match crate::oid::pg_describe_object(
                classid,
                objid,
                objsubid,
                ctx.relation_catalog.as_deref(),
                ctx.index_catalog.as_deref(),
                role_names.iter().map(String::as_str),
                proc_name.as_deref(),
            ) {
                Some(s) => Ok(Value::String(s)),
                None => Ok(Value::Null),
            }
        }
        "PG_IDENTIFY_OBJECT" => {
            if vals[0].is_null() || vals[1].is_null() || vals[2].is_null() {
                return Ok(Value::Null);
            }
            let parse_u32 = |v: &Value, label: &str| -> Result<u32> {
                match v {
                    Value::Int(n) => u32::try_from(*n).map_err(|_| {
                        TakyonicError::Sql(format!("PG_IDENTIFY_OBJECT {label} out of range"))
                    }),
                    other => other.to_display().parse::<u32>().map_err(|_| {
                        TakyonicError::Sql(format!(
                            "PG_IDENTIFY_OBJECT {label} must be an OID"
                        ))
                    }),
                }
            };
            let classid = parse_u32(&vals[0], "classid")?;
            let objid = parse_u32(&vals[1], "objid")?;
            let objsubid = match &vals[2] {
                Value::Int(n) => i32::try_from(*n).unwrap_or(0),
                other => other.to_display().parse::<i32>().unwrap_or(0),
            };
            let role_names: Vec<String> = ctx
                .auth_catalog
                .as_ref()
                .map(|c| c.read().role_names().map(str::to_string).collect())
                .unwrap_or_default();
            let proc_name = if classid == crate::oid::CLASS_PG_PROC {
                crate::sql::regproc_name_for_oid(objid)
            } else {
                None
            };
            match crate::oid::pg_identify_object(
                classid,
                objid,
                objsubid,
                ctx.relation_catalog.as_deref(),
                ctx.index_catalog.as_deref(),
                role_names.iter().map(String::as_str),
                proc_name.as_deref(),
            ) {
                Some(s) => Ok(Value::String(s)),
                None => Ok(Value::Null),
            }
        }
        "PG_SIZE_PRETTY" => {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            let n = match &vals[0] {
                Value::Int(i) => *i,
                Value::Float(f) => *f as i64,
                other => {
                    let s = other.to_display();
                    s.parse::<i64>().or_else(|_| {
                        s.parse::<f64>().map(|f| f as i64)
                    }).map_err(|_| {
                        TakyonicError::Sql("PG_SIZE_PRETTY requires a numeric argument".into())
                    })?
                }
            };
            Ok(Value::String(crate::sql::pg_size_pretty(n)))
        }
        "PG_SIZE_BYTES" => {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            Ok(Value::Int(crate::sql::pg_size_bytes(
                &vals[0].to_display(),
            )?))
        }
        "PG_TYPEOF" => {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            match crate::sql::pg_typeof_value(&vals[0]) {
                Some(t) => Ok(Value::String(t.into())),
                None => Ok(Value::Null),
            }
        }
        "CURRENT_DATE" => Ok(Value::String(crate::sql::date_from_timestamp_text(
            &ctx.statement_timestamp,
        ))),
        "CURRENT_TIME" | "LOCALTIME" => Ok(Value::String(crate::sql::time_from_timestamp_text(
            &ctx.statement_timestamp,
        ))),
        "NUM_NONNULLS" => {
            Ok(Value::Int(vals.iter().filter(|v| !v.is_null()).count() as i64))
        }
        "NUM_NULLS" => {
            Ok(Value::Int(vals.iter().filter(|v| v.is_null()).count() as i64))
        }
        "RANDOM" => Ok(Value::Float(crate::sql::random_f64())),
        "GEN_RANDOM_UUID" => Ok(Value::String(crate::sql::gen_random_uuid())),
        "PG_SLEEP" => {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            let secs = vals[0].as_f64().ok_or_else(|| {
                TakyonicError::Sql("PG_SLEEP requires a numeric argument".into())
            })?;
            crate::sql::pg_sleep(secs)?;
            Ok(Value::Null)
        }
        "PG_NOTIFY" => {
            if vals.iter().any(|v| v.is_null()) {
                return Ok(Value::Null);
            }
            crate::sql::pg_notify(&vals[0].to_display(), &vals[1].to_display())?;
            Ok(Value::Null)
        }
        "PG_NOTIFICATION_QUEUE_USAGE" => {
            Ok(Value::Float(crate::sql::pg_notification_queue_usage(
                ctx.session_id,
            )))
        }
        "PG_LISTENING_CHANNELS" => {
            Ok(Value::String(crate::sql::format_listening_channels(
                &ctx.listening_channels,
            )))
        }
        "PG_COLUMN_SIZE" => {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            match crate::sql::pg_column_size(&vals[0]) {
                Some(n) => Ok(Value::Int(n)),
                None => Ok(Value::Null),
            }
        }
        "TXID_CURRENT" | "PG_CURRENT_XACT_ID" => Ok(Value::Int(ctx.txid as i64)),
        "PG_EXPORT_SNAPSHOT" => Ok(Value::String(crate::sql::pg_export_snapshot(ctx.txid))),
        "PG_CURRENT_SNAPSHOT" | "TXID_CURRENT_SNAPSHOT" => {
            Ok(Value::String(crate::sql::pg_current_snapshot(ctx.txid)))
        }
        "PG_SNAPSHOT_XMIN" => {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            match crate::sql::pg_snapshot_xmin(&vals[0].to_display()) {
                Some(n) => Ok(Value::Int(n as i64)),
                None => Ok(Value::Null),
            }
        }
        "PG_SNAPSHOT_XMAX" => {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            match crate::sql::pg_snapshot_xmax(&vals[0].to_display()) {
                Some(n) => Ok(Value::Int(n as i64)),
                None => Ok(Value::Null),
            }
        }
        "PG_VISIBLE_IN_SNAPSHOT" => {
            if vals.iter().any(|v| v.is_null()) {
                return Ok(Value::Null);
            }
            let xid = match &vals[0] {
                Value::Int(i) => *i,
                Value::Float(f) => *f as i64,
                other => other.to_display().parse::<i64>().map_err(|_| {
                    TakyonicError::Sql(
                        "PG_VISIBLE_IN_SNAPSHOT requires an integer xid argument".into(),
                    )
                })?,
            };
            match crate::sql::pg_visible_in_snapshot(xid, &vals[1].to_display()) {
                Some(b) => Ok(Value::Bool(b)),
                None => Ok(Value::Null),
            }
        }
        "TXID_STATUS" | "PG_XACT_STATUS" => {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            let xid = match &vals[0] {
                Value::Int(i) => *i,
                Value::Float(f) => *f as i64,
                other => other.to_display().parse::<i64>().map_err(|_| {
                    TakyonicError::Sql(format!("{name} requires an integer xid argument"))
                })?,
            };
            match crate::sql::txid_status(xid, ctx.txid) {
                Some(s) => Ok(Value::String(s.into())),
                None => Ok(Value::Null),
            }
        }
        "PG_POSTMASTER_START_TIME" => {
            Ok(Value::String(crate::sql::pg_postmaster_start_time()))
        }
        "CURRENT_SETTING" => {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            let name = vals[0].to_display();
            let missing_ok = vals.get(1).map(|v| v.is_truthy()).unwrap_or(false);
            match crate::sql::current_setting_value(
                &name,
                &ctx.search_path,
                &ctx.transaction_isolation,
                &ctx.current_user,
                &ctx.current_catalog,
                &ctx.timezone,
            ) {
                Some(v) => Ok(Value::String(v)),
                None if missing_ok => Ok(Value::Null),
                None => Err(TakyonicError::Sql(format!(
                    "unrecognized configuration parameter \"{name}\""
                ))),
            }
        }
        "SET_CONFIG" => {
            if vals[0].is_null() || vals[1].is_null() {
                return Ok(Value::Null);
            }
            let name = vals[0].to_display();
            let value = vals[1].to_display();
            let is_local = vals[2].is_truthy();
            let out = crate::sql::set_config(&name, &value, is_local, ctx.in_transaction)?;
            Ok(Value::String(out))
        }
        "HAS_TABLE_PRIVILEGE" => {
            let (user_opt, table, priv_s) = if vals.len() == 2 {
                if vals[0].is_null() || vals[1].is_null() {
                    return Ok(Value::Null);
                }
                (None, vals[0].to_display(), vals[1].to_display())
            } else {
                if vals[0].is_null() || vals[1].is_null() || vals[2].is_null() {
                    return Ok(Value::Null);
                }
                (
                    Some(vals[0].to_display()),
                    vals[1].to_display(),
                    vals[2].to_display(),
                )
            };
            let Some(catalog) = ctx.auth_catalog.as_ref() else {
                return Err(TakyonicError::Sql(
                    "HAS_TABLE_PRIVILEGE requires a session".into(),
                ));
            };
            let privs = crate::rbac::Privilege::parse_list(&priv_s)?;
            let cat = catalog.read();
            let held = if let Some(user) = user_opt {
                let other = cat.auth_context(&user)?;
                cat.has_any_table_privilege(&other, &table, &privs)
            } else {
                let Some(auth) = ctx.auth.as_ref() else {
                    return Err(TakyonicError::Sql(
                        "HAS_TABLE_PRIVILEGE requires a session".into(),
                    ));
                };
                cat.has_any_table_privilege(auth, &table, &privs)
            };
            Ok(Value::Bool(held))
        }
        "HAS_COLUMN_PRIVILEGE" => {
            let (user_opt, table, column, priv_s) = if vals.len() == 3 {
                if vals[0].is_null() || vals[1].is_null() || vals[2].is_null() {
                    return Ok(Value::Null);
                }
                (
                    None,
                    vals[0].to_display(),
                    vals[1].to_display(),
                    vals[2].to_display(),
                )
            } else {
                if vals[0].is_null()
                    || vals[1].is_null()
                    || vals[2].is_null()
                    || vals[3].is_null()
                {
                    return Ok(Value::Null);
                }
                (
                    Some(vals[0].to_display()),
                    vals[1].to_display(),
                    vals[2].to_display(),
                    vals[3].to_display(),
                )
            };
            let Some(catalog) = ctx.auth_catalog.as_ref() else {
                return Err(TakyonicError::Sql(
                    "HAS_COLUMN_PRIVILEGE requires a session".into(),
                ));
            };
            let privs = crate::rbac::ColumnPrivilege::parse_list(&priv_s)?;
            let cat = catalog.read();
            let held = if let Some(user) = user_opt {
                let other = cat.auth_context(&user)?;
                cat.has_any_column_privilege(&other, &table, &column, &privs)
            } else {
                let Some(auth) = ctx.auth.as_ref() else {
                    return Err(TakyonicError::Sql(
                        "HAS_COLUMN_PRIVILEGE requires a session".into(),
                    ));
                };
                cat.has_any_column_privilege(auth, &table, &column, &privs)
            };
            Ok(Value::Bool(held))
        }
        "HAS_ANY_COLUMN_PRIVILEGE" => {
            let (user_opt, table, priv_s) = if vals.len() == 2 {
                if vals[0].is_null() || vals[1].is_null() {
                    return Ok(Value::Null);
                }
                (None, vals[0].to_display(), vals[1].to_display())
            } else {
                if vals[0].is_null() || vals[1].is_null() || vals[2].is_null() {
                    return Ok(Value::Null);
                }
                (
                    Some(vals[0].to_display()),
                    vals[1].to_display(),
                    vals[2].to_display(),
                )
            };
            let Some(catalog) = ctx.auth_catalog.as_ref() else {
                return Err(TakyonicError::Sql(
                    "HAS_ANY_COLUMN_PRIVILEGE requires a session".into(),
                ));
            };
            let privs = crate::rbac::ColumnPrivilege::parse_list(&priv_s)?;
            let cat = catalog.read();
            // No per-column ACL: privilege on any column ≡ table-level column priv check.
            let held = if let Some(user) = user_opt {
                let other = cat.auth_context(&user)?;
                cat.has_any_column_privilege(&other, &table, "", &privs)
            } else {
                let Some(auth) = ctx.auth.as_ref() else {
                    return Err(TakyonicError::Sql(
                        "HAS_ANY_COLUMN_PRIVILEGE requires a session".into(),
                    ));
                };
                cat.has_any_column_privilege(auth, &table, "", &privs)
            };
            Ok(Value::Bool(held))
        }
        "OBJ_DESCRIPTION" => {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            if vals.len() == 2 && !vals[1].is_null() {
                let catalog = vals[1].to_display().to_ascii_lowercase();
                if catalog != "pg_class" {
                    return Err(TakyonicError::Sql(format!(
                        "unsupported obj_description catalog \"{catalog}\" (supported: pg_class)"
                    )));
                }
            }
            let key = match &vals[0] {
                Value::Int(oid) => {
                    let Some(cat) = ctx.relation_catalog.as_ref() else {
                        return Ok(Value::Null);
                    };
                    let Some(entry) = cat.by_oid(*oid as u32) else {
                        return Ok(Value::Null);
                    };
                    format!("t:{}", entry.name)
                }
                other => {
                    let table = other.to_display();
                    format!("t:{}", table.trim().to_ascii_lowercase())
                }
            };
            match ctx.comments.as_ref().and_then(|c| c.read().get(&key).cloned()) {
                Some(s) => Ok(Value::String(s)),
                None => Ok(Value::Null),
            }
        }
        "COL_DESCRIPTION" => {
            if vals[0].is_null() || vals[1].is_null() {
                return Ok(Value::Null);
            }
            let key = match (&vals[0], &vals[1]) {
                (Value::Int(oid), Value::Int(attnum)) => {
                    let Some(cat) = ctx.relation_catalog.as_ref() else {
                        return Ok(Value::Null);
                    };
                    let Some(col) = cat.column_at(*oid as u32, *attnum) else {
                        return Ok(Value::Null);
                    };
                    let Some(entry) = cat.by_oid(*oid as u32) else {
                        return Ok(Value::Null);
                    };
                    format!("c:{}.{}", entry.name, col)
                }
                _ => {
                    let table = vals[0].to_display();
                    let column = vals[1].to_display();
                    format!(
                        "c:{}.{}",
                        table.trim().to_ascii_lowercase(),
                        column.trim().to_ascii_lowercase()
                    )
                }
            };
            match ctx.comments.as_ref().and_then(|c| c.read().get(&key).cloned()) {
                Some(s) => Ok(Value::String(s)),
                None => Ok(Value::Null),
            }
        }
        "TO_REGCLASS" => {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            let name = vals[0].to_display();
            let Some(cat) = ctx.relation_catalog.as_ref() else {
                return Ok(Value::Null);
            };
            match cat.oid_of(&name) {
                Some(oid) => Ok(Value::Int(i64::from(oid))),
                None => Ok(Value::Null),
            }
        }
        "TO_REGROLE" => {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            let name = vals[0]
                .to_display()
                .trim()
                .trim_matches('"')
                .to_ascii_lowercase();
            if name.is_empty() {
                return Ok(Value::Null);
            }
            let Some(catalog) = ctx.auth_catalog.as_ref() else {
                return Ok(Value::Null);
            };
            let cat = catalog.read();
            if cat.get_role(&name).is_some() {
                Ok(Value::Int(i64::from(crate::oid::role_oid(&name))))
            } else {
                Ok(Value::Null)
            }
        }
        "TO_REGNAMESPACE" => {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            let name = vals[0]
                .to_display()
                .trim()
                .trim_matches('"')
                .to_ascii_lowercase();
            if name.is_empty() {
                return Ok(Value::Null);
            }
            if crate::oid::namespace_exists(&name)
                || crate::rbac::schema_exists(&name, &ctx.search_path)
            {
                Ok(Value::Int(i64::from(crate::oid::namespace_oid(&name))))
            } else {
                Ok(Value::Null)
            }
        }
        "TO_REGTYPE" => {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            let name = vals[0].to_display();
            match crate::oid::type_oid_from_name(&name) {
                Some(oid) => Ok(Value::Int(i64::from(oid))),
                None => Ok(Value::Null),
            }
        }
        "FORMAT_TYPE" => {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            let type_oid = match &vals[0] {
                Value::Int(n) => *n,
                other => {
                    return Err(TakyonicError::Sql(format!(
                        "FORMAT_TYPE type_oid must be integer, got {other:?}"
                    )));
                }
            };
            let typmod = match &vals[1] {
                Value::Null => -1,
                Value::Int(n) => *n,
                Value::String(s) if s.is_empty() => -1,
                other => {
                    return Err(TakyonicError::Sql(format!(
                        "FORMAT_TYPE typmod must be integer or NULL, got {other:?}"
                    )));
                }
            };
            Ok(Value::String(crate::oid::format_type(type_oid, typmod)))
        }
        "PG_GET_USERBYID" => {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            let oid = match &vals[0] {
                Value::Int(n) => *n as u32,
                other => {
                    return Err(TakyonicError::Sql(format!(
                        "PG_GET_USERBYID requires integer oid, got {other:?}"
                    )));
                }
            };
            let Some(catalog) = ctx.auth_catalog.as_ref() else {
                return Ok(Value::Null);
            };
            let names: Vec<String> = catalog.read().role_names().map(str::to_string).collect();
            match crate::oid::user_by_oid(oid, names.iter().map(String::as_str)) {
                Some(name) => Ok(Value::String(name.to_string())),
                None => Ok(Value::Null),
            }
        }
        "PG_RELATION_SIZE" | "PG_TABLE_SIZE" | "PG_TOTAL_RELATION_SIZE" | "PG_INDEXES_SIZE" => {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            let owned;
            let key = match &vals[0] {
                Value::Int(n) => crate::oid::NameOrOid::Oid(*n as u32),
                _ => {
                    owned = vals[0].to_display();
                    if owned.is_empty() {
                        return Ok(Value::Null);
                    }
                    crate::oid::NameOrOid::Name(owned.as_str())
                }
            };
            let Some(sizes) = ctx.relation_sizes.as_ref() else {
                return Ok(Value::Null);
            };
            let Some(entry) = sizes.get(key) else {
                return Ok(Value::Null);
            };
            let bytes = match name {
                "PG_TOTAL_RELATION_SIZE" => entry.total_bytes,
                "PG_INDEXES_SIZE" => entry.index_bytes(),
                _ => entry.heap_bytes,
            };
            Ok(Value::Int(bytes as i64))
        }
        "PG_DATABASE_SIZE" => {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            let db = vals[0]
                .to_display()
                .trim()
                .trim_matches('"')
                .to_ascii_lowercase();
            if db.is_empty() {
                return Ok(Value::Null);
            }
            // Single-database product: only `postgres` / current_catalog exist.
            if db != ctx.current_catalog.to_ascii_lowercase() && db != "postgres" {
                return Ok(Value::Null);
            }
            let Some(sizes) = ctx.relation_sizes.as_ref() else {
                return Ok(Value::Int(0));
            };
            Ok(Value::Int(sizes.database_bytes() as i64))
        }
        "PG_TABLESPACE_LOCATION" => {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            let arg = match &vals[0] {
                Value::Int(n) => n.to_string(),
                other => other.to_display(),
            };
            Ok(Value::String(crate::rbac::pg_tablespace_location(&arg)?))
        }
        "SHOBJ_DESCRIPTION" => {
            if vals[0].is_null() || vals[1].is_null() {
                return Ok(Value::Null);
            }
            let name = vals[0].to_display();
            let catalog = vals[1].to_display().to_ascii_lowercase();
            let prefix = match catalog.as_str() {
                "pg_authid" | "pg_roles" => "r",
                "pg_database" => "d",
                other => {
                    return Err(TakyonicError::Sql(format!(
                        "unsupported shobj_description catalog \"{other}\" \
                         (supported: pg_authid, pg_roles, pg_database)"
                    )));
                }
            };
            let key = format!("{prefix}:{}", name.trim().to_ascii_lowercase());
            match ctx.comments.as_ref().and_then(|c| c.read().get(&key).cloned()) {
                Some(s) => Ok(Value::String(s)),
                None => Ok(Value::Null),
            }
        }
        "HAS_SCHEMA_PRIVILEGE" => {
            let (user_opt, schema, priv_s) = if vals.len() == 2 {
                if vals[0].is_null() || vals[1].is_null() {
                    return Ok(Value::Null);
                }
                (None, vals[0].to_display(), vals[1].to_display())
            } else {
                if vals[0].is_null() || vals[1].is_null() || vals[2].is_null() {
                    return Ok(Value::Null);
                }
                (
                    Some(vals[0].to_display()),
                    vals[1].to_display(),
                    vals[2].to_display(),
                )
            };
            let privs = crate::rbac::SchemaPrivilege::parse_list(&priv_s)?;
            let held = if let Some(user) = user_opt.as_ref() {
                let Some(catalog) = ctx.auth_catalog.as_ref() else {
                    return Err(TakyonicError::Sql(
                        "HAS_SCHEMA_PRIVILEGE requires a session".into(),
                    ));
                };
                let cat = catalog.read();
                let auth = cat.auth_context(user)?;
                cat.has_any_schema_privilege(&auth, &schema, &ctx.search_path, &privs)?
            } else if let Some(catalog) = ctx.auth_catalog.as_ref() {
                let cat = catalog.read();
                let auth = if let Some(a) = ctx.auth.as_ref() {
                    a.clone()
                } else {
                    cat.auth_context(&ctx.current_user).unwrap_or_else(|_| {
                        crate::rbac::AuthContext {
                            user: ctx.current_user.clone(),
                            roles: std::collections::BTreeSet::from([ctx.current_user.clone()]),
                            is_superuser: ctx.current_user.eq_ignore_ascii_case("postgres"),
                        }
                    })
                };
                cat.has_any_schema_privilege(&auth, &schema, &ctx.search_path, &privs)?
            } else {
                let is_superuser = if let Some(auth) = ctx.auth.as_ref() {
                    auth.is_superuser
                } else {
                    ctx.current_user.eq_ignore_ascii_case("postgres")
                };
                crate::rbac::has_schema_privilege(
                    is_superuser,
                    &schema,
                    &ctx.search_path,
                    &privs,
                )?
            };
            Ok(Value::Bool(held))
        }
        "HAS_DATABASE_PRIVILEGE" => {
            let (user_opt, database, priv_s) = if vals.len() == 2 {
                if vals[0].is_null() || vals[1].is_null() {
                    return Ok(Value::Null);
                }
                (None, vals[0].to_display(), vals[1].to_display())
            } else {
                if vals[0].is_null() || vals[1].is_null() || vals[2].is_null() {
                    return Ok(Value::Null);
                }
                (
                    Some(vals[0].to_display()),
                    vals[1].to_display(),
                    vals[2].to_display(),
                )
            };
            let privs = crate::rbac::DatabasePrivilege::parse_list(&priv_s)?;
            let is_superuser = if let Some(user) = user_opt.as_ref() {
                let Some(catalog) = ctx.auth_catalog.as_ref() else {
                    return Err(TakyonicError::Sql(
                        "HAS_DATABASE_PRIVILEGE requires a session".into(),
                    ));
                };
                catalog.read().auth_context(user)?.is_superuser
            } else if let Some(auth) = ctx.auth.as_ref() {
                auth.is_superuser
            } else {
                ctx.current_user.eq_ignore_ascii_case("postgres")
            };
            let held = crate::rbac::has_database_privilege(is_superuser, &database, &privs)?;
            Ok(Value::Bool(held))
        }
        "HAS_TABLESPACE_PRIVILEGE" => {
            let (user_opt, tablespace, priv_s) = if vals.len() == 2 {
                if vals[0].is_null() || vals[1].is_null() {
                    return Ok(Value::Null);
                }
                (None, vals[0].to_display(), vals[1].to_display())
            } else {
                if vals[0].is_null() || vals[1].is_null() || vals[2].is_null() {
                    return Ok(Value::Null);
                }
                (
                    Some(vals[0].to_display()),
                    vals[1].to_display(),
                    vals[2].to_display(),
                )
            };
            let privs = crate::rbac::TablespacePrivilege::parse_list(&priv_s)?;
            let is_superuser = if let Some(user) = user_opt.as_ref() {
                let Some(catalog) = ctx.auth_catalog.as_ref() else {
                    return Err(TakyonicError::Sql(
                        "HAS_TABLESPACE_PRIVILEGE requires a session".into(),
                    ));
                };
                catalog.read().auth_context(user)?.is_superuser
            } else if let Some(auth) = ctx.auth.as_ref() {
                auth.is_superuser
            } else {
                ctx.current_user.eq_ignore_ascii_case("postgres")
            };
            let held = crate::rbac::has_tablespace_privilege(is_superuser, &tablespace, &privs)?;
            Ok(Value::Bool(held))
        }
        "HAS_FUNCTION_PRIVILEGE" => {
            let (user_opt, function, priv_s) = if vals.len() == 2 {
                if vals[0].is_null() || vals[1].is_null() {
                    return Ok(Value::Null);
                }
                (None, vals[0].to_display(), vals[1].to_display())
            } else {
                if vals[0].is_null() || vals[1].is_null() || vals[2].is_null() {
                    return Ok(Value::Null);
                }
                (
                    Some(vals[0].to_display()),
                    vals[1].to_display(),
                    vals[2].to_display(),
                )
            };
            let privs = crate::rbac::FunctionPrivilege::parse_list(&priv_s)?;
            let is_superuser = if let Some(user) = user_opt.as_ref() {
                let Some(catalog) = ctx.auth_catalog.as_ref() else {
                    return Err(TakyonicError::Sql(
                        "HAS_FUNCTION_PRIVILEGE requires a session".into(),
                    ));
                };
                catalog.read().auth_context(user)?.is_superuser
            } else if let Some(auth) = ctx.auth.as_ref() {
                auth.is_superuser
            } else {
                ctx.current_user.eq_ignore_ascii_case("postgres")
            };
            let held = crate::rbac::has_function_privilege(
                is_superuser,
                &function,
                &privs,
                crate::sql::is_known_sql_function,
            );
            Ok(Value::Bool(held))
        }
        "PG_HAS_ROLE" => {
            let (user_opt, role_v, priv_s) = if vals.len() == 2 {
                if vals[0].is_null() || vals[1].is_null() {
                    return Ok(Value::Null);
                }
                (None, &vals[0], vals[1].to_display())
            } else {
                if vals[0].is_null() || vals[1].is_null() || vals[2].is_null() {
                    return Ok(Value::Null);
                }
                (Some(&vals[0]), &vals[1], vals[2].to_display())
            };
            let privs = crate::rbac::RolePrivilege::parse_list(&priv_s)?;
            let Some(catalog) = ctx.auth_catalog.as_ref() else {
                return Err(TakyonicError::Sql(
                    "PG_HAS_ROLE requires a session".into(),
                ));
            };
            let cat = catalog.read();
            let resolve = |v: &Value| -> Result<String> {
                match v {
                    Value::Int(n) => {
                        let oid = u32::try_from(*n).map_err(|_| {
                            TakyonicError::Sql(format!("role with OID {n} does not exist"))
                        })?;
                        let names: Vec<&str> = cat.role_names().collect();
                        crate::oid::user_by_oid(oid, names)
                            .map(str::to_string)
                            .ok_or_else(|| {
                                TakyonicError::Sql(format!(
                                    "role with OID {oid} does not exist"
                                ))
                            })
                    }
                    other => {
                        let name = crate::rbac::role_name_leaf(&other.to_display());
                        if name.is_empty() {
                            return Err(TakyonicError::Sql(
                                "role \"\" does not exist".into(),
                            ));
                        }
                        Ok(name)
                    }
                }
            };
            let role = resolve(role_v)?;
            let user = if let Some(u) = user_opt {
                resolve(u)?
            } else {
                crate::rbac::role_name_leaf(&ctx.current_user)
            };
            let held = cat.has_role_privilege(&user, &role, &privs)?;
            Ok(Value::Bool(held))
        }
        "HAS_TYPE_PRIVILEGE" => {
            let (user_opt, type_v, priv_s) = if vals.len() == 2 {
                if vals[0].is_null() || vals[1].is_null() {
                    return Ok(Value::Null);
                }
                (None, &vals[0], vals[1].to_display())
            } else {
                if vals[0].is_null() || vals[1].is_null() || vals[2].is_null() {
                    return Ok(Value::Null);
                }
                (Some(vals[0].to_display()), &vals[1], vals[2].to_display())
            };
            let privs = crate::rbac::TypePrivilege::parse_list(&priv_s)?;
            let is_superuser = if let Some(user) = user_opt.as_ref() {
                let Some(catalog) = ctx.auth_catalog.as_ref() else {
                    return Err(TakyonicError::Sql(
                        "HAS_TYPE_PRIVILEGE requires a session".into(),
                    ));
                };
                catalog.read().auth_context(user)?.is_superuser
            } else if let Some(auth) = ctx.auth.as_ref() {
                auth.is_superuser
            } else {
                ctx.current_user.eq_ignore_ascii_case("postgres")
            };
            let type_key = match type_v {
                Value::Int(n) => {
                    let oid = u32::try_from(*n).map_err(|_| {
                        TakyonicError::Sql(format!("type with OID {n} does not exist"))
                    })?;
                    let formatted = crate::oid::format_type(i64::from(oid), -1);
                    if formatted.starts_with("???") {
                        return Err(TakyonicError::Sql(format!(
                            "type with OID {oid} does not exist"
                        )));
                    }
                    // Prefer canonical name from format_type for existence checks.
                    formatted
                }
                other => other.to_display(),
            };
            let held = crate::rbac::has_type_privilege(
                is_superuser,
                &type_key,
                &privs,
                |n| crate::oid::type_oid_from_name(n).is_some(),
            )?;
            Ok(Value::Bool(held))
        }
        "HAS_SEQUENCE_PRIVILEGE" => {
            let (user_opt, seq_name, priv_s) = if vals.len() == 2 {
                if vals[0].is_null() || vals[1].is_null() {
                    return Ok(Value::Null);
                }
                (None, vals[0].to_display(), vals[1].to_display())
            } else {
                if vals[0].is_null() || vals[1].is_null() || vals[2].is_null() {
                    return Ok(Value::Null);
                }
                (
                    Some(vals[0].to_display()),
                    vals[1].to_display(),
                    vals[2].to_display(),
                )
            };
            let privs = crate::rbac::SequencePrivilege::parse_list(&priv_s)?;
            let is_superuser = if let Some(user) = user_opt.as_ref() {
                let Some(catalog) = ctx.auth_catalog.as_ref() else {
                    return Err(TakyonicError::Sql(
                        "HAS_SEQUENCE_PRIVILEGE requires a session".into(),
                    ));
                };
                catalog.read().auth_context(user)?.is_superuser
            } else if let Some(auth) = ctx.auth.as_ref() {
                auth.is_superuser
            } else {
                ctx.current_user.eq_ignore_ascii_case("postgres")
            };
            let held = crate::rbac::has_sequence_privilege(
                is_superuser,
                &seq_name,
                &privs,
                crate::sql::sequence_exists,
            )?;
            Ok(Value::Bool(held))
        }
        "INET_SERVER_ADDR" => Ok(match &ctx.inet_server_addr {
            Some(a) => Value::String(a.clone()),
            None => Value::Null,
        }),
        "INET_SERVER_PORT" => Ok(match ctx.inet_server_port {
            Some(p) => Value::Int(p),
            None => Value::Null,
        }),
        "INET_CLIENT_ADDR" => Ok(match &ctx.inet_client_addr {
            Some(a) => Value::String(a.clone()),
            None => Value::Null,
        }),
        "INET_CLIENT_PORT" => Ok(match ctx.inet_client_port {
            Some(p) => Value::Int(p),
            None => Value::Null,
        }),
        "SETSEED" => {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            let s = vals[0].as_f64().ok_or_else(|| {
                TakyonicError::Sql("SETSEED requires a numeric argument".into())
            })?;
            crate::sql::setseed(s)?;
            Ok(Value::Null)
        }
        "GREATEST" | "LEAST" => {
            // PG: ignore NULLs; only all-NULL → NULL.
            let non_null: Vec<&Value> = vals.iter().filter(|v| !v.is_null()).collect();
            if non_null.is_empty() {
                return Ok(Value::Null);
            }
            let prefer_min = name == "LEAST";
            let all_num = non_null.iter().all(|v| v.as_f64().is_some());
            if all_num {
                let mut best = non_null[0].as_f64().unwrap();
                let mut best_v = non_null[0].clone();
                for v in &non_null[1..] {
                    let f = v.as_f64().unwrap();
                    if (prefer_min && f < best) || (!prefer_min && f > best) {
                        best = f;
                        best_v = (*v).clone();
                    }
                }
                Ok(best_v)
            } else {
                let mut best = non_null[0].to_display();
                let mut best_v = non_null[0].clone();
                for v in &non_null[1..] {
                    let s = v.to_display();
                    if (prefer_min && s < best) || (!prefer_min && s > best) {
                        best = s;
                        best_v = (*v).clone();
                    }
                }
                Ok(best_v)
            }
        }
        "EXTRACT" | "DATE_PART" => {
            if vals[0].is_null() || vals[1].is_null() {
                return Ok(Value::Null);
            }
            let field = vals[0].to_display().to_ascii_uppercase();
            let src = vals[1].to_display();
            if field == "EPOCH" {
                return Ok(Value::Float(crate::sql::extract_epoch_secs(&src)?));
            }
            let parts = crate::sql::parse_timestamp_parts(&src).ok_or_else(|| {
                TakyonicError::Sql(format!(
                    "EXTRACT source is not a date/timestamp: `{src}`"
                ))
            })?;
            let (y, m, d, hh, mm, ss) = parts;
            let n = match field.as_str() {
                "YEAR" | "YEARS" => y as i64,
                "MONTH" | "MONTHS" => m as i64,
                "DAY" | "DAYS" => d as i64,
                "HOUR" | "HOURS" => hh as i64,
                "MINUTE" | "MINUTES" => mm as i64,
                "SECOND" | "SECONDS" => ss as i64,
                "QUARTER" => ((m - 1) / 3 + 1) as i64,
                "DOY" | "DAYOFYEAR" => {
                    // Approx day-of-year ignoring leap for simple path — use civil days.
                    crate::sql::day_of_year(y, m, d) as i64
                }
                other => {
                    return Err(TakyonicError::Sql(format!(
                        "unsupported EXTRACT field `{other}` \
                         (YEAR/MONTH/DAY/HOUR/MINUTE/SECOND/QUARTER/DOY/EPOCH)"
                    )));
                }
            };
            Ok(Value::Int(n))
        }
        "DATE_TRUNC" => {
            if vals[0].is_null() || vals[1].is_null() {
                return Ok(Value::Null);
            }
            let field = vals[0].to_display();
            let src = vals[1].to_display();
            Ok(Value::String(crate::sql::date_trunc_text(&field, &src)?))
        }
        "MAKE_DATE" => {
            if vals.iter().any(|v| v.is_null()) {
                return Ok(Value::Null);
            }
            let y = vals[0].as_f64().ok_or_else(|| {
                TakyonicError::Sql("MAKE_DATE year must be numeric".into())
            })? as i64;
            let m = vals[1].as_f64().ok_or_else(|| {
                TakyonicError::Sql("MAKE_DATE month must be numeric".into())
            })? as i64;
            let d = vals[2].as_f64().ok_or_else(|| {
                TakyonicError::Sql("MAKE_DATE day must be numeric".into())
            })? as i64;
            Ok(Value::String(crate::sql::make_date_text(y, m, d)?))
        }
        "MAKE_TIME" => {
            if vals.iter().any(|v| v.is_null()) {
                return Ok(Value::Null);
            }
            let h = vals[0].as_f64().ok_or_else(|| {
                TakyonicError::Sql("MAKE_TIME hour must be numeric".into())
            })? as i64;
            let mi = vals[1].as_f64().ok_or_else(|| {
                TakyonicError::Sql("MAKE_TIME minute must be numeric".into())
            })? as i64;
            let s = vals[2].as_f64().ok_or_else(|| {
                TakyonicError::Sql("MAKE_TIME second must be numeric".into())
            })?;
            Ok(Value::String(crate::sql::make_time_text(h, mi, s)?))
        }
        "MAKE_TIMESTAMP" => {
            if vals.iter().any(|v| v.is_null()) {
                return Ok(Value::Null);
            }
            let y = vals[0].as_f64().ok_or_else(|| {
                TakyonicError::Sql("MAKE_TIMESTAMP year must be numeric".into())
            })? as i64;
            let m = vals[1].as_f64().ok_or_else(|| {
                TakyonicError::Sql("MAKE_TIMESTAMP month must be numeric".into())
            })? as i64;
            let d = vals[2].as_f64().ok_or_else(|| {
                TakyonicError::Sql("MAKE_TIMESTAMP day must be numeric".into())
            })? as i64;
            let h = vals[3].as_f64().ok_or_else(|| {
                TakyonicError::Sql("MAKE_TIMESTAMP hour must be numeric".into())
            })? as i64;
            let mi = vals[4].as_f64().ok_or_else(|| {
                TakyonicError::Sql("MAKE_TIMESTAMP minute must be numeric".into())
            })? as i64;
            let s = vals[5].as_f64().ok_or_else(|| {
                TakyonicError::Sql("MAKE_TIMESTAMP second must be numeric".into())
            })?;
            Ok(Value::String(crate::sql::make_timestamp_text(
                y, m, d, h, mi, s,
            )?))
        }
        "MAKE_INTERVAL" => {
            if vals.iter().any(|v| v.is_null()) {
                return Ok(Value::Null);
            }
            let mut parts = [0i64; 6];
            let mut secs = 0.0f64;
            for (i, v) in vals.iter().enumerate() {
                let n = v.as_f64().ok_or_else(|| {
                    TakyonicError::Sql("MAKE_INTERVAL arguments must be numeric".into())
                })?;
                if i < 6 {
                    parts[i] = n as i64;
                } else {
                    secs = n;
                }
            }
            let total = crate::sql::make_interval_secs(
                parts[0], parts[1], parts[2], parts[3], parts[4], parts[5], secs,
            );
            Ok(Value::String(crate::sql::encode_interval_secs(total)))
        }
        "ISFINITE" => {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            Ok(Value::Bool(crate::sql::is_finite_text(
                &vals[0].to_display(),
            )?))
        }
        "OVERLAPS" => {
            if vals.iter().any(|v| v.is_null()) {
                return Ok(Value::Null);
            }
            Ok(Value::Bool(crate::sql::periods_overlap(
                &vals[0].to_display(),
                &vals[1].to_display(),
                &vals[2].to_display(),
                &vals[3].to_display(),
            )?))
        }
        "TIMEZONE" => {
            if vals[0].is_null() || vals[1].is_null() {
                return Ok(Value::Null);
            }
            // PG: timezone(zone, timestamp) — zone first (opposite of AT TIME ZONE operand order).
            Ok(Value::String(crate::sql::at_time_zone(
                &vals[1].to_display(),
                &vals[0].to_display(),
            )?))
        }
        "DATE_BIN" => {
            if vals.iter().any(|v| v.is_null()) {
                return Ok(Value::Null);
            }
            let stride = crate::sql::interval_arg_secs(&vals[0].to_display())?;
            let source = vals[1].to_display();
            let origin = if vals.len() >= 3 {
                vals[2].to_display()
            } else {
                crate::sql::DATE_BIN_DEFAULT_ORIGIN.to_string()
            };
            Ok(Value::String(crate::sql::date_bin_text(
                stride, &source, &origin,
            )?))
        }
        "JUSTIFY_HOURS" | "JUSTIFY_DAYS" | "JUSTIFY_INTERVAL" => {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            Ok(Value::String(crate::sql::justify_interval_arg(
                &vals[0].to_display(),
            )?))
        }
        "AGE" => {
            if vals.iter().any(|v| v.is_null()) {
                return Ok(Value::Null);
            }
            let (later, earlier) = if vals.len() == 1 {
                (ctx.statement_timestamp.clone(), vals[0].to_display())
            } else {
                (vals[0].to_display(), vals[1].to_display())
            };
            let secs = crate::sql::age_secs(&later, &earlier)?;
            Ok(Value::String(crate::sql::encode_interval_secs(secs)))
        }
        "TO_CHAR" => {
            if vals[0].is_null() || vals[1].is_null() {
                return Ok(Value::Null);
            }
            Ok(Value::String(crate::sql::to_char_timestamp(
                &vals[0].to_display(),
                &vals[1].to_display(),
            )?))
        }
        "TO_TIMESTAMP" => {
            if vals[0].is_null() || vals[1].is_null() {
                return Ok(Value::Null);
            }
            Ok(Value::String(crate::sql::to_timestamp_text(
                &vals[0].to_display(),
                &vals[1].to_display(),
            )?))
        }
        "TO_DATE" => {
            if vals[0].is_null() || vals[1].is_null() {
                return Ok(Value::Null);
            }
            Ok(Value::String(crate::sql::to_date_text(
                &vals[0].to_display(),
                &vals[1].to_display(),
            )?))
        }
        "TO_NUMBER" => {
            if vals[0].is_null() || vals[1].is_null() {
                return Ok(Value::Null);
            }
            let n = crate::sql::to_number_text(
                &vals[0].to_display(),
                &vals[1].to_display(),
            )?;
            if n.fract() == 0.0 && n >= i64::MIN as f64 && n <= i64::MAX as f64 {
                Ok(Value::Int(n as i64))
            } else {
                Ok(Value::Float(n))
            }
        }
        "ARRAY_LENGTH" => {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            let dim = if vals[1].is_null() {
                1i64
            } else {
                vals[1].as_f64().ok_or_else(|| {
                    TakyonicError::Sql("ARRAY_LENGTH dimension must be numeric".into())
                })? as i64
            };
            if dim != 1 {
                return Err(TakyonicError::Sql(
                    "ARRAY_LENGTH only supports dimension 1".into(),
                ));
            }
            let n = match args.first() {
                Some(Expression::Array(items)) => items.len() as i64,
                _ => parse_array_display_len(&vals[0].to_display())? as i64,
            };
            Ok(Value::Int(n))
        }
        "CARDINALITY" => {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            let n = match args.first() {
                Some(Expression::Array(items)) => items.len() as i64,
                _ => parse_array_display_len(&vals[0].to_display())? as i64,
            };
            Ok(Value::Int(n))
        }
        "ARRAY_CAT" => {
            if vals.iter().any(|v| v.is_null()) {
                return Ok(Value::Null);
            }
            let left = match args.first() {
                Some(e) => eval_array_elements(e, row, ctx)?,
                None => return Err(TakyonicError::Sql("ARRAY_CAT missing args".into())),
            };
            let right = match args.get(1) {
                Some(e) => eval_array_elements(e, row, ctx)?,
                None => return Err(TakyonicError::Sql("ARRAY_CAT missing args".into())),
            };
            let mut parts = Vec::with_capacity(left.len() + right.len());
            for v in left.into_iter().chain(right) {
                parts.push(value_to_field(&v));
            }
            Ok(Value::String(format!("[{}]", parts.join(","))))
        }
        "STRING_TO_ARRAY" => {
            if vals[0].is_null() || vals[1].is_null() {
                return Ok(Value::Null);
            }
            let null_string = match vals.get(2) {
                None => None,
                Some(v) if v.is_null() => None,
                Some(v) => Some(v.to_display()),
            };
            Ok(Value::String(crate::sql::string_to_array(
                &vals[0].to_display(),
                &vals[1].to_display(),
                null_string.as_deref(),
            )))
        }
        "SPLIT_PART" => {
            if vals[0].is_null() || vals[1].is_null() || vals[2].is_null() {
                return Ok(Value::Null);
            }
            let field = vals[2].as_f64().ok_or_else(|| {
                TakyonicError::Sql("split_part field must be numeric".into())
            })? as i64;
            Ok(Value::String(crate::sql::split_part(
                &vals[0].to_display(),
                &vals[1].to_display(),
                field,
            )?))
        }
        "REGEXP_SPLIT_TO_ARRAY" => {
            if vals[0].is_null() || vals[1].is_null() {
                return Ok(Value::Null);
            }
            let flags = match vals.get(2) {
                None => None,
                Some(v) if v.is_null() => None,
                Some(v) => Some(v.to_display()),
            };
            Ok(Value::String(crate::sql::regexp_split_to_array(
                &vals[0].to_display(),
                &vals[1].to_display(),
                flags.as_deref(),
            )?))
        }
        "ARRAY_TO_STRING" => {
            if vals[0].is_null() || vals[1].is_null() {
                return Ok(Value::Null);
            }
            let elems = eval_array_elements(&args[0], row, ctx)?;
            let null_string = match vals.get(2) {
                None => None,
                Some(v) if v.is_null() => None,
                Some(v) => Some(v.to_display()),
            };
            Ok(Value::String(crate::sql::array_to_string(
                &elems,
                &vals[1].to_display(),
                null_string.as_deref(),
            )))
        }
        "ARRAY_CONTAINS" | "ARRAY_CONTAINED_BY" | "ARRAY_OVERLAP" => {
            if vals.iter().any(|v| v.is_null()) {
                return Ok(Value::Null);
            }
            let left = eval_array_elements(&args[0], row, ctx)?;
            let right = eval_array_elements(&args[1], row, ctx)?;
            let left_keys: Vec<String> = left.iter().map(|v| v.to_display()).collect();
            let right_keys: Vec<String> = right.iter().map(|v| v.to_display()).collect();
            let ok = match name {
                "ARRAY_CONTAINS" => right_keys.iter().all(|r| left_keys.iter().any(|l| l == r)),
                "ARRAY_CONTAINED_BY" => {
                    left_keys.iter().all(|l| right_keys.iter().any(|r| r == l))
                }
                "ARRAY_OVERLAP" => left_keys.iter().any(|l| right_keys.iter().any(|r| r == l)),
                _ => unreachable!(),
            };
            Ok(Value::Bool(ok))
        }
        "JSON_GET" | "JSON_GET_TEXT" => {
            if vals[0].is_null() || vals[1].is_null() {
                return Ok(Value::Null);
            }
            let as_text = name == "JSON_GET_TEXT";
            crate::sql::json_get(&vals[0].to_display(), &vals[1], as_text)
        }
        "JSON_TYPEOF" | "JSONB_TYPEOF" => {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            Ok(Value::String(crate::sql::json_typeof(
                &vals[0].to_display(),
            )?))
        }
        "JSON_PATH_GET" | "JSON_PATH_GET_TEXT" => {
            if vals[0].is_null() || vals[1].is_null() {
                return Ok(Value::Null);
            }
            let as_text = name == "JSON_PATH_GET_TEXT";
            crate::sql::json_path_get(&vals[0].to_display(), &vals[1].to_display(), as_text)
        }
        "JSON_CONTAINS" | "JSON_CONTAINED_BY" => {
            if vals.iter().any(|v| v.is_null()) {
                return Ok(Value::Null);
            }
            let (hay, needle) = if name == "JSON_CONTAINS" {
                (vals[0].to_display(), vals[1].to_display())
            } else {
                (vals[1].to_display(), vals[0].to_display())
            };
            Ok(Value::Bool(crate::sql::json_contains(&hay, &needle)?))
        }
        "JSON_CONCAT" => {
            if vals.iter().any(|v| v.is_null()) {
                return Ok(Value::Null);
            }
            Ok(Value::String(crate::sql::json_concat(
                &vals[0].to_display(),
                &vals[1].to_display(),
            )?))
        }
        "JSONB_SET" | "JSON_SET" => {
            if vals[0].is_null() || vals[1].is_null() || vals[2].is_null() {
                return Ok(Value::Null);
            }
            let create_missing = match vals.get(3) {
                None => true,
                Some(v) if v.is_null() => true,
                Some(v) => v.is_truthy(),
            };
            Ok(Value::String(crate::sql::jsonb_set(
                &vals[0].to_display(),
                &vals[1].to_display(),
                &vals[2].to_display(),
                create_missing,
            )?))
        }
        "JSONB_BUILD_OBJECT" | "JSON_BUILD_OBJECT" => {
            let mut pairs = Vec::with_capacity(vals.len() / 2);
            let mut i = 0;
            while i + 1 < vals.len() {
                pairs.push((vals[i].clone(), vals[i + 1].clone()));
                i += 2;
            }
            Ok(Value::String(crate::sql::jsonb_build_object(&pairs)?))
        }
        "JSONB_BUILD_ARRAY" | "JSON_BUILD_ARRAY" => {
            Ok(Value::String(crate::sql::jsonb_build_array(&vals)))
        }
        "JSONB_PRETTY" | "JSON_PRETTY" => {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            Ok(Value::String(crate::sql::jsonb_pretty(
                &vals[0].to_display(),
            )?))
        }
        "JSON_DELETE" => {
            if vals[0].is_null() || vals[1].is_null() {
                return Ok(Value::Null);
            }
            Ok(Value::String(crate::sql::json_delete(
                &vals[0].to_display(),
                &vals[1],
            )?))
        }
        "JSON_PATH_DELETE" => {
            if vals[0].is_null() || vals[1].is_null() {
                return Ok(Value::Null);
            }
            Ok(Value::String(crate::sql::json_path_delete(
                &vals[0].to_display(),
                &vals[1].to_display(),
            )?))
        }
        "JSONB_INSERT" | "JSON_INSERT" => {
            if vals[0].is_null() || vals[1].is_null() || vals[2].is_null() {
                return Ok(Value::Null);
            }
            let insert_after = match vals.get(3) {
                None => false,
                Some(v) if v.is_null() => false,
                Some(v) => v.is_truthy(),
            };
            Ok(Value::String(crate::sql::jsonb_insert(
                &vals[0].to_display(),
                &vals[1].to_display(),
                &vals[2].to_display(),
                insert_after,
            )?))
        }
        "JSONB_STRIP_NULLS" | "JSON_STRIP_NULLS" => {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            Ok(Value::String(crate::sql::jsonb_strip_nulls(
                &vals[0].to_display(),
            )?))
        }
        "TO_JSON" | "TO_JSONB" | "ARRAY_TO_JSON" => {
            Ok(Value::String(crate::sql::to_json(&vals[0])))
        }
        "JSON_ARRAY_LENGTH" | "JSONB_ARRAY_LENGTH" => {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            Ok(Value::Int(crate::sql::json_array_length(
                &vals[0].to_display(),
            )?))
        }
        "IS_JSON" | "JSON_IS_VALID" => {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            Ok(Value::Bool(crate::sql::is_json(&vals[0].to_display())))
        }
        "JSONB_PATH_EXISTS" | "JSON_PATH_EXISTS" => {
            if vals[0].is_null() || vals[1].is_null() {
                return Ok(Value::Null);
            }
            Ok(Value::Bool(crate::sql::json_path_exists(
                &vals[0].to_display(),
                &vals[1].to_display(),
            )?))
        }
        "JSONB_EXTRACT_PATH"
        | "JSON_EXTRACT_PATH"
        | "JSONB_EXTRACT_PATH_TEXT"
        | "JSON_EXTRACT_PATH_TEXT" => {
            if vals[0].is_null() || vals.iter().skip(1).any(|v| v.is_null()) {
                return Ok(Value::Null);
            }
            let segs: Vec<String> = vals.iter().skip(1).map(|v| v.to_display()).collect();
            let as_text = name.ends_with("_TEXT");
            crate::sql::json_extract_path(&vals[0].to_display(), &segs, as_text)
        }
        other => Err(TakyonicError::Sql(format!(
            "unsupported scalar function `{other}`"
        ))),
    }
}

fn eval_row_to_json(
    args: &[Expression],
    row: &Record,
    ctx: &ExecutionContext,
) -> Result<Value> {
    if args.is_empty() {
        let fields: Vec<(String, Value)> = row
            .fields
            .iter()
            .map(|(k, v)| (k.clone(), Value::from_text(v)))
            .collect();
        return Ok(Value::String(crate::sql::row_to_json_object(&fields)));
    }
    let mut fields = Vec::with_capacity(args.len());
    for (i, a) in args.iter().enumerate() {
        let key = match a {
            Expression::Column(c) => c.clone(),
            _ => format!("f{}", i + 1),
        };
        fields.push((key, evaluate(a, row, ctx)?));
    }
    Ok(Value::String(crate::sql::row_to_json_object(&fields)))
}

fn eval_array_elements(
    expr: &Expression,
    row: &Record,
    ctx: &ExecutionContext,
) -> Result<Vec<Value>> {
    match expr {
        Expression::Array(items) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                out.push(evaluate(item, row, ctx)?);
            }
            Ok(out)
        }
        Expression::ScalarFunction { name, args } if name == "ARRAY_CAT" && args.len() == 2 => {
            let mut out = eval_array_elements(&args[0], row, ctx)?;
            out.extend(eval_array_elements(&args[1], row, ctx)?);
            Ok(out)
        }
        other => {
            let v = evaluate(other, row, ctx)?;
            if v.is_null() {
                return Ok(Vec::new());
            }
            parse_array_display_elements(&v.to_display())
        }
    }
}

fn parse_array_display_len(s: &str) -> Result<usize> {
    Ok(parse_array_display_elements(s)?.len())
}

fn parse_array_display_elements(s: &str) -> Result<Vec<Value>> {
    let s = s.trim();
    let inner = s
        .strip_prefix('[')
        .and_then(|x| x.strip_suffix(']'))
        .ok_or_else(|| TakyonicError::Sql(format!("not an array value: `{s}`")))?;
    if inner.is_empty() {
        return Ok(Vec::new());
    }
    Ok(inner
        .split(',')
        .map(|p| Value::from_text(p.trim()))
        .collect())
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
        Expression::Like {
            expr: inner,
            pattern,
            case_insensitive,
            negated,
            any,
            escape,
        } => Ok(Expression::Like {
            expr: Box::new(rewrite_uncorrelated_subqueries(*inner, ctx, txn)?),
            pattern: Box::new(rewrite_uncorrelated_subqueries(*pattern, ctx, txn)?),
            case_insensitive,
            negated,
            any,
            escape,
        }),
        Expression::SimilarTo {
            expr: inner,
            pattern,
            negated,
            escape,
        } => Ok(Expression::SimilarTo {
            expr: Box::new(rewrite_uncorrelated_subqueries(*inner, ctx, txn)?),
            pattern: Box::new(rewrite_uncorrelated_subqueries(*pattern, ctx, txn)?),
            negated,
            escape,
        }),
        Expression::RegexMatch {
            expr: inner,
            pattern,
            case_insensitive,
            negated,
        } => Ok(Expression::RegexMatch {
            expr: Box::new(rewrite_uncorrelated_subqueries(*inner, ctx, txn)?),
            pattern: Box::new(rewrite_uncorrelated_subqueries(*pattern, ctx, txn)?),
            case_insensitive,
            negated,
        }),
        Expression::AtTimeZone {
            timestamp,
            time_zone,
        } => Ok(Expression::AtTimeZone {
            timestamp: Box::new(rewrite_uncorrelated_subqueries(*timestamp, ctx, txn)?),
            time_zone: Box::new(rewrite_uncorrelated_subqueries(*time_zone, ctx, txn)?),
        }),
        Expression::Case {
            when_then,
            else_result,
        } => {
            let mut arms = Vec::with_capacity(when_then.len());
            for (cond, result) in when_then {
                arms.push((
                    rewrite_uncorrelated_subqueries(cond, ctx, txn)?,
                    rewrite_uncorrelated_subqueries(result, ctx, txn)?,
                ));
            }
            let else_result = match else_result {
                Some(e) => Some(Box::new(rewrite_uncorrelated_subqueries(*e, ctx, txn)?)),
                None => None,
            };
            Ok(Expression::Case {
                when_then: arms,
                else_result,
            })
        },
        Expression::IsNull { expr, negated } => Ok(Expression::IsNull {
            expr: Box::new(rewrite_uncorrelated_subqueries(*expr, ctx, txn)?),
            negated,
        }),
        Expression::IsBoolTest {
            expr,
            test,
            negated,
        } => Ok(Expression::IsBoolTest {
            expr: Box::new(rewrite_uncorrelated_subqueries(*expr, ctx, txn)?),
            test,
            negated,
        }),
        Expression::IsDistinctFrom {
            left,
            right,
            negated,
        } => Ok(Expression::IsDistinctFrom {
            left: Box::new(rewrite_uncorrelated_subqueries(*left, ctx, txn)?),
            right: Box::new(rewrite_uncorrelated_subqueries(*right, ctx, txn)?),
            negated,
        }),
        Expression::QuantifiedCmp {
            left,
            op,
            right,
            quantifier,
        } => Ok(Expression::QuantifiedCmp {
            left: Box::new(rewrite_uncorrelated_subqueries(*left, ctx, txn)?),
            op,
            right: Box::new(rewrite_uncorrelated_subqueries(*right, ctx, txn)?),
            quantifier,
        }),
        Expression::Not { expr } => Ok(Expression::Not {
            expr: Box::new(rewrite_uncorrelated_subqueries(*expr, ctx, txn)?),
        }),
        Expression::Coalesce(args) => {
            let mut out = Vec::with_capacity(args.len());
            for a in args {
                out.push(rewrite_uncorrelated_subqueries(a, ctx, txn)?);
            }
            Ok(Expression::Coalesce(out))
        }
        Expression::Cast {
            expr,
            target,
            try_cast,
        } => Ok(Expression::Cast {
            expr: Box::new(rewrite_uncorrelated_subqueries(*expr, ctx, txn)?),
            target,
            try_cast,
        }),
        Expression::NullIf { left, right } => Ok(Expression::NullIf {
            left: Box::new(rewrite_uncorrelated_subqueries(*left, ctx, txn)?),
            right: Box::new(rewrite_uncorrelated_subqueries(*right, ctx, txn)?),
        }),
        Expression::ScalarFunction { name, args } => {
            let mut out = Vec::with_capacity(args.len());
            for a in args {
                out.push(rewrite_uncorrelated_subqueries(a, ctx, txn)?);
            }
            Ok(Expression::ScalarFunction { name, args: out })
        }
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
        // Correlated: leave intact for per-row [`ApplyExec`] evaluation.
        Expression::InSubquery {
            correlated: true, ..
        }
        | Expression::Exists {
            correlated: true, ..
        }
        | Expression::ScalarSubquery {
            correlated: true, ..
        } => Ok(expr),
        other => Ok(other),
    }
}

fn predicate_has_correlated(expr: &Expression) -> bool {
    match expr {
        Expression::InSubquery {
            correlated: true, ..
        }
        | Expression::Exists {
            correlated: true, ..
        }
        | Expression::ScalarSubquery {
            correlated: true, ..
        } => true,
        Expression::BinaryOp { left, right, .. }
        | Expression::And { left, right }
        | Expression::Or { left, right }
        | Expression::Arith { left, right, .. }
        | Expression::Like {
            expr: left,
            pattern: right,
            ..
        }
        | Expression::SimilarTo {
            expr: left,
            pattern: right,
            ..
        }
        | Expression::RegexMatch {
            expr: left,
            pattern: right,
            ..
        }
        | Expression::AtTimeZone {
            timestamp: left,
            time_zone: right,
        } => {
            predicate_has_correlated(left) || predicate_has_correlated(right)
        }
        Expression::Case {
            when_then,
            else_result,
        } => {
            when_then.iter().any(|(c, r)| {
                predicate_has_correlated(c) || predicate_has_correlated(r)
            }) || else_result
                .as_ref()
                .is_some_and(|e| predicate_has_correlated(e))
        }
        Expression::IsNull { expr, .. }
        | Expression::IsBoolTest { expr, .. }
        | Expression::Cast { expr, .. }
        | Expression::Not { expr } => {
            predicate_has_correlated(expr)
        }
        Expression::NullIf { left, right }
        | Expression::IsDistinctFrom { left, right, .. }
        | Expression::QuantifiedCmp { left, right, .. } => {
            predicate_has_correlated(left) || predicate_has_correlated(right)
        }
        Expression::ScalarFunction { args, .. } => args.iter().any(predicate_has_correlated),
        Expression::Coalesce(args) => args.iter().any(predicate_has_correlated),
        Expression::InList { expr, .. } => predicate_has_correlated(expr),
        _ => false,
    }
}

/// Bind [`Expression::OuterRef`] nodes to literals from the outer row, then
/// evaluate correlated IN/EXISTS/scalar subqueries for that row.
fn evaluate_bool_correlated(
    expr: &Expression,
    row: &Record,
    ctx: &ExecutionContext,
    txn: &mut Transaction,
) -> Result<bool> {
    match expr {
        Expression::And { left, right } => Ok(evaluate_bool_correlated(left, row, ctx, txn)?
            && evaluate_bool_correlated(right, row, ctx, txn)?),
        Expression::Or { left, right } => Ok(evaluate_bool_correlated(left, row, ctx, txn)?
            || evaluate_bool_correlated(right, row, ctx, txn)?),
        Expression::Not { expr } => Ok(!evaluate_bool_correlated(expr, row, ctx, txn)?),
        Expression::Exists {
            subquery,
            negated,
            correlated: true,
        } => {
            let bound = bind_outer_refs_plan(subquery, row)?;
            let rows = execute_subquery_rows(&bound, ctx, txn)?;
            let exists = !rows.is_empty();
            Ok(if *negated { !exists } else { exists })
        }
        Expression::InSubquery {
            expr: inner,
            subquery,
            value_column,
            negated,
            correlated: true,
        } => {
            let needle = evaluate(inner, row, ctx)?;
            if needle.is_null() {
                return Ok(false); // WHERE: UNKNOWN does not match
            }
            let bound = bind_outer_refs_plan(subquery, row)?;
            let list = execute_subquery_column(&bound, value_column, ctx, txn)?;
            let mut saw_null = false;
            let mut found = false;
            for v in &list {
                if v.is_null() {
                    saw_null = true;
                    continue;
                }
                if values_equal(&needle, v) {
                    found = true;
                    break;
                }
            }
            if found {
                Ok(!*negated)
            } else if saw_null {
                Ok(false) // WHERE: UNKNOWN
            } else {
                Ok(*negated)
            }
        }
        Expression::BinaryOp { left, op, right } => {
            // Scalar subquery may sit on either side.
            let lv = evaluate_value_correlated(left, row, ctx, txn)?;
            let rv = evaluate_value_correlated(right, row, ctx, txn)?;
            if lv.is_null() || rv.is_null() {
                return Ok(false); // WHERE: UNKNOWN does not match
            }
            Ok(compare_sql_values(&lv, *op, &rv))
        }
        other => evaluate_bool(other, row, ctx),
    }
}

fn evaluate_value_correlated(
    expr: &Expression,
    row: &Record,
    ctx: &ExecutionContext,
    txn: &mut Transaction,
) -> Result<Value> {
    match expr {
        Expression::ScalarSubquery {
            subquery,
            value_column,
            correlated: true,
        } => {
            let bound = bind_outer_refs_plan(subquery, row)?;
            let list = execute_subquery_column(&bound, value_column, ctx, txn)?;
            if list.len() > 1 {
                return Err(TakyonicError::Sql(
                    "scalar subquery returned more than one row".into(),
                ));
            }
            Ok(list.into_iter().next().unwrap_or(Value::Null))
        }
        other => evaluate(other, row, ctx),
    }
}

fn bind_outer_refs_plan(plan: &LogicalPlan, outer: &Record) -> Result<LogicalPlan> {
    Ok(match plan {
        LogicalPlan::Select {
            table,
            filters,
            predicate,
        } => LogicalPlan::Select {
            table: table.clone(),
            filters: filters.clone(),
            predicate: predicate
                .as_ref()
                .map(|p| bind_outer_refs_expr(p, outer))
                .transpose()?,
        },
        LogicalPlan::Filter { input, predicate } => LogicalPlan::Filter {
            input: Box::new(bind_outer_refs_plan(input, outer)?),
            predicate: bind_outer_refs_expr(predicate, outer)?,
        },
        LogicalPlan::Join {
            left,
            right,
            on,
            join_type,
        } => LogicalPlan::Join {
            left: Box::new(bind_outer_refs_plan(left, outer)?),
            right: Box::new(bind_outer_refs_plan(right, outer)?),
            on: bind_outer_refs_expr(on, outer)?,
            join_type: *join_type,
        },
        LogicalPlan::Aggregate {
            input,
            group_exprs,
            aggr_exprs,
        } => LogicalPlan::Aggregate {
            input: Box::new(bind_outer_refs_plan(input, outer)?),
            group_exprs: group_exprs
                .iter()
                .map(|e| bind_outer_refs_expr(e, outer))
                .collect::<Result<Vec<_>>>()?,
            aggr_exprs: aggr_exprs
                .iter()
                .map(|e| bind_outer_refs_expr(e, outer))
                .collect::<Result<Vec<_>>>()?,
        },
        LogicalPlan::Sort { input, exprs } => LogicalPlan::Sort {
            input: Box::new(bind_outer_refs_plan(input, outer)?),
            exprs: exprs.clone(),
        },
        LogicalPlan::Limit {
            input,
            skip,
            fetch,
            with_ties,
            ties_order,
        } => LogicalPlan::Limit {
            input: Box::new(bind_outer_refs_plan(input, outer)?),
            skip: *skip,
            fetch: *fetch,
            with_ties: *with_ties,
            ties_order: ties_order
                .iter()
                .map(|s| {
                    Ok(SortExpr {
                        expr: bind_outer_refs_expr(&s.expr, outer)?,
                        asc: s.asc,
                        nulls_first: s.nulls_first,
                    })
                })
                .collect::<Result<Vec<_>>>()?,
        },
        LogicalPlan::Project { input, columns } => LogicalPlan::Project {
            input: Box::new(bind_outer_refs_plan(input, outer)?),
            columns: columns
                .iter()
                .map(|(n, e)| Ok((n.clone(), bind_outer_refs_expr(e, outer)?)))
                .collect::<Result<Vec<_>>>()?,
        },
        LogicalPlan::Window { input, calls } => LogicalPlan::Window {
            input: Box::new(bind_outer_refs_plan(input, outer)?),
            calls: calls
                .iter()
                .map(|c| {
                    Ok(crate::sql::WindowCall {
                        output_column: c.output_column.clone(),
                        kind: c.kind,
                        partition_by: c
                            .partition_by
                            .iter()
                            .map(|e| bind_outer_refs_expr(e, outer))
                            .collect::<Result<Vec<_>>>()?,
                        order_by: c
                            .order_by
                            .iter()
                            .map(|s| {
                                Ok(crate::sql::SortExpr {
                                    expr: bind_outer_refs_expr(&s.expr, outer)?,
                                    asc: s.asc,
                                    nulls_first: s.nulls_first,
                                })
                            })
                            .collect::<Result<Vec<_>>>()?,
                        value: c
                            .value
                            .as_ref()
                            .map(|e| bind_outer_refs_expr(e, outer))
                            .transpose()?,
                        offset: c.offset,
                        default_value: c
                            .default_value
                            .as_ref()
                            .map(|e| bind_outer_refs_expr(e, outer))
                            .transpose()?,
                        frame: c.frame.clone(),
                        filter: c
                            .filter
                            .as_ref()
                            .map(|e| bind_outer_refs_expr(e, outer))
                            .transpose()?,
                        ignore_nulls: c.ignore_nulls,
                    })
                })
                .collect::<Result<Vec<_>>>()?,
        },
        LogicalPlan::SubqueryAlias { alias, input } => LogicalPlan::SubqueryAlias {
            alias: alias.clone(),
            input: Box::new(bind_outer_refs_plan(input, outer)?),
        },
        other => other.clone(),
    })
}

fn bind_outer_refs_expr(expr: &Expression, outer: &Record) -> Result<Expression> {
    Ok(match expr {
        Expression::OuterRef(name) => {
            let text = outer.get(name).ok_or_else(|| {
                TakyonicError::Sql(format!("outer reference `{name}` missing on outer row"))
            })?;
            Expression::Literal(text.to_string())
        }
        Expression::BinaryOp { left, op, right } => Expression::BinaryOp {
            left: Box::new(bind_outer_refs_expr(left, outer)?),
            op: *op,
            right: Box::new(bind_outer_refs_expr(right, outer)?),
        },
        Expression::And { left, right } => Expression::And {
            left: Box::new(bind_outer_refs_expr(left, outer)?),
            right: Box::new(bind_outer_refs_expr(right, outer)?),
        },
        Expression::Or { left, right } => Expression::Or {
            left: Box::new(bind_outer_refs_expr(left, outer)?),
            right: Box::new(bind_outer_refs_expr(right, outer)?),
        },
        Expression::Arith { left, op, right } => Expression::Arith {
            left: Box::new(bind_outer_refs_expr(left, outer)?),
            op: *op,
            right: Box::new(bind_outer_refs_expr(right, outer)?),
        },
        Expression::Like {
            expr: inner,
            pattern,
            case_insensitive,
            negated,
            any,
            escape,
        } => Expression::Like {
            expr: Box::new(bind_outer_refs_expr(inner, outer)?),
            pattern: Box::new(bind_outer_refs_expr(pattern, outer)?),
            case_insensitive: *case_insensitive,
            negated: *negated,
            any: *any,
            escape: *escape,
        },
        Expression::SimilarTo {
            expr: inner,
            pattern,
            negated,
            escape,
        } => Expression::SimilarTo {
            expr: Box::new(bind_outer_refs_expr(inner, outer)?),
            pattern: Box::new(bind_outer_refs_expr(pattern, outer)?),
            negated: *negated,
            escape: *escape,
        },
        Expression::RegexMatch {
            expr: inner,
            pattern,
            case_insensitive,
            negated,
        } => Expression::RegexMatch {
            expr: Box::new(bind_outer_refs_expr(inner, outer)?),
            pattern: Box::new(bind_outer_refs_expr(pattern, outer)?),
            case_insensitive: *case_insensitive,
            negated: *negated,
        },
        Expression::AtTimeZone {
            timestamp,
            time_zone,
        } => Expression::AtTimeZone {
            timestamp: Box::new(bind_outer_refs_expr(timestamp, outer)?),
            time_zone: Box::new(bind_outer_refs_expr(time_zone, outer)?),
        },
        Expression::Case {
            when_then,
            else_result,
        } => {
            let mut arms = Vec::with_capacity(when_then.len());
            for (cond, result) in when_then {
                arms.push((
                    bind_outer_refs_expr(cond, outer)?,
                    bind_outer_refs_expr(result, outer)?,
                ));
            }
            let else_result = match else_result {
                Some(e) => Some(Box::new(bind_outer_refs_expr(e, outer)?)),
                None => None,
            };
            Expression::Case {
                when_then: arms,
                else_result,
            }
        }
        Expression::IsNull { expr, negated } => Expression::IsNull {
            expr: Box::new(bind_outer_refs_expr(expr, outer)?),
            negated: *negated,
        },
        Expression::IsBoolTest {
            expr,
            test,
            negated,
        } => Expression::IsBoolTest {
            expr: Box::new(bind_outer_refs_expr(expr, outer)?),
            test: *test,
            negated: *negated,
        },
        Expression::IsDistinctFrom {
            left,
            right,
            negated,
        } => Expression::IsDistinctFrom {
            left: Box::new(bind_outer_refs_expr(left, outer)?),
            right: Box::new(bind_outer_refs_expr(right, outer)?),
            negated: *negated,
        },
        Expression::QuantifiedCmp {
            left,
            op,
            right,
            quantifier,
        } => Expression::QuantifiedCmp {
            left: Box::new(bind_outer_refs_expr(left, outer)?),
            op: *op,
            right: Box::new(bind_outer_refs_expr(right, outer)?),
            quantifier: *quantifier,
        },
        Expression::Not { expr } => Expression::Not {
            expr: Box::new(bind_outer_refs_expr(expr, outer)?),
        },
        Expression::Coalesce(args) => {
            let mut out = Vec::with_capacity(args.len());
            for a in args {
                out.push(bind_outer_refs_expr(a, outer)?);
            }
            Expression::Coalesce(out)
        }
        Expression::Cast {
            expr,
            target,
            try_cast,
        } => Expression::Cast {
            expr: Box::new(bind_outer_refs_expr(expr, outer)?),
            target: *target,
            try_cast: *try_cast,
        },
        Expression::NullIf { left, right } => Expression::NullIf {
            left: Box::new(bind_outer_refs_expr(left, outer)?),
            right: Box::new(bind_outer_refs_expr(right, outer)?),
        },
        Expression::ScalarFunction { name, args } => {
            let mut out = Vec::with_capacity(args.len());
            for a in args {
                out.push(bind_outer_refs_expr(a, outer)?);
            }
            Expression::ScalarFunction {
                name: name.clone(),
                args: out,
            }
        }
        Expression::InList {
            expr: inner,
            list,
            negated,
        } => Expression::InList {
            expr: Box::new(bind_outer_refs_expr(inner, outer)?),
            list: list.clone(),
            negated: *negated,
        },
        other => other.clone(),
    })
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

/// SQL `AND` under three-valued logic (`NULL` = UNKNOWN).
fn sql_and_3vl(left: &Value, right: &Value) -> Value {
    match (three_valued_bool(left), three_valued_bool(right)) {
        (Some(false), _) | (_, Some(false)) => Value::Bool(false),
        (Some(true), Some(true)) => Value::Bool(true),
        _ => Value::Null,
    }
}

/// SQL `OR` under three-valued logic (`NULL` = UNKNOWN).
fn sql_or_3vl(left: &Value, right: &Value) -> Value {
    match (three_valued_bool(left), three_valued_bool(right)) {
        (Some(true), _) | (_, Some(true)) => Value::Bool(true),
        (Some(false), Some(false)) => Value::Bool(false),
        _ => Value::Null,
    }
}

fn three_valued_bool(v: &Value) -> Option<bool> {
    if v.is_null() {
        None
    } else {
        Some(v.is_truthy())
    }
}

/// `text LIKE ANY (patterns)` under SQL three-valued logic.
fn eval_like_any(
    text: &str,
    elems: &[Value],
    case_insensitive: bool,
    escape: Option<char>,
) -> Option<bool> {
    let mut saw_true = false;
    let mut saw_unknown = false;
    for e in elems {
        if e.is_null() {
            saw_unknown = true;
            continue;
        }
        let pat = value_to_field(e);
        if crate::sql::sql_like_match(text, &pat, case_insensitive, escape) {
            saw_true = true;
        }
    }
    if saw_true {
        Some(true)
    } else if saw_unknown {
        None
    } else {
        Some(false)
    }
}

/// `left op ANY|ALL (elems)` under SQL three-valued logic.
fn eval_quantified_cmp(
    left: &Value,
    op: FilterOp,
    elems: &[Value],
    quantifier: crate::sql::Quantifier,
) -> Value {
    let mut saw_true = false;
    let mut saw_false = false;
    let mut saw_unknown = false;
    for e in elems {
        let cmp = if left.is_null() || e.is_null() {
            None
        } else {
            Some(compare_sql_values(left, op, e))
        };
        match cmp {
            Some(true) => saw_true = true,
            Some(false) => saw_false = true,
            None => saw_unknown = true,
        }
    }
    match quantifier {
        crate::sql::Quantifier::Any => {
            if saw_true {
                Value::Bool(true)
            } else if saw_unknown {
                Value::Null
            } else {
                Value::Bool(false)
            }
        }
        crate::sql::Quantifier::All => {
            if saw_false {
                Value::Bool(false)
            } else if saw_unknown {
                Value::Null
            } else {
                Value::Bool(true)
            }
        }
    }
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
    fn merge_join_exec_matches_sorted_inputs() {
        let users = vec![
            Record::new().set("id", "1").set("name", "Ada"),
            Record::new().set("id", "2").set("name", "Bob"),
            Record::new().set("id", "3").set("name", "Cy"),
        ];
        let orders = vec![
            Record::new().set("order_id", "10").set("user_id", "1"),
            Record::new().set("order_id", "11").set("user_id", "1"),
            Record::new().set("order_id", "20").set("user_id", "3"),
        ];
        let mut join = MergeJoinExec::from_sorted_rows(
            users,
            orders,
            Expression::Column("id".into()),
            Expression::Column("user_id".into()),
        );
        let mut rows = Vec::new();
        while let Some(r) = join.next_row().unwrap() {
            rows.push(r);
        }
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].get("name"), Some("Ada"));
        assert_eq!(rows[0].get("order_id"), Some("10"));
        assert_eq!(rows[1].get("order_id"), Some("11"));
        assert_eq!(rows[2].get("name"), Some("Cy"));
        assert_eq!(rows[2].get("order_id"), Some("20"));
    }

    #[test]
    fn equi_join_prefers_merge_when_both_sides_sorted() {
        let plan = LogicalPlan::Join {
            left: Box::new(LogicalPlan::Sort {
                input: Box::new(LogicalPlan::Select {
                    table: "users".into(),
                    filters: vec![],
                    predicate: None,
                }),
                exprs: vec![SortExpr::asc(Expression::Column("id".into()))],
            }),
            right: Box::new(LogicalPlan::Sort {
                input: Box::new(LogicalPlan::Select {
                    table: "orders".into(),
                    filters: vec![],
                    predicate: None,
                }),
                exprs: vec![SortExpr::asc(Expression::Column("user_id".into()))],
            }),
            on: Expression::BinaryOp {
                left: Box::new(Expression::Column("id".into())),
                op: FilterOp::Eq,
                right: Box::new(Expression::Column("user_id".into())),
            },
            join_type: JoinType::Inner,
        };
        let physical = optimize(&plan).unwrap();
        let text = explain_physical(&physical);
        assert!(
            text.contains("MergeJoin"),
            "EXPLAIN must show MergeJoin, got:\n{text}"
        );
        match physical {
            PhysicalPlan::MergeJoin {
                left_key,
                right_key,
                join_type,
                ..
            } => {
                assert_eq!(join_type, JoinType::Inner);
                assert_eq!(left_key, Expression::Column("id".into()));
                assert_eq!(right_key, Expression::Column("user_id".into()));
            }
            other => panic!("expected MergeJoin, got {other:?}"),
        }
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
    fn left_outer_hash_join_preserves_unmatched_left() {
        let (engine, root) = temp_engine("left-hashjoin");
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
                "INSERT INTO orders (order_id, user_id) VALUES (10, 1), (20, 2)",
            )
            .unwrap(),
            &ctx,
            engine.begin().unwrap(),
        )
        .unwrap();

        let sql =
            "SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id";
        let plan = LogicalPlanner::plan(sql).unwrap();
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
                    join_type: JoinType::Left,
                    ..
                }
            ),
            "expected Left HashJoin, got {physical:?}"
        );

        let mut txn = engine.begin().unwrap();
        let out = execute_plan(&plan, &ctx, &mut txn).unwrap();
        txn.abort();

        // Ada, Bob matched + Cy unmatched = 3 rows.
        assert_eq!(out.len(), 3);
        let cy = out
            .iter()
            .find(|r| r.get("name") == Some("Cy"))
            .expect("Cy must appear from LEFT JOIN");
        assert_eq!(cy.get("order_id").unwrap_or(""), "");
        assert_eq!(cy.get("user_id").unwrap_or(""), "");

        engine.close().unwrap();
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn left_outer_nested_loop_preserves_unmatched_left() {
        let users = vec![
            Record::new().set("id", "1").set("name", "Ada"),
            Record::new().set("id", "2").set("name", "Bob"),
        ];
        let orders = vec![Record::new().set("order_id", "10").set("user_id", "1")];
        let condition = Expression::BinaryOp {
            left: Box::new(Expression::Column("id".into())),
            op: FilterOp::Eq,
            right: Box::new(Expression::Column("user_id".into())),
        };
        let mut join = NestedLoopJoin::from_rows_with_type(
            users,
            orders,
            condition,
            JoinType::Left,
        );
        let out = collect_rows(&mut join).unwrap();
        assert_eq!(out.len(), 2);
        let bob = out
            .iter()
            .find(|r| r.get("name") == Some("Bob"))
            .expect("Bob unmatched left row");
        assert_eq!(bob.get("order_id").unwrap_or(""), "");
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
    fn correlated_exists_filters_per_outer_row() {
        let (engine, root) = temp_engine("outerref");
        engine
            .register_table(TableSchema::new("employees", "id", vec![]))
            .unwrap();
        engine
            .register_table(TableSchema::new("dept_budget", "dept", vec![]))
            .unwrap();
        let ctx = ExecutionContext::new();

        execute_plan_autocommit(
            &LogicalPlanner::plan(
                "INSERT INTO employees (id, dept) VALUES \
                 (1, 'Engineering'), (2, 'Sales'), (3, 'Engineering')",
            )
            .unwrap(),
            &ctx,
            engine.begin().unwrap(),
        )
        .unwrap();
        execute_plan_autocommit(
            &LogicalPlanner::plan(
                "INSERT INTO dept_budget (dept, budget) VALUES ('Engineering', 100)",
            )
            .unwrap(),
            &ctx,
            engine.begin().unwrap(),
        )
        .unwrap();

        let sql = "SELECT id FROM employees e WHERE EXISTS (
            SELECT 1 FROM dept_budget d WHERE d.dept = e.dept
        )";
        let plan = LogicalPlanner::plan(sql).unwrap();
        let physical = optimize_with_catalog(
            &plan,
            &|t| engine.table_schema(t).ok(),
            &|_| None,
        )
        .unwrap();
        let explain = explain_physical(&physical);
        assert!(
            explain.contains("HashSemiJoin"),
            "correlated equi EXISTS must unnest to HashSemiJoin, got:\n{explain}"
        );

        let mut txn = engine.begin().unwrap();
        let out = execute_plan(&plan, &ctx, &mut txn).unwrap();
        txn.abort();

        let mut ids: Vec<_> = out
            .iter()
            .map(|r| r.get("id").unwrap().to_string())
            .collect();
        ids.sort();
        assert_eq!(ids, vec!["1".to_string(), "3".to_string()]);

        engine.close().unwrap();
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn correlated_in_filters_per_outer_row() {
        let (engine, root) = temp_engine("outerref-in");
        engine
            .register_table(TableSchema::new("employees", "id", vec![]))
            .unwrap();
        engine
            .register_table(TableSchema::new("dept_budget", "dept", vec![]))
            .unwrap();
        let ctx = ExecutionContext::new();

        execute_plan_autocommit(
            &LogicalPlanner::plan(
                "INSERT INTO employees (id, dept) VALUES \
                 (1, 'Engineering'), (2, 'Sales'), (3, 'Engineering')",
            )
            .unwrap(),
            &ctx,
            engine.begin().unwrap(),
        )
        .unwrap();
        execute_plan_autocommit(
            &LogicalPlanner::plan(
                "INSERT INTO dept_budget (dept, budget) VALUES ('Engineering', 100)",
            )
            .unwrap(),
            &ctx,
            engine.begin().unwrap(),
        )
        .unwrap();

        let sql = "SELECT id FROM employees e WHERE e.dept IN (
            SELECT d.dept FROM dept_budget d WHERE d.dept = e.dept
        )";
        let plan = LogicalPlanner::plan(sql).unwrap();
        let physical = optimize_with_catalog(
            &plan,
            &|t| engine.table_schema(t).ok(),
            &|_| None,
        )
        .unwrap();
        let explain = explain_physical(&physical);
        assert!(
            explain.contains("HashSemiJoin"),
            "correlated equi IN must unnest to HashSemiJoin, got:\n{explain}"
        );
        let mut txn = engine.begin().unwrap();
        let out = execute_plan(&plan, &ctx, &mut txn).unwrap();
        txn.abort();

        let mut ids: Vec<_> = out
            .iter()
            .map(|r| r.get("id").unwrap().to_string())
            .collect();
        ids.sort();
        assert_eq!(ids, vec!["1".to_string(), "3".to_string()]);

        engine.close().unwrap();
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn correlated_scalar_filters_per_outer_row() {
        let (engine, root) = temp_engine("outerref-scalar");
        engine
            .register_table(TableSchema::new("employees", "id", vec![]))
            .unwrap();
        engine
            .register_table(TableSchema::new("dept_budget", "dept", vec![]))
            .unwrap();
        let ctx = ExecutionContext::new();

        execute_plan_autocommit(
            &LogicalPlanner::plan(
                "INSERT INTO employees (id, dept, salary) VALUES \
                 (1, 'Engineering', 50), (2, 'Sales', 40), (3, 'Engineering', 120)",
            )
            .unwrap(),
            &ctx,
            engine.begin().unwrap(),
        )
        .unwrap();
        execute_plan_autocommit(
            &LogicalPlanner::plan(
                "INSERT INTO dept_budget (dept, budget) VALUES \
                 ('Engineering', 100), ('Sales', 100)",
            )
            .unwrap(),
            &ctx,
            engine.begin().unwrap(),
        )
        .unwrap();

        // Outer rows whose salary is below the correlated dept budget.
        let sql = "SELECT id FROM employees e WHERE e.salary < (
            SELECT d.budget FROM dept_budget d WHERE d.dept = e.dept
        )";
        let plan = LogicalPlanner::plan(sql).unwrap();
        let mut txn = engine.begin().unwrap();
        let out = execute_plan(&plan, &ctx, &mut txn).unwrap();
        txn.abort();

        let mut ids: Vec<_> = out
            .iter()
            .map(|r| r.get("id").unwrap().to_string())
            .collect();
        ids.sort();
        // emp 1: 50 < 100, emp 2: 40 < 100, emp 3: 120 < 100 → false
        assert_eq!(ids, vec!["1".to_string(), "2".to_string()]);

        engine.close().unwrap();
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn correlated_scalar_still_uses_apply() {
        let (engine, root) = temp_engine("outerref-scalar-apply");
        engine
            .register_table(TableSchema::new("employees", "id", vec![]))
            .unwrap();
        engine
            .register_table(TableSchema::new("dept_budget", "dept", vec![]))
            .unwrap();
        let sql = "SELECT id FROM employees e WHERE e.salary < (
            SELECT d.budget FROM dept_budget d WHERE d.dept = e.dept
        )";
        let plan = LogicalPlanner::plan(sql).unwrap();
        let physical = optimize_with_catalog(
            &plan,
            &|t| engine.table_schema(t).ok(),
            &|_| None,
        )
        .unwrap();
        let explain = explain_physical(&physical);
        assert!(
            explain.contains("Apply"),
            "scalar correlated must remain Apply, got:\n{explain}"
        );
        engine.close().unwrap();
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn correlated_exists_unnest_beats_apply_on_large_outer() {
        let (engine, root) = temp_engine("outerref-bench");
        engine
            .register_table(TableSchema::new("employees", "id", vec![]))
            .unwrap();
        engine
            .register_table(TableSchema::new("dept_budget", "dept", vec![]))
            .unwrap();
        let ctx = ExecutionContext::new();
        let mut values = String::from("INSERT INTO employees (id, dept) VALUES ");
        for i in 0..2000 {
            if i > 0 {
                values.push_str(", ");
            }
            let dept = if i % 2 == 0 { "Engineering" } else { "Sales" };
            values.push_str(&format!("({i}, '{dept}')"));
        }
        execute_plan_autocommit(
            &LogicalPlanner::plan(&values).unwrap(),
            &ctx,
            engine.begin().unwrap(),
        )
        .unwrap();
        execute_plan_autocommit(
            &LogicalPlanner::plan(
                "INSERT INTO dept_budget (dept, budget) VALUES ('Engineering', 1)",
            )
            .unwrap(),
            &ctx,
            engine.begin().unwrap(),
        )
        .unwrap();

        let sql = "SELECT id FROM employees e WHERE EXISTS (
            SELECT 1 FROM dept_budget d WHERE d.dept = e.dept
        )";
        let plan = LogicalPlanner::plan(sql).unwrap();
        let physical = optimize_with_catalog(
            &plan,
            &|t| engine.table_schema(t).ok(),
            &|_| None,
        )
        .unwrap();
        assert!(
            explain_physical(&physical).contains("HashSemiJoin"),
            "bench query must use unnest path"
        );
        let t0 = std::time::Instant::now();
        let mut txn = engine.begin().unwrap();
        let out = execute_plan(&plan, &ctx, &mut txn).unwrap();
        txn.abort();
        let elapsed = t0.elapsed();
        assert_eq!(out.len(), 1000, "only Engineering rows match");
        assert!(
            elapsed.as_secs_f64() < 5.0,
            "unnest EXISTS too slow: {elapsed:?}"
        );

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
        let mut lim = LimitExec::new(values, 3, Some(2), false, Vec::new(), ctx);
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
