//! SQL parser + logical planner — bridge from standard SQL into Takyonic's CBO / MVCC APIs.
//!
//! ```ignore
//! let plan = LogicalPlanner::plan(
//!     "SELECT * FROM users WHERE status = 'active' AND city = 'Ankara'"
//! )?;
//! // → LogicalPlan::Select { table: "users", filters: [status=active, city=Ankara] }
//! ```

use std::collections::HashMap;

use sqlparser::ast::{
    Action, AlterColumnOperation, AlterTable, AlterTableOperation, Analyze, Array, AssignmentTarget, BinaryOperator,
    CastKind, CeilFloorKind, ColumnOption, CreateIndex, CreateRole, CreateTable, CreateUser,
    DataType, DateTimeField, Distinct as AstDistinct, DuplicateTreatment, Expr, FromTable, Function, FunctionArg,
    FunctionArgExpr, FunctionArgumentClause, FunctionArguments, Grant, GrantObjects, Grantee, GranteeName, GroupByExpr,
    Ident, IndexColumn, Interval as AstInterval, JoinConstraint, JoinOperator, LimitClause, Fetch,
    ObjectName, ObjectNamePart, ObjectType, OrderBy, OrderByExpr, OrderByKind, Password,
    Privileges as AstPrivileges, Query, RenameTableNameKind, Revoke, Select, SelectItem, Set, SetExpr, SetOperator,
    SetQuantifier, Statement, Subscript, AccessExpr,
    TableConstraint, TableFactor, TableObject, TransactionMode, TrimWhereField, UnaryOperator,
    VacuumStatement, Value as SqlValue, ValueWithSpan, WindowFrameBound, WindowFrameUnits,
    WindowSpec, WindowType, NullTreatment,
};
use sqlparser::ast::{NamedWindowDefinition, NamedWindowExpr};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use crate::error::{Result, TakyonicError};
use crate::query::{Filter, FilterOp};
use crate::rbac::Privilege;
use crate::schema::ColumnSpec;
use crate::vector::{DistanceMetric, VectorIndexSpec};

/// SQL scalar value used for bind parameters and expression evaluation.
///
/// Distinct from [`crate::types::Value`] (LSM byte payloads).
#[derive(Clone, Debug)]
pub enum Value {
    /// SQL NULL.
    Null,
    /// Signed 64-bit integer.
    Int(i64),
    /// IEEE-754 double (JIT maps to Cranelift `F64`).
    Float(f64),
    /// UTF-8 text.
    String(String),
    /// Boolean.
    Bool(bool),
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Null, Self::Null) => true,
            (Self::Int(a), Self::Int(b)) => a == b,
            (Self::Float(a), Self::Float(b)) => a.to_bits() == b.to_bits(),
            (Self::String(a), Self::String(b)) => a == b,
            (Self::Bool(a), Self::Bool(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for Value {}

impl std::hash::Hash for Value {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            Self::Null => {}
            Self::Int(n) => n.hash(state),
            Self::Float(f) => f.to_bits().hash(state),
            Self::String(s) => s.hash(state),
            Self::Bool(b) => b.hash(state),
        }
    }
}

impl Value {
    /// Infer a value from a text bind / literal (`"25"` → Int, `"1.5"` → Float, else String).
    pub fn from_text(s: &str) -> Self {
        if let Ok(n) = s.parse::<i64>() {
            return Self::Int(n);
        }
        if let Ok(f) = s.parse::<f64>() {
            if s.contains('.') || s.contains('e') || s.contains('E') {
                return Self::Float(f);
            }
        }
        match s.to_ascii_lowercase().as_str() {
            "true" | "t" => Self::Bool(true),
            "false" | "f" => Self::Bool(false),
            _ => Self::String(s.to_string()),
        }
    }

    /// Display form for residual string comparisons / Debug helpers.
    pub fn to_display(&self) -> String {
        match self {
            Self::Null => String::new(),
            Self::Int(n) => n.to_string(),
            Self::Float(f) => f.to_string(),
            Self::String(s) => s.clone(),
            Self::Bool(b) => b.to_string(),
        }
    }

    /// Truthiness for bare scalar predicates.
    pub fn is_truthy(&self) -> bool {
        match self {
            Self::Null => false,
            Self::Bool(b) => *b,
            Self::Int(n) => *n != 0,
            Self::Float(f) => *f != 0.0,
            Self::String(s) => !s.is_empty() && s != "0" && s != "false",
        }
    }

    /// SQL NULL-ness (empty string is the on-disk NULL sentinel).
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null) || matches!(self, Self::String(s) if s.is_empty())
    }

    /// Coerce to `f64` when numeric.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Self::Int(n) => Some(*n as f64),
            Self::Float(f) => Some(*f),
            Self::Bool(b) => Some(if *b { 1.0 } else { 0.0 }),
            Self::String(s) => s.parse().ok(),
            Self::Null => None,
        }
    }
}

/// Arithmetic operator for [`Expression::Arith`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ArithOp {
    /// `+`
    Add,
    /// `-`
    Sub,
    /// `*`
    Mul,
    /// `/` (integer truncating when both sides are Int; else float).
    Div,
}

/// One `ORDER BY` key: expression + direction + nulls placement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SortExpr {
    /// Sort key expression (column, aggregate result column, …).
    pub expr: Expression,
    /// `true` = ASC (default), `false` = DESC.
    pub asc: bool,
    /// `true` = NULLS FIRST, `false` = NULLS LAST.
    ///
    /// When omitted in SQL, PostgreSQL defaults to NULLS LAST for ASC and
    /// NULLS FIRST for DESC (`nulls_first == !asc`).
    pub nulls_first: bool,
}

impl SortExpr {
    /// Build an ascending sort key (NULLS LAST — PG default for ASC).
    pub fn asc(expr: Expression) -> Self {
        Self {
            expr,
            asc: true,
            nulls_first: false,
        }
    }

    /// Build a descending sort key (NULLS FIRST — PG default for DESC).
    pub fn desc(expr: Expression) -> Self {
        Self {
            expr,
            asc: false,
            nulls_first: true,
        }
    }
}

/// Window function kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WindowKind {
    /// `ROW_NUMBER() OVER (…)`.
    RowNumber,
    /// `RANK() OVER (…)` — ties share rank; next rank skips.
    Rank,
    /// `DENSE_RANK() OVER (…)` — ties share rank; next rank is consecutive.
    DenseRank,
    /// `LAG(value [, offset [, default]]) OVER (…)`.
    Lag,
    /// `LEAD(value [, offset [, default]]) OVER (…)`.
    Lead,
    /// `NTILE(buckets) OVER (…)`.
    Ntile,
    /// `FIRST_VALUE(expr) OVER (…)` — value from first row of the partition.
    FirstValue,
    /// `LAST_VALUE(expr) OVER (…)` — value from last row of the partition.
    LastValue,
    /// `NTH_VALUE(expr, n) OVER (…)` — value from the n-th row of the partition (1-based).
    NthValue,
    /// `PERCENT_RANK() OVER (…)` — `(rank - 1) / (partition_rows - 1)`.
    PercentRank,
    /// `CUME_DIST() OVER (…)` — cumulative distribution within the partition.
    CumeDist,
    /// `SUM(expr) OVER (…)` — framed / partition sum.
    Sum,
    /// `AVG(expr) OVER (…)`.
    Avg,
    /// `COUNT(*)` / `COUNT(expr) OVER (…)`.
    Count,
    /// `MIN(expr) OVER (…)`.
    Min,
    /// `MAX(expr) OVER (…)`.
    Max,
    /// `STRING_AGG(expr, delim) OVER (…)`.
    StringAgg,
    /// `ARRAY_AGG(expr) OVER (…)`.
    ArrayAgg,
    /// `BOOL_AND(expr)` / `EVERY(expr) OVER (…)`.
    BoolAnd,
    /// `BOOL_OR(expr) OVER (…)`.
    BoolOr,
    /// `JSON_AGG(expr) OVER (…)`.
    JsonAgg,
    /// `JSONB_AGG(expr) OVER (…)`.
    JsonbAgg,
    /// `STDDEV` / `STDDEV_SAMP` OVER ….
    StddevSamp,
    /// `STDDEV_POP` OVER ….
    StddevPop,
    /// `VARIANCE` / `VAR_SAMP` OVER ….
    VarSamp,
    /// `VAR_POP` OVER ….
    VarPop,
    /// `CORR(y, x) OVER (…)`.
    Corr,
    /// `COVAR_POP(y, x) OVER (…)`.
    CovarPop,
    /// `COVAR_SAMP(y, x) OVER (…)`.
    CovarSamp,
    /// `REGR_SLOPE(y, x) OVER (…)`.
    RegrSlope,
    /// `REGR_INTERCEPT(y, x) OVER (…)`.
    RegrIntercept,
    /// `REGR_R2(y, x) OVER (…)`.
    RegrR2,
    /// `REGR_COUNT(y, x) OVER (…)`.
    RegrCount,
    /// `REGR_AVGX(y, x) OVER (…)`.
    RegrAvgX,
    /// `REGR_AVGY(y, x) OVER (…)`.
    RegrAvgY,
    /// `REGR_SXX(y, x) OVER (…)`.
    RegrSxx,
    /// `REGR_SYY(y, x) OVER (…)`.
    RegrSyy,
    /// `REGR_SXY(y, x) OVER (…)`.
    RegrSxy,
    /// `BIT_AND(expr) OVER (…)`.
    BitAnd,
    /// `BIT_OR(expr) OVER (…)`.
    BitOr,
    /// `MODE(expr) OVER (…)`.
    Mode,
    /// `JSON_OBJECT_AGG(key, value) OVER (…)`.
    JsonObjectAgg,
    /// `JSONB_OBJECT_AGG(key, value) OVER (…)`.
    JsonbObjectAgg,
}

/// Bound of a window frame (inclusive semantics resolved at execution).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FrameBound {
    /// `UNBOUNDED PRECEDING`
    UnboundedPreceding,
    /// `UNBOUNDED FOLLOWING`
    UnboundedFollowing,
    /// `CURRENT ROW`
    CurrentRow,
    /// `n PRECEDING`
    Preceding(u64),
    /// `n FOLLOWING`
    Following(u64),
}

/// `ROWS` / `RANGE` / `GROUPS` frame units.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FrameUnits {
    /// Physical row offsets.
    Rows,
    /// Logical peer groups from `ORDER BY` (UNBOUNDED / CURRENT ROW / numeric offsets).
    Range,
    /// Count of peer groups from `ORDER BY`.
    Groups,
}

/// PostgreSQL `EXCLUDE` option on a window frame.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum FrameExclude {
    /// `EXCLUDE NO OTHERS` (default) — keep every frame row.
    #[default]
    NoOthers,
    /// `EXCLUDE CURRENT ROW` — drop the current row.
    CurrentRow,
    /// `EXCLUDE GROUP` — drop the current row and its `ORDER BY` peers.
    Group,
    /// `EXCLUDE TIES` — drop peers, but keep the current row.
    Ties,
}

/// Explicit `ROWS|RANGE|GROUPS BETWEEN start AND end` frame.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WindowRowsFrame {
    /// Frame units.
    pub units: FrameUnits,
    /// Frame start bound.
    pub start: FrameBound,
    /// Frame end bound.
    pub end: FrameBound,
    /// `EXCLUDE …` (default [`FrameExclude::NoOthers`]).
    pub exclude: FrameExclude,
}

/// One window function call to materialize as an output column.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WindowCall {
    /// Output column name (alias or default function name).
    pub output_column: String,
    /// Function kind.
    pub kind: WindowKind,
    /// `OVER (PARTITION BY …)` keys (empty → single partition).
    pub partition_by: Vec<Expression>,
    /// `OVER (ORDER BY …)` keys (empty → input order within partition).
    pub order_by: Vec<SortExpr>,
    /// Value expression for `LAG`/`LEAD` / `FIRST_VALUE` / `LAST_VALUE` / `NTH_VALUE`.
    pub value: Option<Expression>,
    /// Offset for `LAG`/`LEAD`/`NTH_VALUE`, or bucket count for `NTILE` (default 1).
    pub offset: i64,
    /// Optional default when `LAG`/`LEAD` steps outside the partition,
    /// delimiter for window `STRING_AGG`, `x` for `CORR`/`COVAR_*`/`REGR_*`,
    /// or value for `JSON*_OBJECT_AGG`.
    pub default_value: Option<Expression>,
    /// Optional `ROWS`/`RANGE` frame; `None` → full partition for value/agg windows.
    pub frame: Option<WindowRowsFrame>,
    /// Optional `FILTER (WHERE …)` predicate (aggregate windows only).
    pub filter: Option<Expression>,
    /// `IGNORE NULLS` for LAG/LEAD/FIRST_VALUE/LAST_VALUE/NTH_VALUE.
    pub ignore_nulls: bool,
}

/// Stable output column name for an aggregate expression (`sum(salary)`, `count(*)`, …).
///
/// Matches the field names emitted by [`crate::executor::AggregateExec`].
pub fn aggregate_result_column(expr: &Expression) -> Option<String> {
    match expr {
        Expression::AggregateFunction {
            name,
            args,
            filter,
            distinct,
            ..
        } => {
            let lower = name.to_ascii_lowercase();
            let base = if args.is_empty() {
                format!("{lower}(*)")
            } else {
                let arg = match &args[0] {
                    Expression::Column(c) => c.clone(),
                    Expression::Literal(s) => s.clone(),
                    Expression::Arith { left, op, right } => {
                        let l = match left.as_ref() {
                            Expression::Column(c) => c.as_str(),
                            Expression::Literal(s) => s.as_str(),
                            _ => "?",
                        };
                        let r = match right.as_ref() {
                            Expression::Column(c) => c.as_str(),
                            Expression::Literal(s) => s.as_str(),
                            _ => "?",
                        };
                        let sym = match op {
                            ArithOp::Add => "+",
                            ArithOp::Sub => "-",
                            ArithOp::Mul => "*",
                            ArithOp::Div => "/",
                        };
                        format!("{l} {sym} {r}")
                    }
                    _ => "?".into(),
                };
                if *distinct {
                    format!("{lower}(distinct {arg})")
                } else {
                    format!("{lower}({arg})")
                }
            };
            if filter.is_some() {
                Some(format!("{base} filter"))
            } else {
                Some(base)
            }
        }
        _ => None,
    }
}

/// Rewrite post-aggregate ORDER BY keys: `SUM(salary)` → column `sum(salary)`.
fn rewrite_sort_expr_for_output(expr: Expression) -> Expression {
    if let Some(col) = aggregate_result_column(&expr) {
        Expression::Column(col)
    } else {
        expr
    }
}

/// Rewrite HAVING aggregates to AggregateExec output column names.
fn rewrite_having_for_aggregate_output(expr: Expression) -> Expression {
    match expr {
        Expression::AggregateFunction { .. } => {
            if let Some(col) = aggregate_result_column(&expr) {
                Expression::Column(col)
            } else {
                expr
            }
        }
        Expression::BinaryOp { left, op, right } => Expression::BinaryOp {
            left: Box::new(rewrite_having_for_aggregate_output(*left)),
            op,
            right: Box::new(rewrite_having_for_aggregate_output(*right)),
        },
        Expression::And { left, right } => Expression::And {
            left: Box::new(rewrite_having_for_aggregate_output(*left)),
            right: Box::new(rewrite_having_for_aggregate_output(*right)),
        },
        Expression::Or { left, right } => Expression::Or {
            left: Box::new(rewrite_having_for_aggregate_output(*left)),
            right: Box::new(rewrite_having_for_aggregate_output(*right)),
        },
        Expression::Arith { left, op, right } => Expression::Arith {
            left: Box::new(rewrite_having_for_aggregate_output(*left)),
            op,
            right: Box::new(rewrite_having_for_aggregate_output(*right)),
        },
        Expression::Not { expr } => Expression::Not {
            expr: Box::new(rewrite_having_for_aggregate_output(*expr)),
        },
        other => other,
    }
}

fn expr_contains_aggregate(expr: &Expression) -> bool {
    match expr {
        Expression::AggregateFunction { .. } => true,
        Expression::BinaryOp { left, right, .. }
        | Expression::And { left, right }
        | Expression::Or { left, right }
        | Expression::Arith { left, right, .. } => {
            expr_contains_aggregate(left) || expr_contains_aggregate(right)
        }
        Expression::Not { expr }
        | Expression::IsNull { expr, .. }
        | Expression::IsBoolTest { expr, .. }
        | Expression::Cast { expr, .. } => expr_contains_aggregate(expr),
        Expression::IsDistinctFrom { left, right, .. }
        | Expression::QuantifiedCmp { left, right, .. }
        | Expression::NullIf { left, right } => {
            expr_contains_aggregate(left) || expr_contains_aggregate(right)
        },
        Expression::Case {
            when_then,
            else_result,
        } => {
            when_then.iter().any(|(w, t)| {
                expr_contains_aggregate(w) || expr_contains_aggregate(t)
            }) || else_result
                .as_ref()
                .is_some_and(|e| expr_contains_aggregate(e))
        }
        Expression::ScalarFunction { args, .. } => args.iter().any(expr_contains_aggregate),
        _ => false,
    }
}

/// Collect aggregate calls from an expression (HAVING / SELECT), de-duped by output name.
fn collect_aggregates_into(expr: &Expression, out: &mut Vec<Expression>) {
    match expr {
        Expression::AggregateFunction { .. } => {
            let name = aggregate_result_column(expr);
            let exists = name
                .as_ref()
                .is_some_and(|n| out.iter().any(|e| aggregate_result_column(e).as_ref() == Some(n)));
            if !exists {
                out.push(expr.clone());
            }
        }
        Expression::BinaryOp { left, right, .. }
        | Expression::And { left, right }
        | Expression::Or { left, right }
        | Expression::Arith { left, right, .. }
        | Expression::IsDistinctFrom { left, right, .. }
        | Expression::QuantifiedCmp { left, right, .. }
        | Expression::NullIf { left, right } => {
            collect_aggregates_into(left, out);
            collect_aggregates_into(right, out);
        }
        Expression::Not { expr }
        | Expression::IsNull { expr, .. }
        | Expression::IsBoolTest { expr, .. }
        | Expression::Cast { expr, .. } => collect_aggregates_into(expr, out),
        Expression::Case {
            when_then,
            else_result,
        } => {
            for (w, t) in when_then {
                collect_aggregates_into(w, out);
                collect_aggregates_into(t, out);
            }
            if let Some(e) = else_result {
                collect_aggregates_into(e, out);
            }
        }
        Expression::ScalarFunction { args, .. } => {
            for a in args {
                collect_aggregates_into(a, out);
            }
        }
        _ => {}
    }
}

/// Join kind for [`LogicalPlan::Join`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JoinType {
    /// `INNER JOIN` / plain `JOIN`.
    Inner,
    /// `LEFT [OUTER] JOIN`.
    Left,
    /// `RIGHT [OUTER] JOIN`.
    Right,
    /// `FULL [OUTER] JOIN`.
    Full,
    /// Semi-join: yield left rows that have ≥1 match on the right (IN / EXISTS).
    Semi,
    /// Anti-join: yield left rows with no match on the right (NOT IN / NOT EXISTS).
    Anti,
}

/// Scalar / boolean / aggregate expression in a logical plan.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Expression {
    /// Column reference (`id`, `users.id` → leaf name).
    Column(String),
    /// Reference to a column from an outer query row (correlated subquery).
    OuterRef(String),
    /// Literal value (string form; coerced at eval time).
    Literal(String),
    /// Prepared-statement parameter `$1`, `$2`, … stored **0-based** (`$1` → `0`).
    Parameter(usize),
    /// Binary comparison (`=`, `>`, …).
    BinaryOp {
        /// Left operand.
        left: Box<Expression>,
        /// Comparison operator.
        op: FilterOp,
        /// Right operand.
        right: Box<Expression>,
    },
    /// Boolean AND of two predicates.
    And {
        /// Left predicate.
        left: Box<Expression>,
        /// Right predicate.
        right: Box<Expression>,
    },
    /// Boolean OR of two predicates.
    Or {
        /// Left predicate.
        left: Box<Expression>,
        /// Right predicate.
        right: Box<Expression>,
    },
    /// Arithmetic (`+`, `-`, `*`, `/`).
    Arith {
        /// Left operand.
        left: Box<Expression>,
        /// Operator.
        op: ArithOp,
        /// Right operand.
        right: Box<Expression>,
    },
    /// Aggregate call (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, …).
    ///
    /// Only valid inside [`LogicalPlan::Aggregate`]; scalar `evaluate` rejects these.
    AggregateFunction {
        /// Uppercase function name (`COUNT`, `SUM`, …).
        name: String,
        /// Function arguments (`COUNT(*)` → empty; `SUM(salary)` → `[Column("salary")]`).
        args: Vec<Expression>,
        /// Optional `FILTER (WHERE …)` predicate; row is skipped when false/NULL.
        filter: Option<Box<Expression>>,
        /// `COUNT(DISTINCT …)` / `agg(DISTINCT …)` — unique inputs only.
        distinct: bool,
        /// `agg(...) ORDER BY …` inside the argument list (e.g. `string_agg`).
        order_by: Vec<SortExpr>,
    },
    /// `expr [NOT] IN (SELECT …)` — subquery plan is inlined / unnested by the CBO.
    InSubquery {
        /// Left-hand expression.
        expr: Box<Expression>,
        /// Subquery producing candidate values (first projected column).
        subquery: Box<LogicalPlan>,
        /// Output column name of the subquery value (SemiJoin right key).
        value_column: String,
        /// `NOT IN`.
        negated: bool,
        /// True when the subquery references outer-query columns.
        correlated: bool,
    },
    /// `[NOT] EXISTS (SELECT …)`.
    Exists {
        /// Subquery checked for non-emptiness.
        subquery: Box<LogicalPlan>,
        /// `NOT EXISTS`.
        negated: bool,
        /// True when correlated to the outer row.
        correlated: bool,
    },
    /// Scalar subquery `(SELECT …)` — must yield ≤1 row / 1 column at runtime.
    ScalarSubquery {
        /// Subquery plan.
        subquery: Box<LogicalPlan>,
        /// First projected column name.
        value_column: String,
        /// True when correlated.
        correlated: bool,
    },
    /// Materialized `expr [NOT] IN (v1, v2, …)` (after uncorrelated subquery eval).
    InList {
        /// Left-hand expression.
        expr: Box<Expression>,
        /// Candidate values.
        list: Vec<Value>,
        /// `NOT IN`.
        negated: bool,
    },
    /// Vector distance `left <-> right` (Euclidean) or cosine variant.
    VectorDistance {
        /// Typically a column holding an embedding.
        left: Box<Expression>,
        /// Query vector (`ARRAY[…]` / `[…]` literal).
        right: Box<Expression>,
        /// Distance metric.
        metric: DistanceMetric,
    },
    /// SQL array literal `ARRAY[…]` — evaluated to a vector text encoding.
    Array(Vec<Expression>),
    /// 1-based array subscript `arr[i]`.
    ArrayIndex {
        /// Array expression.
        array: Box<Expression>,
        /// Index expression (1-based).
        index: Box<Expression>,
    },
    /// `expr [NOT] LIKE|ILIKE [ANY] pattern` (optional ESCAPE).
    Like {
        /// Subject expression.
        expr: Box<Expression>,
        /// Pattern (`%` / `_`; ESCAPE disables metacharacters), or array when `any`.
        pattern: Box<Expression>,
        /// `ILIKE` (case-insensitive).
        case_insensitive: bool,
        /// `NOT LIKE` / `NOT ILIKE`.
        negated: bool,
        /// `LIKE ANY (array)` / `ILIKE ANY (array)` — true if any pattern matches.
        any: bool,
        /// Optional single-character ESCAPE.
        escape: Option<char>,
    },
    /// `expr [NOT] SIMILAR TO pattern` (optional ESCAPE) — SQL regex dialect.
    SimilarTo {
        /// Subject expression.
        expr: Box<Expression>,
        /// SQL `SIMILAR TO` pattern (`%`/`_`/`|`/`*`/`+`/`()` / `[…]`).
        pattern: Box<Expression>,
        /// `NOT SIMILAR TO`.
        negated: bool,
        /// Optional single-character ESCAPE (PG default `\`).
        escape: Option<char>,
    },
    /// PostgreSQL `expr ~|~*|!~|!~* pattern` — POSIX regex match.
    RegexMatch {
        /// Subject expression.
        expr: Box<Expression>,
        /// POSIX regex pattern.
        pattern: Box<Expression>,
        /// `~*` / `!~*` (case-insensitive).
        case_insensitive: bool,
        /// `!~` / `!~*`.
        negated: bool,
    },
    /// `timestamp AT TIME ZONE zone` — shift / reinterpret wall time.
    AtTimeZone {
        /// Timestamp (with or without offset).
        timestamp: Box<Expression>,
        /// Zone name or offset (`UTC`, `+03:00`, …).
        time_zone: Box<Expression>,
    },
    /// `CASE WHEN … THEN … [ELSE …] END` (simple CASE rewritten to equality WHENs).
    Case {
        /// `(WHEN predicate, THEN result)` arms in order.
        when_then: Vec<(Expression, Expression)>,
        /// Optional `ELSE` result (`NULL` when absent and no WHEN matches).
        else_result: Option<Box<Expression>>,
    },
    /// `expr IS [NOT] NULL`.
    IsNull {
        /// Subject expression.
        expr: Box<Expression>,
        /// `IS NOT NULL` when true.
        negated: bool,
    },
    /// `expr IS [NOT] TRUE|FALSE|UNKNOWN` — PostgreSQL boolean tests (never NULL).
    IsBoolTest {
        /// Subject expression.
        expr: Box<Expression>,
        /// Which boolean constant to test against.
        test: BoolTest,
        /// `IS NOT …` when true.
        negated: bool,
    },
    /// `a IS [NOT] DISTINCT FROM b` — NULL-safe inequality / equality.
    IsDistinctFrom {
        /// Left operand.
        left: Box<Expression>,
        /// Right operand.
        right: Box<Expression>,
        /// `IS NOT DISTINCT FROM` when true (NULL-safe equality).
        negated: bool,
    },
    /// `left op ANY|SOME|ALL (array_or_list_expr)` — quantified comparison (3VL).
    QuantifiedCmp {
        /// Left scalar operand.
        left: Box<Expression>,
        /// Comparison operator (`=`, `<>`, `<`, …).
        op: FilterOp,
        /// Right-hand array / list-producing expression.
        right: Box<Expression>,
        /// `ANY`/`SOME` vs `ALL`.
        quantifier: Quantifier,
    },
    /// `COALESCE(a, b, …)` — first non-NULL argument.
    Coalesce(Vec<Expression>),
    /// `CAST(expr AS type)` / `expr::type` (optional soft/`TRY_CAST`).
    Cast {
        /// Value being cast.
        expr: Box<Expression>,
        /// Target SQL family.
        target: CastTarget,
        /// Soft cast: failure → NULL instead of error.
        try_cast: bool,
    },
    /// `NULLIF(a, b)` — NULL when `a = b`, else `a`.
    NullIf {
        /// Left argument.
        left: Box<Expression>,
        /// Right argument.
        right: Box<Expression>,
    },
    /// Non-aggregate scalar call (`LOWER`, `UPPER`, `LENGTH`, `TRIM`, `SUBSTRING`, …).
    ScalarFunction {
        /// Uppercase function name.
        name: String,
        /// Positional arguments.
        args: Vec<Expression>,
    },
    /// Boolean `NOT expr`.
    Not {
        /// Predicate or scalar to negate.
        expr: Box<Expression>,
    },
}

/// PostgreSQL `IS TRUE` / `IS FALSE` / `IS UNKNOWN` target.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BoolTest {
    /// `IS TRUE`
    True,
    /// `IS FALSE`
    False,
    /// `IS UNKNOWN` (SQL NULL)
    Unknown,
}

/// Infer output column names for `CREATE TABLE AS` from a logical plan.
pub fn ctas_output_columns(plan: &LogicalPlan) -> Result<Vec<String>> {
    match plan {
        LogicalPlan::Project { columns, .. } => {
            Ok(columns.iter().map(|(n, _)| n.clone()).collect())
        }
        LogicalPlan::Values { columns, .. } => Ok(columns.clone()),
        LogicalPlan::Aggregate {
            group_exprs,
            aggr_exprs,
            ..
        } => {
            let mut names = Vec::new();
            for g in group_exprs {
                match g {
                    Expression::Column(c) => names.push(c.clone()),
                    other => names.push(format!("{other:?}")),
                }
            }
            for a in aggr_exprs {
                names.push(
                    aggregate_result_column(a)
                        .unwrap_or_else(|| "aggr".into()),
                );
            }
            Ok(names)
        }
        LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Filter { input, .. }
        | LogicalPlan::Window { input, .. }
        | LogicalPlan::Distinct { input, .. }
        | LogicalPlan::DistinctOn { input, .. }
        | LogicalPlan::SubqueryAlias { input, .. } => ctas_output_columns(input),
        LogicalPlan::Select { .. } => Ok(Vec::new()), // resolve via table schema at exec
        other => Err(TakyonicError::Sql(format!(
            "CREATE TABLE AS SELECT cannot infer columns from {other:?}"
        ))),
    }
}

/// Quantifier for `op ANY|SOME|ALL (…)` comparisons.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Quantifier {
    /// `ANY` / `SOME` — true if any element matches.
    Any,
    /// `ALL` — true if every element matches (empty → true).
    All,
}

/// `ON CONFLICT` action for INSERT (subset of PostgreSQL upsert).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OnConflict {
    /// Skip the row when the primary key already exists.
    DoNothing,
    /// Update the existing row using `SET` assignments (and optional `WHERE`).
    DoUpdate {
        /// Column → assignment expression (`EXCLUDED.col` rewritten to `__excluded.col`).
        assignments: Vec<(String, Expression)>,
        /// Optional `WHERE` predicate; skip update when false/NULL.
        selection: Option<Expression>,
    },
}

/// Storage-field prefix for `EXCLUDED.col` values during upsert evaluation.
pub const EXCLUDED_FIELD_PREFIX: &str = "__excluded.";

/// `RETURNING` clause on INSERT / UPDATE / DELETE.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Returning {
    /// `RETURNING *` — emit the full row after the DML.
    Star,
    /// Explicit `RETURNING expr [AS name], …`.
    List(Vec<(String, Expression)>),
}

/// Target type family for [`Expression::Cast`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CastTarget {
    /// `TEXT` / `VARCHAR` / `CHAR`.
    Text,
    /// `INT` / `BIGINT` / `SMALLINT`.
    Int,
    /// `FLOAT` / `DOUBLE` / `REAL`.
    Float,
    /// `BOOL`.
    Bool,
    /// `JSON` / `JSONB` (stored as compact JSON text).
    Json,
}

/// One supported `ALTER TABLE` operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AlterTableOp {
    /// `ADD [COLUMN] [IF NOT EXISTS] col type`.
    AddColumn {
        /// Column specification.
        column: ColumnSpec,
        /// `IF NOT EXISTS`.
        if_not_exists: bool,
        /// Column was declared as `SERIAL` / `BIGSERIAL` / `SMALLSERIAL`.
        is_serial: bool,
    },
    /// `DROP [COLUMN] [IF EXISTS] col`.
    DropColumn {
        /// Column name.
        name: String,
        /// `IF EXISTS`.
        if_exists: bool,
    },
    /// `RENAME [COLUMN] old TO new`.
    RenameColumn {
        /// Existing column name.
        old_name: String,
        /// New column name.
        new_name: String,
    },
    /// `RENAME TO new_table`.
    RenameTable {
        /// New table name.
        new_name: String,
    },
    /// `ALTER COLUMN col TYPE typ` / `SET DATA TYPE typ`.
    SetDataType {
        /// Column name.
        name: String,
        /// Canonical type token (`INT`, `TEXT`, …).
        data_type: String,
    },
}

/// `UNION` / `INTERSECT` / `EXCEPT` set operators.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SetOpKind {
    /// `UNION` — concatenation (distinct or `ALL`).
    Union,
    /// `INTERSECT` — rows present in both sides.
    Intersect,
    /// `EXCEPT` — rows in left but not right.
    Except,
}

impl SetOpKind {
    /// SQL keyword for EXPLAIN / errors.
    pub fn sql_name(self) -> &'static str {
        match self {
            Self::Union => "UNION",
            Self::Intersect => "INTERSECT",
            Self::Except => "EXCEPT",
        }
    }
}

/// `COPY … FROM|TO` destination / source.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CopyIoTarget {
    /// Filesystem path (`COPY t FROM/TO '/path'`).
    File(String),
    /// PostgreSQL wire `COPY FROM STDIN`.
    Stdin,
    /// PostgreSQL wire `COPY TO STDOUT`.
    Stdout,
}

/// Logical plan produced by translating a SQL AST into Takyonic primitives.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LogicalPlan {
    /// `SELECT ... FROM table WHERE ...` → CBO filters + optional Volcano predicate.
    Select {
        /// Target table.
        table: String,
        /// Conjunctive equality/range predicates from WHERE (literal-only; CBO).
        filters: Vec<Filter>,
        /// Full WHERE expression tree (supports `$n` parameters).
        predicate: Option<Expression>,
    },
    /// Column projection (`SELECT a, b AS x`) over an input plan.
    ///
    /// Applied outermost so `ORDER BY` / `WHERE` still see the full row.
    Project {
        /// Child plan.
        input: Box<LogicalPlan>,
        /// `(output_name, source_expr)` pairs; order is the SELECT list order.
        columns: Vec<(String, Expression)>,
    },
    /// Window functions (`ROW_NUMBER() OVER …`) — adds columns onto input rows.
    Window {
        /// Child plan (after WHERE / GROUP BY / HAVING).
        input: Box<LogicalPlan>,
        /// Window calls to compute (output columns appended on each row).
        calls: Vec<WindowCall>,
    },
    /// `INSERT INTO table (cols...) VALUES (...)` [ON CONFLICT …] [RETURNING …].
    ///
    /// When `query` is `Some`, rows come from that SELECT (VALUES must be empty).
    Insert {
        /// Target table.
        table: String,
        /// Explicit column list.
        columns: Vec<String>,
        /// One expression row per VALUES tuple (may contain `$n` parameters).
        values: Vec<Vec<Expression>>,
        /// Optional `INSERT … SELECT` source plan (`None` for VALUES).
        query: Option<Box<LogicalPlan>>,
        /// Optional `ON CONFLICT` action.
        on_conflict: Option<OnConflict>,
        /// Optional `RETURNING` projection.
        returning: Option<Returning>,
    },
    /// `UPDATE table SET col = expr, ... [WHERE ...] [RETURNING …]`.
    Update {
        /// Target table.
        table: String,
        /// Column → assignment expression.
        assignments: HashMap<String, Expression>,
        /// Optional WHERE predicate.
        selection: Option<Expression>,
        /// Optional `RETURNING` projection.
        returning: Option<Returning>,
    },
    /// `DELETE FROM table [WHERE ...] [RETURNING …]`.
    Delete {
        /// Target table.
        table: String,
        /// Optional WHERE predicate.
        selection: Option<Expression>,
        /// Optional `RETURNING` projection.
        returning: Option<Returning>,
    },
    /// `TRUNCATE [TABLE] name` — delete all rows (MVCC deletes).
    Truncate {
        /// Target table.
        table: String,
        /// `IF EXISTS` — no-op when the table is missing.
        if_exists: bool,
    },
    /// `COPY table [(cols…)] FROM|TO {'path'|STDIN|STDOUT}` — TSV bulk load.
    Copy {
        /// Target / source table.
        table: String,
        /// Optional column list (empty = catalog column order).
        columns: Vec<String>,
        /// `true` = COPY TO; `false` = COPY FROM.
        to: bool,
        /// File path, or stdin/stdout protocol target.
        target: CopyIoTarget,
    },
    /// `SELECT ... FROM left JOIN right ON ...` (Inner for now).
    Join {
        /// Left input plan.
        left: Box<LogicalPlan>,
        /// Right input plan.
        right: Box<LogicalPlan>,
        /// Join condition (`ON` expression).
        on: Expression,
        /// Join kind.
        join_type: JoinType,
    },
    /// `BEGIN [TRANSACTION]`.
    Begin,
    /// `COMMIT`.
    Commit,
    /// `ROLLBACK`.
    Rollback,
    /// Aggregation over an input plan (`GROUP BY` + aggregate SELECT list).
    Aggregate {
        /// Child plan providing input rows (typically [`LogicalPlan::Select`] or Join).
        input: Box<LogicalPlan>,
        /// Grouping key expressions (empty → global aggregate over all rows).
        group_exprs: Vec<Expression>,
        /// Aggregate expressions from the SELECT list (`COUNT`, `SUM`, …).
        aggr_exprs: Vec<Expression>,
    },
    /// MPP aggregate: partial aggregation on workers + shuffle + final merge.
    DistributedAggregate {
        /// Child plan (typically a table scan / select).
        input: Box<LogicalPlan>,
        /// Grouping keys.
        group_exprs: Vec<Expression>,
        /// Aggregate expressions.
        aggr_exprs: Vec<Expression>,
    },
    /// MPP equi-join: shuffle both sides on the join key then local hash join.
    DistributedJoin {
        /// Left input.
        left: Box<LogicalPlan>,
        /// Right input.
        right: Box<LogicalPlan>,
        /// Join condition.
        on: Expression,
        /// Join kind.
        join_type: JoinType,
        /// How to redistribute both sides before the local join.
        distribution: crate::shuffle::Distribution,
    },
    /// `ORDER BY` — sort child rows by one or more keys.
    Sort {
        /// Child plan.
        input: Box<LogicalPlan>,
        /// Sort keys (priority order).
        exprs: Vec<SortExpr>,
    },
    /// `LIMIT` / `OFFSET` / `FETCH FIRST … [WITH TIES]`.
    Limit {
        /// Child plan.
        input: Box<LogicalPlan>,
        /// Rows to skip (`OFFSET`).
        skip: usize,
        /// Max rows to yield (`LIMIT` / `FETCH`); `None` = unbounded (OFFSET-only).
        fetch: Option<usize>,
        /// `FETCH … WITH TIES` — keep peers of the last included `ORDER BY` key.
        with_ties: bool,
        /// Sort keys used for WITH TIES peer expansion (from the child `ORDER BY`).
        ties_order: Vec<SortExpr>,
    },
    /// `CREATE INDEX` / `CREATE VECTOR INDEX` name ON table (column) [WITH (…)].
    CreateIndex {
        /// Index name.
        name: String,
        /// Target table.
        table: String,
        /// Indexed column.
        column: String,
        /// `IF NOT EXISTS`.
        if_not_exists: bool,
        /// When set, create an HNSW vector index instead of a B-Tree.
        vector: Option<VectorIndexSpec>,
    },
    /// `CREATE TABLE name (cols…)`.
    CreateTable {
        /// Table name.
        name: String,
        /// Primary-key column name (exactly one required).
        primary_key: String,
        /// Declared columns (includes the PK column).
        columns: Vec<ColumnSpec>,
        /// `IF NOT EXISTS`.
        if_not_exists: bool,
        /// Columns declared as `SERIAL` / `BIGSERIAL` / `SMALLSERIAL` (sequence + OWNED BY).
        serial_columns: Vec<String>,
    },
    /// `CREATE TABLE name [(cols…)] AS SELECT …`.
    CreateTableAs {
        /// Table name.
        name: String,
        /// Query producing seed rows (and column shapes when `columns` is empty).
        query: Box<LogicalPlan>,
        /// Optional explicit column names from `CREATE TABLE t (a, b) AS …`.
        columns: Vec<String>,
        /// `IF NOT EXISTS`.
        if_not_exists: bool,
    },
    /// `ALTER TABLE … ADD/DROP COLUMN`.
    AlterTable {
        /// Target table.
        name: String,
        /// Ordered list of supported alter ops.
        operations: Vec<AlterTableOp>,
    },
    /// `DROP INDEX name`.
    DropIndex {
        /// Index name.
        name: String,
        /// `IF EXISTS`.
        if_exists: bool,
    },
    /// `DROP TABLE name`.
    DropTable {
        /// Table name.
        name: String,
        /// `IF EXISTS`.
        if_exists: bool,
    },
    /// `EXPLAIN <statement>` — returns the physical plan text (not executed).
    Explain {
        /// Inner statement plan.
        plan: Box<LogicalPlan>,
    },
    /// `ANALYZE <table>` — gather / refresh table statistics for the CBO.
    Analyze {
        /// Target table.
        table: String,
    },
    /// `VACUUM <table>` — reclaim dead MVCC versions below the epoch watermark.
    Vacuum {
        /// Target table.
        table: String,
    },
    /// `REBALANCE TABLE name` — move one hot partition fragment to a colder node.
    Rebalance {
        /// Partitioned target table.
        table: String,
    },
    /// `CREATE USER` / `CREATE ROLE`.
    CreateRole {
        /// Role / user name.
        name: String,
        /// May log in (`CREATE USER` / `LOGIN`).
        can_login: bool,
        /// Superuser flag.
        is_superuser: bool,
        /// Cleartext password (hashed before storage); required when `can_login`.
        password: Option<String>,
        /// `IF NOT EXISTS`.
        if_not_exists: bool,
    },
    /// `DROP ROLE` / `DROP USER`.
    DropRole {
        /// Role name.
        name: String,
        /// `IF EXISTS`.
        if_exists: bool,
    },
    /// `GRANT <priv> ON <table> TO <grantee>`.
    Grant {
        /// Privilege list (`ALL` expands to all four).
        privileges: Vec<crate::rbac::Privilege>,
        /// Target table.
        table: String,
        /// Role / user receiving the grant.
        grantee: String,
    },
    /// `REVOKE <priv> ON <table> FROM <grantee>`.
    Revoke {
        /// Privileges to remove.
        privileges: Vec<crate::rbac::Privilege>,
        /// Target table.
        table: String,
        /// Role / user losing the grant.
        grantee: String,
    },
    /// `GRANT <priv> ON SCHEMA <schema> TO <grantee>`.
    GrantSchema {
        /// Schema privileges (`USAGE` / `CREATE`; `ALL` → both).
        privileges: Vec<crate::rbac::SchemaPrivilege>,
        /// Target schema.
        schema: String,
        /// Role / user receiving the grant.
        grantee: String,
    },
    /// `REVOKE <priv> ON SCHEMA <schema> FROM <grantee>`.
    RevokeSchema {
        /// Schema privileges to remove.
        privileges: Vec<crate::rbac::SchemaPrivilege>,
        /// Target schema.
        schema: String,
        /// Role / user losing the grant.
        grantee: String,
    },
    /// `GRANT SELECT (col…) ON <table> TO <grantee>` (column ACL).
    GrantColumn {
        /// Privilege + column list pairs.
        specs: Vec<crate::rbac::ColumnGrantSpec>,
        /// Target table.
        table: String,
        /// Role / user receiving the grant.
        grantee: String,
    },
    /// `REVOKE SELECT (col…) ON <table> FROM <grantee>`.
    RevokeColumn {
        /// Privilege + column list pairs.
        specs: Vec<crate::rbac::ColumnGrantSpec>,
        /// Target table.
        table: String,
        /// Role / user losing the grant.
        grantee: String,
    },
    /// `GRANT <role> TO <member>` (role membership).
    GrantRole {
        /// Role being granted.
        role: String,
        /// Member receiving the role.
        member: String,
    },
    /// `WHERE` / residual predicate over an arbitrary child plan.
    Filter {
        /// Child plan.
        input: Box<LogicalPlan>,
        /// Boolean predicate (may contain subqueries).
        predicate: Expression,
    },
    /// CTE / derived table: named inline view over `input`.
    SubqueryAlias {
        /// Alias introduced by `WITH name AS (…)`, `FROM (… ) AS name`, etc.
        alias: String,
        /// Inner plan producing the view's rows.
        input: Box<LogicalPlan>,
    },
    /// `UNION` / `INTERSECT` / `EXCEPT` (and `… ALL`) of two query plans.
    Union {
        /// Left operand.
        left: Box<LogicalPlan>,
        /// Right operand.
        right: Box<LogicalPlan>,
        /// Which set operator.
        op: SetOpKind,
        /// `true` = multiset (`ALL`); `false` = distinct.
        all: bool,
    },
    /// `SELECT DISTINCT` — eliminate duplicate output rows.
    Distinct {
        /// Child plan (typically after projection).
        input: Box<LogicalPlan>,
    },
    /// `SELECT DISTINCT ON (exprs)` — keep first row per ON-key group (PG).
    ///
    /// Placed after `ORDER BY` and before `LIMIT` / projection so ON expressions
    /// can reference input columns (same scope as `ORDER BY`).
    DistinctOn {
        /// Child plan (typically after sort).
        input: Box<LogicalPlan>,
        /// DISTINCT ON expressions (evaluated per row).
        exprs: Vec<Expression>,
    },
    /// `VALUES (…), (…)` — in-memory constant row set (PG `column1`… or alias names).
    Values {
        /// Output column names.
        columns: Vec<String>,
        /// Rows of scalar expressions (literals / parameters).
        rows: Vec<Vec<Expression>>,
    },
    /// `FROM generate_series(start, stop [, step])` — integer or timestamp series.
    GenerateSeries {
        /// Inclusive start (integer value, or unix seconds when `as_timestamp`).
        start: i64,
        /// Inclusive stop (integer value, or unix seconds when `as_timestamp`).
        stop: i64,
        /// Step (default 1 for integers; INTERVAL seconds for timestamps; non-zero).
        step: i64,
        /// Output column name (`generate_series` or alias column).
        column: String,
        /// Optional `WITH ORDINALITY` column (`ordinality` or alias).
        ordinality_column: Option<String>,
        /// When true, emit formatted timestamps instead of integers.
        as_timestamp: bool,
        /// Prefer `YYYY-MM-DD` output when bounds were date-only.
        date_only: bool,
    },
    /// `FROM unnest(ARRAY[…])` / correlated `LATERAL unnest(col)` — expand array into rows.
    Unnest {
        /// Array expression (`ARRAY[…]`, column, or array-producing scalar).
        array: Expression,
        /// Output column name (`unnest` or alias column).
        column: String,
        /// Optional `WITH ORDINALITY` / `WITH OFFSET` column.
        ordinality_column: Option<String>,
        /// When true, ordinality/offset is 0-based (`WITH OFFSET`); else 1-based (`WITH ORDINALITY`).
        zero_based_ordinality: bool,
    },
    /// `FROM jsonb_array_elements(doc)` / `_text` — expand a JSON array.
    ///
    /// `doc` may be a folded literal or a row-dependent expression (correlated
    /// `LATERAL` joins).
    JsonArrayElements {
        /// JSON array document expression (literal or per-outer-row).
        doc: Expression,
        /// Output column name (`value` or alias).
        column: String,
        /// When true, emit scalar JSON as text (strip quotes for strings).
        as_text: bool,
        /// Optional `WITH ORDINALITY` column.
        ordinality_column: Option<String>,
    },
    /// `FROM json_each(doc)` / `jsonb_each` / `*_text` — expand object key/value pairs.
    JsonEach {
        /// JSON object document expression (literal or per-outer-row).
        doc: Expression,
        /// Key column name (`key` or alias).
        key_column: String,
        /// Value column name (`value` or alias).
        value_column: String,
        /// When true, emit values as text.
        as_text: bool,
        /// Optional `WITH ORDINALITY` column.
        ordinality_column: Option<String>,
    },
    /// `FROM jsonb_object_keys(doc)` — expand object keys into rows.
    JsonObjectKeys {
        /// JSON object document expression (literal or per-outer-row).
        doc: Expression,
        /// Output column name (`jsonb_object_keys` or alias).
        column: String,
        /// Optional `WITH ORDINALITY` column.
        ordinality_column: Option<String>,
    },
    /// `FROM regexp_split_to_table(string, pattern [, flags])` — expand regex splits into rows.
    RegexpSplitToTable {
        /// Input string (literal or per-outer-row).
        string: Expression,
        /// Regex pattern expression.
        pattern: Expression,
        /// Optional flag expression (`i`, …).
        flags: Option<Expression>,
        /// Output column name (`regexp_split_to_table` or alias).
        column: String,
        /// Optional `WITH ORDINALITY` column.
        ordinality_column: Option<String>,
    },
    /// `FROM regexp_matches(string, pattern [, flags])` — one row per match as text[] display.
    RegexpMatches {
        /// Input string (literal or per-outer-row).
        string: Expression,
        /// Regex pattern expression.
        pattern: Expression,
        /// Optional flag expression (`g`/`i`, …).
        flags: Option<Expression>,
        /// Output column name (`regexp_matches` or alias).
        column: String,
        /// Optional `WITH ORDINALITY` column.
        ordinality_column: Option<String>,
    },
    /// `SET name TO value` / `SET name = value` (session GUC stub).
    Set {
        /// Setting name (`search_path`, `transaction_isolation`, …).
        name: String,
        /// Assigned value (normalized string).
        value: String,
    },
    /// `SHOW name` — return current session setting.
    Show {
        /// Setting name.
        name: String,
    },
    /// `COMMENT ON TABLE|COLUMN|ROLE|DATABASE … IS '…'|NULL` (persisted under `COMMENTS`).
    Comment {
        /// `table`, `column`, `role`, or `database`.
        object_type: String,
        /// Table / role / database name (also used for `COMMENT ON TABLE`).
        table: String,
        /// Column name when `object_type == "column"`.
        column: Option<String>,
        /// Comment text; `None` clears.
        comment: Option<String>,
    },
    /// `LISTEN channel` — register interest in NOTIFY channel (session-local).
    Listen {
        /// Channel name.
        channel: String,
    },
    /// `UNLISTEN channel` / `UNLISTEN *` — drop one or all LISTEN registrations.
    Unlisten {
        /// Channel to drop; `None` means all (`*`).
        channel: Option<String>,
    },
    /// `NOTIFY channel [, payload]` — signal listeners on `channel`.
    Notify {
        /// Channel name.
        channel: String,
        /// Optional payload (empty string when omitted).
        payload: String,
    },
    /// `CREATE SEQUENCE name [START [WITH] n] [INCREMENT [BY] n]`.
    CreateSequence {
        /// Sequence name (schema-stripped, lowercased at exec).
        name: String,
        /// `IF NOT EXISTS`.
        if_not_exists: bool,
        /// Initial `last_value` before first `nextval` (`is_called = false`).
        start: i64,
        /// Step applied after the first call.
        increment: i64,
    },
    /// `DROP SEQUENCE [IF EXISTS] name`.
    DropSequence {
        /// Sequence name.
        name: String,
        /// `IF EXISTS`.
        if_exists: bool,
    },
    /// `ALTER SEQUENCE name …` (RESTART / INCREMENT / OWNED BY / RENAME TO).
    AlterSequence {
        /// Sequence name.
        name: String,
        /// `RESTART [WITH] n`.
        restart: Option<i64>,
        /// `INCREMENT [BY] n`.
        increment: Option<i64>,
        /// `OWNED BY table.col` (`Some(None)` = `OWNED BY NONE`).
        owned_by: Option<Option<(String, String)>>,
        /// `RENAME TO new_name`.
        rename_to: Option<String>,
    },
}

/// SQL → AST → [`LogicalPlan`] translator.
pub struct LogicalPlanner;

impl LogicalPlanner {
    /// Parse a single SQL statement into a [`LogicalPlan`].
    pub fn plan(sql: &str) -> Result<LogicalPlan> {
        let trimmed = sql.trim_start();
        if let Some((role, member)) = try_parse_grant_role_membership(trimmed) {
            return Ok(LogicalPlan::GrantRole { role, member });
        }
        if let Some(plan) = try_parse_listen_unlisten(trimmed) {
            return Ok(plan);
        }
        if let Some(plan) = try_parse_notify(trimmed) {
            return Ok(plan);
        }
        if let Some(plan) = try_parse_create_drop_sequence(trimmed) {
            return Ok(plan);
        }
        if let Some(plan) = try_parse_alter_sequence(trimmed) {
            return Ok(plan);
        }
        if let Some(plan) = try_parse_rebalance(trimmed) {
            return Ok(plan);
        }
        let sql = preprocess_sql(sql);
        let dialect = PostgreSqlDialect {};
        let stmts = Parser::parse_sql(&dialect, &sql)
            .map_err(|e| TakyonicError::Sql(format!("parse error: {e}")))?;
        if stmts.len() != 1 {
            return Err(TakyonicError::Sql(format!(
                "expected exactly one statement, got {}",
                stmts.len()
            )));
        }
        Self::from_statement(&stmts[0])
    }

    /// Translate an already-parsed AST statement.
    pub fn from_statement(stmt: &Statement) -> Result<LogicalPlan> {
        match stmt {
            Statement::Query(query) => Self::plan_select(query),
            Statement::Insert(insert) => Self::plan_insert(insert),
            Statement::Update(update) => Self::plan_update(update),
            Statement::Delete(delete) => Self::plan_delete(delete),
            Statement::StartTransaction { .. } => Ok(LogicalPlan::Begin),
            Statement::Commit { .. } => Ok(LogicalPlan::Commit),
            Statement::Rollback { .. } => Ok(LogicalPlan::Rollback),
            Statement::CreateIndex(create) => Self::plan_create_index(create),
            Statement::CreateTable(create) => Self::plan_create_table(create),
            Statement::AlterTable(alter) => Self::plan_alter_table(alter),
            Statement::CreateRole(create) => Self::plan_create_role(create),
            Statement::CreateUser(create) => Self::plan_create_user(create),
            Statement::Grant(grant) => Self::plan_grant(grant),
            Statement::Revoke(revoke) => Self::plan_revoke(revoke),
            Statement::Drop {
                object_type: ObjectType::Index,
                names,
                if_exists,
                ..
            } => Self::plan_drop_index(names, *if_exists),
            Statement::Drop {
                object_type: ObjectType::Table,
                names,
                if_exists,
                ..
            } => Self::plan_drop_table(names, *if_exists),
            Statement::Drop {
                object_type: ObjectType::Role | ObjectType::User,
                names,
                if_exists,
                ..
            } => Self::plan_drop_role(names, *if_exists),
            Statement::Explain { statement, .. } => Ok(LogicalPlan::Explain {
                plan: Box::new(Self::from_statement(statement)?),
            }),
            Statement::Analyze(analyze) => Self::plan_analyze(analyze),
            Statement::Vacuum(vacuum) => Self::plan_vacuum(vacuum),
            Statement::Truncate(truncate) => Self::plan_truncate(truncate),
            Statement::Copy {
                source,
                to,
                target,
                values,
                ..
            } => Self::plan_copy(source, *to, target, values),
            Statement::Set(set) => Self::plan_set(set),
            Statement::ShowVariable { variable } => Self::plan_show(variable),
            Statement::Comment {
                object_type,
                object_name,
                comment,
                ..
            } => Self::plan_comment(*object_type, object_name, comment.clone()),
            other => Err(TakyonicError::Sql(format!(
                "unsupported statement: {other}"
            ))),
        }
    }

    fn plan_comment(
        object_type: sqlparser::ast::CommentObject,
        object_name: &sqlparser::ast::ObjectName,
        comment: Option<String>,
    ) -> Result<LogicalPlan> {
        use sqlparser::ast::CommentObject;
        match object_type {
            CommentObject::Table => {
                let table = object_name_leaf(object_name)?;
                Ok(LogicalPlan::Comment {
                    object_type: "table".into(),
                    table,
                    column: None,
                    comment,
                })
            }
            CommentObject::Column => {
                let idents: Vec<String> = object_name
                    .0
                    .iter()
                    .filter_map(|p| match p {
                        ObjectNamePart::Identifier(ident) => Some(ident.value.clone()),
                        _ => None,
                    })
                    .collect();
                if idents.len() < 2 {
                    return Err(TakyonicError::Sql(
                        "COMMENT ON COLUMN requires table.column".into(),
                    ));
                }
                let column = idents.last().cloned().unwrap();
                let table = idents[idents.len() - 2].clone();
                Ok(LogicalPlan::Comment {
                    object_type: "column".into(),
                    table,
                    column: Some(column),
                    comment,
                })
            }
            CommentObject::Role | CommentObject::User => {
                let name = object_name_leaf(object_name)?;
                Ok(LogicalPlan::Comment {
                    object_type: "role".into(),
                    table: name,
                    column: None,
                    comment,
                })
            }
            CommentObject::Database => {
                let name = object_name_leaf(object_name)?;
                Ok(LogicalPlan::Comment {
                    object_type: "database".into(),
                    table: name,
                    column: None,
                    comment,
                })
            }
            other => Err(TakyonicError::Sql(format!(
                "unsupported COMMENT ON {other}"
            ))),
        }
    }

    fn plan_analyze(analyze: &Analyze) -> Result<LogicalPlan> {
        let name = analyze.table_name.as_ref().ok_or_else(|| {
            TakyonicError::Sql("ANALYZE requires a table name".into())
        })?;
        Ok(LogicalPlan::Analyze {
            table: object_name_leaf(name)?,
        })
    }

    fn plan_vacuum(vacuum: &VacuumStatement) -> Result<LogicalPlan> {
        let name = vacuum.table_name.as_ref().ok_or_else(|| {
            TakyonicError::Sql("VACUUM requires a table name".into())
        })?;
        Ok(LogicalPlan::Vacuum {
            table: object_name_leaf(name)?,
        })
    }

    fn plan_truncate(truncate: &sqlparser::ast::Truncate) -> Result<LogicalPlan> {
        if truncate.table_names.len() != 1 {
            return Err(TakyonicError::Sql(
                "TRUNCATE supports exactly one table".into(),
            ));
        }
        if truncate.partitions.is_some() {
            return Err(TakyonicError::Sql(
                "TRUNCATE … PARTITION is not supported".into(),
            ));
        }
        if truncate.on_cluster.is_some() {
            return Err(TakyonicError::Sql(
                "TRUNCATE … ON CLUSTER is not supported".into(),
            ));
        }
        if matches!(
            truncate.cascade,
            Some(sqlparser::ast::CascadeOption::Cascade)
        ) {
            return Err(TakyonicError::Sql(
                "TRUNCATE … CASCADE is not supported".into(),
            ));
        }
        // RESTART/CONTINUE IDENTITY and RESTRICT are accepted as no-ops (no
        // identity sequences / no FK enforcement yet).
        let table = object_name_leaf(&truncate.table_names[0].name)?;
        Ok(LogicalPlan::Truncate {
            table,
            if_exists: truncate.if_exists,
        })
    }

    fn plan_copy(
        source: &sqlparser::ast::CopySource,
        to: bool,
        target: &sqlparser::ast::CopyTarget,
        _values: &[Option<String>],
    ) -> Result<LogicalPlan> {
        use sqlparser::ast::{CopySource, CopyTarget};
        let (table, columns) = match source {
            CopySource::Table {
                table_name,
                columns,
            } => {
                let table = object_name_leaf(table_name)?;
                let cols: Vec<String> = columns.iter().map(|c| c.value.clone()).collect();
                (table, cols)
            }
            CopySource::Query(_) => {
                return Err(TakyonicError::Sql(
                    "COPY (query) is not supported yet".into(),
                ));
            }
        };
        let target = match target {
            CopyTarget::File { filename } => CopyIoTarget::File(filename.clone()),
            CopyTarget::Stdin => CopyIoTarget::Stdin,
            CopyTarget::Stdout => CopyIoTarget::Stdout,
            other => {
                return Err(TakyonicError::Sql(format!(
                    "COPY target `{other}` is not supported (use a file path, STDIN, or STDOUT)"
                )));
            }
        };
        Ok(LogicalPlan::Copy {
            table,
            columns,
            to,
            target,
        })
    }

    fn plan_set(set: &Set) -> Result<LogicalPlan> {
        match set {
            Set::SingleAssignment {
                variable, values, ..
            } => {
                let name = normalize_guc_name(&object_name_leaf(variable)?);
                if values.is_empty() {
                    return Err(TakyonicError::Sql("SET requires a value".into()));
                }
                let value = if name == "search_path" && values.len() > 1 {
                    let parts: Result<Vec<_>> = values.iter().map(set_value_to_string).collect();
                    parts?.join(", ")
                } else {
                    set_value_to_string(&values[0])?
                };
                let value = normalize_guc_value(&name, &value)?;
                Ok(LogicalPlan::Set { name, value })
            }
            Set::SetTransaction { modes, .. } => {
                let mut isolation = None;
                for mode in modes {
                    if let TransactionMode::IsolationLevel(level) = mode {
                        isolation = Some(transaction_isolation_name(level)?);
                    }
                }
                let value = isolation.ok_or_else(|| {
                    TakyonicError::Sql(
                        "SET TRANSACTION requires ISOLATION LEVEL \
                         (READ COMMITTED or REPEATABLE READ)"
                            .into(),
                    )
                })?;
                Ok(LogicalPlan::Set {
                    name: "transaction_isolation".into(),
                    value,
                })
            }
            other => Err(TakyonicError::Sql(format!(
                "unsupported SET form: {other}"
            ))),
        }
    }

    fn plan_show(variable: &[Ident]) -> Result<LogicalPlan> {
        if variable.is_empty() {
            return Err(TakyonicError::Sql("SHOW requires a variable name".into()));
        }
        let name = normalize_guc_name(
            &variable
                .iter()
                .map(|i| i.value.as_str())
                .collect::<Vec<_>>()
                .join("_"),
        );
        Ok(LogicalPlan::Show { name })
    }

    fn plan_select(query: &Query) -> Result<LogicalPlan> {
        Self::plan_query(query, &HashMap::new(), &[])
    }

    /// Plan a query with inherited CTEs and outer-column scope (for correlation).
    fn plan_query(
        query: &Query,
        parent_ctes: &HashMap<String, LogicalPlan>,
        outer_columns: &[String],
    ) -> Result<LogicalPlan> {
        let mut ctes = parent_ctes.clone();
        if let Some(with) = &query.with {
            for cte in &with.cte_tables {
                let name = cte.alias.name.value.clone();
                let plan = Self::plan_query(&cte.query, &ctes, &[])?;
                ctes.insert(name, plan);
            }
        }

        let plan = match query.body.as_ref() {
            SetExpr::Select(s) => Self::plan_select_body(
                s.as_ref(),
                &ctes,
                outer_columns,
                query.order_by.as_ref(),
                query.limit_clause.as_ref(),
                query.fetch.as_ref(),
            )?,
            other => {
                let mut plan = Self::plan_set_expr(other, &ctes, outer_columns)?;
                let scope_for_order = collect_plan_output_hints(&plan);
                if let Some(order_by) = &query.order_by {
                    let exprs = plan_order_by_ctx(
                        order_by,
                        &ctes,
                        outer_columns,
                        &scope_for_order,
                        None,
                        &scope_for_order,
                    )?;
                    if !exprs.is_empty() {
                        plan = LogicalPlan::Sort {
                            input: Box::new(plan),
                            exprs,
                        };
                    }
                }
                plan = apply_limit_and_fetch(
                    plan,
                    query.limit_clause.as_ref(),
                    query.fetch.as_ref(),
                )?;
                plan
            }
        };
        Ok(plan)
    }

    fn plan_set_expr(
        expr: &SetExpr,
        ctes: &HashMap<String, LogicalPlan>,
        outer_columns: &[String],
    ) -> Result<LogicalPlan> {
        match expr {
            SetExpr::Select(s) => Self::plan_select_body(
                s.as_ref(),
                ctes,
                outer_columns,
                None,
                None,
                None,
            ),
            SetExpr::Query(q) => Self::plan_query(q, ctes, outer_columns),
            SetExpr::SetOperation {
                left,
                op,
                set_quantifier,
                right,
            } => {
                let kind = match op {
                    SetOperator::Union => SetOpKind::Union,
                    SetOperator::Intersect => SetOpKind::Intersect,
                    SetOperator::Except => SetOpKind::Except,
                    other => {
                        return Err(TakyonicError::Sql(format!(
                            "unsupported set operator: {other}"
                        )));
                    }
                };
                let left = Self::plan_set_expr(left.as_ref(), ctes, outer_columns)?;
                let right = Self::plan_set_expr(right.as_ref(), ctes, outer_columns)?;
                let all = matches!(set_quantifier, SetQuantifier::All);
                Ok(LogicalPlan::Union {
                    left: Box::new(left),
                    right: Box::new(right),
                    op: kind,
                    all,
                })
            }
            SetExpr::Values(v) => plan_values_clause(v, ctes, outer_columns, None),
            other => Err(TakyonicError::Sql(format!(
                "unsupported query body: {other}"
            ))),
        }
    }

    /// Plan a single `SELECT` body.
    ///
    /// When `order_by` / `limit` are provided (top-level SELECT), they are applied
    /// **before** projection so `ORDER BY` can reference non-selected columns.
    fn plan_select_body(
        select: &Select,
        ctes: &HashMap<String, LogicalPlan>,
        outer_columns: &[String],
        order_by: Option<&OrderBy>,
        limit_clause: Option<&LimitClause>,
        fetch_clause: Option<&Fetch>,
    ) -> Result<LogicalPlan> {
        if select.from.is_empty() {
            return Err(TakyonicError::Sql(
                "SELECT requires a FROM clause".into(),
            ));
        }
        let from = &select.from[0];

        let local_from = {
            let mut s = Vec::new();
            for twj in &select.from {
                s.extend(from_relation_scope_names(&twj.relation));
                for join in &twj.joins {
                    s.extend(from_relation_scope_names(&join.relation));
                }
            }
            s
        };

        // Resolve FROM (base table, CTE alias, derived subquery, or table function).
        let mut plan = Self::plan_from_item_ctx(&from.relation, ctes, &[])?;
        let mut left_scope = from_relation_scope_names(&from.relation);
        // Parent outer + this query's FROM aliases — nested subqueries see these
        // as OuterRef candidates. This query's own WHERE uses `outer_columns` only
        // for OuterRef (so local aliases stay as Column).
        let scope_for_subqueries = {
            let mut s = outer_columns.to_vec();
            s.extend(local_from.iter().cloned());
            s
        };
        for join in &from.joins {
            let (join_type, on) = plan_join_operator_ctx(
                &join.join_operator,
                ctes,
                outer_columns,
                &scope_for_subqueries,
            )?;
            let lateral_outer = if relation_is_lateral(&join.relation) {
                left_scope.clone()
            } else {
                Vec::new()
            };
            let right = Self::plan_from_item_ctx(&join.relation, ctes, &lateral_outer)?;
            plan = LogicalPlan::Join {
                left: Box::new(plan),
                right: Box::new(right),
                on,
                join_type,
            };
            left_scope.extend(from_relation_scope_names(&join.relation));
        }
        // Additional comma-separated FROM items → implicit CROSS JOIN.
        for twj in select.from.iter().skip(1) {
            // Comma-FROM is not LATERAL; correlated table-fn args stay rejected.
            let right = Self::plan_from_item_ctx(&twj.relation, ctes, &[])?;
            plan = LogicalPlan::Join {
                left: Box::new(plan),
                right: Box::new(right),
                on: Expression::Literal("true".into()),
                join_type: JoinType::Inner,
            };
            left_scope.extend(from_relation_scope_names(&twj.relation));
            for join in &twj.joins {
                let (join_type, on) = plan_join_operator_ctx(
                    &join.join_operator,
                    ctes,
                    outer_columns,
                    &scope_for_subqueries,
                )?;
                let lateral_outer = if relation_is_lateral(&join.relation) {
                    left_scope.clone()
                } else {
                    Vec::new()
                };
                let right = Self::plan_from_item_ctx(&join.relation, ctes, &lateral_outer)?;
                plan = LogicalPlan::Join {
                    left: Box::new(plan),
                    right: Box::new(right),
                    on,
                    join_type,
                };
                left_scope.extend(from_relation_scope_names(&join.relation));
            }
        }

        // Refresh scope with plan hints after joins are attached.
        let scope_for_subqueries = {
            let mut s = scope_for_subqueries;
            s.extend(collect_plan_output_hints(&plan));
            s
        };

        // WHERE — may contain IN/EXISTS/scalar subqueries.
        if let Some(selection) = &select.selection {
            let (filters, predicate) = plan_where_ctx(
                Some(selection),
                ctes,
                outer_columns,
                &scope_for_subqueries,
            )?;
            plan = match plan {
                LogicalPlan::Select {
                    table,
                    filters: mut existing,
                    predicate: None,
                } if from.joins.is_empty() && select.from.len() == 1 => {
                    existing.extend(filters);
                    LogicalPlan::Select {
                        table,
                        filters: existing,
                        predicate,
                    }
                }
                other => {
                    if let Some(pred) = predicate {
                        LogicalPlan::Filter {
                            input: Box::new(other),
                            predicate: pred,
                        }
                    } else {
                        other
                    }
                }
            };
        }

        let (group_exprs, aggr_exprs, has_agg, having) = plan_projection_aggregates_ctx(
            select,
            ctes,
            outer_columns,
            &scope_for_subqueries,
        )?;
        let is_aggregate = has_agg || !group_exprs.is_empty();
        if is_aggregate {
            plan = LogicalPlan::Aggregate {
                input: Box::new(plan),
                group_exprs,
                aggr_exprs: aggr_exprs.clone(),
            };
            if let Some(pred) = having {
                let pred = rewrite_having_for_aggregate_output(pred);
                plan = LogicalPlan::Filter {
                    input: Box::new(plan),
                    predicate: pred,
                };
            }
        } else if having.is_some() {
            return Err(TakyonicError::Sql(
                "HAVING without GROUP BY / aggregates is unsupported".into(),
            ));
        }

        // Window functions after WHERE/GROUP BY/HAVING, before query ORDER BY / LIMIT.
        let mut window_proj: Option<Vec<(String, Expression)>> = None;
        if !is_aggregate {
            let (columns, windows) = plan_projection_list_with_windows(
                select,
                ctes,
                outer_columns,
                &scope_for_subqueries,
            )?;
            if !windows.is_empty() {
                plan = LogicalPlan::Window {
                    input: Box::new(plan),
                    calls: windows,
                };
            }
            window_proj = columns;
        }

        // DISTINCT ON uses the same expression scope as ORDER BY.
        let distinct_on_exprs = match &select.distinct {
            Some(AstDistinct::On(on_exprs)) => {
                if on_exprs.is_empty() {
                    return Err(TakyonicError::Sql(
                        "DISTINCT ON requires at least one expression".into(),
                    ));
                }
                Some(
                    on_exprs
                        .iter()
                        .map(|e| {
                            Ok(rewrite_sort_expr_for_output(expr_to_expression_ctx(
                                e,
                                ctes,
                                outer_columns,
                                &scope_for_subqueries,
                            )?))
                        })
                        .collect::<Result<Vec<_>>>()?,
                )
            }
            _ => None,
        };

        if let Some(order_by) = order_by {
            let exprs = plan_order_by_ctx(
                order_by,
                ctes,
                outer_columns,
                &scope_for_subqueries,
                Some(select),
                &[],
            )?;
            if !exprs.is_empty() {
                plan = LogicalPlan::Sort {
                    input: Box::new(plan),
                    exprs,
                };
            }
        }

        // After ORDER BY, before LIMIT — PG DISTINCT ON keeps the first sorted row
        // of each ON-key group.
        if let Some(exprs) = distinct_on_exprs {
            if let LogicalPlan::Sort {
                exprs: sort_exprs, ..
            } = &plan
            {
                if sort_exprs.len() < exprs.len()
                    || !sort_exprs
                        .iter()
                        .zip(exprs.iter())
                        .all(|(s, o)| &s.expr == o)
                {
                    return Err(TakyonicError::Sql(
                        "SELECT DISTINCT ON expressions must match initial ORDER BY expressions"
                            .into(),
                    ));
                }
            }
            plan = LogicalPlan::DistinctOn {
                input: Box::new(plan),
                exprs,
            };
        }

        plan = apply_limit_and_fetch(plan, limit_clause, fetch_clause)?;

        // Projection last so ORDER BY / WHERE still see full rows (PG-compatible).
        if !is_aggregate {
            if let Some(columns) = window_proj {
                plan = LogicalPlan::Project {
                    input: Box::new(plan),
                    columns,
                };
            }
        }

        match &select.distinct {
            None | Some(AstDistinct::All) | Some(AstDistinct::On(_)) => {}
            Some(AstDistinct::Distinct) => {
                plan = LogicalPlan::Distinct {
                    input: Box::new(plan),
                };
            }
        }

        Ok(plan)
    }

    fn plan_from_item_ctx(
        factor: &TableFactor,
        ctes: &HashMap<String, LogicalPlan>,
        lateral_outer: &[String],
    ) -> Result<LogicalPlan> {
        match factor {
            TableFactor::Table {
                name,
                alias,
                args: Some(tf_args),
                with_ordinality,
                ..
            } => {
                let table = object_name_leaf(name)?;
                let upper = table.to_ascii_uppercase();
                if tf_args.settings.is_some() {
                    return Err(TakyonicError::Sql(format!(
                        "{upper} SETTINGS clause is unsupported"
                    )));
                }
                match upper.as_str() {
                    "GENERATE_SERIES" => {
                        let spec = parse_generate_series_args(&tf_args.args)?;
                        let (column, ordinality_column) = srf_value_and_ordinality_columns(
                            alias.as_ref(),
                            "generate_series",
                            *with_ordinality,
                        );
                        Ok(LogicalPlan::GenerateSeries {
                            start: spec.start,
                            stop: spec.stop,
                            step: spec.step,
                            column,
                            ordinality_column,
                            as_timestamp: spec.as_timestamp,
                            date_only: spec.date_only,
                        })
                    }
                    "JSONB_ARRAY_ELEMENTS"
                    | "JSON_ARRAY_ELEMENTS"
                    | "JSONB_ARRAY_ELEMENTS_TEXT"
                    | "JSON_ARRAY_ELEMENTS_TEXT" => plan_json_array_elements_srf(
                        &upper,
                        &tf_args.args,
                        alias.as_ref(),
                        ctes,
                        lateral_outer,
                        *with_ordinality,
                    ),
                    "JSONB_OBJECT_KEYS" | "JSON_OBJECT_KEYS" => plan_json_object_srf(
                        &upper,
                        &tf_args.args,
                        alias.as_ref(),
                        ctes,
                        lateral_outer,
                        *with_ordinality,
                    ),
                    "JSON_EACH"
                    | "JSONB_EACH"
                    | "JSON_EACH_TEXT"
                    | "JSONB_EACH_TEXT" => plan_json_object_srf(
                        &upper,
                        &tf_args.args,
                        alias.as_ref(),
                        ctes,
                        lateral_outer,
                        *with_ordinality,
                    ),
                    "REGEXP_SPLIT_TO_TABLE" => plan_regexp_text_srf(
                        "REGEXP_SPLIT_TO_TABLE",
                        &tf_args.args,
                        alias.as_ref(),
                        ctes,
                        lateral_outer,
                        *with_ordinality,
                    ),
                    "REGEXP_MATCHES" => plan_regexp_text_srf(
                        "REGEXP_MATCHES",
                        &tf_args.args,
                        alias.as_ref(),
                        ctes,
                        lateral_outer,
                        *with_ordinality,
                    ),
                    "UNNEST" => plan_unnest_srf(
                        &tf_args.args,
                        alias.as_ref(),
                        ctes,
                        lateral_outer,
                        *with_ordinality,
                    ),
                    other if *with_ordinality => Err(TakyonicError::Sql(format!(
                        "{other} WITH ORDINALITY is not yet supported"
                    ))),
                    other => Err(TakyonicError::Sql(format!(
                        "unsupported table function `{other}` \
                         (generate_series, jsonb_array_elements, unnest, regexp_split_to_table, regexp_matches, …)"
                    ))),
                }
            }
            TableFactor::Function {
                lateral,
                name,
                args,
                alias,
                with_ordinality,
                ..
            } => {
                let upper = object_name_leaf(name)?.to_ascii_uppercase();
                let allow_correlated = *lateral && !lateral_outer.is_empty();
                match upper.as_str() {
                    "GENERATE_SERIES" => {
                        if !allow_correlated {
                            ensure_table_fn_args_are_literals(args, &upper)?;
                        }
                        let spec = parse_generate_series_args(args)?;
                        let (column, ordinality_column) = srf_value_and_ordinality_columns(
                            alias.as_ref(),
                            "generate_series",
                            *with_ordinality,
                        );
                        Ok(LogicalPlan::GenerateSeries {
                            start: spec.start,
                            stop: spec.stop,
                            step: spec.step,
                            column,
                            ordinality_column,
                            as_timestamp: spec.as_timestamp,
                            date_only: spec.date_only,
                        })
                    }
                    "JSONB_ARRAY_ELEMENTS"
                    | "JSON_ARRAY_ELEMENTS"
                    | "JSONB_ARRAY_ELEMENTS_TEXT"
                    | "JSON_ARRAY_ELEMENTS_TEXT" => {
                        if !allow_correlated {
                            ensure_table_fn_args_are_literals(args, &upper)?;
                        }
                        plan_json_array_elements_srf(
                            &upper,
                            args,
                            alias.as_ref(),
                            ctes,
                            lateral_outer,
                            *with_ordinality,
                        )
                    }
                    "JSONB_OBJECT_KEYS" | "JSON_OBJECT_KEYS" => {
                        if !allow_correlated {
                            ensure_table_fn_args_are_literals(args, &upper)?;
                        }
                        plan_json_object_srf(
                            &upper,
                            args,
                            alias.as_ref(),
                            ctes,
                            lateral_outer,
                            *with_ordinality,
                        )
                    }
                    "JSON_EACH"
                    | "JSONB_EACH"
                    | "JSON_EACH_TEXT"
                    | "JSONB_EACH_TEXT" => {
                        if !allow_correlated {
                            ensure_table_fn_args_are_literals(args, &upper)?;
                        }
                        plan_json_object_srf(
                            &upper,
                            args,
                            alias.as_ref(),
                            ctes,
                            lateral_outer,
                            *with_ordinality,
                        )
                    }
                    "UNNEST" => plan_unnest_srf(
                        args,
                        alias.as_ref(),
                        ctes,
                        lateral_outer,
                        *with_ordinality,
                    ),
                    "REGEXP_SPLIT_TO_TABLE" => plan_regexp_text_srf(
                        "REGEXP_SPLIT_TO_TABLE",
                        args,
                        alias.as_ref(),
                        ctes,
                        lateral_outer,
                        *with_ordinality,
                    ),
                    "REGEXP_MATCHES" => plan_regexp_text_srf(
                        "REGEXP_MATCHES",
                        args,
                        alias.as_ref(),
                        ctes,
                        lateral_outer,
                        *with_ordinality,
                    ),
                    other if *with_ordinality => Err(TakyonicError::Sql(format!(
                        "{other} WITH ORDINALITY is not yet supported"
                    ))),
                    other => Err(TakyonicError::Sql(format!(
                        "unsupported table function `{other}` \
                         (generate_series, jsonb_array_elements, unnest, regexp_split_to_table, regexp_matches, …)"
                    ))),
                }
            }
            TableFactor::Table { name, alias, .. } => {
                let table = object_name_leaf(name)?;
                if let Some(cte_plan) = ctes.get(&table) {
                    let alias = alias
                        .as_ref()
                        .map(|a| a.name.value.clone())
                        .unwrap_or(table);
                    Ok(LogicalPlan::SubqueryAlias {
                        alias,
                        input: Box::new(cte_plan.clone()),
                    })
                } else {
                    Ok(LogicalPlan::Select {
                        table,
                        filters: Vec::new(),
                        predicate: None,
                    })
                }
            }
            TableFactor::Derived {
                subquery, alias, ..
            } => {
                let alias_info = alias.as_ref().ok_or_else(|| {
                    TakyonicError::Sql("derived FROM subquery requires an alias".into())
                })?;
                let alias_name = alias_info.name.value.clone();
                let col_aliases: Vec<String> = alias_info
                    .columns
                    .iter()
                    .map(|c| c.name.value.clone())
                    .collect();
                let inner = if let SetExpr::Values(v) = subquery.body.as_ref() {
                    let names = if col_aliases.is_empty() {
                        None
                    } else {
                        Some(col_aliases.as_slice())
                    };
                    plan_values_clause(v, ctes, &[], names)?
                } else {
                    let mut inner = Self::plan_query(subquery, ctes, &[])?;
                    if !col_aliases.is_empty() {
                        if let LogicalPlan::Values { columns, .. } = &mut inner {
                            if col_aliases.len() != columns.len() {
                                return Err(TakyonicError::Sql(format!(
                                    "VALUES alias has {} columns but row width is {}",
                                    col_aliases.len(),
                                    columns.len()
                                )));
                            }
                            *columns = col_aliases;
                        }
                    }
                    inner
                };
                Ok(LogicalPlan::SubqueryAlias {
                    alias: alias_name,
                    input: Box::new(inner),
                })
            }
            TableFactor::UNNEST {
                alias,
                array_exprs,
                with_offset,
                with_offset_alias,
                with_ordinality,
                ..
            } => {
                if *with_offset && *with_ordinality {
                    return Err(TakyonicError::Sql(
                        "UNNEST WITH OFFSET and WITH ORDINALITY together are not supported"
                            .into(),
                    ));
                }
                if array_exprs.len() != 1 {
                    return Err(TakyonicError::Sql(
                        "UNNEST currently supports exactly one array argument".into(),
                    ));
                }
                let array = expr_to_expression_ctx(
                    &array_exprs[0],
                    ctes,
                    lateral_outer,
                    lateral_outer,
                )?;
                if expr_needs_row_eval(&array) && lateral_outer.is_empty() {
                    return Err(TakyonicError::Sql(
                        "correlated LATERAL UNNEST arguments are not yet supported \
                         (use literals or CROSS JOIN LATERAL unnest(…))"
                            .into(),
                    ));
                }
                let (column, ordinality_column, zero_based_ordinality) = if *with_offset {
                    let (column, _) =
                        srf_value_and_ordinality_columns(alias.as_ref(), "unnest", false);
                    let offset = with_offset_alias
                        .as_ref()
                        .map(|i| i.value.clone())
                        .unwrap_or_else(|| "offset".into());
                    (column, Some(offset), true)
                } else {
                    let (column, ordinality_column) = srf_value_and_ordinality_columns(
                        alias.as_ref(),
                        "unnest",
                        *with_ordinality,
                    );
                    (column, ordinality_column, false)
                };
                Ok(LogicalPlan::Unnest {
                    array,
                    column,
                    ordinality_column,
                    zero_based_ordinality,
                })
            }
            other => Err(TakyonicError::Sql(format!(
                "unsupported FROM relation: {other}"
            ))),
        }
    }

    fn plan_insert(insert: &sqlparser::ast::Insert) -> Result<LogicalPlan> {
        let table = table_object_name(&insert.table)?;
        let columns: Vec<String> = insert
            .columns
            .iter()
            .map(object_name_leaf)
            .collect::<Result<_>>()?;
        if columns.is_empty() {
            return Err(TakyonicError::Sql(
                "INSERT requires an explicit column list".into(),
            ));
        }
        let source = insert
            .source
            .as_ref()
            .ok_or_else(|| TakyonicError::Sql("INSERT missing VALUES".into()))?;
        let returning = plan_returning_clause(insert.returning.as_deref())?;
        let on_conflict = plan_on_conflict(insert.on.as_ref())?;
        match source.body.as_ref() {
            SetExpr::Values(value_rows) => {
                let mut values = Vec::with_capacity(value_rows.rows.len());
                for row in &value_rows.rows {
                    let exprs_src: &[sqlparser::ast::Expr] = row;
                    if exprs_src.len() != columns.len() {
                        return Err(TakyonicError::Sql(format!(
                            "INSERT row has {} values for {} columns",
                            exprs_src.len(),
                            columns.len()
                        )));
                    }
                    let mut exprs = Vec::with_capacity(exprs_src.len());
                    for expr in exprs_src {
                        exprs.push(expr_to_expression(expr)?);
                    }
                    values.push(exprs);
                }
                Ok(LogicalPlan::Insert {
                    table,
                    columns,
                    values,
                    query: None,
                    on_conflict,
                    returning,
                })
            }
            SetExpr::Select(_) | SetExpr::Query(_) | SetExpr::SetOperation { .. } => {
                let query = Self::plan_query(source, &HashMap::new(), &[])?;
                Ok(LogicalPlan::Insert {
                    table,
                    columns,
                    values: Vec::new(),
                    query: Some(Box::new(query)),
                    on_conflict,
                    returning,
                })
            }
            other => Err(TakyonicError::Sql(format!(
                "INSERT supports VALUES or SELECT, got {other}"
            ))),
        }
    }

    fn plan_update(update: &sqlparser::ast::Update) -> Result<LogicalPlan> {
        if !update.table.joins.is_empty() {
            return Err(TakyonicError::Sql(
                "UPDATE with JOINs is not supported".into(),
            ));
        }
        let table = table_factor_name(&update.table.relation)?;
        let mut assignments = HashMap::new();
        for assignment in &update.assignments {
            let col = match &assignment.target {
                AssignmentTarget::ColumnName(name) => object_name_leaf(name)?,
                AssignmentTarget::Tuple(_) => {
                    return Err(TakyonicError::Sql(
                        "tuple UPDATE assignments are unsupported".into(),
                    ));
                }
            };
            assignments.insert(col, expr_to_expression(&assignment.value)?);
        }
        if assignments.is_empty() {
            return Err(TakyonicError::Sql("UPDATE requires SET assignments".into()));
        }
        let selection = match &update.selection {
            Some(expr) => Some(expr_to_expression(expr)?),
            None => None,
        };
        let returning = plan_returning_clause(update.returning.as_deref())?;
        Ok(LogicalPlan::Update {
            table,
            assignments,
            selection,
            returning,
        })
    }

    fn plan_delete(delete: &sqlparser::ast::Delete) -> Result<LogicalPlan> {
        let from_tables = match &delete.from {
            FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => tables,
        };
        if from_tables.len() != 1 {
            return Err(TakyonicError::Sql(
                "DELETE requires exactly one target table".into(),
            ));
        }
        if !from_tables[0].joins.is_empty() {
            return Err(TakyonicError::Sql(
                "DELETE with JOINs is not supported".into(),
            ));
        }
        let table = table_factor_name(&from_tables[0].relation)?;
        let selection = match &delete.selection {
            Some(expr) => Some(expr_to_expression(expr)?),
            None => None,
        };
        let returning = plan_returning_clause(delete.returning.as_deref())?;
        Ok(LogicalPlan::Delete {
            table,
            selection,
            returning,
        })
    }

    fn plan_create_index(create: &CreateIndex) -> Result<LogicalPlan> {
        let name = create
            .name
            .as_ref()
            .ok_or_else(|| TakyonicError::Sql("CREATE INDEX requires an index name".into()))?;
        let name = object_name_leaf(name)?;
        let table = object_name_leaf(&create.table_name)?;
        if create.columns.len() != 1 {
            return Err(TakyonicError::Sql(
                "CREATE INDEX supports exactly one column".into(),
            ));
        }
        let column = match &create.columns[0].column.expr {
            Expr::Identifier(ident) => ident.value.clone(),
            Expr::CompoundIdentifier(parts) => parts
                .last()
                .map(|i| i.value.clone())
                .ok_or_else(|| TakyonicError::Sql("empty index column".into()))?,
            other => {
                return Err(TakyonicError::Sql(format!(
                    "unsupported index column expression: {other}"
                )));
            }
        };
        let vector = parse_vector_index_with(&create.with)?;
        Ok(LogicalPlan::CreateIndex {
            name,
            table,
            column,
            if_not_exists: create.if_not_exists,
            vector,
        })
    }

    fn plan_create_table(create: &CreateTable) -> Result<LogicalPlan> {
        if create.like.is_some() || create.clone.is_some() {
            return Err(TakyonicError::Sql(
                "CREATE TABLE LIKE/CLONE is not supported".into(),
            ));
        }
        if create.temporary || create.external || create.transient {
            return Err(TakyonicError::Sql(
                "TEMPORARY/EXTERNAL/TRANSIENT tables are not supported".into(),
            ));
        }

        let name = object_name_leaf(&create.name)?;
        if let Some(query) = &create.query {
            if !create.constraints.is_empty() {
                return Err(TakyonicError::Sql(
                    "CREATE TABLE AS SELECT does not support table constraints".into(),
                ));
            }
            let columns: Vec<String> = create
                .columns
                .iter()
                .map(|c| c.name.value.clone())
                .collect();
            let query = Self::plan_query(query, &HashMap::new(), &[])?;
            return Ok(LogicalPlan::CreateTableAs {
                name,
                query: Box::new(query),
                columns,
                if_not_exists: create.if_not_exists,
            });
        }

        if create.columns.is_empty() {
            return Err(TakyonicError::Sql(
                "CREATE TABLE requires at least one column".into(),
            ));
        }

        let mut columns = Vec::with_capacity(create.columns.len());
        let mut serial_columns = Vec::new();
        let mut pk_from_columns: Vec<String> = Vec::new();
        for col in &create.columns {
            let col_name = col.name.value.clone();
            let (data_type, is_serial) = expand_serial_sql_type(&canonicalize_sql_type(&col.data_type));
            if is_serial {
                serial_columns.push(col_name.clone());
            }
            let mut spec = ColumnSpec::new(col_name.clone(), data_type);
            if is_serial {
                // SERIAL implies NOT NULL in PostgreSQL.
                spec.nullable = false;
            }
            for opt in &col.options {
                match &opt.option {
                    ColumnOption::PrimaryKey(_) => {
                        pk_from_columns.push(col_name.clone());
                        spec.nullable = false;
                    }
                    ColumnOption::NotNull => {
                        spec.nullable = false;
                    }
                    ColumnOption::Null => {
                        spec.nullable = true;
                    }
                    ColumnOption::Default(expr) => {
                        spec.default_sql = Some(expr.to_string());
                    }
                    ColumnOption::Unique(_) => {
                        spec.unique = true;
                    }
                    _ => {}
                }
            }
            columns.push(spec);
        }

        let mut pk_from_table: Vec<String> = Vec::new();
        for constraint in &create.constraints {
            match constraint {
                TableConstraint::PrimaryKey(pk) => {
                    for idx_col in &pk.columns {
                        pk_from_table.push(index_column_name(idx_col)?);
                    }
                }
                TableConstraint::Unique(u) => {
                    if u.columns.len() != 1 {
                        return Err(TakyonicError::Sql(
                            "composite UNIQUE constraints are not supported yet".into(),
                        ));
                    }
                    let c = index_column_name(&u.columns[0])?;
                    if let Some(spec) = columns.iter_mut().find(|s| s.name == c) {
                        spec.unique = true;
                    }
                }
                TableConstraint::ForeignKey(_) | TableConstraint::Check { .. } => {
                    return Err(TakyonicError::Sql(format!(
                        "unsupported table constraint in CREATE TABLE: {constraint} \
                         (FOREIGN KEY / CHECK are not implemented)"
                    )));
                }
                other => {
                    return Err(TakyonicError::Sql(format!(
                        "unsupported table constraint in CREATE TABLE: {other}"
                    )));
                }
            }
        }

        let primary_key = match (pk_from_columns.as_slice(), pk_from_table.as_slice()) {
            ([pk], []) | ([], [pk]) => pk.clone(),
            ([pk], [pk2]) if pk == pk2 => pk.clone(),
            ([], []) => {
                return Err(TakyonicError::Sql(
                    "CREATE TABLE requires a PRIMARY KEY".into(),
                ));
            }
            _ => {
                return Err(TakyonicError::Sql(
                    "CREATE TABLE requires exactly one PRIMARY KEY column".into(),
                ));
            }
        };

        if !columns.iter().any(|c| c.name == primary_key) {
            return Err(TakyonicError::Sql(format!(
                "PRIMARY KEY column `{primary_key}` is not in the column list"
            )));
        }

        Ok(LogicalPlan::CreateTable {
            name,
            primary_key,
            columns,
            if_not_exists: create.if_not_exists,
            serial_columns,
        })
    }

    fn plan_alter_table(alter: &AlterTable) -> Result<LogicalPlan> {
        if alter.operations.is_empty() {
            return Err(TakyonicError::Sql(
                "ALTER TABLE requires at least one operation".into(),
            ));
        }
        let name = object_name_leaf(&alter.name)?;
        let mut operations = Vec::with_capacity(alter.operations.len());
        for op in &alter.operations {
            match op {
                AlterTableOperation::AddColumn {
                    column_def,
                    if_not_exists,
                    ..
                } => {
                    let (data_type, is_serial) =
                        expand_serial_sql_type(&canonicalize_sql_type(&column_def.data_type));
                    operations.push(AlterTableOp::AddColumn {
                        column: ColumnSpec::new(column_def.name.value.clone(), data_type),
                        if_not_exists: *if_not_exists,
                        is_serial,
                    });
                }
                AlterTableOperation::DropColumn {
                    column_names,
                    if_exists,
                    ..
                } => {
                    if column_names.len() != 1 {
                        return Err(TakyonicError::Sql(
                            "DROP COLUMN supports exactly one column".into(),
                        ));
                    }
                    operations.push(AlterTableOp::DropColumn {
                        name: column_names[0].value.clone(),
                        if_exists: *if_exists,
                    });
                }
                AlterTableOperation::RenameColumn {
                    old_column_name,
                    new_column_name,
                } => {
                    operations.push(AlterTableOp::RenameColumn {
                        old_name: old_column_name.value.clone(),
                        new_name: new_column_name.value.clone(),
                    });
                }
                AlterTableOperation::RenameTable { table_name } => {
                    let new_name = match table_name {
                        RenameTableNameKind::To(n) | RenameTableNameKind::As(n) => {
                            object_name_leaf(n)?
                        }
                    };
                    operations.push(AlterTableOp::RenameTable { new_name });
                }
                AlterTableOperation::AlterColumn { column_name, op } => match op {
                    AlterColumnOperation::SetDataType {
                        data_type,
                        using,
                        ..
                    } => {
                        if using.is_some() {
                            return Err(TakyonicError::Sql(
                                "ALTER COLUMN … TYPE … USING is not supported".into(),
                            ));
                        }
                        operations.push(AlterTableOp::SetDataType {
                            name: column_name.value.clone(),
                            data_type: canonicalize_sql_type(data_type),
                        });
                    }
                    other => {
                        return Err(TakyonicError::Sql(format!(
                            "unsupported ALTER COLUMN operation: {other}"
                        )));
                    }
                },
                other => {
                    return Err(TakyonicError::Sql(format!(
                        "unsupported ALTER TABLE operation: {other}"
                    )));
                }
            }
        }
        Ok(LogicalPlan::AlterTable { name, operations })
    }

    fn plan_drop_index(names: &[ObjectName], if_exists: bool) -> Result<LogicalPlan> {
        if names.len() != 1 {
            return Err(TakyonicError::Sql(
                "DROP INDEX requires exactly one index name".into(),
            ));
        }
        Ok(LogicalPlan::DropIndex {
            name: object_name_leaf(&names[0])?,
            if_exists,
        })
    }

    fn plan_drop_table(names: &[ObjectName], if_exists: bool) -> Result<LogicalPlan> {
        if names.len() != 1 {
            return Err(TakyonicError::Sql(
                "DROP TABLE requires exactly one table name".into(),
            ));
        }
        Ok(LogicalPlan::DropTable {
            name: object_name_leaf(&names[0])?,
            if_exists,
        })
    }

    fn plan_drop_role(names: &[ObjectName], if_exists: bool) -> Result<LogicalPlan> {
        if names.len() != 1 {
            return Err(TakyonicError::Sql(
                "DROP ROLE requires exactly one name".into(),
            ));
        }
        Ok(LogicalPlan::DropRole {
            name: object_name_leaf(&names[0])?,
            if_exists,
        })
    }

    fn plan_create_role(create: &CreateRole) -> Result<LogicalPlan> {
        if create.names.len() != 1 {
            return Err(TakyonicError::Sql(
                "CREATE ROLE supports exactly one name".into(),
            ));
        }
        let name = object_name_leaf(&create.names[0])?;
        let password = match &create.password {
            Some(Password::Password(expr)) => Some(expr_password_literal(expr)?),
            Some(Password::NullPassword) | None => None,
        };
        let can_login = create.login.unwrap_or(password.is_some());
        Ok(LogicalPlan::CreateRole {
            name,
            can_login,
            is_superuser: create.superuser.unwrap_or(false),
            password,
            if_not_exists: create.if_not_exists,
        })
    }

    fn plan_create_user(create: &CreateUser) -> Result<LogicalPlan> {
        Ok(LogicalPlan::CreateRole {
            name: create.name.value.clone(),
            can_login: true,
            is_superuser: false,
            password: None,
            if_not_exists: create.if_not_exists,
        })
    }

    fn plan_grant(grant: &Grant) -> Result<LogicalPlan> {
        let grantee = grantee_name(&grant.grantees)?;
        match grant.objects.as_ref() {
            Some(GrantObjects::Schemas(names)) if names.len() == 1 => {
                let schema = object_name_leaf(&names[0])?;
                let privileges = ast_privileges_to_schema(&grant.privileges)?;
                Ok(LogicalPlan::GrantSchema {
                    privileges,
                    schema,
                    grantee,
                })
            }
            _ => {
                let table = grant_object_table(grant.objects.as_ref())?;
                if let Some(specs) = ast_privileges_to_column(&grant.privileges)? {
                    Ok(LogicalPlan::GrantColumn {
                        specs,
                        table,
                        grantee,
                    })
                } else {
                    let privileges = ast_privileges_to_rbac(&grant.privileges)?;
                    Ok(LogicalPlan::Grant {
                        privileges,
                        table,
                        grantee,
                    })
                }
            }
        }
    }

    fn plan_revoke(revoke: &Revoke) -> Result<LogicalPlan> {
        let grantee = grantee_name(&revoke.grantees)?;
        match revoke.objects.as_ref() {
            Some(GrantObjects::Schemas(names)) if names.len() == 1 => {
                let schema = object_name_leaf(&names[0])?;
                let privileges = ast_privileges_to_schema(&revoke.privileges)?;
                Ok(LogicalPlan::RevokeSchema {
                    privileges,
                    schema,
                    grantee,
                })
            }
            _ => {
                let table = grant_object_table(revoke.objects.as_ref())?;
                if let Some(specs) = ast_privileges_to_column(&revoke.privileges)? {
                    Ok(LogicalPlan::RevokeColumn {
                        specs,
                        table,
                        grantee,
                    })
                } else {
                    let privileges = ast_privileges_to_rbac(&revoke.privileges)?;
                    Ok(LogicalPlan::Revoke {
                        privileges,
                        table,
                        grantee,
                    })
                }
            }
        }
    }
}

/// Convenience facade used by the Smart Client / engine SQL entrypoints.
pub struct SqlEngine;

impl SqlEngine {
    /// Parse SQL into a logical plan.
    pub fn plan(sql: &str) -> Result<LogicalPlan> {
        LogicalPlanner::plan(sql)
    }
}

/// Translate WHERE into CBO filters (literals only) + full expression tree.
#[allow(dead_code)]
#[allow(dead_code)]
fn plan_where(selection: Option<&Expr>) -> Result<(Vec<Filter>, Option<Expression>)> {
    plan_where_ctx(selection, &HashMap::new(), &[], &[])
}

fn plan_where_ctx(
    selection: Option<&Expr>,
    ctes: &HashMap<String, LogicalPlan>,
    outer_ref_scope: &[String],
    subquery_outer: &[String],
) -> Result<(Vec<Filter>, Option<Expression>)> {
    match selection {
        None => Ok((Vec::new(), None)),
        Some(expr) => {
            let predicate =
                expr_to_expression_ctx(expr, ctes, outer_ref_scope, subquery_outer)?;
            // Parameterized / subquery / complex WHERE: CBO gets no driving filter.
            let filters = if expression_has_subquery(&predicate) {
                Vec::new()
            } else {
                flatten_and_predicates(expr).unwrap_or_default()
            };
            Ok((filters, Some(predicate)))
        }
    }
}

/// Rough column names produced by a plan (for correlation heuristics).
fn collect_plan_output_hints(plan: &LogicalPlan) -> Vec<String> {
    match plan {
        LogicalPlan::Select { table, .. } => vec![table.clone()],
        LogicalPlan::Values { columns, .. } => columns.clone(),
        LogicalPlan::GenerateSeries {
            column,
            ordinality_column,
            ..
        } => {
            let mut v = vec![column.clone()];
            if let Some(o) = ordinality_column {
                v.push(o.clone());
            }
            v
        }
        LogicalPlan::Unnest {
            column,
            ordinality_column,
            ..
        } => {
            let mut v = vec![column.clone()];
            if let Some(o) = ordinality_column {
                v.push(o.clone());
            }
            v
        }
        LogicalPlan::JsonArrayElements {
            column,
            ordinality_column,
            ..
        } => {
            let mut v = vec![column.clone()];
            if let Some(o) = ordinality_column {
                v.push(o.clone());
            }
            v
        }
        LogicalPlan::JsonEach {
            key_column,
            value_column,
            ordinality_column,
            ..
        } => {
            let mut v = vec![key_column.clone(), value_column.clone()];
            if let Some(o) = ordinality_column {
                v.push(o.clone());
            }
            v
        }
        LogicalPlan::JsonObjectKeys {
            column,
            ordinality_column,
            ..
        } => {
            let mut v = vec![column.clone()];
            if let Some(o) = ordinality_column {
                v.push(o.clone());
            }
            v
        }
        LogicalPlan::RegexpSplitToTable {
            column,
            ordinality_column,
            ..
        }
        | LogicalPlan::RegexpMatches {
            column,
            ordinality_column,
            ..
        } => {
            let mut v = vec![column.clone()];
            if let Some(o) = ordinality_column {
                v.push(o.clone());
            }
            v
        }
        LogicalPlan::SubqueryAlias { alias, input } => {
            let mut v = vec![alias.clone()];
            v.extend(collect_plan_output_hints(input));
            v
        }
        LogicalPlan::Join { left, right, .. }
        | LogicalPlan::DistributedJoin { left, right, .. } => {
            let mut v = collect_plan_output_hints(left);
            v.extend(collect_plan_output_hints(right));
            v
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
        } => {
            let mut v = Vec::new();
            for g in group_exprs {
                if let Expression::Column(c) = g {
                    v.push(c.clone());
                }
            }
            for a in aggr_exprs {
                if let Some(c) = aggregate_result_column(a) {
                    v.push(c);
                }
            }
            v
        }
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Window { input, .. }
        | LogicalPlan::Distinct { input, .. }
        | LogicalPlan::DistinctOn { input, .. } => collect_plan_output_hints(input),
        LogicalPlan::Project { columns, .. } => columns.iter().map(|(n, _)| n.clone()).collect(),
        LogicalPlan::Union { left, .. } => collect_plan_output_hints(left),
        _ => Vec::new(),
    }
}

fn expression_has_subquery(expr: &Expression) -> bool {
    match expr {
        Expression::InSubquery { .. }
        | Expression::Exists { .. }
        | Expression::ScalarSubquery { .. } => true,
        Expression::BinaryOp { left, right, .. }
        | Expression::And { left, right }
        | Expression::Or { left, right }
        | Expression::Arith { left, right, .. }
        | Expression::VectorDistance { left, right, .. }
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
            expression_has_subquery(left) || expression_has_subquery(right)
        }
        Expression::InList { expr, .. } => expression_has_subquery(expr),
        Expression::AggregateFunction { args, filter, .. } => {
            args.iter().any(expression_has_subquery)
                || filter
                    .as_ref()
                    .is_some_and(|f| expression_has_subquery(f))
        }
        Expression::Array(items) => items.iter().any(expression_has_subquery),
        Expression::ArrayIndex { array, index } => {
            expression_has_subquery(array) || expression_has_subquery(index)
        }
        Expression::Case {
            when_then,
            else_result,
        } => {
            when_then.iter().any(|(c, r)| {
                expression_has_subquery(c) || expression_has_subquery(r)
            }) || else_result
                .as_ref()
                .is_some_and(|e| expression_has_subquery(e))
        }
        Expression::IsNull { expr, .. } | Expression::IsBoolTest { expr, .. } => {
            expression_has_subquery(expr)
        }
        Expression::IsDistinctFrom { left, right, .. }
        | Expression::QuantifiedCmp { left, right, .. }
        | Expression::NullIf { left, right } => {
            expression_has_subquery(left) || expression_has_subquery(right)
        }
        Expression::Coalesce(args) => args.iter().any(expression_has_subquery),
        Expression::Cast { expr, .. } | Expression::Not { expr } => {
            expression_has_subquery(expr)
        }
        Expression::ScalarFunction { args, .. } => args.iter().any(expression_has_subquery),
        Expression::Column(_)
        | Expression::OuterRef(_)
        | Expression::Literal(_)
        | Expression::Parameter(_) => false,
    }
}

/// Correlation heuristic: subquery body references a column name that appears in
/// the outer scope list but is not produced by the subquery's own plan hints.
#[allow(dead_code)]
fn is_correlated_to_outer(
    expr: &Expression,
    inner_plan: &LogicalPlan,
    outer_columns: &[String],
) -> bool {
    if outer_columns.is_empty() {
        return false;
    }
    let inner: std::collections::HashSet<String> =
        collect_plan_output_hints(inner_plan).into_iter().collect();
    let mut cols = Vec::new();
    walk_columns(expr, &mut |c| cols.push(c.to_string()));
    // Also walk predicates inside the subquery plan.
    walk_plan_columns(inner_plan, &mut |c| cols.push(c.to_string()));
    for c in cols {
        if outer_columns.iter().any(|o| o == &c) && !inner.contains(&c) {
            return true;
        }
    }
    false
}

fn walk_plan_columns(plan: &LogicalPlan, f: &mut dyn FnMut(&str)) {
    match plan {
        LogicalPlan::Select { predicate, .. } => {
            if let Some(p) = predicate {
                walk_columns(p, f);
            }
        }
        LogicalPlan::Filter { input, predicate } => {
            walk_columns(predicate, f);
            walk_plan_columns(input, f);
        }
        LogicalPlan::Join { left, right, on, .. }
        | LogicalPlan::DistributedJoin { left, right, on, .. } => {
            walk_columns(on, f);
            walk_plan_columns(left, f);
            walk_plan_columns(right, f);
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
        } => {
            for e in group_exprs.iter().chain(aggr_exprs.iter()) {
                walk_columns(e, f);
            }
            walk_plan_columns(input, f);
        }
        LogicalPlan::Sort { input, exprs } => {
            for e in exprs {
                walk_columns(&e.expr, f);
            }
            walk_plan_columns(input, f);
        }
        LogicalPlan::Limit { input, .. }
        | LogicalPlan::SubqueryAlias { input, .. }
        | LogicalPlan::Distinct { input, .. } => {
            walk_plan_columns(input, f);
        }
        LogicalPlan::DistinctOn { input, exprs } => {
            for e in exprs {
                walk_columns(e, f);
            }
            walk_plan_columns(input, f);
        }
        LogicalPlan::Union { left, right, .. } => {
            walk_plan_columns(left, f);
            walk_plan_columns(right, f);
        }
        LogicalPlan::Project { input, columns } => {
            for (_, e) in columns {
                walk_columns(e, f);
            }
            walk_plan_columns(input, f);
        }
        LogicalPlan::Window { input, calls } => {
            for c in calls {
                for p in &c.partition_by {
                    walk_columns(p, f);
                }
                for s in &c.order_by {
                    walk_columns(&s.expr, f);
                }
                if let Some(v) = &c.value {
                    walk_columns(v, f);
                }
                if let Some(d) = &c.default_value {
                    walk_columns(d, f);
                }
            }
            walk_plan_columns(input, f);
        }
        LogicalPlan::Explain { plan } => walk_plan_columns(plan, f),
        _ => {}
    }
}

fn walk_columns(expr: &Expression, f: &mut dyn FnMut(&str)) {
    match expr {
        Expression::Column(c) => f(c),
        Expression::BinaryOp { left, right, .. }
        | Expression::And { left, right }
        | Expression::Or { left, right }
        | Expression::Arith { left, right, .. }
        | Expression::VectorDistance { left, right, .. }
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
            walk_columns(left, f);
            walk_columns(right, f);
        }
        Expression::InSubquery { expr, .. } | Expression::InList { expr, .. } => {
            walk_columns(expr, f);
        }
        Expression::Array(items) => {
            for a in items {
                walk_columns(a, f);
            }
        }
        Expression::ArrayIndex { array, index } => {
            walk_columns(array, f);
            walk_columns(index, f);
        }
        Expression::Case {
            when_then,
            else_result,
        } => {
            for (cond, result) in when_then {
                walk_columns(cond, f);
                walk_columns(result, f);
            }
            if let Some(e) = else_result {
                walk_columns(e, f);
            }
        }
        Expression::IsNull { expr, .. }
        | Expression::IsBoolTest { expr, .. }
        | Expression::Cast { expr, .. }
        | Expression::Not { expr } => {
            walk_columns(expr, f);
        }
        Expression::IsDistinctFrom { left, right, .. }
        | Expression::QuantifiedCmp { left, right, .. }
        | Expression::NullIf { left, right } => {
            walk_columns(left, f);
            walk_columns(right, f);
        }
        Expression::ScalarFunction { args, .. }
        | Expression::Coalesce(args) => {
            for a in args {
                walk_columns(a, f);
            }
        }
        Expression::AggregateFunction { args, filter, .. } => {
            for a in args {
                walk_columns(a, f);
            }
            if let Some(pred) = filter {
                walk_columns(pred, f);
            }
        }
        Expression::Exists { .. }
        | Expression::ScalarSubquery { .. }
        | Expression::OuterRef(_)
        | Expression::Literal(_)
        | Expression::Parameter(_) => {}
    }
}

/// Rewrite `CREATE VECTOR INDEX` / `CREATE USER … PASSWORD` into forms sqlparser accepts.
fn preprocess_sql(sql: &str) -> String {
    let trimmed = sql.trim_start();
    let upper = trimmed.to_ascii_uppercase();
    let base = if let Some(rest) = upper.strip_prefix("CREATE VECTOR INDEX") {
        let orig_rest = &trimmed[trimmed.len() - rest.len()..];
        format!("CREATE INDEX{orig_rest}")
    } else if let Some(rest) = upper.strip_prefix("EXPLAIN CREATE VECTOR INDEX") {
        let orig_rest = &trimmed[trimmed.len() - rest.len()..];
        format!("EXPLAIN CREATE INDEX{orig_rest}")
    } else if let Some(rewritten) = rewrite_create_user(trimmed) {
        rewritten
    } else {
        sql.to_string()
    };
    rewrite_window_exclude(&base)
}

/// `CREATE USER name WITH PASSWORD 'x' [SUPERUSER]` → `CREATE ROLE name WITH LOGIN PASSWORD 'x' [SUPERUSER]`.
fn rewrite_create_user(sql: &str) -> Option<String> {
    let upper = sql.to_ascii_uppercase();
    if !upper.starts_with("CREATE USER ") || !upper.contains("PASSWORD") {
        return None;
    }
    // Strip leading CREATE USER
    let rest = sql["CREATE USER ".len()..].trim_start();
    // Insert LOGIN after WITH when present, else after name.
    if let Some(idx) = rest.to_ascii_uppercase().find(" WITH ") {
        let (name, opts) = rest.split_at(idx);
        // opts starts with " WITH ..."
        let opts_upper = opts.to_ascii_uppercase();
        if opts_upper.contains(" LOGIN") {
            return Some(format!("CREATE ROLE {name}{opts}"));
        }
        // WITH PASSWORD → WITH LOGIN PASSWORD
        let opts2 = opts.replacen(" WITH ", " WITH LOGIN ", 1);
        Some(format!("CREATE ROLE {}{}", name.trim(), opts2))
    } else {
        // CREATE USER name PASSWORD 'x'
        Some(format!("CREATE ROLE {rest} LOGIN"))
    }
}

/// Sentinel PARTITION BY literal used to carry `EXCLUDE` through sqlparser (which
/// does not yet model window-frame EXCLUDE).
const TK_EXCLUDE_PREFIX: &str = "__tk_exclude:";

fn frame_exclude_sentinel(ex: FrameExclude) -> &'static str {
    match ex {
        FrameExclude::NoOthers => "__tk_exclude:no_others__",
        FrameExclude::CurrentRow => "__tk_exclude:current_row__",
        FrameExclude::Group => "__tk_exclude:group__",
        FrameExclude::Ties => "__tk_exclude:ties__",
    }
}

fn parse_frame_exclude_sentinel(s: &str) -> Option<FrameExclude> {
    match s {
        "__tk_exclude:no_others__" => Some(FrameExclude::NoOthers),
        "__tk_exclude:current_row__" => Some(FrameExclude::CurrentRow),
        "__tk_exclude:group__" => Some(FrameExclude::Group),
        "__tk_exclude:ties__" => Some(FrameExclude::Ties),
        _ => None,
    }
}

fn exclude_from_ast_expr(expr: &Expr) -> Option<FrameExclude> {
    match expr {
        Expr::Value(ValueWithSpan {
            value: SqlValue::SingleQuotedString(s),
            ..
        }) if s.starts_with(TK_EXCLUDE_PREFIX) => parse_frame_exclude_sentinel(s),
        _ => None,
    }
}

fn peel_exclude_from_partition(exprs: &mut Vec<Expr>) -> FrameExclude {
    let mut exclude = FrameExclude::NoOthers;
    exprs.retain(|e| match exclude_from_ast_expr(e) {
        Some(ex) => {
            exclude = ex;
            false
        }
        None => true,
    });
    exclude
}

fn reinject_exclude_partition(mut exprs: Vec<Expr>, exclude: FrameExclude) -> Vec<Expr> {
    if exclude == FrameExclude::NoOthers {
        return exprs;
    }
    let lit = Expr::Value(ValueWithSpan {
        value: SqlValue::SingleQuotedString(frame_exclude_sentinel(exclude).into()),
        span: sqlparser::tokenizer::Span::empty(),
    });
    exprs.insert(0, lit);
    exprs
}

/// Rewrite PG `EXCLUDE …` inside window specs into a PARTITION BY sentinel literal
/// so sqlparser can accept the statement.
fn rewrite_window_exclude(sql: &str) -> String {
    let mut matches = Vec::new();
    let bytes = sql.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        if bytes[i] == b'\'' || bytes[i] == b'"' {
            let quote = bytes[i];
            i += 1;
            while i < bytes.len() {
                if bytes[i] == quote {
                    i += 1;
                    break;
                }
                if bytes[i] == b'\\' {
                    i = (i + 2).min(bytes.len());
                    continue;
                }
                i += 1;
            }
            continue;
        }
        if matches_ci(sql, i, "EXCLUDE") && is_ident_boundary(sql, i, "EXCLUDE".len()) {
            let after = skip_ws(sql, i + "EXCLUDE".len());
            if let Some((ex, end)) = parse_exclude_option(sql, after) {
                if let Some(open) = find_enclosing_lparen(sql, i) {
                    matches.push((open, i, end, ex));
                    i = end;
                    continue;
                }
            }
        }
        i += 1;
    }
    if matches.is_empty() {
        return sql.to_string();
    }
    let mut out = sql.to_string();
    for (open, ex_start, ex_end, ex) in matches.into_iter().rev() {
        out.replace_range(ex_start..ex_end, "");
        if let Some(close) = find_matching_rparen(&out, open) {
            let body = out[open + 1..close].to_string();
            let new_body = inject_exclude_into_window_body(&body, ex);
            out.replace_range(open + 1..close, &new_body);
        }
    }
    out
}

fn matches_ci(sql: &str, i: usize, kw: &str) -> bool {
    let end = i + kw.len();
    end <= sql.len() && sql.as_bytes()[i..end].eq_ignore_ascii_case(kw.as_bytes())
}

fn is_ident_boundary(sql: &str, start: usize, len: usize) -> bool {
    let before_ok = start == 0
        || !sql.as_bytes()[start - 1].is_ascii_alphanumeric() && sql.as_bytes()[start - 1] != b'_';
    let end = start + len;
    let after_ok = end >= sql.len()
        || !sql.as_bytes()[end].is_ascii_alphanumeric() && sql.as_bytes()[end] != b'_';
    before_ok && after_ok
}

fn skip_ws(sql: &str, mut i: usize) -> usize {
    while i < sql.len() && sql.as_bytes()[i].is_ascii_whitespace() {
        i += 1;
    }
    i
}

fn parse_exclude_option(sql: &str, i: usize) -> Option<(FrameExclude, usize)> {
    if matches_ci(sql, i, "CURRENT") {
        let j = skip_ws(sql, i + "CURRENT".len());
        if matches_ci(sql, j, "ROW") && is_ident_boundary(sql, j, 3) {
            return Some((FrameExclude::CurrentRow, j + 3));
        }
    }
    if matches_ci(sql, i, "GROUP") && is_ident_boundary(sql, i, 5) {
        return Some((FrameExclude::Group, i + 5));
    }
    if matches_ci(sql, i, "TIES") && is_ident_boundary(sql, i, 4) {
        return Some((FrameExclude::Ties, i + 4));
    }
    if matches_ci(sql, i, "NO") {
        let j = skip_ws(sql, i + 2);
        if matches_ci(sql, j, "OTHERS") && is_ident_boundary(sql, j, 6) {
            return Some((FrameExclude::NoOthers, j + 6));
        }
    }
    None
}

fn find_enclosing_lparen(sql: &str, from: usize) -> Option<usize> {
    let bytes = sql.as_bytes();
    let mut depth = 0i32;
    let mut i = from;
    while i > 0 {
        i -= 1;
        match bytes[i] {
            b')' => depth += 1,
            b'(' => {
                if depth == 0 {
                    return Some(i);
                }
                depth -= 1;
            }
            b'\'' | b'"' => {
                let quote = bytes[i];
                while i > 0 {
                    i -= 1;
                    if bytes[i] == quote {
                        // check escaped ''
                        if i > 0 && bytes[i - 1] == quote {
                            i -= 1;
                            continue;
                        }
                        break;
                    }
                }
            }
            _ => {}
        }
    }
    None
}

fn find_matching_rparen(sql: &str, open: usize) -> Option<usize> {
    let bytes = sql.as_bytes();
    let mut depth = 0i32;
    let mut i = open;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' | b'"' => {
                let quote = bytes[i];
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == quote {
                        i += 1;
                        break;
                    }
                    i += 1;
                }
                continue;
            }
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

fn inject_exclude_into_window_body(body: &str, exclude: FrameExclude) -> String {
    if exclude == FrameExclude::NoOthers {
        return body.to_string();
    }
    let lit = format!("'{}'", frame_exclude_sentinel(exclude));
    let ws_len = body.len() - body.trim_start().len();
    let ws = &body[..ws_len];
    let rest = body.trim_start();

    let (name_prefix, after_name) = match take_leading_ident(rest) {
        Some((ident, rem)) => {
            let rem_trim = rem.trim_start();
            let u = rem_trim.to_ascii_uppercase();
            if rem_trim.is_empty()
                || u.starts_with("PARTITION")
                || u.starts_with("ORDER")
                || u.starts_with("ROWS")
                || u.starts_with("RANGE")
                || u.starts_with("GROUPS")
            {
                (format!("{ident} "), rem_trim)
            } else {
                (String::new(), rest)
            }
        }
        None => (String::new(), rest),
    };

    let after = after_name.trim_start();
    let upper = after.to_ascii_uppercase();
    if upper.starts_with("PARTITION BY") {
        let pb_rest = after["PARTITION BY".len()..].trim_start();
        // Preserve original casing of PARTITION BY by rewriting from `after`.
        let pb_kw_end = after
            .char_indices()
            .skip_while(|(_, c)| c.is_ascii_whitespace())
            .map(|(i, _)| i)
            .next()
            .unwrap_or(0);
        let _ = pb_kw_end;
        format!("{ws}{name_prefix}PARTITION BY {lit}, {pb_rest}")
    } else if after.is_empty() {
        format!("{ws}{name_prefix}PARTITION BY {lit}")
    } else {
        format!("{ws}{name_prefix}PARTITION BY {lit} {after}")
    }
}

fn take_leading_ident(s: &str) -> Option<(&str, &str)> {
    let bytes = s.as_bytes();
    if bytes.is_empty() {
        return None;
    }
    if bytes[0] == b'"' {
        let mut i = 1usize;
        while i < bytes.len() && bytes[i] != b'"' {
            i += 1;
        }
        if i < bytes.len() {
            i += 1;
        }
        return Some((&s[..i], &s[i..]));
    }
    if !bytes[0].is_ascii_alphabetic() && bytes[0] != b'_' {
        return None;
    }
    let mut i = 1usize;
    while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
        i += 1;
    }
    let ident = &s[..i];
    let upper = ident.to_ascii_uppercase();
    match upper.as_str() {
        "PARTITION" | "ORDER" | "ROWS" | "RANGE" | "GROUPS" | "EXCLUDE" | "BETWEEN" | "UNBOUNDED"
        | "CURRENT" | "PRECEDING" | "FOLLOWING" | "AND" | "NO" | "OTHERS" | "GROUP" | "TIES" => {
            None
        }
        _ => Some((ident, &s[i..])),
    }
}

/// Detect `GRANT <rolename> TO <member>` (role membership, not table privilege).
fn try_parse_grant_role_membership(sql: &str) -> Option<(String, String)> {
    let upper = sql.to_ascii_uppercase();
    if !upper.starts_with("GRANT ") || !upper.contains(" TO ") {
        return None;
    }
    // Privilege grants contain ON; membership grants do not.
    if upper.contains(" ON ") {
        return None;
    }
    let body = &sql["GRANT ".len()..];
    let to_idx = body.to_ascii_uppercase().find(" TO ")?;
    let role = body[..to_idx].trim().to_string();
    let member = body[to_idx + 4..].trim().trim_end_matches(';').trim().to_string();
    // Reject if role looks like a privilege keyword.
    let role_up = role.to_ascii_uppercase();
    if matches!(
        role_up.as_str(),
        "SELECT" | "INSERT" | "UPDATE" | "DELETE" | "ALL" | "ALL PRIVILEGES"
    ) {
        return None;
    }
    if role.is_empty() || member.is_empty() || role.contains(',') {
        return None;
    }
    Some((role, member))
}

/// Parse `LISTEN name` / `UNLISTEN name|*` (sqlparser 0.62 has no Listen AST).
fn try_parse_listen_unlisten(sql: &str) -> Option<LogicalPlan> {
    let s = sql.trim().trim_end_matches(';').trim();
    let upper = s.to_ascii_uppercase();
    if let Some(rest) = upper.strip_prefix("LISTEN ") {
        let _ = rest;
        let channel = s["LISTEN ".len()..].trim().trim_matches('"');
        if channel.is_empty() || channel.contains(char::is_whitespace) {
            return None;
        }
        return Some(LogicalPlan::Listen {
            channel: channel.to_string(),
        });
    }
    if let Some(rest) = upper.strip_prefix("UNLISTEN ") {
        let _ = rest;
        let channel = s["UNLISTEN ".len()..].trim().trim_matches('"');
        if channel == "*" {
            return Some(LogicalPlan::Unlisten { channel: None });
        }
        if channel.is_empty() || channel.contains(char::is_whitespace) {
            return None;
        }
        return Some(LogicalPlan::Unlisten {
            channel: Some(channel.to_string()),
        });
    }
    None
}

/// Parse `NOTIFY channel` / `NOTIFY channel, 'payload'` (sqlparser 0.62 has no Notify AST).
fn try_parse_notify(sql: &str) -> Option<LogicalPlan> {
    let s = sql.trim().trim_end_matches(';').trim();
    let upper = s.to_ascii_uppercase();
    if !upper.starts_with("NOTIFY ") {
        return None;
    }
    let rest = s["NOTIFY ".len()..].trim();
    if rest.is_empty() {
        return None;
    }
    let (channel_raw, payload) = if let Some(idx) = rest.find(',') {
        let ch = rest[..idx].trim();
        let pay = rest[idx + 1..].trim();
        let payload = parse_notify_payload(pay)?;
        (ch, payload)
    } else {
        (rest, String::new())
    };
    let channel = channel_raw.trim_matches('"');
    if channel.is_empty() || channel.contains(char::is_whitespace) {
        return None;
    }
    Some(LogicalPlan::Notify {
        channel: channel.to_string(),
        payload,
    })
}

/// Parse `REBALANCE TABLE name` (ops command; not in sqlparser).
fn try_parse_rebalance(sql: &str) -> Option<LogicalPlan> {
    let s = sql.trim().trim_end_matches(';').trim();
    let upper = s.to_ascii_uppercase();
    if !upper.starts_with("REBALANCE ") {
        return None;
    }
    let rest = s["REBALANCE ".len()..].trim();
    let rest_u = rest.to_ascii_uppercase();
    let table_part = if rest_u.starts_with("TABLE ") {
        rest["TABLE ".len()..].trim()
    } else {
        rest
    };
    let table = table_part
        .trim_matches('"')
        .split_whitespace()
        .next()?
        .to_string();
    if table.is_empty() {
        return None;
    }
    Some(LogicalPlan::Rebalance { table })
}

fn parse_notify_payload(raw: &str) -> Option<String> {
    let t = raw.trim();
    if t.is_empty() {
        return Some(String::new());
    }
    if (t.starts_with('\'') && t.ends_with('\'')) || (t.starts_with('"') && t.ends_with('"')) {
        let inner = &t[1..t.len() - 1];
        return Some(inner.replace("''", "'"));
    }
    None
}

/// Parse `CREATE SEQUENCE` / `DROP SEQUENCE` (minimal options).
fn try_parse_create_drop_sequence(sql: &str) -> Option<LogicalPlan> {
    let s = sql.trim().trim_end_matches(';').trim();
    let upper = s.to_ascii_uppercase();
    if let Some(rest_u) = upper.strip_prefix("CREATE SEQUENCE ") {
        let rest = s["CREATE SEQUENCE ".len()..].trim();
        let (if_not_exists, rest) = if rest_u.starts_with("IF NOT EXISTS ") {
            (true, rest["IF NOT EXISTS ".len()..].trim())
        } else {
            (false, rest)
        };
        let mut start = 1_i64;
        let mut increment = 1_i64;
        let orig: Vec<&str> = rest.split_whitespace().collect();
        let name = orig.first()?.trim_matches('"');
        if name.is_empty() {
            return None;
        }
        let mut i = 1; // skip name
        while i < orig.len() {
            let u = orig[i].to_ascii_uppercase();
            match u.as_str() {
                "START" => {
                    i += 1;
                    if i < orig.len() && orig[i].eq_ignore_ascii_case("WITH") {
                        i += 1;
                    }
                    start = orig.get(i)?.parse().ok()?;
                    i += 1;
                }
                "INCREMENT" => {
                    i += 1;
                    if i < orig.len() && orig[i].eq_ignore_ascii_case("BY") {
                        i += 1;
                    }
                    increment = orig.get(i)?.parse().ok()?;
                    i += 1;
                }
                "AS" => {
                    // AS integer|bigint — ignore type token
                    i += 2;
                }
                _ => return None,
            }
        }
        return Some(LogicalPlan::CreateSequence {
            name: name.to_string(),
            if_not_exists,
            start,
            increment,
        });
    }
    if let Some(rest_u) = upper.strip_prefix("DROP SEQUENCE ") {
        let rest = s["DROP SEQUENCE ".len()..].trim();
        let (if_exists, rest) = if rest_u.starts_with("IF EXISTS ") {
            (true, rest["IF EXISTS ".len()..].trim())
        } else {
            (false, rest)
        };
        let name = rest.split_whitespace().next()?.trim_matches('"');
        if name.is_empty() || name.contains(',') {
            return None;
        }
        return Some(LogicalPlan::DropSequence {
            name: name.to_string(),
            if_exists,
        });
    }
    None
}

/// Parse `ALTER SEQUENCE name [RESTART [WITH] n] [INCREMENT [BY] n] [OWNED BY …]`.
fn try_parse_alter_sequence(sql: &str) -> Option<LogicalPlan> {
    let s = sql.trim().trim_end_matches(';').trim();
    let upper = s.to_ascii_uppercase();
    if !upper.starts_with("ALTER SEQUENCE ") {
        return None;
    }
    let rest = s["ALTER SEQUENCE ".len()..].trim();
    let orig: Vec<&str> = rest.split_whitespace().collect();
    let name = orig.first()?.trim_matches('"');
    if name.is_empty() {
        return None;
    }
    let mut restart = None;
    let mut increment = None;
    let mut owned_by = None;
    let mut rename_to = None;
    let mut i = 1;
    while i < orig.len() {
        let u = orig[i].to_ascii_uppercase();
        match u.as_str() {
            "RESTART" => {
                i += 1;
                if i < orig.len() && orig[i].eq_ignore_ascii_case("WITH") {
                    i += 1;
                }
                restart = Some(orig.get(i)?.parse().ok()?);
                i += 1;
            }
            "INCREMENT" => {
                i += 1;
                if i < orig.len() && orig[i].eq_ignore_ascii_case("BY") {
                    i += 1;
                }
                increment = Some(orig.get(i)?.parse().ok()?);
                i += 1;
            }
            "OWNED" => {
                i += 1;
                if i >= orig.len() || !orig[i].eq_ignore_ascii_case("BY") {
                    return None;
                }
                i += 1;
                let target = orig.get(i)?;
                if target.eq_ignore_ascii_case("NONE") {
                    owned_by = Some(None);
                } else {
                    let (table, col) = target.split_once('.')?;
                    let table = table.trim_matches('"');
                    let col = col.trim_matches('"');
                    if table.is_empty() || col.is_empty() {
                        return None;
                    }
                    owned_by = Some(Some((table.to_string(), col.to_string())));
                }
                i += 1;
            }
            "RENAME" => {
                i += 1;
                if i >= orig.len() || !orig[i].eq_ignore_ascii_case("TO") {
                    return None;
                }
                i += 1;
                let new_name = orig.get(i)?.trim_matches('"');
                if new_name.is_empty() {
                    return None;
                }
                rename_to = Some(new_name.to_string());
                i += 1;
            }
            _ => return None,
        }
    }
    if restart.is_none() && increment.is_none() && owned_by.is_none() && rename_to.is_none() {
        return None;
    }
    Some(LogicalPlan::AlterSequence {
        name: name.to_string(),
        restart,
        increment,
        owned_by,
        rename_to,
    })
}

fn expr_password_literal(expr: &Expr) -> Result<String> {
    match expr {
        Expr::Value(ValueWithSpan {
            value: SqlValue::SingleQuotedString(s),
            ..
        }) => Ok(s.clone()),
        other => Err(TakyonicError::Sql(format!(
            "PASSWORD expects a string literal, got {other}"
        ))),
    }
}

fn ast_privileges_to_rbac(privs: &AstPrivileges) -> Result<Vec<Privilege>> {
    match privs {
        AstPrivileges::All { .. } => Ok(vec![
            Privilege::Select,
            Privilege::Insert,
            Privilege::Update,
            Privilege::Delete,
        ]),
        AstPrivileges::Actions(actions) => {
            let mut out = Vec::new();
            for a in actions {
                match a {
                    Action::Select { .. } => out.push(Privilege::Select),
                    Action::Insert { .. } => out.push(Privilege::Insert),
                    Action::Update { .. } => out.push(Privilege::Update),
                    Action::Delete => out.push(Privilege::Delete),
                    other => {
                        return Err(TakyonicError::Sql(format!(
                            "unsupported privilege action: {other:?}"
                        )));
                    }
                }
            }
            if out.is_empty() {
                return Err(TakyonicError::Sql("GRANT/REVOKE requires privileges".into()));
            }
            Ok(out)
        }
    }
}

fn ast_privileges_to_schema(privs: &AstPrivileges) -> Result<Vec<crate::rbac::SchemaPrivilege>> {
    match privs {
        AstPrivileges::All { .. } => Ok(vec![
            crate::rbac::SchemaPrivilege::Usage,
            crate::rbac::SchemaPrivilege::Create,
        ]),
        AstPrivileges::Actions(actions) => {
            let mut out = Vec::new();
            for a in actions {
                match a {
                    Action::Usage => out.push(crate::rbac::SchemaPrivilege::Usage),
                    Action::Create { .. } => out.push(crate::rbac::SchemaPrivilege::Create),
                    other => {
                        return Err(TakyonicError::Sql(format!(
                            "unsupported schema privilege action: {other:?}"
                        )));
                    }
                }
            }
            if out.is_empty() {
                return Err(TakyonicError::Sql(
                    "GRANT/REVOKE ON SCHEMA requires privileges".into(),
                ));
            }
            Ok(out)
        }
    }
}

/// Column-targeted privileges (`SELECT (a,b)`). `None` → table-level GRANT.
fn ast_privileges_to_column(
    privs: &AstPrivileges,
) -> Result<Option<Vec<crate::rbac::ColumnGrantSpec>>> {
    match privs {
        AstPrivileges::All { .. } => Ok(None),
        AstPrivileges::Actions(actions) => {
            let mut specs = Vec::new();
            let mut saw_columns = false;
            let mut saw_table = false;
            for a in actions {
                let (priv_, cols) = match a {
                    Action::Select { columns } => (crate::rbac::ColumnPrivilege::Select, columns),
                    Action::Insert { columns } => (crate::rbac::ColumnPrivilege::Insert, columns),
                    Action::Update { columns } => (crate::rbac::ColumnPrivilege::Update, columns),
                    Action::References { columns } => {
                        (crate::rbac::ColumnPrivilege::References, columns)
                    }
                    Action::Delete => {
                        saw_table = true;
                        continue;
                    }
                    other => {
                        return Err(TakyonicError::Sql(format!(
                            "unsupported privilege action: {other:?}"
                        )));
                    }
                };
                match cols {
                    Some(idents) if !idents.is_empty() => {
                        saw_columns = true;
                        specs.push(crate::rbac::ColumnGrantSpec {
                            privilege: priv_,
                            columns: idents.iter().map(|i| i.value.clone()).collect(),
                        });
                    }
                    _ => {
                        saw_table = true;
                    }
                }
            }
            if saw_columns && saw_table {
                return Err(TakyonicError::Sql(
                    "GRANT/REVOKE cannot mix table-level and column-level privileges".into(),
                ));
            }
            if saw_columns {
                Ok(Some(specs))
            } else {
                Ok(None)
            }
        }
    }
}

fn grant_object_table(objects: Option<&GrantObjects>) -> Result<String> {
    match objects {
        Some(GrantObjects::Tables(names)) if names.len() == 1 => object_name_leaf(&names[0]),
        Some(other) => Err(TakyonicError::Sql(format!(
            "GRANT/REVOKE supports a single table, got {other:?}"
        ))),
        None => Err(TakyonicError::Sql(
            "GRANT/REVOKE requires ON <table>".into(),
        )),
    }
}

fn grantee_name(grantees: &[Grantee]) -> Result<String> {
    if grantees.len() != 1 {
        return Err(TakyonicError::Sql(
            "GRANT/REVOKE supports exactly one grantee".into(),
        ));
    }
    match &grantees[0].name {
        Some(GranteeName::ObjectName(name)) => object_name_leaf(name),
        Some(GranteeName::UserHost { user, .. }) => Ok(user.value.clone()),
        None => Err(TakyonicError::Sql("missing grantee name".into())),
    }
}

/// Parse `WITH (DIMENSION=n, TYPE=HNSW, METRIC=…)` options into a vector spec.
fn parse_vector_index_with(with: &[Expr]) -> Result<Option<VectorIndexSpec>> {
    if with.is_empty() {
        return Ok(None);
    }
    let mut dimension: Option<usize> = None;
    let mut metric = DistanceMetric::Euclidean;
    let mut index_type = String::from("HNSW");
    let mut is_vector = false;
    for opt in with {
        match opt {
            Expr::BinaryOp {
                left,
                op: BinaryOperator::Eq,
                right,
            } => {
                let key = match left.as_ref() {
                    Expr::Identifier(i) => i.value.to_ascii_uppercase(),
                    _ => continue,
                };
                match key.as_str() {
                    "DIMENSION" | "DIMS" | "DIM" => {
                        let dim = match right.as_ref() {
                            Expr::Value(ValueWithSpan {
                                value: SqlValue::Number(n, _),
                                ..
                            }) => n.parse::<usize>().map_err(|_| {
                                TakyonicError::Sql(format!("invalid DIMENSION `{n}`"))
                            })?,
                            other => {
                                return Err(TakyonicError::Sql(format!(
                                    "DIMENSION expects a number, got {other}"
                                )));
                            }
                        };
                        dimension = Some(dim);
                        is_vector = true;
                    }
                    "TYPE" => {
                        let t = match right.as_ref() {
                            Expr::Identifier(i) => i.value.to_ascii_uppercase(),
                            Expr::Value(ValueWithSpan {
                                value: SqlValue::SingleQuotedString(s),
                                ..
                            }) => s.to_ascii_uppercase(),
                            other => {
                                return Err(TakyonicError::Sql(format!(
                                    "TYPE expects an identifier, got {other}"
                                )));
                            }
                        };
                        if t == "HNSW" {
                            is_vector = true;
                            index_type = t;
                        } else {
                            return Err(TakyonicError::Sql(format!(
                                "unsupported vector index TYPE `{t}` (expected HNSW)"
                            )));
                        }
                    }
                    "METRIC" | "DISTANCE" => {
                        let m = match right.as_ref() {
                            Expr::Identifier(i) => i.value.clone(),
                            Expr::Value(ValueWithSpan {
                                value: SqlValue::SingleQuotedString(s),
                                ..
                            }) => s.clone(),
                            other => {
                                return Err(TakyonicError::Sql(format!(
                                    "METRIC expects an identifier, got {other}"
                                )));
                            }
                        };
                        metric = DistanceMetric::parse(&m)?;
                        is_vector = true;
                    }
                    _ => {}
                }
            }
            _ => {}
        }
    }
    if !is_vector {
        return Ok(None);
    }
    let dimension = dimension.ok_or_else(|| {
        TakyonicError::Sql("vector index WITH clause requires DIMENSION=<n>".into())
    })?;
    Ok(Some(VectorIndexSpec {
        dimension,
        metric,
        index_type,
    }))
}

/// Parse `ON CONFLICT` (`DO NOTHING` / `DO UPDATE SET …`).
fn plan_on_conflict(on: Option<&sqlparser::ast::OnInsert>) -> Result<Option<OnConflict>> {
    match on {
        None => Ok(None),
        Some(sqlparser::ast::OnInsert::OnConflict(oc)) => match &oc.action {
            sqlparser::ast::OnConflictAction::DoNothing => Ok(Some(OnConflict::DoNothing)),
            sqlparser::ast::OnConflictAction::DoUpdate(du) => {
                if du.assignments.is_empty() {
                    return Err(TakyonicError::Sql(
                        "ON CONFLICT DO UPDATE requires SET assignments".into(),
                    ));
                }
                let mut assignments = Vec::with_capacity(du.assignments.len());
                for assignment in &du.assignments {
                    let col = match &assignment.target {
                        AssignmentTarget::ColumnName(name) => object_name_leaf(name)?,
                        AssignmentTarget::Tuple(_) => {
                            return Err(TakyonicError::Sql(
                                "tuple ON CONFLICT DO UPDATE assignments are unsupported".into(),
                            ));
                        }
                    };
                    let rewritten = rewrite_excluded_expr(&assignment.value)?;
                    assignments.push((col, expr_to_expression(&rewritten)?));
                }
                let selection = match &du.selection {
                    Some(expr) => {
                        let rewritten = rewrite_excluded_expr(expr)?;
                        Some(expr_to_expression(&rewritten)?)
                    }
                    None => None,
                };
                Ok(Some(OnConflict::DoUpdate {
                    assignments,
                    selection,
                }))
            }
        },
        Some(other) => Err(TakyonicError::Sql(format!(
            "unsupported ON INSERT clause: {other}"
        ))),
    }
}

/// Rewrite `EXCLUDED.col` → identifier `__excluded.col` for upsert expression planning.
fn rewrite_excluded_expr(expr: &Expr) -> Result<Expr> {
    match expr {
        Expr::CompoundIdentifier(parts)
            if parts.len() >= 2 && parts[0].value.eq_ignore_ascii_case("EXCLUDED") =>
        {
            let col = &parts[parts.len() - 1].value;
            Ok(Expr::Identifier(Ident::new(format!(
                "{EXCLUDED_FIELD_PREFIX}{col}"
            ))))
        }
        Expr::Nested(inner) => Ok(Expr::Nested(Box::new(rewrite_excluded_expr(inner)?))),
        Expr::UnaryOp { op, expr: inner } => Ok(Expr::UnaryOp {
            op: op.clone(),
            expr: Box::new(rewrite_excluded_expr(inner)?),
        }),
        Expr::BinaryOp { left, op, right } => Ok(Expr::BinaryOp {
            left: Box::new(rewrite_excluded_expr(left)?),
            op: op.clone(),
            right: Box::new(rewrite_excluded_expr(right)?),
        }),
        Expr::Cast {
            kind,
            expr: inner,
            data_type,
            format,
            array,
        } => Ok(Expr::Cast {
            kind: kind.clone(),
            expr: Box::new(rewrite_excluded_expr(inner)?),
            data_type: data_type.clone(),
            format: format.clone(),
            array: *array,
        }),
        Expr::Case {
            operand,
            conditions,
            else_result,
            case_token,
            end_token,
        } => {
            let operand = match operand {
                Some(o) => Some(Box::new(rewrite_excluded_expr(o)?)),
                None => None,
            };
            let mut new_conds = Vec::with_capacity(conditions.len());
            for c in conditions {
                new_conds.push(sqlparser::ast::CaseWhen {
                    condition: rewrite_excluded_expr(&c.condition)?,
                    result: rewrite_excluded_expr(&c.result)?,
                });
            }
            let else_result = match else_result {
                Some(e) => Some(Box::new(rewrite_excluded_expr(e)?)),
                None => None,
            };
            Ok(Expr::Case {
                operand,
                conditions: new_conds,
                else_result,
                case_token: case_token.clone(),
                end_token: end_token.clone(),
            })
        }
        other => Ok(other.clone()),
    }
}

/// Parse a RETURNING clause into [`Returning`].
fn plan_returning_clause(items: Option<&[SelectItem]>) -> Result<Option<Returning>> {
    let Some(items) = items else {
        return Ok(None);
    };
    if items.is_empty() {
        return Ok(None);
    }
    let mut list = Vec::new();
    let mut saw_star = false;
    for item in items {
        match item {
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {
                saw_star = true;
            }
            SelectItem::UnnamedExpr(e) => {
                let planned = expr_to_expression(e)?;
                let name = projection_output_name(e, &planned)?;
                list.push((name, planned));
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                let planned = expr_to_expression(expr)?;
                list.push((alias.value.clone(), planned));
            }
            SelectItem::ExprWithAliases { .. } => {
                return Err(TakyonicError::Sql(
                    "multi-alias RETURNING expressions are unsupported".into(),
                ));
            }
        }
    }
    if saw_star {
        if !list.is_empty() {
            return Err(TakyonicError::Sql(
                "RETURNING * cannot be mixed with other expressions".into(),
            ));
        }
        return Ok(Some(Returning::Star));
    }
    if list.is_empty() {
        return Ok(None);
    }
    Ok(Some(Returning::List(list)))
}

/// First projected column name of a SELECT list (for IN/scalar subquery keys).
fn first_projection_column(
    select: &sqlparser::ast::Select,
    ctes: &HashMap<String, LogicalPlan>,
    outer_ref_scope: &[String],
) -> Result<String> {
    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(e) => {
                let planned = expr_to_expression_ctx(e, ctes, outer_ref_scope, outer_ref_scope)?;
                return match planned {
                    Expression::Column(c) => Ok(c),
                    Expression::AggregateFunction { .. } => aggregate_result_column(&planned)
                        .ok_or_else(|| TakyonicError::Sql("aggregate projection unnamed".into())),
                    other => Err(TakyonicError::Sql(format!(
                        "subquery must project a simple column, got {other:?}"
                    ))),
                };
            }
            SelectItem::ExprWithAlias { alias, .. } => {
                return Ok(alias.value.clone());
            }
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {
                return Err(TakyonicError::Sql(
                    "subquery used in IN/scalar context cannot use SELECT *".into(),
                ));
            }
            SelectItem::ExprWithAliases { .. } => {
                return Err(TakyonicError::Sql(
                    "multi-alias SELECT expressions are unsupported".into(),
                ));
            }
        }
    }
    Err(TakyonicError::Sql("subquery has empty projection".into()))
}

/// Parse a non-aggregate SELECT list into a projection (or `None` for `SELECT *`).
///
/// Also extracts window calls (`ROW_NUMBER() OVER …`) rewritten to column refs.
fn plan_projection_list_with_windows(
    select: &sqlparser::ast::Select,
    ctes: &HashMap<String, LogicalPlan>,
    outer_ref_scope: &[String],
    subquery_outer: &[String],
) -> Result<(Option<Vec<(String, Expression)>>, Vec<WindowCall>)> {
    let named_windows = build_named_windows(&select.named_window)?;
    let mut columns = Vec::new();
    let mut windows = Vec::new();
    let mut saw_wildcard = false;
    for item in &select.projection {
        match item {
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {
                saw_wildcard = true;
            }
            SelectItem::UnnamedExpr(e) => {
                if let Some(call) = try_plan_window_call(
                    e,
                    None,
                    ctes,
                    outer_ref_scope,
                    subquery_outer,
                    &named_windows,
                )? {
                    let name = call.output_column.clone();
                    windows.push(call);
                    columns.push((name.clone(), Expression::Column(name)));
                } else {
                    let planned =
                        expr_to_expression_ctx(e, ctes, outer_ref_scope, subquery_outer)?;
                    let name = projection_output_name(e, &planned)?;
                    columns.push((name, planned));
                }
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                if let Some(call) = try_plan_window_call(
                    expr,
                    Some(alias.value.as_str()),
                    ctes,
                    outer_ref_scope,
                    subquery_outer,
                    &named_windows,
                )? {
                    let name = call.output_column.clone();
                    windows.push(call);
                    columns.push((name.clone(), Expression::Column(name)));
                } else {
                    let planned =
                        expr_to_expression_ctx(expr, ctes, outer_ref_scope, subquery_outer)?;
                    columns.push((alias.value.clone(), planned));
                }
            }
            SelectItem::ExprWithAliases { .. } => {
                return Err(TakyonicError::Sql(
                    "multi-alias SELECT expressions are unsupported".into(),
                ));
            }
        }
    }
    if saw_wildcard {
        if !columns.is_empty() || !windows.is_empty() {
            return Err(TakyonicError::Sql(
                "mixing SELECT * with explicit columns is unsupported".into(),
            ));
        }
        return Ok((None, windows));
    }
    if columns.is_empty() {
        return Err(TakyonicError::Sql("SELECT list is empty".into()));
    }
    Ok((Some(columns), windows))
}

/// Build `WINDOW name AS (…)` definitions into a name → resolved [`WindowSpec`] map.
fn build_named_windows(defs: &[NamedWindowDefinition]) -> Result<HashMap<String, WindowSpec>> {
    let mut map = HashMap::new();
    for NamedWindowDefinition(ident, expr) in defs {
        let name = ident.value.clone();
        if map.contains_key(&name) {
            return Err(TakyonicError::Sql(format!(
                "window name `{name}` is defined more than once"
            )));
        }
        let spec = match expr {
            NamedWindowExpr::WindowSpec(spec) => {
                if let Some(base_name) = &spec.window_name {
                    let base = map.get(&base_name.value).ok_or_else(|| {
                        TakyonicError::Sql(format!(
                            "unknown window name `{}` in WINDOW `{name}`",
                            base_name.value
                        ))
                    })?;
                    merge_window_spec(base, spec)
                } else {
                    spec.clone()
                }
            }
            NamedWindowExpr::NamedWindow(other) => map.get(&other.value).cloned().ok_or_else(|| {
                TakyonicError::Sql(format!(
                    "unknown window name `{}` in WINDOW `{name}`",
                    other.value
                ))
            })?,
        };
        map.insert(name, spec);
    }
    Ok(map)
}

fn merge_window_spec(base: &WindowSpec, overlay: &WindowSpec) -> WindowSpec {
    let mut base_pb = base.partition_by.clone();
    let base_ex = peel_exclude_from_partition(&mut base_pb);
    let mut over_pb = overlay.partition_by.clone();
    let over_ex = peel_exclude_from_partition(&mut over_pb);
    let exclude = if over_ex != FrameExclude::NoOthers {
        over_ex
    } else {
        base_ex
    };
    let partition_by = if over_pb.is_empty() {
        base_pb
    } else {
        over_pb
    };
    WindowSpec {
        window_name: None,
        partition_by: reinject_exclude_partition(partition_by, exclude),
        order_by: if overlay.order_by.is_empty() {
            base.order_by.clone()
        } else {
            overlay.order_by.clone()
        },
        window_frame: overlay
            .window_frame
            .clone()
            .or_else(|| base.window_frame.clone()),
    }
}

/// Parse window functions (`ROW_NUMBER` / `RANK` / `DENSE_RANK` / `LAG` / `LEAD` / …) from SELECT.
fn try_plan_window_call(
    expr: &Expr,
    alias: Option<&str>,
    ctes: &HashMap<String, LogicalPlan>,
    outer_ref_scope: &[String],
    subquery_outer: &[String],
    named_windows: &HashMap<String, WindowSpec>,
) -> Result<Option<WindowCall>> {
    let Expr::Function(func) = expr else {
        return Ok(None);
    };
    let Some(over) = &func.over else {
        return Ok(None);
    };
    let name = object_name_leaf(&func.name)?;
    let upper = name.to_ascii_uppercase();
    let (kind, default_col, needs_value) = match upper.as_str() {
        "ROW_NUMBER" => (WindowKind::RowNumber, "row_number", false),
        "RANK" => (WindowKind::Rank, "rank", false),
        "DENSE_RANK" => (WindowKind::DenseRank, "dense_rank", false),
        "LAG" => (WindowKind::Lag, "lag", true),
        "LEAD" => (WindowKind::Lead, "lead", true),
        "NTILE" => (WindowKind::Ntile, "ntile", false),
        "FIRST_VALUE" => (WindowKind::FirstValue, "first_value", true),
        "LAST_VALUE" => (WindowKind::LastValue, "last_value", true),
        "NTH_VALUE" => (WindowKind::NthValue, "nth_value", true),
        "PERCENT_RANK" => (WindowKind::PercentRank, "percent_rank", false),
        "CUME_DIST" => (WindowKind::CumeDist, "cume_dist", false),
        "SUM" => (WindowKind::Sum, "sum", true),
        "AVG" => (WindowKind::Avg, "avg", true),
        "MIN" => (WindowKind::Min, "min", true),
        "MAX" => (WindowKind::Max, "max", true),
        "COUNT" => (WindowKind::Count, "count", false),
        "STRING_AGG" => (WindowKind::StringAgg, "string_agg", false),
        "ARRAY_AGG" => (WindowKind::ArrayAgg, "array_agg", true),
        "BOOL_AND" | "EVERY" => (WindowKind::BoolAnd, "bool_and", true),
        "BOOL_OR" => (WindowKind::BoolOr, "bool_or", true),
        "JSON_AGG" => (WindowKind::JsonAgg, "json_agg", true),
        "JSONB_AGG" => (WindowKind::JsonbAgg, "jsonb_agg", true),
        "STDDEV" | "STDDEV_SAMP" => (WindowKind::StddevSamp, "stddev", true),
        "STDDEV_POP" => (WindowKind::StddevPop, "stddev_pop", true),
        "VARIANCE" | "VAR_SAMP" => (WindowKind::VarSamp, "variance", true),
        "VAR_POP" => (WindowKind::VarPop, "var_pop", true),
        "CORR" => (WindowKind::Corr, "corr", false),
        "COVAR_POP" => (WindowKind::CovarPop, "covar_pop", false),
        "COVAR_SAMP" => (WindowKind::CovarSamp, "covar_samp", false),
        "REGR_SLOPE" => (WindowKind::RegrSlope, "regr_slope", false),
        "REGR_INTERCEPT" => (WindowKind::RegrIntercept, "regr_intercept", false),
        "REGR_R2" => (WindowKind::RegrR2, "regr_r2", false),
        "REGR_COUNT" => (WindowKind::RegrCount, "regr_count", false),
        "REGR_AVGX" => (WindowKind::RegrAvgX, "regr_avgx", false),
        "REGR_AVGY" => (WindowKind::RegrAvgY, "regr_avgy", false),
        "REGR_SXX" => (WindowKind::RegrSxx, "regr_sxx", false),
        "REGR_SYY" => (WindowKind::RegrSyy, "regr_syy", false),
        "REGR_SXY" => (WindowKind::RegrSxy, "regr_sxy", false),
        "BIT_AND" => (WindowKind::BitAnd, "bit_and", true),
        "BIT_OR" => (WindowKind::BitOr, "bit_or", true),
        "MODE" => (WindowKind::Mode, "mode", true),
        "JSON_OBJECT_AGG" => (WindowKind::JsonObjectAgg, "json_object_agg", false),
        "JSONB_OBJECT_AGG" => (WindowKind::JsonbObjectAgg, "jsonb_object_agg", false),
        _ => {
            return Err(TakyonicError::Sql(format!(
                "window function `{name}` is not yet supported \
                 (ranking / offset / value / distribution / aggregates / stats)"
            )));
        }
    };
    let mut value = None;
    let mut offset = 1i64;
    let mut default_value = None;
    if needs_value {
        let single_value = matches!(
            kind,
            WindowKind::FirstValue
                | WindowKind::LastValue
                | WindowKind::Sum
                | WindowKind::Avg
                | WindowKind::Min
                | WindowKind::Max
                | WindowKind::ArrayAgg
                | WindowKind::BoolAnd
                | WindowKind::BoolOr
                | WindowKind::JsonAgg
                | WindowKind::JsonbAgg
                | WindowKind::StddevSamp
                | WindowKind::StddevPop
                | WindowKind::VarSamp
                | WindowKind::VarPop
                | WindowKind::BitAnd
                | WindowKind::BitOr
                | WindowKind::Mode
        );
        let nth_value = matches!(kind, WindowKind::NthValue);
        let args = match &func.args {
            FunctionArguments::List(list) => &list.args,
            FunctionArguments::None => {
                return Err(TakyonicError::Sql(if single_value {
                    format!("{upper}(expr) requires a value argument")
                } else if nth_value {
                    format!("{upper}(expr, n) requires value and n arguments")
                } else {
                    format!("{upper}(value [, offset [, default]]) requires a value argument")
                }));
            }
            FunctionArguments::Subquery(_) => {
                return Err(TakyonicError::Sql(format!(
                    "{upper}() subquery arguments are unsupported"
                )));
            }
        };
        if single_value {
            if args.len() != 1 {
                return Err(TakyonicError::Sql(format!(
                    "{upper}(expr) requires exactly one argument"
                )));
            }
            value = Some(function_arg_to_expression_ctx(
                &args[0],
                ctes,
                outer_ref_scope,
                subquery_outer,
            )?);
        } else if nth_value {
            if args.len() != 2 {
                return Err(TakyonicError::Sql(format!(
                    "{upper}(expr, n) requires exactly two arguments"
                )));
            }
            value = Some(function_arg_to_expression_ctx(
                &args[0],
                ctes,
                outer_ref_scope,
                subquery_outer,
            )?);
            let n_expr = function_arg_to_expression_ctx(
                &args[1],
                ctes,
                outer_ref_scope,
                subquery_outer,
            )?;
            offset = match &n_expr {
                Expression::Literal(s) => s.parse::<i64>().map_err(|_| {
                    TakyonicError::Sql(format!(
                        "{upper} n must be an integer literal, got `{s}`"
                    ))
                })?,
                other => {
                    return Err(TakyonicError::Sql(format!(
                        "{upper} n must be an integer literal, got {other:?}"
                    )));
                }
            };
            if offset <= 0 {
                return Err(TakyonicError::Sql(format!(
                    "{upper} n must be a positive integer"
                )));
            }
        } else {
            if !(1..=3).contains(&args.len()) {
                return Err(TakyonicError::Sql(format!(
                    "{upper}(value [, offset [, default]]) takes 1 to 3 arguments"
                )));
            }
            value = Some(function_arg_to_expression_ctx(
                &args[0],
                ctes,
                outer_ref_scope,
                subquery_outer,
            )?);
            if args.len() >= 2 {
                let off_expr = function_arg_to_expression_ctx(
                    &args[1],
                    ctes,
                    outer_ref_scope,
                    subquery_outer,
                )?;
                offset = match &off_expr {
                    Expression::Literal(s) => s.parse::<i64>().map_err(|_| {
                        TakyonicError::Sql(format!(
                            "{upper} offset must be an integer literal, got `{s}`"
                        ))
                    })?,
                    Expression::ScalarFunction { name, args }
                        if name == "NEGATE" && args.len() == 1 =>
                    {
                        -match &args[0] {
                            Expression::Literal(s) => s.parse::<i64>().map_err(|_| {
                                TakyonicError::Sql(format!(
                                    "{upper} offset must be an integer literal, got `{s}`"
                                ))
                            })?,
                            other => {
                                return Err(TakyonicError::Sql(format!(
                                    "{upper} offset must be an integer literal, got {other:?}"
                                )));
                            }
                        }
                    }
                    other => {
                        return Err(TakyonicError::Sql(format!(
                            "{upper} offset must be an integer literal, got {other:?}"
                        )));
                    }
                };
                if offset <= 0 {
                    return Err(TakyonicError::Sql(format!(
                        "{upper} offset must be a positive integer"
                    )));
                }
            }
            if args.len() >= 3 {
                default_value = Some(function_arg_to_expression_ctx(
                    &args[2],
                    ctes,
                    outer_ref_scope,
                    subquery_outer,
                )?);
            }
        }
    } else if kind == WindowKind::Ntile {
        let args = match &func.args {
            FunctionArguments::List(list) => &list.args,
            FunctionArguments::None => {
                return Err(TakyonicError::Sql(
                    "NTILE(buckets) requires a positive integer argument".into(),
                ));
            }
            FunctionArguments::Subquery(_) => {
                return Err(TakyonicError::Sql(
                    "NTILE() subquery arguments are unsupported".into(),
                ));
            }
        };
        if args.len() != 1 {
            return Err(TakyonicError::Sql(
                "NTILE(buckets) requires exactly one argument".into(),
            ));
        }
        let buckets_expr =
            function_arg_to_expression_ctx(&args[0], ctes, outer_ref_scope, subquery_outer)?;
        offset = match &buckets_expr {
            Expression::Literal(s) => s.parse::<i64>().map_err(|_| {
                TakyonicError::Sql(format!(
                    "NTILE buckets must be an integer literal, got `{s}`"
                ))
            })?,
            other => {
                return Err(TakyonicError::Sql(format!(
                    "NTILE buckets must be an integer literal, got {other:?}"
                )));
            }
        };
        if offset <= 0 {
            return Err(TakyonicError::Sql(
                "NTILE buckets must be a positive integer".into(),
            ));
        }
    } else if kind == WindowKind::Count {
        match &func.args {
            FunctionArguments::None => {}
            FunctionArguments::List(list) if list.args.is_empty() => {}
            FunctionArguments::List(list) if list.args.len() == 1 => {
                match &list.args[0] {
                    FunctionArg::Unnamed(FunctionArgExpr::Wildcard)
                    | FunctionArg::Unnamed(FunctionArgExpr::QualifiedWildcard(_)) => {}
                    other => {
                        value = Some(function_arg_to_expression_ctx(
                            other,
                            ctes,
                            outer_ref_scope,
                            subquery_outer,
                        )?);
                    }
                }
            }
            FunctionArguments::List(_) => {
                return Err(TakyonicError::Sql(
                    "COUNT() OVER takes at most one argument".into(),
                ));
            }
            FunctionArguments::Subquery(_) => {
                return Err(TakyonicError::Sql(
                    "COUNT() subquery arguments are unsupported".into(),
                ));
            }
        }
    } else if matches!(
        kind,
        WindowKind::StringAgg | WindowKind::JsonObjectAgg | WindowKind::JsonbObjectAgg
    ) {
        let args = match &func.args {
            FunctionArguments::List(list) => &list.args,
            FunctionArguments::None => {
                return Err(TakyonicError::Sql(format!(
                    "{upper}(a, b) requires two arguments"
                )));
            }
            FunctionArguments::Subquery(_) => {
                return Err(TakyonicError::Sql(format!(
                    "{upper}() subquery arguments are unsupported"
                )));
            }
        };
        if args.len() != 2 {
            return Err(TakyonicError::Sql(format!(
                "{upper}(a, b) requires exactly two arguments"
            )));
        }
        value = Some(function_arg_to_expression_ctx(
            &args[0],
            ctes,
            outer_ref_scope,
            subquery_outer,
        )?);
        default_value = Some(function_arg_to_expression_ctx(
            &args[1],
            ctes,
            outer_ref_scope,
            subquery_outer,
        )?);
    } else if matches!(
        kind,
        WindowKind::Corr
            | WindowKind::CovarPop
            | WindowKind::CovarSamp
            | WindowKind::RegrSlope
            | WindowKind::RegrIntercept
            | WindowKind::RegrR2
            | WindowKind::RegrCount
            | WindowKind::RegrAvgX
            | WindowKind::RegrAvgY
            | WindowKind::RegrSxx
            | WindowKind::RegrSyy
            | WindowKind::RegrSxy
    ) {
        let args = match &func.args {
            FunctionArguments::List(list) => &list.args,
            FunctionArguments::None => {
                return Err(TakyonicError::Sql(format!(
                    "{upper}(y, x) requires two arguments"
                )));
            }
            FunctionArguments::Subquery(_) => {
                return Err(TakyonicError::Sql(format!(
                    "{upper}() subquery arguments are unsupported"
                )));
            }
        };
        if args.len() != 2 {
            return Err(TakyonicError::Sql(format!(
                "{upper}(y, x) requires exactly two arguments"
            )));
        }
        value = Some(function_arg_to_expression_ctx(
            &args[0],
            ctes,
            outer_ref_scope,
            subquery_outer,
        )?);
        default_value = Some(function_arg_to_expression_ctx(
            &args[1],
            ctes,
            outer_ref_scope,
            subquery_outer,
        )?);
    } else {
        match &func.args {
            FunctionArguments::None => {}
            FunctionArguments::List(list) if list.args.is_empty() => {}
            _ => {
                return Err(TakyonicError::Sql(format!(
                    "{upper}() does not take arguments"
                )));
            }
        }
    }
    let resolved;
    let spec: &WindowSpec = match over {
        WindowType::NamedWindow(ident) => named_windows.get(&ident.value).ok_or_else(|| {
            TakyonicError::Sql(format!("unknown window name `{}`", ident.value))
        })?,
        WindowType::WindowSpec(inline) => {
            if let Some(base_name) = &inline.window_name {
                let base = named_windows.get(&base_name.value).ok_or_else(|| {
                    TakyonicError::Sql(format!("unknown window name `{}`", base_name.value))
                })?;
                resolved = merge_window_spec(base, inline);
                &resolved
            } else {
                inline
            }
        }
    };
    finish_window_call(
        kind,
        default_col,
        alias,
        upper.as_str(),
        spec,
        value,
        offset,
        default_value,
        func.filter.as_deref(),
        func.null_treatment,
        ctes,
        outer_ref_scope,
        subquery_outer,
    )
}

fn window_kind_allows_filter(kind: WindowKind) -> bool {
    matches!(
        kind,
        WindowKind::Sum
            | WindowKind::Avg
            | WindowKind::Count
            | WindowKind::Min
            | WindowKind::Max
            | WindowKind::StringAgg
            | WindowKind::ArrayAgg
            | WindowKind::BoolAnd
            | WindowKind::BoolOr
            | WindowKind::JsonAgg
            | WindowKind::JsonbAgg
            | WindowKind::StddevSamp
            | WindowKind::StddevPop
            | WindowKind::VarSamp
            | WindowKind::VarPop
            | WindowKind::Corr
            | WindowKind::CovarPop
            | WindowKind::CovarSamp
            | WindowKind::RegrSlope
            | WindowKind::RegrIntercept
            | WindowKind::RegrR2
            | WindowKind::RegrCount
            | WindowKind::RegrAvgX
            | WindowKind::RegrAvgY
            | WindowKind::RegrSxx
            | WindowKind::RegrSyy
            | WindowKind::RegrSxy
            | WindowKind::BitAnd
            | WindowKind::BitOr
            | WindowKind::Mode
            | WindowKind::JsonObjectAgg
            | WindowKind::JsonbObjectAgg
    )
}

fn finish_window_call(
    kind: WindowKind,
    default_col: &str,
    alias: Option<&str>,
    upper: &str,
    spec: &WindowSpec,
    value: Option<Expression>,
    offset: i64,
    default_value: Option<Expression>,
    filter_ast: Option<&Expr>,
    null_treatment: Option<NullTreatment>,
    ctes: &HashMap<String, LogicalPlan>,
    outer_ref_scope: &[String],
    subquery_outer: &[String],
) -> Result<Option<WindowCall>> {
    let ignore_nulls = match null_treatment {
        None | Some(NullTreatment::RespectNulls) => false,
        Some(NullTreatment::IgnoreNulls) => {
            if !matches!(
                kind,
                WindowKind::Lag
                    | WindowKind::Lead
                    | WindowKind::FirstValue
                    | WindowKind::LastValue
                    | WindowKind::NthValue
            ) {
                return Err(TakyonicError::Sql(format!(
                    "{upper}() IGNORE NULLS is only supported for \
                     LAG/LEAD/FIRST_VALUE/LAST_VALUE/NTH_VALUE"
                )));
            }
            true
        }
    };
    let filter = match filter_ast {
        None => None,
        Some(pred) => {
            if !window_kind_allows_filter(kind) {
                return Err(TakyonicError::Sql(format!(
                    "{upper}() FILTER is only supported for aggregate window functions"
                )));
            }
            Some(expr_to_expression_ctx(
                pred,
                ctes,
                outer_ref_scope,
                subquery_outer,
            )?)
        }
    };
    let frame = match &spec.window_frame {
        None => None,
        Some(wf) => {
            let units = match wf.units {
                WindowFrameUnits::Rows => FrameUnits::Rows,
                WindowFrameUnits::Range => FrameUnits::Range,
                WindowFrameUnits::Groups => FrameUnits::Groups,
            };
            let start = plan_frame_bound(&wf.start_bound)?;
            let end = match &wf.end_bound {
                Some(b) => plan_frame_bound(b)?,
                None => FrameBound::CurrentRow,
            };
            Some(WindowRowsFrame {
                units,
                start,
                end,
                exclude: FrameExclude::NoOthers,
            })
        }
    };
    let mut partition_ast = spec.partition_by.clone();
    let exclude = peel_exclude_from_partition(&mut partition_ast);
    let mut partition_by = Vec::new();
    for e in &partition_ast {
        partition_by.push(expr_to_expression_ctx(
            e,
            ctes,
            outer_ref_scope,
            subquery_outer,
        )?);
    }
    let mut order_by = Vec::new();
    for e in &spec.order_by {
        order_by.push(plan_order_by_expr_ctx(
            e,
            ctes,
            outer_ref_scope,
            subquery_outer,
        )?);
    }
    let mut frame = frame;
    if let Some(f) = frame.as_mut() {
        f.exclude = exclude;
    } else if exclude != FrameExclude::NoOthers {
        // PG default frame when EXCLUDE is present without an explicit frame.
        frame = Some(WindowRowsFrame {
            units: FrameUnits::Range,
            start: FrameBound::UnboundedPreceding,
            end: if order_by.is_empty() {
                FrameBound::UnboundedFollowing
            } else {
                FrameBound::CurrentRow
            },
            exclude,
        });
    }
    if let Some(f) = &frame {
        if f.units == FrameUnits::Range && range_frame_has_value_offset(f) && order_by.len() != 1 {
            return Err(TakyonicError::Sql(format!(
                "{upper}() RANGE value offsets require exactly one ORDER BY expression"
            )));
        }
        if f.units == FrameUnits::Groups && order_by.is_empty() {
            return Err(TakyonicError::Sql(format!(
                "{upper}() GROUPS frames require ORDER BY"
            )));
        }
    }
    let output_column = alias
        .map(str::to_string)
        .unwrap_or_else(|| default_col.into());
    Ok(Some(WindowCall {
        output_column,
        kind,
        partition_by,
        order_by,
        value,
        offset,
        default_value,
        frame,
        filter,
        ignore_nulls,
    }))
}

fn range_frame_has_value_offset(frame: &WindowRowsFrame) -> bool {
    matches!(
        frame.start,
        FrameBound::Preceding(_) | FrameBound::Following(_)
    ) || matches!(
        frame.end,
        FrameBound::Preceding(_) | FrameBound::Following(_)
    )
}

fn plan_frame_bound(bound: &WindowFrameBound) -> Result<FrameBound> {
    match bound {
        WindowFrameBound::CurrentRow => Ok(FrameBound::CurrentRow),
        WindowFrameBound::Preceding(None) => Ok(FrameBound::UnboundedPreceding),
        WindowFrameBound::Following(None) => Ok(FrameBound::UnboundedFollowing),
        WindowFrameBound::Preceding(Some(e)) => Ok(FrameBound::Preceding(frame_bound_literal(e)?)),
        WindowFrameBound::Following(Some(e)) => Ok(FrameBound::Following(frame_bound_literal(e)?)),
    }
}

fn frame_bound_literal(expr: &Expr) -> Result<u64> {
    match expr {
        Expr::Value(ValueWithSpan { value, .. }) => match value {
            SqlValue::Number(s, _) => s.parse::<u64>().map_err(|_| {
                TakyonicError::Sql(format!(
                    "window frame bound must be a non-negative integer, got `{s}`"
                ))
            }),
            other => Err(TakyonicError::Sql(format!(
                "window frame bound must be an integer literal, got {other}"
            ))),
        },
        other => Err(TakyonicError::Sql(format!(
            "window frame bound must be an integer literal, got {other}"
        ))),
    }
}

#[allow(dead_code)]
fn plan_simple_projection_list(
    select: &sqlparser::ast::Select,
    ctes: &HashMap<String, LogicalPlan>,
    outer_ref_scope: &[String],
    subquery_outer: &[String],
) -> Result<Option<Vec<(String, Expression)>>> {
    let (cols, windows) =
        plan_projection_list_with_windows(select, ctes, outer_ref_scope, subquery_outer)?;
    if !windows.is_empty() {
        return Err(TakyonicError::Sql(
            "internal: unexpected window calls in plan_simple_projection_list".into(),
        ));
    }
    Ok(cols)
}

fn projection_output_name(expr: &Expr, planned: &Expression) -> Result<String> {
    match planned {
        Expression::Column(name) | Expression::OuterRef(name) => Ok(name.clone()),
        _ => match expr {
            Expr::Identifier(ident) => Ok(ident.value.clone()),
            Expr::CompoundIdentifier(parts) => parts
                .last()
                .map(|i| i.value.clone())
                .ok_or_else(|| TakyonicError::Sql("empty projection identifier".into())),
            other => Ok(other.to_string()),
        },
    }
}

/// Split SELECT list + GROUP BY into grouping keys and aggregate expressions.
///
/// Returns `(group_exprs, aggr_exprs, has_aggregate_in_projection)`.
#[allow(dead_code)]
fn plan_projection_aggregates(
    select: &sqlparser::ast::Select,
) -> Result<(Vec<Expression>, Vec<Expression>, bool, Option<Expression>)> {
    plan_projection_aggregates_ctx(select, &HashMap::new(), &[], &[])
}

fn plan_projection_aggregates_ctx(
    select: &sqlparser::ast::Select,
    ctes: &HashMap<String, LogicalPlan>,
    outer_ref_scope: &[String],
    subquery_outer: &[String],
) -> Result<(Vec<Expression>, Vec<Expression>, bool, Option<Expression>)> {
    let group_exprs =
        plan_group_by_ctx(select, ctes, outer_ref_scope, subquery_outer)?;
    let mut aggr_exprs = Vec::new();
    let mut has_agg = false;

    for item in &select.projection {
        let expr = match item {
            SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => e,
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {
                if !group_exprs.is_empty() || has_agg {
                    return Err(TakyonicError::Sql(
                        "SELECT * is not supported with GROUP BY / aggregates".into(),
                    ));
                }
                continue;
            }
            SelectItem::ExprWithAliases { .. } => {
                return Err(TakyonicError::Sql(
                    "multi-alias SELECT expressions are unsupported".into(),
                ));
            }
        };
        // Window functions are planned separately; do not treat as aggregates here.
        if matches!(
            expr,
            Expr::Function(f) if f.over.is_some()
        ) {
            continue;
        }
        let planned = expr_to_expression_ctx(expr, ctes, outer_ref_scope, subquery_outer)?;
        if matches!(planned, Expression::AggregateFunction { .. }) {
            has_agg = true;
            aggr_exprs.push(planned);
        } else if !group_exprs.is_empty() {
            if !group_exprs.iter().any(|g| g == &planned) {
                return Err(TakyonicError::Sql(format!(
                    "SELECT expression `{expr}` must appear in GROUP BY or be an aggregate"
                )));
            }
        }
    }

    let having = if let Some(h) = &select.having {
        Some(expr_to_expression_ctx(h, ctes, outer_ref_scope, subquery_outer)?)
    } else {
        None
    };

    // Aggregates that appear only in HAVING still need AggregateExec slots.
    if let Some(h) = &having {
        if expr_contains_aggregate(h) {
            has_agg = true;
            collect_aggregates_into(h, &mut aggr_exprs);
        }
    }

    Ok((group_exprs, aggr_exprs, has_agg, having))
}

#[allow(dead_code)]
fn plan_group_by(select: &sqlparser::ast::Select) -> Result<Vec<Expression>> {
    plan_group_by_ctx(select, &HashMap::new(), &[], &[])
}

fn plan_group_by_ctx(
    select: &sqlparser::ast::Select,
    ctes: &HashMap<String, LogicalPlan>,
    outer_ref_scope: &[String],
    subquery_outer: &[String],
) -> Result<Vec<Expression>> {
    match &select.group_by {
        GroupByExpr::Expressions(exprs, modifiers) => {
            if !modifiers.is_empty() {
                return Err(TakyonicError::Sql(
                    "GROUP BY modifiers (ROLLUP/CUBE/…) are unsupported".into(),
                ));
            }
            exprs
                .iter()
                .map(|e| expr_to_expression_ctx(e, ctes, outer_ref_scope, subquery_outer))
                .collect()
        }
        GroupByExpr::All(modifiers) => {
            if !modifiers.is_empty() {
                return Err(TakyonicError::Sql(
                    "GROUP BY ALL with ROLLUP/CUBE/… modifiers is unsupported".into(),
                ));
            }
            expand_group_by_all(select, ctes, outer_ref_scope, subquery_outer)
        }
    }
}

/// Expand `GROUP BY ALL` to every non-aggregate SELECT-list expression.
fn expand_group_by_all(
    select: &sqlparser::ast::Select,
    ctes: &HashMap<String, LogicalPlan>,
    outer_ref_scope: &[String],
    subquery_outer: &[String],
) -> Result<Vec<Expression>> {
    let mut out = Vec::new();
    for item in &select.projection {
        let expr = match item {
            SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => e,
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {
                return Err(TakyonicError::Sql(
                    "SELECT * is not supported with GROUP BY ALL".into(),
                ));
            }
            SelectItem::ExprWithAliases { .. } => {
                return Err(TakyonicError::Sql(
                    "multi-alias SELECT expressions are unsupported".into(),
                ));
            }
        };
        if matches!(expr, Expr::Function(f) if f.over.is_some()) {
            continue;
        }
        let planned = expr_to_expression_ctx(expr, ctes, outer_ref_scope, subquery_outer)?;
        if matches!(planned, Expression::AggregateFunction { .. }) {
            continue;
        }
        if !out.iter().any(|g| g == &planned) {
            out.push(planned);
        }
    }
    Ok(out)
}

#[allow(dead_code)]
fn plan_order_by(order_by: &OrderBy) -> Result<Vec<SortExpr>> {
    plan_order_by_ctx(order_by, &HashMap::new(), &[], &[], None, &[])
}

fn plan_order_by_ctx(
    order_by: &OrderBy,
    ctes: &HashMap<String, LogicalPlan>,
    outer_ref_scope: &[String],
    subquery_outer: &[String],
    select_for_all: Option<&Select>,
    output_hints: &[String],
) -> Result<Vec<SortExpr>> {
    if order_by.interpolate.is_some() {
        return Err(TakyonicError::Sql(
            "ORDER BY INTERPOLATE is not supported".into(),
        ));
    }
    // sqlparser's PostgreSqlDialect does not set `supports_order_by_all`, so
    // `ORDER BY ALL [ASC|DESC …]` arrives as a single Identifier("ALL") expression.
    let all_opts = match &order_by.kind {
        OrderByKind::All(opts) => Some(opts),
        OrderByKind::Expressions(exprs) if is_order_by_all_ident(exprs) => {
            Some(&exprs[0].options)
        }
        OrderByKind::Expressions(_) => None,
    };
    if let Some(opts) = all_opts {
        let asc = opts.asc.unwrap_or(true);
        let nulls_first = opts.nulls_first.unwrap_or(!asc);
        return if let Some(select) = select_for_all {
            expand_order_by_all(
                select,
                asc,
                nulls_first,
                ctes,
                outer_ref_scope,
                subquery_outer,
            )
        } else if !output_hints.is_empty() {
            Ok(output_hints
                .iter()
                .map(|c| SortExpr {
                    expr: Expression::Column(c.clone()),
                    asc,
                    nulls_first,
                })
                .collect())
        } else {
            Err(TakyonicError::Sql(
                "ORDER BY ALL requires a SELECT list or known output columns".into(),
            ))
        };
    }
    match &order_by.kind {
        OrderByKind::Expressions(exprs) => exprs
            .iter()
            .map(|e| plan_order_by_expr_ctx(e, ctes, outer_ref_scope, subquery_outer))
            .collect(),
        OrderByKind::All(_) => unreachable!("ORDER BY ALL handled above"),
    }
}

fn is_order_by_all_ident(exprs: &[OrderByExpr]) -> bool {
    match exprs {
        [OrderByExpr {
            expr: Expr::Identifier(id),
            with_fill: None,
            ..
        }] if id.value.eq_ignore_ascii_case("ALL") => true,
        _ => false,
    }
}

/// Expand `ORDER BY ALL` to every SELECT-list expression (aggregates → output names).
fn expand_order_by_all(
    select: &Select,
    asc: bool,
    nulls_first: bool,
    ctes: &HashMap<String, LogicalPlan>,
    outer_ref_scope: &[String],
    subquery_outer: &[String],
) -> Result<Vec<SortExpr>> {
    let mut out = Vec::new();
    for item in &select.projection {
        let expr = match item {
            SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => e,
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {
                return Err(TakyonicError::Sql(
                    "SELECT * is not supported with ORDER BY ALL".into(),
                ));
            }
            SelectItem::ExprWithAliases { .. } => {
                return Err(TakyonicError::Sql(
                    "multi-alias SELECT expressions are unsupported".into(),
                ));
            }
        };
        if matches!(expr, Expr::Function(f) if f.over.is_some()) {
            let name = match item {
                SelectItem::ExprWithAlias { alias, .. } => alias.value.clone(),
                _ => projection_output_name(
                    expr,
                    &Expression::Column("window".into()),
                )
                .unwrap_or_else(|_| "window".into()),
            };
            out.push(SortExpr {
                expr: Expression::Column(name),
                asc,
                nulls_first,
            });
            continue;
        }
        let planned = expr_to_expression_ctx(expr, ctes, outer_ref_scope, subquery_outer)?;
        let sort_expr = rewrite_sort_expr_for_output(planned);
        out.push(SortExpr {
            expr: sort_expr,
            asc,
            nulls_first,
        });
    }
    if out.is_empty() {
        return Err(TakyonicError::Sql(
            "ORDER BY ALL requires at least one SELECT expression".into(),
        ));
    }
    Ok(out)
}

#[allow(dead_code)]
fn plan_order_by_expr(obe: &OrderByExpr) -> Result<SortExpr> {
    plan_order_by_expr_ctx(obe, &HashMap::new(), &[], &[])
}

fn plan_order_by_expr_ctx(
    obe: &OrderByExpr,
    ctes: &HashMap<String, LogicalPlan>,
    outer_ref_scope: &[String],
    subquery_outer: &[String],
) -> Result<SortExpr> {
    if obe.with_fill.is_some() {
        return Err(TakyonicError::Sql(
            "ORDER BY WITH FILL is not supported".into(),
        ));
    }
    let expr = rewrite_sort_expr_for_output(expr_to_expression_ctx(
        &obe.expr,
        ctes,
        outer_ref_scope,
        subquery_outer,
    )?);
    let asc = obe.options.asc.unwrap_or(true);
    // PG: ASC → NULLS LAST; DESC → NULLS FIRST when unspecified.
    let nulls_first = obe.options.nulls_first.unwrap_or(!asc);
    Ok(SortExpr {
        expr,
        asc,
        nulls_first,
    })
}

fn plan_limit_clause(clause: &LimitClause) -> Result<(usize, Option<usize>)> {
    match clause {
        LimitClause::LimitOffset {
            limit,
            offset,
            limit_by,
        } => {
            if !limit_by.is_empty() {
                return Err(TakyonicError::Sql("LIMIT BY is not supported".into()));
            }
            let fetch = match limit {
                None => None,
                Some(expr) => Some(expr_to_usize(expr, "LIMIT")?),
            };
            let skip = match offset {
                None => 0,
                Some(off) => expr_to_usize(&off.value, "OFFSET")?,
            };
            Ok((skip, fetch))
        }
        LimitClause::OffsetCommaLimit { offset, limit } => {
            let skip = expr_to_usize(offset, "OFFSET")?;
            let fetch = expr_to_usize(limit, "LIMIT")?;
            Ok((skip, Some(fetch)))
        }
    }
}

/// Apply `LIMIT`/`OFFSET` and/or `FETCH FIRST … [WITH TIES]` on top of `plan`.
fn apply_limit_and_fetch(
    plan: LogicalPlan,
    limit_clause: Option<&LimitClause>,
    fetch_clause: Option<&Fetch>,
) -> Result<LogicalPlan> {
    let (skip, limit_fetch) = match limit_clause {
        Some(c) => plan_limit_clause(c)?,
        None => (0, None),
    };

    let (fetch, with_ties) = match fetch_clause {
        None => (limit_fetch, false),
        Some(f) => {
            if f.percent {
                return Err(TakyonicError::Sql(
                    "FETCH … PERCENT is not supported".into(),
                ));
            }
            if limit_fetch.is_some() {
                return Err(TakyonicError::Sql(
                    "cannot use LIMIT and FETCH together".into(),
                ));
            }
            let n = match &f.quantity {
                None => 1usize,
                Some(expr) => expr_to_usize(expr, "FETCH")?,
            };
            (Some(n), f.with_ties)
        }
    };

    if with_ties {
        let Some(n) = fetch else {
            return Err(TakyonicError::Sql(
                "FETCH WITH TIES requires a row count".into(),
            ));
        };
        let ties_order = match &plan {
            LogicalPlan::Sort { exprs, .. } if !exprs.is_empty() => exprs.clone(),
            _ => {
                return Err(TakyonicError::Sql(
                    "FETCH WITH TIES requires ORDER BY".into(),
                ));
            }
        };
        return Ok(LogicalPlan::Limit {
            input: Box::new(plan),
            skip,
            fetch: Some(n),
            with_ties: true,
            ties_order,
        });
    }

    if skip > 0 || fetch.is_some() {
        Ok(LogicalPlan::Limit {
            input: Box::new(plan),
            skip,
            fetch,
            with_ties: false,
            ties_order: Vec::new(),
        })
    } else {
        Ok(plan)
    }
}

fn expr_to_usize(expr: &Expr, label: &str) -> Result<usize> {
    match expr {
        Expr::Value(ValueWithSpan {
            value: SqlValue::Number(n, _),
            ..
        }) => n
            .parse::<usize>()
            .map_err(|_| TakyonicError::Sql(format!("{label} must be a non-negative integer"))),
        Expr::Value(ValueWithSpan {
            value: SqlValue::Placeholder(p),
            ..
        }) => Err(TakyonicError::Sql(format!(
            "parameterized {label} ({p}) is not yet supported"
        ))),
        other => Err(TakyonicError::Sql(format!(
            "{label} requires an integer literal, got {other}"
        ))),
    }
}

fn is_aggregate_fn(name: &str) -> bool {
    matches!(
        name.to_ascii_uppercase().as_str(),
        "COUNT"
            | "SUM"
            | "AVG"
            | "MIN"
            | "MAX"
            | "JSON_AGG"
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
    )
}

#[allow(dead_code)]
fn function_to_expression(func: &Function) -> Result<Expression> {
    function_to_expression_ctx(func, &HashMap::new(), &[], &[])
}

fn function_arg_list(
    func: &Function,
    ctes: &HashMap<String, LogicalPlan>,
    outer_ref_scope: &[String],
    subquery_outer: &[String],
) -> Result<Vec<Expression>> {
    match &func.args {
        FunctionArguments::None => Ok(Vec::new()),
        FunctionArguments::List(list) => {
            let mut out = Vec::with_capacity(list.args.len());
            for arg in &list.args {
                out.push(function_arg_to_expression_ctx(
                    arg,
                    ctes,
                    outer_ref_scope,
                    subquery_outer,
                )?);
            }
            Ok(out)
        }
        FunctionArguments::Subquery(_) => Err(TakyonicError::Sql(
            "subquery function arguments are unsupported".into(),
        )),
    }
}

/// `row_to_json(alias)` → whole current row; `row_to_json(ROW(a,b))` / `(a,b)` → object.
fn plan_row_to_json(
    func: &Function,
    ctes: &HashMap<String, LogicalPlan>,
    outer_ref_scope: &[String],
    subquery_outer: &[String],
) -> Result<Expression> {
    let raw_args: Vec<&Expr> = match &func.args {
        FunctionArguments::None => Vec::new(),
        FunctionArguments::List(list) => list
            .args
            .iter()
            .map(|a| match a {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Ok(e),
                FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => Err(TakyonicError::Sql(
                    "row_to_json(*) is not supported; use row_to_json(table_alias)".into(),
                )),
                other => Err(TakyonicError::Sql(format!(
                    "unsupported row_to_json argument: {other}"
                ))),
            })
            .collect::<Result<Vec<_>>>()?,
        FunctionArguments::Subquery(_) => {
            return Err(TakyonicError::Sql(
                "row_to_json does not accept subquery arguments".into(),
            ));
        }
    };
    if raw_args.len() != 1 {
        return Err(TakyonicError::Sql(
            "ROW_TO_JSON requires exactly one argument".into(),
        ));
    }
    let arg = raw_args[0];
    // Whole-row reference: row_to_json(emp) / row_to_json(t)
    if matches!(arg, Expr::Identifier(_)) {
        return Ok(Expression::ScalarFunction {
            name: "ROW_TO_JSON".into(),
            args: Vec::new(),
        });
    }
    // Explicit row constructor: ROW(id, name) or (id, name)
    let field_exprs: Vec<&Expr> = match arg {
        Expr::Function(inner) => {
            let leaf = object_name_leaf(&inner.name)?;
            if leaf.eq_ignore_ascii_case("ROW") {
                match &inner.args {
                    FunctionArguments::List(list) => list
                        .args
                        .iter()
                        .map(|a| match a {
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Ok(e),
                            other => Err(TakyonicError::Sql(format!(
                                "unsupported ROW() argument: {other}"
                            ))),
                        })
                        .collect::<Result<Vec<_>>>()?,
                    _ => {
                        return Err(TakyonicError::Sql(
                            "ROW() requires a list of field expressions".into(),
                        ));
                    }
                }
            } else {
                return Err(TakyonicError::Sql(format!(
                    "row_to_json expects a row alias, ROW(...), or (…), got function `{leaf}`"
                )));
            }
        }
        Expr::Tuple(elems) => elems.iter().collect(),
        other => {
            return Err(TakyonicError::Sql(format!(
                "row_to_json expects a row alias, ROW(...), or (…), got {other}"
            )));
        }
    };
    let mut args = Vec::with_capacity(field_exprs.len());
    for e in field_exprs {
        args.push(expr_to_expression_ctx(
            e,
            ctes,
            outer_ref_scope,
            subquery_outer,
        )?);
    }
    Ok(Expression::ScalarFunction {
        name: "ROW_TO_JSON".into(),
        args,
    })
}

fn is_scalar_fn(name: &str) -> bool {
    matches!(
        name,
        "LOWER"
            | "UPPER"
            | "LENGTH"
            | "CHAR_LENGTH"
            | "CHARACTER_LENGTH"
            | "OCTET_LENGTH"
            | "BIT_LENGTH"
            | "TRIM"
            | "BTRIM"
            | "LTRIM"
            | "RTRIM"
            | "TRANSLATE"
            | "SUBSTRING"
            | "SUBSTR"
            | "CONCAT"
            | "CONCAT_WS"
            | "FORMAT"
            | "QUOTE_IDENT"
            | "QUOTE_LITERAL"
            | "QUOTE_NULLABLE"
            | "WIDTH_BUCKET"
            | "REPLACE"
            | "REGEXP_REPLACE"
            | "REGEXP_LIKE"
            | "LPAD"
            | "RPAD"
            | "REPEAT"
            | "LEFT"
            | "RIGHT"
            | "REVERSE"
            | "INITCAP"
            | "ASCII"
            | "CHR"
            | "MD5"
            | "ENCODE"
            | "DECODE"
            | "STARTS_WITH"
            | "ENDS_WITH"
            | "OVERLAY"
            | "POSITION"
            | "STRPOS"
            | "ABS"
            | "ROUND"
            | "CEIL"
            | "CEILING"
            | "FLOOR"
            | "TRUNC"
            | "SIGN"
            | "MOD"
            | "DIV"
            | "POWER"
            | "POW"
            | "SQRT"
            | "CBRT"
            | "LN"
            | "LOG"
            | "EXP"
            | "PI"
            | "SIN"
            | "COS"
            | "TAN"
            | "ASIN"
            | "ACOS"
            | "ATAN"
            | "ATAN2"
            | "RADIANS"
            | "DEGREES"
            | "NEGATE"
            | "NOW"
            | "CURRENT_TIMESTAMP"
            | "CURRENT_DATE"
            | "CURRENT_TIME"
            | "LOCALTIMESTAMP"
            | "LOCALTIME"
            | "CLOCK_TIMESTAMP"
            | "TIMEOFDAY"
            | "STATEMENT_TIMESTAMP"
            | "TRANSACTION_TIMESTAMP"
            | "CURRENT_USER"
            | "SESSION_USER"
            | "USER"
            | "CURRENT_ROLE"
            | "CURRENT_SCHEMA"
            | "CURRENT_CATALOG"
            | "CURRENT_SCHEMAS"
            | "VERSION"
            | "PG_BACKEND_PID"
            | "PG_IS_IN_RECOVERY"
            | "PG_JIT_AVAILABLE"
            | "PG_CURRENT_WAL_LSN"
            | "PG_CURRENT_WAL_INSERT_LSN"
            | "PG_CURRENT_WAL_FLUSH_LSN"
            | "PG_WAL_LSN_DIFF"
            | "PG_WALFILE_NAME"
            | "PG_WALFILE_NAME_OFFSET"
            | "PG_SWITCH_WAL"
            | "PG_SWITCH_XLOG"
            | "PG_LAST_WAL_RECEIVE_LSN"
            | "PG_LAST_WAL_REPLAY_LSN"
            | "PG_LAST_XACT_REPLAY_TIMESTAMP"
            | "PG_IS_WAL_REPLAY_PAUSED"
            | "PG_WAL_REPLAY_PAUSE"
            | "PG_WAL_REPLAY_RESUME"
            | "PG_IS_IN_BACKUP"
            | "PG_BACKUP_START_TIME"
            | "PG_BACKUP_START"
            | "PG_START_BACKUP"
            | "PG_BACKUP_STOP"
            | "PG_STOP_BACKUP"
            | "PG_CREATE_RESTORE_POINT"
            | "PG_PROMOTE"
            | "PG_RELOAD_CONF"
            | "PG_ROTATE_LOGFILE"
            | "PG_CONF_LOAD_TIME"
            | "NEXTVAL"
            | "CURRVAL"
            | "LASTVAL"
            | "SETVAL"
            | "PG_GET_SERIAL_SEQUENCE"
            | "PG_SEQUENCE_LAST_VALUE"
            | "CURRENT_QUERY"
            | "PG_TRY_ADVISORY_LOCK"
            | "PG_ADVISORY_LOCK"
            | "PG_ADVISORY_UNLOCK"
            | "PG_TRY_ADVISORY_LOCK_SHARED"
            | "PG_ADVISORY_LOCK_SHARED"
            | "PG_ADVISORY_UNLOCK_SHARED"
            | "PG_TRY_ADVISORY_XACT_LOCK"
            | "PG_ADVISORY_XACT_LOCK"
            | "PG_TRY_ADVISORY_XACT_LOCK_SHARED"
            | "PG_ADVISORY_XACT_LOCK_SHARED"
            | "PG_ADVISORY_UNLOCK_ALL"
            | "PG_TYPEOF"
            | "GETDATABASEENCODING"
            | "PG_CLIENT_ENCODING"
            | "PG_ENCODING_TO_CHAR"
            | "PG_CHAR_TO_ENCODING"
            | "PG_TABLE_IS_VISIBLE"
            | "PG_TYPE_IS_VISIBLE"
            | "PG_FUNCTION_IS_VISIBLE"
            | "TO_REGPROC"
            | "TO_REGPROCEDURE"
            | "TO_REGOPER"
            | "TO_REGOPERATOR"
            | "PG_OPERATOR_IS_VISIBLE"
            | "TO_REGCOLLATION"
            | "PG_COLLATION_IS_VISIBLE"
            | "PG_RELATION_IS_UPDATABLE"
            | "PG_COLUMN_IS_UPDATABLE"
            | "PG_GET_INDEXDEF"
            | "PG_DESCRIBE_OBJECT"
            | "PG_IDENTIFY_OBJECT"
            | "PG_SIZE_PRETTY"
            | "PG_SIZE_BYTES"
            | "GEN_RANDOM_UUID"
            | "PG_SLEEP"
            | "PG_COLUMN_SIZE"
            | "PG_NOTIFY"
            | "PG_NOTIFICATION_QUEUE_USAGE"
            | "PG_LISTENING_CHANNELS"
            | "TXID_CURRENT"
            | "PG_CURRENT_XACT_ID"
            | "TXID_STATUS"
            | "PG_XACT_STATUS"
            | "PG_EXPORT_SNAPSHOT"
            | "PG_CURRENT_SNAPSHOT"
            | "TXID_CURRENT_SNAPSHOT"
            | "PG_SNAPSHOT_XMIN"
            | "PG_SNAPSHOT_XMAX"
            | "PG_VISIBLE_IN_SNAPSHOT"
            | "PG_CANCEL_BACKEND"
            | "PG_TERMINATE_BACKEND"
            | "PG_POSTMASTER_START_TIME"
            | "CURRENT_SETTING"
            | "SET_CONFIG"
            | "HAS_TABLE_PRIVILEGE"
            | "HAS_COLUMN_PRIVILEGE"
            | "HAS_ANY_COLUMN_PRIVILEGE"
            | "OBJ_DESCRIPTION"
            | "COL_DESCRIPTION"
            | "SHOBJ_DESCRIPTION"
            | "TO_REGCLASS"
            | "TO_REGROLE"
            | "TO_REGNAMESPACE"
            | "TO_REGTYPE"
            | "FORMAT_TYPE"
            | "PG_GET_USERBYID"
            | "PG_RELATION_SIZE"
            | "PG_TABLE_SIZE"
            | "PG_TOTAL_RELATION_SIZE"
            | "PG_INDEXES_SIZE"
            | "PG_DATABASE_SIZE"
            | "PG_TABLESPACE_LOCATION"
            | "HAS_SCHEMA_PRIVILEGE"
            | "HAS_DATABASE_PRIVILEGE"
            | "HAS_TABLESPACE_PRIVILEGE"
            | "HAS_FUNCTION_PRIVILEGE"
            | "HAS_TYPE_PRIVILEGE"
            | "HAS_SEQUENCE_PRIVILEGE"
            | "PG_HAS_ROLE"
            | "INET_SERVER_ADDR"
            | "INET_SERVER_PORT"
            | "INET_CLIENT_ADDR"
            | "INET_CLIENT_PORT"
            | "GREATEST"
            | "LEAST"
            | "NUM_NONNULLS"
            | "NUM_NULLS"
            | "RANDOM"
            | "SETSEED"
            | "EXTRACT"
            | "DATE_PART"
            | "DATE_TRUNC"
            | "AGE"
            | "TO_CHAR"
            | "TO_TIMESTAMP"
            | "TO_DATE"
            | "TO_NUMBER"
            | "MAKE_DATE"
            | "MAKE_TIME"
            | "MAKE_TIMESTAMP"
            | "MAKE_INTERVAL"
            | "ISFINITE"
            | "TIMEZONE"
            | "DATE_BIN"
            | "JUSTIFY_HOURS"
            | "JUSTIFY_DAYS"
            | "JUSTIFY_INTERVAL"
            | "OVERLAPS"
            | "ARRAY_LENGTH"
            | "CARDINALITY"
            | "ARRAY_CAT"
            | "ARRAY_CONTAINS"
            | "ARRAY_CONTAINED_BY"
            | "ARRAY_OVERLAP"
            | "STRING_TO_ARRAY"
            | "ARRAY_TO_STRING"
            | "SPLIT_PART"
            | "REGEXP_SPLIT_TO_ARRAY"
            | "JSON_GET"
            | "JSON_GET_TEXT"
            | "JSON_TYPEOF"
            | "JSONB_TYPEOF"
            | "JSON_PATH_GET"
            | "JSON_PATH_GET_TEXT"
            | "JSON_CONTAINS"
            | "JSON_CONTAINED_BY"
            | "JSONB_SET"
            | "JSON_SET"
            | "JSON_CONCAT"
            | "JSONB_BUILD_OBJECT"
            | "JSON_BUILD_OBJECT"
            | "JSONB_BUILD_ARRAY"
            | "JSON_BUILD_ARRAY"
            | "JSONB_PRETTY"
            | "JSON_PRETTY"
            | "JSON_DELETE"
            | "JSON_PATH_DELETE"
            | "JSONB_INSERT"
            | "JSON_INSERT"
            | "JSONB_STRIP_NULLS"
            | "JSON_STRIP_NULLS"
            | "TO_JSON"
            | "TO_JSONB"
            | "ARRAY_TO_JSON"
            | "ROW_TO_JSON"
            | "JSON_ARRAY_LENGTH"
            | "JSONB_ARRAY_LENGTH"
            | "IS_JSON"
            | "JSON_IS_VALID"
            | "JSONB_PATH_EXISTS"
            | "JSON_PATH_EXISTS"
            | "JSONB_EXTRACT_PATH"
            | "JSON_EXTRACT_PATH"
            | "JSONB_EXTRACT_PATH_TEXT"
            | "JSON_EXTRACT_PATH_TEXT"
    )
}

/// True when `name` is a built-in scalar SQL function Takyonic can evaluate.
pub fn is_known_sql_function(name: &str) -> bool {
    is_scalar_fn(&name.trim().to_ascii_uppercase())
}

fn function_to_expression_ctx(
    func: &Function,
    ctes: &HashMap<String, LogicalPlan>,
    outer_ref_scope: &[String],
    subquery_outer: &[String],
) -> Result<Expression> {
    let name = object_name_leaf(&func.name)?;
    let upper = name.to_ascii_uppercase();
    if upper == "COALESCE" {
        if func.over.is_some() || func.filter.is_some() {
            return Err(TakyonicError::Sql(
                "COALESCE does not support OVER/FILTER".into(),
            ));
        }
        let args = function_arg_list(func, ctes, outer_ref_scope, subquery_outer)?;
        if args.is_empty() {
            return Err(TakyonicError::Sql(
                "COALESCE requires at least one argument".into(),
            ));
        }
        return Ok(Expression::Coalesce(args));
    }
    if upper == "NULLIF" {
        if func.over.is_some() || func.filter.is_some() {
            return Err(TakyonicError::Sql(
                "NULLIF does not support OVER/FILTER".into(),
            ));
        }
        let args = function_arg_list(func, ctes, outer_ref_scope, subquery_outer)?;
        if args.len() != 2 {
            return Err(TakyonicError::Sql(
                "NULLIF requires exactly two arguments".into(),
            ));
        }
        return Ok(Expression::NullIf {
            left: Box::new(args[0].clone()),
            right: Box::new(args[1].clone()),
        });
    }
    if upper == "ROW_TO_JSON" {
        if func.over.is_some() || func.filter.is_some() {
            return Err(TakyonicError::Sql(
                "ROW_TO_JSON does not support OVER/FILTER".into(),
            ));
        }
        return plan_row_to_json(func, ctes, outer_ref_scope, subquery_outer);
    }
    if is_scalar_fn(&upper) {
        if func.over.is_some() || func.filter.is_some() {
            return Err(TakyonicError::Sql(format!(
                "{upper} does not support OVER/FILTER"
            )));
        }
        let args = function_arg_list(func, ctes, outer_ref_scope, subquery_outer)?;
        match upper.as_str() {
            "LOWER" | "UPPER" | "LENGTH" | "CHAR_LENGTH" | "CHARACTER_LENGTH"
            | "OCTET_LENGTH" | "BIT_LENGTH"
                if args.len() != 1 =>
            {
                return Err(TakyonicError::Sql(format!(
                    "{upper} requires exactly one argument"
                )));
            }
            "TRIM" if !(1..=3).contains(&args.len()) => {
                return Err(TakyonicError::Sql(
                    "TRIM requires string [, side] [, characters] \
                     (or SQL TRIM(LEADING|TRAILING|BOTH [chars] FROM expr))"
                        .into(),
                ));
            }
            "BTRIM" | "LTRIM" | "RTRIM" if !(1..=2).contains(&args.len()) => {
                return Err(TakyonicError::Sql(format!(
                    "{upper} requires string [, characters]"
                )));
            }
            "TRANSLATE" if args.len() != 3 => {
                return Err(TakyonicError::Sql(
                    "TRANSLATE requires string, from, and to arguments".into(),
                ));
            }
            "SUBSTRING" | "SUBSTR" if !(2..=3).contains(&args.len()) => {
                return Err(TakyonicError::Sql(
                    "SUBSTRING/SUBSTR requires 2 or 3 arguments".into(),
                ));
            }
            "CONCAT" if args.is_empty() => {
                return Err(TakyonicError::Sql(
                    "CONCAT requires at least one argument".into(),
                ));
            }
            "CONCAT_WS" if args.len() < 2 => {
                return Err(TakyonicError::Sql(
                    "CONCAT_WS requires separator and at least one value".into(),
                ));
            }
            "FORMAT" if args.is_empty() => {
                return Err(TakyonicError::Sql(
                    "FORMAT requires a format string".into(),
                ));
            }
            "QUOTE_IDENT" | "QUOTE_LITERAL" | "QUOTE_NULLABLE" if args.len() != 1 => {
                return Err(TakyonicError::Sql(format!(
                    "{upper} requires exactly one argument"
                )));
            }
            "WIDTH_BUCKET" if args.len() != 4 => {
                return Err(TakyonicError::Sql(
                    "WIDTH_BUCKET requires operand, low, high, and count".into(),
                ));
            }
            "REPLACE" if args.len() != 3 => {
                return Err(TakyonicError::Sql(
                    "REPLACE requires exactly three arguments".into(),
                ));
            }
            "REGEXP_REPLACE" if !(3..=4).contains(&args.len()) => {
                return Err(TakyonicError::Sql(
                    "REGEXP_REPLACE requires string, pattern, replacement [, flags]".into(),
                ));
            }
            "REGEXP_LIKE" if !(2..=3).contains(&args.len()) => {
                return Err(TakyonicError::Sql(
                    "REGEXP_LIKE requires string, pattern [, flags]".into(),
                ));
            }
            "LPAD" | "RPAD" if !(2..=3).contains(&args.len()) => {
                return Err(TakyonicError::Sql(format!(
                    "{upper} requires string, length [, fill]"
                )));
            }
            "REPEAT" if args.len() != 2 => {
                return Err(TakyonicError::Sql(
                    "REPEAT requires string and count arguments".into(),
                ));
            }
            "LEFT" | "RIGHT" if args.len() != 2 => {
                return Err(TakyonicError::Sql(format!(
                    "{upper} requires string and length arguments"
                )));
            }
            "REVERSE" if args.len() != 1 => {
                return Err(TakyonicError::Sql(
                    "REVERSE requires exactly one string argument".into(),
                ));
            }
            "INITCAP" | "ASCII" if args.len() != 1 => {
                return Err(TakyonicError::Sql(format!(
                    "{upper} requires exactly one string argument"
                )));
            }
            "CHR" if args.len() != 1 => {
                return Err(TakyonicError::Sql(
                    "CHR requires exactly one integer argument".into(),
                ));
            }
            "MD5" if args.len() != 1 => {
                return Err(TakyonicError::Sql(
                    "MD5 requires exactly one string argument".into(),
                ));
            }
            "ENCODE" | "DECODE" if args.len() != 2 => {
                return Err(TakyonicError::Sql(format!(
                    "{upper} requires data and format arguments"
                )));
            }
            "STARTS_WITH" if args.len() != 2 => {
                return Err(TakyonicError::Sql(
                    "STARTS_WITH requires string and prefix arguments".into(),
                ));
            }
            "ENDS_WITH" if args.len() != 2 => {
                return Err(TakyonicError::Sql(
                    "ENDS_WITH requires string and suffix arguments".into(),
                ));
            }
            "OVERLAY" if !(3..=4).contains(&args.len()) => {
                return Err(TakyonicError::Sql(
                    "OVERLAY requires string, replacement, from [, for]".into(),
                ));
            }
            "POSITION" | "STRPOS" if args.len() != 2 => {
                return Err(TakyonicError::Sql(
                    "POSITION/STRPOS requires exactly two arguments".into(),
                ));
            }
            "ABS" | "CEIL" | "CEILING" | "FLOOR" | "NEGATE" | "SIGN" | "SQRT" | "CBRT" | "LN"
            | "EXP" | "SIN" | "COS" | "TAN" | "ASIN" | "ACOS" | "ATAN" | "RADIANS" | "DEGREES"
                if args.len() != 1 =>
            {
                return Err(TakyonicError::Sql(format!(
                    "{upper} requires exactly one argument"
                )));
            }
            "ATAN2" if args.len() != 2 => {
                return Err(TakyonicError::Sql(
                    "ATAN2 requires y and x arguments".into(),
                ));
            }
            "PI" if !args.is_empty() => {
                return Err(TakyonicError::Sql("PI takes no arguments".into()));
            }
            "NOW" | "CURRENT_TIMESTAMP" | "CURRENT_DATE" | "CURRENT_TIME" | "LOCALTIMESTAMP"
            | "LOCALTIME" | "CLOCK_TIMESTAMP" | "TIMEOFDAY" | "STATEMENT_TIMESTAMP"
            | "TRANSACTION_TIMESTAMP" | "CURRENT_USER" | "SESSION_USER" | "USER"
            | "CURRENT_ROLE" | "CURRENT_SCHEMA" | "CURRENT_CATALOG" | "VERSION"
            | "PG_BACKEND_PID" | "PG_IS_IN_RECOVERY" | "PG_JIT_AVAILABLE"
            | "PG_RELOAD_CONF" | "PG_ROTATE_LOGFILE"
            | "CURRENT_QUERY"
            | "PG_NOTIFICATION_QUEUE_USAGE"
            | "PG_LISTENING_CHANNELS"
            | "PG_ADVISORY_UNLOCK_ALL"
            | "GETDATABASEENCODING"
            | "PG_CLIENT_ENCODING" | "GEN_RANDOM_UUID" | "PG_POSTMASTER_START_TIME"
            | "PG_CONF_LOAD_TIME"
            | "LASTVAL"
            | "TXID_CURRENT" | "PG_CURRENT_XACT_ID" | "INET_SERVER_ADDR" | "INET_SERVER_PORT"
            | "INET_CLIENT_ADDR" | "INET_CLIENT_PORT"
            | "PG_EXPORT_SNAPSHOT" | "PG_CURRENT_SNAPSHOT" | "TXID_CURRENT_SNAPSHOT"
            | "PG_CURRENT_WAL_LSN" | "PG_CURRENT_WAL_INSERT_LSN"
            | "PG_CURRENT_WAL_FLUSH_LSN"
            | "PG_SWITCH_WAL" | "PG_SWITCH_XLOG"
            | "PG_LAST_WAL_RECEIVE_LSN" | "PG_LAST_WAL_REPLAY_LSN"
            | "PG_LAST_XACT_REPLAY_TIMESTAMP" | "PG_IS_WAL_REPLAY_PAUSED"
            | "PG_WAL_REPLAY_PAUSE" | "PG_WAL_REPLAY_RESUME"
            | "PG_IS_IN_BACKUP" | "PG_BACKUP_START_TIME"
            | "PG_BACKUP_STOP" | "PG_STOP_BACKUP"
                if !args.is_empty() =>
            {
                return Err(TakyonicError::Sql(format!(
                    "{upper} takes no arguments"
                )));
            }
            "PG_BACKUP_START" | "PG_START_BACKUP" if !(1..=3).contains(&args.len()) => {
                return Err(TakyonicError::Sql(format!(
                    "{upper} requires label [, fast [, exclusive]]"
                )));
            }
            "PG_CREATE_RESTORE_POINT" if args.len() != 1 => {
                return Err(TakyonicError::Sql(
                    "PG_CREATE_RESTORE_POINT requires exactly one name argument".into(),
                ));
            }
            "PG_PROMOTE" if args.len() > 2 => {
                return Err(TakyonicError::Sql(
                    "PG_PROMOTE accepts at most wait and wait_seconds arguments".into(),
                ));
            }
            "PG_WAL_LSN_DIFF" if args.len() != 2 => {
                return Err(TakyonicError::Sql(
                    "PG_WAL_LSN_DIFF requires two LSN arguments".into(),
                ));
            }
            "PG_WALFILE_NAME" | "PG_WALFILE_NAME_OFFSET" if args.len() != 1 => {
                return Err(TakyonicError::Sql(format!(
                    "{upper} requires exactly one LSN argument"
                )));
            }
            "PG_TRY_ADVISORY_LOCK" | "PG_ADVISORY_LOCK" | "PG_ADVISORY_UNLOCK"
            | "PG_TRY_ADVISORY_LOCK_SHARED" | "PG_ADVISORY_LOCK_SHARED"
            | "PG_ADVISORY_UNLOCK_SHARED"
            | "PG_TRY_ADVISORY_XACT_LOCK" | "PG_ADVISORY_XACT_LOCK"
            | "PG_TRY_ADVISORY_XACT_LOCK_SHARED" | "PG_ADVISORY_XACT_LOCK_SHARED"
                if !(1..=2).contains(&args.len()) =>
            {
                return Err(TakyonicError::Sql(format!(
                    "{upper} requires key bigint [, or key1 int, key2 int]"
                )));
            }
            "CURRENT_SCHEMAS" if args.len() != 1 => {
                return Err(TakyonicError::Sql(
                    "CURRENT_SCHEMAS requires a boolean include_implicit argument".into(),
                ));
            }
            "PG_TYPEOF" if args.len() != 1 => {
                return Err(TakyonicError::Sql(
                    "PG_TYPEOF requires exactly one argument".into(),
                ));
            }
            "TXID_STATUS" | "PG_XACT_STATUS" if args.len() != 1 => {
                return Err(TakyonicError::Sql(format!(
                    "{upper} requires exactly one xid argument"
                )));
            }
            "PG_SNAPSHOT_XMIN" | "PG_SNAPSHOT_XMAX" if args.len() != 1 => {
                return Err(TakyonicError::Sql(format!(
                    "{upper} requires exactly one snapshot argument"
                )));
            }
            "PG_VISIBLE_IN_SNAPSHOT" if args.len() != 2 => {
                return Err(TakyonicError::Sql(
                    "PG_VISIBLE_IN_SNAPSHOT requires xid and snapshot arguments".into(),
                ));
            }
            "PG_CANCEL_BACKEND" | "PG_TERMINATE_BACKEND" if args.len() != 1 => {
                return Err(TakyonicError::Sql(format!(
                    "{upper} requires exactly one pid argument"
                )));
            }
            "PG_ENCODING_TO_CHAR" | "PG_CHAR_TO_ENCODING" if args.len() != 1 => {
                return Err(TakyonicError::Sql(format!(
                    "{upper} requires exactly one argument"
                )));
            }
            "PG_TABLE_IS_VISIBLE" | "PG_TYPE_IS_VISIBLE" if args.len() != 1 => {
                return Err(TakyonicError::Sql(format!(
                    "{upper} requires exactly one argument"
                )));
            }
            "PG_FUNCTION_IS_VISIBLE" | "TO_REGPROC" | "TO_REGPROCEDURE" if args.len() != 1 => {
                return Err(TakyonicError::Sql(format!(
                    "{upper} requires exactly one argument"
                )));
            }
            "PG_OPERATOR_IS_VISIBLE" | "TO_REGOPER" | "TO_REGOPERATOR" if args.len() != 1 => {
                return Err(TakyonicError::Sql(format!(
                    "{upper} requires exactly one argument"
                )));
            }
            "PG_COLLATION_IS_VISIBLE" | "TO_REGCOLLATION" if args.len() != 1 => {
                return Err(TakyonicError::Sql(format!(
                    "{upper} requires exactly one argument"
                )));
            }
            "PG_RELATION_IS_UPDATABLE" if args.len() != 2 => {
                return Err(TakyonicError::Sql(
                    "PG_RELATION_IS_UPDATABLE requires relation, include_triggers".into(),
                ));
            }
            "PG_COLUMN_IS_UPDATABLE" if args.len() != 3 => {
                return Err(TakyonicError::Sql(
                    "PG_COLUMN_IS_UPDATABLE requires table, column, include_triggers".into(),
                ));
            }
            "PG_GET_INDEXDEF" if !(1..=3).contains(&args.len()) => {
                return Err(TakyonicError::Sql(
                    "PG_GET_INDEXDEF requires index [, column_no, pretty]".into(),
                ));
            }
            "PG_DESCRIBE_OBJECT" if args.len() != 3 => {
                return Err(TakyonicError::Sql(
                    "PG_DESCRIBE_OBJECT requires classid, objid, objsubid".into(),
                ));
            }
            "PG_IDENTIFY_OBJECT" if args.len() != 3 => {
                return Err(TakyonicError::Sql(
                    "PG_IDENTIFY_OBJECT requires classid, objid, objsubid".into(),
                ));
            }
            "PG_SIZE_PRETTY" if args.len() != 1 => {
                return Err(TakyonicError::Sql(
                    "PG_SIZE_PRETTY requires exactly one numeric argument".into(),
                ));
            }
            "PG_SIZE_BYTES" if args.len() != 1 => {
                return Err(TakyonicError::Sql(
                    "PG_SIZE_BYTES requires exactly one text argument".into(),
                ));
            }
            "PG_SLEEP" | "PG_COLUMN_SIZE" if args.len() != 1 => {
                return Err(TakyonicError::Sql(format!(
                    "{upper} requires exactly one argument"
                )));
            }
            "PG_NOTIFY" if args.len() != 2 => {
                return Err(TakyonicError::Sql(
                    "PG_NOTIFY requires channel and payload arguments".into(),
                ));
            }
            "NEXTVAL" | "CURRVAL" if args.len() != 1 => {
                return Err(TakyonicError::Sql(format!(
                    "{upper} requires a sequence name argument"
                )));
            }
            "SETVAL" if args.len() != 2 && args.len() != 3 => {
                return Err(TakyonicError::Sql(
                    "SETVAL requires sequence, value, and optional is_called".into(),
                ));
            }
            "PG_GET_SERIAL_SEQUENCE" if args.len() != 2 => {
                return Err(TakyonicError::Sql(
                    "PG_GET_SERIAL_SEQUENCE requires table and column arguments".into(),
                ));
            }
            "PG_SEQUENCE_LAST_VALUE" if args.len() != 1 => {
                return Err(TakyonicError::Sql(
                    "PG_SEQUENCE_LAST_VALUE requires a sequence name argument".into(),
                ));
            }
            "CURRENT_SETTING" if !(1..=2).contains(&args.len()) => {
                return Err(TakyonicError::Sql(
                    "CURRENT_SETTING requires name [, missing_ok]".into(),
                ));
            }
            "SET_CONFIG" if args.len() != 3 => {
                return Err(TakyonicError::Sql(
                    "SET_CONFIG requires name, value, is_local".into(),
                ));
            }
            "HAS_TABLE_PRIVILEGE" if !(2..=3).contains(&args.len()) => {
                return Err(TakyonicError::Sql(
                    "HAS_TABLE_PRIVILEGE requires table, privilege [, or user, table, privilege]"
                        .into(),
                ));
            }
            "HAS_COLUMN_PRIVILEGE" if !(3..=4).contains(&args.len()) => {
                return Err(TakyonicError::Sql(
                    "HAS_COLUMN_PRIVILEGE requires table, column, privilege [, or user, table, column, privilege]"
                        .into(),
                ));
            }
            "HAS_ANY_COLUMN_PRIVILEGE" if !(2..=3).contains(&args.len()) => {
                return Err(TakyonicError::Sql(
                    "HAS_ANY_COLUMN_PRIVILEGE requires table, privilege [, or user, table, privilege]"
                        .into(),
                ));
            }
            "OBJ_DESCRIPTION" if !(1..=2).contains(&args.len()) => {
                return Err(TakyonicError::Sql(
                    "OBJ_DESCRIPTION requires object (name or oid) [, catalog]".into(),
                ));
            }
            "COL_DESCRIPTION" if args.len() != 2 => {
                return Err(TakyonicError::Sql(
                    "COL_DESCRIPTION requires (table, column) or (oid, attnum)".into(),
                ));
            }
            "SHOBJ_DESCRIPTION" if args.len() != 2 => {
                return Err(TakyonicError::Sql(
                    "SHOBJ_DESCRIPTION requires name, catalog (e.g. pg_authid / pg_database)".into(),
                ));
            }
            "TO_REGCLASS" if args.len() != 1 => {
                return Err(TakyonicError::Sql(
                    "TO_REGCLASS requires a relation name".into(),
                ));
            }
            "TO_REGROLE" if args.len() != 1 => {
                return Err(TakyonicError::Sql(
                    "TO_REGROLE requires a role name".into(),
                ));
            }
            "TO_REGNAMESPACE" if args.len() != 1 => {
                return Err(TakyonicError::Sql(
                    "TO_REGNAMESPACE requires a schema name".into(),
                ));
            }
            "TO_REGTYPE" if args.len() != 1 => {
                return Err(TakyonicError::Sql(
                    "TO_REGTYPE requires a type name".into(),
                ));
            }
            "FORMAT_TYPE" if args.len() != 2 => {
                return Err(TakyonicError::Sql(
                    "FORMAT_TYPE requires type_oid, typmod".into(),
                ));
            }
            "PG_GET_USERBYID" if args.len() != 1 => {
                return Err(TakyonicError::Sql(
                    "PG_GET_USERBYID requires a role oid".into(),
                ));
            }
            "PG_RELATION_SIZE" | "PG_TABLE_SIZE" | "PG_TOTAL_RELATION_SIZE" | "PG_INDEXES_SIZE"
                if args.len() != 1 =>
            {
                return Err(TakyonicError::Sql(format!(
                    "{upper} requires a relation name or oid"
                )));
            }
            "PG_DATABASE_SIZE" if args.len() != 1 => {
                return Err(TakyonicError::Sql(
                    "PG_DATABASE_SIZE requires a database name".into(),
                ));
            }
            "PG_TABLESPACE_LOCATION" if args.len() != 1 => {
                return Err(TakyonicError::Sql(
                    "PG_TABLESPACE_LOCATION requires a tablespace name or OID".into(),
                ));
            }
            "HAS_SCHEMA_PRIVILEGE" if !(2..=3).contains(&args.len()) => {
                return Err(TakyonicError::Sql(
                    "HAS_SCHEMA_PRIVILEGE requires schema, privilege [, or user, schema, privilege]"
                        .into(),
                ));
            }
            "HAS_DATABASE_PRIVILEGE" if !(2..=3).contains(&args.len()) => {
                return Err(TakyonicError::Sql(
                    "HAS_DATABASE_PRIVILEGE requires database, privilege [, or user, database, privilege]"
                        .into(),
                ));
            }
            "HAS_TABLESPACE_PRIVILEGE" if !(2..=3).contains(&args.len()) => {
                return Err(TakyonicError::Sql(
                    "HAS_TABLESPACE_PRIVILEGE requires tablespace, privilege [, or user, tablespace, privilege]"
                        .into(),
                ));
            }
            "HAS_FUNCTION_PRIVILEGE" if !(2..=3).contains(&args.len()) => {
                return Err(TakyonicError::Sql(
                    "HAS_FUNCTION_PRIVILEGE requires function, privilege [, or user, function, privilege]"
                        .into(),
                ));
            }
            "HAS_TYPE_PRIVILEGE" if !(2..=3).contains(&args.len()) => {
                return Err(TakyonicError::Sql(
                    "HAS_TYPE_PRIVILEGE requires type, privilege [, or user, type, privilege]"
                        .into(),
                ));
            }
            "HAS_SEQUENCE_PRIVILEGE" if !(2..=3).contains(&args.len()) => {
                return Err(TakyonicError::Sql(
                    "HAS_SEQUENCE_PRIVILEGE requires sequence, privilege [, or user, sequence, privilege]"
                        .into(),
                ));
            }
            "PG_HAS_ROLE" if !(2..=3).contains(&args.len()) => {
                return Err(TakyonicError::Sql(
                    "PG_HAS_ROLE requires role, privilege [, or user, role, privilege]".into(),
                ));
            }
            "ROUND" | "TRUNC" if !(1..=2).contains(&args.len()) => {
                return Err(TakyonicError::Sql(format!(
                    "{upper} requires one or two arguments"
                )));
            }
            "LOG" if !(1..=2).contains(&args.len()) => {
                return Err(TakyonicError::Sql(
                    "LOG requires one or two numeric arguments".into(),
                ));
            }
            "MOD" | "DIV" | "POWER" | "POW" if args.len() != 2 => {
                return Err(TakyonicError::Sql(format!(
                    "{upper} requires exactly two arguments"
                )));
            }
            "GREATEST" | "LEAST" if args.len() < 2 => {
                return Err(TakyonicError::Sql(format!(
                    "{upper} requires at least two arguments"
                )));
            }
            "NUM_NONNULLS" | "NUM_NULLS" if args.is_empty() => {
                return Err(TakyonicError::Sql(format!(
                    "{upper} requires at least one argument"
                )));
            }
            "RANDOM" if !args.is_empty() => {
                return Err(TakyonicError::Sql("RANDOM takes no arguments".into()));
            }
            "SETSEED" if args.len() != 1 => {
                return Err(TakyonicError::Sql(
                    "SETSEED requires exactly one numeric argument in [-1, 1]".into(),
                ));
            }
            "EXTRACT" | "DATE_PART" if args.len() != 2 => {
                return Err(TakyonicError::Sql(
                    "EXTRACT/DATE_PART requires field and source expression".into(),
                ));
            }
            "OVERLAPS" if args.len() != 4 => {
                return Err(TakyonicError::Sql(
                    "OVERLAPS requires two (start, end|interval) pairs".into(),
                ));
            }
            "DATE_TRUNC" if args.len() != 2 => {
                return Err(TakyonicError::Sql(
                    "DATE_TRUNC requires field and source expression".into(),
                ));
            }
            "MAKE_DATE" if args.len() != 3 => {
                return Err(TakyonicError::Sql(
                    "MAKE_DATE requires year, month, day".into(),
                ));
            }
            "MAKE_TIME" if args.len() != 3 => {
                return Err(TakyonicError::Sql(
                    "MAKE_TIME requires hour, minute, second".into(),
                ));
            }
            "MAKE_TIMESTAMP" if args.len() != 6 => {
                return Err(TakyonicError::Sql(
                    "MAKE_TIMESTAMP requires year, month, day, hour, minute, second".into(),
                ));
            }
            "MAKE_INTERVAL" if !(1..=7).contains(&args.len()) => {
                return Err(TakyonicError::Sql(
                    "MAKE_INTERVAL requires 1..7 args: years, months, weeks, days, hours, mins, secs"
                        .into(),
                ));
            }
            "ISFINITE" if args.len() != 1 => {
                return Err(TakyonicError::Sql(
                    "ISFINITE requires exactly one timestamp or interval argument".into(),
                ));
            }
            "TIMEZONE" if args.len() != 2 => {
                return Err(TakyonicError::Sql(
                    "TIMEZONE requires zone and timestamp arguments".into(),
                ));
            }
            "DATE_BIN" if !(2..=3).contains(&args.len()) => {
                return Err(TakyonicError::Sql(
                    "DATE_BIN requires stride, source [, origin]".into(),
                ));
            }
            "JUSTIFY_HOURS" | "JUSTIFY_DAYS" | "JUSTIFY_INTERVAL" if args.len() != 1 => {
                return Err(TakyonicError::Sql(format!(
                    "{upper} requires exactly one interval argument"
                )));
            }
            "AGE" if !(1..=2).contains(&args.len()) => {
                return Err(TakyonicError::Sql(
                    "AGE requires one or two timestamp arguments".into(),
                ));
            }
            "TO_CHAR" | "TO_TIMESTAMP" | "TO_DATE" | "TO_NUMBER" if args.len() != 2 => {
                return Err(TakyonicError::Sql(format!(
                    "{upper} requires value and format arguments"
                )));
            }
            "ARRAY_LENGTH" if args.len() != 2 => {
                return Err(TakyonicError::Sql(
                    "ARRAY_LENGTH requires array and dimension arguments".into(),
                ));
            }
            "CARDINALITY" if args.len() != 1 => {
                return Err(TakyonicError::Sql(
                    "CARDINALITY requires exactly one array argument".into(),
                ));
            }
            "ARRAY_CAT" if args.len() != 2 => {
                return Err(TakyonicError::Sql(
                    "ARRAY_CAT requires exactly two array arguments".into(),
                ));
            }
            "STRING_TO_ARRAY" if !(2..=3).contains(&args.len()) => {
                return Err(TakyonicError::Sql(
                    "STRING_TO_ARRAY requires string, delimiter [, null_string]".into(),
                ));
            }
            "ARRAY_TO_STRING" if !(2..=3).contains(&args.len()) => {
                return Err(TakyonicError::Sql(
                    "ARRAY_TO_STRING requires array, delimiter [, null_string]".into(),
                ));
            }
            "SPLIT_PART" if args.len() != 3 => {
                return Err(TakyonicError::Sql(
                    "SPLIT_PART requires string, delimiter, and field number".into(),
                ));
            }
            "REGEXP_SPLIT_TO_ARRAY" if !(2..=3).contains(&args.len()) => {
                return Err(TakyonicError::Sql(
                    "REGEXP_SPLIT_TO_ARRAY requires string, pattern [, flags]".into(),
                ));
            }
            "ARRAY_CONTAINS" | "ARRAY_CONTAINED_BY" | "ARRAY_OVERLAP" if args.len() != 2 => {
                return Err(TakyonicError::Sql(format!(
                    "{upper} requires exactly two array arguments"
                )));
            }
            "JSON_GET" | "JSON_GET_TEXT" if args.len() != 2 => {
                return Err(TakyonicError::Sql(format!(
                    "{upper} requires json and key/index arguments"
                )));
            }
            "JSON_TYPEOF" | "JSONB_TYPEOF" if args.len() != 1 => {
                return Err(TakyonicError::Sql(format!(
                    "{upper} requires exactly one json argument"
                )));
            }
            "JSON_PATH_GET" | "JSON_PATH_GET_TEXT" if args.len() != 2 => {
                return Err(TakyonicError::Sql(format!(
                    "{upper} requires json and path arguments"
                )));
            }
            "JSON_CONTAINS" | "JSON_CONTAINED_BY" if args.len() != 2 => {
                return Err(TakyonicError::Sql(format!(
                    "{upper} requires exactly two json arguments"
                )));
            }
            "JSONB_SET" | "JSON_SET" if !(3..=4).contains(&args.len()) => {
                return Err(TakyonicError::Sql(format!(
                    "{upper} requires target, path, new_value [, create_missing]"
                )));
            }
            "JSON_CONCAT" if args.len() != 2 => {
                return Err(TakyonicError::Sql(
                    "JSON_CONCAT requires exactly two json arguments".into(),
                ));
            }
            "JSONB_BUILD_OBJECT" | "JSON_BUILD_OBJECT" if args.len() % 2 != 0 => {
                return Err(TakyonicError::Sql(format!(
                    "{upper} requires an even number of arguments (key/value pairs)"
                )));
            }
            "JSONB_PRETTY" | "JSON_PRETTY" if args.len() != 1 => {
                return Err(TakyonicError::Sql(format!(
                    "{upper} requires exactly one json argument"
                )));
            }
            "JSON_DELETE" | "JSON_PATH_DELETE" if args.len() != 2 => {
                return Err(TakyonicError::Sql(format!(
                    "{upper} requires json and key/path arguments"
                )));
            }
            "JSONB_INSERT" | "JSON_INSERT" if !(3..=4).contains(&args.len()) => {
                return Err(TakyonicError::Sql(format!(
                    "{upper} requires target, path, new_value [, insert_after]"
                )));
            }
            "JSONB_STRIP_NULLS" | "JSON_STRIP_NULLS" if args.len() != 1 => {
                return Err(TakyonicError::Sql(format!(
                    "{upper} requires exactly one json argument"
                )));
            }
            "TO_JSON" | "TO_JSONB" | "ARRAY_TO_JSON" if args.len() != 1 => {
                return Err(TakyonicError::Sql(format!(
                    "{upper} requires exactly one argument"
                )));
            }
            "JSON_ARRAY_LENGTH" | "JSONB_ARRAY_LENGTH" if args.len() != 1 => {
                return Err(TakyonicError::Sql(format!(
                    "{upper} requires exactly one json array argument"
                )));
            }
            "IS_JSON" | "JSON_IS_VALID" if args.len() != 1 => {
                return Err(TakyonicError::Sql(format!(
                    "{upper} requires exactly one text argument"
                )));
            }
            "JSONB_PATH_EXISTS" | "JSON_PATH_EXISTS" if args.len() != 2 => {
                return Err(TakyonicError::Sql(format!(
                    "{upper} requires json and path arguments"
                )));
            }
            "JSONB_EXTRACT_PATH"
            | "JSON_EXTRACT_PATH"
            | "JSONB_EXTRACT_PATH_TEXT"
            | "JSON_EXTRACT_PATH_TEXT"
                if args.len() < 2 =>
            {
                return Err(TakyonicError::Sql(format!(
                    "{upper} requires json and at least one path element"
                )));
            }
            "ROW_TO_JSON" => {} // validated in plan_row_to_json
            _ => {}
        }
        return Ok(Expression::ScalarFunction {
            name: upper,
            args,
        });
    }
    if !is_aggregate_fn(&upper) {
        return Err(TakyonicError::Sql(format!(
            "unsupported function `{name}` (aggregates, COALESCE/NULLIF, \
             string/math scalars, date/time helpers)"
        )));
    }
    if func.over.is_some() {
        return Err(TakyonicError::Sql(
            "window functions (OVER) are not supported".into(),
        ));
    }
    let filter = match &func.filter {
        None => None,
        Some(pred) => Some(Box::new(expr_to_expression_ctx(
            pred,
            ctes,
            outer_ref_scope,
            subquery_outer,
        )?)),
    };
    let mut distinct = false;
    let mut order_by = Vec::new();
    let args = match &func.args {
        FunctionArguments::None => Vec::new(),
        FunctionArguments::List(list) => {
            if matches!(
                list.duplicate_treatment,
                Some(DuplicateTreatment::Distinct)
            ) {
                distinct = true;
            }
            for clause in &list.clauses {
                match clause {
                    FunctionArgumentClause::OrderBy(exprs) => {
                        for e in exprs {
                            order_by.push(plan_order_by_expr_ctx(
                                e,
                                ctes,
                                outer_ref_scope,
                                subquery_outer,
                            )?);
                        }
                    }
                    other => {
                        return Err(TakyonicError::Sql(format!(
                            "unsupported aggregate argument clause: {other}"
                        )));
                    }
                }
            }
            let mut out = Vec::with_capacity(list.args.len());
            for arg in &list.args {
                out.push(function_arg_to_expression_ctx(arg, ctes, outer_ref_scope, subquery_outer)?);
            }
            out
        }
        FunctionArguments::Subquery(_) => {
            return Err(TakyonicError::Sql(
                "subquery function arguments are unsupported".into(),
            ));
        }
    };
    let args = if upper == "COUNT" && args.len() == 1 {
        match &args[0] {
            Expression::Literal(s) if s == "*" => Vec::new(),
            _ => args,
        }
    } else {
        args
    };
    // PG ordered-set: MODE() / PERCENTILE_*() WITHIN GROUP (ORDER BY expr).
    let (args, order_by) = if upper == "MODE" && !func.within_group.is_empty() {
        if !args.is_empty() {
            return Err(TakyonicError::Sql(
                "MODE() WITHIN GROUP does not accept direct arguments".into(),
            ));
        }
        if func.within_group.len() != 1 {
            return Err(TakyonicError::Sql(
                "MODE() WITHIN GROUP requires exactly one ORDER BY expression".into(),
            ));
        }
        let mut wg_order = Vec::new();
        for e in &func.within_group {
            wg_order.push(plan_order_by_expr_ctx(
                e,
                ctes,
                outer_ref_scope,
                subquery_outer,
            )?);
        }
        let mode_arg = wg_order[0].expr.clone();
        (vec![mode_arg], wg_order)
    } else if matches!(upper.as_str(), "PERCENTILE_CONT" | "PERCENTILE_DISC")
        && !func.within_group.is_empty()
    {
        if args.len() != 1 {
            return Err(TakyonicError::Sql(format!(
                "{upper}(fraction) WITHIN GROUP requires exactly one fraction argument"
            )));
        }
        if func.within_group.len() != 1 {
            return Err(TakyonicError::Sql(format!(
                "{upper}() WITHIN GROUP requires exactly one ORDER BY expression"
            )));
        }
        let mut wg_order = Vec::new();
        for e in &func.within_group {
            wg_order.push(plan_order_by_expr_ctx(
                e,
                ctes,
                outer_ref_scope,
                subquery_outer,
            )?);
        }
        let mut merged = args;
        merged.push(wg_order[0].expr.clone());
        (merged, wg_order)
    } else {
        (args, order_by)
    };
    if distinct && args.is_empty() {
        return Err(TakyonicError::Sql(
            "COUNT(DISTINCT *) is not supported; use COUNT(DISTINCT expr)".into(),
        ));
    }
    if matches!(upper.as_str(), "JSON_AGG" | "JSONB_AGG" | "ARRAY_AGG") && args.len() != 1 {
        return Err(TakyonicError::Sql(format!(
            "{upper} requires exactly one argument"
        )));
    }
    if matches!(upper.as_str(), "BOOL_AND" | "BOOL_OR" | "EVERY") && args.len() != 1 {
        return Err(TakyonicError::Sql(format!(
            "{upper} requires exactly one argument"
        )));
    }
    if matches!(upper.as_str(), "BIT_AND" | "BIT_OR") && args.len() != 1 {
        return Err(TakyonicError::Sql(format!(
            "{upper} requires exactly one argument"
        )));
    }
    if matches!(
        upper.as_str(),
        "STDDEV" | "STDDEV_POP" | "STDDEV_SAMP" | "VARIANCE" | "VAR_POP" | "VAR_SAMP"
    ) && args.len() != 1
    {
        return Err(TakyonicError::Sql(format!(
            "{upper} requires exactly one argument"
        )));
    }
    if matches!(
        upper.as_str(),
        "CORR"
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
    ) && args.len() != 2
    {
        return Err(TakyonicError::Sql(format!(
            "{upper} requires exactly two arguments (y, x)"
        )));
    }
    if upper == "STRING_AGG" && args.len() != 2 {
        return Err(TakyonicError::Sql(
            "STRING_AGG requires exactly two arguments (expression, delimiter)".into(),
        ));
    }
    if matches!(upper.as_str(), "JSON_OBJECT_AGG" | "JSONB_OBJECT_AGG") && args.len() != 2 {
        return Err(TakyonicError::Sql(format!(
            "{upper} requires exactly two arguments (key, value)"
        )));
    }
    if upper == "MODE" && args.len() != 1 {
        return Err(TakyonicError::Sql(
            "MODE requires one argument or MODE() WITHIN GROUP (ORDER BY expr)".into(),
        ));
    }
    if matches!(upper.as_str(), "PERCENTILE_CONT" | "PERCENTILE_DISC") {
        if args.len() != 2 {
            return Err(TakyonicError::Sql(format!(
                "{upper}(fraction) WITHIN GROUP (ORDER BY expr) is required"
            )));
        }
        let frac = expr_as_fraction_literal(&args[0])?;
        if !(0.0..=1.0).contains(&frac) {
            return Err(TakyonicError::Sql(format!(
                "{upper} fraction must be between 0 and 1, got {frac}"
            )));
        }
    }
    Ok(Expression::AggregateFunction {
        name: match upper.as_str() {
            "EVERY" => "BOOL_AND".into(),
            "STDDEV" => "STDDEV_SAMP".into(),
            "VARIANCE" => "VAR_SAMP".into(),
            _ => upper,
        },
        args,
        filter,
        distinct,
        order_by,
    })
}

#[allow(dead_code)]
fn function_arg_to_expression(arg: &FunctionArg) -> Result<Expression> {
    function_arg_to_expression_ctx(arg, &HashMap::new(), &[], &[])
}

fn function_arg_to_expression_ctx(
    arg: &FunctionArg,
    ctes: &HashMap<String, LogicalPlan>,
    outer_ref_scope: &[String],
    subquery_outer: &[String],
) -> Result<Expression> {
    match arg {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => {
            expr_to_expression_ctx(e, ctes, outer_ref_scope, subquery_outer)
        }
        FunctionArg::Unnamed(FunctionArgExpr::Wildcard)
        | FunctionArg::Unnamed(FunctionArgExpr::WildcardWithOptions(_)) => {
            Ok(Expression::Literal("*".into()))
        }
        FunctionArg::Unnamed(FunctionArgExpr::QualifiedWildcard(_)) => Err(TakyonicError::Sql(
            "qualified wildcard function args are unsupported".into(),
        )),
        FunctionArg::Named { .. } | FunctionArg::ExprNamed { .. } => Err(TakyonicError::Sql(
            "named function arguments are unsupported".into(),
        )),
    }
}

#[allow(dead_code)]
fn plan_join_operator(op: &JoinOperator) -> Result<(JoinType, Expression)> {
    plan_join_operator_ctx(op, &HashMap::new(), &[], &[])
}

fn plan_join_operator_ctx(
    op: &JoinOperator,
    ctes: &HashMap<String, LogicalPlan>,
    outer_ref_scope: &[String],
    subquery_outer: &[String],
) -> Result<(JoinType, Expression)> {
    let (join_type, constraint) = match op {
        JoinOperator::Join(c) | JoinOperator::Inner(c) => (JoinType::Inner, c),
        JoinOperator::Left(c) | JoinOperator::LeftOuter(c) => (JoinType::Left, c),
        JoinOperator::Right(c) | JoinOperator::RightOuter(c) => (JoinType::Right, c),
        JoinOperator::FullOuter(c) => (JoinType::Full, c),
        JoinOperator::CrossJoin(c) => (JoinType::Inner, c),
        other => {
            return Err(TakyonicError::Sql(format!(
                "unsupported JOIN operator: {other:?}"
            )));
        }
    };
    let on = match constraint {
        JoinConstraint::On(expr) => {
            expr_to_expression_ctx(expr, ctes, outer_ref_scope, subquery_outer)?
        }
        JoinConstraint::None => Expression::Literal("true".into()),
        other => {
            return Err(TakyonicError::Sql(format!(
                "JOIN requires ON condition (or CROSS JOIN), got {other:?}"
            )));
        }
    };
    Ok((join_type, on))
}

/// Translate a SQL expression into our simplified [`Expression`] tree.
fn expr_to_expression(expr: &Expr) -> Result<Expression> {
    expr_to_expression_ctx(expr, &HashMap::new(), &[], &[])
}

fn expr_to_expression_ctx(
    expr: &Expr,
    ctes: &HashMap<String, LogicalPlan>,
    outer_ref_scope: &[String],
    subquery_outer: &[String],
) -> Result<Expression> {
    match expr {
        Expr::Identifier(ident) => {
            let upper = ident.value.to_ascii_uppercase();
            if matches!(
                upper.as_str(),
                "CURRENT_TIMESTAMP" | "CURRENT_DATE" | "CURRENT_TIME" | "LOCALTIMESTAMP" | "LOCALTIME"
                    | "CURRENT_ROLE"
            ) {
                return Ok(Expression::ScalarFunction {
                    name: upper,
                    args: Vec::new(),
                });
            }
            Ok(Expression::Column(ident.value.clone()))
        }
        Expr::CompoundIdentifier(parts) => {
            if parts.len() >= 2 {
                let qual = &parts[parts.len() - 2].value;
                let col = &parts[parts.len() - 1].value;
                if outer_ref_scope.iter().any(|o| o == qual) {
                    return Ok(Expression::OuterRef(col.clone()));
                }
            }
            parts
                .last()
                .map(|i| Expression::Column(i.value.clone()))
                .ok_or_else(|| TakyonicError::Sql("empty compound identifier".into()))
        }
        Expr::Value(ValueWithSpan { value, .. }) => match value {
            SqlValue::Placeholder(p) => Ok(Expression::Parameter(parse_placeholder(p)?)),
            other => Ok(Expression::Literal(sql_value_to_string(other)?)),
        },
        Expr::Nested(inner) => expr_to_expression_ctx(inner, ctes, outer_ref_scope, subquery_outer),
        Expr::UnaryOp { op, expr } => {
            let inner = expr_to_expression_ctx(expr, ctes, outer_ref_scope, subquery_outer)?;
            match op {
                UnaryOperator::Plus => Ok(inner),
                UnaryOperator::Minus => Ok(Expression::ScalarFunction {
                    name: "NEGATE".into(),
                    args: vec![inner],
                }),
                UnaryOperator::PGAbs => Ok(Expression::ScalarFunction {
                    name: "ABS".into(),
                    args: vec![inner],
                }),
                UnaryOperator::Not | UnaryOperator::BangNot => Ok(Expression::Not {
                    expr: Box::new(inner),
                }),
                other => Err(TakyonicError::Sql(format!(
                    "unsupported unary operator: {other}"
                ))),
            }
        }
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => Ok(Expression::And {
            left: Box::new(expr_to_expression_ctx(left, ctes, outer_ref_scope, subquery_outer)?),
            right: Box::new(expr_to_expression_ctx(right, ctes, outer_ref_scope, subquery_outer)?),
        }),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Or,
            right,
        } => Ok(Expression::Or {
            left: Box::new(expr_to_expression_ctx(left, ctes, outer_ref_scope, subquery_outer)?),
            right: Box::new(expr_to_expression_ctx(right, ctes, outer_ref_scope, subquery_outer)?),
        }),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Overlaps,
            right,
        } => {
            let (a, b) = overlaps_tuple_pair(left, ctes, outer_ref_scope, subquery_outer)?;
            let (c, d) = overlaps_tuple_pair(right, ctes, outer_ref_scope, subquery_outer)?;
            Ok(Expression::ScalarFunction {
                name: "OVERLAPS".into(),
                args: vec![a, b, c, d],
            })
        }
        Expr::BinaryOp { left, op, right } => {
            let left_e = Box::new(expr_to_expression_ctx(left, ctes, outer_ref_scope, subquery_outer)?);
            let right_e = Box::new(expr_to_expression_ctx(right, ctes, outer_ref_scope, subquery_outer)?);
            if let Some(arith) = match op {
                BinaryOperator::Plus => Some(ArithOp::Add),
                BinaryOperator::Minus => {
                    // `jsonb - key` deletes; numeric minus stays arithmetic.
                    if expr_looks_like_json(&left_e) {
                        None
                    } else {
                        Some(ArithOp::Sub)
                    }
                }
                BinaryOperator::Multiply => Some(ArithOp::Mul),
                BinaryOperator::Divide => Some(ArithOp::Div),
                _ => None,
            } {
                return Ok(Expression::Arith {
                    left: left_e,
                    op: arith,
                    right: right_e,
                });
            }
            if matches!(op, BinaryOperator::Minus) && expr_looks_like_json(&left_e) {
                return Ok(Expression::ScalarFunction {
                    name: "JSON_DELETE".into(),
                    args: vec![*left_e, *right_e],
                });
            }
            if matches!(op, BinaryOperator::HashMinus) {
                return Ok(Expression::ScalarFunction {
                    name: "JSON_PATH_DELETE".into(),
                    args: vec![*left_e, *right_e],
                });
            }
            if matches!(op, BinaryOperator::LtDashGt) {
                return Ok(Expression::VectorDistance {
                    left: left_e,
                    right: right_e,
                    metric: DistanceMetric::Euclidean,
                });
            }
            if matches!(op, BinaryOperator::StringConcat) {
                if expr_looks_like_sql_array(&left_e) || expr_looks_like_sql_array(&right_e) {
                    return Ok(Expression::ScalarFunction {
                        name: "ARRAY_CAT".into(),
                        args: vec![*left_e, *right_e],
                    });
                }
                if expr_looks_like_json(&left_e) || expr_looks_like_json(&right_e) {
                    return Ok(Expression::ScalarFunction {
                        name: "JSON_CONCAT".into(),
                        args: vec![*left_e, *right_e],
                    });
                }
                return Ok(Expression::ScalarFunction {
                    name: "CONCAT".into(),
                    args: vec![*left_e, *right_e],
                });
            }
            let array_cmp = match op {
                BinaryOperator::AtArrow => {
                    if expr_looks_like_sql_array(&left_e) || expr_looks_like_sql_array(&right_e) {
                        Some("ARRAY_CONTAINS")
                    } else {
                        Some("JSON_CONTAINS")
                    }
                }
                BinaryOperator::ArrowAt => {
                    if expr_looks_like_sql_array(&left_e) || expr_looks_like_sql_array(&right_e) {
                        Some("ARRAY_CONTAINED_BY")
                    } else {
                        Some("JSON_CONTAINED_BY")
                    }
                }
                BinaryOperator::PGOverlap => Some("ARRAY_OVERLAP"),
                BinaryOperator::Arrow => Some("JSON_GET"),
                BinaryOperator::LongArrow => Some("JSON_GET_TEXT"),
                BinaryOperator::HashArrow => Some("JSON_PATH_GET"),
                BinaryOperator::HashLongArrow => Some("JSON_PATH_GET_TEXT"),
                _ => None,
            };
            if let Some(name) = array_cmp {
                return Ok(Expression::ScalarFunction {
                    name: name.into(),
                    args: vec![*left_e, *right_e],
                });
            }
            let like = match op {
                BinaryOperator::PGLikeMatch => Some((false, false)),
                BinaryOperator::PGILikeMatch => Some((true, false)),
                BinaryOperator::PGNotLikeMatch => Some((false, true)),
                BinaryOperator::PGNotILikeMatch => Some((true, true)),
                _ => None,
            };
            if let Some((case_insensitive, negated)) = like {
                return Ok(Expression::Like {
                    expr: left_e,
                    pattern: right_e,
                    case_insensitive,
                    negated,
                    any: false,
                    escape: None,
                });
            }
            let regex = match op {
                BinaryOperator::PGRegexMatch => Some((false, false)),
                BinaryOperator::PGRegexIMatch => Some((true, false)),
                BinaryOperator::PGRegexNotMatch => Some((false, true)),
                BinaryOperator::PGRegexNotIMatch => Some((true, true)),
                _ => None,
            };
            if let Some((case_insensitive, negated)) = regex {
                return Ok(Expression::RegexMatch {
                    expr: left_e,
                    pattern: right_e,
                    case_insensitive,
                    negated,
                });
            }
            let filter_op = match op {
                BinaryOperator::Eq => FilterOp::Eq,
                BinaryOperator::NotEq => FilterOp::Ne,
                BinaryOperator::Gt => FilterOp::Gt,
                BinaryOperator::GtEq => FilterOp::Gte,
                BinaryOperator::Lt => FilterOp::Lt,
                BinaryOperator::LtEq => FilterOp::Lte,
                other => {
                    return Err(TakyonicError::Sql(format!(
                        "unsupported expression operator: {other}"
                    )));
                }
            };
            Ok(Expression::BinaryOp {
                left: left_e,
                op: filter_op,
                right: right_e,
            })
        }
        Expr::Array(Array { elem, .. }) => {
            let mut items = Vec::with_capacity(elem.len());
            for e in elem {
                items.push(expr_to_expression_ctx(e, ctes, outer_ref_scope, subquery_outer)?);
            }
            Ok(Expression::Array(items))
        }
        Expr::CompoundFieldAccess {
            root,
            access_chain,
        } => {
            let mut expr =
                expr_to_expression_ctx(root, ctes, outer_ref_scope, subquery_outer)?;
            for access in access_chain {
                match access {
                    AccessExpr::Subscript(Subscript::Index { index }) => {
                        let idx = expr_to_expression_ctx(
                            index,
                            ctes,
                            outer_ref_scope,
                            subquery_outer,
                        )?;
                        expr = Expression::ArrayIndex {
                            array: Box::new(expr),
                            index: Box::new(idx),
                        };
                    }
                    other => {
                        return Err(TakyonicError::Sql(format!(
                            "unsupported field access `{other}` (only arr[i] subscript)"
                        )));
                    }
                }
            }
            Ok(expr)
        }
        Expr::Function(func) => function_to_expression_ctx(func, ctes, outer_ref_scope, subquery_outer),
        Expr::InSubquery {
            expr: left,
            subquery,
            negated,
        } => {
            let left_expr = expr_to_expression_ctx(left, ctes, outer_ref_scope, subquery_outer)?;
            let sub_plan = LogicalPlanner::plan_query(subquery, ctes, subquery_outer)?;
            let value_column = subquery_value_column(subquery, ctes, subquery_outer)?;
            let correlated = plan_is_correlated(&sub_plan, subquery_outer);
            Ok(Expression::InSubquery {
                expr: Box::new(left_expr),
                subquery: Box::new(sub_plan),
                value_column,
                negated: *negated,
                correlated,
            })
        }
        Expr::Exists { subquery, negated } => {
            let sub_plan = LogicalPlanner::plan_query(subquery, ctes, subquery_outer)?;
            let correlated = plan_is_correlated(&sub_plan, subquery_outer);
            Ok(Expression::Exists {
                subquery: Box::new(sub_plan),
                negated: *negated,
                correlated,
            })
        }
        Expr::Subquery(subquery) => {
            let sub_plan = LogicalPlanner::plan_query(subquery, ctes, subquery_outer)?;
            let value_column = subquery_value_column(subquery, ctes, subquery_outer)?;
            let correlated = plan_is_correlated(&sub_plan, subquery_outer);
            Ok(Expression::ScalarSubquery {
                subquery: Box::new(sub_plan),
                value_column,
                correlated,
            })
        }
        Expr::InList {
            expr: left,
            list,
            negated,
        } => {
            let left_expr = expr_to_expression_ctx(left, ctes, outer_ref_scope, subquery_outer)?;
            let mut values = Vec::with_capacity(list.len());
            for item in list {
                match expr_to_expression_ctx(item, ctes, outer_ref_scope, subquery_outer)? {
                    Expression::Literal(s) => values.push(Value::from_text(&s)),
                    Expression::Parameter(_) => {
                        return Err(TakyonicError::Sql(
                            "parameterized IN-list values are not yet supported".into(),
                        ));
                    }
                    other => {
                        return Err(TakyonicError::Sql(format!(
                            "IN-list requires literals, got {other:?}"
                        )));
                    }
                }
            }
            Ok(Expression::InList {
                expr: Box::new(left_expr),
                list: values,
                negated: *negated,
            })
        }
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            let subject = expr_to_expression_ctx(expr, ctes, outer_ref_scope, subquery_outer)?;
            let low = expr_to_expression_ctx(low, ctes, outer_ref_scope, subquery_outer)?;
            let high = expr_to_expression_ctx(high, ctes, outer_ref_scope, subquery_outer)?;
            // `x BETWEEN lo AND hi` → `x >= lo AND x <= hi`
            // `x NOT BETWEEN lo AND hi` → `x < lo OR x > hi`
            if *negated {
                Ok(Expression::Or {
                    left: Box::new(Expression::BinaryOp {
                        left: Box::new(subject.clone()),
                        op: FilterOp::Lt,
                        right: Box::new(low),
                    }),
                    right: Box::new(Expression::BinaryOp {
                        left: Box::new(subject),
                        op: FilterOp::Gt,
                        right: Box::new(high),
                    }),
                })
            } else {
                Ok(Expression::And {
                    left: Box::new(Expression::BinaryOp {
                        left: Box::new(subject.clone()),
                        op: FilterOp::Gte,
                        right: Box::new(low),
                    }),
                    right: Box::new(Expression::BinaryOp {
                        left: Box::new(subject),
                        op: FilterOp::Lte,
                        right: Box::new(high),
                    }),
                })
            }
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            let else_result = match else_result {
                Some(e) => Some(Box::new(expr_to_expression_ctx(
                    e,
                    ctes,
                    outer_ref_scope,
                    subquery_outer,
                )?)),
                None => None,
            };
            let mut when_then = Vec::with_capacity(conditions.len());
            if let Some(op) = operand {
                let subject =
                    expr_to_expression_ctx(op, ctes, outer_ref_scope, subquery_outer)?;
                for arm in conditions {
                    let when_val = expr_to_expression_ctx(
                        &arm.condition,
                        ctes,
                        outer_ref_scope,
                        subquery_outer,
                    )?;
                    let then_e = expr_to_expression_ctx(
                        &arm.result,
                        ctes,
                        outer_ref_scope,
                        subquery_outer,
                    )?;
                    when_then.push((
                        Expression::BinaryOp {
                            left: Box::new(subject.clone()),
                            op: FilterOp::Eq,
                            right: Box::new(when_val),
                        },
                        then_e,
                    ));
                }
            } else {
                for arm in conditions {
                    when_then.push((
                        expr_to_expression_ctx(
                            &arm.condition,
                            ctes,
                            outer_ref_scope,
                            subquery_outer,
                        )?,
                        expr_to_expression_ctx(
                            &arm.result,
                            ctes,
                            outer_ref_scope,
                            subquery_outer,
                        )?,
                    ));
                }
            }
            Ok(Expression::Case {
                when_then,
                else_result,
            })
        }
        Expr::IsNull(inner) => Ok(Expression::IsNull {
            expr: Box::new(expr_to_expression_ctx(
                inner,
                ctes,
                outer_ref_scope,
                subquery_outer,
            )?),
            negated: false,
        }),
        Expr::IsNotNull(inner) => Ok(Expression::IsNull {
            expr: Box::new(expr_to_expression_ctx(
                inner,
                ctes,
                outer_ref_scope,
                subquery_outer,
            )?),
            negated: true,
        }),
        Expr::IsTrue(inner) => Ok(Expression::IsBoolTest {
            expr: Box::new(expr_to_expression_ctx(
                inner,
                ctes,
                outer_ref_scope,
                subquery_outer,
            )?),
            test: BoolTest::True,
            negated: false,
        }),
        Expr::IsNotTrue(inner) => Ok(Expression::IsBoolTest {
            expr: Box::new(expr_to_expression_ctx(
                inner,
                ctes,
                outer_ref_scope,
                subquery_outer,
            )?),
            test: BoolTest::True,
            negated: true,
        }),
        Expr::IsFalse(inner) => Ok(Expression::IsBoolTest {
            expr: Box::new(expr_to_expression_ctx(
                inner,
                ctes,
                outer_ref_scope,
                subquery_outer,
            )?),
            test: BoolTest::False,
            negated: false,
        }),
        Expr::IsNotFalse(inner) => Ok(Expression::IsBoolTest {
            expr: Box::new(expr_to_expression_ctx(
                inner,
                ctes,
                outer_ref_scope,
                subquery_outer,
            )?),
            test: BoolTest::False,
            negated: true,
        }),
        Expr::IsUnknown(inner) => Ok(Expression::IsBoolTest {
            expr: Box::new(expr_to_expression_ctx(
                inner,
                ctes,
                outer_ref_scope,
                subquery_outer,
            )?),
            test: BoolTest::Unknown,
            negated: false,
        }),
        Expr::IsNotUnknown(inner) => Ok(Expression::IsBoolTest {
            expr: Box::new(expr_to_expression_ctx(
                inner,
                ctes,
                outer_ref_scope,
                subquery_outer,
            )?),
            test: BoolTest::Unknown,
            negated: true,
        }),
        Expr::IsDistinctFrom(left, right) => Ok(Expression::IsDistinctFrom {
            left: Box::new(expr_to_expression_ctx(
                left,
                ctes,
                outer_ref_scope,
                subquery_outer,
            )?),
            right: Box::new(expr_to_expression_ctx(
                right,
                ctes,
                outer_ref_scope,
                subquery_outer,
            )?),
            negated: false,
        }),
        Expr::IsNotDistinctFrom(left, right) => Ok(Expression::IsDistinctFrom {
            left: Box::new(expr_to_expression_ctx(
                left,
                ctes,
                outer_ref_scope,
                subquery_outer,
            )?),
            right: Box::new(expr_to_expression_ctx(
                right,
                ctes,
                outer_ref_scope,
                subquery_outer,
            )?),
            negated: true,
        }),
        Expr::AnyOp {
            left,
            compare_op,
            right,
            ..
        } => plan_quantified_cmp(
            left,
            compare_op,
            right,
            Quantifier::Any,
            ctes,
            outer_ref_scope,
            subquery_outer,
        ),
        Expr::AllOp {
            left,
            compare_op,
            right,
        } => plan_quantified_cmp(
            left,
            compare_op,
            right,
            Quantifier::All,
            ctes,
            outer_ref_scope,
            subquery_outer,
        ),
        Expr::Cast {
            kind,
            expr,
            data_type,
            array,
            ..
        } => {
            if *array {
                return Err(TakyonicError::Sql(
                    "CAST(... AS type ARRAY) is not supported".into(),
                ));
            }
            let try_cast = matches!(kind, CastKind::TryCast | CastKind::SafeCast);
            if !matches!(
                kind,
                CastKind::Cast | CastKind::DoubleColon | CastKind::TryCast | CastKind::SafeCast
            ) {
                return Err(TakyonicError::Sql(format!(
                    "unsupported cast kind: {kind:?}"
                )));
            }
            Ok(Expression::Cast {
                expr: Box::new(expr_to_expression_ctx(
                    expr,
                    ctes,
                    outer_ref_scope,
                    subquery_outer,
                )?),
                target: cast_target_from_datatype(data_type)?,
                try_cast,
            })
        }
        Expr::Substring {
            expr,
            substring_from,
            substring_for,
            ..
        } => {
            let mut args = vec![expr_to_expression_ctx(
                expr,
                ctes,
                outer_ref_scope,
                subquery_outer,
            )?];
            match substring_from {
                Some(from) => args.push(expr_to_expression_ctx(
                    from,
                    ctes,
                    outer_ref_scope,
                    subquery_outer,
                )?),
                None => args.push(Expression::Literal("1".into())),
            }
            if let Some(for_len) = substring_for {
                args.push(expr_to_expression_ctx(
                    for_len,
                    ctes,
                    outer_ref_scope,
                    subquery_outer,
                )?);
            }
            Ok(Expression::ScalarFunction {
                name: "SUBSTRING".into(),
                args,
            })
        }
        Expr::Overlay {
            expr,
            overlay_what,
            overlay_from,
            overlay_for,
        } => {
            let mut args = vec![
                expr_to_expression_ctx(expr, ctes, outer_ref_scope, subquery_outer)?,
                expr_to_expression_ctx(overlay_what, ctes, outer_ref_scope, subquery_outer)?,
                expr_to_expression_ctx(overlay_from, ctes, outer_ref_scope, subquery_outer)?,
            ];
            if let Some(for_len) = overlay_for {
                args.push(expr_to_expression_ctx(
                    for_len,
                    ctes,
                    outer_ref_scope,
                    subquery_outer,
                )?);
            }
            Ok(Expression::ScalarFunction {
                name: "OVERLAY".into(),
                args,
            })
        }
        Expr::Position { expr, r#in } => Ok(Expression::ScalarFunction {
            // POSITION(needle IN haystack) → STRPOS(haystack, needle) arg order
            name: "STRPOS".into(),
            args: vec![
                expr_to_expression_ctx(r#in, ctes, outer_ref_scope, subquery_outer)?,
                expr_to_expression_ctx(expr, ctes, outer_ref_scope, subquery_outer)?,
            ],
        }),
        Expr::Ceil { expr, field } => {
            if !matches!(
                field,
                CeilFloorKind::DateTimeField(DateTimeField::NoDateTime)
            ) {
                return Err(TakyonicError::Sql(
                    "CEIL scale/datetime forms are not supported".into(),
                ));
            }
            Ok(Expression::ScalarFunction {
                name: "CEIL".into(),
                args: vec![expr_to_expression_ctx(
                    expr,
                    ctes,
                    outer_ref_scope,
                    subquery_outer,
                )?],
            })
        }
        Expr::Floor { expr, field } => {
            if !matches!(
                field,
                CeilFloorKind::DateTimeField(DateTimeField::NoDateTime)
            ) {
                return Err(TakyonicError::Sql(
                    "FLOOR scale/datetime forms are not supported".into(),
                ));
            }
            Ok(Expression::ScalarFunction {
                name: "FLOOR".into(),
                args: vec![expr_to_expression_ctx(
                    expr,
                    ctes,
                    outer_ref_scope,
                    subquery_outer,
                )?],
            })
        }
        Expr::Extract { field, expr, .. } => Ok(Expression::ScalarFunction {
            name: "EXTRACT".into(),
            args: vec![
                Expression::Literal(field.to_string()),
                expr_to_expression_ctx(expr, ctes, outer_ref_scope, subquery_outer)?,
            ],
        }),
        Expr::Trim {
            expr,
            trim_where,
            trim_what,
            trim_characters,
        } => {
            if trim_characters.is_some() {
                return Err(TakyonicError::Sql(
                    "TRIM(... FROM ...) with character list syntax is not yet supported"
                        .into(),
                ));
            }
            let side = match trim_where {
                None | Some(TrimWhereField::Both) => "BOTH",
                Some(TrimWhereField::Leading) => "LEADING",
                Some(TrimWhereField::Trailing) => "TRAILING",
            };
            let mut args = vec![expr_to_expression_ctx(
                expr,
                ctes,
                outer_ref_scope,
                subquery_outer,
            )?];
            args.push(Expression::Literal(side.into()));
            if let Some(what) = trim_what {
                args.push(expr_to_expression_ctx(
                    what,
                    ctes,
                    outer_ref_scope,
                    subquery_outer,
                )?);
            }
            Ok(Expression::ScalarFunction {
                name: "TRIM".into(),
                args,
            })
        }
        Expr::Like {
            negated,
            any,
            expr,
            pattern,
            escape_char,
        } => plan_like_expr(
            false,
            *negated,
            *any,
            expr,
            pattern,
            escape_char.as_ref(),
            ctes,
            outer_ref_scope,
            subquery_outer,
        ),
        Expr::ILike {
            negated,
            any,
            expr,
            pattern,
            escape_char,
        } => plan_like_expr(
            true,
            *negated,
            *any,
            expr,
            pattern,
            escape_char.as_ref(),
            ctes,
            outer_ref_scope,
            subquery_outer,
        ),
        Expr::SimilarTo {
            negated,
            expr,
            pattern,
            escape_char,
        } => plan_similar_to_expr(
            *negated,
            expr,
            pattern,
            escape_char.as_ref(),
            ctes,
            outer_ref_scope,
            subquery_outer,
        ),
        Expr::AtTimeZone {
            timestamp,
            time_zone,
        } => Ok(Expression::AtTimeZone {
            timestamp: Box::new(expr_to_expression_ctx(
                timestamp,
                ctes,
                outer_ref_scope,
                subquery_outer,
            )?),
            time_zone: Box::new(expr_to_expression_ctx(
                time_zone,
                ctes,
                outer_ref_scope,
                subquery_outer,
            )?),
        }),
        Expr::Interval(AstInterval {
            value,
            leading_field,
            last_field,
            ..
        }) => {
            if last_field.is_some() {
                return Err(TakyonicError::Sql(
                    "INTERVAL field ranges (e.g. HOUR TO MINUTE) are not yet supported".into(),
                ));
            }
            let text = match value.as_ref() {
                Expr::Value(ValueWithSpan { value, .. }) => sql_value_to_string(value)?,
                other => {
                    return Err(TakyonicError::Sql(format!(
                        "INTERVAL value must be a string literal, got {other}"
                    )));
                }
            };
            let secs = parse_interval_to_secs(&text, leading_field.as_ref())?;
            Ok(Expression::Literal(encode_interval_secs(secs)))
        }
        other => Err(TakyonicError::Sql(format!(
            "unsupported expression: {other}"
        ))),
    }
}

/// `(start, end|interval)` pair for SQL `OVERLAPS`.
fn overlaps_tuple_pair(
    expr: &Expr,
    ctes: &HashMap<String, LogicalPlan>,
    outer_ref_scope: &[String],
    subquery_outer: &[String],
) -> Result<(Expression, Expression)> {
    match expr {
        Expr::Tuple(elems) if elems.len() == 2 => Ok((
            expr_to_expression_ctx(&elems[0], ctes, outer_ref_scope, subquery_outer)?,
            expr_to_expression_ctx(&elems[1], ctes, outer_ref_scope, subquery_outer)?,
        )),
        other => Err(TakyonicError::Sql(format!(
            "OVERLAPS requires (start, end|interval) row pairs, got {other}"
        ))),
    }
}

fn plan_values_clause(
    values: &sqlparser::ast::Values,
    ctes: &HashMap<String, LogicalPlan>,
    outer_columns: &[String],
    column_names: Option<&[String]>,
) -> Result<LogicalPlan> {
    if values.rows.is_empty() {
        return Err(TakyonicError::Sql("VALUES requires at least one row".into()));
    }
    let width = values.rows[0].len();
    if width == 0 {
        return Err(TakyonicError::Sql("VALUES rows must not be empty".into()));
    }
    for (i, row) in values.rows.iter().enumerate() {
        if row.len() != width {
            return Err(TakyonicError::Sql(format!(
                "VALUES row {i} has {} columns, expected {width}",
                row.len()
            )));
        }
    }
    let columns = if let Some(names) = column_names {
        if names.len() != width {
            return Err(TakyonicError::Sql(format!(
                "VALUES alias has {} columns but row width is {width}",
                names.len()
            )));
        }
        names.to_vec()
    } else {
        // PostgreSQL default names for bare VALUES.
        (1..=width).map(|i| format!("column{i}")).collect()
    };
    let mut rows = Vec::with_capacity(values.rows.len());
    for row in &values.rows {
        let mut exprs = Vec::with_capacity(width);
        for e in row.iter() {
            exprs.push(expr_to_expression_ctx(
                e,
                ctes,
                outer_columns,
                outer_columns,
            )?);
        }
        rows.push(exprs);
    }
    Ok(LogicalPlan::Values { columns, rows })
}

fn plan_quantified_cmp(
    left: &Expr,
    compare_op: &BinaryOperator,
    right: &Expr,
    quantifier: Quantifier,
    ctes: &HashMap<String, LogicalPlan>,
    outer_ref_scope: &[String],
    subquery_outer: &[String],
) -> Result<Expression> {
    let filter_op = match compare_op {
        BinaryOperator::Eq => FilterOp::Eq,
        BinaryOperator::NotEq => FilterOp::Ne,
        BinaryOperator::Gt => FilterOp::Gt,
        BinaryOperator::GtEq => FilterOp::Gte,
        BinaryOperator::Lt => FilterOp::Lt,
        BinaryOperator::LtEq => FilterOp::Lte,
        other => {
            return Err(TakyonicError::Sql(format!(
                "unsupported quantified comparison operator: {other}"
            )));
        }
    };
    // PG: `x = ANY(subq)` ≡ `x IN (subq)`; `x <> ALL(subq)` ≡ `x NOT IN (subq)`.
    if let Expr::Subquery(subquery) = right {
        let rewrite_in = match (quantifier, filter_op) {
            (Quantifier::Any, FilterOp::Eq) => Some(false),
            (Quantifier::All, FilterOp::Ne) => Some(true),
            _ => None,
        };
        if let Some(negated) = rewrite_in {
            let left_expr = expr_to_expression_ctx(left, ctes, outer_ref_scope, subquery_outer)?;
            let sub_plan = LogicalPlanner::plan_query(subquery, ctes, subquery_outer)?;
            let value_column = subquery_value_column(subquery, ctes, subquery_outer)?;
            let correlated = plan_is_correlated(&sub_plan, subquery_outer);
            return Ok(Expression::InSubquery {
                expr: Box::new(left_expr),
                subquery: Box::new(sub_plan),
                value_column,
                negated,
                correlated,
            });
        }
        return Err(TakyonicError::Sql(
            "quantified comparison with subquery currently supports only \
             `= ANY|SOME` and `<> ALL`"
                .into(),
        ));
    }
    Ok(Expression::QuantifiedCmp {
        left: Box::new(expr_to_expression_ctx(
            left,
            ctes,
            outer_ref_scope,
            subquery_outer,
        )?),
        op: filter_op,
        right: Box::new(expr_to_expression_ctx(
            right,
            ctes,
            outer_ref_scope,
            subquery_outer,
        )?),
        quantifier,
    })
}

fn plan_like_expr(
    case_insensitive: bool,
    negated: bool,
    any: bool,
    expr: &Expr,
    pattern: &Expr,
    escape_char: Option<&ValueWithSpan>,
    ctes: &HashMap<String, LogicalPlan>,
    outer_ref_scope: &[String],
    subquery_outer: &[String],
) -> Result<Expression> {
    let escape = match escape_char {
        None => None,
        Some(ValueWithSpan {
            value: SqlValue::SingleQuotedString(s),
            ..
        }) => {
            let mut chars = s.chars();
            let c = chars.next().ok_or_else(|| {
                TakyonicError::Sql("ESCAPE requires a single character".into())
            })?;
            if chars.next().is_some() {
                return Err(TakyonicError::Sql(
                    "ESCAPE requires a single character".into(),
                ));
            }
            Some(c)
        }
        Some(other) => {
            return Err(TakyonicError::Sql(format!(
                "unsupported ESCAPE literal: {other}"
            )));
        }
    };
    Ok(Expression::Like {
        expr: Box::new(expr_to_expression_ctx(
            expr,
            ctes,
            outer_ref_scope,
            subquery_outer,
        )?),
        pattern: Box::new(expr_to_expression_ctx(
            pattern,
            ctes,
            outer_ref_scope,
            subquery_outer,
        )?),
        case_insensitive,
        negated,
        any,
        escape,
    })
}

fn plan_similar_to_expr(
    negated: bool,
    expr: &Expr,
    pattern: &Expr,
    escape_char: Option<&ValueWithSpan>,
    ctes: &HashMap<String, LogicalPlan>,
    outer_ref_scope: &[String],
    subquery_outer: &[String],
) -> Result<Expression> {
    let escape = match escape_char {
        None => Some('\\'), // PostgreSQL default escape for SIMILAR TO
        Some(ValueWithSpan {
            value: SqlValue::SingleQuotedString(s),
            ..
        }) => {
            let mut chars = s.chars();
            let c = chars.next().ok_or_else(|| {
                TakyonicError::Sql("ESCAPE requires a single character".into())
            })?;
            if chars.next().is_some() {
                return Err(TakyonicError::Sql(
                    "ESCAPE requires a single character".into(),
                ));
            }
            Some(c)
        }
        Some(other) => {
            return Err(TakyonicError::Sql(format!(
                "unsupported ESCAPE literal: {other}"
            )));
        }
    };
    Ok(Expression::SimilarTo {
        expr: Box::new(expr_to_expression_ctx(
            expr,
            ctes,
            outer_ref_scope,
            subquery_outer,
        )?),
        pattern: Box::new(expr_to_expression_ctx(
            pattern,
            ctes,
            outer_ref_scope,
            subquery_outer,
        )?),
        negated,
        escape,
    })
}

/// SQL `LIKE` / `ILIKE` pattern match (`%` any sequence, `_` one character).
pub fn sql_like_match(
    text: &str,
    pattern: &str,
    case_insensitive: bool,
    escape: Option<char>,
) -> bool {
    let (text, pattern) = if case_insensitive {
        (text.to_lowercase(), pattern.to_lowercase())
    } else {
        (text.to_string(), pattern.to_string())
    };
    let hay: Vec<char> = text.chars().collect();
    let pat: Vec<char> = pattern.chars().collect();
    like_match_chars(&hay, &pat, escape)
}

fn like_match_chars(hay: &[char], pat: &[char], escape: Option<char>) -> bool {
    let mut hi = 0usize;
    let mut pi = 0usize;
    let mut star_hi: Option<usize> = None;
    let mut star_pi: Option<usize> = None;

    while hi < hay.len() {
        if pi < pat.len() {
            let escaped = escape == Some(pat[pi]) && pi + 1 < pat.len();
            if escaped {
                if hay[hi] == pat[pi + 1] {
                    hi += 1;
                    pi += 2;
                    continue;
                }
            } else if pat[pi] == '_' {
                hi += 1;
                pi += 1;
                continue;
            } else if pat[pi] == '%' {
                star_hi = Some(hi);
                star_pi = Some(pi + 1);
                pi += 1;
                continue;
            } else if hay[hi] == pat[pi] {
                hi += 1;
                pi += 1;
                continue;
            }
        }
        if let (Some(sh), Some(sp)) = (star_hi, star_pi) {
            hi = sh + 1;
            star_hi = Some(hi);
            pi = sp;
            continue;
        }
        return false;
    }

    while pi < pat.len() {
        if escape == Some(pat[pi]) && pi + 1 < pat.len() {
            return false;
        }
        if pat[pi] == '%' {
            pi += 1;
            continue;
        }
        return false;
    }
    true
}

/// PostgreSQL / SQL `SIMILAR TO` match (pattern converted to a POSIX regex).
pub fn sql_similar_to_match(text: &str, pattern: &str, escape: Option<char>) -> Result<bool> {
    let posix = similar_to_posix(pattern, escape)?;
    let wrapped = format!("^(?:{posix})$");
    let re = regex::Regex::new(&wrapped).map_err(|e| {
        TakyonicError::Sql(format!("invalid SIMILAR TO pattern `{pattern}`: {e}"))
    })?;
    Ok(re.is_match(text))
}

/// Convert a SQL `SIMILAR TO` pattern into a POSIX extended regex fragment.
fn similar_to_posix(pattern: &str, escape: Option<char>) -> Result<String> {
    let esc = escape.unwrap_or('\\');
    let chars: Vec<char> = pattern.chars().collect();
    let mut out = String::new();
    let mut i = 0usize;
    let mut in_class = false;
    while i < chars.len() {
        let c = chars[i];
        if c == esc {
            if i + 1 >= chars.len() {
                return Err(TakyonicError::Sql(
                    "SIMILAR TO pattern ends with an escape character".into(),
                ));
            }
            let lit = chars[i + 1];
            out.push_str(&regex::escape(&lit.to_string()));
            i += 2;
            continue;
        }
        if in_class {
            out.push(c);
            if c == ']' {
                in_class = false;
            }
            i += 1;
            continue;
        }
        match c {
            '%' => out.push_str(".*"),
            '_' => out.push('.'),
            '[' => {
                in_class = true;
                out.push('[');
            }
            // SQL SIMILAR TO metacharacters → regex metacharacters.
            '|' | '*' | '+' | '(' | ')' => out.push(c),
            // Regex metacharacters that are literals in SIMILAR TO.
            '.' | '^' | '$' | '{' | '}' | '?' | '\\' => {
                out.push_str(&regex::escape(&c.to_string()));
            }
            _ => out.push_str(&regex::escape(&c.to_string())),
        }
        i += 1;
    }
    if in_class {
        return Err(TakyonicError::Sql(
            "SIMILAR TO pattern has an unclosed character class".into(),
        ));
    }
    Ok(out)
}

fn subquery_value_column(
    query: &Query,
    ctes: &HashMap<String, LogicalPlan>,
    outer_columns: &[String],
) -> Result<String> {
    let select = match query.body.as_ref() {
        SetExpr::Select(s) => s.as_ref(),
        other => {
            return Err(TakyonicError::Sql(format!(
                "subquery body must be SELECT, got {other}"
            )));
        }
    };
    first_projection_column(select, ctes, outer_columns)
}

fn plan_is_correlated(plan: &LogicalPlan, outer_columns: &[String]) -> bool {
    if plan_has_outer_ref(plan) {
        return true;
    }
    if outer_columns.is_empty() {
        return false;
    }
    let inner: std::collections::HashSet<String> =
        collect_plan_output_hints(plan).into_iter().collect();
    let mut cols = Vec::new();
    walk_plan_columns(plan, &mut |c| cols.push(c.to_string()));
    cols.iter()
        .any(|c| outer_columns.iter().any(|o| o == c) && !inner.contains(c))
}

fn plan_has_outer_ref(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Select {
            predicate: Some(p), ..
        } => expr_has_outer_ref(p),
        LogicalPlan::Filter { input, predicate } => {
            expr_has_outer_ref(predicate) || plan_has_outer_ref(input)
        }
        LogicalPlan::Join { left, right, on, .. }
        | LogicalPlan::DistributedJoin { left, right, on, .. } => {
            expr_has_outer_ref(on) || plan_has_outer_ref(left) || plan_has_outer_ref(right)
        }
        LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::DistributedAggregate { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Project { input, .. }
        | LogicalPlan::Window { input, .. }
        | LogicalPlan::SubqueryAlias { input, .. }
        | LogicalPlan::Distinct { input, .. }
        | LogicalPlan::Explain { plan: input } => plan_has_outer_ref(input),
        LogicalPlan::DistinctOn { input, exprs } => {
            exprs.iter().any(expr_has_outer_ref) || plan_has_outer_ref(input)
        }
        LogicalPlan::Union { left, right, .. } => {
            plan_has_outer_ref(left) || plan_has_outer_ref(right)
        }
        LogicalPlan::JsonArrayElements { doc, .. }
        | LogicalPlan::JsonEach { doc, .. }
        | LogicalPlan::JsonObjectKeys { doc, .. } => expr_has_outer_ref(doc),
        LogicalPlan::Unnest { array, .. } => expr_has_outer_ref(array),
        LogicalPlan::RegexpSplitToTable {
            string,
            pattern,
            flags,
            ..
        }
        | LogicalPlan::RegexpMatches {
            string,
            pattern,
            flags,
            ..
        } => {
            expr_has_outer_ref(string)
                || expr_has_outer_ref(pattern)
                || flags.as_ref().is_some_and(|f| expr_has_outer_ref(f))
        }
        _ => false,
    }
}

fn expr_has_outer_ref(expr: &Expression) -> bool {
    match expr {
        Expression::OuterRef(_) => true,
        Expression::BinaryOp { left, right, .. }
        | Expression::And { left, right }
        | Expression::Or { left, right }
        | Expression::Arith { left, right, .. }
        | Expression::VectorDistance { left, right, .. }
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
            expr_has_outer_ref(left) || expr_has_outer_ref(right)
        }
        Expression::InList { expr, .. } => expr_has_outer_ref(expr),
        Expression::InSubquery { expr, subquery, .. } => {
            expr_has_outer_ref(expr) || plan_has_outer_ref(subquery)
        }
        Expression::Exists { subquery, .. } | Expression::ScalarSubquery { subquery, .. } => {
            plan_has_outer_ref(subquery)
        }
        Expression::AggregateFunction { args, filter, .. } => {
            args.iter().any(expr_has_outer_ref)
                || filter.as_ref().is_some_and(|p| expr_has_outer_ref(p))
        }
        Expression::Array(items) => items.iter().any(expr_has_outer_ref),
        Expression::ArrayIndex { array, index } => {
            expr_has_outer_ref(array) || expr_has_outer_ref(index)
        }
        Expression::Case {
            when_then,
            else_result,
        } => {
            when_then
                .iter()
                .any(|(c, r)| expr_has_outer_ref(c) || expr_has_outer_ref(r))
                || else_result.as_ref().is_some_and(|e| expr_has_outer_ref(e))
        }
        Expression::IsNull { expr, .. } | Expression::IsBoolTest { expr, .. } => {
            expr_has_outer_ref(expr)
        }
        Expression::Coalesce(args) => args.iter().any(expr_has_outer_ref),
        Expression::Cast { expr, .. } | Expression::Not { expr } => expr_has_outer_ref(expr),
        Expression::IsDistinctFrom { left, right, .. }
        | Expression::QuantifiedCmp { left, right, .. }
        | Expression::NullIf { left, right } => {
            expr_has_outer_ref(left) || expr_has_outer_ref(right)
        }
        Expression::ScalarFunction { args, .. } => args.iter().any(expr_has_outer_ref),
        Expression::Column(_) | Expression::Literal(_) | Expression::Parameter(_) => false,
    }
}

/// True when evaluating `expr` needs the current (outer) row or bind parameters.
pub(crate) fn expr_needs_row_eval(expr: &Expression) -> bool {
    match expr {
        Expression::Column(_) | Expression::OuterRef(_) | Expression::Parameter(_) => true,
        Expression::Literal(_) => false,
        Expression::BinaryOp { left, right, .. }
        | Expression::And { left, right }
        | Expression::Or { left, right }
        | Expression::Arith { left, right, .. }
        | Expression::VectorDistance { left, right, .. }
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
        }
        | Expression::NullIf { left, right }
        | Expression::IsDistinctFrom { left, right, .. }
        | Expression::QuantifiedCmp { left, right, .. } => {
            expr_needs_row_eval(left) || expr_needs_row_eval(right)
        }
        Expression::InList { expr, .. } => expr_needs_row_eval(expr),
        Expression::InSubquery { .. }
        | Expression::Exists { .. }
        | Expression::ScalarSubquery { .. }
        | Expression::AggregateFunction { .. } => true,
        Expression::Array(items) => items.iter().any(expr_needs_row_eval),
        Expression::ArrayIndex { array, index } => {
            expr_needs_row_eval(array) || expr_needs_row_eval(index)
        }
        Expression::Case {
            when_then,
            else_result,
        } => {
            when_then
                .iter()
                .any(|(c, r)| expr_needs_row_eval(c) || expr_needs_row_eval(r))
                || else_result.as_ref().is_some_and(|e| expr_needs_row_eval(e))
        }
        Expression::IsNull { expr, .. }
        | Expression::IsBoolTest { expr, .. }
        | Expression::Cast { expr, .. }
        | Expression::Not { expr } => expr_needs_row_eval(expr),
        Expression::Coalesce(args) | Expression::ScalarFunction { args, .. } => {
            args.iter().any(expr_needs_row_eval)
        }
    }
}

/// True when a FROM relation is marked `LATERAL`.
fn relation_is_lateral(factor: &TableFactor) -> bool {
    match factor {
        TableFactor::Function { lateral, .. } | TableFactor::Derived { lateral, .. } => *lateral,
        _ => false,
    }
}

/// Table / alias names visible for OuterRef qualification in this FROM item.
fn from_relation_scope_names(factor: &TableFactor) -> Vec<String> {
    match factor {
        TableFactor::Table { name, alias, args, .. } => {
            let mut names = Vec::new();
            if let Ok(table) = object_name_leaf(name) {
                names.push(table);
            }
            if let Some(a) = alias {
                names.push(a.name.value.clone());
                for col in &a.columns {
                    names.push(col.name.value.clone());
                }
            } else if args.is_some() {
                // bare generate_series → column name is the function name
                if let Ok(table) = object_name_leaf(name) {
                    if table.eq_ignore_ascii_case("generate_series") {
                        names.push("generate_series".into());
                    }
                }
            }
            names
        }
        TableFactor::Derived { alias, .. } => alias
            .as_ref()
            .map(|a| vec![a.name.value.clone()])
            .unwrap_or_default(),
        TableFactor::UNNEST { alias, .. } => {
            if let Some(a) = alias {
                let mut names = vec![a.name.value.clone()];
                for col in &a.columns {
                    names.push(col.name.value.clone());
                }
                names
            } else {
                vec!["unnest".into()]
            }
        }
        TableFactor::Function { name, alias, .. } => {
            let mut names = Vec::new();
            if let Some(a) = alias {
                names.push(a.name.value.clone());
                for col in &a.columns {
                    names.push(col.name.value.clone());
                }
            } else if let Ok(table) = object_name_leaf(name) {
                names.push(table);
            }
            names
        }
        _ => Vec::new(),
    }
}

fn plan_unnest_srf(
    args: &[FunctionArg],
    alias: Option<&sqlparser::ast::TableAlias>,
    ctes: &HashMap<String, LogicalPlan>,
    lateral_outer: &[String],
    with_ordinality: bool,
) -> Result<LogicalPlan> {
    if args.len() != 1 {
        return Err(TakyonicError::Sql(
            "UNNEST currently supports exactly one array argument".into(),
        ));
    }
    let array =
        function_arg_to_expression_ctx(&args[0], ctes, lateral_outer, lateral_outer)?;
    if expr_needs_row_eval(&array) && lateral_outer.is_empty() {
        return Err(TakyonicError::Sql(
            "correlated LATERAL UNNEST arguments are not yet supported \
             (use literals or CROSS JOIN LATERAL unnest(…))"
                .into(),
        ));
    }
    let (column, ordinality_column) =
        srf_value_and_ordinality_columns(alias, "unnest", with_ordinality);
    Ok(LogicalPlan::Unnest {
        array,
        column,
        ordinality_column,
        zero_based_ordinality: false,
    })
}

/// Materialize `UNNEST` rows from a const-folded array expression.
pub fn materialize_unnest(
    array: &Expression,
    column: &str,
    ordinality_column: Option<&str>,
    zero_based_ordinality: bool,
) -> Result<Vec<crate::schema::Record>> {
    let elems = match array {
        Expression::Array(items) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                match item {
                    Expression::Literal(s) => out.push(s.clone()),
                    other => {
                        return Err(TakyonicError::Sql(format!(
                            "UNNEST element must be a literal, got {other:?}"
                        )));
                    }
                }
            }
            out
        }
        Expression::Literal(s) => parse_array_display_texts(s)?,
        other => {
            return Err(TakyonicError::Sql(format!(
                "UNNEST requires an ARRAY[…] argument, got {other:?}"
            )));
        }
    };
    Ok(elems
        .into_iter()
        .enumerate()
        .map(|(i, text)| {
            let mut row = crate::schema::Record::new().set(column, text);
            if let Some(ord) = ordinality_column {
                let n = if zero_based_ordinality { i } else { i + 1 };
                row = row.set(ord, n.to_string());
            }
            row
        })
        .collect())
}

fn parse_array_display_texts(s: &str) -> Result<Vec<String>> {
    let s = s.trim();
    let inner = s
        .strip_prefix('[')
        .and_then(|x| x.strip_suffix(']'))
        .ok_or_else(|| TakyonicError::Sql(format!("not an array value: `{s}`")))?;
    if inner.is_empty() {
        return Ok(Vec::new());
    }
    Ok(inner.split(',').map(|p| p.trim().to_string()).collect())
}

/// Value column + optional `WITH ORDINALITY` column from a table-function alias.
fn srf_value_and_ordinality_columns(
    alias: Option<&sqlparser::ast::TableAlias>,
    default_value: &str,
    with_ordinality: bool,
) -> (String, Option<String>) {
    let value = if let Some(a) = alias {
        if let Some(col) = a.columns.first() {
            col.name.value.clone()
        } else {
            // `AS g` without column list — use alias name (matches prior generate_series).
            a.name.value.clone()
        }
    } else {
        default_value.to_string()
    };
    let ordinality = if with_ordinality {
        Some(
            alias
                .and_then(|a| a.columns.get(1))
                .map(|c| c.name.value.clone())
                .unwrap_or_else(|| "ordinality".into()),
        )
    } else {
        None
    };
    (value, ordinality)
}

fn json_srf_single_column_name(
    alias: Option<&sqlparser::ast::TableAlias>,
    default: &str,
) -> String {
    if let Some(a) = alias {
        if let Some(col) = a.columns.first() {
            return col.name.value.clone();
        }
        // Bare `AS t` keeps PG default column name (`value`), not the table alias.
        if a.columns.is_empty() && a.name.value.eq_ignore_ascii_case(default) {
            return a.name.value.clone();
        }
        if !a.columns.is_empty() {
            return a.columns[0].name.value.clone();
        }
    }
    default.into()
}

fn json_each_column_names(
    alias: Option<&sqlparser::ast::TableAlias>,
    with_ordinality: bool,
) -> (String, String, Option<String>) {
    let (key, value) = if let Some(a) = alias {
        if a.columns.len() >= 2 {
            (
                a.columns[0].name.value.clone(),
                a.columns[1].name.value.clone(),
            )
        } else if a.columns.len() == 1 {
            (a.columns[0].name.value.clone(), "value".into())
        } else {
            ("key".into(), "value".into())
        }
    } else {
        ("key".into(), "value".into())
    };
    let ordinality = if with_ordinality {
        Some(
            alias
                .and_then(|a| a.columns.get(2))
                .map(|c| c.name.value.clone())
                .unwrap_or_else(|| "ordinality".into()),
        )
    } else {
        None
    };
    (key, value, ordinality)
}

/// Plan `jsonb_array_elements` / `_text` — literals or correlated LATERAL expressions.
fn plan_json_array_elements_srf(
    upper: &str,
    args: &[FunctionArg],
    alias: Option<&sqlparser::ast::TableAlias>,
    ctes: &HashMap<String, LogicalPlan>,
    lateral_outer: &[String],
    with_ordinality: bool,
) -> Result<LogicalPlan> {
    if args.len() != 1 {
        return Err(TakyonicError::Sql(format!(
            "{upper} requires exactly one json argument"
        )));
    }
    let expr =
        function_arg_to_expression_ctx(&args[0], ctes, lateral_outer, lateral_outer)?;
    if expr_needs_row_eval(&expr) && lateral_outer.is_empty() {
        return Err(TakyonicError::Sql(format!(
            "correlated LATERAL {upper} arguments are not yet supported \
             (use literals or CROSS JOIN LATERAL)"
        )));
    }
    let doc = match json_doc_from_planned_expr(&expr, upper) {
        Ok(lit) => Expression::Literal(lit),
        Err(_) => expr,
    };
    let as_text = upper.ends_with("_TEXT");
    let (column, ordinality_column) =
        json_srf_value_and_ordinality(alias, "value", with_ordinality);
    Ok(LogicalPlan::JsonArrayElements {
        doc,
        column,
        as_text,
        ordinality_column,
    })
}

fn plan_json_object_srf(
    upper: &str,
    args: &[FunctionArg],
    alias: Option<&sqlparser::ast::TableAlias>,
    ctes: &HashMap<String, LogicalPlan>,
    lateral_outer: &[String],
    with_ordinality: bool,
) -> Result<LogicalPlan> {
    if args.len() != 1 {
        return Err(TakyonicError::Sql(format!(
            "{upper} requires exactly one json argument"
        )));
    }
    let expr =
        function_arg_to_expression_ctx(&args[0], ctes, lateral_outer, lateral_outer)?;
    if expr_needs_row_eval(&expr) && lateral_outer.is_empty() {
        return Err(TakyonicError::Sql(format!(
            "correlated LATERAL {upper} arguments are not yet supported \
             (use literals or CROSS JOIN LATERAL)"
        )));
    }
    let doc = match json_doc_from_planned_expr(&expr, upper) {
        Ok(lit) => Expression::Literal(lit),
        Err(_) => expr,
    };
    let as_text = upper.ends_with("_TEXT");
    if upper.contains("OBJECT_KEYS") {
        let default = if upper.starts_with("JSONB") {
            "jsonb_object_keys"
        } else {
            "json_object_keys"
        };
        let (column, ordinality_column) =
            json_srf_value_and_ordinality(alias, default, with_ordinality);
        Ok(LogicalPlan::JsonObjectKeys {
            doc,
            column,
            ordinality_column,
        })
    } else {
        let (key_column, value_column, ordinality_column) =
            json_each_column_names(alias, with_ordinality);
        Ok(LogicalPlan::JsonEach {
            doc,
            key_column,
            value_column,
            as_text,
            ordinality_column,
        })
    }
}

fn json_srf_value_and_ordinality(
    alias: Option<&sqlparser::ast::TableAlias>,
    default: &str,
    with_ordinality: bool,
) -> (String, Option<String>) {
    let column = json_srf_single_column_name(alias, default);
    let ordinality = if with_ordinality {
        Some(
            alias
                .and_then(|a| a.columns.get(1))
                .map(|c| c.name.value.clone())
                .unwrap_or_else(|| "ordinality".into()),
        )
    } else {
        None
    };
    (column, ordinality)
}

fn plan_regexp_text_srf(
    fn_name: &str,
    args: &[FunctionArg],
    alias: Option<&sqlparser::ast::TableAlias>,
    ctes: &HashMap<String, LogicalPlan>,
    lateral_outer: &[String],
    with_ordinality: bool,
) -> Result<LogicalPlan> {
    if !(2..=3).contains(&args.len()) {
        return Err(TakyonicError::Sql(format!(
            "{fn_name} requires string, pattern [, flags]"
        )));
    }
    let string =
        function_arg_to_expression_ctx(&args[0], ctes, lateral_outer, lateral_outer)?;
    let pattern =
        function_arg_to_expression_ctx(&args[1], ctes, lateral_outer, lateral_outer)?;
    let flags = args
        .get(2)
        .map(|a| function_arg_to_expression_ctx(a, ctes, lateral_outer, lateral_outer))
        .transpose()?;
    let needs = expr_needs_row_eval(&string)
        || expr_needs_row_eval(&pattern)
        || flags.as_ref().is_some_and(expr_needs_row_eval);
    if needs && lateral_outer.is_empty() {
        return Err(TakyonicError::Sql(format!(
            "correlated LATERAL {fn_name} arguments are not yet supported \
             (use literals or CROSS JOIN LATERAL)"
        )));
    }
    let default_col = fn_name.to_ascii_lowercase();
    let (column, ordinality_column) =
        json_srf_value_and_ordinality(alias, &default_col, with_ordinality);
    match fn_name {
        "REGEXP_SPLIT_TO_TABLE" => Ok(LogicalPlan::RegexpSplitToTable {
            string,
            pattern,
            flags,
            column,
            ordinality_column,
        }),
        "REGEXP_MATCHES" => Ok(LogicalPlan::RegexpMatches {
            string,
            pattern,
            flags,
            column,
            ordinality_column,
        }),
        other => Err(TakyonicError::Sql(format!(
            "internal: unknown regexp SRF `{other}`"
        ))),
    }
}

fn ensure_table_fn_args_are_literals(args: &[FunctionArg], fn_name: &str) -> Result<()> {
    for arg in args {
        let expr = match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => e,
            FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => {
                return Err(TakyonicError::Sql(format!(
                    "{fn_name} does not accept *"
                )));
            }
            other => {
                return Err(TakyonicError::Sql(format!(
                    "unsupported {fn_name} argument: {other}"
                )));
            }
        };
        if !expr_is_json_srf_literal(expr) {
            return Err(TakyonicError::Sql(format!(
                "correlated LATERAL {fn_name} arguments are not yet supported \
                 (use literals)"
            )));
        }
    }
    Ok(())
}

fn expr_is_json_srf_literal(expr: &Expr) -> bool {
    match expr {
        Expr::Value(_) => true,
        Expr::Cast { expr, .. } => expr_is_json_srf_literal(expr),
        Expr::UnaryOp { expr, .. } => expr_is_json_srf_literal(expr),
        Expr::Interval(AstInterval { value, .. }) => expr_is_json_srf_literal(value),
        _ => false,
    }
}

fn json_doc_from_planned_expr(expr: &Expression, fn_name: &str) -> Result<String> {
    match expr {
        Expression::Literal(s) => {
            let v: serde_json::Value = serde_json::from_str(s.trim()).map_err(|e| {
                TakyonicError::Sql(format!("{fn_name}: invalid JSON literal: {e}"))
            })?;
            Ok(v.to_string())
        }
        Expression::Cast {
            expr,
            target: CastTarget::Json,
            ..
        } => json_doc_from_planned_expr(expr, fn_name),
        other => Err(TakyonicError::Sql(format!(
            "{fn_name} currently requires a JSON/JSONB literal (got {other:?})"
        ))),
    }
}

/// Materialize `jsonb_array_elements` / `_text` into rows.
pub fn materialize_json_array_elements(
    doc: &str,
    column: &str,
    as_text: bool,
    ordinality_column: Option<&str>,
) -> Result<Vec<crate::schema::Record>> {
    let v: serde_json::Value = serde_json::from_str(doc.trim()).map_err(|e| {
        TakyonicError::Sql(format!("invalid JSON document: {e}"))
    })?;
    let arr = v.as_array().ok_or_else(|| {
        TakyonicError::Sql("jsonb_array_elements requires a JSON array".into())
    })?;
    let mut rows = Vec::with_capacity(arr.len());
    for (i, elem) in arr.iter().enumerate() {
        let text = if as_text {
            json_value_as_text(elem)
        } else {
            elem.to_string()
        };
        let mut row = crate::schema::Record::new().set(column, text);
        if let Some(ord) = ordinality_column {
            row = row.set(ord, (i + 1).to_string());
        }
        rows.push(row);
    }
    Ok(rows)
}

/// Materialize `json_each` / `jsonb_each` / `*_text` into rows.
pub fn materialize_json_each(
    doc: &str,
    key_column: &str,
    value_column: &str,
    as_text: bool,
    ordinality_column: Option<&str>,
) -> Result<Vec<crate::schema::Record>> {
    let v: serde_json::Value = serde_json::from_str(doc.trim()).map_err(|e| {
        TakyonicError::Sql(format!("invalid JSON document: {e}"))
    })?;
    let obj = v.as_object().ok_or_else(|| {
        TakyonicError::Sql("json_each requires a JSON object".into())
    })?;
    let mut rows = Vec::with_capacity(obj.len());
    for (i, (k, val)) in obj.iter().enumerate() {
        let value_text = if as_text {
            json_value_as_text(val)
        } else {
            val.to_string()
        };
        let mut row = crate::schema::Record::new()
            .set(key_column, k.clone())
            .set(value_column, value_text);
        if let Some(ord) = ordinality_column {
            row = row.set(ord, (i + 1).to_string());
        }
        rows.push(row);
    }
    Ok(rows)
}

/// Materialize `jsonb_object_keys` / `json_object_keys` into rows.
pub fn materialize_json_object_keys(
    doc: &str,
    column: &str,
    ordinality_column: Option<&str>,
) -> Result<Vec<crate::schema::Record>> {
    let v: serde_json::Value = serde_json::from_str(doc.trim()).map_err(|e| {
        TakyonicError::Sql(format!("invalid JSON document: {e}"))
    })?;
    let obj = v.as_object().ok_or_else(|| {
        TakyonicError::Sql("jsonb_object_keys requires a JSON object".into())
    })?;
    let mut rows = Vec::with_capacity(obj.len());
    for (i, k) in obj.keys().enumerate() {
        let mut row = crate::schema::Record::new().set(column, k.clone());
        if let Some(ord) = ordinality_column {
            row = row.set(ord, (i + 1).to_string());
        }
        rows.push(row);
    }
    Ok(rows)
}

fn json_value_as_text(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Null => String::new(),
        other => other.to_string(),
    }
}

/// Parsed `generate_series` bounds (integer or timestamp/interval).
struct GenerateSeriesSpec {
    start: i64,
    stop: i64,
    step: i64,
    as_timestamp: bool,
    date_only: bool,
}

fn parse_generate_series_args(args: &[FunctionArg]) -> Result<GenerateSeriesSpec> {
    if !(2..=3).contains(&args.len()) {
        return Err(TakyonicError::Sql(
            "generate_series requires 2 or 3 arguments".into(),
        ));
    }
    let mut exprs = Vec::with_capacity(args.len());
    for arg in args {
        exprs.push(function_arg_to_expression(arg)?);
    }
    // Prefer integer series when start/stop look like ints.
    if let (Ok(start), Ok(stop)) = (
        expr_as_i64_literal(&exprs[0]),
        expr_as_i64_literal(&exprs[1]),
    ) {
        let step = if let Some(s) = exprs.get(2) {
            expr_as_i64_literal(s)?
        } else {
            1
        };
        if step == 0 {
            return Err(TakyonicError::Sql(
                "generate_series step must not be zero".into(),
            ));
        }
        return Ok(GenerateSeriesSpec {
            start,
            stop,
            step,
            as_timestamp: false,
            date_only: false,
        });
    }

    let (start_unix, start_date_only) = expr_as_timestamp_unix(&exprs[0])?;
    let (stop_unix, stop_date_only) = expr_as_timestamp_unix(&exprs[1])?;
    let step = match exprs.get(2) {
        Some(s) => expr_as_interval_secs(s)?,
        None => {
            return Err(TakyonicError::Sql(
                "generate_series(timestamp, timestamp) requires an INTERVAL step".into(),
            ));
        }
    };
    if step == 0 {
        return Err(TakyonicError::Sql(
            "generate_series step must not be zero".into(),
        ));
    }
    Ok(GenerateSeriesSpec {
        start: start_unix,
        stop: stop_unix,
        step,
        as_timestamp: true,
        date_only: start_date_only && stop_date_only,
    })
}

fn expr_as_i64_literal(expr: &Expression) -> Result<i64> {
    match expr {
        Expression::Literal(s) => {
            if decode_interval_secs(s).is_some() {
                return Err(TakyonicError::Sql(
                    "generate_series integer bounds cannot be INTERVAL".into(),
                ));
            }
            s.parse::<i64>().map_err(|_| {
                TakyonicError::Sql(format!(
                    "generate_series arguments must be integer literals, got `{s}`"
                ))
            })
        }
        Expression::ScalarFunction { name, args }
            if name == "NEGATE" && args.len() == 1 =>
        {
            Ok(-expr_as_i64_literal(&args[0])?)
        }
        Expression::Cast { expr, target, .. }
            if matches!(target, CastTarget::Int | CastTarget::Text) =>
        {
            expr_as_i64_literal(expr)
        }
        other => Err(TakyonicError::Sql(format!(
            "generate_series arguments must be integer literals, got {other:?}"
        ))),
    }
}

/// Parse a literal fraction for ordered-set aggregates (`0`, `0.5`, `1`).
pub(crate) fn expr_as_fraction_literal(expr: &Expression) -> Result<f64> {
    match expr {
        Expression::Literal(s) => {
            if let Ok(i) = s.parse::<i64>() {
                return Ok(i as f64);
            }
            s.parse::<f64>().map_err(|_| {
                TakyonicError::Sql(format!(
                    "percentile fraction must be a numeric literal, got `{s}`"
                ))
            })
        }
        Expression::Cast { expr, .. } => expr_as_fraction_literal(expr),
        other => Err(TakyonicError::Sql(format!(
            "percentile fraction must be a numeric literal, got {other:?}"
        ))),
    }
}

fn expr_as_timestamp_unix(expr: &Expression) -> Result<(i64, bool)> {
    match expr {
        Expression::Literal(s) => {
            if decode_interval_secs(s).is_some() {
                return Err(TakyonicError::Sql(
                    "generate_series timestamp bound cannot be INTERVAL".into(),
                ));
            }
            let (y, m, d, hh, mm, ss) = parse_timestamp_parts(s).ok_or_else(|| {
                TakyonicError::Sql(format!(
                    "generate_series timestamp bound is not a date/timestamp: `{s}`"
                ))
            })?;
            let date_only = s.trim().len() == 10;
            Ok((timestamp_to_unix(y, m, d, hh, mm, ss), date_only))
        }
        Expression::Cast { expr, .. } => expr_as_timestamp_unix(expr),
        other => Err(TakyonicError::Sql(format!(
            "generate_series timestamp bounds must be date/timestamp literals, got {other:?}"
        ))),
    }
}

fn expr_as_interval_secs(expr: &Expression) -> Result<i64> {
    match expr {
        Expression::Literal(s) => decode_interval_secs(s).ok_or_else(|| {
            TakyonicError::Sql(format!(
                "generate_series step must be an INTERVAL, got `{s}`"
            ))
        }),
        Expression::Cast { expr, .. } => expr_as_interval_secs(expr),
        other => Err(TakyonicError::Sql(format!(
            "generate_series step must be an INTERVAL, got {other:?}"
        ))),
    }
}

/// Materialize `generate_series` rows (integer or timestamp; capped for safety).
pub fn materialize_generate_series(
    start: i64,
    stop: i64,
    step: i64,
    column: &str,
    ordinality_column: Option<&str>,
    as_timestamp: bool,
    date_only: bool,
) -> Result<Vec<crate::schema::Record>> {
    if step == 0 {
        return Err(TakyonicError::Sql(
            "generate_series step must not be zero".into(),
        ));
    }
    const MAX_ROWS: usize = 1_000_000;
    let mut rows = Vec::new();
    let mut n = start;
    while if step > 0 { n <= stop } else { n >= stop } {
        if rows.len() >= MAX_ROWS {
            return Err(TakyonicError::Sql(format!(
                "generate_series exceeds {MAX_ROWS} rows"
            )));
        }
        let cell = if as_timestamp {
            format_unix_timestamp(n, date_only)
        } else {
            n.to_string()
        };
        let mut row = crate::schema::Record::new().set(column, cell);
        if let Some(ord) = ordinality_column {
            row = row.set(ord, (rows.len() + 1).to_string());
        }
        rows.push(row);
        let next = n.saturating_add(step);
        if next == n {
            break;
        }
        n = next;
    }
    Ok(rows)
}

/// Parse `$1` / `$2` → 0-based index.
fn parse_placeholder(raw: &str) -> Result<usize> {
    let trimmed = raw.trim();
    let num = trimmed
        .strip_prefix('$')
        .ok_or_else(|| TakyonicError::Sql(format!("invalid placeholder `{raw}`")))?;
    let one_based: usize = num
        .parse()
        .map_err(|_| TakyonicError::Sql(format!("invalid placeholder `{raw}`")))?;
    if one_based == 0 {
        return Err(TakyonicError::Sql(
            "parameter placeholders are 1-based ($1, $2, …)".into(),
        ));
    }
    Ok(one_based - 1)
}

fn flatten_and_predicates(expr: &Expr) -> Result<Vec<Filter>> {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            let mut out = flatten_and_predicates(left)?;
            out.extend(flatten_and_predicates(right)?);
            Ok(out)
        }
        Expr::Nested(inner) => flatten_and_predicates(inner),
        Expr::BinaryOp { left, op, right } => {
            let filter_op = match op {
                BinaryOperator::Eq => FilterOp::Eq,
                BinaryOperator::NotEq => FilterOp::Ne,
                BinaryOperator::Gt => FilterOp::Gt,
                BinaryOperator::GtEq => FilterOp::Gte,
                BinaryOperator::Lt => FilterOp::Lt,
                BinaryOperator::LtEq => FilterOp::Lte,
                other => {
                    return Err(TakyonicError::Sql(format!(
                        "unsupported WHERE operator: {other}"
                    )));
                }
            };
            // Prefer `col op literal`; also accept `literal op col` by flipping.
            if let (Ok(col), Ok(val)) = (expr_ident(left), expr_literal(right)) {
                return Ok(vec![Filter {
                    column: col,
                    op: filter_op,
                    value: val,
                }]);
            }
            if let (Ok(val), Ok(col)) = (expr_literal(left), expr_ident(right)) {
                let flipped = match filter_op {
                    FilterOp::Eq | FilterOp::Ne => filter_op,
                    FilterOp::Gt => FilterOp::Lt,
                    FilterOp::Gte => FilterOp::Lte,
                    FilterOp::Lt => FilterOp::Gt,
                    FilterOp::Lte => FilterOp::Gte,
                };
                return Ok(vec![Filter {
                    column: col,
                    op: flipped,
                    value: val,
                }]);
            }
            Err(TakyonicError::Sql(format!(
                "unsupported WHERE clause: {expr}"
            )))
        }
        other => Err(TakyonicError::Sql(format!(
            "unsupported WHERE expression: {other}"
        ))),
    }
}

fn table_factor_name(factor: &TableFactor) -> Result<String> {
    match factor {
        TableFactor::Table { name, .. } => object_name_leaf(name),
        other => Err(TakyonicError::Sql(format!(
            "unsupported FROM relation: {other}"
        ))),
    }
}

fn table_object_name(table: &TableObject) -> Result<String> {
    match table {
        TableObject::TableName(name) => object_name_leaf(name),
        other => Err(TakyonicError::Sql(format!(
            "unsupported INSERT target: {other}"
        ))),
    }
}

fn object_name_leaf(name: &ObjectName) -> Result<String> {
    let part = name
        .0
        .last()
        .ok_or_else(|| TakyonicError::Sql("empty object name".into()))?;
    match part {
        ObjectNamePart::Identifier(ident) => Ok(ident.value.clone()),
        ObjectNamePart::Function(_) => Err(TakyonicError::Sql(
            "function object names are unsupported".into(),
        )),
    }
}

fn index_column_name(col: &IndexColumn) -> Result<String> {
    match &col.column.expr {
        Expr::Identifier(ident) => Ok(ident.value.clone()),
        Expr::CompoundIdentifier(parts) => parts
            .last()
            .map(|i| i.value.clone())
            .ok_or_else(|| TakyonicError::Sql("empty PRIMARY KEY column".into())),
        other => Err(TakyonicError::Sql(format!(
            "unsupported PRIMARY KEY column expression: {other}"
        ))),
    }
}

/// Map sqlparser [`DataType`] to a whitespace-free catalog token.
fn canonicalize_sql_type(dt: &DataType) -> String {
    match dt {
        DataType::Int(_) | DataType::Integer(_) | DataType::Int4(_) | DataType::Int32 => {
            "INT".into()
        }
        DataType::SmallInt(_) | DataType::Int2(_) | DataType::Int16 => "SMALLINT".into(),
        DataType::BigInt(_) | DataType::Int8(_) | DataType::Int64 => "BIGINT".into(),
        DataType::Boolean | DataType::Bool => "BOOL".into(),
        DataType::Text | DataType::String(_) => "TEXT".into(),
        DataType::Varchar(_) | DataType::Nvarchar(_) | DataType::Char(_) | DataType::Character(_) => {
            "TEXT".into()
        }
        DataType::Float(_) | DataType::Real | DataType::Float4 => "FLOAT".into(),
        DataType::Double(_) | DataType::Float8 | DataType::DoublePrecision => "DOUBLE".into(),
        other => {
            let raw = other.to_string().to_ascii_uppercase();
            raw.chars()
                .map(|c| if c.is_whitespace() { '_' } else { c })
                .collect()
        }
    }
}

/// Expand `SERIAL` family to base integer types; returns `(canonical, is_serial)`.
fn expand_serial_sql_type(raw: &str) -> (String, bool) {
    match raw.to_ascii_uppercase().as_str() {
        "SERIAL" | "SERIAL4" => ("INT".into(), true),
        "BIGSERIAL" | "SERIAL8" => ("BIGINT".into(), true),
        "SMALLSERIAL" | "SERIAL2" => ("SMALLINT".into(), true),
        other => (other.to_string(), false),
    }
}

/// Default sequence name for a SERIAL column (`{table}_{column}_seq`).
pub fn serial_sequence_name(table: &str, column: &str) -> String {
    format!(
        "{}_{}_seq",
        normalize_sequence_name(table),
        column.trim().trim_matches('"').to_ascii_lowercase()
    )
}

/// Create the backing sequence for a SERIAL column and mark OWNED BY.
pub fn create_serial_sequence(table: &str, column: &str) -> Result<()> {
    let seq = serial_sequence_name(table, column);
    create_sequence(&seq, true, 1, 1)?;
    alter_sequence(
        &seq,
        None,
        None,
        Some(Some((
            normalize_sequence_name(table),
            column.trim().trim_matches('"').to_ascii_lowercase(),
        ))),
        None,
    )?;
    Ok(())
}

/// True when an expression is (or clearly yields) a SQL `ARRAY[…]` value.
fn expr_looks_like_sql_array(expr: &Expression) -> bool {
    match expr {
        Expression::Array(_) => true,
        Expression::ScalarFunction { name, .. }
            if matches!(
                name.as_str(),
                "ARRAY_CAT" | "ARRAY_CONTAINS" | "ARRAY_CONTAINED_BY" | "ARRAY_OVERLAP"
                    | "STRING_TO_ARRAY"
                    | "REGEXP_SPLIT_TO_ARRAY"
            ) =>
        {
            true
        }
        _ => false,
    }
}

/// True when an expression is JSON/JSONB-typed or a JSON helper result.
fn expr_looks_like_json(expr: &Expression) -> bool {
    match expr {
        Expression::Cast {
            target: CastTarget::Json,
            ..
        } => true,
        Expression::ScalarFunction { name, .. }
            if matches!(
                name.as_str(),
                "JSON_GET"
                    | "JSON_GET_TEXT"
                    | "JSON_PATH_GET"
                    | "JSON_PATH_GET_TEXT"
                    | "JSON_CONCAT"
                    | "JSONB_SET"
                    | "JSON_SET"
                    | "JSON_TYPEOF"
                    | "JSONB_TYPEOF"
                    | "JSON_CONTAINS"
                    | "JSON_CONTAINED_BY"
                    | "JSONB_BUILD_OBJECT"
                    | "JSON_BUILD_OBJECT"
                    | "JSONB_BUILD_ARRAY"
                    | "JSON_BUILD_ARRAY"
                    | "JSONB_PRETTY"
                    | "JSON_PRETTY"
                    | "JSON_DELETE"
                    | "JSON_PATH_DELETE"
                    | "JSONB_INSERT"
                    | "JSON_INSERT"
                    | "JSONB_STRIP_NULLS"
                    | "JSON_STRIP_NULLS"
                    | "TO_JSON"
                    | "TO_JSONB"
                    | "ARRAY_TO_JSON"
                    | "ROW_TO_JSON"
                    | "JSON_ARRAY_LENGTH"
                    | "JSONB_ARRAY_LENGTH"
                    | "IS_JSON"
                    | "JSON_IS_VALID"
                    | "JSONB_PATH_EXISTS"
                    | "JSON_PATH_EXISTS"
                    | "JSONB_EXTRACT_PATH"
                    | "JSON_EXTRACT_PATH"
                    | "JSONB_EXTRACT_PATH_TEXT"
                    | "JSON_EXTRACT_PATH_TEXT"
            ) =>
        {
            true
        }
        _ => false,
    }
}

fn cast_target_from_datatype(dt: &DataType) -> Result<CastTarget> {
    match canonicalize_sql_type(dt).as_str() {
        "TEXT" | "VARCHAR" | "CHAR" | "CHARACTER" | "NVARCHAR" => Ok(CastTarget::Text),
        "INT" | "INTEGER" | "SMALLINT" | "BIGINT" => Ok(CastTarget::Int),
        "FLOAT" | "DOUBLE" | "REAL" | "NUMERIC" | "DECIMAL" => Ok(CastTarget::Float),
        "BOOL" | "BOOLEAN" => Ok(CastTarget::Bool),
        "JSON" | "JSONB" => Ok(CastTarget::Json),
        other => Err(TakyonicError::Sql(format!(
            "unsupported CAST target type `{other}` \
             (supported: TEXT/VARCHAR, INT/BIGINT, FLOAT/DOUBLE, BOOL, JSON/JSONB)"
        ))),
    }
}

/// Apply a SQL cast to a runtime [`Value`].
pub fn cast_sql_value(value: &Value, target: CastTarget, try_cast: bool) -> Result<Value> {
    if value.is_null() {
        return Ok(Value::Null);
    }
    let hard = |err: String| -> Result<Value> {
        if try_cast {
            Ok(Value::Null)
        } else {
            Err(TakyonicError::Sql(err))
        }
    };
    match target {
        CastTarget::Text => Ok(Value::String(value.to_display())),
        CastTarget::Int => {
            if let Some(f) = value.as_f64() {
                if f.is_finite() {
                    return Ok(Value::Int(f as i64));
                }
            }
            let s = value.to_display();
            match s.parse::<i64>() {
                Ok(n) => Ok(Value::Int(n)),
                Err(_) => hard(format!("cannot cast `{s}` to INT")),
            }
        }
        CastTarget::Float => {
            if let Some(f) = value.as_f64() {
                return Ok(Value::Float(f));
            }
            let s = value.to_display();
            match s.parse::<f64>() {
                Ok(f) => Ok(Value::Float(f)),
                Err(_) => hard(format!("cannot cast `{s}` to FLOAT")),
            }
        }
        CastTarget::Bool => match value {
            Value::Bool(b) => Ok(Value::Bool(*b)),
            Value::Int(n) => Ok(Value::Bool(*n != 0)),
            Value::Float(f) => Ok(Value::Bool(*f != 0.0)),
            Value::String(s) => match s.to_ascii_lowercase().as_str() {
                "true" | "t" | "1" | "yes" | "y" | "on" => Ok(Value::Bool(true)),
                "false" | "f" | "0" | "no" | "n" | "off" => Ok(Value::Bool(false)),
                other => hard(format!("cannot cast `{other}` to BOOL")),
            },
            Value::Null => Ok(Value::Null),
        },
        CastTarget::Json => {
            let s = value.to_display();
            match serde_json::from_str::<serde_json::Value>(&s) {
                Ok(v) => Ok(Value::String(v.to_string())),
                Err(e) => hard(format!("cannot cast to JSON/JSONB: {e}")),
            }
        }
    }
}

/// `json -> key` / `json ->> key` (key may be text or integer array index).
pub fn json_get(doc: &str, key: &Value, as_text: bool) -> Result<Value> {
    let root: serde_json::Value = serde_json::from_str(doc.trim()).map_err(|e| {
        TakyonicError::Sql(format!("invalid JSON document: {e}"))
    })?;
    let child = match key {
        Value::Int(i) => {
            if *i < 0 {
                None
            } else {
                root.get(*i as usize).cloned()
            }
        }
        Value::Float(f) if f.fract() == 0.0 && *f >= 0.0 => {
            root.get(*f as usize).cloned()
        }
        other => {
            let k = other.to_display();
            if let Ok(i) = k.parse::<usize>() {
                if root.is_array() {
                    root.get(i).cloned()
                } else {
                    root.get(&k).cloned()
                }
            } else {
                root.get(&k).cloned()
            }
        }
    };
    match child {
        None | Some(serde_json::Value::Null) => Ok(Value::Null),
        Some(v) if as_text => match v {
            serde_json::Value::String(s) => Ok(Value::String(s)),
            serde_json::Value::Bool(b) => Ok(Value::String(b.to_string())),
            serde_json::Value::Number(n) => Ok(Value::String(n.to_string())),
            other => Ok(Value::String(other.to_string())),
        },
        Some(v) => Ok(Value::String(v.to_string())),
    }
}

/// `json_typeof` / `jsonb_typeof` type name.
pub fn json_typeof(doc: &str) -> Result<String> {
    let v: serde_json::Value = serde_json::from_str(doc.trim()).map_err(|e| {
        TakyonicError::Sql(format!("invalid JSON document: {e}"))
    })?;
    Ok(match v {
        serde_json::Value::Null => "null".into(),
        serde_json::Value::Bool(_) => "boolean".into(),
        serde_json::Value::Number(_) => "number".into(),
        serde_json::Value::String(_) => "string".into(),
        serde_json::Value::Array(_) => "array".into(),
        serde_json::Value::Object(_) => "object".into(),
    })
}

/// Parse a Postgres text path `'{a,b,0}'` into path segments.
pub fn parse_json_text_path(path: &str) -> Result<Vec<String>> {
    let p = path.trim();
    let inner = p
        .strip_prefix('{')
        .and_then(|s| s.strip_suffix('}'))
        .ok_or_else(|| {
            TakyonicError::Sql(format!(
                "JSON path must look like '{{a,b}}', got `{path}`"
            ))
        })?;
    if inner.is_empty() {
        return Ok(Vec::new());
    }
    Ok(inner
        .split(',')
        .map(|s| s.trim().trim_matches('"').to_string())
        .filter(|s| !s.is_empty())
        .collect())
}

/// `json #> path` / `json #>> path`.
pub fn json_path_get(doc: &str, path: &str, as_text: bool) -> Result<Value> {
    let mut cur: serde_json::Value = serde_json::from_str(doc.trim()).map_err(|e| {
        TakyonicError::Sql(format!("invalid JSON document: {e}"))
    })?;
    for seg in parse_json_text_path(path)? {
        cur = if let Ok(i) = seg.parse::<usize>() {
            match cur.get(i) {
                Some(v) => v.clone(),
                None => return Ok(Value::Null),
            }
        } else {
            match cur.get(&seg) {
                Some(v) => v.clone(),
                None => return Ok(Value::Null),
            }
        };
    }
    match cur {
        serde_json::Value::Null => Ok(Value::Null),
        v if as_text => match v {
            serde_json::Value::String(s) => Ok(Value::String(s)),
            serde_json::Value::Bool(b) => Ok(Value::String(b.to_string())),
            serde_json::Value::Number(n) => Ok(Value::String(n.to_string())),
            other => Ok(Value::String(other.to_string())),
        },
        v => Ok(Value::String(v.to_string())),
    }
}

/// `jsonb_path_exists(doc, '{a,b}')` — true when the text path resolves.
pub fn json_path_exists(doc: &str, path: &str) -> Result<bool> {
    let mut cur: serde_json::Value = serde_json::from_str(doc.trim()).map_err(|e| {
        TakyonicError::Sql(format!("invalid JSON document: {e}"))
    })?;
    for seg in parse_json_text_path(path)? {
        let next = if let Ok(i) = seg.parse::<usize>() {
            cur.get(i).cloned()
        } else {
            cur.get(&seg).cloned()
        };
        match next {
            Some(v) => cur = v,
            None => return Ok(false),
        }
    }
    Ok(true)
}

/// `jsonb_extract_path(doc, 'a', 'b')` / `_text` — walk path elements.
pub fn json_extract_path(doc: &str, segments: &[String], as_text: bool) -> Result<Value> {
    let path = format!("{{{}}}", segments.join(","));
    json_path_get(doc, &path, as_text)
}

/// Postgres-ish JSONB containment (`@>`): `haystack` contains `needle`.
pub fn json_contains(haystack: &str, needle: &str) -> Result<bool> {
    let hay: serde_json::Value = serde_json::from_str(haystack.trim()).map_err(|e| {
        TakyonicError::Sql(format!("invalid JSON document: {e}"))
    })?;
    let need: serde_json::Value = serde_json::from_str(needle.trim()).map_err(|e| {
        TakyonicError::Sql(format!("invalid JSON document: {e}"))
    })?;
    Ok(json_value_contains(&hay, &need))
}

fn json_value_contains(hay: &serde_json::Value, need: &serde_json::Value) -> bool {
    match (hay, need) {
        (_, serde_json::Value::Null) => hay.is_null(),
        (serde_json::Value::Object(h), serde_json::Value::Object(n)) => n.iter().all(|(k, nv)| {
            h.get(k)
                .map(|hv| json_value_contains(hv, nv))
                .unwrap_or(false)
        }),
        (serde_json::Value::Array(h), serde_json::Value::Array(n)) => n
            .iter()
            .all(|nv| h.iter().any(|hv| json_value_contains(hv, nv))),
        (serde_json::Value::Array(h), other) => h.iter().any(|hv| json_value_contains(hv, other)),
        (a, b) => a == b,
    }
}

/// Merge two JSON values (`||`): objects deep-merge shallowly (right wins keys);
/// arrays concatenate; otherwise right replaces left.
pub fn json_concat(left: &str, right: &str) -> Result<String> {
    let mut l: serde_json::Value = serde_json::from_str(left.trim()).map_err(|e| {
        TakyonicError::Sql(format!("invalid JSON document: {e}"))
    })?;
    let r: serde_json::Value = serde_json::from_str(right.trim()).map_err(|e| {
        TakyonicError::Sql(format!("invalid JSON document: {e}"))
    })?;
    match (&mut l, r) {
        (serde_json::Value::Object(lo), serde_json::Value::Object(ro)) => {
            for (k, v) in ro {
                lo.insert(k, v);
            }
        }
        (serde_json::Value::Array(la), serde_json::Value::Array(ra)) => {
            la.extend(ra);
        }
        (_, r) => l = r,
    }
    Ok(l.to_string())
}

/// Convert a SQL [`Value`] into a JSON value for `jsonb_build_*`.
///
/// Strings that already parse as JSON keep their structure (so `::jsonb`
/// arguments nest correctly); other strings become JSON strings.
pub fn value_to_json(v: &Value) -> serde_json::Value {
    if v.is_null() {
        return serde_json::Value::Null;
    }
    match v {
        Value::Null => serde_json::Value::Null,
        Value::Int(n) => serde_json::json!(*n),
        Value::Float(f) => serde_json::json!(*f),
        Value::Bool(b) => serde_json::json!(*b),
        Value::String(s) => match serde_json::from_str::<serde_json::Value>(s.trim()) {
            Ok(j) => j,
            Err(_) => serde_json::Value::String(s.clone()),
        },
    }
}

/// `json_array_length` / `jsonb_array_length` — length of a JSON array.
pub fn json_array_length(doc: &str) -> Result<i64> {
    let v: serde_json::Value = serde_json::from_str(doc.trim()).map_err(|e| {
        TakyonicError::Sql(format!("invalid JSON document: {e}"))
    })?;
    match v {
        serde_json::Value::Array(arr) => Ok(arr.len() as i64),
        _ => Err(TakyonicError::Sql(
            "json_array_length requires a JSON array".into(),
        )),
    }
}

/// `is_json` / `json_is_valid` — true when `s` parses as JSON (PG `IS JSON` stand-in).
pub fn is_json(s: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(s.trim()).is_ok()
}

/// `string_to_array(str, delimiter [, null_string])`.
pub fn string_to_array(s: &str, delim: &str, null_string: Option<&str>) -> String {
    if delim.is_empty() {
        // PG: empty delimiter → one-element array of the whole string.
        let elem = match null_string {
            Some(n) if s == n => String::new(),
            _ => s.to_string(),
        };
        return format!("[{elem}]");
    }
    let parts: Vec<String> = s
        .split(delim)
        .map(|p| match null_string {
            Some(n) if p == n => String::new(),
            _ => p.to_string(),
        })
        .collect();
    format!("[{}]", parts.join(","))
}

/// `split_part(string, delimiter, field)` — 1-based field; missing → empty string.
pub fn split_part(s: &str, delim: &str, field: i64) -> Result<String> {
    if field == 0 {
        return Err(TakyonicError::Sql(
            "split_part field must not be zero".into(),
        ));
    }
    let parts: Vec<&str> = if delim.is_empty() {
        vec![s]
    } else {
        s.split(delim).collect()
    };
    if field > 0 {
        Ok(parts
            .get((field as usize) - 1)
            .copied()
            .unwrap_or("")
            .to_string())
    } else {
        // Negative: count from the end (PG 14+).
        let idx = parts.len() as i64 + field;
        if idx < 0 {
            Ok(String::new())
        } else {
            Ok(parts
                .get(idx as usize)
                .copied()
                .unwrap_or("")
                .to_string())
        }
    }
}

/// Compile a PG-ish `regexp_*` pattern with optional flags (`i` = case-insensitive).
/// Returns `(regex, global)` where `global` is true when flag `g` is present.
fn compile_sql_regex(pattern: &str, flags: Option<&str>) -> Result<(regex::Regex, bool)> {
    let mut builder = regex::RegexBuilder::new(pattern);
    let mut global = false;
    if let Some(f) = flags {
        for c in f.chars() {
            match c {
                'i' | 'I' => {
                    builder.case_insensitive(true);
                }
                'c' | 'C' => {
                    builder.case_insensitive(false);
                }
                'g' | 'G' => {
                    global = true;
                }
                // Ignore other POSIX-ish letters for now.
                'n' | 'N' | 'x' | 'X' | 'm' | 'M' | 's' | 'S' | 'p' | 'P' | 'w' | 'W' | 'q'
                | 'Q' => {}
                other => {
                    return Err(TakyonicError::Sql(format!(
                        "unsupported regexp flag `{other}`"
                    )));
                }
            }
        }
    }
    let re = builder
        .build()
        .map_err(|e| TakyonicError::Sql(format!("invalid regular expression: {e}")))?;
    Ok((re, global))
}

/// Map PG-style `\1` backrefs to Rust `$1` in replacement templates.
fn normalize_regexp_replacement(repl: &str) -> String {
    let mut out = String::with_capacity(repl.len());
    let bytes = repl.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'\\' && i + 1 < bytes.len() && bytes[i + 1].is_ascii_digit() {
            out.push('$');
            out.push(bytes[i + 1] as char);
            i += 2;
        } else {
            out.push(bytes[i] as char);
            i += 1;
        }
    }
    out
}

/// `regexp_replace(string, pattern, replacement [, flags])`.
/// Without `g`, only the first match is replaced (PG default).
pub fn regexp_replace(
    s: &str,
    pattern: &str,
    replacement: &str,
    flags: Option<&str>,
) -> Result<String> {
    let (re, global) = compile_sql_regex(pattern, flags)?;
    let repl = normalize_regexp_replacement(replacement);
    if global {
        Ok(re.replace_all(s, repl.as_str()).into_owned())
    } else {
        Ok(re.replace(s, repl.as_str()).into_owned())
    }
}

/// `regexp_like(string, pattern [, flags])` — true if pattern matches.
pub fn regexp_like(s: &str, pattern: &str, flags: Option<&str>) -> Result<bool> {
    let (re, _) = compile_sql_regex(pattern, flags)?;
    Ok(re.is_match(s))
}

/// Capture groups (or whole match) for each regexp match, as display arrays.
/// Without `g`, at most one match row; with `g`, all non-overlapping matches.
pub fn regexp_match_rows(
    s: &str,
    pattern: &str,
    flags: Option<&str>,
) -> Result<Vec<String>> {
    let (re, global) = compile_sql_regex(pattern, flags)?;
    let mut out = Vec::new();
    for caps in re.captures_iter(s) {
        let parts: Vec<String> = if caps.len() <= 1 {
            vec![caps
                .get(0)
                .map(|m| m.as_str().to_string())
                .unwrap_or_default()]
        } else {
            (1..caps.len())
                .map(|i| {
                    caps.get(i)
                        .map(|m| m.as_str().to_string())
                        .unwrap_or_default()
                })
                .collect()
        };
        out.push(format!("[{}]", parts.join(",")));
        if !global {
            break;
        }
    }
    Ok(out)
}

/// Materialize `regexp_matches` into rows (text[] display per match).
pub fn materialize_regexp_matches(
    string: &str,
    pattern: &str,
    flags: Option<&str>,
    column: &str,
    ordinality_column: Option<&str>,
) -> Result<Vec<crate::schema::Record>> {
    let rows = regexp_match_rows(string, pattern, flags)?;
    Ok(rows
        .into_iter()
        .enumerate()
        .map(|(i, p)| {
            let mut row = crate::schema::Record::new().set(column, p);
            if let Some(ord) = ordinality_column {
                row = row.set(ord, (i + 1).to_string());
            }
            row
        })
        .collect())
}

/// `lpad(string, length [, fill])` — left-pad (or truncate) to `length` chars.
pub fn lpad(s: &str, length: i64, fill: &str) -> Result<String> {
    pad_string(s, length, fill, true)
}

/// `rpad(string, length [, fill])` — right-pad (or truncate) to `length` chars.
pub fn rpad(s: &str, length: i64, fill: &str) -> Result<String> {
    pad_string(s, length, fill, false)
}

fn pad_string(s: &str, length: i64, fill: &str, left: bool) -> Result<String> {
    if length < 0 {
        return Err(TakyonicError::Sql(
            "lpad/rpad length must not be negative".into(),
        ));
    }
    let len = length as usize;
    let chars: Vec<char> = s.chars().collect();
    if chars.len() >= len {
        return Ok(chars.into_iter().take(len).collect());
    }
    if fill.is_empty() {
        return Ok(chars.into_iter().collect());
    }
    let fill_chars: Vec<char> = fill.chars().collect();
    let need = len - chars.len();
    let mut pad = String::with_capacity(need);
    while pad.chars().count() < need {
        for c in &fill_chars {
            if pad.chars().count() >= need {
                break;
            }
            pad.push(*c);
        }
    }
    if left {
        Ok(format!("{pad}{s}"))
    } else {
        Ok(format!("{s}{pad}"))
    }
}

/// `repeat(string, count)` — concatenate `string` `count` times.
pub fn repeat(s: &str, count: i64) -> Result<String> {
    if count < 0 {
        return Err(TakyonicError::Sql(
            "repeat count must not be negative".into(),
        ));
    }
    if count == 0 || s.is_empty() {
        return Ok(String::new());
    }
    // Cap to avoid pathological allocations in tests/queries.
    const MAX_CHARS: usize = 1_000_000;
    let total = s.chars().count().saturating_mul(count as usize);
    if total > MAX_CHARS {
        return Err(TakyonicError::Sql(
            "repeat result exceeds size limit".into(),
        ));
    }
    Ok(s.repeat(count as usize))
}

/// `left(string, n)` — first `n` characters (`n <= 0` → empty).
pub fn left(s: &str, n: i64) -> String {
    if n <= 0 {
        return String::new();
    }
    s.chars().take(n as usize).collect()
}

/// `right(string, n)` — last `n` characters (`n <= 0` → empty).
pub fn right(s: &str, n: i64) -> String {
    if n <= 0 {
        return String::new();
    }
    let chars: Vec<char> = s.chars().collect();
    let take = (n as usize).min(chars.len());
    chars[chars.len() - take..].iter().collect()
}

/// `reverse(string)` — reverse character order.
pub fn reverse(s: &str) -> String {
    s.chars().rev().collect()
}

/// `initcap(string)` — uppercase first letter of each alphanumeric word.
pub fn initcap(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut at_word_start = true;
    for c in s.chars() {
        if c.is_alphanumeric() {
            if at_word_start {
                for u in c.to_uppercase() {
                    out.push(u);
                }
                at_word_start = false;
            } else {
                for l in c.to_lowercase() {
                    out.push(l);
                }
            }
        } else {
            out.push(c);
            at_word_start = true;
        }
    }
    out
}

/// `ascii(string)` — Unicode code point of the first character (empty → 0).
pub fn ascii(s: &str) -> i64 {
    s.chars().next().map(|c| c as u32 as i64).unwrap_or(0)
}

/// `chr(n)` — character for Unicode code point `n`.
pub fn chr(n: i64) -> Result<String> {
    if n < 0 {
        return Err(TakyonicError::Sql(
            "chr argument must not be negative".into(),
        ));
    }
    if n > u32::MAX as i64 {
        return Err(TakyonicError::Sql(
            "chr argument out of Unicode range".into(),
        ));
    }
    char::from_u32(n as u32)
        .map(|c| c.to_string())
        .ok_or_else(|| TakyonicError::Sql(format!("chr: invalid code point {n}")))
}

/// `md5(string)` — lowercase hex digest of UTF-8 bytes.
pub fn md5_hex(s: &str) -> String {
    use md5::{Digest, Md5};
    let digest = Md5::digest(s.as_bytes());
    bytes_to_hex(&digest)
}

fn bytes_to_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0xf) as usize] as char);
    }
    out
}

fn hex_to_bytes(s: &str) -> Result<Vec<u8>> {
    let s = s.trim();
    if s.len() % 2 != 0 {
        return Err(TakyonicError::Sql(
            "decode hex input must have even length".into(),
        ));
    }
    let mut out = Vec::with_capacity(s.len() / 2);
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let hi = hex_nibble(bytes[i])?;
        let lo = hex_nibble(bytes[i + 1])?;
        out.push((hi << 4) | lo);
        i += 2;
    }
    Ok(out)
}

fn hex_nibble(b: u8) -> Result<u8> {
    match b {
        b'0'..=b'9' => Ok(b - b'0'),
        b'a'..=b'f' => Ok(b - b'a' + 10),
        b'A'..=b'F' => Ok(b - b'A' + 10),
        _ => Err(TakyonicError::Sql(format!(
            "invalid hex digit `{}`",
            b as char
        ))),
    }
}

/// `encode(data, format)` — `hex` or `base64` (data as UTF-8 bytes).
pub fn encode_bytes(data: &str, format: &str) -> Result<String> {
    match format.to_ascii_lowercase().as_str() {
        "hex" => Ok(bytes_to_hex(data.as_bytes())),
        "base64" => Ok(base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            data.as_bytes(),
        )),
        other => Err(TakyonicError::Sql(format!(
            "unsupported encode format `{other}` (hex, base64)"
        ))),
    }
}

/// `decode(string, format)` — decode `hex`/`base64` to UTF-8 text (lossy on invalid UTF-8).
pub fn decode_bytes(data: &str, format: &str) -> Result<String> {
    let raw = match format.to_ascii_lowercase().as_str() {
        "hex" => hex_to_bytes(data)?,
        "base64" => base64::Engine::decode(
            &base64::engine::general_purpose::STANDARD,
            data.trim(),
        )
        .map_err(|e| TakyonicError::Sql(format!("invalid base64: {e}")))?,
        other => {
            return Err(TakyonicError::Sql(format!(
                "unsupported decode format `{other}` (hex, base64)"
            )));
        }
    };
    Ok(String::from_utf8_lossy(&raw).into_owned())
}

/// `starts_with(string, prefix)` — true if `string` begins with `prefix`.
pub fn starts_with(s: &str, prefix: &str) -> bool {
    s.starts_with(prefix)
}

/// `ends_with(string, suffix)` — true if `string` ends with `suffix`.
pub fn ends_with(s: &str, suffix: &str) -> bool {
    s.ends_with(suffix)
}

/// `overlay(string placing replacement from start [for count])` — 1-based.
/// When `for_count` is omitted, replace `replacement.len()` characters.
pub fn overlay(s: &str, replacement: &str, from: i64, for_count: Option<i64>) -> Result<String> {
    if from <= 0 {
        return Err(TakyonicError::Sql(
            "overlay FROM position must be >= 1".into(),
        ));
    }
    let chars: Vec<char> = s.chars().collect();
    let start = ((from as usize) - 1).min(chars.len());
    let remove = match for_count {
        Some(n) if n < 0 => {
            return Err(TakyonicError::Sql(
                "overlay FOR count must not be negative".into(),
            ));
        }
        Some(n) => n as usize,
        None => replacement.chars().count(),
    };
    let end = (start + remove).min(chars.len());
    let mut out = String::with_capacity(s.len() + replacement.len());
    out.extend(chars[..start].iter());
    out.push_str(replacement);
    out.extend(chars[end..].iter());
    Ok(out)
}

/// `translate(string, from, to)` — map/delete characters by parallel sets.
pub fn translate(s: &str, from: &str, to: &str) -> String {
    let from_chars: Vec<char> = from.chars().collect();
    let to_chars: Vec<char> = to.chars().collect();
    s.chars()
        .filter_map(|c| {
            if let Some(i) = from_chars.iter().position(|&f| f == c) {
                to_chars.get(i).copied()
            } else {
                Some(c)
            }
        })
        .collect()
}

fn trim_chars_set(characters: Option<&str>) -> Vec<char> {
    characters
        .unwrap_or(" ")
        .chars()
        .collect()
}

fn char_in_set(c: char, set: &[char]) -> bool {
    set.contains(&c)
}

/// `btrim(string [, characters])` — trim both ends (default: space).
pub fn btrim(s: &str, characters: Option<&str>) -> String {
    let set = trim_chars_set(characters);
    let chars: Vec<char> = s.chars().collect();
    let start = chars.iter().position(|c| !char_in_set(*c, &set)).unwrap_or(chars.len());
    let end = chars
        .iter()
        .rposition(|c| !char_in_set(*c, &set))
        .map(|i| i + 1)
        .unwrap_or(start);
    chars[start..end.max(start)].iter().collect()
}

/// `ltrim(string [, characters])` — trim start (default: space).
pub fn ltrim(s: &str, characters: Option<&str>) -> String {
    let set = trim_chars_set(characters);
    s.chars()
        .skip_while(|c| char_in_set(*c, &set))
        .collect()
}

/// `rtrim(string [, characters])` — trim end (default: space).
pub fn rtrim(s: &str, characters: Option<&str>) -> String {
    let set = trim_chars_set(characters);
    let chars: Vec<char> = s.chars().collect();
    let end = chars
        .iter()
        .rposition(|c| !char_in_set(*c, &set))
        .map(|i| i + 1)
        .unwrap_or(0);
    chars[..end].iter().collect()
}

/// `concat_ws(sep, …)` — join non-NULL values with `sep` (NULLs skipped).
pub fn concat_ws(sep: &str, parts: &[Value]) -> String {
    let mut out = String::new();
    let mut first = true;
    for v in parts {
        if v.is_null() {
            continue;
        }
        if !first {
            out.push_str(sep);
        }
        out.push_str(&v.to_display());
        first = false;
    }
    out
}

/// `quote_ident(string)` — double-quote an identifier (`"` → `""`).
pub fn quote_ident(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        if c == '"' {
            out.push('"');
        }
        out.push(c);
    }
    out.push('"');
    out
}

/// `quote_literal(string)` — single-quote a SQL string literal (`'` → `''`).
pub fn quote_literal(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for c in s.chars() {
        if c == '\'' {
            out.push('\'');
        }
        out.push(c);
    }
    out.push('\'');
    out
}

/// `quote_nullable(value)` — `NULL` text for nulls, else [`quote_literal`].
pub fn quote_nullable(v: &Value) -> String {
    if v.is_null() {
        "NULL".into()
    } else {
        quote_literal(&v.to_display())
    }
}

/// `width_bucket(operand, low, high, count)` — histogram bucket index (PG numeric form).
pub fn width_bucket(operand: f64, low: f64, high: f64, count: i64) -> Result<i64> {
    if count <= 0 {
        return Err(TakyonicError::Sql(
            "width_bucket count must be greater than zero".into(),
        ));
    }
    if low == high {
        return Err(TakyonicError::Sql(
            "width_bucket low and high bounds must differ".into(),
        ));
    }
    // PG allows reversed bounds: buckets counted from high toward low.
    let (b1, b2, reverse) = if low < high {
        (low, high, false)
    } else {
        (high, low, true)
    };
    let bucket = if operand < b1 {
        0
    } else if operand >= b2 {
        count + 1
    } else {
        let idx = ((operand - b1) / (b2 - b1) * count as f64).floor() as i64 + 1;
        idx.clamp(1, count)
    };
    Ok(if reverse { count + 1 - bucket } else { bucket })
}

/// `sign(x)` — -1, 0, or 1.
pub fn sign(f: f64) -> f64 {
    if f > 0.0 {
        1.0
    } else if f < 0.0 {
        -1.0
    } else {
        0.0
    }
}

/// `trunc(x [, digits])` — truncate toward zero (optional decimal places).
pub fn trunc_num(f: f64, digits: i64) -> f64 {
    if digits == 0 {
        f.trunc()
    } else if digits > 0 {
        let p = 10f64.powi(digits as i32);
        (f * p).trunc() / p
    } else {
        let p = 10f64.powi((-digits) as i32);
        (f / p).trunc() * p
    }
}

/// `div(y, x)` — integer division truncated toward zero.
pub fn div_int(y: f64, x: f64) -> Result<i64> {
    if x == 0.0 {
        return Err(TakyonicError::Sql("division by zero".into()));
    }
    Ok((y / x).trunc() as i64)
}

/// `log(x)` base-10, or `log(b, x)` log base `b` of `x`.
pub fn log_num(args: &[f64]) -> Result<f64> {
    match args {
        [x] => {
            if *x <= 0.0 {
                return Err(TakyonicError::Sql(
                    "cannot take logarithm of a non-positive number".into(),
                ));
            }
            Ok(x.log10())
        }
        [b, x] => {
            if *x <= 0.0 || *b <= 0.0 || *b == 1.0 {
                return Err(TakyonicError::Sql(
                    "invalid logarithm arguments".into(),
                ));
            }
            Ok(x.log(*b))
        }
        _ => Err(TakyonicError::Sql(
            "LOG requires one or two numeric arguments".into(),
        )),
    }
}

/// `format(fmt, …)` — subset of PG format: `%s`, `%I`, `%L`, `%%`.
pub fn format_sql(fmt: &str, args: &[Value]) -> Result<String> {
    let mut out = String::with_capacity(fmt.len());
    let mut arg_i = 0;
    let chars: Vec<char> = fmt.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        if chars[i] != '%' {
            out.push(chars[i]);
            i += 1;
            continue;
        }
        i += 1;
        if i >= chars.len() {
            return Err(TakyonicError::Sql(
                "format: trailing % without conversion".into(),
            ));
        }
        match chars[i] {
            '%' => {
                out.push('%');
                i += 1;
            }
            's' | 'I' | 'L' => {
                let conv = chars[i];
                i += 1;
                if arg_i >= args.len() {
                    return Err(TakyonicError::Sql(
                        "format: too few arguments".into(),
                    ));
                }
                let v = &args[arg_i];
                arg_i += 1;
                match conv {
                    's' => {
                        if !v.is_null() {
                            out.push_str(&v.to_display());
                        }
                    }
                    'I' => {
                        if v.is_null() {
                            return Err(TakyonicError::Sql(
                                "format: null values cannot be formatted as an identifier".into(),
                            ));
                        }
                        out.push_str(&quote_ident(&v.to_display()));
                    }
                    'L' => {
                        if v.is_null() {
                            out.push_str("NULL");
                        } else {
                            out.push_str(&quote_literal(&v.to_display()));
                        }
                    }
                    _ => unreachable!(),
                }
            }
            other => {
                return Err(TakyonicError::Sql(format!(
                    "format: unsupported conversion `%{other}` (use %s, %I, %L, %%)"
                )));
            }
        }
    }
    Ok(out)
}

/// `regexp_split_to_array(string, pattern [, flags])` → display form `[a,b,c]`.
pub fn regexp_split_to_array(s: &str, pattern: &str, flags: Option<&str>) -> Result<String> {
    let parts = regexp_split_parts(s, pattern, flags)?;
    Ok(format!("[{}]", parts.join(",")))
}

/// Split `s` on `pattern` into owned parts (shared by array + table forms).
pub fn regexp_split_parts(s: &str, pattern: &str, flags: Option<&str>) -> Result<Vec<String>> {
    let (re, _) = compile_sql_regex(pattern, flags)?;
    Ok(re.split(s).map(|p| p.to_string()).collect())
}

/// Materialize `regexp_split_to_table` into rows.
pub fn materialize_regexp_split_to_table(
    string: &str,
    pattern: &str,
    flags: Option<&str>,
    column: &str,
    ordinality_column: Option<&str>,
) -> Result<Vec<crate::schema::Record>> {
    let parts = regexp_split_parts(string, pattern, flags)?;
    Ok(parts
        .into_iter()
        .enumerate()
        .map(|(i, p)| {
            let mut row = crate::schema::Record::new().set(column, p);
            if let Some(ord) = ordinality_column {
                row = row.set(ord, (i + 1).to_string());
            }
            row
        })
        .collect())
}

/// `array_to_string(array, delimiter [, null_string])`.
pub fn array_to_string(elems: &[Value], delim: &str, null_string: Option<&str>) -> String {
    let mut out = Vec::with_capacity(elems.len());
    for v in elems {
        if v.is_null() {
            if let Some(n) = null_string {
                out.push(n.to_string());
            }
            // else skip NULLs (PG default)
        } else {
            out.push(v.to_display());
        }
    }
    out.join(delim)
}

/// `to_json` / `to_jsonb` / `array_to_json` — serialize a SQL value as JSON text.
pub fn to_json(v: &Value) -> String {
    value_to_json(v).to_string()
}

/// Build a JSON object from `(key, value)` pairs (used by `row_to_json`).
pub fn row_to_json_object(fields: &[(String, Value)]) -> String {
    let mut map = serde_json::Map::new();
    for (k, v) in fields {
        map.insert(k.clone(), value_to_json(v));
    }
    serde_json::Value::Object(map).to_string()
}

/// `jsonb_build_object(k1, v1, …)` — even-length key/value list.
pub fn jsonb_build_object(pairs: &[(Value, Value)]) -> Result<String> {
    let mut map = serde_json::Map::new();
    for (key, val) in pairs {
        if key.is_null() {
            return Err(TakyonicError::Sql(
                "jsonb_build_object key arguments must not be NULL".into(),
            ));
        }
        map.insert(key.to_display(), value_to_json(val));
    }
    Ok(serde_json::Value::Object(map).to_string())
}

/// `jsonb_build_array(v1, …)` — arbitrary arity (including empty → `[]`).
pub fn jsonb_build_array(elems: &[Value]) -> String {
    let arr: Vec<serde_json::Value> = elems.iter().map(value_to_json).collect();
    serde_json::Value::Array(arr).to_string()
}

/// `jsonb_pretty(doc)` — indented JSON text.
pub fn jsonb_pretty(doc: &str) -> Result<String> {
    let v: serde_json::Value = serde_json::from_str(doc.trim()).map_err(|e| {
        TakyonicError::Sql(format!("invalid JSON document: {e}"))
    })?;
    serde_json::to_string_pretty(&v).map_err(|e| {
        TakyonicError::Sql(format!("jsonb_pretty failed: {e}"))
    })
}

/// `jsonb - key` / `jsonb - index` — delete object key or array element.
pub fn json_delete(doc: &str, key: &Value) -> Result<String> {
    let mut root: serde_json::Value = serde_json::from_str(doc.trim()).map_err(|e| {
        TakyonicError::Sql(format!("invalid JSON document: {e}"))
    })?;
    match &mut root {
        serde_json::Value::Object(map) => {
            // Multi-key: ARRAY['a','b'] serializes as `{a,b}`-ish text or JSON array.
            let display = key.to_display();
            if let Ok(serde_json::Value::Array(keys)) =
                serde_json::from_str::<serde_json::Value>(display.trim())
            {
                for k in keys {
                    if let Some(s) = k.as_str() {
                        map.remove(s);
                    }
                }
            } else if display.starts_with('{') && display.ends_with('}') {
                for seg in parse_json_text_path(&display)? {
                    map.remove(&seg);
                }
            } else {
                map.remove(&display);
            }
        }
        serde_json::Value::Array(arr) => {
            let idx = match key {
                Value::Int(i) => *i,
                Value::Float(f) if f.fract() == 0.0 => *f as i64,
                other => other
                    .to_display()
                    .parse::<i64>()
                    .map_err(|_| TakyonicError::Sql(
                        "json array `-` requires an integer index".into(),
                    ))?,
            };
            let len = arr.len() as i64;
            let pos = if idx < 0 { len + idx } else { idx };
            if pos >= 0 && (pos as usize) < arr.len() {
                arr.remove(pos as usize);
            }
        }
        _ => {}
    }
    Ok(root.to_string())
}

/// `jsonb #- '{a,b}'` — delete at path.
pub fn json_path_delete(doc: &str, path: &str) -> Result<String> {
    let mut root: serde_json::Value = serde_json::from_str(doc.trim()).map_err(|e| {
        TakyonicError::Sql(format!("invalid JSON document: {e}"))
    })?;
    let segs = parse_json_text_path(path)?;
    if segs.is_empty() {
        return Ok(root.to_string());
    }
    json_delete_at(&mut root, &segs);
    Ok(root.to_string())
}

fn json_delete_at(node: &mut serde_json::Value, segs: &[String]) -> bool {
    if segs.is_empty() {
        return false;
    }
    let head = &segs[0];
    let rest = &segs[1..];
    if rest.is_empty() {
        return match node {
            serde_json::Value::Object(map) => map.remove(head).is_some(),
            serde_json::Value::Array(arr) => {
                if let Ok(i) = head.parse::<usize>() {
                    if i < arr.len() {
                        arr.remove(i);
                        return true;
                    }
                }
                false
            }
            _ => false,
        };
    }
    match node {
        serde_json::Value::Object(map) => map
            .get_mut(head)
            .map(|child| json_delete_at(child, rest))
            .unwrap_or(false),
        serde_json::Value::Array(arr) => {
            let Ok(i) = head.parse::<usize>() else {
                return false;
            };
            if i >= arr.len() {
                return false;
            }
            json_delete_at(&mut arr[i], rest)
        }
        _ => false,
    }
}

/// `jsonb_set(target, path, new_value [, create_missing])`.
pub fn jsonb_set(
    target: &str,
    path: &str,
    new_value: &str,
    create_missing: bool,
) -> Result<String> {
    let mut root: serde_json::Value = serde_json::from_str(target.trim()).map_err(|e| {
        TakyonicError::Sql(format!("invalid JSON document: {e}"))
    })?;
    let new_v: serde_json::Value = serde_json::from_str(new_value.trim()).map_err(|e| {
        TakyonicError::Sql(format!("invalid JSON new_value: {e}"))
    })?;
    let segs = parse_json_text_path(path)?;
    if segs.is_empty() {
        return Ok(new_v.to_string());
    }
    if !json_set_at(&mut root, &segs, new_v, create_missing) {
        // Path missing and create_missing=false → return original unchanged.
    }
    Ok(root.to_string())
}

/// `jsonb_insert(target, path, new_value [, insert_after])`.
///
/// Inserts into an object (new key) or array (at index). Existing object keys
/// are left unchanged (Postgres raises; we no-op for friendlier SQL polish).
pub fn jsonb_insert(
    target: &str,
    path: &str,
    new_value: &str,
    insert_after: bool,
) -> Result<String> {
    let mut root: serde_json::Value = serde_json::from_str(target.trim()).map_err(|e| {
        TakyonicError::Sql(format!("invalid JSON document: {e}"))
    })?;
    let new_v: serde_json::Value = serde_json::from_str(new_value.trim()).map_err(|e| {
        TakyonicError::Sql(format!("invalid JSON new_value: {e}"))
    })?;
    let segs = parse_json_text_path(path)?;
    if segs.is_empty() {
        return Err(TakyonicError::Sql(
            "jsonb_insert path must not be empty".into(),
        ));
    }
    json_insert_at(&mut root, &segs, new_v, insert_after)?;
    Ok(root.to_string())
}

fn json_insert_at(
    node: &mut serde_json::Value,
    segs: &[String],
    new_v: serde_json::Value,
    insert_after: bool,
) -> Result<()> {
    let head = &segs[0];
    let rest = &segs[1..];
    if rest.is_empty() {
        match node {
            serde_json::Value::Object(map) => {
                if !map.contains_key(head) {
                    map.insert(head.clone(), new_v);
                }
                Ok(())
            }
            serde_json::Value::Array(arr) => {
                let Ok(i) = head.parse::<usize>() else {
                    return Err(TakyonicError::Sql(
                        "jsonb_insert into array requires integer path segment".into(),
                    ));
                };
                let mut at = if insert_after { i.saturating_add(1) } else { i };
                if at > arr.len() {
                    at = arr.len();
                }
                arr.insert(at, new_v);
                Ok(())
            }
            _ => Err(TakyonicError::Sql(
                "jsonb_insert path parent must be object or array".into(),
            )),
        }
    } else {
        match node {
            serde_json::Value::Object(map) => {
                let child = map.get_mut(head).ok_or_else(|| {
                    TakyonicError::Sql(format!(
                        "jsonb_insert path element `{head}` does not exist"
                    ))
                })?;
                json_insert_at(child, rest, new_v, insert_after)
            }
            serde_json::Value::Array(arr) => {
                let i = head.parse::<usize>().map_err(|_| {
                    TakyonicError::Sql(
                        "jsonb_insert into array requires integer path segment".into(),
                    )
                })?;
                let child = arr.get_mut(i).ok_or_else(|| {
                    TakyonicError::Sql(format!(
                        "jsonb_insert path index `{i}` out of bounds"
                    ))
                })?;
                json_insert_at(child, rest, new_v, insert_after)
            }
            _ => Err(TakyonicError::Sql(
                "jsonb_insert path parent must be object or array".into(),
            )),
        }
    }
}

/// `jsonb_strip_nulls(doc)` — drop object fields whose value is JSON null.
pub fn jsonb_strip_nulls(doc: &str) -> Result<String> {
    let mut root: serde_json::Value = serde_json::from_str(doc.trim()).map_err(|e| {
        TakyonicError::Sql(format!("invalid JSON document: {e}"))
    })?;
    strip_nulls_inplace(&mut root);
    Ok(root.to_string())
}

fn strip_nulls_inplace(node: &mut serde_json::Value) {
    match node {
        serde_json::Value::Object(map) => {
            let keys: Vec<String> = map
                .iter()
                .filter(|(_, v)| v.is_null())
                .map(|(k, _)| k.clone())
                .collect();
            for k in keys {
                map.remove(&k);
            }
            for v in map.values_mut() {
                strip_nulls_inplace(v);
            }
        }
        serde_json::Value::Array(arr) => {
            for v in arr.iter_mut() {
                strip_nulls_inplace(v);
            }
        }
        _ => {}
    }
}

fn json_set_at(
    node: &mut serde_json::Value,
    segs: &[String],
    new_v: serde_json::Value,
    create_missing: bool,
) -> bool {
    if segs.is_empty() {
        *node = new_v;
        return true;
    }
    let head = &segs[0];
    let rest = &segs[1..];
    if rest.is_empty() {
        return match node {
            serde_json::Value::Object(map) => {
                if map.contains_key(head) || create_missing {
                    map.insert(head.clone(), new_v);
                    true
                } else {
                    false
                }
            }
            serde_json::Value::Array(arr) => {
                let Ok(i) = head.parse::<usize>() else {
                    return false;
                };
                if i < arr.len() {
                    arr[i] = new_v;
                    true
                } else if create_missing && i == arr.len() {
                    arr.push(new_v);
                    true
                } else {
                    false
                }
            }
            _ => false,
        };
    }
    match node {
        serde_json::Value::Object(map) => {
            if !map.contains_key(head) {
                if !create_missing {
                    return false;
                }
                // Next segment decides object vs array container.
                let child = if rest[0].parse::<usize>().is_ok() {
                    serde_json::Value::Array(Vec::new())
                } else {
                    serde_json::Value::Object(serde_json::Map::new())
                };
                map.insert(head.clone(), child);
            }
            let child = map.get_mut(head).unwrap();
            json_set_at(child, rest, new_v, create_missing)
        }
        serde_json::Value::Array(arr) => {
            let Ok(i) = head.parse::<usize>() else {
                return false;
            };
            if i >= arr.len() {
                return false;
            }
            json_set_at(&mut arr[i], rest, new_v, create_missing)
        }
        _ => false,
    }
}

/// `VERSION()` — PG-compatible banner announcing Takyonic as the engine.
pub fn version_text() -> String {
    format!(
        "PostgreSQL 16.0 (Takyonic {}) on {}-unknown-linux-gnu, compiled by rustc",
        env!("CARGO_PKG_VERSION"),
        std::env::consts::ARCH,
    )
}

/// `getdatabaseencoding()` / `pg_client_encoding()` — always UTF8.
pub fn database_encoding() -> &'static str {
    "UTF8"
}

/// PostgreSQL encoding id for UTF8 (`pg_wchar.h` / `pg_enc`).
pub const PG_UTF8_ENCODING: i64 = 6;

/// `pg_encoding_to_char(encoding)` — subset of PG encoding names; unknown → `""`.
pub fn pg_encoding_to_char(encoding: i64) -> &'static str {
    match encoding {
        0 => "SQL_ASCII",
        6 => "UTF8",
        8 => "LATIN1",
        36 => "WIN1252",
        _ => "",
    }
}

/// `pg_char_to_encoding(name)` — inverse of [`pg_encoding_to_char`]; unknown → `-1`.
pub fn pg_char_to_encoding(name: &str) -> i64 {
    match name.trim().to_ascii_uppercase().as_str() {
        "SQL_ASCII" => 0,
        "UTF8" | "UNICODE" => 6,
        "LATIN1" | "ISO" | "ISO88591" | "ISO-8859-1" => 8,
        "WIN1252" | "WINDOWS1252" => 36,
        _ => -1,
    }
}

static REGPROC_OIDS: std::sync::OnceLock<
    std::sync::Mutex<std::collections::BTreeMap<u32, String>>,
> = std::sync::OnceLock::new();

fn regproc_oid_map() -> &'static std::sync::Mutex<std::collections::BTreeMap<u32, String>> {
    REGPROC_OIDS.get_or_init(|| std::sync::Mutex::new(std::collections::BTreeMap::new()))
}

fn remember_regproc(oid: u32, leaf: String) {
    if let Ok(mut g) = regproc_oid_map().lock() {
        g.insert(oid, leaf);
    }
}

/// `to_regproc` / `to_regprocedure` — synthetic OID for a known SQL scalar, else NULL.
pub fn to_regproc(name: &str) -> Option<u32> {
    let leaf = crate::rbac::function_name_leaf(name);
    if leaf.is_empty() || !is_known_sql_function(&leaf) {
        return None;
    }
    let oid = crate::oid::relation_oid(&leaf);
    remember_regproc(oid, leaf);
    Some(oid)
}

/// `pg_function_is_visible(name)` — known scalars live in `pg_catalog` (always visible).
pub fn pg_function_is_visible_name(name: &str) -> bool {
    let leaf = crate::rbac::function_name_leaf(name);
    if leaf.is_empty() || !is_known_sql_function(&leaf) {
        return false;
    }
    remember_regproc(crate::oid::relation_oid(&leaf), leaf);
    true
}

/// `pg_function_is_visible(oid)` — true for OIDs previously resolved via name/`to_regproc`.
pub fn pg_function_is_visible_oid(oid: u32) -> bool {
    let Ok(g) = regproc_oid_map().lock() else {
        return false;
    };
    g.get(&oid)
        .map(|n| is_known_sql_function(n))
        .unwrap_or(false)
}

/// Resolve a previously remembered `to_regproc` OID back to a function name.
pub fn regproc_name_for_oid(oid: u32) -> Option<String> {
    regproc_oid_map()
        .lock()
        .ok()?
        .get(&oid)
        .cloned()
}

/// Normalize operator identity (`pg_catalog.=(integer,integer)` → `=`).
pub fn operator_name_leaf(spec: &str) -> String {
    let s = spec.trim().trim_matches('"');
    let before_paren = s.split('(').next().unwrap_or(s).trim();
    before_paren
        .rsplit('.')
        .next()
        .unwrap_or(before_paren)
        .trim()
        .to_string()
}

/// True when `name` is a supported SQL / JSON / vector operator symbol.
pub fn is_known_sql_operator(name: &str) -> bool {
    matches!(
        operator_name_leaf(name).as_str(),
        "=" | "<>"
            | "!="
            | "<"
            | ">"
            | "<="
            | ">="
            | "+"
            | "-"
            | "*"
            | "/"
            | "%"
            | "^"
            | "||"
            | "->"
            | "->>"
            | "#>"
            | "#>>"
            | "@>"
            | "<@"
            | "?"
            | "?|"
            | "?&"
            | "<->"
            | "~~"
            | "~~*"
            | "!~~"
            | "!~~*"
            | "~"
            | "~*"
            | "!~"
            | "!~*"
            | "&&"
            | "<<"
            | ">>"
    )
}

static REGOPER_OIDS: std::sync::OnceLock<
    std::sync::Mutex<std::collections::BTreeMap<u32, String>>,
> = std::sync::OnceLock::new();

fn regoper_oid_map() -> &'static std::sync::Mutex<std::collections::BTreeMap<u32, String>> {
    REGOPER_OIDS.get_or_init(|| std::sync::Mutex::new(std::collections::BTreeMap::new()))
}

fn remember_regoper(oid: u32, leaf: String) {
    if let Ok(mut g) = regoper_oid_map().lock() {
        g.insert(oid, leaf);
    }
}

/// `to_regoper` / `to_regoperator` — synthetic OID for a known operator, else NULL.
pub fn to_regoper(name: &str) -> Option<u32> {
    let leaf = operator_name_leaf(name);
    if leaf.is_empty() || !is_known_sql_operator(&leaf) {
        return None;
    }
    // Prefix so operator OIDs do not collide with relation/function name hashes.
    let oid = crate::oid::relation_oid(&format!("oper:{leaf}"));
    remember_regoper(oid, leaf);
    Some(oid)
}

/// `pg_operator_is_visible(name)` — known operators live in `pg_catalog`.
pub fn pg_operator_is_visible_name(name: &str) -> bool {
    let leaf = operator_name_leaf(name);
    if leaf.is_empty() || !is_known_sql_operator(&leaf) {
        return false;
    }
    let oid = crate::oid::relation_oid(&format!("oper:{leaf}"));
    remember_regoper(oid, leaf);
    true
}

/// `pg_operator_is_visible(oid)` — true for OIDs previously resolved via name/`to_regoper`.
pub fn pg_operator_is_visible_oid(oid: u32) -> bool {
    let Ok(g) = regoper_oid_map().lock() else {
        return false;
    };
    g.get(&oid)
        .map(|n| is_known_sql_operator(n))
        .unwrap_or(false)
}

/// Normalize collation identity (`pg_catalog."C"` → `c` / keep `default`).
pub fn collation_name_leaf(spec: &str) -> String {
    let s = spec.trim().trim_matches('"');
    let leaf = s
        .rsplit('.')
        .next()
        .unwrap_or(s)
        .trim()
        .trim_matches('"');
    leaf.to_ascii_lowercase()
}

/// Builtin collations Takyonic recognizes (`default` / `C` / `POSIX` / `ucs_basic`).
pub fn is_known_sql_collation(name: &str) -> bool {
    matches!(
        collation_name_leaf(name).as_str(),
        "default" | "c" | "posix" | "ucs_basic"
    )
}

static REGCOLLATION_OIDS: std::sync::OnceLock<
    std::sync::Mutex<std::collections::BTreeMap<u32, String>>,
> = std::sync::OnceLock::new();

fn regcollation_oid_map() -> &'static std::sync::Mutex<std::collections::BTreeMap<u32, String>> {
    REGCOLLATION_OIDS.get_or_init(|| std::sync::Mutex::new(std::collections::BTreeMap::new()))
}

fn remember_regcollation(oid: u32, leaf: String) {
    if let Ok(mut g) = regcollation_oid_map().lock() {
        g.insert(oid, leaf);
    }
}

/// `to_regcollation(name)` — synthetic OID for a known collation, else NULL.
pub fn to_regcollation(name: &str) -> Option<u32> {
    let leaf = collation_name_leaf(name);
    if leaf.is_empty() || !is_known_sql_collation(&leaf) {
        return None;
    }
    let oid = crate::oid::relation_oid(&format!("coll:{leaf}"));
    remember_regcollation(oid, leaf);
    Some(oid)
}

/// `pg_collation_is_visible(name)` — known collations live in `pg_catalog`.
pub fn pg_collation_is_visible_name(name: &str) -> bool {
    let leaf = collation_name_leaf(name);
    if leaf.is_empty() || !is_known_sql_collation(&leaf) {
        return false;
    }
    let oid = crate::oid::relation_oid(&format!("coll:{leaf}"));
    remember_regcollation(oid, leaf);
    true
}

/// `pg_collation_is_visible(oid)` — true for OIDs previously resolved via name/`to_regcollation`.
pub fn pg_collation_is_visible_oid(oid: u32) -> bool {
    let Ok(g) = regcollation_oid_map().lock() else {
        return false;
    };
    g.get(&oid)
        .map(|n| is_known_sql_collation(n))
        .unwrap_or(false)
}

static NEXT_ADVISORY_SESSION: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(1);

/// Allocate a unique session id for advisory-lock ownership.
pub fn alloc_advisory_session_id() -> u64 {
    NEXT_ADVISORY_SESSION.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

/// Pack PG two-int advisory key form into one `bigint` key.
pub fn advisory_lock_key_pair(k1: i32, k2: i32) -> i64 {
    ((i64::from(k1)) << 32) | i64::from(k2 as u32)
}

struct AdvisoryHeld {
    /// Exclusive holder: `(session_id, reentrant_count)`.
    exclusive: Option<(u64, u32)>,
    /// Shared holders: `session_id → reentrant_count`.
    shared: std::collections::BTreeMap<u64, u32>,
}

impl AdvisoryHeld {
    fn empty() -> Self {
        Self {
            exclusive: None,
            shared: std::collections::BTreeMap::new(),
        }
    }

    fn is_empty(&self) -> bool {
        self.exclusive.is_none() && self.shared.is_empty()
    }
}

static ADVISORY_LOCKS: std::sync::OnceLock<
    std::sync::Mutex<std::collections::BTreeMap<i64, AdvisoryHeld>>,
> = std::sync::OnceLock::new();

fn advisory_locks() -> &'static std::sync::Mutex<std::collections::BTreeMap<i64, AdvisoryHeld>> {
    ADVISORY_LOCKS.get_or_init(|| std::sync::Mutex::new(std::collections::BTreeMap::new()))
}

/// `pg_try_advisory_lock(key)` — exclusive, session-scoped; reentrant for the same session.
pub fn pg_try_advisory_lock(session_id: u64, key: i64) -> bool {
    let Ok(mut g) = advisory_locks().lock() else {
        return false;
    };
    let held = g.entry(key).or_insert_with(AdvisoryHeld::empty);
    if !held.shared.is_empty() {
        return false;
    }
    match held.exclusive {
        Some((owner, count)) if owner == session_id => {
            held.exclusive = Some((session_id, count.saturating_add(1)));
            true
        }
        Some(_) => false,
        None => {
            held.exclusive = Some((session_id, 1));
            true
        }
    }
}

/// `pg_advisory_lock(key)` — non-blocking stub: error if another session holds the lock.
pub fn pg_advisory_lock(session_id: u64, key: i64) -> Result<()> {
    if pg_try_advisory_lock(session_id, key) {
        Ok(())
    } else {
        Err(TakyonicError::Sql(
            "could not obtain advisory lock (held by another session)".into(),
        ))
    }
}

/// `pg_advisory_unlock(key)` — release one exclusive lock level; true if this session held it.
pub fn pg_advisory_unlock(session_id: u64, key: i64) -> bool {
    let Ok(mut g) = advisory_locks().lock() else {
        return false;
    };
    let Some(held) = g.get_mut(&key) else {
        return false;
    };
    match held.exclusive {
        Some((owner, count)) if owner == session_id => {
            if count <= 1 {
                held.exclusive = None;
            } else {
                held.exclusive = Some((session_id, count - 1));
            }
            if held.is_empty() {
                g.remove(&key);
            }
            true
        }
        _ => false,
    }
}

/// `pg_try_advisory_lock_shared(key)` — shared, session-scoped; conflicts with exclusive.
pub fn pg_try_advisory_lock_shared(session_id: u64, key: i64) -> bool {
    let Ok(mut g) = advisory_locks().lock() else {
        return false;
    };
    let held = g.entry(key).or_insert_with(AdvisoryHeld::empty);
    if held.exclusive.is_some() {
        return false;
    }
    let entry = held.shared.entry(session_id).or_insert(0);
    *entry = entry.saturating_add(1);
    true
}

/// `pg_advisory_lock_shared(key)` — non-blocking stub: error if exclusive is held.
pub fn pg_advisory_lock_shared(session_id: u64, key: i64) -> Result<()> {
    if pg_try_advisory_lock_shared(session_id, key) {
        Ok(())
    } else {
        Err(TakyonicError::Sql(
            "could not obtain shared advisory lock (exclusive held)".into(),
        ))
    }
}

/// `pg_advisory_unlock_shared(key)` — release one shared lock level for this session.
pub fn pg_advisory_unlock_shared(session_id: u64, key: i64) -> bool {
    let Ok(mut g) = advisory_locks().lock() else {
        return false;
    };
    let Some(held) = g.get_mut(&key) else {
        return false;
    };
    match held.shared.get_mut(&session_id) {
        Some(count) if *count > 1 => {
            *count -= 1;
            true
        }
        Some(_) => {
            held.shared.remove(&session_id);
            if held.is_empty() {
                g.remove(&key);
            }
            true
        }
        None => false,
    }
}

/// `pg_advisory_unlock_all()` — drop every advisory lock (exclusive + shared) owned by this session.
pub fn pg_advisory_unlock_all(session_id: u64) -> u32 {
    let Ok(mut g) = advisory_locks().lock() else {
        return 0;
    };
    let mut released = 0u32;
    g.retain(|_, held| {
        if matches!(held.exclusive, Some((owner, _)) if owner == session_id) {
            held.exclusive = None;
            released += 1;
        }
        if held.shared.remove(&session_id).is_some() {
            released += 1;
        }
        !held.is_empty()
    });
    // Drop xact bookkeeping for this session too.
    if let Ok(mut x) = advisory_xact_owned().lock() {
        x.remove(&session_id);
    }
    if let Ok(mut x) = advisory_xact_shared_owned().lock() {
        x.remove(&session_id);
    }
    released
}

static ADVISORY_XACT_OWNED: std::sync::OnceLock<
    std::sync::Mutex<std::collections::BTreeMap<u64, std::collections::BTreeMap<i64, u32>>>,
> = std::sync::OnceLock::new();

fn advisory_xact_owned()
-> &'static std::sync::Mutex<std::collections::BTreeMap<u64, std::collections::BTreeMap<i64, u32>>>
{
    ADVISORY_XACT_OWNED.get_or_init(|| std::sync::Mutex::new(std::collections::BTreeMap::new()))
}

static ADVISORY_XACT_SHARED_OWNED: std::sync::OnceLock<
    std::sync::Mutex<std::collections::BTreeMap<u64, std::collections::BTreeMap<i64, u32>>>,
> = std::sync::OnceLock::new();

fn advisory_xact_shared_owned()
-> &'static std::sync::Mutex<std::collections::BTreeMap<u64, std::collections::BTreeMap<i64, u32>>>
{
    ADVISORY_XACT_SHARED_OWNED
        .get_or_init(|| std::sync::Mutex::new(std::collections::BTreeMap::new()))
}

/// `pg_try_advisory_xact_lock(key)` — exclusive; released at transaction end.
pub fn pg_try_advisory_xact_lock(session_id: u64, key: i64) -> bool {
    if !pg_try_advisory_lock(session_id, key) {
        return false;
    }
    if let Ok(mut g) = advisory_xact_owned().lock() {
        *g.entry(session_id)
            .or_default()
            .entry(key)
            .or_insert(0) += 1;
    }
    true
}

/// `pg_advisory_xact_lock(key)` — non-blocking stub (same conflict rules as session exclusive).
pub fn pg_advisory_xact_lock(session_id: u64, key: i64) -> Result<()> {
    if pg_try_advisory_xact_lock(session_id, key) {
        Ok(())
    } else {
        Err(TakyonicError::Sql(
            "could not obtain transaction-level advisory lock (held by another session)".into(),
        ))
    }
}

/// `pg_try_advisory_xact_lock_shared(key)` — shared; released at transaction end.
pub fn pg_try_advisory_xact_lock_shared(session_id: u64, key: i64) -> bool {
    if !pg_try_advisory_lock_shared(session_id, key) {
        return false;
    }
    if let Ok(mut g) = advisory_xact_shared_owned().lock() {
        *g.entry(session_id)
            .or_default()
            .entry(key)
            .or_insert(0) += 1;
    }
    true
}

/// `pg_advisory_xact_lock_shared(key)` — non-blocking stub.
pub fn pg_advisory_xact_lock_shared(session_id: u64, key: i64) -> Result<()> {
    if pg_try_advisory_xact_lock_shared(session_id, key) {
        Ok(())
    } else {
        Err(TakyonicError::Sql(
            "could not obtain shared transaction-level advisory lock (exclusive held)".into(),
        ))
    }
}

/// Release all transaction-scoped advisory locks for `session_id` (COMMIT/ROLLBACK / auto-commit).
pub fn pg_advisory_xact_unlock_all(session_id: u64) -> u32 {
    let owned = advisory_xact_owned()
        .lock()
        .ok()
        .and_then(|mut g| g.remove(&session_id))
        .unwrap_or_default();
    let shared_owned = advisory_xact_shared_owned()
        .lock()
        .ok()
        .and_then(|mut g| g.remove(&session_id))
        .unwrap_or_default();
    let mut released = 0u32;
    for (key, count) in owned {
        for _ in 0..count {
            if pg_advisory_unlock(session_id, key) {
                released += 1;
            }
        }
    }
    for (key, count) in shared_owned {
        for _ in 0..count {
            if pg_advisory_unlock_shared(session_id, key) {
                released += 1;
            }
        }
    }
    released
}

/// `pg_size_pretty(bytes)` — human-readable size (PG-style binary units).
pub fn pg_size_pretty(bytes: i64) -> String {
    let neg = bytes < 0;
    let mut n = bytes.unsigned_abs();
    const UNITS: [&str; 6] = ["bytes", "kB", "MB", "GB", "TB", "PB"];
    let mut unit = 0usize;
    // PG switches unit when value would be ≥ 10 of the next unit (approx).
    while unit + 1 < UNITS.len() && n >= 10 * 1024 {
        n /= 1024;
        unit += 1;
    }
    let body = if unit == 0 {
        format!("{n} {}", UNITS[unit])
    } else {
        format!("{n} {}", UNITS[unit])
    };
    if neg {
        format!("-{body}")
    } else {
        body
    }
}

/// `pg_size_bytes(text)` — parse a `pg_size_pretty`-style size string to bytes.
pub fn pg_size_bytes(text: &str) -> Result<i64> {
    let s = text.trim();
    if s.is_empty() {
        return Err(TakyonicError::Sql(
            "PG_SIZE_BYTES requires a non-empty size string".into(),
        ));
    }
    let neg = s.starts_with('-');
    let s = s.strip_prefix(['+', '-']).unwrap_or(s).trim_start();
    let (num_str, unit_str) = {
        let end = s
            .char_indices()
            .find(|(_, c)| !(c.is_ascii_digit() || *c == '.'))
            .map(|(i, _)| i)
            .unwrap_or(s.len());
        (s[..end].trim(), s[end..].trim())
    };
    if num_str.is_empty() {
        return Err(TakyonicError::Sql(
            "PG_SIZE_BYTES could not parse numeric size".into(),
        ));
    }
    let n: f64 = num_str.parse().map_err(|_| {
        TakyonicError::Sql("PG_SIZE_BYTES could not parse numeric size".into())
    })?;
    if !n.is_finite() || n < 0.0 {
        return Err(TakyonicError::Sql(
            "PG_SIZE_BYTES size must be a non-negative finite number".into(),
        ));
    }
    let mult: f64 = match unit_str.to_ascii_lowercase().as_str() {
        "" | "b" | "byte" | "bytes" => 1.0,
        "kb" | "k" => 1024.0,
        "mb" | "m" => 1024.0 * 1024.0,
        "gb" | "g" => 1024.0 * 1024.0 * 1024.0,
        "tb" | "t" => 1024.0f64.powi(4),
        "pb" | "p" => 1024.0f64.powi(5),
        other => {
            return Err(TakyonicError::Sql(format!(
                "PG_SIZE_BYTES unrecognized size unit `{other}`"
            )));
        }
    };
    let bytes = (n * mult).round() as i64;
    Ok(if neg { -bytes } else { bytes })
}

/// `pg_typeof` type name from a runtime [`Value`] (approximate PG names).
pub fn pg_typeof_value(v: &Value) -> Option<&'static str> {
    match v {
        Value::Null => None,
        Value::Int(_) => Some("bigint"),
        Value::Float(_) => Some("double precision"),
        Value::Bool(_) => Some("boolean"),
        Value::String(s) if decode_interval_secs(s).is_some() => Some("interval"),
        Value::String(_) => Some("text"),
    }
}

thread_local! {
    /// PG-style `random()` / `setseed()` state (per OS thread).
    static PG_RNG: std::cell::Cell<u64> = const { std::cell::Cell::new(0x4d59_5a6e_ad1b_bc8d) };
}

/// `setseed(x)` — seed in `[-1, 1]` (PostgreSQL). Returns void/`NULL` in SQL.
pub fn setseed(seed: f64) -> Result<()> {
    if !(-1.0..=1.0).contains(&seed) || seed.is_nan() {
        return Err(TakyonicError::Sql(
            "SETSEED argument must be in [-1, 1]".into(),
        ));
    }
    // Map [-1,1] onto a non-zero u64 state (PG uses a similar scaled int seed).
    let scaled = (seed * (i32::MAX as f64)) as i32;
    let mut state = scaled as u64;
    if state == 0 {
        state = 0x4d59_5a6e_ad1b_bc8d;
    }
    PG_RNG.with(|c| c.set(state));
    Ok(())
}

/// `random()` — uniform in `[0, 1)` from the thread-local seed.
pub fn random_f64() -> f64 {
    PG_RNG.with(|c| {
        // SplitMix64 step
        let mut z = c.get().wrapping_add(0x9e37_79b9_7f4a_7c15);
        c.set(z);
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^= z >> 31;
        ((z >> 11) as f64) * (1.0 / ((1u64 << 53) as f64))
    })
}

/// Next 64 bits from the `random()`/`setseed()` PRNG.
fn random_u64() -> u64 {
    PG_RNG.with(|c| {
        let mut z = c.get().wrapping_add(0x9e37_79b9_7f4a_7c15);
        c.set(z);
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    })
}

/// `gen_random_uuid()` — RFC 4122 UUID version 4 text.
pub fn gen_random_uuid() -> String {
    let a = random_u64();
    let b = random_u64();
    let mut bytes = [0u8; 16];
    bytes[..8].copy_from_slice(&a.to_le_bytes());
    bytes[8..].copy_from_slice(&b.to_le_bytes());
    bytes[6] = (bytes[6] & 0x0f) | 0x40; // version 4
    bytes[8] = (bytes[8] & 0x3f) | 0x80; // IETF variant
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0],
        bytes[1],
        bytes[2],
        bytes[3],
        bytes[4],
        bytes[5],
        bytes[6],
        bytes[7],
        bytes[8],
        bytes[9],
        bytes[10],
        bytes[11],
        bytes[12],
        bytes[13],
        bytes[14],
        bytes[15],
    )
}

/// `pg_sleep(seconds)` — sleep for `seconds` (fractional OK). Returns void/`NULL`.
pub fn pg_sleep(seconds: f64) -> Result<()> {
    if seconds.is_nan() || seconds < 0.0 {
        return Err(TakyonicError::Sql(
            "PG_SLEEP argument must be a non-negative number".into(),
        ));
    }
    if seconds > 0.0 {
        let dur = std::time::Duration::from_secs_f64(seconds.min(86_400.0));
        std::thread::sleep(dur);
    }
    Ok(())
}

/// Soft capacity for the process-local async notification queues (usage fraction).
const NOTIFY_QUEUE_CAPACITY: usize = 1000;

struct NotifyRegistry {
    listeners: std::collections::BTreeMap<u64, std::collections::BTreeSet<String>>,
    queues: std::collections::BTreeMap<u64, Vec<(String, String)>>,
}

fn notify_registry() -> &'static std::sync::Mutex<NotifyRegistry> {
    static REG: std::sync::OnceLock<std::sync::Mutex<NotifyRegistry>> = std::sync::OnceLock::new();
    REG.get_or_init(|| {
        std::sync::Mutex::new(NotifyRegistry {
            listeners: std::collections::BTreeMap::new(),
            queues: std::collections::BTreeMap::new(),
        })
    })
}

/// Register `session_id` as listening on `channel` (for NOTIFY delivery).
pub fn register_listen(session_id: u64, channel: &str) {
    let mut g = notify_registry().lock().unwrap_or_else(|e| e.into_inner());
    g.listeners
        .entry(session_id)
        .or_default()
        .insert(channel.to_string());
}

/// Drop one channel (`Some`) or all channels (`None`) for `session_id`.
pub fn register_unlisten(session_id: u64, channel: Option<&str>) {
    let mut g = notify_registry().lock().unwrap_or_else(|e| e.into_inner());
    match channel {
        Some(ch) => {
            if let Some(set) = g.listeners.get_mut(&session_id) {
                set.remove(ch);
                if set.is_empty() {
                    g.listeners.remove(&session_id);
                }
            }
        }
        None => {
            g.listeners.remove(&session_id);
        }
    }
}

/// Pending `(channel, payload)` notifications for a session (test / future pgwire drain).
pub fn drain_notifications(session_id: u64) -> Vec<(String, String)> {
    let mut g = notify_registry().lock().unwrap_or_else(|e| e.into_inner());
    g.queues.remove(&session_id).unwrap_or_default()
}

/// `pg_notify(channel, payload)` — enqueue to every session currently LISTENing on `channel`.
pub fn pg_notify(channel: &str, payload: &str) -> Result<()> {
    if channel.is_empty() {
        return Err(TakyonicError::Sql(
            "PG_NOTIFY channel name must not be empty".into(),
        ));
    }
    let mut g = notify_registry().lock().unwrap_or_else(|e| e.into_inner());
    let targets: Vec<u64> = g
        .listeners
        .iter()
        .filter_map(|(sid, chans)| {
            if chans.contains(channel) {
                Some(*sid)
            } else {
                None
            }
        })
        .collect();
    for sid in targets {
        let q = g.queues.entry(sid).or_default();
        if q.len() >= NOTIFY_QUEUE_CAPACITY {
            return Err(TakyonicError::Sql(
                "too many notifications in the notification queue".into(),
            ));
        }
        q.push((channel.to_string(), payload.to_string()));
    }
    Ok(())
}

/// `pg_notification_queue_usage()` — this session's pending / capacity.
pub fn pg_notification_queue_usage(session_id: u64) -> f64 {
    let g = notify_registry().lock().unwrap_or_else(|e| e.into_inner());
    let pending = g.queues.get(&session_id).map(|q| q.len()).unwrap_or(0);
    (pending as f64 / NOTIFY_QUEUE_CAPACITY as f64).min(1.0)
}

/// `pg_listening_channels()` — format session LISTEN set as a text array stub.
pub fn format_listening_channels(channels: &[String]) -> String {
    if channels.is_empty() {
        "[]".into()
    } else {
        format!("[{}]", channels.join(","))
    }
}

/// `pg_column_size(any)` — approximate on-disk/datum byte size.
pub fn pg_column_size(v: &Value) -> Option<i64> {
    match v {
        Value::Null => None,
        Value::Bool(_) => Some(1),
        Value::Int(_) => Some(8),
        Value::Float(_) => Some(8),
        Value::String(s) => Some(s.len() as i64),
    }
}

static TXID_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

/// Allocate the next synthetic transaction id (statement-scoped via [`crate::executor::ExecutionContext`]).
pub fn next_txid() -> u64 {
    TXID_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

/// `txid_status(xid)` / `pg_xact_status(xid)` — status for synthetic statement xids.
///
/// Past ids are treated as committed; the active statement xid is in progress;
/// unknown/future/non-positive → NULL.
pub fn txid_status(xid: i64, current: u64) -> Option<&'static str> {
    if xid <= 0 {
        return None;
    }
    let xid = xid as u64;
    if xid == current {
        Some("in progress")
    } else if xid < current {
        Some("committed")
    } else {
        None
    }
}

/// `pg_export_snapshot()` — opaque snapshot id for the current statement xid.
pub fn pg_export_snapshot(txid: u64) -> String {
    format!("{txid:08X}-{txid:08X}-1")
}

/// `pg_current_snapshot()` / `txid_current_snapshot()` — `xmin:xmax:xip` text form.
pub fn pg_current_snapshot(txid: u64) -> String {
    format!("{txid}:{}:", txid.saturating_add(1))
}

/// Parse `xmin:xmax:xip_list` snapshot text (`xip` is comma-separated, may be empty).
pub fn parse_snapshot_text(s: &str) -> Option<(u64, u64, Vec<u64>)> {
    let mut parts = s.splitn(3, ':');
    let xmin = parts.next()?.parse::<u64>().ok()?;
    let xmax = parts.next()?.parse::<u64>().ok()?;
    let xip = match parts.next() {
        None | Some("") => Vec::new(),
        Some(rest) => rest
            .split(',')
            .filter(|p| !p.is_empty())
            .map(|p| p.parse::<u64>().ok())
            .collect::<Option<Vec<_>>>()?,
    };
    Some((xmin, xmax, xip))
}

/// `pg_snapshot_xmin(snapshot)`.
pub fn pg_snapshot_xmin(snapshot: &str) -> Option<u64> {
    parse_snapshot_text(snapshot).map(|(xmin, _, _)| xmin)
}

/// `pg_snapshot_xmax(snapshot)`.
pub fn pg_snapshot_xmax(snapshot: &str) -> Option<u64> {
    parse_snapshot_text(snapshot).map(|(_, xmax, _)| xmax)
}

/// `pg_visible_in_snapshot(xid, snapshot)` — classic xmin/xmax/xip visibility.
pub fn pg_visible_in_snapshot(xid: i64, snapshot: &str) -> Option<bool> {
    if xid <= 0 {
        return None;
    }
    let xid = xid as u64;
    let (xmin, xmax, xip) = parse_snapshot_text(snapshot)?;
    if xid < xmin {
        Some(true)
    } else if xid >= xmax {
        Some(false)
    } else {
        Some(!xip.contains(&xid))
    }
}

/// `pg_signal_backend` — stub: succeed only for this process.
pub fn pg_signal_backend(pid: i64) -> bool {
    pid > 0 && pid == std::process::id() as i64
}

/// Synthetic WAL byte offset (shared by current/insert/flush LSN stubs).
static WAL_LSN_BYTES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0x0100_0000);

/// Format a byte offset as PostgreSQL `hi/lo` LSN text.
pub fn format_wal_lsn(bytes: u64) -> String {
    format!("{:X}/{:08X}", bytes >> 32, bytes as u32)
}

/// Parse `A/B` or `A/BBBBBBBB` LSN text into a byte offset.
pub fn parse_wal_lsn(s: &str) -> Option<u64> {
    let (hi, lo) = s.trim().split_once('/')?;
    let hi = u64::from_str_radix(hi, 16).ok()?;
    let lo = u64::from_str_radix(lo, 16).ok()?;
    if lo > u64::from(u32::MAX) {
        return None;
    }
    Some((hi << 32) | lo)
}

/// `pg_current_wal_lsn()` / insert / flush — same synthetic LSN for the stub.
pub fn pg_current_wal_lsn() -> String {
    format_wal_lsn(WAL_LSN_BYTES.load(std::sync::atomic::Ordering::Relaxed))
}

/// `pg_wal_lsn_diff(lsn1, lsn2)` — byte difference `lsn1 - lsn2`.
pub fn pg_wal_lsn_diff(lsn1: &str, lsn2: &str) -> Option<i64> {
    let a = parse_wal_lsn(lsn1)?;
    let b = parse_wal_lsn(lsn2)?;
    Some(a as i64 - b as i64)
}

/// Default WAL segment size (16 MiB), matching stock PostgreSQL.
const WAL_SEGSIZE: u64 = 16 * 1024 * 1024;

/// `pg_walfile_name(lsn)` — `TLI + log + seg` hex filename for a 16 MiB segment layout.
pub fn pg_walfile_name(lsn: &str) -> Option<String> {
    let bytes = parse_wal_lsn(lsn)?;
    let segno = bytes / WAL_SEGSIZE;
    let segs_per_id = 0x1_0000_0000u64 / WAL_SEGSIZE;
    let log = (segno / segs_per_id) as u32;
    let seg = (segno % segs_per_id) as u32;
    let tli = 1u32;
    Some(format!("{tli:08X}{log:08X}{seg:08X}"))
}

/// `pg_walfile_name_offset(lsn)` — `"filename,file_offset"` text form (record stub).
pub fn pg_walfile_name_offset(lsn: &str) -> Option<String> {
    let bytes = parse_wal_lsn(lsn)?;
    let name = pg_walfile_name(lsn)?;
    let offset = bytes % WAL_SEGSIZE;
    Some(format!("{name},{offset}"))
}

/// `pg_switch_wal()` / `pg_switch_xlog()` — advance synthetic LSN to the next segment boundary.
pub fn pg_switch_wal() -> String {
    loop {
        let cur = WAL_LSN_BYTES.load(std::sync::atomic::Ordering::Relaxed);
        let next = (cur / WAL_SEGSIZE + 1) * WAL_SEGSIZE;
        if WAL_LSN_BYTES
            .compare_exchange(
                cur,
                next,
                std::sync::atomic::Ordering::Relaxed,
                std::sync::atomic::Ordering::Relaxed,
            )
            .is_ok()
        {
            return format_wal_lsn(next);
        }
    }
}

static WAL_REPLAY_PAUSED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// `pg_is_wal_replay_paused()`.
pub fn pg_is_wal_replay_paused() -> bool {
    WAL_REPLAY_PAUSED.load(std::sync::atomic::Ordering::Relaxed)
}

/// `pg_wal_replay_pause()` — mark replay paused (process-global stub).
pub fn pg_wal_replay_pause() {
    WAL_REPLAY_PAUSED.store(true, std::sync::atomic::Ordering::Relaxed);
}

/// `pg_wal_replay_resume()` — clear replay paused flag.
pub fn pg_wal_replay_resume() {
    WAL_REPLAY_PAUSED.store(false, std::sync::atomic::Ordering::Relaxed);
}

static IN_BACKUP: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);
static BACKUP_START_TIME: std::sync::OnceLock<std::sync::Mutex<Option<String>>> =
    std::sync::OnceLock::new();

fn backup_start_time_slot() -> &'static std::sync::Mutex<Option<String>> {
    BACKUP_START_TIME.get_or_init(|| std::sync::Mutex::new(None))
}

/// `pg_is_in_backup()`.
pub fn pg_is_in_backup() -> bool {
    IN_BACKUP.load(std::sync::atomic::Ordering::Relaxed)
}

/// `pg_backup_start_time()` — wall clock when the open backup started, else NULL.
pub fn pg_backup_start_time() -> Option<String> {
    backup_start_time_slot().lock().ok()?.clone()
}

/// `pg_backup_start(label)` / `pg_start_backup` — open a non-exclusive backup stub; returns LSN.
pub fn pg_backup_start(label: &str) -> Result<String> {
    if label.is_empty() {
        return Err(TakyonicError::Sql(
            "PG_BACKUP_START requires a non-empty label".into(),
        ));
    }
    if IN_BACKUP.swap(true, std::sync::atomic::Ordering::Relaxed) {
        return Err(TakyonicError::Sql(
            "a backup is already in progress".into(),
        ));
    }
    if let Ok(mut g) = backup_start_time_slot().lock() {
        *g = Some(utc_now_timestamp());
    }
    Ok(pg_current_wal_lsn())
}

/// `pg_backup_stop()` / `pg_stop_backup` — end backup stub; returns stop LSN.
pub fn pg_backup_stop() -> Result<String> {
    if !IN_BACKUP.swap(false, std::sync::atomic::Ordering::Relaxed) {
        return Err(TakyonicError::Sql("there is no backup in progress".into()));
    }
    if let Ok(mut g) = backup_start_time_slot().lock() {
        *g = None;
    }
    Ok(pg_current_wal_lsn())
}

static RESTORE_POINTS: std::sync::OnceLock<std::sync::Mutex<Vec<(String, String)>>> =
    std::sync::OnceLock::new();

fn restore_points() -> &'static std::sync::Mutex<Vec<(String, String)>> {
    RESTORE_POINTS.get_or_init(|| std::sync::Mutex::new(Vec::new()))
}

/// `pg_create_restore_point(name)` — record a named restore point; returns its LSN.
pub fn pg_create_restore_point(name: &str) -> Result<String> {
    if name.is_empty() {
        return Err(TakyonicError::Sql(
            "PG_CREATE_RESTORE_POINT requires a non-empty name".into(),
        ));
    }
    let lsn = pg_current_wal_lsn();
    if let Ok(mut g) = restore_points().lock() {
        g.push((name.to_string(), lsn.clone()));
    }
    Ok(lsn)
}

/// `pg_promote([wait [, wait_seconds]])` — no-op on primary; returns false (not a standby).
pub fn pg_promote() -> bool {
    false
}

static POSTMASTER_START: std::sync::OnceLock<String> = std::sync::OnceLock::new();
static CONF_LOAD_TIME: std::sync::OnceLock<std::sync::Mutex<String>> = std::sync::OnceLock::new();

fn conf_load_time_slot() -> &'static std::sync::Mutex<String> {
    CONF_LOAD_TIME.get_or_init(|| std::sync::Mutex::new(pg_postmaster_start_time()))
}

fn utc_now_timestamp_frac() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let dur = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let (y, m, d, hh, mm, ss) = civil_from_days(dur.as_secs() as i64);
    format!(
        "{y:04}-{m:02}-{d:02} {hh:02}:{mm:02}:{ss:02}.{:06}+00",
        dur.subsec_micros()
    )
}

/// `pg_postmaster_start_time()` — process start timestamp (frozen at first call).
pub fn pg_postmaster_start_time() -> String {
    POSTMASTER_START
        .get_or_init(utc_now_timestamp)
        .clone()
}

/// `pg_conf_load_time()` — last config reload timestamp (starts at postmaster start).
pub fn pg_conf_load_time() -> String {
    conf_load_time_slot()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clone()
}

/// `pg_reload_conf()` — bump conf-load time and report success.
pub fn pg_reload_conf() -> bool {
    let mut slot = conf_load_time_slot()
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let mut next = utc_now_timestamp_frac();
    // Guarantee a visible advance even within the same wall-clock microsecond.
    if next <= *slot {
        next = format!("{next}+");
    }
    *slot = next;
    true
}

/// `pg_rotate_logfile()` — success stub (no log files yet).
pub fn pg_rotate_logfile() -> bool {
    true
}

struct SequenceState {
    last_value: i64,
    is_called: bool,
    increment: i64,
}

struct SequenceRegistry {
    sequences: std::collections::BTreeMap<String, SequenceState>,
    /// `table.column` (lowercased) → sequence name for `pg_get_serial_sequence`.
    owned: std::collections::BTreeMap<(String, String), String>,
    /// Per-session: last `nextval` result + which sequences have been advanced.
    session_last: std::collections::BTreeMap<u64, i64>,
    session_seen: std::collections::BTreeMap<u64, std::collections::BTreeSet<String>>,
    /// When set, mutations are mirrored to `data_dir/SEQUENCES`.
    persist_dir: Option<std::path::PathBuf>,
}

fn sequence_registry() -> &'static std::sync::Mutex<SequenceRegistry> {
    static REG: std::sync::OnceLock<std::sync::Mutex<SequenceRegistry>> = std::sync::OnceLock::new();
    REG.get_or_init(|| {
        std::sync::Mutex::new(SequenceRegistry {
            sequences: std::collections::BTreeMap::new(),
            owned: std::collections::BTreeMap::new(),
            session_last: std::collections::BTreeMap::new(),
            session_seen: std::collections::BTreeMap::new(),
            persist_dir: None,
        })
    })
}

const SEQUENCES_FILE: &str = "SEQUENCES";

fn persist_sequences_locked(g: &SequenceRegistry) -> Result<()> {
    let Some(dir) = g.persist_dir.as_ref() else {
        return Ok(());
    };
    std::fs::create_dir_all(dir)?;
    let path = dir.join(SEQUENCES_FILE);
    let tmp = dir.join(format!("{SEQUENCES_FILE}.tmp"));
    {
        let mut f = std::fs::File::create(&tmp)?;
        use std::io::Write;
        writeln!(f, "# Takyonic sequences")?;
        for (name, seq) in &g.sequences {
            writeln!(
                f,
                "SEQ {} {} {} {}",
                name,
                seq.last_value,
                if seq.is_called { 1 } else { 0 },
                seq.increment
            )?;
        }
        for ((table, col), seq) in &g.owned {
            writeln!(f, "OWNED {table} {col} {seq}")?;
        }
        f.sync_all()?;
    }
    std::fs::rename(&tmp, &path)?;
    if let Some(parent) = path.parent() {
        let _ = std::fs::File::open(parent).and_then(|f| f.sync_all());
    }
    Ok(())
}

/// Load durable sequences from `data_dir/SEQUENCES` and enable persistence.
///
/// Replaces the in-memory registry contents (session maps are cleared). Call from
/// [`crate::engine::TakyonicEngine::open`].
pub fn load_sequences(data_dir: &std::path::Path) -> Result<()> {
    let path = data_dir.join(SEQUENCES_FILE);
    let mut sequences = std::collections::BTreeMap::new();
    let mut owned = std::collections::BTreeMap::new();
    if path.exists() {
        let text = std::fs::read_to_string(&path)?;
        for (lineno, raw) in text.lines().enumerate() {
            let line = raw.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let mut parts = line.split_whitespace();
            let tag = parts.next().unwrap_or("");
            match tag {
                "SEQ" => {
                    let name = parts.next().ok_or_else(|| {
                        TakyonicError::Engine(format!(
                            "SEQUENCES line {}: missing name",
                            lineno + 1
                        ))
                    })?;
                    let last: i64 = parts
                        .next()
                        .ok_or_else(|| {
                            TakyonicError::Engine(format!(
                                "SEQUENCES line {}: missing last_value",
                                lineno + 1
                            ))
                        })?
                        .parse()
                        .map_err(|e| {
                            TakyonicError::Engine(format!(
                                "SEQUENCES line {}: bad last_value: {e}",
                                lineno + 1
                            ))
                        })?;
                    let called: u8 = parts
                        .next()
                        .unwrap_or("0")
                        .parse()
                        .unwrap_or(0);
                    let increment: i64 = parts.next().unwrap_or("1").parse().unwrap_or(1);
                    sequences.insert(
                        name.to_string(),
                        SequenceState {
                            last_value: last,
                            is_called: called != 0,
                            increment: if increment == 0 { 1 } else { increment },
                        },
                    );
                }
                "OWNED" => {
                    let table = parts.next().ok_or_else(|| {
                        TakyonicError::Engine(format!(
                            "SEQUENCES line {}: OWNED missing table",
                            lineno + 1
                        ))
                    })?;
                    let col = parts.next().ok_or_else(|| {
                        TakyonicError::Engine(format!(
                            "SEQUENCES line {}: OWNED missing column",
                            lineno + 1
                        ))
                    })?;
                    let seq = parts.next().ok_or_else(|| {
                        TakyonicError::Engine(format!(
                            "SEQUENCES line {}: OWNED missing sequence",
                            lineno + 1
                        ))
                    })?;
                    owned.insert(
                        (table.to_ascii_lowercase(), col.to_ascii_lowercase()),
                        seq.to_string(),
                    );
                }
                other => {
                    return Err(TakyonicError::Engine(format!(
                        "SEQUENCES line {}: unknown tag `{other}`",
                        lineno + 1
                    )));
                }
            }
        }
    }
    let mut g = sequence_registry().lock().unwrap_or_else(|e| e.into_inner());
    g.sequences = sequences;
    g.owned = owned;
    g.session_last.clear();
    g.session_seen.clear();
    g.persist_dir = Some(data_dir.to_path_buf());
    Ok(())
}

fn touch_sequences(g: &SequenceRegistry) {
    if let Err(e) = persist_sequences_locked(g) {
        tracing::warn!(error = %e, "failed to persist SEQUENCES");
    }
}

fn normalize_sequence_name(name: &str) -> String {
    let t = name.trim().trim_matches('"');
    // Strip optional schema prefix: public.seq → seq
    if let Some((_, leaf)) = t.rsplit_once('.') {
        leaf.to_ascii_lowercase()
    } else {
        t.to_ascii_lowercase()
    }
}

fn ensure_sequence<'a>(g: &'a mut SequenceRegistry, name: &str) -> &'a mut SequenceState {
    g.sequences
        .entry(name.to_string())
        .or_insert(SequenceState {
            last_value: 1,
            is_called: false,
            increment: 1,
        })
}

/// `nextval(regclass)` — advance sequence (auto-creates missing sequences at 1).
pub fn nextval(session_id: u64, name: &str) -> Result<i64> {
    let key = normalize_sequence_name(name);
    if key.is_empty() {
        return Err(TakyonicError::Sql(
            "NEXTVAL requires a non-empty sequence name".into(),
        ));
    }
    let mut g = sequence_registry().lock().unwrap_or_else(|e| e.into_inner());
    let out = {
        let seq = ensure_sequence(&mut g, &key);
        if seq.is_called {
            seq.last_value = seq.last_value.saturating_add(seq.increment);
            seq.last_value
        } else {
            seq.is_called = true;
            seq.last_value
        }
    };
    g.session_last.insert(session_id, out);
    g.session_seen.entry(session_id).or_default().insert(key);
    touch_sequences(&g);
    Ok(out)
}

/// `currval(regclass)` — last `nextval` for this sequence in this session.
pub fn currval(session_id: u64, name: &str) -> Result<i64> {
    let key = normalize_sequence_name(name);
    let g = sequence_registry().lock().unwrap_or_else(|e| e.into_inner());
    let seen = g.session_seen.get(&session_id).is_some_and(|s| s.contains(&key));
    if !seen {
        return Err(TakyonicError::Sql(format!(
            "currval of sequence \"{key}\" is not yet defined in this session"
        )));
    }
    let seq = g.sequences.get(&key).ok_or_else(|| {
        TakyonicError::Sql(format!("relation \"{key}\" does not exist"))
    })?;
    if !seq.is_called {
        return Err(TakyonicError::Sql(format!(
            "currval of sequence \"{key}\" is not yet defined in this session"
        )));
    }
    Ok(seq.last_value)
}

/// `lastval()` — most recent `nextval` in this session.
pub fn lastval(session_id: u64) -> Result<i64> {
    let g = sequence_registry().lock().unwrap_or_else(|e| e.into_inner());
    g.session_last.get(&session_id).copied().ok_or_else(|| {
        TakyonicError::Sql("lastval is not yet defined in this session".into())
    })
}

/// `setval(regclass, value [, is_called])` — set sequence state; returns `value`.
pub fn setval(session_id: u64, name: &str, value: i64, is_called: bool) -> Result<i64> {
    let key = normalize_sequence_name(name);
    if key.is_empty() {
        return Err(TakyonicError::Sql(
            "SETVAL requires a non-empty sequence name".into(),
        ));
    }
    let mut g = sequence_registry().lock().unwrap_or_else(|e| e.into_inner());
    {
        let seq = ensure_sequence(&mut g, &key);
        seq.last_value = value;
        seq.is_called = is_called;
    }
    if is_called {
        g.session_last.insert(session_id, value);
        g.session_seen.entry(session_id).or_default().insert(key);
    }
    touch_sequences(&g);
    Ok(value)
}

/// `CREATE SEQUENCE` — register (or no-op with `IF NOT EXISTS`).
pub fn create_sequence(name: &str, if_not_exists: bool, start: i64, increment: i64) -> Result<()> {
    let key = normalize_sequence_name(name);
    if key.is_empty() {
        return Err(TakyonicError::Sql(
            "CREATE SEQUENCE requires a non-empty name".into(),
        ));
    }
    if increment == 0 {
        return Err(TakyonicError::Sql(
            "INCREMENT must not be zero".into(),
        ));
    }
    let mut g = sequence_registry().lock().unwrap_or_else(|e| e.into_inner());
    if g.sequences.contains_key(&key) {
        if if_not_exists {
            return Ok(());
        }
        return Err(TakyonicError::Sql(format!(
            "relation \"{key}\" already exists"
        )));
    }
    g.sequences.insert(
        key,
        SequenceState {
            last_value: start,
            is_called: false,
            increment,
        },
    );
    touch_sequences(&g);
    Ok(())
}

/// `DROP SEQUENCE` — remove sequence (error unless `IF EXISTS`).
pub fn drop_sequence(name: &str, if_exists: bool) -> Result<()> {
    let key = normalize_sequence_name(name);
    if key.is_empty() {
        return Err(TakyonicError::Sql(
            "DROP SEQUENCE requires a non-empty name".into(),
        ));
    }
    let mut g = sequence_registry().lock().unwrap_or_else(|e| e.into_inner());
    if g.sequences.remove(&key).is_none() && !if_exists {
        return Err(TakyonicError::Sql(format!(
            "sequence \"{key}\" does not exist"
        )));
    }
    g.owned.retain(|_, seq| seq != &key);
    touch_sequences(&g);
    Ok(())
}

/// `ALTER SEQUENCE` — restart / increment / owned-by / rename.
pub fn alter_sequence(
    name: &str,
    restart: Option<i64>,
    increment: Option<i64>,
    owned_by: Option<Option<(String, String)>>,
    rename_to: Option<&str>,
) -> Result<()> {
    let key = normalize_sequence_name(name);
    if key.is_empty() {
        return Err(TakyonicError::Sql(
            "ALTER SEQUENCE requires a non-empty name".into(),
        ));
    }
    if let Some(0) = increment {
        return Err(TakyonicError::Sql("INCREMENT must not be zero".into()));
    }
    let mut g = sequence_registry().lock().unwrap_or_else(|e| e.into_inner());
    {
        let seq = g.sequences.get_mut(&key).ok_or_else(|| {
            TakyonicError::Sql(format!("sequence \"{key}\" does not exist"))
        })?;
        if let Some(v) = restart {
            seq.last_value = v;
            seq.is_called = false;
        }
        if let Some(inc) = increment {
            seq.increment = inc;
        }
    }
    if let Some(owner) = owned_by {
        g.owned.retain(|_, seq| seq != &key);
        if let Some((table, column)) = owner {
            let table = table.to_ascii_lowercase();
            let column = column.to_ascii_lowercase();
            g.owned.insert((table, column), key.clone());
        }
    }
    if let Some(new_name) = rename_to {
        let new_key = normalize_sequence_name(new_name);
        if new_key.is_empty() {
            return Err(TakyonicError::Sql(
                "RENAME TO requires a non-empty name".into(),
            ));
        }
        if new_key != key {
            if g.sequences.contains_key(&new_key) {
                return Err(TakyonicError::Sql(format!(
                    "relation \"{new_key}\" already exists"
                )));
            }
            let state = g.sequences.remove(&key).ok_or_else(|| {
                TakyonicError::Sql(format!("sequence \"{key}\" does not exist"))
            })?;
            g.sequences.insert(new_key.clone(), state);
            for seq in g.owned.values_mut() {
                if seq == &key {
                    *seq = new_key.clone();
                }
            }
        }
    }
    touch_sequences(&g);
    Ok(())
}

/// `pg_get_serial_sequence(table, column)` — owned sequence name, or NULL.
pub fn pg_get_serial_sequence(table: &str, column: &str) -> Option<String> {
    let table = normalize_sequence_name(table);
    let column = column.trim().trim_matches('"').to_ascii_lowercase();
    let g = sequence_registry().lock().unwrap_or_else(|e| e.into_inner());
    g.owned
        .get(&(table, column))
        .map(|seq| format!("public.{seq}"))
}

/// Drop every sequence `OWNED BY` columns of `table` (DROP TABLE cleanup).
pub fn drop_sequences_owned_by_table(table: &str) {
    let table = normalize_sequence_name(table);
    let mut g = sequence_registry().lock().unwrap_or_else(|e| e.into_inner());
    let seqs: Vec<String> = g
        .owned
        .iter()
        .filter_map(|((t, _), seq)| if t == &table { Some(seq.clone()) } else { None })
        .collect();
    g.owned.retain(|(t, _), _| t != &table);
    for seq in seqs {
        g.sequences.remove(&seq);
    }
    touch_sequences(&g);
}

/// Drop the sequence `OWNED BY table.column`, if any (DROP COLUMN cleanup).
pub fn drop_sequence_owned_by_column(table: &str, column: &str) {
    let table = normalize_sequence_name(table);
    let column = column.trim().trim_matches('"').to_ascii_lowercase();
    let mut g = sequence_registry().lock().unwrap_or_else(|e| e.into_inner());
    if let Some(seq) = g.owned.remove(&(table, column)) {
        g.sequences.remove(&seq);
    }
    touch_sequences(&g);
}

/// Fill missing columns from catalog `DEFAULT` expressions (after SERIAL fill).
pub fn fill_column_defaults(
    session_id: u64,
    schema: &crate::schema::TableSchema,
    record: &mut crate::schema::Record,
    ctx: &crate::executor::ExecutionContext,
) -> Result<()> {
    for col in &schema.columns {
        if record.get(&col.name).is_some() {
            continue;
        }
        let Some(def_sql) = &col.default_sql else {
            continue;
        };
        let v = eval_default_sql(session_id, def_sql, ctx)?;
        *record = record.clone().set(col.name.clone(), v);
    }
    Ok(())
}

fn eval_default_sql(
    session_id: u64,
    def_sql: &str,
    ctx: &crate::executor::ExecutionContext,
) -> Result<String> {
    let trimmed = def_sql.trim();
    let upper = trimmed.to_ascii_uppercase();
    // Common defaults without a full re-parse.
    if upper == "NOW()"
        || upper == "CURRENT_TIMESTAMP"
        || upper == "CURRENT_TIMESTAMP()"
    {
        return Ok(utc_now_timestamp());
    }
    if upper == "CURRENT_DATE" || upper == "CURRENT_DATE()" {
        return Ok(utc_now_timestamp()[..10].to_string());
    }
    if upper == "GEN_RANDOM_UUID()" || upper == "UUID_GENERATE_V4()" {
        return Ok(gen_random_uuid());
    }
    // nextval('seq') / nextval("seq")
    if let Some(rest) = upper.strip_prefix("NEXTVAL(") {
        let _ = rest;
        if let Some(name) = extract_paren_string_arg(trimmed) {
            return Ok(nextval(session_id, &name)?.to_string());
        }
    }
    // Literal: 'text' or number
    if let Some(s) = trimmed.strip_prefix('\'').and_then(|s| s.strip_suffix('\'')) {
        return Ok(s.replace("''", "'"));
    }
    if trimmed.parse::<i64>().is_ok() || trimmed.parse::<f64>().is_ok() {
        return Ok(trimmed.to_string());
    }
    if matches!(upper.as_str(), "TRUE" | "FALSE" | "NULL") {
        return Ok(if upper == "NULL" {
            String::new()
        } else {
            upper.to_ascii_lowercase()
        });
    }
    // Fall back: plan `SELECT <expr>` projection via LogicalPlanner is heavy;
    // evaluate as a scalar SQL expression through the existing parser.
    let sql = format!("SELECT ({trimmed}) AS d");
    let plan = LogicalPlanner::plan(&sql)?;
    match plan {
        LogicalPlan::Project { columns, .. } => {
            if let Some((_, expr)) = columns.first() {
                let v = crate::executor::evaluate(expr, &crate::schema::Record::new(), ctx)?;
                return Ok(crate::executor::value_to_field(&v));
            }
        }
        LogicalPlan::Select { .. } => {}
        _ => {}
    }
    Err(TakyonicError::Sql(format!(
        "unsupported DEFAULT expression: {def_sql}"
    )))
}

fn extract_paren_string_arg(s: &str) -> Option<String> {
    let start = s.find('(')?;
    let end = s.rfind(')')?;
    let inner = s[start + 1..end].trim();
    let inner = inner.trim_matches('\'').trim_matches('"');
    if inner.is_empty() {
        None
    } else {
        Some(inner.to_string())
    }
}

/// Fill missing columns that have a serial sequence with `nextval`.
pub fn fill_serial_defaults(
    session_id: u64,
    table: &str,
    column_names: &[String],
    record: &mut crate::schema::Record,
) -> Result<()> {
    for col in column_names {
        if record.get(col).is_some() {
            continue;
        }
        if let Some(reg) = pg_get_serial_sequence(table, col) {
            let seq = reg
                .strip_prefix("public.")
                .unwrap_or(reg.as_str());
            let v = nextval(session_id, seq)?;
            *record = record.clone().set(col.clone(), v.to_string());
        }
    }
    Ok(())
}

/// `pg_sequence_last_value(regclass)` — last issued value, or NULL if never used.
pub fn pg_sequence_last_value(name: &str) -> Result<Option<i64>> {
    let key = normalize_sequence_name(name);
    if key.is_empty() {
        return Err(TakyonicError::Sql(
            "PG_SEQUENCE_LAST_VALUE requires a non-empty sequence name".into(),
        ));
    }
    let g = sequence_registry().lock().unwrap_or_else(|e| e.into_inner());
    match g.sequences.get(&key) {
        None => Err(TakyonicError::Sql(format!(
            "relation \"{key}\" does not exist"
        ))),
        Some(seq) if seq.is_called => Ok(Some(seq.last_value)),
        Some(_) => Ok(None),
    }
}

/// True when a named sequence is registered.
pub fn sequence_exists(name: &str) -> bool {
    let key = normalize_sequence_name(name);
    if key.is_empty() {
        return false;
    }
    sequence_registry()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .sequences
        .contains_key(&key)
}

/// Resolve a known GUC for `current_setting(name)`.
pub fn current_setting_value(
    name: &str,
    search_path: &str,
    transaction_isolation: &str,
    current_user: &str,
    current_catalog: &str,
    timezone: &str,
) -> Option<String> {
    let key = name.trim().to_ascii_lowercase();
    if let Some(v) = guc_overlay_get(&key) {
        return Some(v);
    }
    match key.as_str() {
        "search_path" => Some(search_path.to_string()),
        "transaction_isolation" => Some(transaction_isolation.to_string()),
        "timezone" | "time_zone" => Some(timezone.to_string()),
        "server_version" => Some(format!("16.0 (Takyonic {})", env!("CARGO_PKG_VERSION"))),
        "server_encoding" | "client_encoding" => Some("UTF8".into()),
        "server_version_num" => Some("160000".into()),
        "is_superuser" => Some(
            if current_user.eq_ignore_ascii_case("postgres") {
                "on"
            } else {
                "off"
            }
            .into(),
        ),
        "session_user" | "current_user" => Some(current_user.to_string()),
        "current_catalog" | "current_database" => Some(current_catalog.to_string()),
        _ => None,
    }
}

thread_local! {
    static GUC_OVERLAY: std::cell::RefCell<std::collections::HashMap<String, String>> =
        std::cell::RefCell::new(std::collections::HashMap::new());
    static GUC_LOCAL_KEYS: std::cell::RefCell<std::collections::HashSet<String>> =
        std::cell::RefCell::new(std::collections::HashSet::new());
}

fn guc_overlay_get(name: &str) -> Option<String> {
    GUC_OVERLAY.with(|o| o.borrow().get(name).cloned())
}

/// Clear in-flight `set_config` overlay (start of statement).
pub fn clear_guc_overlay() {
    GUC_OVERLAY.with(|o| o.borrow_mut().clear());
    GUC_LOCAL_KEYS.with(|o| o.borrow_mut().clear());
}

/// Drain overlay written by `set_config` during the last statement.
pub fn take_guc_overlay() -> (std::collections::HashMap<String, String>, std::collections::HashSet<String>) {
    let values = GUC_OVERLAY.with(|o| std::mem::take(&mut *o.borrow_mut()));
    let local = GUC_LOCAL_KEYS.with(|o| std::mem::take(&mut *o.borrow_mut()));
    (values, local)
}

/// `set_config(name, value, is_local)` — set a writable GUC; returns the new value.
pub fn set_config(
    name: &str,
    value: &str,
    is_local: bool,
    in_transaction: bool,
) -> Result<String> {
    if is_local && !in_transaction {
        return Err(TakyonicError::Sql(
            "SET LOCAL can only be used in transaction blocks".into(),
        ));
    }
    let name = normalize_guc_name(name);
    let value = normalize_guc_value(&name, value)?;
    GUC_OVERLAY.with(|o| {
        o.borrow_mut().insert(name.clone(), value.clone());
    });
    GUC_LOCAL_KEYS.with(|o| {
        let mut local = o.borrow_mut();
        if is_local {
            local.insert(name.clone());
        } else {
            local.remove(&name);
        }
    });
    Ok(value)
}

/// UTC wall-clock helpers for `NOW()` / `CURRENT_*` (no external time crate).
pub fn utc_now_timestamp() -> String {
    let (y, m, d, hh, mm, ss) = utc_parts_now();
    format!("{y:04}-{m:02}-{d:02} {hh:02}:{mm:02}:{ss:02}+00")
}

/// `TIMEOFDAY()` — live wall clock as PG-style text (`Thu Jan 15 12:00:00.000000 2026 UTC`).
pub fn timeofday_now() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let dur = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    format_timeofday(dur.as_secs() as i64, dur.subsec_micros())
}

/// Format Unix seconds + microseconds as `TIMEOFDAY` text (always `UTC` suffix).
pub fn format_timeofday(secs: i64, micros: u32) -> String {
    const WDAYS: [&str; 7] = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
    const MONTHS: [&str; 12] = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];
    let (y, m, d, hh, mm, ss) = civil_from_days(secs);
    let days = secs.div_euclid(86_400);
    // 1970-01-01 was Thursday → index 4 when Sunday = 0.
    let wday = WDAYS[((days + 4).rem_euclid(7)) as usize];
    let mon = MONTHS[(m as usize).saturating_sub(1).min(11)];
    format!("{wday} {mon} {d:02} {hh:02}:{mm:02}:{ss:02}.{micros:06} {y:04} UTC")
}

/// `CURRENT_DATE` → `YYYY-MM-DD`.
pub fn utc_now_date() -> String {
    let (y, m, d, _, _, _) = utc_parts_now();
    format!("{y:04}-{m:02}-{d:02}")
}

/// `CURRENT_TIME` → `HH:MM:SS+00`.
pub fn utc_now_time() -> String {
    let (_, _, _, hh, mm, ss) = utc_parts_now();
    format!("{hh:02}:{mm:02}:{ss:02}+00")
}

/// Date portion of a `YYYY-MM-DD …` timestamp text.
pub fn date_from_timestamp_text(ts: &str) -> String {
    ts.get(..10).unwrap_or(ts).to_string()
}

/// Time portion of a `YYYY-MM-DD HH:MM:SS+00` timestamp text.
pub fn time_from_timestamp_text(ts: &str) -> String {
    let rest = ts.get(11..).unwrap_or("00:00:00+00");
    if rest.contains('+') || rest.contains('Z') || rest.ends_with('z') {
        rest.to_string()
    } else if rest.len() >= 8 {
        format!("{}+00", &rest[..8])
    } else {
        format!("{rest}+00")
    }
}

fn utc_parts_now() -> (i32, u32, u32, u32, u32, u32) {
    use std::time::{SystemTime, UNIX_EPOCH};
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    civil_from_days(secs)
}

/// Convert Unix seconds to UTC Y-M-D h:m:s (Howard Hinnant algorithm).
fn civil_from_days(secs: i64) -> (i32, u32, u32, u32, u32, u32) {
    let days = secs.div_euclid(86_400);
    let tod = secs.rem_euclid(86_400) as u32;
    let hour = tod / 3600;
    let min = (tod % 3600) / 60;
    let sec = tod % 60;
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = (yoe as i64) + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y as i32, m, d, hour, min, sec)
}

/// Parse `YYYY-MM-DD` or `YYYY-MM-DD[ T]HH:MM:SS…` into UTC parts.
pub fn parse_timestamp_parts(s: &str) -> Option<(i32, u32, u32, u32, u32, u32)> {
    let s = s.trim();
    if s.len() < 10 {
        return None;
    }
    let y: i32 = s.get(0..4)?.parse().ok()?;
    if s.as_bytes().get(4) != Some(&b'-') {
        return None;
    }
    let m: u32 = s.get(5..7)?.parse().ok()?;
    if s.as_bytes().get(7) != Some(&b'-') {
        return None;
    }
    let d: u32 = s.get(8..10)?.parse().ok()?;
    if s.len() == 10 {
        return Some((y, m, d, 0, 0, 0));
    }
    let rest = s.get(10..)?.trim_start();
    let rest = rest
        .strip_prefix('T')
        .or_else(|| rest.strip_prefix(' '))
        .unwrap_or(rest);
    if rest.len() < 8 {
        return Some((y, m, d, 0, 0, 0));
    }
    let hh: u32 = rest.get(0..2)?.parse().ok()?;
    let mm: u32 = rest.get(3..5)?.parse().ok()?;
    let ss: u32 = rest.get(6..8)?.parse().ok()?;
    Some((y, m, d, hh, mm, ss))
}

/// True when `s` carries an explicit numeric timezone offset (`+00`, `-05:30`, …).
pub fn timestamp_has_offset(s: &str) -> bool {
    parse_timestamp_offset_secs(s).is_some()
}

/// Offset seconds from a timestamp suffix (`…+03:00` / `…-05` / `…Z`), if present.
pub fn parse_timestamp_offset_secs(s: &str) -> Option<i64> {
    let s = s.trim();
    if s.ends_with('Z') || s.ends_with('z') {
        return Some(0);
    }
    let bytes = s.as_bytes();
    let mut i = bytes.len();
    while i > 0 {
        let c = bytes[i - 1];
        if c.is_ascii_digit() || c == b':' {
            i -= 1;
            continue;
        }
        if c == b'+' || c == b'-' {
            let sign = if c == b'+' { 1i64 } else { -1 };
            let off = std::str::from_utf8(&bytes[i..]).ok()?;
            return Some(sign * parse_hm_offset(off)?);
        }
        break;
    }
    None
}

fn parse_hm_offset(off: &str) -> Option<i64> {
    let off = off.trim();
    if off.is_empty() {
        return None;
    }
    if let Some((h, m)) = off.split_once(':') {
        let hh: i64 = h.parse().ok()?;
        let mm: i64 = m.parse().ok()?;
        return Some(hh * 3600 + mm * 60);
    }
    let hh: i64 = off.parse().ok()?;
    Some(hh * 3600)
}

/// Parse a **fixed** zone literal (`UTC`, `GMT`, `+03:00`, `UTC-5`) into seconds east of UTC.
///
/// IANA names are rejected here — use [`zone_offset_secs_at`] / [`at_time_zone`] which are
/// DST-aware. Prefer [`normalize_timezone`] for `SET TimeZone` validation.
pub fn parse_zone_offset_secs(zone: &str) -> Result<i64> {
    parse_fixed_zone_offset_secs(zone).ok_or_else(|| {
        TakyonicError::Sql(format!(
            "unsupported time zone `{zone}` (use UTC/GMT, ±HH[:MM] offset, or an IANA name)"
        ))
    })
}

fn parse_fixed_zone_offset_secs(zone: &str) -> Option<i64> {
    let z = zone.trim();
    let upper = z.to_ascii_uppercase();
    if upper == "UTC" || upper == "GMT" || upper == "Z" {
        return Some(0);
    }
    let rest = upper
        .strip_prefix("UTC")
        .or_else(|| upper.strip_prefix("GMT"))
        .unwrap_or(&upper);
    let rest = rest.trim();
    if rest.is_empty() {
        return Some(0);
    }
    let (sign, body) = if let Some(b) = rest.strip_prefix('+') {
        (1i64, b)
    } else if let Some(b) = rest.strip_prefix('-') {
        (-1i64, b)
    } else if rest.as_bytes().first().is_some_and(|c| c.is_ascii_digit()) {
        (1i64, rest)
    } else {
        return None;
    };
    Some(sign * parse_hm_offset(body)?)
}

/// True when `zone` is a known fixed offset or IANA Time Zone Database name.
pub fn timezone_is_known(zone: &str) -> bool {
    let z = zone.trim();
    if z.is_empty() {
        return false;
    }
    if parse_fixed_zone_offset_secs(z).is_some() {
        return true;
    }
    tzdb::tz_by_name(z).is_some()
}

/// Validate and normalize a `TimeZone` / `timezone` GUC value.
///
/// Fixed offsets keep a compact form (`UTC`, `+03`, `-05:30`). IANA names are
/// trimmed and kept as provided (lookup is case-insensitive).
pub fn normalize_timezone(value: &str) -> Result<String> {
    let z = value.trim().trim_matches('\'').trim_matches('"');
    if z.is_empty() {
        return Err(TakyonicError::Sql(
            "invalid value for parameter \"TimeZone\": must not be empty".into(),
        ));
    }
    if let Some(off) = parse_fixed_zone_offset_secs(z) {
        if off == 0 {
            return Ok("UTC".into());
        }
        let sign = if off >= 0 { '+' } else { '-' };
        let abs = off.unsigned_abs();
        let hh = abs / 3600;
        let mm = (abs % 3600) / 60;
        return Ok(if mm == 0 {
            format!("{sign}{hh}")
        } else {
            format!("{sign}{hh:02}:{mm:02}")
        });
    }
    if tzdb::tz_by_name(z).is_some() {
        return Ok(z.to_string());
    }
    Err(TakyonicError::Sql(format!(
        "invalid value for parameter \"TimeZone\": time zone `{z}` not recognized"
    )))
}

/// Seconds east of UTC for `zone` at the given Unix instant (DST-aware for IANA).
pub fn zone_offset_secs_at(zone: &str, unix_utc: i64) -> Result<i64> {
    if let Some(off) = parse_fixed_zone_offset_secs(zone) {
        return Ok(off);
    }
    let tz = tzdb::tz_by_name(zone).ok_or_else(|| {
        TakyonicError::Sql(format!(
            "unsupported time zone `{zone}` (use UTC/GMT, ±HH[:MM] offset, or an IANA name)"
        ))
    })?;
    let lt = tz.find_local_time_type(unix_utc).map_err(|e| {
        TakyonicError::Sql(format!("time zone `{zone}` lookup failed: {e}"))
    })?;
    Ok(i64::from(lt.ut_offset()))
}

/// Interpret civil wall clock `(y-m-d hh:mm:ss)` as local time in `zone` → Unix UTC.
pub fn local_wall_to_unix(
    y: i32,
    m: u32,
    d: u32,
    hh: u32,
    mm: u32,
    ss: u32,
    zone: &str,
) -> Result<i64> {
    if let Some(off) = parse_fixed_zone_offset_secs(zone) {
        return Ok(timestamp_to_unix(y, m, d, hh, mm, ss) - off);
    }
    let tz = tzdb::tz_by_name(zone).ok_or_else(|| {
        TakyonicError::Sql(format!(
            "unsupported time zone `{zone}` (use UTC/GMT, ±HH[:MM] offset, or an IANA name)"
        ))
    })?;
    let found = tz::DateTime::find(
        y,
        m as u8,
        d as u8,
        hh as u8,
        mm as u8,
        ss as u8,
        0,
        tz,
    )
    .map_err(|e| TakyonicError::Sql(format!("time zone `{zone}` local convert failed: {e}")))?;
    // Prefer unique instant; on DST fold/gap take earliest (PG-ish).
    let dt = found
        .unique()
        .or_else(|| found.earliest())
        .ok_or_else(|| {
            TakyonicError::Sql(format!(
                "time zone `{zone}`: no local time for {y:04}-{m:02}-{d:02} {hh:02}:{mm:02}:{ss:02}"
            ))
        })?;
    Ok(dt.unix_time())
}

/// Evaluate `timestamp AT TIME ZONE zone` (fixed offsets + IANA / DST).
///
/// * timestamp **with** offset → wall clock in `zone` (no offset suffix)
/// * timestamp **without** offset → interpret as local in `zone`, emit UTC (`…+00`)
pub fn at_time_zone(ts: &str, zone: &str) -> Result<String> {
    let (y, m, d, hh, mm, ss) = parse_timestamp_parts(ts).ok_or_else(|| {
        TakyonicError::Sql(format!(
            "AT TIME ZONE source is not a date/timestamp: `{ts}`"
        ))
    })?;
    let wall = timestamp_to_unix(y, m, d, hh, mm, ss);
    if let Some(src_off) = parse_timestamp_offset_secs(ts) {
        let absolute = wall - src_off;
        let zone_off = zone_offset_secs_at(zone, absolute)?;
        let local = absolute + zone_off;
        let with_utc = format_unix_timestamp(local, false);
        Ok(with_utc
            .strip_suffix("+00")
            .unwrap_or(&with_utc)
            .to_string())
    } else {
        let absolute = local_wall_to_unix(y, m, d, hh, mm, ss, zone)?;
        Ok(format_unix_timestamp(absolute, false))
    }
}

/// Day-of-year for Gregorian `y-m-d` (1..=366).
pub fn day_of_year(y: i32, m: u32, d: u32) -> u32 {
    let leap = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
    let month_days = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    let mut n = d;
    for i in 1..m as usize {
        n += month_days[i];
        if i == 2 && leap {
            n += 1;
        }
    }
    n
}

/// Internal marker for encoded INTERVAL values (`__ivl:<seconds>`).
pub const INTERVAL_MARKER: &str = "__ivl:";

/// Encode interval duration as a tagged literal string.
pub fn encode_interval_secs(secs: i64) -> String {
    format!("{INTERVAL_MARKER}{secs}")
}

/// `MAKE_INTERVAL(years, months, weeks, days, hours, mins, secs)` → total seconds.
///
/// Years/months use fixed 365-day / 30-day approximations (same as simple AGE paths).
pub fn make_interval_secs(
    years: i64,
    months: i64,
    weeks: i64,
    days: i64,
    hours: i64,
    mins: i64,
    secs: f64,
) -> i64 {
    years * 365 * 86_400
        + months * 30 * 86_400
        + weeks * 7 * 86_400
        + days * 86_400
        + hours * 3_600
        + mins * 60
        + secs.floor() as i64
}

/// Decode a tagged interval literal; `None` if not an interval encoding.
pub fn decode_interval_secs(s: &str) -> Option<i64> {
    s.strip_prefix(INTERVAL_MARKER)?.parse().ok()
}

/// Resolve an INTERVAL argument from tagged encoding or PG interval text.
pub fn interval_arg_secs(s: &str) -> Result<i64> {
    if let Some(secs) = decode_interval_secs(s) {
        return Ok(secs);
    }
    parse_interval_to_secs(s, None)
}

/// Default `DATE_BIN` origin (`TIMESTAMP '2001-01-01'`).
pub const DATE_BIN_DEFAULT_ORIGIN: &str = "2001-01-01 00:00:00";

/// `DATE_BIN(stride, source [, origin])` — floor `source` onto `stride` grid from `origin`.
pub fn date_bin_text(stride_secs: i64, source: &str, origin: &str) -> Result<String> {
    if stride_secs <= 0 {
        return Err(TakyonicError::Sql(
            "DATE_BIN stride must be a positive interval".into(),
        ));
    }
    let (sy, sm, sd, shh, smi, sss) = parse_timestamp_parts(source).ok_or_else(|| {
        TakyonicError::Sql(format!(
            "DATE_BIN source is not a date/timestamp: `{source}`"
        ))
    })?;
    let (oy, om, od, ohh, omi, oss) = parse_timestamp_parts(origin).ok_or_else(|| {
        TakyonicError::Sql(format!(
            "DATE_BIN origin is not a date/timestamp: `{origin}`"
        ))
    })?;
    let src = timestamp_to_unix(sy, sm, sd, shh, smi, sss);
    let orig = timestamp_to_unix(oy, om, od, ohh, omi, oss);
    let bin = orig + (src - orig).div_euclid(stride_secs) * stride_secs;
    Ok(format_unix_timestamp(bin, false))
}

/// Resolve a period endpoint: second arg is end timestamp or interval length.
pub fn resolve_period_unix(start: &str, end_or_ivl: &str) -> Result<(i64, i64)> {
    let s = {
        let (y, m, d, hh, mm, ss) = parse_timestamp_parts(start).ok_or_else(|| {
            TakyonicError::Sql(format!("OVERLAPS start is not a date/timestamp: `{start}`"))
        })?;
        let mut u = timestamp_to_unix(y, m, d, hh, mm, ss);
        if let Some(off) = parse_timestamp_offset_secs(start) {
            u -= off;
        }
        u
    };
    if let Some(iv) = decode_interval_secs(end_or_ivl) {
        return Ok((s, s + iv));
    }
    if parse_timestamp_parts(end_or_ivl).is_none() {
        if let Ok(iv) = parse_interval_to_secs(end_or_ivl, None) {
            return Ok((s, s + iv));
        }
    }
    let (y, m, d, hh, mm, ss) = parse_timestamp_parts(end_or_ivl).ok_or_else(|| {
        TakyonicError::Sql(format!(
            "OVERLAPS end is not a date/timestamp/interval: `{end_or_ivl}`"
        ))
    })?;
    let mut e = timestamp_to_unix(y, m, d, hh, mm, ss);
    if let Some(off) = parse_timestamp_offset_secs(end_or_ivl) {
        e -= off;
    }
    Ok((s, e))
}

/// SQL `OVERLAPS` — half-open periods after normalizing endpoint order (`S < E`).
pub fn periods_overlap(s1: &str, e1: &str, s2: &str, e2: &str) -> Result<bool> {
    let (mut a, mut b) = resolve_period_unix(s1, e1)?;
    let (mut c, mut d) = resolve_period_unix(s2, e2)?;
    if a > b {
        std::mem::swap(&mut a, &mut b);
    }
    if c > d {
        std::mem::swap(&mut c, &mut d);
    }
    Ok(a < d && c < b)
}

/// `EXTRACT(EPOCH FROM …)` / `DATE_PART('epoch', …)` — Unix seconds as `f64`.
///
/// Accepts timestamps (optional offset → absolute UTC) and intervals (tagged or
/// display text). `±infinity` maps to `±∞`.
pub fn extract_epoch_secs(s: &str) -> Result<f64> {
    let t = s.trim();
    let lower = t.to_ascii_lowercase();
    if lower == "infinity" || lower == "+infinity" {
        return Ok(f64::INFINITY);
    }
    if lower == "-infinity" {
        return Ok(f64::NEG_INFINITY);
    }
    if let Some(secs) = decode_interval_secs(t) {
        return Ok(secs as f64);
    }
    if let Some((y, m, d, hh, mm, ss)) = parse_timestamp_parts(t) {
        let mut unix = timestamp_to_unix(y, m, d, hh, mm, ss);
        if let Some(off) = parse_timestamp_offset_secs(t) {
            unix -= off;
        }
        return Ok(unix as f64);
    }
    if let Ok(secs) = parse_interval_to_secs(t, None) {
        return Ok(secs as f64);
    }
    Err(TakyonicError::Sql(format!(
        "EXTRACT(EPOCH) source is not a date/timestamp/interval: `{s}`"
    )))
}

/// `ISFINITE(timestamp|interval)` — false for ±infinity, true for finite values.
pub fn is_finite_text(s: &str) -> Result<bool> {
    let t = s.trim();
    let lower = t.to_ascii_lowercase();
    if lower == "infinity" || lower == "+infinity" || lower == "-infinity" {
        return Ok(false);
    }
    if decode_interval_secs(t).is_some() {
        return Ok(true);
    }
    if parse_timestamp_parts(t).is_some() {
        return Ok(true);
    }
    // Plain interval display forms from format_interval_secs / INTERVAL literals
    // are stored tagged; reject unrecognized text.
    Err(TakyonicError::Sql(format!(
        "ISFINITE argument is not a timestamp or interval: `{s}`"
    )))
}

/// Human-readable INTERVAL display (Postgres-ish).
///
/// Hours are folded into days; days ≥ 30 are folded into 30-day months (`mon`/`mons`),
/// matching `JUSTIFY_INTERVAL` display semantics.
pub fn format_interval_secs(secs: i64) -> String {
    let neg = secs < 0;
    let mut rem = secs.unsigned_abs();
    let mut days = rem / 86_400;
    rem %= 86_400;
    let months = days / 30;
    days %= 30;
    let hours = rem / 3600;
    rem %= 3600;
    let mins = rem / 60;
    let secs_u = rem % 60;
    let mut parts = Vec::new();
    if months > 0 {
        parts.push(format!(
            "{months} {}",
            if months == 1 { "mon" } else { "mons" }
        ));
    }
    if days > 0 {
        parts.push(format!(
            "{days} {}",
            if days == 1 { "day" } else { "days" }
        ));
    }
    if hours > 0 || mins > 0 || secs_u > 0 || parts.is_empty() {
        parts.push(format!("{hours:02}:{mins:02}:{secs_u:02}"));
    }
    let body = parts.join(" ");
    if neg {
        format!("-{body}")
    } else {
        body
    }
}

/// `JUSTIFY_*` — validate interval and re-encode (display is always justified).
pub fn justify_interval_arg(s: &str) -> Result<String> {
    let secs = interval_arg_secs(s)?;
    Ok(encode_interval_secs(secs))
}

fn datetime_field_secs(field: &DateTimeField) -> Result<i64> {
    match field {
        DateTimeField::Second | DateTimeField::Seconds => Ok(1),
        DateTimeField::Minute | DateTimeField::Minutes => Ok(60),
        DateTimeField::Hour | DateTimeField::Hours => Ok(3_600),
        DateTimeField::Day | DateTimeField::Days => Ok(86_400),
        DateTimeField::Week(_) | DateTimeField::Weeks => Ok(86_400 * 7),
        DateTimeField::Month | DateTimeField::Months => Ok(86_400 * 30),
        DateTimeField::Year | DateTimeField::Years => Ok(86_400 * 365),
        other => Err(TakyonicError::Sql(format!(
            "unsupported INTERVAL unit `{other}`"
        ))),
    }
}

fn parse_hms_to_secs(s: &str) -> Option<i64> {
    let parts: Vec<&str> = s.split(':').collect();
    if parts.len() != 3 {
        return None;
    }
    let h: i64 = parts[0].parse().ok()?;
    let m: i64 = parts[1].parse().ok()?;
    let sec_part = parts[2].split('.').next()?;
    let sec: i64 = sec_part.parse().ok()?;
    if !(0..60).contains(&m) || !(0..60).contains(&sec) {
        return None;
    }
    Some(h * 3600 + m * 60 + sec)
}

/// Parse INTERVAL body text (+ optional leading field) into total seconds.
pub fn parse_interval_to_secs(text: &str, leading: Option<&DateTimeField>) -> Result<i64> {
    let text = text.trim();
    if let Some(field) = leading {
        let n: f64 = text.parse().map_err(|_| {
            TakyonicError::Sql(format!(
                "INTERVAL '{text}' with unit `{field}` is not a number"
            ))
        })?;
        let unit = datetime_field_secs(field)? as f64;
        return Ok((n * unit).round() as i64);
    }
    let lower = text.to_ascii_lowercase();
    if let Some(secs) = parse_hms_to_secs(&lower) {
        return Ok(secs);
    }
    let mut total = 0i64;
    let mut consumed = false;
    let mut tokens = lower.split_whitespace().peekable();
    while let Some(tok) = tokens.next() {
        if let Some(secs) = parse_hms_to_secs(tok) {
            total += secs;
            consumed = true;
            continue;
        }
        let n: f64 = tok.parse().map_err(|_| {
            TakyonicError::Sql(format!(
                "cannot parse INTERVAL '{text}' (unexpected token `{tok}`)"
            ))
        })?;
        let unit = tokens.next().ok_or_else(|| {
            TakyonicError::Sql(format!(
                "INTERVAL '{text}' is missing a unit after `{tok}`"
            ))
        })?;
        let mult = match unit.trim_end_matches('s') {
            "second" | "sec" => 1,
            "minute" | "min" => 60,
            "hour" | "hr" => 3_600,
            "day" => 86_400,
            "week" => 86_400 * 7,
            "month" => 86_400 * 30,
            "year" => 86_400 * 365,
            other => {
                return Err(TakyonicError::Sql(format!(
                    "unsupported INTERVAL unit `{other}` in '{text}'"
                )));
            }
        };
        total += (n * mult as f64).round() as i64;
        consumed = true;
    }
    if !consumed {
        return Err(TakyonicError::Sql(format!(
            "cannot parse empty INTERVAL '{text}'"
        )));
    }
    Ok(total)
}

/// Days since Unix epoch for Gregorian civil date (Howard Hinnant).
pub fn days_from_civil(y: i32, m: u32, d: u32) -> i64 {
    let y = y as i64;
    let m = m as i64;
    let d = d as i64;
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u64;
    let mp = if m > 2 { m - 3 } else { m + 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy as u64;
    (era * 146_097 + doe as i64) - 719_468
}

/// Unix seconds for a UTC civil timestamp.
pub fn timestamp_to_unix(y: i32, m: u32, d: u32, hh: u32, mm: u32, ss: u32) -> i64 {
    days_from_civil(y, m, d) * 86_400 + (hh * 3600 + mm * 60 + ss) as i64
}

/// Format Unix seconds as `YYYY-MM-DD HH:MM:SS+00` (or date-only when time is midnight
/// and `date_only` is true).
pub fn format_unix_timestamp(secs: i64, date_only: bool) -> String {
    let (y, m, d, hh, mm, ss) = civil_from_days(secs);
    if date_only && hh == 0 && mm == 0 && ss == 0 {
        format!("{y:04}-{m:02}-{d:02}")
    } else {
        format!("{y:04}-{m:02}-{d:02} {hh:02}:{mm:02}:{ss:02}+00")
    }
}

/// Add `delta_secs` to a date/timestamp text value.
pub fn add_secs_to_timestamp_text(src: &str, delta_secs: i64) -> Result<String> {
    let (y, m, d, hh, mm, ss) = parse_timestamp_parts(src).ok_or_else(|| {
        TakyonicError::Sql(format!(
            "cannot add INTERVAL to non-timestamp value `{src}`"
        ))
    })?;
    let date_only = src.trim().len() == 10;
    let unix = timestamp_to_unix(y, m, d, hh, mm, ss);
    Ok(format_unix_timestamp(unix + delta_secs, date_only))
}

/// Days in Gregorian month `m` (1..=12) for year `y`.
pub fn days_in_month(y: i32, m: u32) -> u32 {
    let leap = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if leap => 29,
        2 => 28,
        _ => 0,
    }
}

fn as_i32_component(label: &str, n: i64) -> Result<i32> {
    i32::try_from(n).map_err(|_| {
        TakyonicError::Sql(format!("MAKE_* {label} out of range: {n}"))
    })
}

/// `MAKE_DATE(year, month, day)` → `YYYY-MM-DD`.
pub fn make_date_text(year: i64, month: i64, day: i64) -> Result<String> {
    let y = as_i32_component("year", year)?;
    let m = as_i32_component("month", month)?;
    let d = as_i32_component("day", day)?;
    if !(1..=12).contains(&m) {
        return Err(TakyonicError::Sql(format!(
            "MAKE_DATE month out of range: {month}"
        )));
    }
    let dim = days_in_month(y, m as u32);
    if d < 1 || d as u32 > dim {
        return Err(TakyonicError::Sql(format!(
            "MAKE_DATE day out of range: {year}-{month}-{day}"
        )));
    }
    Ok(format!("{y:04}-{m:02}-{d:02}"))
}

/// `MAKE_TIME(hour, minute, second)` → `HH:MM:SS`.
pub fn make_time_text(hour: i64, minute: i64, second: f64) -> Result<String> {
    if !(0..=23).contains(&hour) {
        return Err(TakyonicError::Sql(format!(
            "MAKE_TIME hour out of range: {hour}"
        )));
    }
    if !(0..=59).contains(&minute) {
        return Err(TakyonicError::Sql(format!(
            "MAKE_TIME minute out of range: {minute}"
        )));
    }
    if !(0.0..60.0).contains(&second) {
        return Err(TakyonicError::Sql(format!(
            "MAKE_TIME second out of range: {second}"
        )));
    }
    let ss = second.floor() as u32;
    Ok(format!("{hour:02}:{minute:02}:{ss:02}"))
}

/// `MAKE_TIMESTAMP(y, m, d, h, min, sec)` → `YYYY-MM-DD HH:MM:SS+00`.
pub fn make_timestamp_text(
    year: i64,
    month: i64,
    day: i64,
    hour: i64,
    minute: i64,
    second: f64,
) -> Result<String> {
    let _ = make_date_text(year, month, day)?;
    let _ = make_time_text(hour, minute, second)?;
    let y = as_i32_component("year", year)?;
    let m = as_i32_component("month", month)? as u32;
    let d = as_i32_component("day", day)? as u32;
    let hh = hour as u32;
    let mm = minute as u32;
    let ss = second.floor() as u32;
    Ok(format_unix_timestamp(
        timestamp_to_unix(y, m, d, hh, mm, ss),
        false,
    ))
}

/// Truncate a date/timestamp text to the given field (`year`/`month`/`day`/…).
pub fn date_trunc_text(field: &str, src: &str) -> Result<String> {
    let (y, m, d, hh, mm, ss) = parse_timestamp_parts(src).ok_or_else(|| {
        TakyonicError::Sql(format!(
            "DATE_TRUNC source is not a date/timestamp: `{src}`"
        ))
    })?;
    let field = field.trim().to_ascii_lowercase();
    let (ty, tm, td, th, tmin, ts) = match field.as_str() {
        "year" | "years" => (y, 1, 1, 0, 0, 0),
        "quarter" => {
            let q_month = ((m - 1) / 3) * 3 + 1;
            (y, q_month, 1, 0, 0, 0)
        }
        "month" | "months" => (y, m, 1, 0, 0, 0),
        "week" | "weeks" => {
            // Truncate to Monday 00:00 UTC (ISO-ish).
            let unix = timestamp_to_unix(y, m, d, 0, 0, 0);
            let days = unix.div_euclid(86_400);
            // Unix epoch 1970-01-01 was Thursday; weekday Mon=0..Sun=6
            let weekday = (days + 3).rem_euclid(7); // 0=Mon … 6=Sun
            let monday = unix - weekday * 86_400;
            let (wy, wm, wd, _, _, _) = civil_from_days(monday);
            (wy, wm, wd, 0, 0, 0)
        }
        "day" | "days" => (y, m, d, 0, 0, 0),
        "hour" | "hours" => (y, m, d, hh, 0, 0),
        "minute" | "minutes" => (y, m, d, hh, mm, 0),
        "second" | "seconds" => (y, m, d, hh, mm, ss),
        other => {
            return Err(TakyonicError::Sql(format!(
                "unsupported DATE_TRUNC field `{other}` \
                 (year/quarter/month/week/day/hour/minute/second)"
            )));
        }
    };
    Ok(format_unix_timestamp(
        timestamp_to_unix(ty, tm, td, th, tmin, ts),
        false,
    ))
}

/// Seconds from `earlier` to `later` (AGE-style difference).
pub fn age_secs(later: &str, earlier: &str) -> Result<i64> {
    let (y1, m1, d1, hh1, mm1, ss1) = parse_timestamp_parts(later).ok_or_else(|| {
        TakyonicError::Sql(format!("AGE argument is not a date/timestamp: `{later}`"))
    })?;
    let (y0, m0, d0, hh0, mm0, ss0) = parse_timestamp_parts(earlier).ok_or_else(|| {
        TakyonicError::Sql(format!("AGE argument is not a date/timestamp: `{earlier}`"))
    })?;
    Ok(timestamp_to_unix(y1, m1, d1, hh1, mm1, ss1)
        - timestamp_to_unix(y0, m0, d0, hh0, mm0, ss0))
}

const TO_CHAR_TOKENS: &[&str] = &[
    "YYYY", "HH24", "HH12", "HH", "MM", "DD", "MI", "SS", "YY",
];

fn next_format_piece(fmt: &str) -> Result<(&str, &str)> {
    if fmt.is_empty() {
        return Ok(("", ""));
    }
    if fmt.starts_with('"') {
        let rest = &fmt[1..];
        let end = rest.find('"').ok_or_else(|| {
            TakyonicError::Sql("TO_CHAR/TO_TIMESTAMP format has unclosed quoted literal".into())
        })?;
        return Ok((&rest[..end], &rest[end + 1..]));
    }
    for tok in TO_CHAR_TOKENS {
        if fmt.len() >= tok.len() && fmt[..tok.len()].eq_ignore_ascii_case(tok) {
            return Ok((tok, &fmt[tok.len()..]));
        }
    }
    // Single literal character (separator / punctuation / space).
    let mut chars = fmt.chars();
    let ch = chars.next().unwrap();
    Ok((&fmt[..ch.len_utf8()], chars.as_str()))
}

/// Parse next `TO_NUMBER` format piece (`FM`, `MI`, `9`, `0`, `D`/`.`, `G`/`,`, `S`, literal).
fn next_number_format_piece(fmt: &str) -> (&str, &str) {
    if fmt.is_empty() {
        return ("", "");
    }
    if fmt.len() >= 2 && fmt[..2].eq_ignore_ascii_case("FM") {
        return ("FM", &fmt[2..]);
    }
    if fmt.len() >= 2 && fmt[..2].eq_ignore_ascii_case("MI") {
        return ("MI", &fmt[2..]);
    }
    let ch = fmt.chars().next().unwrap();
    (&fmt[..ch.len_utf8()], &fmt[ch.len_utf8()..])
}

/// `TO_NUMBER(text, format)` — subset of PG numeric templates (`9`/`0`/`D`/`G`/`S`/`FM`/`.`,`).
pub fn to_number_text(src: &str, fmt: &str) -> Result<f64> {
    let mut input = src.trim();
    let mut rest = fmt;
    let mut fill = false;
    let mut sign = 1.0_f64;
    let mut buf = String::new();
    let mut seen_dot = false;
    let mut sign_from_template = false;

    // Optional leading sign even without `S` in the template (PG accepts `-12` / `99`).
    if let Some(stripped) = input.strip_prefix('-') {
        sign = -1.0;
        input = stripped.trim_start();
    } else if let Some(stripped) = input.strip_prefix('+') {
        input = stripped.trim_start();
    }

    while !rest.is_empty() {
        let (piece, next) = next_number_format_piece(rest);
        rest = next;
        let upper = piece.to_ascii_uppercase();
        match upper.as_str() {
            "FM" => fill = true,
            "9" | "0" => {
                if fill || upper == "9" {
                    input = input.trim_start_matches(' ');
                }
                if let Some(ch) = input.chars().next() {
                    if ch.is_ascii_digit() {
                        buf.push(ch);
                        input = &input[ch.len_utf8()..];
                    } else if upper == "0" {
                        return Err(TakyonicError::Sql(format!(
                            "TO_NUMBER expected digit for `0` in `{src}` (format `{fmt}`)"
                        )));
                    }
                } else if upper == "0" {
                    return Err(TakyonicError::Sql(format!(
                        "TO_NUMBER expected digit for `0` in `{src}` (format `{fmt}`)"
                    )));
                }
            }
            "D" | "." => {
                if seen_dot {
                    return Err(TakyonicError::Sql(
                        "TO_NUMBER format has multiple decimal markers".into(),
                    ));
                }
                seen_dot = true;
                if input.starts_with('.') || input.starts_with(',') {
                    input = &input[1..];
                }
                buf.push('.');
            }
            "G" | "," => {
                if input.starts_with(',')
                    || input.starts_with('.')
                    || input.starts_with(' ')
                    || input.starts_with('_')
                {
                    input = &input[1..];
                }
            }
            "S" | "MI" => {
                sign_from_template = true;
                input = input.trim_start_matches(' ');
                if input.starts_with('-') {
                    sign = -1.0;
                    input = &input[1..];
                } else if input.starts_with('+') {
                    input = &input[1..];
                } else if input.ends_with('-') {
                    sign = -1.0;
                    input = input[..input.len() - 1].trim_end();
                } else if input.ends_with('+') {
                    input = input[..input.len() - 1].trim_end();
                }
            }
            _ => {
                if piece.chars().all(|c| c.is_whitespace()) {
                    input = input.trim_start_matches(|c: char| c.is_whitespace());
                    continue;
                }
                if !input.starts_with(piece) {
                    return Err(TakyonicError::Sql(format!(
                        "TO_NUMBER expected `{piece}` in `{src}` (format `{fmt}`)"
                    )));
                }
                input = &input[piece.len()..];
            }
        }
    }

    let leftover = input.trim();
    if !leftover.is_empty() {
        if leftover == "-" && !sign_from_template {
            sign = -1.0;
        } else if leftover != "+"
            && leftover.chars().any(|c| c.is_ascii_digit() || c == '.')
        {
            return Err(TakyonicError::Sql(format!(
                "TO_NUMBER leftover input `{leftover}` for `{src}` (format `{fmt}`)"
            )));
        } else if leftover == "-" {
            sign = -1.0;
        }
    }

    let body = buf.trim_end_matches('.').trim_start_matches('.');
    let n = if body.is_empty() {
        0.0
    } else {
        body.parse::<f64>().map_err(|_| {
            TakyonicError::Sql(format!(
                "TO_NUMBER could not parse `{src}` with format `{fmt}`"
            ))
        })?
    };
    Ok(n * sign)
}

/// Format a date/timestamp with a Postgres-ish template (`YYYY-MM-DD HH24:MI:SS`).
pub fn to_char_timestamp(src: &str, fmt: &str) -> Result<String> {
    let (y, m, d, hh, mm, ss) = parse_timestamp_parts(src).ok_or_else(|| {
        TakyonicError::Sql(format!(
            "TO_CHAR source is not a date/timestamp: `{src}`"
        ))
    })?;
    let mut out = String::new();
    let mut rest = fmt;
    while !rest.is_empty() {
        let (piece, next) = next_format_piece(rest)?;
        rest = next;
        let upper = piece.to_ascii_uppercase();
        match upper.as_str() {
            "YYYY" => out.push_str(&format!("{y:04}")),
            "YY" => out.push_str(&format!("{:02}", y.rem_euclid(100))),
            "MM" => out.push_str(&format!("{m:02}")),
            "DD" => out.push_str(&format!("{d:02}")),
            "HH24" | "HH" => out.push_str(&format!("{hh:02}")),
            "HH12" => {
                let h12 = match hh % 12 {
                    0 => 12,
                    n => n,
                };
                out.push_str(&format!("{h12:02}"));
            }
            "MI" => out.push_str(&format!("{mm:02}")),
            "SS" => out.push_str(&format!("{ss:02}")),
            _ => out.push_str(piece),
        }
    }
    Ok(out)
}

/// Parse text with a format template into `YYYY-MM-DD HH:MM:SS+00`.
pub fn to_timestamp_text(src: &str, fmt: &str) -> Result<String> {
    let mut y: i32 = 1970;
    let mut m: u32 = 1;
    let mut d: u32 = 1;
    let mut hh: u32 = 0;
    let mut mi: u32 = 0;
    let mut ss: u32 = 0;
    let mut input = src.trim();
    let mut rest = fmt;
    while !rest.is_empty() {
        let (piece, next) = next_format_piece(rest)?;
        rest = next;
        let upper = piece.to_ascii_uppercase();
        match upper.as_str() {
            "YYYY" => {
                let (rem, v) = take_format_digits(input, 4, src, fmt)?;
                y = v as i32;
                input = rem;
            }
            "YY" => {
                let (rem, v) = take_format_digits(input, 2, src, fmt)?;
                y = 2000 + v as i32;
                input = rem;
            }
            "MM" => {
                let (rem, v) = take_format_digits(input, 2, src, fmt)?;
                m = v;
                input = rem;
            }
            "DD" => {
                let (rem, v) = take_format_digits(input, 2, src, fmt)?;
                d = v;
                input = rem;
            }
            "HH24" | "HH" | "HH12" => {
                let (rem, v) = take_format_digits(input, 2, src, fmt)?;
                hh = v;
                input = rem;
            }
            "MI" => {
                let (rem, v) = take_format_digits(input, 2, src, fmt)?;
                mi = v;
                input = rem;
            }
            "SS" => {
                let (rem, v) = take_format_digits(input, 2, src, fmt)?;
                ss = v;
                input = rem;
            }
            _ => {
                if !input.starts_with(piece) {
                    return Err(TakyonicError::Sql(format!(
                        "TO_TIMESTAMP expected `{piece}` in `{src}` (format `{fmt}`)"
                    )));
                }
                input = &input[piece.len()..];
            }
        }
    }
    if !(1..=12).contains(&m) || !(1..=31).contains(&d) || hh > 23 || mi > 59 || ss > 59 {
        return Err(TakyonicError::Sql(format!(
            "TO_TIMESTAMP produced out-of-range timestamp from `{src}`"
        )));
    }
    Ok(format_unix_timestamp(
        timestamp_to_unix(y, m, d, hh, mi, ss),
        false,
    ))
}

/// Parse text with a format template into `YYYY-MM-DD` (date only).
pub fn to_date_text(src: &str, fmt: &str) -> Result<String> {
    let ts = to_timestamp_text(src, fmt)?;
    let (y, m, d, _, _, _) = parse_timestamp_parts(&ts).ok_or_else(|| {
        TakyonicError::Sql(format!(
            "TO_DATE could not parse `{src}` with format `{fmt}`"
        ))
    })?;
    make_date_text(y as i64, m as i64, d as i64)
}

fn take_format_digits<'a>(
    input: &'a str,
    n: usize,
    src: &str,
    fmt: &str,
) -> Result<(&'a str, u32)> {
    if input.len() < n || !input[..n].bytes().all(|b| b.is_ascii_digit()) {
        return Err(TakyonicError::Sql(format!(
            "TO_TIMESTAMP could not parse `{src}` with format `{fmt}`"
        )));
    }
    let v: u32 = input[..n].parse().map_err(|_| {
        TakyonicError::Sql(format!(
            "TO_TIMESTAMP could not parse `{src}` with format `{fmt}`"
        ))
    })?;
    Ok((&input[n..], v))
}

fn expr_ident(expr: &Expr) -> Result<String> {
    match expr {
        Expr::Identifier(ident) => Ok(ident.value.clone()),
        Expr::CompoundIdentifier(parts) => parts
            .last()
            .map(|i| i.value.clone())
            .ok_or_else(|| TakyonicError::Sql("empty compound identifier".into())),
        other => Err(TakyonicError::Sql(format!(
            "expected column identifier, got {other}"
        ))),
    }
}

fn expr_literal(expr: &Expr) -> Result<String> {
    match expr {
        Expr::Value(ValueWithSpan { value, .. }) => sql_value_to_string(value),
        Expr::Identifier(ident) => Ok(ident.value.clone()),
        other => Err(TakyonicError::Sql(format!(
            "expected literal value, got {other}"
        ))),
    }
}

fn sql_value_to_string(v: &SqlValue) -> Result<String> {
    match v {
        SqlValue::Number(n, _) => Ok(n.clone()),
        SqlValue::SingleQuotedString(s)
        | SqlValue::DoubleQuotedString(s)
        | SqlValue::TripleSingleQuotedString(s)
        | SqlValue::TripleDoubleQuotedString(s) => Ok(s.clone()),
        SqlValue::Boolean(b) => Ok(b.to_string()),
        SqlValue::Null => Ok(String::new()),
        other => Err(TakyonicError::Sql(format!(
            "unsupported SQL literal: {other}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_select_and_filters() {
        let plan =
            LogicalPlanner::plan("SELECT * FROM users WHERE status = 'active' AND city = 'Ankara'")
                .unwrap();
        match plan {
            LogicalPlan::Select {
                table,
                filters,
                predicate,
            } => {
                assert_eq!(table, "users");
                assert_eq!(filters.len(), 2);
                assert_eq!(filters[0].column, "status");
                assert_eq!(filters[0].op, FilterOp::Eq);
                assert_eq!(filters[0].value, "active");
                assert_eq!(filters[1].column, "city");
                assert_eq!(filters[1].value, "Ankara");
                assert!(predicate.is_some());
            }
            other => panic!("expected Select, got {other:?}"),
        }
    }

    #[test]
    fn parses_parameterized_where() {
        let plan = LogicalPlanner::plan("SELECT * FROM users WHERE age > $1").unwrap();
        match plan {
            LogicalPlan::Select {
                table,
                filters,
                predicate,
            } => {
                assert_eq!(table, "users");
                assert!(filters.is_empty()); // $1 is not a literal CBO filter
                match predicate {
                    Some(Expression::BinaryOp {
                        left,
                        op: FilterOp::Gt,
                        right,
                    }) => {
                        assert_eq!(*left, Expression::Column("age".into()));
                        assert_eq!(*right, Expression::Parameter(0));
                    }
                    other => panic!("expected age > $1, got {other:?}"),
                }
            }
            other => panic!("expected Select, got {other:?}"),
        }
    }

    #[test]
    fn parses_insert_values() {
        let plan =
            LogicalPlanner::plan("INSERT INTO users (id, name, city) VALUES (1, 'Ada', 'Bursa')")
                .unwrap();
        match plan {
            LogicalPlan::Insert {
                table,
                columns,
                values,
                query: None,
                returning: _,
                on_conflict: _,
            } => {
                assert_eq!(table, "users");
                assert_eq!(columns, vec!["id", "name", "city"]);
                assert_eq!(values.len(), 1);
                assert_eq!(values[0][0], Expression::Literal("1".into()));
                assert_eq!(values[0][1], Expression::Literal("Ada".into()));
                assert_eq!(values[0][2], Expression::Literal("Bursa".into()));
            }
            other => panic!("expected Insert, got {other:?}"),
        }
    }

    #[test]
    fn parses_insert_select() {
        match LogicalPlanner::plan(
            "INSERT INTO dest (id, name) SELECT id, name FROM users WHERE age > 20",
        )
        .unwrap()
        {
            LogicalPlan::Insert {
                table,
                columns,
                values,
                query: Some(q),
                on_conflict: None,
                returning: None,
            } => {
                assert_eq!(table, "dest");
                assert_eq!(columns, vec!["id", "name"]);
                assert!(values.is_empty());
                assert!(matches!(
                    q.as_ref(),
                    LogicalPlan::Project { .. } | LogicalPlan::Filter { .. }
                ));
            }
            other => panic!("expected Insert SELECT, got {other:?}"),
        }
        match LogicalPlanner::plan(
            "INSERT INTO dest (id, name) SELECT id, name FROM users \
             ON CONFLICT DO NOTHING RETURNING id",
        )
        .unwrap()
        {
            LogicalPlan::Insert {
                query: Some(_),
                on_conflict: Some(OnConflict::DoNothing),
                returning: Some(Returning::List(_)),
                ..
            } => {}
            other => panic!("expected Insert SELECT ON CONFLICT RETURNING, got {other:?}"),
        }
    }

    #[test]
    fn parses_insert_returning() {
        let plan = LogicalPlanner::plan(
            "INSERT INTO users (id, name) VALUES (1, 'Ada') RETURNING id, name AS n",
        )
        .unwrap();
        match plan {
            LogicalPlan::Insert {
                returning: Some(Returning::List(cols)),
                ..
            } => {
                assert_eq!(cols.len(), 2);
                assert_eq!(cols[0].0, "id");
                assert_eq!(cols[0].1, Expression::Column("id".into()));
                assert_eq!(cols[1].0, "n");
                assert_eq!(cols[1].1, Expression::Column("name".into()));
            }
            other => panic!("expected Insert RETURNING list, got {other:?}"),
        }
        match LogicalPlanner::plan("INSERT INTO users (id) VALUES (1) RETURNING *").unwrap() {
            LogicalPlan::Insert {
                returning: Some(Returning::Star),
                ..
            } => {}
            other => panic!("expected Insert RETURNING *, got {other:?}"),
        }
    }

    #[test]
    fn parses_insert_on_conflict_do_nothing() {
        match LogicalPlanner::plan(
            "INSERT INTO users (id, name) VALUES (1, 'Ada') ON CONFLICT DO NOTHING",
        )
        .unwrap()
        {
            LogicalPlan::Insert {
                on_conflict: Some(OnConflict::DoNothing),
                returning: None,
                ..
            } => {}
            other => panic!("expected ON CONFLICT DO NOTHING, got {other:?}"),
        }
        match LogicalPlanner::plan(
            "INSERT INTO users (id, name) VALUES (1, 'Ada') ON CONFLICT (id) DO NOTHING \
             RETURNING id",
        )
        .unwrap()
        {
            LogicalPlan::Insert {
                on_conflict: Some(OnConflict::DoNothing),
                returning: Some(Returning::List(_)),
                ..
            } => {}
            other => panic!("expected ON CONFLICT (id) DO NOTHING RETURNING, got {other:?}"),
        }
        assert!(LogicalPlanner::plan(
            "INSERT INTO users (id, name) VALUES (1, 'Ada') \
             ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name"
        )
        .is_ok());
    }

    #[test]
    fn parses_insert_on_conflict_do_update() {
        match LogicalPlanner::plan(
            "INSERT INTO users (id, name, age) VALUES (1, 'Ada', 31) \
             ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, age = EXCLUDED.age",
        )
        .unwrap()
        {
            LogicalPlan::Insert {
                on_conflict:
                    Some(OnConflict::DoUpdate {
                        assignments,
                        selection: None,
                    }),
                ..
            } => {
                assert_eq!(assignments.len(), 2);
                assert_eq!(assignments[0].0, "name");
                assert_eq!(
                    assignments[0].1,
                    Expression::Column(format!("{}name", EXCLUDED_FIELD_PREFIX))
                );
                assert_eq!(assignments[1].0, "age");
            }
            other => panic!("expected DO UPDATE, got {other:?}"),
        }
        match LogicalPlanner::plan(
            "INSERT INTO users (id, name, age) VALUES (1, 'Ada', 31) \
             ON CONFLICT DO UPDATE SET age = 99 WHERE users.age < 50",
        )
        .unwrap()
        {
            LogicalPlan::Insert {
                on_conflict:
                    Some(OnConflict::DoUpdate {
                        selection: Some(_),
                        ..
                    }),
                ..
            } => {}
            other => panic!("expected DO UPDATE WHERE, got {other:?}"),
        }
    }

    #[test]
    fn parses_update_set_where() {
        let plan =
            LogicalPlanner::plan("UPDATE users SET age = 31 WHERE name = 'Ada'").unwrap();
        match plan {
            LogicalPlan::Update {
                table,
                assignments,
                selection,
                returning: _,
            } => {
                assert_eq!(table, "users");
                assert_eq!(
                    assignments.get("age"),
                    Some(&Expression::Literal("31".into()))
                );
                match selection {
                    Some(Expression::BinaryOp {
                        left,
                        op: FilterOp::Eq,
                        right,
                    }) => {
                        assert_eq!(*left, Expression::Column("name".into()));
                        assert_eq!(*right, Expression::Literal("Ada".into()));
                    }
                    other => panic!("expected name = Ada, got {other:?}"),
                }
            }
            other => panic!("expected Update, got {other:?}"),
        }
    }

    #[test]
    fn parses_update_returning() {
        let plan = LogicalPlanner::plan(
            "UPDATE users SET age = 32 WHERE id = 1 RETURNING id, age",
        )
        .unwrap();
        match plan {
            LogicalPlan::Update {
                returning: Some(Returning::List(cols)),
                ..
            } => {
                assert_eq!(cols.len(), 2);
                assert_eq!(cols[0].0, "id");
                assert_eq!(cols[1].0, "age");
            }
            other => panic!("expected Update RETURNING, got {other:?}"),
        }
    }

    #[test]
    fn parses_delete_where() {
        let plan = LogicalPlanner::plan("DELETE FROM users WHERE age < 25").unwrap();
        match plan {
            LogicalPlan::Delete {
                table,
                selection,
                returning: _,
            } => {
                assert_eq!(table, "users");
                match selection {
                    Some(Expression::BinaryOp {
                        left,
                        op: FilterOp::Lt,
                        right,
                    }) => {
                        assert_eq!(*left, Expression::Column("age".into()));
                        assert_eq!(*right, Expression::Literal("25".into()));
                    }
                    other => panic!("expected age < 25, got {other:?}"),
                }
            }
            other => panic!("expected Delete, got {other:?}"),
        }
    }

    #[test]
    fn parses_delete_returning() {
        let plan =
            LogicalPlanner::plan("DELETE FROM users WHERE id = 1 RETURNING id, name").unwrap();
        match plan {
            LogicalPlan::Delete {
                returning: Some(Returning::List(cols)),
                ..
            } => {
                assert_eq!(cols.len(), 2);
                assert_eq!(cols[0].0, "id");
                assert_eq!(cols[1].0, "name");
            }
            other => panic!("expected Delete RETURNING, got {other:?}"),
        }
    }

    #[test]
    fn parses_truncate_table() {
        match LogicalPlanner::plan("TRUNCATE TABLE users").unwrap() {
            LogicalPlan::Truncate {
                table,
                if_exists: false,
            } => assert_eq!(table, "users"),
            other => panic!("expected Truncate, got {other:?}"),
        }
        match LogicalPlanner::plan("TRUNCATE TABLE IF EXISTS ghost").unwrap() {
            LogicalPlan::Truncate {
                table,
                if_exists: true,
            } => assert_eq!(table, "ghost"),
            other => panic!("expected Truncate IF EXISTS, got {other:?}"),
        }
        assert!(
            LogicalPlanner::plan("TRUNCATE TABLE a, b")
                .unwrap_err()
                .to_string()
                .contains("exactly one table")
        );
    }

    #[test]
    fn parses_inner_join_on() {
        let plan = LogicalPlanner::plan(
            "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id",
        )
        .unwrap();
        match plan {
            LogicalPlan::Join {
                left,
                right,
                on,
                join_type,
            } => {
                assert_eq!(join_type, JoinType::Inner);
                match left.as_ref() {
                    LogicalPlan::Select { table, filters, .. } => {
                        assert_eq!(table, "users");
                        assert!(filters.is_empty());
                    }
                    other => panic!("expected left Select, got {other:?}"),
                }
                match right.as_ref() {
                    LogicalPlan::Select { table, .. } => assert_eq!(table, "orders"),
                    other => panic!("expected right Select, got {other:?}"),
                }
                match on {
                    Expression::BinaryOp {
                        left,
                        op: FilterOp::Eq,
                        right,
                    } => {
                        assert_eq!(*left, Expression::Column("id".into()));
                        assert_eq!(*right, Expression::Column("user_id".into()));
                    }
                    other => panic!("expected Eq ON, got {other:?}"),
                }
            }
            other => panic!("expected Join, got {other:?}"),
        }
    }

    #[test]
    fn parses_transaction_control() {
        assert_eq!(LogicalPlanner::plan("BEGIN").unwrap(), LogicalPlan::Begin);
        assert_eq!(
            LogicalPlanner::plan("BEGIN TRANSACTION").unwrap(),
            LogicalPlan::Begin
        );
        assert_eq!(LogicalPlanner::plan("COMMIT").unwrap(), LogicalPlan::Commit);
        assert_eq!(
            LogicalPlanner::plan("ROLLBACK").unwrap(),
            LogicalPlan::Rollback
        );
    }

    #[test]
    fn parses_group_by_aggregates() {
        let plan = LogicalPlanner::plan(
            "SELECT department, COUNT(id), SUM(salary) FROM employees GROUP BY department",
        )
        .unwrap();
        match plan {
            LogicalPlan::Aggregate {
                input,
                group_exprs,
                aggr_exprs,
            } => {
                match input.as_ref() {
                    LogicalPlan::Select { table, .. } => assert_eq!(table, "employees"),
                    other => panic!("expected Select input, got {other:?}"),
                }
                assert_eq!(group_exprs, vec![Expression::Column("department".into())]);
                assert_eq!(
                    aggr_exprs[0],
                    Expression::AggregateFunction {
                        name: "COUNT".into(),
                        args: vec![Expression::Column("id".into())],
                                            filter: None,
                                            distinct: false,
                                            order_by: vec![],
                    }
                );
                assert_eq!(
                    aggr_exprs[1],
                    Expression::AggregateFunction {
                        name: "SUM".into(),
                        args: vec![Expression::Column("salary".into())],
                                            filter: None,
                                            distinct: false,
                                            order_by: vec![],
                    }
                );
            }
            other => panic!("expected Aggregate, got {other:?}"),
        }

        let json_agg = LogicalPlanner::plan(
            "SELECT department, json_agg(name) FROM employees GROUP BY department",
        )
        .unwrap();
        match json_agg {
            LogicalPlan::Aggregate { aggr_exprs, .. } => {
                assert_eq!(
                    aggr_exprs[0],
                    Expression::AggregateFunction {
                        name: "JSON_AGG".into(),
                        args: vec![Expression::Column("name".into())],
                                            filter: None,
                                            distinct: false,
                                            order_by: vec![],
                    }
                );
            }
            other => panic!("expected Aggregate for json_agg, got {other:?}"),
        }
        assert!(LogicalPlanner::plan("SELECT json_agg() FROM employees").is_err());

        let obj_agg = LogicalPlanner::plan(
            "SELECT department, json_object_agg(name, id) FROM employees GROUP BY department",
        )
        .unwrap();
        match obj_agg {
            LogicalPlan::Aggregate { aggr_exprs, .. } => {
                assert_eq!(
                    aggr_exprs[0],
                    Expression::AggregateFunction {
                        name: "JSON_OBJECT_AGG".into(),
                        args: vec![
                            Expression::Column("name".into()),
                            Expression::Column("id".into()),
                        ],
                                            filter: None,
                                            distinct: false,
                                            order_by: vec![],
                    }
                );
            }
            other => panic!("expected Aggregate for json_object_agg, got {other:?}"),
        }
        assert!(LogicalPlanner::plan(
            "SELECT json_object_agg(name) FROM employees"
        )
        .is_err());

        let string_agg = LogicalPlanner::plan(
            "SELECT department, string_agg(name, ',') FROM employees GROUP BY department",
        )
        .unwrap();
        match string_agg {
            LogicalPlan::Aggregate { aggr_exprs, .. } => {
                assert_eq!(
                    aggr_exprs[0],
                    Expression::AggregateFunction {
                        name: "STRING_AGG".into(),
                        args: vec![
                            Expression::Column("name".into()),
                            Expression::Literal(",".into()),
                        ],
                                            filter: None,
                                            distinct: false,
                                            order_by: vec![],
                    }
                );
            }
            other => panic!("expected Aggregate for string_agg, got {other:?}"),
        }
        assert!(LogicalPlanner::plan("SELECT string_agg(name) FROM employees").is_err());

        let array_agg = LogicalPlanner::plan(
            "SELECT department, array_agg(name) FROM employees GROUP BY department",
        )
        .unwrap();
        match array_agg {
            LogicalPlan::Aggregate { aggr_exprs, .. } => {
                assert_eq!(
                    aggr_exprs[0],
                    Expression::AggregateFunction {
                        name: "ARRAY_AGG".into(),
                        args: vec![Expression::Column("name".into())],
                                            filter: None,
                                            distinct: false,
                                            order_by: vec![],
                    }
                );
            }
            other => panic!("expected Aggregate for array_agg, got {other:?}"),
        }
        assert!(LogicalPlanner::plan("SELECT array_agg() FROM employees").is_err());

        let bool_and = LogicalPlanner::plan(
            "SELECT department, bool_and(active) FROM employees GROUP BY department",
        )
        .unwrap();
        match bool_and {
            LogicalPlan::Aggregate { aggr_exprs, .. } => {
                assert_eq!(
                    aggr_exprs[0],
                    Expression::AggregateFunction {
                        name: "BOOL_AND".into(),
                        args: vec![Expression::Column("active".into())],
                                            filter: None,
                                            distinct: false,
                                            order_by: vec![],
                    }
                );
            }
            other => panic!("expected Aggregate for bool_and, got {other:?}"),
        }
        let every = LogicalPlanner::plan("SELECT every(active) FROM employees").unwrap();
        match every {
            LogicalPlan::Aggregate { aggr_exprs, .. } => {
                assert!(matches!(
                    &aggr_exprs[0],
                    Expression::AggregateFunction { name, .. } if name == "BOOL_AND"
                ));
            }
            other => panic!("expected Aggregate for every, got {other:?}"),
        }
        assert!(LogicalPlanner::plan("SELECT bool_or() FROM employees").is_err());

        let bit_and = LogicalPlanner::plan(
            "SELECT department, bit_and(flags) FROM employees GROUP BY department",
        )
        .unwrap();
        match bit_and {
            LogicalPlan::Aggregate { aggr_exprs, .. } => {
                assert_eq!(
                    aggr_exprs[0],
                    Expression::AggregateFunction {
                        name: "BIT_AND".into(),
                        args: vec![Expression::Column("flags".into())],
                                            filter: None,
                                            distinct: false,
                                            order_by: vec![],
                    }
                );
            }
            other => panic!("expected Aggregate for bit_and, got {other:?}"),
        }
        assert!(LogicalPlanner::plan("SELECT bit_or() FROM employees").is_err());

        let filtered = LogicalPlanner::plan(
            "SELECT COUNT(*) FILTER (WHERE salary > 100) FROM employees",
        )
        .unwrap();
        match filtered {
            LogicalPlan::Aggregate { aggr_exprs, .. } => {
                match &aggr_exprs[0] {
                    Expression::AggregateFunction {
                        name,
                        args,
                        filter,
                        distinct,
                        ..
                    } => {
                        assert_eq!(name, "COUNT");
                        assert!(args.is_empty());
                        assert!(filter.is_some());
                        assert!(!*distinct);
                    }
                    other => panic!("expected filtered COUNT, got {other:?}"),
                }
                assert_eq!(
                    crate::sql::aggregate_result_column(&aggr_exprs[0]).as_deref(),
                    Some("count(*) filter")
                );
            }
            other => panic!("expected Aggregate for FILTER, got {other:?}"),
        }

        let distinct_count = LogicalPlanner::plan(
            "SELECT COUNT(DISTINCT department) FROM employees",
        )
        .unwrap();
        match distinct_count {
            LogicalPlan::Aggregate { aggr_exprs, .. } => {
                match &aggr_exprs[0] {
                    Expression::AggregateFunction {
                        name,
                        args,
                        distinct,
                        filter,
                        ..
                    } => {
                        assert_eq!(name, "COUNT");
                        assert!(*distinct);
                        assert!(filter.is_none());
                        assert_eq!(args, &vec![Expression::Column("department".into())]);
                    }
                    other => panic!("expected COUNT DISTINCT, got {other:?}"),
                }
                assert_eq!(
                    crate::sql::aggregate_result_column(&aggr_exprs[0]).as_deref(),
                    Some("count(distinct department)")
                );
            }
            other => panic!("expected Aggregate for DISTINCT, got {other:?}"),
        }
        assert!(LogicalPlanner::plan("SELECT COUNT(DISTINCT *) FROM employees").is_err());

        let stddev = LogicalPlanner::plan("SELECT stddev(salary), variance(salary) FROM employees")
            .unwrap();
        match stddev {
            LogicalPlan::Aggregate { aggr_exprs, .. } => {
                assert!(matches!(
                    &aggr_exprs[0],
                    Expression::AggregateFunction { name, .. } if name == "STDDEV_SAMP"
                ));
                assert!(matches!(
                    &aggr_exprs[1],
                    Expression::AggregateFunction { name, .. } if name == "VAR_SAMP"
                ));
            }
            other => panic!("expected Aggregate for stddev, got {other:?}"),
        }

        let corr = LogicalPlanner::plan("SELECT corr(y, x), covar_pop(y, x) FROM pts").unwrap();
        match corr {
            LogicalPlan::Aggregate { aggr_exprs, .. } => {
                assert!(matches!(
                    &aggr_exprs[0],
                    Expression::AggregateFunction { name, args, .. }
                        if name == "CORR" && args.len() == 2
                ));
                assert!(matches!(
                    &aggr_exprs[1],
                    Expression::AggregateFunction { name, .. } if name == "COVAR_POP"
                ));
            }
            other => panic!("expected Aggregate for corr, got {other:?}"),
        }
        assert!(LogicalPlanner::plan("SELECT corr(x) FROM pts").is_err());

        let regr = LogicalPlanner::plan(
            "SELECT regr_slope(y, x), regr_intercept(y, x), regr_r2(y, x) FROM pts",
        )
        .unwrap();
        match regr {
            LogicalPlan::Aggregate { aggr_exprs, .. } => {
                assert!(matches!(
                    &aggr_exprs[0],
                    Expression::AggregateFunction { name, .. } if name == "REGR_SLOPE"
                ));
                assert!(matches!(
                    &aggr_exprs[1],
                    Expression::AggregateFunction { name, .. } if name == "REGR_INTERCEPT"
                ));
                assert!(matches!(
                    &aggr_exprs[2],
                    Expression::AggregateFunction { name, .. } if name == "REGR_R2"
                ));
            }
            other => panic!("expected Aggregate for regr_*, got {other:?}"),
        }

        let mode_simple = LogicalPlanner::plan("SELECT mode(color) FROM items").unwrap();
        match mode_simple {
            LogicalPlan::Aggregate { aggr_exprs, .. } => {
                assert!(matches!(
                    &aggr_exprs[0],
                    Expression::AggregateFunction {
                        name,
                        args,
                        order_by,
                        ..
                    } if name == "MODE"
                        && args.len() == 1
                        && order_by.is_empty()
                ));
            }
            other => panic!("expected Aggregate for mode(color), got {other:?}"),
        }
        let mode_wg = LogicalPlanner::plan(
            "SELECT mode() WITHIN GROUP (ORDER BY color DESC) FROM items",
        )
        .unwrap();
        match mode_wg {
            LogicalPlan::Aggregate { aggr_exprs, .. } => match &aggr_exprs[0] {
                Expression::AggregateFunction {
                    name,
                    args,
                    order_by,
                    ..
                } => {
                    assert_eq!(name, "MODE");
                    assert_eq!(args.len(), 1);
                    assert_eq!(order_by.len(), 1);
                    assert!(!order_by[0].asc);
                }
                other => panic!("expected MODE WITHIN GROUP, got {other:?}"),
            },
            other => panic!("expected Aggregate for MODE WITHIN GROUP, got {other:?}"),
        }
        assert!(LogicalPlanner::plan("SELECT mode() FROM items").is_err());

        let pct = LogicalPlanner::plan(
            "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY x), \
             percentile_disc(0.5) WITHIN GROUP (ORDER BY x) FROM nums",
        )
        .unwrap();
        match pct {
            LogicalPlan::Aggregate { aggr_exprs, .. } => {
                assert!(matches!(
                    &aggr_exprs[0],
                    Expression::AggregateFunction {
                        name,
                        args,
                        order_by,
                        ..
                    } if name == "PERCENTILE_CONT"
                        && args.len() == 2
                        && order_by.len() == 1
                ));
                assert!(matches!(
                    &aggr_exprs[1],
                    Expression::AggregateFunction { name, .. } if name == "PERCENTILE_DISC"
                ));
            }
            other => panic!("expected Aggregate for percentile_*, got {other:?}"),
        }
        assert!(LogicalPlanner::plan("SELECT percentile_cont(0.5) FROM nums").is_err());

        let ordered_agg = LogicalPlanner::plan(
            "SELECT string_agg(name, ',' ORDER BY name) FROM employees",
        )
        .unwrap();
        match ordered_agg {
            LogicalPlan::Aggregate { aggr_exprs, .. } => {
                match &aggr_exprs[0] {
                    Expression::AggregateFunction { name, order_by, .. } => {
                        assert_eq!(name, "STRING_AGG");
                        assert_eq!(order_by.len(), 1);
                        assert!(order_by[0].asc);
                        assert_eq!(order_by[0].expr, Expression::Column("name".into()));
                    }
                    other => panic!("expected ordered STRING_AGG, got {other:?}"),
                }
            }
            other => panic!("expected Aggregate for ORDER BY agg, got {other:?}"),
        }

        let count_star = LogicalPlanner::plan("SELECT COUNT(*) FROM employees").unwrap();
        match count_star {
            LogicalPlan::Aggregate {
                group_exprs,
                aggr_exprs,
                ..
            } => {
                assert!(group_exprs.is_empty());
                assert_eq!(
                    aggr_exprs[0],
                    Expression::AggregateFunction {
                        name: "COUNT".into(),
                        args: vec![],
                                            filter: None,
                                            distinct: false,
                                            order_by: vec![],
                    }
                );
            }
            other => panic!("expected Aggregate for COUNT(*), got {other:?}"),
        }
    }

    #[test]
    fn parses_group_by_all() {
        let plan = LogicalPlanner::plan(
            "SELECT department, COUNT(id), SUM(salary) FROM employees GROUP BY ALL",
        )
        .unwrap();
        match plan {
            LogicalPlan::Aggregate {
                group_exprs,
                aggr_exprs,
                ..
            } => {
                assert_eq!(group_exprs, vec![Expression::Column("department".into())]);
                assert_eq!(aggr_exprs.len(), 2);
            }
            other => panic!("expected Aggregate from GROUP BY ALL, got {other:?}"),
        }
        let multi = LogicalPlanner::plan(
            "SELECT department, city, COUNT(*) FROM employees GROUP BY ALL",
        )
        .unwrap();
        match multi {
            LogicalPlan::Aggregate { group_exprs, .. } => {
                assert_eq!(
                    group_exprs,
                    vec![
                        Expression::Column("department".into()),
                        Expression::Column("city".into()),
                    ]
                );
            }
            other => panic!("expected Aggregate, got {other:?}"),
        }
    }

    #[test]
    fn parses_group_by_having_count() {
        let plan = LogicalPlanner::plan(
            "SELECT department, COUNT(*) FROM employees GROUP BY department HAVING COUNT(*) > 1",
        )
        .unwrap();
        match plan {
            LogicalPlan::Filter { input, predicate } => {
                assert!(matches!(input.as_ref(), LogicalPlan::Aggregate { .. }));
                match predicate {
                    Expression::BinaryOp {
                        left,
                        op: FilterOp::Gt,
                        right,
                    } => {
                        assert_eq!(*left, Expression::Column("count(*)".into()));
                        assert_eq!(*right, Expression::Literal("1".into()));
                    }
                    other => panic!("expected count(*) > 1, got {other:?}"),
                }
            }
            other => panic!("expected Filter(Aggregate), got {other:?}"),
        }
    }

    #[test]
    fn parses_bare_having_without_group_by() {
        let plan = LogicalPlanner::plan(
            "SELECT COUNT(*) FROM employees HAVING COUNT(*) > 1",
        )
        .unwrap();
        match plan {
            LogicalPlan::Filter { input, predicate } => {
                match input.as_ref() {
                    LogicalPlan::Aggregate {
                        group_exprs,
                        aggr_exprs,
                        ..
                    } => {
                        assert!(group_exprs.is_empty());
                        assert_eq!(aggr_exprs.len(), 1);
                    }
                    other => panic!("expected Aggregate, got {other:?}"),
                }
                assert!(matches!(
                    predicate,
                    Expression::BinaryOp {
                        left,
                        op: FilterOp::Gt,
                        ..
                    } if *left == Expression::Column("count(*)".into())
                ));
            }
            other => panic!("expected Filter(Aggregate), got {other:?}"),
        }

        let having_only = LogicalPlanner::plan(
            "SELECT COUNT(*) FROM employees HAVING SUM(salary) > 100",
        )
        .unwrap();
        match having_only {
            LogicalPlan::Filter { input, .. } => match input.as_ref() {
                LogicalPlan::Aggregate { aggr_exprs, .. } => {
                    assert_eq!(aggr_exprs.len(), 2, "COUNT + HAVING SUM slots");
                    assert!(aggr_exprs.iter().any(|e| {
                        matches!(e, Expression::AggregateFunction { name, .. } if name == "COUNT")
                    }));
                    assert!(aggr_exprs.iter().any(|e| {
                        matches!(e, Expression::AggregateFunction { name, .. } if name == "SUM")
                    }));
                }
                other => panic!("expected Aggregate with HAVING-only SUM, got {other:?}"),
            },
            other => panic!("expected Filter(Aggregate), got {other:?}"),
        }

        assert!(LogicalPlanner::plan("SELECT name FROM employees HAVING true").is_err());
    }

    #[test]
    fn parses_row_number_window() {
        let plan = LogicalPlanner::plan(
            "SELECT name, ROW_NUMBER() OVER (ORDER BY salary DESC) AS rn FROM employees",
        )
        .unwrap();
        match plan {
            LogicalPlan::Project { input, columns } => {
                assert_eq!(columns.len(), 2);
                assert_eq!(columns[0].0, "name");
                assert_eq!(columns[1].0, "rn");
                assert!(matches!(columns[1].1, Expression::Column(ref c) if c == "rn"));
                match input.as_ref() {
                    LogicalPlan::Window { calls, .. } => {
                        assert_eq!(calls.len(), 1);
                        assert_eq!(calls[0].output_column, "rn");
                        assert_eq!(calls[0].kind, WindowKind::RowNumber);
                        assert_eq!(calls[0].order_by.len(), 1);
                        assert!(!calls[0].order_by[0].asc);
                    }
                    other => panic!("expected Window under Project, got {other:?}"),
                }
            }
            other => panic!("expected Project(Window), got {other:?}"),
        }
        assert!(LogicalPlanner::plan(
            "SELECT ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary) FROM employees"
        )
        .is_ok());

        match LogicalPlanner::plan(
            "SELECT ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary) AS rn FROM employees",
        )
        .unwrap()
        {
            LogicalPlan::Project { input, .. } => match input.as_ref() {
                LogicalPlan::Window { calls, .. } => {
                    assert_eq!(calls[0].partition_by.len(), 1);
                    assert_eq!(calls[0].order_by.len(), 1);
                }
                other => panic!("expected Window with PARTITION BY, got {other:?}"),
            },
            other => panic!("expected Project, got {other:?}"),
        }

        let rank = LogicalPlanner::plan(
            "SELECT RANK() OVER (ORDER BY salary), DENSE_RANK() OVER (ORDER BY salary) FROM employees",
        )
        .unwrap();
        match rank {
            LogicalPlan::Project { input, columns } => {
                assert_eq!(columns[0].0, "rank");
                assert_eq!(columns[1].0, "dense_rank");
                match input.as_ref() {
                    LogicalPlan::Window { calls, .. } => {
                        assert_eq!(calls.len(), 2);
                        assert_eq!(calls[0].kind, WindowKind::Rank);
                        assert_eq!(calls[1].kind, WindowKind::DenseRank);
                    }
                    other => panic!("expected Window, got {other:?}"),
                }
            }
            other => panic!("expected Project(Window) for RANK, got {other:?}"),
        }

        let lag = LogicalPlanner::plan(
            "SELECT name, LAG(salary, 1, 0) OVER (ORDER BY id) AS prev FROM employees",
        )
        .unwrap();
        match lag {
            LogicalPlan::Project { input, .. } => match input.as_ref() {
                LogicalPlan::Window { calls, .. } => {
                    assert_eq!(calls[0].kind, WindowKind::Lag);
                    assert_eq!(calls[0].offset, 1);
                    assert!(calls[0].value.is_some());
                    assert!(calls[0].default_value.is_some());
                }
                other => panic!("expected Window for LAG, got {other:?}"),
            },
            other => panic!("expected Project for LAG, got {other:?}"),
        }

        let ntile = LogicalPlanner::plan(
            "SELECT name, NTILE(3) OVER (ORDER BY salary) AS bucket FROM employees",
        )
        .unwrap();
        match ntile {
            LogicalPlan::Project { input, columns } => {
                assert_eq!(columns[1].0, "bucket");
                match input.as_ref() {
                    LogicalPlan::Window { calls, .. } => {
                        assert_eq!(calls[0].kind, WindowKind::Ntile);
                        assert_eq!(calls[0].offset, 3);
                    }
                    other => panic!("expected Window for NTILE, got {other:?}"),
                }
            }
            other => panic!("expected Project(Window) for NTILE, got {other:?}"),
        }
        assert!(LogicalPlanner::plan("SELECT NTILE(0) OVER (ORDER BY id) FROM employees").is_err());

        let first_last = LogicalPlanner::plan(
            "SELECT name, FIRST_VALUE(salary) OVER (PARTITION BY department ORDER BY salary) AS lo, \
             LAST_VALUE(name) OVER (PARTITION BY department ORDER BY salary) AS hi_name \
             FROM employees",
        )
        .unwrap();
        match first_last {
            LogicalPlan::Project { input, .. } => match input.as_ref() {
                LogicalPlan::Window { calls, .. } => {
                    assert_eq!(calls.len(), 2);
                    assert_eq!(calls[0].kind, WindowKind::FirstValue);
                    assert_eq!(calls[1].kind, WindowKind::LastValue);
                    assert!(calls[0].value.is_some());
                    assert_eq!(calls[0].partition_by.len(), 1);
                }
                other => panic!("expected Window for FIRST/LAST_VALUE, got {other:?}"),
            },
            other => panic!("expected Project for FIRST/LAST_VALUE, got {other:?}"),
        }

        let nth = LogicalPlanner::plan(
            "SELECT name, NTH_VALUE(salary, 2) OVER (ORDER BY salary) AS second FROM employees",
        )
        .unwrap();
        match nth {
            LogicalPlan::Project { input, .. } => match input.as_ref() {
                LogicalPlan::Window { calls, .. } => {
                    assert_eq!(calls[0].kind, WindowKind::NthValue);
                    assert_eq!(calls[0].offset, 2);
                    assert!(calls[0].value.is_some());
                }
                other => panic!("expected Window for NTH_VALUE, got {other:?}"),
            },
            other => panic!("expected Project for NTH_VALUE, got {other:?}"),
        }
        assert!(LogicalPlanner::plan(
            "SELECT NTH_VALUE(salary, 0) OVER (ORDER BY salary) FROM employees"
        )
        .is_err());

        let dist = LogicalPlanner::plan(
            "SELECT PERCENT_RANK() OVER (ORDER BY salary), CUME_DIST() OVER (ORDER BY salary) \
             FROM employees",
        )
        .unwrap();
        match dist {
            LogicalPlan::Project { input, .. } => match input.as_ref() {
                LogicalPlan::Window { calls, .. } => {
                    assert_eq!(calls.len(), 2);
                    assert_eq!(calls[0].kind, WindowKind::PercentRank);
                    assert_eq!(calls[1].kind, WindowKind::CumeDist);
                }
                other => panic!("expected Window for PERCENT_RANK/CUME_DIST, got {other:?}"),
            },
            other => panic!("expected Project for PERCENT_RANK/CUME_DIST, got {other:?}"),
        }

        let framed = LogicalPlanner::plan(
            "SELECT LAST_VALUE(salary) OVER (\
               ORDER BY salary \
               ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING\
             ) AS last_sal FROM employees",
        )
        .unwrap();
        match framed {
            LogicalPlan::Project { input, .. } => match input.as_ref() {
                LogicalPlan::Window { calls, .. } => {
                    let f = calls[0].frame.as_ref().expect("frame present");
                    assert_eq!(f.units, FrameUnits::Rows);
                    assert_eq!(f.start, FrameBound::UnboundedPreceding);
                    assert_eq!(f.end, FrameBound::UnboundedFollowing);
                    assert_eq!(f.exclude, FrameExclude::NoOthers);
                }
                other => panic!("expected Window with frame, got {other:?}"),
            },
            other => panic!("expected Project with framed window, got {other:?}"),
        }

        let excl = LogicalPlanner::plan(
            "SELECT SUM(salary) OVER (\
               ORDER BY id \
               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW \
               EXCLUDE CURRENT ROW) AS s FROM employees",
        )
        .unwrap();
        match excl {
            LogicalPlan::Project { input, .. } => match input.as_ref() {
                LogicalPlan::Window { calls, .. } => {
                    let f = calls[0].frame.as_ref().expect("frame");
                    assert_eq!(f.exclude, FrameExclude::CurrentRow);
                    assert!(calls[0].partition_by.is_empty());
                }
                other => panic!("expected Window with EXCLUDE, got {other:?}"),
            },
            other => panic!("expected Project for EXCLUDE, got {other:?}"),
        }
        let range_ok = LogicalPlanner::plan(
            "SELECT LAST_VALUE(salary) OVER (\
               ORDER BY id RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM employees",
        )
        .unwrap();
        match range_ok {
            LogicalPlan::Project { input, .. } => match input.as_ref() {
                LogicalPlan::Window { calls, .. } => {
                    assert_eq!(calls[0].frame.as_ref().unwrap().units, FrameUnits::Range);
                }
                other => panic!("expected Window with RANGE frame, got {other:?}"),
            },
            other => panic!("expected Project for RANGE frame, got {other:?}"),
        }
        assert!(LogicalPlanner::plan(
            "SELECT SUM(salary) OVER (\
               ORDER BY salary RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM employees"
        )
        .is_ok());
        assert!(LogicalPlanner::plan(
            "SELECT SUM(salary) OVER (\
               ORDER BY salary, id RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM employees"
        )
        .is_err());

        let groups = LogicalPlanner::plan(
            "SELECT SUM(salary) OVER (\
               ORDER BY salary GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM employees",
        )
        .unwrap();
        match groups {
            LogicalPlan::Project { input, .. } => match input.as_ref() {
                LogicalPlan::Window { calls, .. } => {
                    assert_eq!(calls[0].frame.as_ref().unwrap().units, FrameUnits::Groups);
                }
                other => panic!("expected Window with GROUPS frame, got {other:?}"),
            },
            other => panic!("expected Project for GROUPS frame, got {other:?}"),
        }
        assert!(LogicalPlanner::plan(
            "SELECT SUM(salary) OVER (GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM employees"
        )
        .is_err());

        let named = LogicalPlanner::plan(
            "SELECT name, ROW_NUMBER() OVER w AS rn FROM employees WINDOW w AS (ORDER BY salary DESC)",
        )
        .unwrap();
        match named {
            LogicalPlan::Project { input, .. } => match input.as_ref() {
                LogicalPlan::Window { calls, .. } => {
                    assert_eq!(calls[0].kind, WindowKind::RowNumber);
                    assert_eq!(calls[0].order_by.len(), 1);
                    assert!(!calls[0].order_by[0].asc);
                }
                other => panic!("expected Window for named OVER w, got {other:?}"),
            },
            other => panic!("expected Project for named window, got {other:?}"),
        }

        let refined = LogicalPlanner::plan(
            "SELECT RANK() OVER (w ORDER BY salary) FROM employees \
             WINDOW w AS (PARTITION BY department)",
        )
        .unwrap();
        match refined {
            LogicalPlan::Project { input, .. } => match input.as_ref() {
                LogicalPlan::Window { calls, .. } => {
                    assert_eq!(calls[0].kind, WindowKind::Rank);
                    assert_eq!(calls[0].partition_by.len(), 1);
                    assert_eq!(calls[0].order_by.len(), 1);
                }
                other => panic!("expected Window for OVER (w …), got {other:?}"),
            },
            other => panic!("expected Project for refined named window, got {other:?}"),
        }
        assert!(LogicalPlanner::plan(
            "SELECT ROW_NUMBER() OVER missing FROM employees"
        )
        .is_err());

        let wag = LogicalPlanner::plan(
            "SELECT name, SUM(salary) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running \
             FROM employees",
        )
        .unwrap();
        match wag {
            LogicalPlan::Project { input, .. } => match input.as_ref() {
                LogicalPlan::Window { calls, .. } => {
                    assert_eq!(calls[0].kind, WindowKind::Sum);
                    assert!(calls[0].value.is_some());
                    assert!(calls[0].frame.is_some());
                }
                other => panic!("expected Window for SUM OVER, got {other:?}"),
            },
            other => panic!("expected Project for SUM OVER, got {other:?}"),
        }
        assert!(LogicalPlanner::plan(
            "SELECT COUNT(*) OVER (PARTITION BY department) FROM employees"
        )
        .is_ok());

        let stragg = LogicalPlanner::plan(
            "SELECT STRING_AGG(name, ',') OVER (PARTITION BY department ORDER BY id) FROM employees",
        )
        .unwrap();
        match stragg {
            LogicalPlan::Project { input, .. } => match input.as_ref() {
                LogicalPlan::Window { calls, .. } => {
                    assert_eq!(calls[0].kind, WindowKind::StringAgg);
                    assert!(calls[0].value.is_some());
                    assert!(calls[0].default_value.is_some());
                }
                other => panic!("expected Window for STRING_AGG OVER, got {other:?}"),
            },
            other => panic!("expected Project for STRING_AGG OVER, got {other:?}"),
        }
        assert!(LogicalPlanner::plan(
            "SELECT ARRAY_AGG(salary) OVER (ORDER BY id) FROM employees"
        )
        .is_ok());

        let bool_json = LogicalPlanner::plan(
            "SELECT BOOL_AND(active) OVER (PARTITION BY department), \
             JSON_AGG(name) OVER (PARTITION BY department ORDER BY id) FROM employees",
        )
        .unwrap();
        match bool_json {
            LogicalPlan::Project { input, .. } => match input.as_ref() {
                LogicalPlan::Window { calls, .. } => {
                    assert_eq!(calls[0].kind, WindowKind::BoolAnd);
                    assert_eq!(calls[1].kind, WindowKind::JsonAgg);
                }
                other => panic!("expected Window for BOOL_AND/JSON_AGG OVER, got {other:?}"),
            },
            other => panic!("expected Project for BOOL_AND/JSON_AGG OVER, got {other:?}"),
        }

        let stats = LogicalPlanner::plan(
            "SELECT STDDEV(salary) OVER (PARTITION BY department), \
             VAR_POP(salary) OVER (PARTITION BY department) FROM employees",
        )
        .unwrap();
        match stats {
            LogicalPlan::Project { input, .. } => match input.as_ref() {
                LogicalPlan::Window { calls, .. } => {
                    assert_eq!(calls[0].kind, WindowKind::StddevSamp);
                    assert_eq!(calls[1].kind, WindowKind::VarPop);
                }
                other => panic!("expected Window for STDDEV/VAR OVER, got {other:?}"),
            },
            other => panic!("expected Project for STDDEV/VAR OVER, got {other:?}"),
        }

        let filtered = LogicalPlanner::plan(
            "SELECT SUM(salary) FILTER (WHERE salary > 100) OVER (PARTITION BY department) \
             FROM employees",
        )
        .unwrap();
        match filtered {
            LogicalPlan::Project { input, .. } => match input.as_ref() {
                LogicalPlan::Window { calls, .. } => {
                    assert_eq!(calls[0].kind, WindowKind::Sum);
                    assert!(calls[0].filter.is_some());
                }
                other => panic!("expected Window with FILTER, got {other:?}"),
            },
            other => panic!("expected Project for FILTER window, got {other:?}"),
        }
        assert!(LogicalPlanner::plan(
            "SELECT ROW_NUMBER() FILTER (WHERE true) OVER (ORDER BY id) FROM employees"
        )
        .is_err());

        let bivar = LogicalPlanner::plan(
            "SELECT CORR(salary, id) OVER (PARTITION BY department), \
             COVAR_POP(salary, id) OVER (PARTITION BY department) FROM employees",
        )
        .unwrap();
        match bivar {
            LogicalPlan::Project { input, .. } => match input.as_ref() {
                LogicalPlan::Window { calls, .. } => {
                    assert_eq!(calls[0].kind, WindowKind::Corr);
                    assert_eq!(calls[1].kind, WindowKind::CovarPop);
                    assert!(calls[0].value.is_some());
                    assert!(calls[0].default_value.is_some());
                }
                other => panic!("expected Window for CORR/COVAR OVER, got {other:?}"),
            },
            other => panic!("expected Project for CORR/COVAR OVER, got {other:?}"),
        }

        let regr = LogicalPlanner::plan(
            "SELECT REGR_SLOPE(salary, id) OVER (PARTITION BY department), \
             REGR_COUNT(salary, id) OVER (PARTITION BY department) FROM employees",
        )
        .unwrap();
        match regr {
            LogicalPlan::Project { input, .. } => match input.as_ref() {
                LogicalPlan::Window { calls, .. } => {
                    assert_eq!(calls[0].kind, WindowKind::RegrSlope);
                    assert_eq!(calls[1].kind, WindowKind::RegrCount);
                }
                other => panic!("expected Window for REGR OVER, got {other:?}"),
            },
            other => panic!("expected Project for REGR OVER, got {other:?}"),
        }

        let bit_mode = LogicalPlanner::plan(
            "SELECT BIT_AND(id) OVER (PARTITION BY department), \
             MODE(name) OVER (PARTITION BY department) FROM employees",
        )
        .unwrap();
        match bit_mode {
            LogicalPlan::Project { input, .. } => match input.as_ref() {
                LogicalPlan::Window { calls, .. } => {
                    assert_eq!(calls[0].kind, WindowKind::BitAnd);
                    assert_eq!(calls[1].kind, WindowKind::Mode);
                }
                other => panic!("expected Window for BIT_AND/MODE OVER, got {other:?}"),
            },
            other => panic!("expected Project for BIT_AND/MODE OVER, got {other:?}"),
        }

        let jobj = LogicalPlanner::plan(
            "SELECT JSON_OBJECT_AGG(name, id) OVER (PARTITION BY department) FROM employees",
        )
        .unwrap();
        match jobj {
            LogicalPlan::Project { input, .. } => match input.as_ref() {
                LogicalPlan::Window { calls, .. } => {
                    assert_eq!(calls[0].kind, WindowKind::JsonObjectAgg);
                    assert!(calls[0].value.is_some());
                    assert!(calls[0].default_value.is_some());
                }
                other => panic!("expected Window for JSON_OBJECT_AGG OVER, got {other:?}"),
            },
            other => panic!("expected Project for JSON_OBJECT_AGG OVER, got {other:?}"),
        }

        let ign = LogicalPlanner::plan(
            "SELECT LAG(salary) IGNORE NULLS OVER (ORDER BY id) FROM employees",
        )
        .unwrap();
        match ign {
            LogicalPlan::Project { input, .. } => match input.as_ref() {
                LogicalPlan::Window { calls, .. } => {
                    assert_eq!(calls[0].kind, WindowKind::Lag);
                    assert!(calls[0].ignore_nulls);
                }
                other => panic!("expected Window with IGNORE NULLS, got {other:?}"),
            },
            other => panic!("expected Project for IGNORE NULLS, got {other:?}"),
        }
        assert!(LogicalPlanner::plan(
            "SELECT SUM(salary) IGNORE NULLS OVER (ORDER BY id) FROM employees"
        )
        .is_err());
    }

    #[test]
    fn parses_order_by_limit_offset() {
        let plan = LogicalPlanner::plan(
            "SELECT department, SUM(salary) FROM employees GROUP BY department \
             ORDER BY SUM(salary) DESC LIMIT 1",
        )
        .unwrap();
        match plan {
            LogicalPlan::Limit {
                skip: 0,
                fetch: Some(1),
                with_ties: false,
                input,
                ..
            } => match input.as_ref() {
                LogicalPlan::Sort { exprs, input } => {
                    assert_eq!(exprs.len(), 1);
                    assert!(!exprs[0].asc);
                    assert_eq!(
                        exprs[0].expr,
                        Expression::Column("sum(salary)".into()),
                        "ORDER BY SUM(salary) should rewrite to aggregate output column"
                    );
                    assert!(matches!(input.as_ref(), LogicalPlan::Aggregate { .. }));
                }
                other => panic!("expected Sort under Limit, got {other:?}"),
            },
            other => panic!("expected Limit, got {other:?}"),
        }

        let off = LogicalPlanner::plan("SELECT * FROM users ORDER BY name ASC OFFSET 2 LIMIT 5")
            .unwrap();
        match off {
            LogicalPlan::Limit {
                skip: 2,
                fetch: Some(5),
                with_ties: false,
                input,
                ..
            } => match input.as_ref() {
                LogicalPlan::Sort { exprs, .. } => {
                    assert_eq!(exprs[0].expr, Expression::Column("name".into()));
                    assert!(exprs[0].asc);
                }
                other => panic!("expected Sort, got {other:?}"),
            },
            other => panic!("expected Limit with offset, got {other:?}"),
        }

        match LogicalPlanner::plan(
            "SELECT name FROM users ORDER BY age NULLS FIRST",
        )
        .unwrap()
        {
            LogicalPlan::Project { input, .. } => match input.as_ref() {
                LogicalPlan::Sort { exprs, .. } => {
                    assert!(exprs[0].asc);
                    assert!(exprs[0].nulls_first);
                }
                other => panic!("expected Sort with NULLS FIRST, got {other:?}"),
            },
            other => match other {
                LogicalPlan::Sort { exprs, .. } => {
                    assert!(exprs[0].nulls_first);
                }
                o => panic!("expected Sort/Project for NULLS FIRST, got {o:?}"),
            },
        }

        match LogicalPlanner::plan(
            "SELECT name FROM users ORDER BY age DESC NULLS LAST",
        )
        .unwrap()
        {
            LogicalPlan::Project { input, .. } => match input.as_ref() {
                LogicalPlan::Sort { exprs, .. } => {
                    assert!(!exprs[0].asc);
                    assert!(!exprs[0].nulls_first);
                }
                other => panic!("expected Sort NULLS LAST, got {other:?}"),
            },
            other => panic!("expected Project, got {other:?}"),
        }

        // Default ASC → NULLS LAST; default DESC → NULLS FIRST.
        match LogicalPlanner::plan("SELECT name FROM users ORDER BY age").unwrap() {
            LogicalPlan::Project { input, .. } => match input.as_ref() {
                LogicalPlan::Sort { exprs, .. } => {
                    assert!(exprs[0].asc);
                    assert!(!exprs[0].nulls_first);
                }
                other => panic!("expected Sort, got {other:?}"),
            },
            other => panic!("expected Project, got {other:?}"),
        }

        match LogicalPlanner::plan(
            "SELECT name, age FROM users ORDER BY age \
             FETCH FIRST 2 ROWS WITH TIES",
        )
        .unwrap()
        {
            LogicalPlan::Project { input, .. } => match input.as_ref() {
                LogicalPlan::Limit {
                    fetch: Some(2),
                    with_ties: true,
                    ties_order,
                    input: inner,
                    ..
                } => {
                    assert_eq!(ties_order.len(), 1);
                    assert!(matches!(inner.as_ref(), LogicalPlan::Sort { .. }));
                }
                other => panic!("expected Limit WITH TIES, got {other:?}"),
            },
            other => panic!("expected Project(Limit WITH TIES), got {other:?}"),
        }

        assert!(
            LogicalPlanner::plan("SELECT name FROM users FETCH FIRST 1 ROW WITH TIES")
                .unwrap_err()
                .to_string()
                .contains("ORDER BY")
        );
    }

    #[test]
    fn parses_order_by_all() {
        match LogicalPlanner::plan("SELECT name, age FROM users ORDER BY ALL").unwrap() {
            LogicalPlan::Project { input, .. } => match input.as_ref() {
                LogicalPlan::Sort { exprs, .. } => {
                    assert_eq!(exprs.len(), 2);
                    assert_eq!(exprs[0].expr, Expression::Column("name".into()));
                    assert_eq!(exprs[1].expr, Expression::Column("age".into()));
                    assert!(exprs[0].asc && exprs[1].asc);
                    assert!(!exprs[0].nulls_first && !exprs[1].nulls_first);
                }
                other => panic!("expected Sort, got {other:?}"),
            },
            other => panic!("expected Project, got {other:?}"),
        }
        match LogicalPlanner::plan("SELECT name, age FROM users ORDER BY ALL DESC").unwrap() {
            LogicalPlan::Project { input, .. } => match input.as_ref() {
                LogicalPlan::Sort { exprs, .. } => {
                    assert!(!exprs[0].asc && !exprs[1].asc);
                    assert!(exprs[0].nulls_first && exprs[1].nulls_first);
                }
                other => panic!("expected Sort DESC, got {other:?}"),
            },
            other => panic!("expected Project, got {other:?}"),
        }
        match LogicalPlanner::plan(
            "SELECT department, COUNT(*) FROM employees GROUP BY ALL ORDER BY ALL",
        )
        .unwrap()
        {
            LogicalPlan::Sort { exprs, input, .. } => {
                assert_eq!(exprs.len(), 2);
                assert_eq!(exprs[0].expr, Expression::Column("department".into()));
                assert_eq!(exprs[1].expr, Expression::Column("count(*)".into()));
                assert!(matches!(input.as_ref(), LogicalPlan::Aggregate { .. }));
            }
            other => panic!("expected Sort(Aggregate), got {other:?}"),
        }
    }

    #[test]
    fn create_drop_index_and_explain_parse() {
        match LogicalPlanner::plan("CREATE INDEX idx_dept ON employees(department)").unwrap() {
            LogicalPlan::CreateIndex {
                name,
                table,
                column,
                if_not_exists,
                vector: None,
            } => {
                assert_eq!(name, "idx_dept");
                assert_eq!(table, "employees");
                assert_eq!(column, "department");
                assert!(!if_not_exists);
            }
            other => panic!("expected CreateIndex, got {other:?}"),
        }

        match LogicalPlanner::plan(
            "CREATE VECTOR INDEX idx_v ON docs(vec) WITH (DIMENSION=128, TYPE=HNSW)",
        )
        .unwrap()
        {
            LogicalPlan::CreateIndex {
                name,
                table,
                column,
                vector: Some(spec),
                ..
            } => {
                assert_eq!(name, "idx_v");
                assert_eq!(table, "docs");
                assert_eq!(column, "vec");
                assert_eq!(spec.dimension, 128);
                assert_eq!(spec.index_type, "HNSW");
            }
            other => panic!("expected CreateVectorIndex, got {other:?}"),
        }

        match LogicalPlanner::plan(
            "SELECT * FROM docs ORDER BY vec <-> ARRAY[0.1, 0.2] LIMIT 5",
        )
        .unwrap()
        {
            LogicalPlan::Limit {
                fetch: Some(5),
                input,
                ..
            } => match input.as_ref() {
                LogicalPlan::Sort { exprs, .. } => {
                    assert!(matches!(
                        exprs[0].expr,
                        Expression::VectorDistance { .. }
                    ));
                }
                other => panic!("expected Sort over VectorDistance, got {other:?}"),
            },
            other => panic!("expected Limit, got {other:?}"),
        }

        match LogicalPlanner::plan("DROP INDEX IF EXISTS idx_dept").unwrap() {
            LogicalPlan::DropIndex { name, if_exists } => {
                assert_eq!(name, "idx_dept");
                assert!(if_exists);
            }
            other => panic!("expected DropIndex, got {other:?}"),
        }

        match LogicalPlanner::plan(
            "EXPLAIN SELECT * FROM employees WHERE department = 'Engineering'",
        )
        .unwrap()
        {
            LogicalPlan::Explain { plan } => {
                assert!(matches!(plan.as_ref(), LogicalPlan::Select { .. }));
            }
            other => panic!("expected Explain, got {other:?}"),
        }
    }

    #[test]
    fn create_drop_table_parse() {
        match LogicalPlanner::plan(
            "CREATE TABLE items (id BIGINT PRIMARY KEY, name TEXT, qty INT)",
        )
        .unwrap()
        {
            LogicalPlan::CreateTable {
                name,
                primary_key,
                columns,
                if_not_exists,
                serial_columns,
            } => {
                assert_eq!(name, "items");
                assert_eq!(primary_key, "id");
                assert!(!if_not_exists);
                assert!(serial_columns.is_empty());
                assert_eq!(columns.len(), 3);
                assert_eq!(columns[0].name, "id");
                assert_eq!(columns[0].data_type, "BIGINT");
                assert_eq!(columns[1].name, "name");
                assert_eq!(columns[1].data_type, "TEXT");
                assert_eq!(columns[2].name, "qty");
                assert_eq!(columns[2].data_type, "INT");
            }
            other => panic!("expected CreateTable, got {other:?}"),
        }

        match LogicalPlanner::plan(
            "CREATE TABLE IF NOT EXISTS t (k INT, v TEXT, PRIMARY KEY (k))",
        )
        .unwrap()
        {
            LogicalPlan::CreateTable {
                name,
                primary_key,
                if_not_exists,
                ..
            } => {
                assert_eq!(name, "t");
                assert_eq!(primary_key, "k");
                assert!(if_not_exists);
            }
            other => panic!("expected CreateTable, got {other:?}"),
        }

        match LogicalPlanner::plan("DROP TABLE items").unwrap() {
            LogicalPlan::DropTable { name, if_exists } => {
                assert_eq!(name, "items");
                assert!(!if_exists);
            }
            other => panic!("expected DropTable, got {other:?}"),
        }

        assert!(
            LogicalPlanner::plan("CREATE TABLE no_pk (a INT, b TEXT)")
                .unwrap_err()
                .to_string()
                .contains("PRIMARY KEY")
        );
    }

    #[test]
    fn parses_create_table_serial() {
        match LogicalPlanner::plan(
            "CREATE TABLE serials (id SERIAL PRIMARY KEY, name TEXT)",
        )
        .unwrap()
        {
            LogicalPlan::CreateTable {
                name,
                primary_key,
                columns,
                serial_columns,
                ..
            } => {
                assert_eq!(name, "serials");
                assert_eq!(primary_key, "id");
                assert_eq!(columns[0].data_type, "INT");
                assert_eq!(serial_columns, vec!["id".to_string()]);
            }
            other => panic!("expected CreateTable SERIAL, got {other:?}"),
        }
        match LogicalPlanner::plan(
            "CREATE TABLE bigs (id BIGSERIAL PRIMARY KEY, note TEXT)",
        )
        .unwrap()
        {
            LogicalPlan::CreateTable {
                columns,
                serial_columns,
                ..
            } => {
                assert_eq!(columns[0].data_type, "BIGINT");
                assert_eq!(serial_columns, vec!["id".to_string()]);
            }
            other => panic!("expected CreateTable BIGSERIAL, got {other:?}"),
        }
        assert_eq!(serial_sequence_name("Items", "Id"), "items_id_seq");
    }

    #[test]
    fn parses_create_table_as_select() {
        match LogicalPlanner::plan(
            "CREATE TABLE copy AS SELECT id, name FROM users WHERE age > 20",
        )
        .unwrap()
        {
            LogicalPlan::CreateTableAs {
                name,
                columns,
                if_not_exists,
                query,
            } => {
                assert_eq!(name, "copy");
                assert!(columns.is_empty());
                assert!(!if_not_exists);
                assert!(matches!(query.as_ref(), LogicalPlan::Project { .. }));
            }
            other => panic!("expected CreateTableAs, got {other:?}"),
        }
        match LogicalPlanner::plan(
            "CREATE TABLE IF NOT EXISTS t (a TEXT, b TEXT) AS SELECT id, name FROM users",
        )
        .unwrap()
        {
            LogicalPlan::CreateTableAs {
                columns,
                if_not_exists: true,
                ..
            } => {
                assert_eq!(columns, vec!["a".to_string(), "b".to_string()]);
            }
            other => panic!("expected CreateTableAs with renames, got {other:?}"),
        }
    }

    #[test]
    fn alter_table_add_drop_column_parse() {
        match LogicalPlanner::plan("ALTER TABLE items ADD COLUMN note TEXT").unwrap() {
            LogicalPlan::AlterTable { name, operations } => {
                assert_eq!(name, "items");
                assert_eq!(operations.len(), 1);
                match &operations[0] {
                    AlterTableOp::AddColumn {
                        column,
                        if_not_exists,
                        is_serial,
                    } => {
                        assert_eq!(column.name, "note");
                        assert_eq!(column.data_type, "TEXT");
                        assert!(!if_not_exists);
                        assert!(!is_serial);
                    }
                    other => panic!("expected AddColumn, got {other:?}"),
                }
            }
            other => panic!("expected AlterTable, got {other:?}"),
        }

        match LogicalPlanner::plan("ALTER TABLE items ADD COLUMN sid SERIAL").unwrap() {
            LogicalPlan::AlterTable { operations, .. } => match &operations[0] {
                AlterTableOp::AddColumn {
                    column,
                    is_serial: true,
                    ..
                } => {
                    assert_eq!(column.name, "sid");
                    assert_eq!(column.data_type, "INT");
                }
                other => panic!("expected SERIAL AddColumn, got {other:?}"),
            },
            other => panic!("expected AlterTable, got {other:?}"),
        }

        match LogicalPlanner::plan("ALTER TABLE items DROP COLUMN IF EXISTS note").unwrap() {
            LogicalPlan::AlterTable { operations, .. } => match &operations[0] {
                AlterTableOp::DropColumn { name, if_exists } => {
                    assert_eq!(name, "note");
                    assert!(if_exists);
                }
                other => panic!("expected DropColumn, got {other:?}"),
            },
            other => panic!("expected AlterTable, got {other:?}"),
        }

        match LogicalPlanner::plan("ALTER TABLE items RENAME COLUMN name TO title").unwrap() {
            LogicalPlan::AlterTable { operations, .. } => match &operations[0] {
                AlterTableOp::RenameColumn { old_name, new_name } => {
                    assert_eq!(old_name, "name");
                    assert_eq!(new_name, "title");
                }
                other => panic!("expected RenameColumn, got {other:?}"),
            },
            other => panic!("expected AlterTable, got {other:?}"),
        }

        match LogicalPlanner::plan("ALTER TABLE items RENAME TO products").unwrap() {
            LogicalPlan::AlterTable { name, operations } => {
                assert_eq!(name, "items");
                match &operations[0] {
                    AlterTableOp::RenameTable { new_name } => {
                        assert_eq!(new_name, "products");
                    }
                    other => panic!("expected RenameTable, got {other:?}"),
                }
            }
            other => panic!("expected AlterTable, got {other:?}"),
        }

        match LogicalPlanner::plan("ALTER TABLE items ALTER COLUMN qty TYPE BIGINT").unwrap()
        {
            LogicalPlan::AlterTable { operations, .. } => match &operations[0] {
                AlterTableOp::SetDataType { name, data_type } => {
                    assert_eq!(name, "qty");
                    assert_eq!(data_type, "BIGINT");
                }
                other => panic!("expected SetDataType, got {other:?}"),
            },
            other => panic!("expected AlterTable, got {other:?}"),
        }
        match LogicalPlanner::plan(
            "ALTER TABLE items ALTER COLUMN qty SET DATA TYPE INTEGER",
        )
        .unwrap()
        {
            LogicalPlan::AlterTable { operations, .. } => match &operations[0] {
                AlterTableOp::SetDataType { data_type, .. } => {
                    assert_eq!(data_type, "INT");
                }
                other => panic!("expected SetDataType, got {other:?}"),
            },
            other => panic!("expected AlterTable, got {other:?}"),
        }
        assert!(
            LogicalPlanner::plan(
                "ALTER TABLE items ALTER COLUMN qty TYPE INT USING qty::int"
            )
            .unwrap_err()
            .to_string()
            .contains("USING")
        );
    }

    #[test]
    fn select_projection_parse() {
        match LogicalPlanner::plan("SELECT name, qty FROM items").unwrap() {
            LogicalPlan::Project { columns, input } => {
                assert_eq!(columns.len(), 2);
                assert_eq!(columns[0].0, "name");
                assert_eq!(columns[1].0, "qty");
                assert!(matches!(input.as_ref(), LogicalPlan::Select { .. }));
            }
            other => panic!("expected Project, got {other:?}"),
        }

        match LogicalPlanner::plan("SELECT name AS n FROM items").unwrap() {
            LogicalPlan::Project { columns, .. } => {
                assert_eq!(columns[0].0, "n");
            }
            other => panic!("expected Project, got {other:?}"),
        }

        // SELECT * stays as plain Select (no Project wrapper).
        assert!(matches!(
            LogicalPlanner::plan("SELECT * FROM items").unwrap(),
            LogicalPlan::Select { .. }
        ));
    }

    #[test]
    fn parses_union_and_distinct() {
        match LogicalPlanner::plan("SELECT DISTINCT name FROM users").unwrap() {
            LogicalPlan::Distinct { input } => {
                assert!(matches!(input.as_ref(), LogicalPlan::Project { .. }));
            }
            other => panic!("expected Distinct, got {other:?}"),
        }

        match LogicalPlanner::plan(
            "SELECT name FROM users UNION ALL SELECT name FROM users",
        )
        .unwrap()
        {
            LogicalPlan::Union {
                all: true,
                op: SetOpKind::Union,
                ..
            } => {}
            other => panic!("expected Union ALL, got {other:?}"),
        }

        match LogicalPlanner::plan("SELECT name FROM users UNION SELECT name FROM users")
            .unwrap()
        {
            LogicalPlan::Union {
                all: false,
                op: SetOpKind::Union,
                ..
            } => {}
            other => panic!("expected Union distinct, got {other:?}"),
        }

        match LogicalPlanner::plan(
            "SELECT DISTINCT ON (name) name, age FROM users ORDER BY name, age",
        )
        .unwrap()
        {
            LogicalPlan::Project { input, .. } => match input.as_ref() {
                LogicalPlan::DistinctOn { exprs, input: inner } => {
                    assert_eq!(exprs.len(), 1);
                    assert!(matches!(inner.as_ref(), LogicalPlan::Sort { .. }));
                }
                other => panic!("expected DistinctOn under Project, got {other:?}"),
            },
            other => panic!("expected Project(DistinctOn), got {other:?}"),
        }

        assert!(
            LogicalPlanner::plan("SELECT DISTINCT ON (name) name FROM users ORDER BY age")
                .unwrap_err()
                .to_string()
                .contains("must match initial ORDER BY")
        );

        match LogicalPlanner::plan(
            "SELECT name FROM users INTERSECT SELECT name FROM users",
        )
        .unwrap()
        {
            LogicalPlan::Union {
                op: SetOpKind::Intersect,
                all: false,
                ..
            } => {}
            other => panic!("expected Intersect, got {other:?}"),
        }

        match LogicalPlanner::plan("SELECT name FROM users EXCEPT ALL SELECT name FROM users")
            .unwrap()
        {
            LogicalPlan::Union {
                op: SetOpKind::Except,
                all: true,
                ..
            } => {}
            other => panic!("expected Except ALL, got {other:?}"),
        }
    }

    #[test]
    fn sql_like_match_patterns() {
        assert!(sql_like_match("Ada", "Ada", false, None));
        assert!(sql_like_match("Ada", "A%", false, None));
        assert!(sql_like_match("Ada", "%a", false, None));
        assert!(sql_like_match("Ada", "A_a", false, None));
        assert!(!sql_like_match("Ada", "ada", false, None));
        assert!(sql_like_match("Ada", "ada", true, None));
        assert!(sql_like_match("a%b", r"a\%b", false, Some('\\')));
        assert!(!sql_like_match("axb", r"a\%b", false, Some('\\')));
    }

    #[test]
    fn sql_similar_to_match_patterns() {
        assert!(sql_similar_to_match("abc", "abc", Some('\\')).unwrap());
        assert!(sql_similar_to_match("abc", "a%", Some('\\')).unwrap());
        assert!(sql_similar_to_match("abc", "a_c", Some('\\')).unwrap());
        assert!(sql_similar_to_match("abc", "(a|z)%", Some('\\')).unwrap());
        assert!(sql_similar_to_match("zzz", "(a|z)%", Some('\\')).unwrap());
        assert!(!sql_similar_to_match("bbc", "(a|z)%", Some('\\')).unwrap());
        assert!(sql_similar_to_match("aaa", "a+", Some('\\')).unwrap());
        assert!(!sql_similar_to_match("bbb", "a+", Some('\\')).unwrap());
        assert!(sql_similar_to_match("a.b", r"a\.b", Some('\\')).unwrap());
        assert!(!sql_similar_to_match("axb", r"a\.b", Some('\\')).unwrap());
    }

    #[test]
    fn parses_similar_to() {
        match LogicalPlanner::plan("SELECT name FROM users WHERE name SIMILAR TO 'A%'").unwrap() {
            LogicalPlan::Project { input, .. } => match input.as_ref() {
                LogicalPlan::Select {
                    predicate: Some(Expression::SimilarTo { negated: false, .. }),
                    ..
                } => {}
                other => panic!("expected SIMILAR TO, got {other:?}"),
            },
            other => panic!("expected Project, got {other:?}"),
        }
        match LogicalPlanner::plan(
            "SELECT name FROM users WHERE name NOT SIMILAR TO '(A|B)%'",
        )
        .unwrap()
        {
            LogicalPlan::Project { input, .. } => match input.as_ref() {
                LogicalPlan::Select {
                    predicate: Some(Expression::SimilarTo { negated: true, .. }),
                    ..
                } => {}
                other => panic!("expected NOT SIMILAR TO, got {other:?}"),
            },
            other => panic!("expected Project, got {other:?}"),
        }
    }

    #[test]
    fn parses_regex_match_ops() {
        match LogicalPlanner::plan("SELECT name FROM users WHERE name ~ '^A'").unwrap() {
            LogicalPlan::Project { input, .. } => match input.as_ref() {
                LogicalPlan::Select {
                    predicate:
                        Some(Expression::RegexMatch {
                            case_insensitive: false,
                            negated: false,
                            ..
                        }),
                    ..
                } => {}
                other => panic!("expected ~, got {other:?}"),
            },
            other => panic!("expected Project, got {other:?}"),
        }
        match LogicalPlanner::plan("SELECT name FROM users WHERE name ~* 'ada'").unwrap() {
            LogicalPlan::Project { input, .. } => match input.as_ref() {
                LogicalPlan::Select {
                    predicate:
                        Some(Expression::RegexMatch {
                            case_insensitive: true,
                            negated: false,
                            ..
                        }),
                    ..
                } => {}
                other => panic!("expected ~*, got {other:?}"),
            },
            other => panic!("expected Project, got {other:?}"),
        }
        match LogicalPlanner::plan("SELECT name FROM users WHERE name !~ 'B'").unwrap() {
            LogicalPlan::Project { input, .. } => match input.as_ref() {
                LogicalPlan::Select {
                    predicate:
                        Some(Expression::RegexMatch {
                            negated: true,
                            case_insensitive: false,
                            ..
                        }),
                    ..
                } => {}
                other => panic!("expected !~, got {other:?}"),
            },
            other => panic!("expected Project, got {other:?}"),
        }
        match LogicalPlanner::plan("SELECT name FROM users WHERE name !~* 'ADA'").unwrap() {
            LogicalPlan::Project { input, .. } => match input.as_ref() {
                LogicalPlan::Select {
                    predicate:
                        Some(Expression::RegexMatch {
                            negated: true,
                            case_insensitive: true,
                            ..
                        }),
                    ..
                } => {}
                other => panic!("expected !~*, got {other:?}"),
            },
            other => panic!("expected Project, got {other:?}"),
        }
    }

    #[test]
    fn parses_like_and_ilike() {
        match LogicalPlanner::plan("SELECT name FROM users WHERE name LIKE 'A%'").unwrap() {
            LogicalPlan::Project { input, .. } => match input.as_ref() {
                LogicalPlan::Select {
                    predicate: Some(Expression::Like {
                        case_insensitive: false,
                        negated: false,
                        ..
                    }),
                    ..
                } => {}
                other => panic!("expected LIKE Select, got {other:?}"),
            },
            other => panic!("expected Project, got {other:?}"),
        }
        match LogicalPlanner::plan("SELECT name FROM users WHERE name ILIKE '%ADA%'").unwrap() {
            LogicalPlan::Project { input, .. } => match input.as_ref() {
                LogicalPlan::Select {
                    predicate: Some(Expression::Like {
                        case_insensitive: true,
                        ..
                    }),
                    ..
                } => {}
                other => panic!("expected ILIKE Select, got {other:?}"),
            },
            other => panic!("expected Project, got {other:?}"),
        }
        match LogicalPlanner::plan("SELECT name FROM users WHERE name NOT LIKE 'Z%'").unwrap() {
            LogicalPlan::Project { input, .. } => match input.as_ref() {
                LogicalPlan::Select {
                    predicate: Some(Expression::Like { negated: true, .. }),
                    ..
                } => {}
                other => panic!("expected NOT LIKE, got {other:?}"),
            },
            other => panic!("expected Project, got {other:?}"),
        }
    }

    #[test]
    fn parses_like_any() {
        match LogicalPlanner::plan(
            "SELECT name FROM users WHERE name LIKE ANY (ARRAY['A%', 'B%'])",
        )
        .unwrap()
        {
            LogicalPlan::Project { input, .. } => match input.as_ref() {
                LogicalPlan::Select {
                    predicate: Some(Expression::Like {
                        any: true,
                        case_insensitive: false,
                        negated: false,
                        ..
                    }),
                    ..
                } => {}
                other => panic!("expected LIKE ANY, got {other:?}"),
            },
            other => panic!("expected Project, got {other:?}"),
        }
        match LogicalPlanner::plan(
            "SELECT name FROM users WHERE name ILIKE ANY (ARRAY['%ada%'])",
        )
        .unwrap()
        {
            LogicalPlan::Project { input, .. } => match input.as_ref() {
                LogicalPlan::Select {
                    predicate: Some(Expression::Like {
                        any: true,
                        case_insensitive: true,
                        ..
                    }),
                    ..
                } => {}
                other => panic!("expected ILIKE ANY, got {other:?}"),
            },
            other => panic!("expected Project, got {other:?}"),
        }
        match LogicalPlanner::plan(
            "SELECT name FROM users WHERE name NOT LIKE ANY (ARRAY['%z'])",
        )
        .unwrap()
        {
            LogicalPlan::Project { input, .. } => match input.as_ref() {
                LogicalPlan::Select {
                    predicate: Some(Expression::Like {
                        any: true,
                        negated: true,
                        ..
                    }),
                    ..
                } => {}
                other => panic!("expected NOT LIKE ANY, got {other:?}"),
            },
            other => panic!("expected Project, got {other:?}"),
        }
    }

    #[test]
    fn parses_between_as_range_predicate() {
        match LogicalPlanner::plan("SELECT name FROM users WHERE age BETWEEN 20 AND 30")
            .unwrap()
        {
            LogicalPlan::Project { input, .. } => match input.as_ref() {
                LogicalPlan::Select {
                    predicate: Some(Expression::And { left, right }),
                    ..
                } => {
                    assert!(matches!(
                        left.as_ref(),
                        Expression::BinaryOp {
                            op: FilterOp::Gte,
                            ..
                        }
                    ));
                    assert!(matches!(
                        right.as_ref(),
                        Expression::BinaryOp {
                            op: FilterOp::Lte,
                            ..
                        }
                    ));
                }
                other => panic!("expected BETWEEN → AND, got {other:?}"),
            },
            other => panic!("expected Project, got {other:?}"),
        }

        match LogicalPlanner::plan("SELECT name FROM users WHERE age NOT BETWEEN 20 AND 30")
            .unwrap()
        {
            LogicalPlan::Project { input, .. } => match input.as_ref() {
                LogicalPlan::Select {
                    predicate: Some(Expression::Or { left, right }),
                    ..
                } => {
                    assert!(matches!(
                        left.as_ref(),
                        Expression::BinaryOp {
                            op: FilterOp::Lt,
                            ..
                        }
                    ));
                    assert!(matches!(
                        right.as_ref(),
                        Expression::BinaryOp {
                            op: FilterOp::Gt,
                            ..
                        }
                    ));
                }
                other => panic!("expected NOT BETWEEN → OR, got {other:?}"),
            },
            other => panic!("expected Project, got {other:?}"),
        }
    }

    #[test]
    fn parses_case_when() {
        match LogicalPlanner::plan(
            "SELECT CASE WHEN age >= 30 THEN 'senior' ELSE 'junior' END AS band FROM users",
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert_eq!(columns.len(), 1);
                assert_eq!(columns[0].0, "band");
                assert!(matches!(columns[0].1, Expression::Case { .. }));
            }
            other => panic!("expected Project, got {other:?}"),
        }

        match LogicalPlanner::plan(
            "SELECT CASE name WHEN 'Ada' THEN 1 WHEN 'Bob' THEN 2 ELSE 0 END FROM users",
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => match &columns[0].1 {
                Expression::Case {
                    when_then,
                    else_result,
                } => {
                    assert_eq!(when_then.len(), 2);
                    assert!(else_result.is_some());
                    assert!(matches!(
                        when_then[0].0,
                        Expression::BinaryOp {
                            op: FilterOp::Eq,
                            ..
                        }
                    ));
                }
                other => panic!("expected Case, got {other:?}"),
            },
            other => panic!("expected Project, got {other:?}"),
        }
    }

    #[test]
    fn parses_is_null_and_coalesce() {
        match LogicalPlanner::plan("SELECT name FROM users WHERE name IS NOT NULL").unwrap() {
            LogicalPlan::Project { input, .. } => match input.as_ref() {
                LogicalPlan::Select {
                    predicate: Some(Expression::IsNull { negated: true, .. }),
                    ..
                } => {}
                other => panic!("expected IS NOT NULL, got {other:?}"),
            },
            other => panic!("expected Project, got {other:?}"),
        }
        match LogicalPlanner::plan("SELECT COALESCE(name, 'x') AS n FROM users").unwrap() {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(columns[0].1, Expression::Coalesce(_)));
            }
            other => panic!("expected Project, got {other:?}"),
        }
    }

    #[test]
    fn parses_is_distinct_from() {
        match LogicalPlanner::plan(
            "SELECT name FROM users WHERE age IS DISTINCT FROM 30",
        )
        .unwrap()
        {
            LogicalPlan::Project { input, .. } => match input.as_ref() {
                LogicalPlan::Select {
                    predicate: Some(Expression::IsDistinctFrom { negated: false, .. }),
                    ..
                } => {}
                other => panic!("expected IS DISTINCT FROM, got {other:?}"),
            },
            other => panic!("expected Project, got {other:?}"),
        }
        match LogicalPlanner::plan(
            "SELECT name FROM users WHERE age IS NOT DISTINCT FROM NULL",
        )
        .unwrap()
        {
            LogicalPlan::Project { input, .. } => match input.as_ref() {
                LogicalPlan::Select {
                    predicate: Some(Expression::IsDistinctFrom { negated: true, .. }),
                    ..
                } => {}
                other => panic!("expected IS NOT DISTINCT FROM, got {other:?}"),
            },
            other => panic!("expected Project, got {other:?}"),
        }
    }

    #[test]
    fn parses_any_all_quantified_cmp() {
        match LogicalPlanner::plan(
            "SELECT name FROM users WHERE age = ANY(ARRAY[10, 30])",
        )
        .unwrap()
        {
            LogicalPlan::Project { input, .. } => match input.as_ref() {
                LogicalPlan::Select {
                    predicate:
                        Some(Expression::QuantifiedCmp {
                            op: FilterOp::Eq,
                            quantifier: Quantifier::Any,
                            ..
                        }),
                    ..
                } => {}
                other => panic!("expected = ANY, got {other:?}"),
            },
            other => panic!("expected Project, got {other:?}"),
        }
        match LogicalPlanner::plan(
            "SELECT name FROM users WHERE age > ALL(ARRAY[15, 20])",
        )
        .unwrap()
        {
            LogicalPlan::Project { input, .. } => match input.as_ref() {
                LogicalPlan::Select {
                    predicate:
                        Some(Expression::QuantifiedCmp {
                            op: FilterOp::Gt,
                            quantifier: Quantifier::All,
                            ..
                        }),
                    ..
                } => {}
                other => panic!("expected > ALL, got {other:?}"),
            },
            other => panic!("expected Project, got {other:?}"),
        }
        // `= ANY(subquery)` rewrites to InSubquery.
        match LogicalPlanner::plan(
            "SELECT name FROM users WHERE age = ANY(SELECT age FROM users WHERE id = 1)",
        )
        .unwrap()
        {
            LogicalPlan::Project { input, .. } => match input.as_ref() {
                LogicalPlan::Select {
                    predicate: Some(Expression::InSubquery { negated: false, .. }),
                    ..
                } => {}
                other => panic!("expected = ANY subquery → InSubquery, got {other:?}"),
            },
            other => panic!("expected Project, got {other:?}"),
        }
    }

    #[test]
    fn parses_is_true_false_unknown() {
        match LogicalPlanner::plan(
            "SELECT name FROM users WHERE (age > 20) IS TRUE",
        )
        .unwrap()
        {
            LogicalPlan::Project { input, .. } => match input.as_ref() {
                LogicalPlan::Select {
                    predicate:
                        Some(Expression::IsBoolTest {
                            test: BoolTest::True,
                            negated: false,
                            ..
                        }),
                    ..
                } => {}
                other => panic!("expected IS TRUE, got {other:?}"),
            },
            other => panic!("expected Project, got {other:?}"),
        }
        match LogicalPlanner::plan(
            "SELECT name FROM users WHERE (age > 20) IS NOT UNKNOWN",
        )
        .unwrap()
        {
            LogicalPlan::Project { input, .. } => match input.as_ref() {
                LogicalPlan::Select {
                    predicate:
                        Some(Expression::IsBoolTest {
                            test: BoolTest::Unknown,
                            negated: true,
                            ..
                        }),
                    ..
                } => {}
                other => panic!("expected IS NOT UNKNOWN, got {other:?}"),
            },
            other => panic!("expected Project, got {other:?}"),
        }
    }

    #[test]
    fn parses_cast_and_nullif() {
        match LogicalPlanner::plan("SELECT CAST(age AS TEXT) AS a FROM users").unwrap() {
            LogicalPlan::Project { columns, .. } => match &columns[0].1 {
                Expression::Cast {
                    target: CastTarget::Text,
                    try_cast: false,
                    ..
                } => {}
                other => panic!("expected Cast TEXT, got {other:?}"),
            },
            other => panic!("expected Project, got {other:?}"),
        }
        match LogicalPlanner::plan("SELECT age::INT FROM users").unwrap() {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::Cast {
                        target: CastTarget::Int,
                        ..
                    }
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        match LogicalPlanner::plan("SELECT NULLIF(name, 'Ada') FROM users").unwrap() {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(columns[0].1, Expression::NullIf { .. }));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        assert_eq!(
            cast_sql_value(&Value::String("42".into()), CastTarget::Int, false).unwrap(),
            Value::Int(42)
        );
        assert!(            cast_sql_value(&Value::String("x".into()), CastTarget::Int, true)
            .unwrap()
            .is_null());
    }

    #[test]
    fn parses_string_scalars() {
        match LogicalPlanner::plan("SELECT LOWER(name) AS lo FROM users").unwrap() {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, .. } if name == "LOWER"
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        match LogicalPlanner::plan("SELECT UPPER(name) AS up FROM users").unwrap() {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, .. } if name == "UPPER"
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        match LogicalPlanner::plan("SELECT LENGTH(name) AS n FROM users").unwrap() {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, .. } if name == "LENGTH"
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        match LogicalPlanner::plan("SELECT OCTET_LENGTH(name), BIT_LENGTH(name) FROM users")
            .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, .. } if name == "OCTET_LENGTH"
                ));
                assert!(matches!(
                    columns[1].1,
                    Expression::ScalarFunction { ref name, .. } if name == "BIT_LENGTH"
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        match LogicalPlanner::plan("SELECT SUBSTRING(name FROM 1 FOR 2) AS s FROM users").unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "SUBSTRING" && args.len() == 3
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        match LogicalPlanner::plan("SELECT TRIM(name) AS t FROM users").unwrap() {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, .. } if name == "TRIM"
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        match LogicalPlanner::plan(
            "SELECT TRIM(BOTH 'x' FROM name) AS t FROM users",
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => match &columns[0].1 {
                Expression::ScalarFunction { name, args } => {
                    assert_eq!(name, "TRIM");
                    assert_eq!(args.len(), 3);
                    assert!(matches!(&args[1], Expression::Literal(s) if s == "BOTH"));
                    assert!(matches!(&args[2], Expression::Literal(s) if s == "x"));
                }
                other => panic!("expected TRIM, got {other:?}"),
            },
            other => panic!("expected Project, got {other:?}"),
        }
        match LogicalPlanner::plan(
            "SELECT TRIM(LEADING 'xy' FROM 'xyhello') FROM generate_series(1,1)",
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => match &columns[0].1 {
                Expression::ScalarFunction { name, args } => {
                    assert_eq!(name, "TRIM");
                    assert!(matches!(&args[1], Expression::Literal(s) if s == "LEADING"));
                }
                other => panic!("expected TRIM, got {other:?}"),
            },
            other => panic!("expected Project, got {other:?}"),
        }
    }

    #[test]
    fn parses_concat_replace_position() {
        match LogicalPlanner::plan("SELECT CONCAT(name, '-', age) AS c FROM users").unwrap() {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "CONCAT" && args.len() == 3
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        match LogicalPlanner::plan("SELECT name || '!' AS c FROM users").unwrap() {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, .. } if name == "CONCAT"
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        match LogicalPlanner::plan("SELECT REPLACE(name, 'a', 'A') AS r FROM users").unwrap() {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, .. } if name == "REPLACE"
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        match LogicalPlanner::plan("SELECT POSITION('d' IN name) AS p FROM users").unwrap() {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "STRPOS" && args.len() == 2
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
    }

    #[test]
    fn parses_math_scalars() {
        match LogicalPlanner::plan(
            "SELECT ABS(0 - age), ROUND(age / 2.0), CEIL(age / 4.0), FLOOR(age / 4.0) FROM users",
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert_eq!(columns.len(), 4);
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, .. } if name == "ABS"
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        match LogicalPlanner::plan("SELECT MOD(age, 7), POWER(2, 3) FROM users").unwrap() {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, .. } if name == "MOD"
                ));
                assert!(matches!(
                    columns[1].1,
                    Expression::ScalarFunction { ref name, .. } if name == "POWER"
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
    }

    #[test]
    fn parses_not_and_now() {
        match LogicalPlanner::plan("SELECT name FROM users WHERE NOT (age < 18)").unwrap() {
            LogicalPlan::Project { input, .. } => match input.as_ref() {
                LogicalPlan::Select {
                    predicate: Some(Expression::Not { .. }),
                    ..
                } => {}
                other => panic!("expected NOT predicate, got {other:?}"),
            },
            other => panic!("expected Project, got {other:?}"),
        }
        match LogicalPlanner::plan("SELECT NOW() AS t FROM users").unwrap() {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, .. } if name == "NOW"
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        match LogicalPlanner::plan("SELECT CURRENT_DATE AS d FROM users").unwrap() {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, .. } if name == "CURRENT_DATE"
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        let ts = utc_now_timestamp();
        assert!(ts.contains('+'), "expected timezone suffix: {ts}");
        assert_eq!(utc_now_date().len(), 10);
        assert_eq!(
            date_from_timestamp_text("2026-08-02 15:30:45+00"),
            "2026-08-02"
        );
        assert_eq!(
            time_from_timestamp_text("2026-08-02 15:30:45+00"),
            "15:30:45+00"
        );
    }

    #[test]
    fn parses_clock_statement_transaction_timestamp() {
        for sql in [
            "SELECT CLOCK_TIMESTAMP() FROM users",
            "SELECT STATEMENT_TIMESTAMP() FROM users",
            "SELECT TRANSACTION_TIMESTAMP() FROM users",
            "SELECT TIMEOFDAY() FROM users",
        ] {
            match LogicalPlanner::plan(sql).unwrap() {
                LogicalPlan::Project { columns, .. } => {
                    assert!(matches!(
                        columns[0].1,
                        Expression::ScalarFunction { ref name, .. }
                            if name == "CLOCK_TIMESTAMP"
                                || name == "STATEMENT_TIMESTAMP"
                                || name == "TRANSACTION_TIMESTAMP"
                                || name == "TIMEOFDAY"
                    ));
                }
                other => panic!("expected Project for {sql}, got {other:?}"),
            }
        }
        assert_eq!(
            format_timeofday(0, 0),
            "Thu Jan 01 00:00:00.000000 1970 UTC"
        );
        assert_eq!(
            format_timeofday(1_768_478_400, 123_456),
            "Thu Jan 15 12:00:00.123456 2026 UTC"
        );
    }

    #[test]
    fn parses_current_user_schema_catalog() {
        for sql in [
            "SELECT CURRENT_USER FROM users",
            "SELECT SESSION_USER FROM users",
            "SELECT USER FROM users",
            "SELECT CURRENT_CATALOG FROM users",
            "SELECT CURRENT_SCHEMA() FROM users",
        ] {
            match LogicalPlanner::plan(sql).unwrap() {
                LogicalPlan::Project { columns, .. } => {
                    assert!(matches!(
                        columns[0].1,
                        Expression::ScalarFunction { ref name, ref args, .. }
                            if args.is_empty()
                                && matches!(
                                    name.as_str(),
                                    "CURRENT_USER"
                                        | "SESSION_USER"
                                        | "USER"
                                        | "CURRENT_CATALOG"
                                        | "CURRENT_SCHEMA"
                                )
                    ));
                }
                other => panic!("expected Project for {sql}, got {other:?}"),
            }
        }
    }

    #[test]
    fn parses_version() {
        match LogicalPlanner::plan("SELECT VERSION() FROM users").unwrap() {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "VERSION" && args.is_empty()
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        let v = version_text();
        assert!(v.starts_with("PostgreSQL 16.0 (Takyonic "));
        assert!(v.contains(env!("CARGO_PKG_VERSION")));
    }

    #[test]
    fn parses_current_schemas() {
        match LogicalPlanner::plan("SELECT current_schemas(true) FROM users").unwrap() {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "CURRENT_SCHEMAS" && args.len() == 1
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        assert_eq!(
            crate::executor::current_schemas_array("public", false),
            "[public]"
        );
        assert_eq!(
            crate::executor::current_schemas_array("myschema, public", true),
            "[pg_catalog,myschema,public]"
        );
    }

    #[test]
    fn parses_pg_backend_pid_and_recovery() {
        for (sql, expected) in [
            ("SELECT pg_backend_pid() FROM users", "PG_BACKEND_PID"),
            ("SELECT pg_is_in_recovery() FROM users", "PG_IS_IN_RECOVERY"),
            ("SELECT pg_jit_available() FROM users", "PG_JIT_AVAILABLE"),
            ("SELECT current_query() FROM users", "CURRENT_QUERY"),
            ("SELECT pg_reload_conf() FROM users", "PG_RELOAD_CONF"),
            ("SELECT pg_rotate_logfile() FROM users", "PG_ROTATE_LOGFILE"),
            (
                "SELECT pg_notification_queue_usage() FROM users",
                "PG_NOTIFICATION_QUEUE_USAGE",
            ),
            (
                "SELECT pg_last_wal_receive_lsn() FROM users",
                "PG_LAST_WAL_RECEIVE_LSN",
            ),
            (
                "SELECT pg_is_wal_replay_paused() FROM users",
                "PG_IS_WAL_REPLAY_PAUSED",
            ),
        ] {
            match LogicalPlanner::plan(sql).unwrap() {
                LogicalPlan::Project { columns, .. } => {
                    assert!(matches!(
                        columns[0].1,
                        Expression::ScalarFunction { ref name, ref args, .. }
                            if name.as_str() == expected && args.is_empty()
                    ));
                }
                other => panic!("expected Project for {sql}, got {other:?}"),
            }
        }
    }

    #[test]
    fn parses_pg_typeof_and_encoding() {
        match LogicalPlanner::plan("SELECT pg_typeof(1), getdatabaseencoding() FROM users")
            .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "PG_TYPEOF" && args.len() == 1
                ));
                assert!(matches!(
                    columns[1].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "GETDATABASEENCODING" && args.is_empty()
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        assert_eq!(pg_typeof_value(&Value::Int(1)), Some("bigint"));
        assert_eq!(
            pg_typeof_value(&Value::Float(1.5)),
            Some("double precision")
        );
        assert_eq!(pg_typeof_value(&Value::Bool(true)), Some("boolean"));
        assert_eq!(
            pg_typeof_value(&Value::String("x".into())),
            Some("text")
        );
        assert_eq!(
            pg_typeof_value(&Value::String(encode_interval_secs(60))),
            Some("interval")
        );
        assert_eq!(pg_typeof_value(&Value::Null), None);
        assert_eq!(database_encoding(), "UTF8");
    }

    #[test]
    fn parses_pg_encoding_char_roundtrip() {
        match LogicalPlanner::plan(
            "SELECT pg_encoding_to_char(6), pg_char_to_encoding('UTF8') FROM users",
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "PG_ENCODING_TO_CHAR" && args.len() == 1
                ));
                assert!(matches!(
                    columns[1].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "PG_CHAR_TO_ENCODING" && args.len() == 1
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        assert_eq!(pg_encoding_to_char(6), "UTF8");
        assert_eq!(pg_encoding_to_char(0), "SQL_ASCII");
        assert_eq!(pg_encoding_to_char(999), "");
        assert_eq!(pg_char_to_encoding("UTF8"), 6);
        assert_eq!(pg_char_to_encoding("latin1"), 8);
        assert_eq!(pg_char_to_encoding("nope"), -1);
        assert_eq!(
            pg_char_to_encoding(pg_encoding_to_char(PG_UTF8_ENCODING)),
            PG_UTF8_ENCODING
        );
    }

    #[test]
    fn parses_pg_table_type_is_visible() {
        match LogicalPlanner::plan(
            "SELECT pg_table_is_visible('users'), pg_type_is_visible('integer') FROM users",
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "PG_TABLE_IS_VISIBLE" && args.len() == 1
                ));
                assert!(matches!(
                    columns[1].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "PG_TYPE_IS_VISIBLE" && args.len() == 1
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
    }

    #[test]
    fn parses_to_regproc_and_function_visible() {
        match LogicalPlanner::plan(
            "SELECT to_regproc('lower'), pg_function_is_visible('format_type') FROM users",
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "TO_REGPROC" && args.len() == 1
                ));
                assert!(matches!(
                    columns[1].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "PG_FUNCTION_IS_VISIBLE" && args.len() == 1
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        assert!(to_regproc("lower").is_some());
        assert!(to_regproc("nope_fn").is_none());
        assert!(pg_function_is_visible_name("format_type"));
        assert!(!pg_function_is_visible_name("nope_fn"));
        let oid = to_regproc("lower").unwrap();
        assert!(pg_function_is_visible_oid(oid));
        assert!(!pg_function_is_visible_oid(1));
    }

    #[test]
    fn parses_to_regoper_and_operator_visible() {
        match LogicalPlanner::plan(
            "SELECT to_regoper('='), pg_operator_is_visible('<->') FROM users",
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "TO_REGOPER" && args.len() == 1
                ));
                assert!(matches!(
                    columns[1].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "PG_OPERATOR_IS_VISIBLE" && args.len() == 1
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        assert_eq!(operator_name_leaf("pg_catalog.=(integer,integer)"), "=");
        assert!(to_regoper("=").is_some());
        assert!(to_regoper("<->").is_some());
        assert!(to_regoper("nope").is_none());
        assert!(pg_operator_is_visible_name("||"));
        assert!(!pg_operator_is_visible_name("@@@"));
        let oid = to_regoper("=").unwrap();
        assert!(pg_operator_is_visible_oid(oid));
        assert!(!pg_operator_is_visible_oid(1));
    }

    #[test]
    fn parses_to_regcollation_and_collation_visible() {
        match LogicalPlanner::plan(
            "SELECT to_regcollation('C'), pg_collation_is_visible('default') FROM users",
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "TO_REGCOLLATION" && args.len() == 1
                ));
                assert!(matches!(
                    columns[1].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "PG_COLLATION_IS_VISIBLE" && args.len() == 1
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        assert_eq!(collation_name_leaf("pg_catalog.\"C\""), "c");
        assert!(to_regcollation("C").is_some());
        assert!(to_regcollation("default").is_some());
        assert!(to_regcollation("nope").is_none());
        assert!(pg_collation_is_visible_name("POSIX"));
        assert!(!pg_collation_is_visible_name("en_US"));
        let oid = to_regcollation("ucs_basic").unwrap();
        assert!(pg_collation_is_visible_oid(oid));
        assert!(!pg_collation_is_visible_oid(1));
    }

    #[test]
    fn advisory_locks_session_scoped() {
        let a = alloc_advisory_session_id();
        let b = alloc_advisory_session_id();
        let key = 42_i64;
        assert!(pg_try_advisory_lock(a, key));
        assert!(pg_try_advisory_lock(a, key)); // reentrant
        assert!(!pg_try_advisory_lock(b, key));
        assert!(pg_advisory_unlock(a, key));
        assert!(!pg_try_advisory_lock(b, key)); // still held (count 1)
        assert!(pg_advisory_unlock(a, key));
        assert!(pg_try_advisory_lock(b, key));
        assert_eq!(pg_advisory_unlock_all(b), 1);
        assert!(pg_try_advisory_lock(a, key));
        let _ = pg_advisory_unlock_all(a);
        assert_eq!(
            advisory_lock_key_pair(1, 2),
            (1_i64 << 32) | 2
        );
    }

    #[test]
    fn advisory_locks_shared_session_scoped() {
        let a = alloc_advisory_session_id();
        let b = alloc_advisory_session_id();
        let key = 77_i64;
        assert!(pg_try_advisory_lock_shared(a, key));
        assert!(pg_try_advisory_lock_shared(b, key)); // compatible
        assert!(!pg_try_advisory_lock(a, key)); // exclusive blocked by shared
        assert!(pg_advisory_unlock_shared(a, key));
        assert!(!pg_try_advisory_lock(a, key)); // b still shared
        assert!(pg_advisory_unlock_shared(b, key));
        assert!(pg_try_advisory_lock(a, key));
        assert!(!pg_try_advisory_lock_shared(b, key)); // exclusive held
        let _ = pg_advisory_unlock_all(a);
        assert!(pg_try_advisory_lock_shared(a, key));
        assert!(pg_try_advisory_lock_shared(a, key)); // reentrant
        assert_eq!(pg_advisory_unlock_all(a), 1);
        assert!(pg_try_advisory_lock(b, key));
        let _ = pg_advisory_unlock_all(b);
    }

    #[test]
    fn advisory_xact_locks_release_on_end() {
        let a = alloc_advisory_session_id();
        let b = alloc_advisory_session_id();
        let key = 99_i64;
        assert!(pg_try_advisory_xact_lock(a, key));
        assert!(!pg_try_advisory_lock(b, key));
        assert_eq!(pg_advisory_xact_unlock_all(a), 1);
        assert!(pg_try_advisory_lock(b, key));
        let _ = pg_advisory_unlock_all(b);
    }

    #[test]
    fn advisory_xact_locks_shared_release_on_end() {
        let a = alloc_advisory_session_id();
        let b = alloc_advisory_session_id();
        let key = 88_i64;
        assert!(pg_try_advisory_xact_lock_shared(a, key));
        assert!(pg_try_advisory_xact_lock_shared(b, key));
        assert!(!pg_try_advisory_lock(a, key));
        assert_eq!(pg_advisory_xact_unlock_all(a), 1);
        assert!(!pg_try_advisory_lock(a, key)); // b still shared
        assert_eq!(pg_advisory_xact_unlock_all(b), 1);
        assert!(pg_try_advisory_lock(a, key));
        let _ = pg_advisory_unlock_all(a);
    }

    #[test]
    fn parses_pg_advisory_lock_fns() {
        match LogicalPlanner::plan(
            "SELECT pg_try_advisory_lock(1), pg_advisory_unlock(1, 2), pg_advisory_unlock_all() FROM users",
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "PG_TRY_ADVISORY_LOCK" && args.len() == 1
                ));
                assert!(matches!(
                    columns[1].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "PG_ADVISORY_UNLOCK" && args.len() == 2
                ));
                assert!(matches!(
                    columns[2].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "PG_ADVISORY_UNLOCK_ALL" && args.is_empty()
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
    }

    #[test]
    fn parses_pg_advisory_lock_shared_fns() {
        match LogicalPlanner::plan(
            "SELECT pg_try_advisory_lock_shared(1), pg_advisory_unlock_shared(1, 2) FROM users",
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "PG_TRY_ADVISORY_LOCK_SHARED" && args.len() == 1
                ));
                assert!(matches!(
                    columns[1].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "PG_ADVISORY_UNLOCK_SHARED" && args.len() == 2
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
    }

    #[test]
    fn parses_pg_advisory_xact_lock_fns() {
        match LogicalPlanner::plan(
            "SELECT pg_try_advisory_xact_lock(1), pg_advisory_xact_lock(1, 2) FROM users",
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "PG_TRY_ADVISORY_XACT_LOCK" && args.len() == 1
                ));
                assert!(matches!(
                    columns[1].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "PG_ADVISORY_XACT_LOCK" && args.len() == 2
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
    }

    #[test]
    fn parses_pg_advisory_xact_lock_shared_fns() {
        match LogicalPlanner::plan(
            "SELECT pg_try_advisory_xact_lock_shared(1), pg_advisory_xact_lock_shared(1, 2) FROM users",
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "PG_TRY_ADVISORY_XACT_LOCK_SHARED" && args.len() == 1
                ));
                assert!(matches!(
                    columns[1].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "PG_ADVISORY_XACT_LOCK_SHARED" && args.len() == 2
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
    }

    #[test]
    fn parses_pg_relation_is_updatable() {
        match LogicalPlanner::plan(
            "SELECT pg_relation_is_updatable('users', true), pg_relation_is_updatable(1, false) FROM users",
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "PG_RELATION_IS_UPDATABLE" && args.len() == 2
                ));
                assert!(matches!(
                    columns[1].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "PG_RELATION_IS_UPDATABLE" && args.len() == 2
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
    }

    #[test]
    fn parses_pg_column_is_updatable() {
        match LogicalPlanner::plan(
            "SELECT pg_column_is_updatable('users', 'name', true), pg_column_is_updatable(1, 2, false) FROM users",
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "PG_COLUMN_IS_UPDATABLE" && args.len() == 3
                ));
                assert!(matches!(
                    columns[1].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "PG_COLUMN_IS_UPDATABLE" && args.len() == 3
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
    }

    #[test]
    fn parses_pg_get_indexdef() {
        match LogicalPlanner::plan(
            "SELECT pg_get_indexdef('idx_dept'), pg_get_indexdef(1, 0, true) FROM users",
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "PG_GET_INDEXDEF" && args.len() == 1
                ));
                assert!(matches!(
                    columns[1].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "PG_GET_INDEXDEF" && args.len() == 3
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
    }

    #[test]
    fn parses_pg_describe_object() {
        match LogicalPlanner::plan(
            "SELECT pg_describe_object(1259, 1, 0) FROM users",
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "PG_DESCRIBE_OBJECT" && args.len() == 3
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
    }

    #[test]
    fn parses_pg_identify_object() {
        match LogicalPlanner::plan(
            "SELECT pg_identify_object(1259, 1, 0) FROM users",
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "PG_IDENTIFY_OBJECT" && args.len() == 3
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
    }

    #[test]
    fn parses_pg_size_pretty_and_client_encoding() {
        match LogicalPlanner::plan(
            "SELECT pg_size_pretty(1024), pg_client_encoding() FROM users",
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "PG_SIZE_PRETTY" && args.len() == 1
                ));
                assert!(matches!(
                    columns[1].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "PG_CLIENT_ENCODING" && args.is_empty()
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        assert_eq!(pg_size_pretty(0), "0 bytes");
        assert_eq!(pg_size_pretty(1024), "1024 bytes");
        assert_eq!(pg_size_pretty(10240), "10 kB");
        assert_eq!(pg_size_pretty(1_048_576), "1024 kB");
        assert_eq!(pg_size_pretty(10_485_760), "10 MB");
        assert_eq!(pg_size_pretty(-10240), "-10 kB");
        assert_eq!(pg_size_bytes("1024").unwrap(), 1024);
        assert_eq!(pg_size_bytes("1024 bytes").unwrap(), 1024);
        assert_eq!(pg_size_bytes("10 kB").unwrap(), 10 * 1024);
        assert_eq!(pg_size_bytes("1MB").unwrap(), 1024 * 1024);
        assert_eq!(pg_size_bytes("-10 kB").unwrap(), -10 * 1024);
        assert!(pg_size_bytes("").is_err());
        assert!(pg_size_bytes("1 XB").is_err());
    }

    #[test]
    fn parses_pg_size_bytes() {
        match LogicalPlanner::plan("SELECT pg_size_bytes('1 GB') FROM users")
            .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "PG_SIZE_BYTES" && args.len() == 1
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
    }

    #[test]
    fn parses_random_setseed() {
        match LogicalPlanner::plan("SELECT random(), setseed(0.5) FROM users").unwrap() {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "RANDOM" && args.is_empty()
                ));
                assert!(matches!(
                    columns[1].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "SETSEED" && args.len() == 1
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        setseed(0.25).unwrap();
        let a = random_f64();
        setseed(0.25).unwrap();
        let b = random_f64();
        assert_eq!(a, b);
        assert!((0.0..1.0).contains(&a));
        assert!(setseed(1.5).is_err());
    }

    #[test]
    fn parses_current_role_and_gen_random_uuid() {
        match LogicalPlanner::plan(
            "SELECT CURRENT_ROLE, gen_random_uuid() FROM users",
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "CURRENT_ROLE" && args.is_empty()
                ));
                assert!(matches!(
                    columns[1].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "GEN_RANDOM_UUID" && args.is_empty()
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        let u = gen_random_uuid();
        assert_eq!(u.len(), 36);
        assert_eq!(&u[14..15], "4");
    }

    #[test]
    fn parses_pg_sleep_and_column_size() {
        match LogicalPlanner::plan(
            "SELECT pg_sleep(0), pg_column_size('x') FROM users",
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "PG_SLEEP" && args.len() == 1
                ));
                assert!(matches!(
                    columns[1].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "PG_COLUMN_SIZE" && args.len() == 1
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        pg_sleep(0.0).unwrap();
        assert!(pg_sleep(-1.0).is_err());
        assert_eq!(pg_column_size(&Value::String("Ada".into())), Some(3));
        assert_eq!(pg_column_size(&Value::Int(1)), Some(8));
        assert_eq!(pg_column_size(&Value::Bool(true)), Some(1));
        assert_eq!(pg_column_size(&Value::Null), None);
        pg_notify("ch", "payload").unwrap();
        assert!(pg_notify("", "x").is_err());
        assert_eq!(pg_notification_queue_usage(0), 0.0);
        assert_eq!(format_listening_channels(&[]), "[]");
        assert_eq!(
            format_listening_channels(&["a".into(), "b".into()]),
            "[a,b]"
        );
    }

    #[test]
    fn parses_listen_unlisten() {
        match LogicalPlanner::plan("LISTEN alerts").unwrap() {
            LogicalPlan::Listen { channel } => assert_eq!(channel, "alerts"),
            other => panic!("expected Listen, got {other:?}"),
        }
        match LogicalPlanner::plan("UNLISTEN alerts").unwrap() {
            LogicalPlan::Unlisten {
                channel: Some(ch),
            } => assert_eq!(ch, "alerts"),
            other => panic!("expected Unlisten, got {other:?}"),
        }
        match LogicalPlanner::plan("UNLISTEN *").unwrap() {
            LogicalPlan::Unlisten { channel: None } => {}
            other => panic!("expected Unlisten *, got {other:?}"),
        }
    }

    #[test]
    fn parses_notify() {
        match LogicalPlanner::plan("NOTIFY alerts").unwrap() {
            LogicalPlan::Notify { channel, payload } => {
                assert_eq!(channel, "alerts");
                assert_eq!(payload, "");
            }
            other => panic!("expected Notify, got {other:?}"),
        }
        match LogicalPlanner::plan("NOTIFY alerts, 'hello'").unwrap() {
            LogicalPlan::Notify { channel, payload } => {
                assert_eq!(channel, "alerts");
                assert_eq!(payload, "hello");
            }
            other => panic!("expected Notify with payload, got {other:?}"),
        }
    }

    #[test]
    fn notify_delivers_to_listeners() {
        let sid = alloc_advisory_session_id();
        let _ = drain_notifications(sid);
        register_listen(sid, "alerts");
        pg_notify("alerts", "ping").unwrap();
        pg_notify("other", "nope").unwrap();
        assert!((pg_notification_queue_usage(sid) - 0.001).abs() < 1e-9);
        let pending = drain_notifications(sid);
        assert_eq!(pending, vec![("alerts".into(), "ping".into())]);
        assert_eq!(pg_notification_queue_usage(sid), 0.0);
        register_unlisten(sid, None);
    }

    #[test]
    fn parses_pg_notify() {
        match LogicalPlanner::plan(
            "SELECT pg_notify('a', 'b'), pg_notification_queue_usage(), pg_listening_channels() FROM users",
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "PG_NOTIFY" && args.len() == 2
                ));
                assert!(matches!(
                    columns[1].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "PG_NOTIFICATION_QUEUE_USAGE" && args.is_empty()
                ));
                assert!(matches!(
                    columns[2].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "PG_LISTENING_CHANNELS" && args.is_empty()
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
    }

    #[test]
    fn parses_txid_and_postmaster_start() {
        match LogicalPlanner::plan(
            "SELECT txid_current(), pg_current_xact_id(), pg_postmaster_start_time() FROM users",
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, .. } if name == "TXID_CURRENT"
                ));
                assert!(matches!(
                    columns[1].1,
                    Expression::ScalarFunction { ref name, .. } if name == "PG_CURRENT_XACT_ID"
                ));
                assert!(matches!(
                    columns[2].1,
                    Expression::ScalarFunction { ref name, .. }
                        if name == "PG_POSTMASTER_START_TIME"
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        let a = next_txid();
        let b = next_txid();
        assert!(b > a);
        let s1 = pg_postmaster_start_time();
        let s2 = pg_postmaster_start_time();
        assert_eq!(s1, s2);
        assert_eq!(txid_status(0, 5), None);
        assert_eq!(txid_status(5, 5), Some("in progress"));
        assert_eq!(txid_status(3, 5), Some("committed"));
        assert_eq!(txid_status(9, 5), None);
        let snap = pg_export_snapshot(10);
        assert!(snap.contains('-'));
        assert_eq!(pg_current_snapshot(10), "10:11:");
        assert_eq!(pg_snapshot_xmin("10:11:"), Some(10));
        assert_eq!(pg_snapshot_xmax("10:11:"), Some(11));
        assert_eq!(pg_visible_in_snapshot(5, "10:11:"), Some(true));
        assert_eq!(pg_visible_in_snapshot(10, "10:11:"), Some(true));
        assert_eq!(pg_visible_in_snapshot(11, "10:11:"), Some(false));
        assert_eq!(pg_visible_in_snapshot(10, "10:12:10"), Some(false));
        assert_eq!(pg_visible_in_snapshot(0, "10:11:"), None);
        assert!(pg_signal_backend(std::process::id() as i64));
        assert!(!pg_signal_backend(0));
        assert!(!pg_signal_backend(-1));
        assert!(!pg_signal_backend(i64::from(std::process::id()).saturating_add(1)));
        assert_eq!(format_wal_lsn(0x0100_0000), "0/01000000");
        assert_eq!(parse_wal_lsn("0/01000000"), Some(0x0100_0000));
        assert_eq!(pg_wal_lsn_diff("0/01000010", "0/01000000"), Some(0x10));
        assert_eq!(pg_wal_lsn_diff("bad", "0/1"), None);
        let lsn = pg_current_wal_lsn();
        assert!(lsn.contains('/'));
        assert_eq!(pg_wal_lsn_diff(&lsn, &lsn), Some(0));
        assert_eq!(
            pg_walfile_name("0/01000000"),
            Some("000000010000000000000001".into())
        );
        assert_eq!(
            pg_walfile_name_offset("0/01000010"),
            Some("000000010000000000000001,16".into())
        );
        assert_eq!(pg_walfile_name("bad"), None);
        let before = parse_wal_lsn(&pg_current_wal_lsn()).unwrap();
        let switched = parse_wal_lsn(&pg_switch_wal()).unwrap();
        assert!(switched > before);
        assert_eq!(switched % (16 * 1024 * 1024), 0);
        assert_eq!(parse_wal_lsn(&pg_current_wal_lsn()), Some(switched));
        pg_wal_replay_resume();
        assert!(!pg_is_wal_replay_paused());
        pg_wal_replay_pause();
        assert!(pg_is_wal_replay_paused());
        pg_wal_replay_resume();
        assert!(!pg_is_wal_replay_paused());
        // Ensure no leftover backup from a failed prior run.
        let _ = pg_backup_stop();
        assert!(!pg_is_in_backup());
        assert!(pg_backup_start_time().is_none());
        let start_lsn = pg_backup_start("lab").unwrap();
        assert!(pg_is_in_backup());
        assert!(pg_backup_start_time().is_some());
        assert!(pg_backup_start("x").is_err());
        let stop_lsn = pg_backup_stop().unwrap();
        assert!(stop_lsn.contains('/'));
        assert!(start_lsn.contains('/'));
        assert!(!pg_is_in_backup());
        assert!(pg_backup_stop().is_err());
        assert!(pg_create_restore_point("").is_err());
        let rp = pg_create_restore_point("before_ddl").unwrap();
        assert!(rp.contains('/'));
        assert_eq!(rp, pg_current_wal_lsn());
        assert!(!pg_promote());
        let conf0 = pg_conf_load_time();
        assert!(!conf0.is_empty());
        assert!(pg_reload_conf());
        assert_ne!(pg_conf_load_time(), conf0);
        assert!(pg_rotate_logfile());
    }

    #[test]
    fn parses_pg_conf_load_time() {
        match LogicalPlanner::plan("SELECT pg_conf_load_time() FROM users").unwrap() {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "PG_CONF_LOAD_TIME" && args.is_empty()
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
    }

    #[test]
    fn parses_and_runs_sequence_scalars() {
        match LogicalPlanner::plan(
            "SELECT nextval('s'), currval('s'), lastval(), setval('s', 10), setval('s', 20, false) FROM users",
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, .. } if name == "NEXTVAL"
                ));
                assert!(matches!(
                    columns[1].1,
                    Expression::ScalarFunction { ref name, .. } if name == "CURRVAL"
                ));
                assert!(matches!(
                    columns[2].1,
                    Expression::ScalarFunction { ref name, .. } if name == "LASTVAL"
                ));
                assert!(matches!(
                    columns[3].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "SETVAL" && args.len() == 2
                ));
                assert!(matches!(
                    columns[4].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "SETVAL" && args.len() == 3
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        let sid = alloc_advisory_session_id();
        let name = format!("seq_unit_{sid}");
        assert_eq!(nextval(sid, &name).unwrap(), 1);
        assert_eq!(nextval(sid, &name).unwrap(), 2);
        assert_eq!(currval(sid, &name).unwrap(), 2);
        assert_eq!(lastval(sid).unwrap(), 2);
        assert_eq!(setval(sid, &name, 100, true).unwrap(), 100);
        assert_eq!(nextval(sid, &name).unwrap(), 101);
        assert_eq!(setval(sid, &name, 5, false).unwrap(), 5);
        assert_eq!(nextval(sid, &name).unwrap(), 5);
        let other = alloc_advisory_session_id();
        assert!(currval(other, &name).is_err());
        assert!(lastval(other).is_err());
    }

    #[test]
    fn parses_create_drop_sequence() {
        match LogicalPlanner::plan(
            "CREATE SEQUENCE s1 START WITH 10 INCREMENT BY 2",
        )
        .unwrap()
        {
            LogicalPlan::CreateSequence {
                name,
                if_not_exists,
                start,
                increment,
            } => {
                assert_eq!(name, "s1");
                assert!(!if_not_exists);
                assert_eq!(start, 10);
                assert_eq!(increment, 2);
            }
            other => panic!("expected CreateSequence, got {other:?}"),
        }
        match LogicalPlanner::plan("CREATE SEQUENCE IF NOT EXISTS s2").unwrap() {
            LogicalPlan::CreateSequence {
                if_not_exists: true,
                start: 1,
                increment: 1,
                ..
            } => {}
            other => panic!("expected IF NOT EXISTS, got {other:?}"),
        }
        match LogicalPlanner::plan("DROP SEQUENCE IF EXISTS s1").unwrap() {
            LogicalPlan::DropSequence {
                name,
                if_exists: true,
            } => assert_eq!(name, "s1"),
            other => panic!("expected DropSequence, got {other:?}"),
        }
        let sid = alloc_advisory_session_id();
        let name = format!("ddl_seq_{sid}");
        create_sequence(&name, false, 5, 3).unwrap();
        assert!(create_sequence(&name, false, 1, 1).is_err());
        create_sequence(&name, true, 1, 1).unwrap();
        assert_eq!(nextval(sid, &name).unwrap(), 5);
        assert_eq!(nextval(sid, &name).unwrap(), 8);
        drop_sequence(&name, false).unwrap();
        drop_sequence(&name, true).unwrap();
        assert!(drop_sequence(&name, false).is_err());
    }

    #[test]
    fn parses_alter_sequence_and_serial() {
        match LogicalPlanner::plan(
            "ALTER SEQUENCE s1 RESTART WITH 100 INCREMENT BY 2 OWNED BY users.id",
        )
        .unwrap()
        {
            LogicalPlan::AlterSequence {
                name,
                restart: Some(100),
                increment: Some(2),
                owned_by: Some(Some((t, c))),
                rename_to: None,
            } => {
                assert_eq!(name, "s1");
                assert_eq!(t, "users");
                assert_eq!(c, "id");
            }
            other => panic!("expected AlterSequence, got {other:?}"),
        }
        match LogicalPlanner::plan("ALTER SEQUENCE s1 OWNED BY NONE").unwrap() {
            LogicalPlan::AlterSequence {
                owned_by: Some(None),
                rename_to: None,
                ..
            } => {}
            other => panic!("expected OWNED BY NONE, got {other:?}"),
        }
        match LogicalPlanner::plan("ALTER SEQUENCE s1 RENAME TO s2").unwrap() {
            LogicalPlan::AlterSequence {
                name,
                rename_to: Some(new),
                restart: None,
                increment: None,
                owned_by: None,
            } => {
                assert_eq!(name, "s1");
                assert_eq!(new, "s2");
            }
            other => panic!("expected RENAME TO, got {other:?}"),
        }
        match LogicalPlanner::plan(
            "SELECT pg_get_serial_sequence('users', 'id') FROM users",
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "PG_GET_SERIAL_SEQUENCE" && args.len() == 2
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        let sid = alloc_advisory_session_id();
        let name = format!("alter_seq_{sid}");
        let renamed = format!("alter_seq_renamed_{sid}");
        create_sequence(&name, false, 1, 1).unwrap();
        alter_sequence(
            &name,
            Some(50),
            Some(10),
            Some(Some(("t".into(), "id".into()))),
            None,
        )
        .unwrap();
        assert_eq!(
            pg_get_serial_sequence("t", "id"),
            Some(format!("public.{name}"))
        );
        assert_eq!(nextval(sid, &name).unwrap(), 50);
        assert_eq!(nextval(sid, &name).unwrap(), 60);
        alter_sequence(&name, None, None, None, Some(&renamed)).unwrap();
        assert_eq!(
            pg_get_serial_sequence("t", "id"),
            Some(format!("public.{renamed}"))
        );
        assert_eq!(nextval(sid, &renamed).unwrap(), 70);
        alter_sequence(&renamed, None, None, Some(None), None).unwrap();
        assert_eq!(pg_get_serial_sequence("t", "id"), None);
        drop_sequence(&renamed, false).unwrap();
    }

    #[test]
    fn parses_and_runs_pg_sequence_last_value() {
        match LogicalPlanner::plan("SELECT pg_sequence_last_value('s') FROM users").unwrap() {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "PG_SEQUENCE_LAST_VALUE" && args.len() == 1
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        let sid = alloc_advisory_session_id();
        let name = format!("last_seq_{sid}");
        create_sequence(&name, false, 1, 1).unwrap();
        assert_eq!(pg_sequence_last_value(&name).unwrap(), None);
        assert_eq!(nextval(sid, &name).unwrap(), 1);
        assert_eq!(pg_sequence_last_value(&name).unwrap(), Some(1));
        assert_eq!(nextval(sid, &name).unwrap(), 2);
        assert_eq!(pg_sequence_last_value(&name).unwrap(), Some(2));
        drop_sequence(&name, false).unwrap();
        assert!(pg_sequence_last_value(&name).is_err());
    }

    #[test]
    fn parses_has_sequence_privilege() {
        match LogicalPlanner::plan(
            "SELECT has_sequence_privilege('s', 'USAGE'), has_sequence_privilege('u', 's', 'SELECT') FROM users",
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "HAS_SEQUENCE_PRIVILEGE" && args.len() == 2
                ));
                assert!(matches!(
                    columns[1].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "HAS_SEQUENCE_PRIVILEGE" && args.len() == 3
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        let name = format!("priv_seq_{}", alloc_advisory_session_id());
        create_sequence(&name, false, 1, 1).unwrap();
        assert!(crate::rbac::has_sequence_privilege(
            false,
            &name,
            &[crate::rbac::SequencePrivilege::Usage],
            sequence_exists,
        )
        .unwrap());
        assert!(crate::rbac::has_sequence_privilege(
            true,
            &name,
            &[crate::rbac::SequencePrivilege::Update],
            sequence_exists,
        )
        .unwrap());
        assert!(crate::rbac::has_sequence_privilege(
            false,
            "missing_seq_xyz",
            &[crate::rbac::SequencePrivilege::Usage],
            sequence_exists,
        )
        .is_err());
        drop_sequence(&name, false).unwrap();
    }

    #[test]
    fn parses_txid_status() {
        match LogicalPlanner::plan(
            "SELECT txid_status(1), pg_xact_status(2) FROM users",
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "TXID_STATUS" && args.len() == 1
                ));
                assert!(matches!(
                    columns[1].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "PG_XACT_STATUS" && args.len() == 1
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
    }

    #[test]
    fn parses_pg_snapshot_fns() {
        match LogicalPlanner::plan(
            "SELECT pg_export_snapshot(), pg_current_snapshot(), txid_current_snapshot() FROM users",
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, .. }
                        if name == "PG_EXPORT_SNAPSHOT"
                ));
                assert!(matches!(
                    columns[1].1,
                    Expression::ScalarFunction { ref name, .. }
                        if name == "PG_CURRENT_SNAPSHOT"
                ));
                assert!(matches!(
                    columns[2].1,
                    Expression::ScalarFunction { ref name, .. }
                        if name == "TXID_CURRENT_SNAPSHOT"
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
    }

    #[test]
    fn parses_pg_snapshot_inspect_fns() {
        match LogicalPlanner::plan(
            "SELECT pg_snapshot_xmin('1:2:'), pg_snapshot_xmax('1:2:'), pg_visible_in_snapshot(1, '1:2:') FROM users",
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "PG_SNAPSHOT_XMIN" && args.len() == 1
                ));
                assert!(matches!(
                    columns[1].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "PG_SNAPSHOT_XMAX" && args.len() == 1
                ));
                assert!(matches!(
                    columns[2].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "PG_VISIBLE_IN_SNAPSHOT" && args.len() == 2
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
    }

    #[test]
    fn parses_pg_signal_backend_fns() {
        match LogicalPlanner::plan(
            "SELECT pg_cancel_backend(1), pg_terminate_backend(2) FROM users",
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "PG_CANCEL_BACKEND" && args.len() == 1
                ));
                assert!(matches!(
                    columns[1].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "PG_TERMINATE_BACKEND" && args.len() == 1
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
    }

    #[test]
    fn parses_pg_wal_lsn_fns() {
        match LogicalPlanner::plan(
            "SELECT pg_current_wal_lsn(), pg_current_wal_insert_lsn(), pg_current_wal_flush_lsn(), pg_wal_lsn_diff('0/1','0/0') FROM users",
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, .. }
                        if name == "PG_CURRENT_WAL_LSN"
                ));
                assert!(matches!(
                    columns[1].1,
                    Expression::ScalarFunction { ref name, .. }
                        if name == "PG_CURRENT_WAL_INSERT_LSN"
                ));
                assert!(matches!(
                    columns[2].1,
                    Expression::ScalarFunction { ref name, .. }
                        if name == "PG_CURRENT_WAL_FLUSH_LSN"
                ));
                assert!(matches!(
                    columns[3].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "PG_WAL_LSN_DIFF" && args.len() == 2
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
    }

    #[test]
    fn parses_pg_walfile_name_fns() {
        match LogicalPlanner::plan(
            "SELECT pg_walfile_name('0/1'), pg_walfile_name_offset('0/1') FROM users",
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "PG_WALFILE_NAME" && args.len() == 1
                ));
                assert!(matches!(
                    columns[1].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "PG_WALFILE_NAME_OFFSET" && args.len() == 1
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
    }

    #[test]
    fn parses_pg_switch_wal() {
        match LogicalPlanner::plan(
            "SELECT pg_switch_wal(), pg_switch_xlog() FROM users",
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, .. }
                        if name == "PG_SWITCH_WAL"
                ));
                assert!(matches!(
                    columns[1].1,
                    Expression::ScalarFunction { ref name, .. }
                        if name == "PG_SWITCH_XLOG"
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
    }

    #[test]
    fn parses_pg_standby_wal_fns() {
        match LogicalPlanner::plan(
            "SELECT pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn(), \
             pg_last_xact_replay_timestamp(), pg_is_wal_replay_paused() FROM users",
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, .. }
                        if name == "PG_LAST_WAL_RECEIVE_LSN"
                ));
                assert!(matches!(
                    columns[1].1,
                    Expression::ScalarFunction { ref name, .. }
                        if name == "PG_LAST_WAL_REPLAY_LSN"
                ));
                assert!(matches!(
                    columns[2].1,
                    Expression::ScalarFunction { ref name, .. }
                        if name == "PG_LAST_XACT_REPLAY_TIMESTAMP"
                ));
                assert!(matches!(
                    columns[3].1,
                    Expression::ScalarFunction { ref name, .. }
                        if name == "PG_IS_WAL_REPLAY_PAUSED"
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
    }

    #[test]
    fn parses_pg_wal_replay_pause_resume() {
        match LogicalPlanner::plan(
            "SELECT pg_wal_replay_pause(), pg_wal_replay_resume() FROM users",
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, .. }
                        if name == "PG_WAL_REPLAY_PAUSE"
                ));
                assert!(matches!(
                    columns[1].1,
                    Expression::ScalarFunction { ref name, .. }
                        if name == "PG_WAL_REPLAY_RESUME"
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
    }

    #[test]
    fn parses_pg_backup_fns() {
        match LogicalPlanner::plan(
            "SELECT pg_is_in_backup(), pg_backup_start_time(), pg_backup_start('l'), pg_backup_stop() FROM users",
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, .. }
                        if name == "PG_IS_IN_BACKUP"
                ));
                assert!(matches!(
                    columns[1].1,
                    Expression::ScalarFunction { ref name, .. }
                        if name == "PG_BACKUP_START_TIME"
                ));
                assert!(matches!(
                    columns[2].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "PG_BACKUP_START" && args.len() == 1
                ));
                assert!(matches!(
                    columns[3].1,
                    Expression::ScalarFunction { ref name, .. }
                        if name == "PG_BACKUP_STOP"
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
    }

    #[test]
    fn parses_pg_create_restore_point() {
        match LogicalPlanner::plan(
            "SELECT pg_create_restore_point('rp1') FROM users",
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "PG_CREATE_RESTORE_POINT" && args.len() == 1
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
    }

    #[test]
    fn parses_pg_promote() {
        match LogicalPlanner::plan(
            "SELECT pg_promote(), pg_promote(true, 30) FROM users",
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "PG_PROMOTE" && args.is_empty()
                ));
                assert!(matches!(
                    columns[1].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "PG_PROMOTE" && args.len() == 2
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
    }

    #[test]
    fn parses_current_setting() {
        match LogicalPlanner::plan(
            "SELECT current_setting('search_path'), current_setting('x', true) FROM users",
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "CURRENT_SETTING" && args.len() == 1
                ));
                assert!(matches!(
                    columns[1].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "CURRENT_SETTING" && args.len() == 2
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        assert_eq!(
            current_setting_value(
                "search_path",
                "public",
                "repeatable read",
                "postgres",
                "postgres",
                "UTC",
            ),
            Some("public".into())
        );
        assert_eq!(
            current_setting_value("nope", "public", "repeatable read", "postgres", "postgres", "UTC"),
            None
        );
    }

    #[test]
    fn parses_set_config() {
        clear_guc_overlay();
        match LogicalPlanner::plan(
            "SELECT set_config('search_path', 'public', false) FROM users",
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "SET_CONFIG" && args.len() == 3
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        let v = set_config("search_path", "a, b", false, false).unwrap();
        assert_eq!(v, "a, b");
        assert_eq!(
            current_setting_value(
                "search_path",
                "public",
                "repeatable read",
                "postgres",
                "postgres",
                "UTC",
            ),
            Some("a, b".into())
        );
        let err = set_config("search_path", "x", true, false).unwrap_err();
        assert!(err.to_string().contains("SET LOCAL"));
        clear_guc_overlay();
    }

    #[test]
    fn parses_has_table_privilege() {
        match LogicalPlanner::plan(
            "SELECT has_table_privilege('t', 'SELECT'), has_table_privilege('u', 't', 'INSERT') FROM users",
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "HAS_TABLE_PRIVILEGE" && args.len() == 2
                ));
                assert!(matches!(
                    columns[1].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "HAS_TABLE_PRIVILEGE" && args.len() == 3
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
    }

    #[test]
    fn parses_has_column_privilege() {
        match LogicalPlanner::plan(
            "SELECT has_column_privilege('t', 'c', 'SELECT'), has_column_privilege('u', 't', 'c', 'UPDATE') FROM users",
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "HAS_COLUMN_PRIVILEGE" && args.len() == 3
                ));
                assert!(matches!(
                    columns[1].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "HAS_COLUMN_PRIVILEGE" && args.len() == 4
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
    }

    #[test]
    fn parses_has_any_column_privilege() {
        match LogicalPlanner::plan(
            "SELECT has_any_column_privilege('t', 'SELECT'), has_any_column_privilege('u', 't', 'UPDATE') FROM users",
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "HAS_ANY_COLUMN_PRIVILEGE" && args.len() == 2
                ));
                assert!(matches!(
                    columns[1].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "HAS_ANY_COLUMN_PRIVILEGE" && args.len() == 3
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
    }

    #[test]
    fn parses_comment_and_descriptions() {
        match LogicalPlanner::plan("COMMENT ON TABLE users IS 'people'").unwrap() {
            LogicalPlan::Comment {
                object_type,
                table,
                column,
                comment,
            } => {
                assert_eq!(object_type, "table");
                assert_eq!(table, "users");
                assert!(column.is_none());
                assert_eq!(comment.as_deref(), Some("people"));
            }
            other => panic!("expected Comment, got {other:?}"),
        }
        match LogicalPlanner::plan("COMMENT ON COLUMN users.name IS 'full name'").unwrap() {
            LogicalPlan::Comment {
                object_type,
                table,
                column,
                comment,
            } => {
                assert_eq!(object_type, "column");
                assert_eq!(table, "users");
                assert_eq!(column.as_deref(), Some("name"));
                assert_eq!(comment.as_deref(), Some("full name"));
            }
            other => panic!("expected Comment, got {other:?}"),
        }
        match LogicalPlanner::plan(
            "SELECT obj_description('users'), col_description('users', 'name') FROM users",
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "OBJ_DESCRIPTION" && args.len() == 1
                ));
                assert!(matches!(
                    columns[1].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "COL_DESCRIPTION" && args.len() == 2
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        match LogicalPlanner::plan(
            "SELECT to_regclass('users'), obj_description(1, 'pg_class') FROM users",
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "TO_REGCLASS" && args.len() == 1
                ));
                assert!(matches!(
                    columns[1].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "OBJ_DESCRIPTION" && args.len() == 2
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        match LogicalPlanner::plan(
            "SELECT format_type(20, NULL), pg_get_userbyid(1) FROM users",
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "FORMAT_TYPE" && args.len() == 2
                ));
                assert!(matches!(
                    columns[1].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "PG_GET_USERBYID" && args.len() == 1
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        match LogicalPlanner::plan("SELECT to_regrole('postgres') FROM users").unwrap() {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "TO_REGROLE" && args.len() == 1
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        match LogicalPlanner::plan(
            "SELECT to_regnamespace('public'), to_regtype('bigint') FROM users",
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, .. }
                        if name == "TO_REGNAMESPACE"
                ));
                assert!(matches!(
                    columns[1].1,
                    Expression::ScalarFunction { ref name, .. }
                        if name == "TO_REGTYPE"
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        match LogicalPlanner::plan(
            "SELECT pg_relation_size('users'), pg_table_size(1), pg_total_relation_size('users') FROM users",
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "PG_RELATION_SIZE" && args.len() == 1
                ));
                assert!(matches!(
                    columns[1].1,
                    Expression::ScalarFunction { ref name, .. }
                        if name == "PG_TABLE_SIZE"
                ));
                assert!(matches!(
                    columns[2].1,
                    Expression::ScalarFunction { ref name, .. }
                        if name == "PG_TOTAL_RELATION_SIZE"
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        match LogicalPlanner::plan(
            "SELECT pg_indexes_size('users'), pg_database_size('postgres') FROM users",
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, .. }
                        if name == "PG_INDEXES_SIZE"
                ));
                assert!(matches!(
                    columns[1].1,
                    Expression::ScalarFunction { ref name, .. }
                        if name == "PG_DATABASE_SIZE"
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        match LogicalPlanner::plan("COMMENT ON ROLE analyst IS 'readonly'").unwrap() {
            LogicalPlan::Comment {
                object_type,
                table,
                comment,
                ..
            } => {
                assert_eq!(object_type, "role");
                assert_eq!(table, "analyst");
                assert_eq!(comment.as_deref(), Some("readonly"));
            }
            other => panic!("expected Comment role, got {other:?}"),
        }
        match LogicalPlanner::plan(
            "SELECT shobj_description('analyst', 'pg_authid') FROM users",
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "SHOBJ_DESCRIPTION" && args.len() == 2
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
    }

    #[test]
    fn parses_has_schema_privilege() {
        match LogicalPlanner::plan(
            "SELECT has_schema_privilege('public', 'USAGE'), has_schema_privilege('u', 'public', 'CREATE') FROM users",
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "HAS_SCHEMA_PRIVILEGE" && args.len() == 2
                ));
                assert!(matches!(
                    columns[1].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "HAS_SCHEMA_PRIVILEGE" && args.len() == 3
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
    }

    #[test]
    fn parses_has_database_privilege() {
        match LogicalPlanner::plan(
            "SELECT has_database_privilege('postgres', 'CONNECT'), has_database_privilege('u', 'postgres', 'CREATE') FROM users",
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "HAS_DATABASE_PRIVILEGE" && args.len() == 2
                ));
                assert!(matches!(
                    columns[1].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "HAS_DATABASE_PRIVILEGE" && args.len() == 3
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
    }

    #[test]
    fn parses_has_tablespace_privilege() {
        match LogicalPlanner::plan(
            "SELECT has_tablespace_privilege('pg_default', 'CREATE'), has_tablespace_privilege('u', 'pg_default', 'ALL') FROM users",
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "HAS_TABLESPACE_PRIVILEGE" && args.len() == 2
                ));
                assert!(matches!(
                    columns[1].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "HAS_TABLESPACE_PRIVILEGE" && args.len() == 3
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        assert!(crate::rbac::has_tablespace_privilege(
            true,
            "pg_default",
            &[crate::rbac::TablespacePrivilege::Create]
        )
        .unwrap());
        assert!(!crate::rbac::has_tablespace_privilege(
            false,
            "pg_default",
            &[crate::rbac::TablespacePrivilege::Create]
        )
        .unwrap());
        assert!(crate::rbac::has_tablespace_privilege(
            true,
            "missing_ts",
            &[crate::rbac::TablespacePrivilege::Create]
        )
        .is_err());
    }

    #[test]
    fn parses_pg_tablespace_location() {
        match LogicalPlanner::plan(
            "SELECT pg_tablespace_location('pg_default'), pg_tablespace_location(1663) FROM users",
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "PG_TABLESPACE_LOCATION" && args.len() == 1
                ));
                assert!(matches!(
                    columns[1].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "PG_TABLESPACE_LOCATION" && args.len() == 1
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        assert_eq!(
            crate::rbac::pg_tablespace_location("pg_default").unwrap(),
            ""
        );
        assert_eq!(crate::rbac::pg_tablespace_location("1664").unwrap(), "");
        assert!(crate::rbac::pg_tablespace_location("nope").is_err());
    }

    #[test]
    fn parses_has_function_privilege() {
        match LogicalPlanner::plan(
            "SELECT has_function_privilege('format_type', 'EXECUTE'), has_function_privilege('u', 'lower', 'EXECUTE') FROM users",
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "HAS_FUNCTION_PRIVILEGE" && args.len() == 2
                ));
                assert!(matches!(
                    columns[1].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "HAS_FUNCTION_PRIVILEGE" && args.len() == 3
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
    }

    #[test]
    fn parses_pg_has_role() {
        match LogicalPlanner::plan(
            "SELECT pg_has_role('analysts', 'MEMBER'), pg_has_role('u', 'analysts', 'USAGE') FROM users",
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "PG_HAS_ROLE" && args.len() == 2
                ));
                assert!(matches!(
                    columns[1].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "PG_HAS_ROLE" && args.len() == 3
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
    }

    #[test]
    fn parses_has_type_privilege() {
        match LogicalPlanner::plan(
            "SELECT has_type_privilege('integer', 'USAGE'), has_type_privilege('u', 'text', 'USAGE') FROM users",
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "HAS_TYPE_PRIVILEGE" && args.len() == 2
                ));
                assert!(matches!(
                    columns[1].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "HAS_TYPE_PRIVILEGE" && args.len() == 3
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
    }

    #[test]
    fn parses_inet_addr_port() {
        match LogicalPlanner::plan(
            "SELECT inet_server_addr(), inet_server_port(), inet_client_addr(), inet_client_port() FROM users",
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert_eq!(columns.len(), 4);
                for (i, expected) in [
                    "INET_SERVER_ADDR",
                    "INET_SERVER_PORT",
                    "INET_CLIENT_ADDR",
                    "INET_CLIENT_PORT",
                ]
                .iter()
                .enumerate()
                {
                    assert!(matches!(
                        &columns[i].1,
                        Expression::ScalarFunction { name, args, .. }
                            if name == *expected && args.is_empty()
                    ));
                }
            }
            other => panic!("expected Project, got {other:?}"),
        }
    }

    #[test]
    fn parses_num_nulls_nonnulls() {
        match LogicalPlanner::plan("SELECT num_nonnulls(1, NULL, 2), num_nulls(1, NULL) FROM users")
            .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "NUM_NONNULLS" && args.len() == 3
                ));
                assert!(matches!(
                    columns[1].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "NUM_NULLS" && args.len() == 2
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
    }

    #[test]
    fn parses_greatest_least_extract() {
        match LogicalPlanner::plan("SELECT GREATEST(age, 25), LEAST(age, 25) FROM users")
            .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, .. } if name == "GREATEST"
                ));
                assert!(matches!(
                    columns[1].1,
                    Expression::ScalarFunction { ref name, .. } if name == "LEAST"
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        match LogicalPlanner::plan("SELECT EXTRACT(YEAR FROM CURRENT_DATE) FROM users").unwrap() {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "EXTRACT" && args.len() == 2
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        assert_eq!(
            parse_timestamp_parts("2026-08-02 12:30:45+00"),
            Some((2026, 8, 2, 12, 30, 45))
        );
        assert_eq!(day_of_year(2026, 8, 2), 214);
    }

    #[test]
    fn parses_interval_and_arith() {
        match LogicalPlanner::plan("SELECT INTERVAL '1' DAY FROM users").unwrap() {
            LogicalPlan::Project { columns, .. } => {
                assert_eq!(
                    columns[0].1,
                    Expression::Literal(encode_interval_secs(86_400))
                );
            }
            other => panic!("expected Project, got {other:?}"),
        }
        match LogicalPlanner::plan(
            "SELECT '2026-01-15' + INTERVAL '1 day' AS d FROM users",
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(columns[0].1, Expression::Arith { .. }));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        assert_eq!(parse_interval_to_secs("1", Some(&DateTimeField::Day)).unwrap(), 86_400);
        assert_eq!(parse_interval_to_secs("2 hours", None).unwrap(), 7_200);
        assert_eq!(parse_interval_to_secs("02:00:00", None).unwrap(), 7_200);
        assert_eq!(format_interval_secs(86_400), "1 day");
        assert_eq!(
            add_secs_to_timestamp_text("2026-01-15", 86_400).unwrap(),
            "2026-01-16"
        );
        let unix = timestamp_to_unix(2026, 1, 15, 0, 0, 0);
        assert_eq!(format_unix_timestamp(unix, true), "2026-01-15");
    }

    #[test]
    fn parses_date_trunc() {
        match LogicalPlanner::plan(
            "SELECT DATE_TRUNC('day', '2026-08-02 15:30:45') FROM users",
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, .. } if name == "DATE_TRUNC"
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        assert_eq!(
            date_trunc_text("day", "2026-08-02 15:30:45").unwrap(),
            "2026-08-02 00:00:00+00"
        );
        assert_eq!(
            date_trunc_text("hour", "2026-08-02 15:30:45").unwrap(),
            "2026-08-02 15:00:00+00"
        );
        assert_eq!(
            date_trunc_text("month", "2026-08-02 15:30:45").unwrap(),
            "2026-08-01 00:00:00+00"
        );
        assert_eq!(
            date_trunc_text("year", "2026-08-02").unwrap(),
            "2026-01-01 00:00:00+00"
        );
        assert_eq!(
            date_trunc_text("week", "2026-08-02").unwrap(),
            "2026-07-27 00:00:00+00"
        );
    }

    #[test]
    fn parses_make_date_time_timestamp() {
        match LogicalPlanner::plan("SELECT MAKE_DATE(2026, 8, 2) FROM users")
            .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "MAKE_DATE" && args.len() == 3
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        match LogicalPlanner::plan("SELECT MAKE_TIME(15, 30, 45) FROM users")
            .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, .. } if name == "MAKE_TIME"
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        match LogicalPlanner::plan(
            "SELECT MAKE_TIMESTAMP(2026, 8, 2, 15, 30, 45) FROM users",
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "MAKE_TIMESTAMP" && args.len() == 6
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        assert_eq!(make_date_text(2026, 8, 2).unwrap(), "2026-08-02");
        assert_eq!(make_date_text(2024, 2, 29).unwrap(), "2024-02-29");
        assert!(make_date_text(2026, 2, 29).is_err());
        assert_eq!(make_time_text(15, 30, 45.0).unwrap(), "15:30:45");
        assert!(make_time_text(24, 0, 0.0).is_err());
        assert_eq!(
            make_timestamp_text(2026, 8, 2, 15, 30, 45.0).unwrap(),
            "2026-08-02 15:30:45+00"
        );
    }

    #[test]
    fn parses_make_interval() {
        match LogicalPlanner::plan("SELECT MAKE_INTERVAL(0, 0, 0, 1, 2, 30, 0) FROM users")
            .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "MAKE_INTERVAL" && args.len() == 7
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        match LogicalPlanner::plan("SELECT MAKE_INTERVAL(0, 0, 0, 1) FROM users")
            .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "MAKE_INTERVAL" && args.len() == 4
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        assert_eq!(
            make_interval_secs(0, 0, 0, 1, 0, 0, 0.0),
            86_400
        );
        assert_eq!(
            make_interval_secs(0, 0, 0, 0, 2, 30, 0.0),
            2 * 3600 + 30 * 60
        );
        assert_eq!(
            make_interval_secs(1, 0, 0, 0, 0, 0, 0.0),
            365 * 86_400
        );
        assert_eq!(
            format_interval_secs(make_interval_secs(0, 0, 0, 1, 2, 0, 0.0)),
            "1 day 02:00:00"
        );
    }

    #[test]
    fn parses_justify_interval() {
        match LogicalPlanner::plan("SELECT JUSTIFY_HOURS(INTERVAL '25 hours') FROM users")
            .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, .. } if name == "JUSTIFY_HOURS"
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        for name in ["JUSTIFY_DAYS", "JUSTIFY_INTERVAL"] {
            let sql = format!("SELECT {name}(INTERVAL '40 days') FROM users");
            match LogicalPlanner::plan(&sql).unwrap() {
                LogicalPlan::Project { columns, .. } => {
                    assert!(matches!(
                        &columns[0].1,
                        Expression::ScalarFunction { name: n, .. } if n == name
                    ));
                }
                other => panic!("expected Project for {name}, got {other:?}"),
            }
        }
        // Hours fold into days; days ≥ 30 fold into months.
        assert_eq!(
            format_interval_secs(25 * 3600),
            "1 day 01:00:00"
        );
        assert_eq!(format_interval_secs(40 * 86_400), "1 mon 10 days");
        assert_eq!(
            format_interval_secs(make_interval_secs(0, 1, 0, 0, 0, 0, 0.0)),
            "1 mon"
        );
        assert_eq!(
            justify_interval_arg(&encode_interval_secs(40 * 86_400)).unwrap(),
            encode_interval_secs(40 * 86_400)
        );
    }

    #[test]
    fn parses_isfinite() {
        match LogicalPlanner::plan("SELECT ISFINITE('2026-08-02') FROM users")
            .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "ISFINITE" && args.len() == 1
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        assert_eq!(is_finite_text("2026-08-02").unwrap(), true);
        assert_eq!(is_finite_text("2026-08-02 15:30:45+00").unwrap(), true);
        assert_eq!(is_finite_text("infinity").unwrap(), false);
        assert_eq!(is_finite_text("-Infinity").unwrap(), false);
        assert_eq!(
            is_finite_text(&encode_interval_secs(86_400)).unwrap(),
            true
        );
        assert!(is_finite_text("not-a-timestamp").is_err());
    }

    #[test]
    fn extract_epoch_unit() {
        assert_eq!(extract_epoch_secs("1970-01-01 00:00:00").unwrap(), 0.0);
        assert_eq!(
            extract_epoch_secs("2026-01-15 12:00:00+00").unwrap(),
            1_768_478_400.0
        );
        assert_eq!(
            extract_epoch_secs("1970-01-01 01:00:00+01").unwrap(),
            0.0
        );
        assert_eq!(
            extract_epoch_secs(&encode_interval_secs(86_400)).unwrap(),
            86_400.0
        );
        assert_eq!(extract_epoch_secs("1 day").unwrap(), 86_400.0);
        assert!(extract_epoch_secs("infinity").unwrap().is_infinite());
        assert!(extract_epoch_secs("not-a-ts").is_err());
        match LogicalPlanner::plan(
            "SELECT EXTRACT(EPOCH FROM '1970-01-01 00:00:00') FROM users",
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "EXTRACT" && args.len() == 2
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
    }

    #[test]
    fn overlaps_unit() {
        assert!(periods_overlap(
            "2001-02-16",
            "2001-12-21",
            "2001-10-30",
            "2002-10-30"
        )
        .unwrap());
        assert!(!periods_overlap(
            "2001-02-16",
            "2001-12-21",
            "2002-01-01",
            "2002-10-30"
        )
        .unwrap());
        assert!(!periods_overlap(
            "2001-01-01",
            "2001-01-10",
            "2001-01-10",
            "2001-01-20"
        )
        .unwrap());
        assert!(!periods_overlap(
            "2001-02-16",
            &encode_interval_secs(100 * 86_400),
            "2001-10-30",
            "2002-10-30"
        )
        .unwrap());
        match LogicalPlanner::plan(
            "SELECT ('2001-02-16', '2001-12-21') OVERLAPS \
             ('2001-10-30', '2002-10-30') FROM users",
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "OVERLAPS" && args.len() == 4
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
    }

    #[test]
    fn parses_at_time_zone() {
        match LogicalPlanner::plan(
            "SELECT '2026-08-02 12:00:00+00' AT TIME ZONE '+03' FROM users",
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(columns[0].1, Expression::AtTimeZone { .. }));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        assert_eq!(parse_zone_offset_secs("UTC").unwrap(), 0);
        assert_eq!(parse_zone_offset_secs("GMT").unwrap(), 0);
        assert_eq!(parse_zone_offset_secs("+03").unwrap(), 3 * 3600);
        assert_eq!(parse_zone_offset_secs("UTC-5").unwrap(), -5 * 3600);
        assert_eq!(parse_zone_offset_secs("-05:30").unwrap(), -(5 * 3600 + 30 * 60));
        // Fixed-offset parser rejects IANA; full at_time_zone handles them.
        assert!(parse_zone_offset_secs("America/Denver").is_err());
        assert!(timezone_is_known("America/Denver"));
        assert!(timezone_is_known("Europe/Istanbul"));
        assert!(!timezone_is_known("NotA/RealZone"));
        assert_eq!(
            parse_timestamp_offset_secs("2026-08-02 12:00:00+00"),
            Some(0)
        );
        assert_eq!(
            parse_timestamp_offset_secs("2026-08-02 12:00:00-05:00"),
            Some(-5 * 3600)
        );
        assert_eq!(parse_timestamp_offset_secs("2026-08-02 12:00:00"), None);
        // timestamptz → wall clock in zone (no suffix)
        assert_eq!(
            at_time_zone("2026-08-02 12:00:00+00", "+03").unwrap(),
            "2026-08-02 15:00:00"
        );
        // timestamp without tz → treat as local in zone → UTC
        assert_eq!(
            at_time_zone("2026-08-02 15:00:00", "+03").unwrap(),
            "2026-08-02 12:00:00+00"
        );
        assert_eq!(
            at_time_zone("2026-08-02 12:00:00Z", "UTC").unwrap(),
            "2026-08-02 12:00:00"
        );
        // IANA: Europe/Istanbul is UTC+3 year-round (no DST since 2016).
        assert_eq!(
            at_time_zone("2026-08-02 12:00:00+00", "Europe/Istanbul").unwrap(),
            "2026-08-02 15:00:00"
        );
        assert_eq!(
            at_time_zone("2026-08-02 15:00:00", "Europe/Istanbul").unwrap(),
            "2026-08-02 12:00:00+00"
        );
        // America/Denver: MST (UTC-7) in January, MDT (UTC-6) in July.
        assert_eq!(
            at_time_zone("2026-01-15 12:00:00+00", "America/Denver").unwrap(),
            "2026-01-15 05:00:00"
        );
        assert_eq!(
            at_time_zone("2026-07-15 12:00:00+00", "America/Denver").unwrap(),
            "2026-07-15 06:00:00"
        );
        assert!(at_time_zone("2026-08-02 12:00:00+00", "NotA/RealZone").is_err());
        assert_eq!(normalize_timezone("utc").unwrap(), "UTC");
        assert_eq!(normalize_timezone("Europe/Istanbul").unwrap(), "Europe/Istanbul");
        assert!(normalize_timezone("NotA/RealZone").is_err());
    }

    #[test]
    fn parses_timezone_fn() {
        match LogicalPlanner::plan(
            "SELECT TIMEZONE('+03', '2026-08-02 12:00:00+00') FROM users",
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "TIMEZONE" && args.len() == 2
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        // Same semantics as AT TIME ZONE with args reversed.
        assert_eq!(
            at_time_zone("2026-08-02 12:00:00+00", "+03").unwrap(),
            "2026-08-02 15:00:00"
        );
        assert_eq!(
            at_time_zone("2026-08-02 15:00:00", "+03").unwrap(),
            "2026-08-02 12:00:00+00"
        );
    }

    #[test]
    fn parses_date_bin() {
        match LogicalPlanner::plan(
            "SELECT DATE_BIN(INTERVAL '15 minutes', '2026-08-02 15:37:00') FROM users",
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "DATE_BIN" && args.len() == 2
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        match LogicalPlanner::plan(
            "SELECT DATE_BIN(INTERVAL '1 hour', '2026-08-02 15:37:00', '2026-08-02 00:00:00') \
             FROM users",
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "DATE_BIN" && args.len() == 3
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        assert_eq!(interval_arg_secs("15 minutes").unwrap(), 15 * 60);
        assert_eq!(
            date_bin_text(15 * 60, "2026-08-02 15:37:00", DATE_BIN_DEFAULT_ORIGIN).unwrap(),
            "2026-08-02 15:30:00+00"
        );
        assert_eq!(
            date_bin_text(3600, "2026-08-02 15:37:00", "2026-08-02 00:00:00").unwrap(),
            "2026-08-02 15:00:00+00"
        );
        assert!(date_bin_text(0, "2026-08-02", DATE_BIN_DEFAULT_ORIGIN).is_err());
    }

    #[test]
    fn parses_age() {
        match LogicalPlanner::plan(
            "SELECT AGE('2026-08-02', '2026-08-01') FROM users",
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "AGE" && args.len() == 2
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        assert_eq!(age_secs("2026-08-02", "2026-08-01").unwrap(), 86_400);
        assert_eq!(
            format_interval_secs(age_secs("2026-01-15 12:00:00", "2026-01-15 10:00:00").unwrap()),
            "02:00:00"
        );
    }

    #[test]
    fn parses_to_char_to_timestamp() {
        match LogicalPlanner::plan(
            "SELECT TO_CHAR('2026-08-02 15:30:45', 'YYYY-MM-DD HH24:MI:SS') FROM users",
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, .. } if name == "TO_CHAR"
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        assert_eq!(
            to_char_timestamp("2026-08-02 15:30:45", "YYYY-MM-DD HH24:MI:SS").unwrap(),
            "2026-08-02 15:30:45"
        );
        assert_eq!(
            to_char_timestamp("2026-08-02", "\"Y=\"YYYY").unwrap(),
            "Y=2026"
        );
        assert_eq!(
            to_timestamp_text("2026-08-02 15:30:45", "YYYY-MM-DD HH24:MI:SS").unwrap(),
            "2026-08-02 15:30:45+00"
        );
    }

    #[test]
    fn parses_to_date() {
        match LogicalPlanner::plan(
            "SELECT TO_DATE('2026-08-02', 'YYYY-MM-DD') FROM users",
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "TO_DATE" && args.len() == 2
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        assert_eq!(
            to_date_text("2026-08-02", "YYYY-MM-DD").unwrap(),
            "2026-08-02"
        );
        assert_eq!(
            to_date_text("02/08/2026", "DD/MM/YYYY").unwrap(),
            "2026-08-02"
        );
        assert_eq!(
            to_date_text("2026-08-02 15:30:45", "YYYY-MM-DD HH24:MI:SS").unwrap(),
            "2026-08-02"
        );
        assert!(to_date_text("not-a-date", "YYYY-MM-DD").is_err());
    }

    #[test]
    fn parses_to_number() {
        match LogicalPlanner::plan(
            "SELECT TO_NUMBER('1234.56', '9999.99') FROM users",
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "TO_NUMBER" && args.len() == 2
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        assert!((to_number_text("1234.56", "9999.99").unwrap() - 1234.56).abs() < 1e-9);
        assert!((to_number_text("1,234.56", "9,999.99").unwrap() - 1234.56).abs() < 1e-9);
        assert!((to_number_text("1,234.56", "9G999D99").unwrap() - 1234.56).abs() < 1e-9);
        assert_eq!(to_number_text("-42", "S999").unwrap(), -42.0);
        assert_eq!(to_number_text("42", "999").unwrap(), 42.0);
        assert_eq!(to_number_text("42-", "999S").unwrap(), -42.0);
    }

    #[test]
    fn parses_values_clause() {
        match LogicalPlanner::plan("VALUES (1, 'Ada'), (2, 'Bob')").unwrap() {
            LogicalPlan::Values { columns, rows } => {
                assert_eq!(columns, vec!["column1".to_string(), "column2".to_string()]);
                assert_eq!(rows.len(), 2);
                assert_eq!(rows[0].len(), 2);
            }
            other => panic!("expected Values, got {other:?}"),
        }
        match LogicalPlanner::plan(
            "SELECT * FROM (VALUES (1, 'Ada'), (2, 'Bob')) AS t(id, name)",
        )
        .unwrap()
        {
            LogicalPlan::SubqueryAlias { alias, input } => {
                assert_eq!(alias, "t");
                match input.as_ref() {
                    LogicalPlan::Values { columns, rows } => {
                        assert_eq!(
                            columns.as_slice(),
                            ["id".to_string(), "name".to_string()].as_slice()
                        );
                        assert_eq!(rows.len(), 2);
                    }
                    other => panic!("expected Values under alias, got {other:?}"),
                }
            }
            other => panic!("expected SubqueryAlias, got {other:?}"),
        }
    }

    #[test]
    fn parses_generate_series() {
        match LogicalPlanner::plan("SELECT * FROM generate_series(1, 5)").unwrap() {
            LogicalPlan::GenerateSeries {
                start,
                stop,
                step,
                column,
                ordinality_column,
                as_timestamp,
                date_only,
            } => {
                assert_eq!((start, stop, step), (1, 5, 1));
                assert_eq!(column, "generate_series");
                assert!(ordinality_column.is_none());
                assert!(!as_timestamp);
                assert!(!date_only);
            }
            other => panic!("expected GenerateSeries, got {other:?}"),
        }
        match LogicalPlanner::plan("SELECT * FROM generate_series(1, 10, 2) AS g(n)").unwrap()
        {
            LogicalPlan::GenerateSeries {
                start,
                stop,
                step,
                column,
                ordinality_column,
                as_timestamp,
                ..
            } => {
                assert_eq!((start, stop, step, column.as_str()), (1, 10, 2, "n"));
                assert!(ordinality_column.is_none());
                assert!(!as_timestamp);
            }
            other => panic!("expected GenerateSeries, got {other:?}"),
        }
        match LogicalPlanner::plan(
            "SELECT * FROM generate_series(10, 12) WITH ORDINALITY AS t(n, ord)",
        )
        .unwrap()
        {
            LogicalPlan::GenerateSeries {
                column,
                ordinality_column,
                ..
            } => {
                assert_eq!(column, "n");
                assert_eq!(ordinality_column.as_deref(), Some("ord"));
            }
            other => panic!("expected GenerateSeries, got {other:?}"),
        }
        match LogicalPlanner::plan(
            "SELECT * FROM generate_series('2026-01-01', '2026-01-03', INTERVAL '1 day')",
        )
        .unwrap()
        {
            LogicalPlan::GenerateSeries {
                start,
                stop,
                step,
                as_timestamp,
                date_only,
                ..
            } => {
                assert!(as_timestamp);
                assert!(date_only);
                assert_eq!(step, 86_400);
                assert_eq!(stop - start, 2 * 86_400);
            }
            other => panic!("expected timestamp GenerateSeries, got {other:?}"),
        }
        match LogicalPlanner::plan(
            "SELECT * FROM generate_series('2026-01-01 00:00:00', '2026-01-01 02:00:00', INTERVAL '1 hour') AS g(ts)",
        )
        .unwrap()
        {
            LogicalPlan::GenerateSeries {
                column,
                as_timestamp,
                date_only,
                step,
                ..
            } => {
                assert_eq!(column, "ts");
                assert!(as_timestamp);
                assert!(!date_only);
                assert_eq!(step, 3_600);
            }
            other => panic!("expected timestamp GenerateSeries, got {other:?}"),
        }
        assert!(LogicalPlanner::plan(
            "SELECT * FROM generate_series('2026-01-01', '2026-01-03')"
        )
        .is_err());

        let rows =
            materialize_generate_series(1, 5, 1, "generate_series", None, false, false).unwrap();
        assert_eq!(rows.len(), 5);
        assert_eq!(rows[0].get("generate_series"), Some("1"));
        assert_eq!(rows[4].get("generate_series"), Some("5"));
        let down = materialize_generate_series(5, 1, -2, "n", None, false, false).unwrap();
        assert_eq!(
            down
                .iter()
                .map(|r| r.get("n").unwrap().to_string())
                .collect::<Vec<_>>(),
            vec!["5", "3", "1"]
        );
        let with_ord =
            materialize_generate_series(10, 12, 1, "n", Some("ord"), false, false).unwrap();
        assert_eq!(with_ord[0].get("n"), Some("10"));
        assert_eq!(with_ord[0].get("ord"), Some("1"));
        assert_eq!(with_ord[2].get("ord"), Some("3"));

        let day0 = timestamp_to_unix(2026, 1, 1, 0, 0, 0);
        let ts_rows = materialize_generate_series(
            day0,
            day0 + 2 * 86_400,
            86_400,
            "d",
            None,
            true,
            true,
        )
        .unwrap();
        assert_eq!(
            ts_rows
                .iter()
                .map(|r| r.get("d").unwrap().to_string())
                .collect::<Vec<_>>(),
            vec!["2026-01-01", "2026-01-02", "2026-01-03"]
        );
    }

    #[test]
    fn parses_unnest() {
        match LogicalPlanner::plan("SELECT * FROM unnest(ARRAY[1, 2, 3])").unwrap() {
            LogicalPlan::Unnest {
                array,
                column,
                ordinality_column,
                zero_based_ordinality,
            } => {
                assert_eq!(column, "unnest");
                assert!(ordinality_column.is_none());
                assert!(!zero_based_ordinality);
                match array {
                    Expression::Array(items) => {
                        assert_eq!(items.len(), 3);
                        assert!(matches!(items[0], Expression::Literal(ref s) if s == "1"));
                    }
                    other => panic!("expected Array, got {other:?}"),
                }
            }
            other => panic!("expected Unnest, got {other:?}"),
        }
        match LogicalPlanner::plan("SELECT * FROM unnest(ARRAY['a', 'b']) AS t(x)").unwrap() {
            LogicalPlan::Unnest { array, column, .. } => {
                assert_eq!(column, "x");
                assert!(matches!(array, Expression::Array(items) if items.len() == 2));
            }
            other => panic!("expected Unnest, got {other:?}"),
        }
        match LogicalPlanner::plan(
            "SELECT * FROM unnest(ARRAY[10, 20]) WITH ORDINALITY AS t(x, i)",
        )
        .unwrap()
        {
            LogicalPlan::Unnest {
                column,
                ordinality_column,
                ..
            } => {
                assert_eq!(column, "x");
                assert_eq!(ordinality_column.as_deref(), Some("i"));
            }
            other => panic!("expected Unnest, got {other:?}"),
        }
        let rows = materialize_unnest(
            &Expression::Array(vec![
                Expression::Literal("10".into()),
                Expression::Literal("20".into()),
            ]),
            "unnest",
            None,
            false,
        )
        .unwrap();
        assert_eq!(rows[0].get("unnest"), Some("10"));
        assert_eq!(rows[1].get("unnest"), Some("20"));

        match LogicalPlanner::plan(
            "SELECT * FROM UNNEST(ARRAY[10, 20, 30]) AS numbers WITH OFFSET",
        )
        .unwrap()
        {
            LogicalPlan::Unnest {
                column,
                ordinality_column,
                zero_based_ordinality,
                ..
            } => {
                assert_eq!(column, "numbers");
                assert_eq!(ordinality_column.as_deref(), Some("offset"));
                assert!(zero_based_ordinality);
            }
            other => panic!("expected Unnest WITH OFFSET, got {other:?}"),
        }
        let off = materialize_unnest(
            &Expression::Array(vec![
                Expression::Literal("10".into()),
                Expression::Literal("20".into()),
            ]),
            "numbers",
            Some("offset"),
            true,
        )
        .unwrap();
        assert_eq!(off[0].get("offset"), Some("0"));
        assert_eq!(off[1].get("offset"), Some("1"));

        match LogicalPlanner::plan(
            r#"SELECT * FROM emp CROSS JOIN LATERAL unnest(emp.tags) AS t(x)"#,
        )
        .unwrap()
        {
            LogicalPlan::Join { right, .. } => match right.as_ref() {
                LogicalPlan::Unnest { array, column, .. } => {
                    assert_eq!(column, "x");
                    assert!(expr_needs_row_eval(array), "got {array:?}");
                }
                other => panic!("expected Unnest, got {other:?}"),
            },
            other => panic!("expected Join, got {other:?}"),
        }
    }

    #[test]
    fn parses_array_ops() {
        match LogicalPlanner::plan(
            "SELECT array_length(ARRAY[1,2,3], 1), cardinality(ARRAY[1,2]), ARRAY[1,2,3][2] \
             FROM generate_series(1,1)",
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, .. } if name == "ARRAY_LENGTH"
                ));
                assert!(matches!(
                    columns[1].1,
                    Expression::ScalarFunction { ref name, .. } if name == "CARDINALITY"
                ));
                assert!(matches!(columns[2].1, Expression::ArrayIndex { .. }));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        match LogicalPlanner::plan(
            "SELECT ARRAY[1,2] || ARRAY[3] FROM generate_series(1,1)",
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, .. } if name == "ARRAY_CAT"
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
    }

    #[test]
    fn parses_array_contains_ops() {
        match LogicalPlanner::plan(
            "SELECT ARRAY[1,2,3] @> ARRAY[2], ARRAY[1] <@ ARRAY[1,2], ARRAY[1,2] && ARRAY[2,3] \
             FROM generate_series(1,1)",
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, .. } if name == "ARRAY_CONTAINS"
                ));
                assert!(matches!(
                    columns[1].1,
                    Expression::ScalarFunction { ref name, .. } if name == "ARRAY_CONTAINED_BY"
                ));
                assert!(matches!(
                    columns[2].1,
                    Expression::ScalarFunction { ref name, .. } if name == "ARRAY_OVERLAP"
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
    }

    #[test]
    fn parses_json_arrow_ops() {
        match LogicalPlanner::plan(
            "SELECT '{\"a\":1}'::json -> 'a', '{\"a\":1}'::jsonb ->> 'a', \
             jsonb_typeof('{\"a\":1}'::jsonb) FROM generate_series(1,1)",
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, .. } if name == "JSON_GET"
                ));
                assert!(matches!(
                    columns[1].1,
                    Expression::ScalarFunction { ref name, .. } if name == "JSON_GET_TEXT"
                ));
                assert!(matches!(
                    columns[2].1,
                    Expression::ScalarFunction { ref name, .. } if name == "JSONB_TYPEOF"
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        assert_eq!(
            json_get(r#"{"a":1}"#, &Value::String("a".into()), false).unwrap(),
            Value::String("1".into())
        );
        assert_eq!(
            json_get(r#"{"a":"x"}"#, &Value::String("a".into()), true).unwrap(),
            Value::String("x".into())
        );
        assert_eq!(json_typeof(r#"{"a":1}"#).unwrap(), "object");
        assert_eq!(json_typeof("[1,2]").unwrap(), "array");
    }

    #[test]
    fn parses_json_path_and_contains() {
        match LogicalPlanner::plan(
            r#"SELECT '{"a":{"b":2}}'::json #> '{a,b}', '{"a":1}'::jsonb @> '{"a":1}'::jsonb FROM generate_series(1,1)"#,
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, .. } if name == "JSON_PATH_GET"
                ));
                assert!(matches!(
                    columns[1].1,
                    Expression::ScalarFunction { ref name, .. } if name == "JSON_CONTAINS"
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        // ARRAY @> still routes to array ops.
        match LogicalPlanner::plan(
            "SELECT ARRAY[1,2] @> ARRAY[1] FROM generate_series(1,1)",
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, .. } if name == "ARRAY_CONTAINS"
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        assert_eq!(
            json_path_get(r#"{"a":{"b":2}}"#, "{a,b}", false).unwrap(),
            Value::String("2".into())
        );
        assert_eq!(
            json_path_get(r#"{"a":{"b":"x"}}"#, "{a,b}", true).unwrap(),
            Value::String("x".into())
        );
        assert!(json_contains(r#"{"a":1,"b":2}"#, r#"{"a":1}"#).unwrap());
        assert!(!json_contains(r#"{"a":1}"#, r#"{"a":1,"b":2}"#).unwrap());
    }

    #[test]
    fn parses_jsonb_set_and_concat() {
        match LogicalPlanner::plan(
            r#"SELECT jsonb_set('{"a":1}'::jsonb, '{b}', '2'::jsonb), '{"a":1}'::jsonb || '{"b":2}'::jsonb FROM generate_series(1,1)"#,
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, .. } if name == "JSONB_SET"
                ));
                assert!(matches!(
                    columns[1].1,
                    Expression::ScalarFunction { ref name, .. } if name == "JSON_CONCAT"
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        let set = jsonb_set(r#"{"a":1}"#, "{b}", "2", true).unwrap();
        assert!(set.contains("\"b\":2") || set.contains("\"b\": 2"));
        assert!(json_contains(&set, r#"{"a":1}"#).unwrap());
        assert_eq!(
            json_concat(r#"{"a":1}"#, r#"{"b":2}"#).unwrap().contains("\"b\":2")
                || json_concat(r#"{"a":1}"#, r#"{"b":2}"#).unwrap().contains("\"b\": 2"),
            true
        );
        let nested = jsonb_set(r#"{"a":{"b":1}}"#, "{a,b}", "9", true).unwrap();
        assert_eq!(
            json_path_get(&nested, "{a,b}", false).unwrap(),
            Value::String("9".into())
        );
    }

    #[test]
    fn parses_jsonb_build_object_and_array() {
        match LogicalPlanner::plan(
            r#"SELECT jsonb_build_object('a', 1, 'b', true),
                      jsonb_build_array(1, 'x', NULL)
               FROM generate_series(1,1)"#,
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, .. } if name == "JSONB_BUILD_OBJECT"
                ));
                assert!(matches!(
                    columns[1].1,
                    Expression::ScalarFunction { ref name, .. } if name == "JSONB_BUILD_ARRAY"
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        assert!(LogicalPlanner::plan(
            r#"SELECT jsonb_build_object('a') FROM generate_series(1,1)"#
        )
        .is_err());

        let obj = jsonb_build_object(&[
            (Value::String("a".into()), Value::Int(1)),
            (Value::String("b".into()), Value::Bool(true)),
        ])
        .unwrap();
        assert!(json_contains(&obj, r#"{"a":1}"#).unwrap());
        assert!(json_contains(&obj, r#"{"b":true}"#).unwrap());

        let arr = jsonb_build_array(&[Value::Int(1), Value::String("x".into()), Value::Null]);
        assert_eq!(arr, r#"[1,"x",null]"#);

        let nested = jsonb_build_object(&[(
            Value::String("j".into()),
            Value::String(r#"{"k":2}"#.into()),
        )])
        .unwrap();
        assert_eq!(
            json_path_get(&nested, "{j,k}", false).unwrap(),
            Value::String("2".into())
        );
    }

    #[test]
    fn parses_jsonb_pretty_and_delete() {
        match LogicalPlanner::plan(
            r#"SELECT jsonb_pretty('{"a":1}'::jsonb),
                      '{"a":1,"b":2}'::jsonb - 'a',
                      '{"a":{"b":1}}'::jsonb #- '{a,b}'
               FROM generate_series(1,1)"#,
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, .. } if name == "JSONB_PRETTY"
                ));
                assert!(matches!(
                    columns[1].1,
                    Expression::ScalarFunction { ref name, .. } if name == "JSON_DELETE"
                ));
                assert!(matches!(
                    columns[2].1,
                    Expression::ScalarFunction { ref name, .. } if name == "JSON_PATH_DELETE"
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }

        let pretty = jsonb_pretty(r#"{"a":1}"#).unwrap();
        assert!(pretty.contains('\n') && pretty.contains("\"a\""));

        let del = json_delete(r#"{"a":1,"b":2}"#, &Value::String("a".into())).unwrap();
        assert!(!json_contains(&del, r#"{"a":1}"#).unwrap());
        assert!(json_contains(&del, r#"{"b":2}"#).unwrap());

        let arr = json_delete(r#"[10,20,30]"#, &Value::Int(1)).unwrap();
        assert_eq!(arr, "[10,30]");

        let path = json_path_delete(r#"{"a":{"b":1,"c":2}}"#, "{a,b}").unwrap();
        assert_eq!(
            json_path_get(&path, "{a,c}", false).unwrap(),
            Value::String("2".into())
        );
        assert!(json_path_get(&path, "{a,b}", false)
            .unwrap()
            .is_null());
    }

    #[test]
    fn parses_jsonb_insert_and_strip_nulls() {
        match LogicalPlanner::plan(
            r#"SELECT jsonb_insert('{"a":[1,2]}'::jsonb, '{a,1}', '9'::jsonb),
                      jsonb_strip_nulls('{"a":1,"b":null}'::jsonb)
               FROM generate_series(1,1)"#,
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, .. } if name == "JSONB_INSERT"
                ));
                assert!(matches!(
                    columns[1].1,
                    Expression::ScalarFunction { ref name, .. } if name == "JSONB_STRIP_NULLS"
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }

        let inserted = jsonb_insert(r#"{"a":[1,2]}"#, "{a,1}", "9", false).unwrap();
        assert_eq!(
            json_path_get(&inserted, "{a}", false).unwrap(),
            Value::String("[1,9,2]".into())
        );
        let after = jsonb_insert(r#"[1,2]"#, "{1}", "9", true).unwrap();
        assert_eq!(after, "[1,2,9]");
        let obj = jsonb_insert(r#"{"a":1}"#, "{b}", "2", false).unwrap();
        assert!(json_contains(&obj, r#"{"a":1,"b":2}"#).unwrap());

        let stripped = jsonb_strip_nulls(r#"{"a":1,"b":null,"c":{"d":null,"e":2}}"#).unwrap();
        assert!(json_contains(&stripped, r#"{"a":1}"#).unwrap());
        assert!(!stripped.contains("\"b\""));
        assert_eq!(
            json_path_get(&stripped, "{c,e}", false).unwrap(),
            Value::String("2".into())
        );
        assert!(json_path_get(&stripped, "{c,d}", false)
            .unwrap()
            .is_null());
    }

    #[test]
    fn parses_json_array_elements_and_each() {
        match LogicalPlanner::plan(
            r#"SELECT * FROM jsonb_array_elements('[1, 2, 3]'::jsonb)"#,
        )
        .unwrap()
        {
            LogicalPlan::JsonArrayElements {
                doc,
                column,
                as_text,
                ordinality_column,
            } => {
                assert!(
                    matches!(
                        doc,
                        Expression::Literal(ref s) if s == "[1,2,3]"
                    ),
                    "expected folded literal doc, got {doc:?}"
                );
                assert_eq!(column, "value");
                assert!(!as_text);
                assert!(ordinality_column.is_none());
            }
            other => panic!("expected JsonArrayElements, got {other:?}"),
        }
        let rows = materialize_json_array_elements("[1,2,3]", "value", false, None).unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].get("value"), Some("1"));

        match LogicalPlanner::plan(
            r#"SELECT * FROM json_each('{"a":1,"b":true}'::json)"#,
        )
        .unwrap()
        {
            LogicalPlan::JsonEach {
                doc,
                key_column,
                value_column,
                as_text,
                ordinality_column,
            } => {
                match doc {
                    Expression::Literal(ref s) => assert!(s.contains("\"a\"")),
                    other => panic!("expected literal doc, got {other:?}"),
                }
                assert_eq!(key_column, "key");
                assert_eq!(value_column, "value");
                assert!(!as_text);
                assert!(ordinality_column.is_none());
            }
            other => panic!("expected JsonEach, got {other:?}"),
        }
        let pairs = materialize_json_each(r#"{"a":1,"b":true}"#, "key", "value", false, None).unwrap();
        assert_eq!(pairs.len(), 2);
        let mut map = std::collections::BTreeMap::new();
        for r in &pairs {
            map.insert(r.get("key").unwrap().to_string(), r.get("value").unwrap().to_string());
        }
        assert_eq!(map.get("a").map(String::as_str), Some("1"));
        assert_eq!(map.get("b").map(String::as_str), Some("true"));

        match LogicalPlanner::plan(
            r#"SELECT x FROM jsonb_array_elements_text('["Ada","Di"]'::jsonb) AS t(x)"#,
        )
        .unwrap()
        {
            LogicalPlan::Project { input, columns } => {
                assert_eq!(columns[0].0, "x");
                match input.as_ref() {
                    LogicalPlan::JsonArrayElements { column, as_text, .. } => {
                        assert_eq!(column, "x");
                        assert!(*as_text);
                    }
                    other => panic!("expected JsonArrayElements under Project, got {other:?}"),
                }
            }
            LogicalPlan::JsonArrayElements { column, as_text, .. } => {
                assert_eq!(column, "x");
                assert!(as_text);
            }
            other => panic!("unexpected plan {other:?}"),
        }
    }

    #[test]
    fn parses_row_to_json() {
        match LogicalPlanner::plan("SELECT row_to_json(emp) FROM emp").unwrap() {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "ROW_TO_JSON" && args.is_empty()
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        match LogicalPlanner::plan("SELECT row_to_json(ROW(id, name)) FROM emp").unwrap() {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "ROW_TO_JSON" && args.len() == 2
                ));
            }
            other => panic!("expected Project for ROW(), got {other:?}"),
        }
        match LogicalPlanner::plan("SELECT row_to_json((id, name)) FROM emp").unwrap() {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, ref args, .. }
                        if name == "ROW_TO_JSON" && args.len() == 2
                ));
            }
            other => panic!("expected Project for tuple, got {other:?}"),
        }
        assert_eq!(
            row_to_json_object(&[
                ("id".into(), Value::Int(1)),
                ("name".into(), Value::String("Ada".into())),
            ]),
            r#"{"id":1,"name":"Ada"}"#
        );
    }

    #[test]
    fn parses_to_json_and_array_to_json() {
        match LogicalPlanner::plan(
            r#"SELECT to_json(1), to_jsonb('hi'), array_to_json(ARRAY[1,2])
               FROM generate_series(1,1)"#,
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, .. } if name == "TO_JSON"
                ));
                assert!(matches!(
                    columns[1].1,
                    Expression::ScalarFunction { ref name, .. } if name == "TO_JSONB"
                ));
                assert!(matches!(
                    columns[2].1,
                    Expression::ScalarFunction { ref name, .. } if name == "ARRAY_TO_JSON"
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        assert_eq!(to_json(&Value::Int(1)), "1");
        assert_eq!(to_json(&Value::Bool(true)), "true");
        assert_eq!(to_json(&Value::Null), "null");
        assert_eq!(to_json(&Value::String("hi".into())), "\"hi\"");
        assert_eq!(to_json(&Value::String("[1,2]".into())), "[1,2]");
    }

    #[test]
    fn parses_string_to_array_and_array_to_string() {
        match LogicalPlanner::plan(
            r#"SELECT string_to_array('a,b,c', ','), array_to_string(ARRAY[1,2,3], '-')
               FROM generate_series(1,1)"#,
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, .. } if name == "STRING_TO_ARRAY"
                ));
                assert!(matches!(
                    columns[1].1,
                    Expression::ScalarFunction { ref name, .. } if name == "ARRAY_TO_STRING"
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        assert_eq!(string_to_array("a,b,c", ",", None), "[a,b,c]");
        assert_eq!(string_to_array("a,x,c", ",", Some("x")), "[a,,c]");
        assert_eq!(
            array_to_string(&[Value::Int(1), Value::Int(2), Value::Null], ",", None),
            "1,2"
        );
        assert_eq!(
            array_to_string(
                &[Value::Int(1), Value::Null, Value::Int(3)],
                ",",
                Some("?")
            ),
            "1,?,3"
        );
    }

    #[test]
    fn parses_split_part() {
        match LogicalPlanner::plan(
            r#"SELECT split_part('a.b.c', '.', 2) FROM generate_series(1,1)"#,
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, .. } if name == "SPLIT_PART"
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        assert_eq!(split_part("a.b.c", ".", 1).unwrap(), "a");
        assert_eq!(split_part("a.b.c", ".", 2).unwrap(), "b");
        assert_eq!(split_part("a.b.c", ".", 9).unwrap(), "");
        assert_eq!(split_part("a.b.c", ".", -1).unwrap(), "c");
        assert!(split_part("a.b.c", ".", 0).is_err());
    }

    #[test]
    fn parses_regexp_split_to_array() {
        match LogicalPlanner::plan(
            r#"SELECT regexp_split_to_array('a,b,c', ',') FROM generate_series(1,1)"#,
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, .. } if name == "REGEXP_SPLIT_TO_ARRAY"
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        assert_eq!(
            regexp_split_to_array("hello world", "\\s+", None).unwrap(),
            "[hello,world]"
        );
        assert_eq!(
            regexp_split_to_array("aXbXc", "x", Some("i")).unwrap(),
            "[a,b,c]"
        );
        assert!(regexp_split_to_array("a", "[", None).is_err());
    }

    #[test]
    fn parses_regexp_replace() {
        match LogicalPlanner::plan(
            r#"SELECT regexp_replace('foobarbaz', 'b..', 'X') FROM generate_series(1,1)"#,
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, .. } if name == "REGEXP_REPLACE"
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        assert_eq!(
            regexp_replace("foobarbaz", "b..", "X", None).unwrap(),
            "fooXbaz"
        );
        assert_eq!(
            regexp_replace("foobarbaz", "b..", "X", Some("g")).unwrap(),
            "fooXX"
        );
        assert_eq!(
            regexp_replace("AaA", "a", "z", Some("i")).unwrap(),
            "zaA"
        );
        assert_eq!(
            regexp_replace("a1b2", "(\\d)", "[\\1]", Some("g")).unwrap(),
            "a[1]b[2]"
        );
    }

    #[test]
    fn parses_regexp_like_and_matches() {
        match LogicalPlanner::plan(
            r#"SELECT regexp_like('hello', 'h.*o', 'i') FROM generate_series(1,1)"#,
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, .. } if name == "REGEXP_LIKE"
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        assert!(regexp_like("hello", "h.*o", None).unwrap());
        assert!(!regexp_like("hello", "xyz", None).unwrap());
        assert!(regexp_like("Hello", "hello", Some("i")).unwrap());

        match LogicalPlanner::plan(
            r#"SELECT * FROM regexp_matches('foobarbaz', 'b(..)', 'g')"#,
        )
        .unwrap()
        {
            LogicalPlan::RegexpMatches {
                string,
                pattern,
                flags,
                column,
                ordinality_column,
            } => {
                assert!(matches!(string, Expression::Literal(ref s) if s == "foobarbaz"));
                assert!(matches!(pattern, Expression::Literal(ref s) if s == "b(..)"));
                assert!(matches!(flags, Some(Expression::Literal(ref s)) if s == "g"));
                assert_eq!(column, "regexp_matches");
                assert!(ordinality_column.is_none());
            }
            other => panic!("expected RegexpMatches, got {other:?}"),
        }
        assert_eq!(
            regexp_match_rows("foobarbaz", "b(..)", Some("g")).unwrap(),
            vec!["[ar]".to_string(), "[az]".to_string()]
        );
        assert_eq!(
            regexp_match_rows("abc", "x", None).unwrap(),
            Vec::<String>::new()
        );
        assert_eq!(
            regexp_match_rows("a1b2", "\\d", Some("g")).unwrap(),
            vec!["[1]".to_string(), "[2]".to_string()]
        );
    }

    #[test]
    fn parses_lpad_rpad_repeat() {
        match LogicalPlanner::plan(
            r#"SELECT lpad('hi', 5, 'xy'), rpad('hi', 5, '*'), repeat('ab', 3)
               FROM generate_series(1,1)"#,
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, .. } if name == "LPAD"
                ));
                assert!(matches!(
                    columns[1].1,
                    Expression::ScalarFunction { ref name, .. } if name == "RPAD"
                ));
                assert!(matches!(
                    columns[2].1,
                    Expression::ScalarFunction { ref name, .. } if name == "REPEAT"
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        assert_eq!(lpad("hi", 5, "xy").unwrap(), "xyxhi");
        assert_eq!(rpad("hi", 5, "*").unwrap(), "hi***");
        assert_eq!(lpad("hello", 3, " ").unwrap(), "hel");
        assert_eq!(repeat("ab", 3).unwrap(), "ababab");
        assert_eq!(repeat("x", 0).unwrap(), "");
        assert!(repeat("x", -1).is_err());
        assert!(lpad("x", -1, " ").is_err());
    }

    #[test]
    fn parses_left_right_reverse() {
        match LogicalPlanner::plan(
            r#"SELECT left('abcde', 2), right('abcde', 2), reverse('abc')
               FROM generate_series(1,1)"#,
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, .. } if name == "LEFT"
                ));
                assert!(matches!(
                    columns[1].1,
                    Expression::ScalarFunction { ref name, .. } if name == "RIGHT"
                ));
                assert!(matches!(
                    columns[2].1,
                    Expression::ScalarFunction { ref name, .. } if name == "REVERSE"
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        assert_eq!(left("abcde", 2), "ab");
        assert_eq!(right("abcde", 2), "de");
        assert_eq!(left("ab", 10), "ab");
        assert_eq!(right("ab", 0), "");
        assert_eq!(reverse("abc"), "cba");
        assert_eq!(reverse("a👍b"), "b👍a");
    }

    #[test]
    fn parses_initcap_ascii_chr() {
        match LogicalPlanner::plan(
            r#"SELECT initcap('hello world'), ascii('A'), chr(65)
               FROM generate_series(1,1)"#,
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, .. } if name == "INITCAP"
                ));
                assert!(matches!(
                    columns[1].1,
                    Expression::ScalarFunction { ref name, .. } if name == "ASCII"
                ));
                assert!(matches!(
                    columns[2].1,
                    Expression::ScalarFunction { ref name, .. } if name == "CHR"
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        assert_eq!(initcap("hello WORLD"), "Hello World");
        assert_eq!(initcap("foo-bar_baz"), "Foo-Bar_Baz");
        assert_eq!(ascii("A"), 65);
        assert_eq!(ascii(""), 0);
        assert_eq!(chr(65).unwrap(), "A");
        assert!(chr(-1).is_err());
    }

    #[test]
    fn parses_md5_encode_decode() {
        match LogicalPlanner::plan(
            r#"SELECT md5('abc'), encode('hi', 'hex'), decode('6869', 'hex')
               FROM generate_series(1,1)"#,
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, .. } if name == "MD5"
                ));
                assert!(matches!(
                    columns[1].1,
                    Expression::ScalarFunction { ref name, .. } if name == "ENCODE"
                ));
                assert!(matches!(
                    columns[2].1,
                    Expression::ScalarFunction { ref name, .. } if name == "DECODE"
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        assert_eq!(md5_hex("abc"), "900150983cd24fb0d6963f7d28e17f72");
        assert_eq!(encode_bytes("hi", "hex").unwrap(), "6869");
        assert_eq!(decode_bytes("6869", "hex").unwrap(), "hi");
        let b64 = encode_bytes("hi", "base64").unwrap();
        assert_eq!(decode_bytes(&b64, "base64").unwrap(), "hi");
        assert!(encode_bytes("x", "escape").is_err());
    }

    #[test]
    fn parses_starts_with_and_overlay() {
        match LogicalPlanner::plan(
            r#"SELECT starts_with('hello', 'he'),
                      ends_with('hello', 'lo'),
                      overlay('Txxxxas' placing 'hom' from 2 for 4)
               FROM generate_series(1,1)"#,
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, .. } if name == "STARTS_WITH"
                ));
                assert!(matches!(
                    columns[1].1,
                    Expression::ScalarFunction { ref name, .. } if name == "ENDS_WITH"
                ));
                assert!(matches!(
                    columns[2].1,
                    Expression::ScalarFunction { ref name, .. } if name == "OVERLAY"
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        assert!(starts_with("hello", "he"));
        assert!(!starts_with("hello", "lo"));
        assert!(ends_with("hello", "lo"));
        assert!(!ends_with("hello", "he"));
        assert_eq!(
            overlay("Txxxxas", "hom", 2, Some(4)).unwrap(),
            "Thomas"
        );
        assert_eq!(overlay("abcdef", "XY", 3, None).unwrap(), "abXYef");
        assert!(overlay("ab", "x", 0, None).is_err());
    }

    #[test]
    fn parses_translate_and_btrim() {
        match LogicalPlanner::plan(
            r#"SELECT translate('12345', '14', 'ax'), btrim('xyxHelloxyx', 'xy'),
                      ltrim('  hi'), rtrim('hi***', '*')
               FROM generate_series(1,1)"#,
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, .. } if name == "TRANSLATE"
                ));
                assert!(matches!(
                    columns[1].1,
                    Expression::ScalarFunction { ref name, .. } if name == "BTRIM"
                ));
                assert!(matches!(
                    columns[2].1,
                    Expression::ScalarFunction { ref name, .. } if name == "LTRIM"
                ));
                assert!(matches!(
                    columns[3].1,
                    Expression::ScalarFunction { ref name, .. } if name == "RTRIM"
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        assert_eq!(translate("12345", "14", "ax"), "a23x5");
        assert_eq!(translate("hello", "el", "a"), "hao"); // e→a, l deleted
        assert_eq!(btrim("  hi  ", None), "hi");
        assert_eq!(btrim("xyxHelloxyx", Some("xy")), "Hello");
        assert_eq!(ltrim("  hi", None), "hi");
        assert_eq!(rtrim("hi***", Some("*")), "hi");
    }

    #[test]
    fn parses_concat_ws_and_format() {
        match LogicalPlanner::plan(
            r#"SELECT concat_ws(',', 'a', 'b'), format('Hello %s', 'Ada')
               FROM generate_series(1,1)"#,
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, .. } if name == "CONCAT_WS"
                ));
                assert!(matches!(
                    columns[1].1,
                    Expression::ScalarFunction { ref name, .. } if name == "FORMAT"
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        assert_eq!(
            concat_ws(",", &[Value::String("a".into()), Value::Null, Value::String("b".into())]),
            "a,b"
        );
        assert_eq!(
            format_sql("Hello %s!", &[Value::String("Ada".into())]).unwrap(),
            "Hello Ada!"
        );
        assert_eq!(
            format_sql("%I = %L", &[Value::String("x\"y".into()), Value::Null]).unwrap(),
            "\"x\"\"y\" = NULL"
        );
        assert_eq!(format_sql("100%%", &[]).unwrap(), "100%");
        assert!(format_sql("%s", &[]).is_err());
    }

    #[test]
    fn parses_quote_ident_literal() {
        match LogicalPlanner::plan(
            r#"SELECT quote_ident('Foo'), quote_literal('a''b')
               FROM generate_series(1,1)"#,
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, .. } if name == "QUOTE_IDENT"
                ));
                assert!(matches!(
                    columns[1].1,
                    Expression::ScalarFunction { ref name, .. } if name == "QUOTE_LITERAL"
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        assert_eq!(quote_ident("Foo"), "\"Foo\"");
        assert_eq!(quote_ident("x\"y"), "\"x\"\"y\"");
        assert_eq!(quote_literal("a'b"), "'a''b'");
    }

    #[test]
    fn parses_quote_nullable_and_width_bucket() {
        match LogicalPlanner::plan(
            r#"SELECT quote_nullable(NULL), width_bucket(5.35, 0.024, 10.06, 5)
               FROM generate_series(1,1)"#,
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, .. } if name == "QUOTE_NULLABLE"
                ));
                assert!(matches!(
                    columns[1].1,
                    Expression::ScalarFunction { ref name, .. } if name == "WIDTH_BUCKET"
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        assert_eq!(quote_nullable(&Value::Null), "NULL");
        assert_eq!(
            quote_nullable(&Value::String("hi".into())),
            "'hi'"
        );
        assert_eq!(width_bucket(5.35, 0.024, 10.06, 5).unwrap(), 3);
        assert_eq!(width_bucket(-1.0, 0.0, 10.0, 5).unwrap(), 0);
        assert_eq!(width_bucket(10.0, 0.0, 10.0, 5).unwrap(), 6);
        assert!(width_bucket(1.0, 0.0, 0.0, 5).is_err());
    }

    #[test]
    fn parses_sign_trunc_div() {
        match LogicalPlanner::plan(
            r#"SELECT sign(-3), trunc(42.8), trunc(42.89, 1), div(9, 4)
               FROM generate_series(1,1)"#,
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, .. } if name == "SIGN"
                ));
                assert!(matches!(
                    columns[1].1,
                    Expression::ScalarFunction { ref name, .. } if name == "TRUNC"
                ));
                assert!(matches!(
                    columns[3].1,
                    Expression::ScalarFunction { ref name, .. } if name == "DIV"
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        assert_eq!(sign(-3.0), -1.0);
        assert_eq!(sign(0.0), 0.0);
        assert_eq!(trunc_num(42.8, 0), 42.0);
        assert_eq!(trunc_num(-42.8, 0), -42.0);
        assert_eq!(trunc_num(42.89, 1), 42.8);
        assert_eq!(div_int(9.0, 4.0).unwrap(), 2);
        assert_eq!(div_int(-9.0, 4.0).unwrap(), -2);
        assert!(div_int(1.0, 0.0).is_err());
    }

    #[test]
    fn parses_pi_sqrt_cbrt_log() {
        match LogicalPlanner::plan(
            r#"SELECT pi(), sqrt(9), cbrt(8), ln(1), log(100), log(2, 8)
               FROM generate_series(1,1)"#,
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, .. } if name == "PI"
                ));
                assert!(matches!(
                    columns[1].1,
                    Expression::ScalarFunction { ref name, .. } if name == "SQRT"
                ));
                assert!(matches!(
                    columns[2].1,
                    Expression::ScalarFunction { ref name, .. } if name == "CBRT"
                ));
                assert!(matches!(
                    columns[4].1,
                    Expression::ScalarFunction { ref name, .. } if name == "LOG"
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        assert!((log_num(&[100.0]).unwrap() - 2.0).abs() < 1e-12);
        assert!((log_num(&[2.0, 8.0]).unwrap() - 3.0).abs() < 1e-12);
        assert!(log_num(&[-1.0]).is_err());
    }

    #[test]
    fn parses_trig_radians_degrees() {
        match LogicalPlanner::plan(
            r#"SELECT sin(radians(90)), cos(0), degrees(pi()/2)
               FROM generate_series(1,1)"#,
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, .. } if name == "SIN"
                ));
                assert!(matches!(
                    columns[2].1,
                    Expression::ScalarFunction { ref name, .. } if name == "DEGREES"
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        assert!((90f64.to_radians().sin() - 1.0).abs() < 1e-12);
        assert!((std::f64::consts::PI / 2.0).to_degrees() - 90.0 < 1e-12);
    }

    #[test]
    fn parses_json_array_length() {
        match LogicalPlanner::plan(
            r#"SELECT json_array_length('[1,2,3]'::json),
                      jsonb_array_length('[]'::jsonb)
               FROM generate_series(1,1)"#,
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, .. } if name == "JSON_ARRAY_LENGTH"
                ));
                assert!(matches!(
                    columns[1].1,
                    Expression::ScalarFunction { ref name, .. } if name == "JSONB_ARRAY_LENGTH"
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        assert_eq!(json_array_length("[1,2,3]").unwrap(), 3);
        assert_eq!(json_array_length("[]").unwrap(), 0);
        assert!(json_array_length(r#"{"a":1}"#).is_err());
    }

    #[test]
    fn parses_is_json() {
        match LogicalPlanner::plan(
            r#"SELECT is_json('[]'), json_is_valid('{'), is_json(NULL)
               FROM generate_series(1,1)"#,
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, .. } if name == "IS_JSON"
                ));
                assert!(matches!(
                    columns[1].1,
                    Expression::ScalarFunction { ref name, .. } if name == "JSON_IS_VALID"
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        assert!(is_json("[]"));
        assert!(is_json(r#"{"a":1}"#));
        assert!(!is_json("{"));
        assert!(!is_json("not json"));
    }

    #[test]
    fn parses_jsonb_path_exists() {
        match LogicalPlanner::plan(
            r#"SELECT jsonb_path_exists('{"a":{"b":1}}'::jsonb, '{a,b}'),
                      json_path_exists('{"a":1}'::json, '{z}')
               FROM generate_series(1,1)"#,
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, .. } if name == "JSONB_PATH_EXISTS"
                ));
                assert!(matches!(
                    columns[1].1,
                    Expression::ScalarFunction { ref name, .. } if name == "JSON_PATH_EXISTS"
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        assert!(json_path_exists(r#"{"a":{"b":1}}"#, "{a,b}").unwrap());
        assert!(json_path_exists(r#"{"a":null}"#, "{a}").unwrap());
        assert!(!json_path_exists(r#"{"a":1}"#, "{z}").unwrap());
        assert!(json_path_exists(r#"[10,20]"#, "{1}").unwrap());
        assert!(!json_path_exists(r#"[10]"#, "{2}").unwrap());
    }

    #[test]
    fn parses_jsonb_extract_path() {
        match LogicalPlanner::plan(
            r#"SELECT jsonb_extract_path('{"a":{"b":9}}'::jsonb, 'a', 'b'),
                      jsonb_extract_path_text('{"a":{"b":"x"}}'::jsonb, 'a', 'b')
               FROM generate_series(1,1)"#,
        )
        .unwrap()
        {
            LogicalPlan::Project { columns, .. } => {
                assert!(matches!(
                    columns[0].1,
                    Expression::ScalarFunction { ref name, .. } if name == "JSONB_EXTRACT_PATH"
                ));
                assert!(matches!(
                    columns[1].1,
                    Expression::ScalarFunction { ref name, .. } if name == "JSONB_EXTRACT_PATH_TEXT"
                ));
            }
            other => panic!("expected Project, got {other:?}"),
        }
        assert_eq!(
            json_extract_path(r#"{"a":{"b":9}}"#, &["a".into(), "b".into()], false).unwrap(),
            Value::String("9".into())
        );
        assert_eq!(
            json_extract_path(r#"{"a":{"b":"x"}}"#, &["a".into(), "b".into()], true).unwrap(),
            Value::String("x".into())
        );
        assert!(json_extract_path(r#"{"a":1}"#, &["z".into()], false)
            .unwrap()
            .is_null());
    }

    #[test]
    fn parses_jsonb_object_keys() {
        match LogicalPlanner::plan(
            r#"SELECT * FROM jsonb_object_keys('{"a":1,"b":2}'::jsonb)"#,
        )
        .unwrap()
        {
            LogicalPlan::JsonObjectKeys {
                doc,
                column,
                ordinality_column,
            } => {
                match doc {
                    Expression::Literal(ref s) => assert!(s.contains("\"a\"")),
                    other => panic!("expected literal doc, got {other:?}"),
                }
                assert_eq!(column, "jsonb_object_keys");
                assert!(ordinality_column.is_none());
            }
            other => panic!("expected JsonObjectKeys, got {other:?}"),
        }
        let rows = materialize_json_object_keys(r#"{"b":2,"a":1}"#, "k", None).unwrap();
        assert_eq!(rows.len(), 2);
        let mut keys: Vec<_> = rows.iter().map(|r| r.get("k").unwrap().to_string()).collect();
        keys.sort();
        assert_eq!(keys, vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn parses_regexp_split_to_table() {
        match LogicalPlanner::plan(
            r#"SELECT * FROM regexp_split_to_table('hello world', '\s+')"#,
        )
        .unwrap()
        {
            LogicalPlan::RegexpSplitToTable {
                string,
                pattern,
                flags,
                column,
                ordinality_column,
            } => {
                assert!(matches!(string, Expression::Literal(ref s) if s == "hello world"));
                assert!(matches!(pattern, Expression::Literal(ref s) if s == "\\s+"));
                assert!(flags.is_none());
                assert_eq!(column, "regexp_split_to_table");
                assert!(ordinality_column.is_none());
            }
            other => panic!("expected RegexpSplitToTable, got {other:?}"),
        }
        let rows =
            materialize_regexp_split_to_table("aXbXc", "x", Some("i"), "part", None).unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].get("part"), Some("a"));
        assert_eq!(rows[1].get("part"), Some("b"));
        assert_eq!(rows[2].get("part"), Some("c"));
        let with_ord = materialize_regexp_split_to_table(
            "hello world",
            r"\s+",
            None,
            "part",
            Some("ordinality"),
        )
        .unwrap();
        assert_eq!(with_ord.len(), 2);
        assert_eq!(with_ord[0].get("part"), Some("hello"));
        assert_eq!(with_ord[0].get("ordinality"), Some("1"));
        assert_eq!(with_ord[1].get("ordinality"), Some("2"));
    }

    #[test]
    fn parses_cross_join_lateral_json_srf_literal() {
        match LogicalPlanner::plan(
            r#"SELECT * FROM generate_series(1, 2) AS g(n)
               CROSS JOIN LATERAL jsonb_array_elements('[10,20]'::jsonb) AS t(x)"#,
        )
        .unwrap()
        {
            LogicalPlan::Join {
                join_type,
                left,
                right,
                ..
            } => {
                assert_eq!(join_type, JoinType::Inner);
                assert!(matches!(left.as_ref(), LogicalPlan::GenerateSeries { .. }));
                assert!(matches!(
                    right.as_ref(),
                    LogicalPlan::JsonArrayElements { column, .. } if column == "x"
                ));
            }
            other => panic!("expected Join, got {other:?}"),
        }

        match LogicalPlanner::plan(
            r#"SELECT * FROM emp CROSS JOIN LATERAL jsonb_array_elements(emp.tags) AS t(x)"#,
        )
        .unwrap()
        {
            LogicalPlan::Join { right, .. } => match right.as_ref() {
                LogicalPlan::JsonArrayElements { doc, column, .. } => {
                    assert_eq!(column, "x");
                    assert!(
                        expr_needs_row_eval(doc),
                        "correlated doc expected, got {doc:?}"
                    );
                }
                other => panic!("expected JsonArrayElements, got {other:?}"),
            },
            other => panic!("expected Join, got {other:?}"),
        }
    }

    #[test]
    fn parses_set_and_show() {
        match LogicalPlanner::plan("SET search_path TO public").unwrap() {
            LogicalPlan::Set { name, value } => {
                assert_eq!(name, "search_path");
                assert_eq!(value, "public");
            }
            other => panic!("expected Set, got {other:?}"),
        }
        match LogicalPlanner::plan("SET transaction_isolation TO 'repeatable read'").unwrap() {
            LogicalPlan::Set { name, value } => {
                assert_eq!(name, "transaction_isolation");
                assert_eq!(value, "repeatable read");
            }
            other => panic!("expected Set isolation, got {other:?}"),
        }
        match LogicalPlanner::plan("SET TRANSACTION ISOLATION LEVEL READ COMMITTED").unwrap() {
            LogicalPlan::Set { name, value } => {
                assert_eq!(name, "transaction_isolation");
                assert_eq!(value, "read committed");
            }
            other => panic!("expected Set TRANSACTION, got {other:?}"),
        }
        let err = LogicalPlanner::plan("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE").unwrap();
        match err {
            LogicalPlan::Set { name, value } => {
                assert_eq!(name, "transaction_isolation");
                assert_eq!(value, "serializable");
            }
            other => panic!("expected Set SERIALIZABLE, got {other:?}"),
        }
        match LogicalPlanner::plan("SHOW search_path").unwrap() {
            LogicalPlan::Show { name } => assert_eq!(name, "search_path"),
            other => panic!("expected Show, got {other:?}"),
        }
    }

    #[test]
    fn parses_with_cte_and_in_subquery() {
        let sql = "WITH top_depts AS (\
            SELECT department FROM employees GROUP BY department LIMIT 2\
        ) SELECT * FROM employees WHERE department IN (SELECT department FROM top_depts)";
        let plan = LogicalPlanner::plan(sql).unwrap();
        match plan {
            LogicalPlan::Select {
                table,
                predicate: Some(Expression::InSubquery {
                    expr,
                    subquery,
                    value_column,
                    negated,
                    correlated,
                }),
                ..
            } => {
                assert_eq!(table, "employees");
                assert_eq!(*expr, Expression::Column("department".into()));
                assert_eq!(value_column, "department");
                assert!(!negated);
                assert!(!correlated);
                // Subquery FROM top_depts resolves to Project → SubqueryAlias → Limit → Aggregate.
                assert!(
                    matches!(
                        subquery.as_ref(),
                        LogicalPlan::Project { .. }
                            | LogicalPlan::Limit { .. }
                            | LogicalPlan::SubqueryAlias { .. }
                    ),
                    "expected Project/Limit/SubqueryAlias CTE body, got {subquery:?}"
                );
            }
            other => panic!("expected Select+InSubquery, got {other:?}"),
        }
    }

    #[test]
    fn parses_correlated_exists_with_outer_ref() {
        let plan = LogicalPlanner::plan(
            "SELECT id FROM employees e WHERE EXISTS (
                SELECT 1 FROM dept_budget d WHERE d.dept = e.dept
             )",
        )
        .unwrap();
        let pred = match &plan {
            LogicalPlan::Select {
                predicate: Some(p), ..
            }
            | LogicalPlan::Filter { predicate: p, .. } => p,
            LogicalPlan::Project { input, .. } => match input.as_ref() {
                LogicalPlan::Select {
                    predicate: Some(p), ..
                }
                | LogicalPlan::Filter { predicate: p, .. } => p,
                other => panic!("expected Select/Filter under Project, got {other:?}"),
            },
            other => panic!("expected Select/Filter/Project with predicate, got {other:?}"),
        };
        match pred {
            Expression::Exists {
                correlated: true,
                subquery,
                ..
            } => {
                let dbg = format!("{subquery:?}");
                assert!(
                    dbg.contains("OuterRef"),
                    "subquery should contain OuterRef, got {subquery:?}"
                );
            }
            other => panic!("expected correlated Exists, got {other:?}"),
        }
    }

    #[test]
    fn parses_exists_and_scalar_subquery() {
        let exists = LogicalPlanner::plan(
            "SELECT * FROM employees WHERE EXISTS (SELECT id FROM employees WHERE id = 1)",
        )
        .unwrap();
        match exists {
            LogicalPlan::Select {
                predicate: Some(Expression::Exists {
                    negated,
                    correlated,
                    ..
                }),
                ..
            } => {
                assert!(!negated);
                assert!(!correlated);
            }
            other => panic!("expected Exists, got {other:?}"),
        }

        let scalar = LogicalPlanner::plan(
            "SELECT * FROM employees WHERE salary = (SELECT salary FROM employees WHERE id = 1)",
        )
        .unwrap();
        match scalar {
            LogicalPlan::Select {
                predicate: Some(Expression::BinaryOp { right, .. }),
                ..
            } => {
                assert!(matches!(right.as_ref(), Expression::ScalarSubquery { .. }));
            }
            other => panic!("expected scalar subquery in comparison, got {other:?}"),
        }
    }

    #[test]
    fn parses_analyze_table() {
        let plan = LogicalPlanner::plan("ANALYZE employees").unwrap();
        match plan {
            LogicalPlan::Analyze { table } => assert_eq!(table, "employees"),
            other => panic!("expected Analyze, got {other:?}"),
        }
    }

    #[test]
    fn parses_vacuum_table() {
        let plan = LogicalPlanner::plan("VACUUM employees").unwrap();
        match plan {
            LogicalPlan::Vacuum { table } => assert_eq!(table, "employees"),
            other => panic!("expected Vacuum, got {other:?}"),
        }
    }

    #[test]
    fn parses_create_user_grant_revoke() {
        match LogicalPlanner::plan("CREATE USER analyst WITH PASSWORD 'secret'").unwrap() {
            LogicalPlan::CreateRole {
                name,
                can_login,
                password: Some(pw),
                is_superuser,
                ..
            } => {
                assert_eq!(name, "analyst");
                assert!(can_login);
                assert!(!is_superuser);
                assert_eq!(pw, "secret");
            }
            other => panic!("expected CreateRole/user, got {other:?}"),
        }

        match LogicalPlanner::plan("CREATE ROLE analysts").unwrap() {
            LogicalPlan::CreateRole {
                name,
                can_login,
                password: None,
                ..
            } => {
                assert_eq!(name, "analysts");
                assert!(!can_login);
            }
            other => panic!("expected CreateRole, got {other:?}"),
        }

        match LogicalPlanner::plan("GRANT SELECT ON employees TO analyst").unwrap() {
            LogicalPlan::Grant {
                privileges,
                table,
                grantee,
            } => {
                assert_eq!(privileges, vec![crate::rbac::Privilege::Select]);
                assert_eq!(table, "employees");
                assert_eq!(grantee, "analyst");
            }
            other => panic!("expected Grant, got {other:?}"),
        }

        match LogicalPlanner::plan("GRANT USAGE, CREATE ON SCHEMA public TO analyst").unwrap() {
            LogicalPlan::GrantSchema {
                privileges,
                schema,
                grantee,
            } => {
                assert_eq!(
                    privileges,
                    vec![
                        crate::rbac::SchemaPrivilege::Usage,
                        crate::rbac::SchemaPrivilege::Create,
                    ]
                );
                assert_eq!(schema, "public");
                assert_eq!(grantee, "analyst");
            }
            other => panic!("expected GrantSchema, got {other:?}"),
        }

        match LogicalPlanner::plan("GRANT SELECT (name), UPDATE (name) ON employees TO analyst")
            .unwrap()
        {
            LogicalPlan::GrantColumn {
                specs,
                table,
                grantee,
            } => {
                assert_eq!(table, "employees");
                assert_eq!(grantee, "analyst");
                assert_eq!(specs.len(), 2);
                assert_eq!(specs[0].privilege, crate::rbac::ColumnPrivilege::Select);
                assert_eq!(specs[0].columns, vec!["name".to_string()]);
                assert_eq!(specs[1].privilege, crate::rbac::ColumnPrivilege::Update);
            }
            other => panic!("expected GrantColumn, got {other:?}"),
        }

        match LogicalPlanner::plan("REVOKE CREATE ON SCHEMA public FROM analyst").unwrap() {
            LogicalPlan::RevokeSchema {
                privileges,
                schema,
                grantee,
            } => {
                assert_eq!(privileges, vec![crate::rbac::SchemaPrivilege::Create]);
                assert_eq!(schema, "public");
                assert_eq!(grantee, "analyst");
            }
            other => panic!("expected RevokeSchema, got {other:?}"),
        }

        match LogicalPlanner::plan("REVOKE SELECT ON employees FROM analyst").unwrap() {
            LogicalPlan::Revoke {
                privileges,
                table,
                grantee,
            } => {
                assert_eq!(privileges, vec![crate::rbac::Privilege::Select]);
                assert_eq!(table, "employees");
                assert_eq!(grantee, "analyst");
            }
            other => panic!("expected Revoke, got {other:?}"),
        }

        match LogicalPlanner::plan("GRANT analysts TO analyst").unwrap() {
            LogicalPlan::GrantRole { role, member } => {
                assert_eq!(role, "analysts");
                assert_eq!(member, "analyst");
            }
            other => panic!("expected GrantRole, got {other:?}"),
        }
    }
}

fn normalize_guc_name(name: &str) -> String {
    name.trim().trim_matches('"').to_ascii_lowercase().replace('-', "_")
}

fn set_value_to_string(expr: &Expr) -> Result<String> {
    match expr {
        Expr::Identifier(ident) => Ok(ident.value.clone()),
        Expr::Value(ValueWithSpan { value, .. }) => match value {
            SqlValue::SingleQuotedString(s)
            | SqlValue::DoubleQuotedString(s)
            | SqlValue::NationalStringLiteral(s)
            | SqlValue::HexStringLiteral(s) => Ok(s.clone()),
            SqlValue::Number(n, _) => Ok(n.clone()),
            SqlValue::Boolean(b) => Ok(b.to_string()),
            SqlValue::Null => Ok("".into()),
            other => Ok(other.to_string()),
        },
        Expr::CompoundIdentifier(parts) => Ok(parts
            .iter()
            .map(|p| p.value.as_str())
            .collect::<Vec<_>>()
            .join(".")),
        other => Ok(other.to_string()),
    }
}

fn normalize_guc_value(name: &str, value: &str) -> Result<String> {
    let v = value.trim().trim_matches('\'').trim_matches('"');
    match name {
        "transaction_isolation" => normalize_transaction_isolation(v),
        "search_path" => Ok(v.to_string()),
        "timezone" | "time_zone" => normalize_timezone(v),
        _ => Err(TakyonicError::Sql(format!(
            "unsupported SET variable `{name}` \
             (supported: search_path, transaction_isolation, TimeZone)"
        ))),
    }
}

/// Takyonic default is Snapshot Isolation + OCC (PostgreSQL-equivalent:
/// `repeatable read`). `read committed` is accepted as a GUC alias — the
/// engine still provides SI (stronger than requested). `serializable` enables
/// minimal SSI (write-skew abort). Dirty reads (`read uncommitted`) are rejected.
pub fn normalize_transaction_isolation(value: &str) -> Result<String> {
    let n = value
        .trim()
        .trim_matches('\'')
        .trim_matches('"')
        .to_ascii_lowercase()
        .replace('-', " ");
    match n.as_str() {
        "read committed" | "repeatable read" | "serializable" => Ok(n),
        "read uncommitted" => Err(TakyonicError::Sql(
            "transaction_isolation `read uncommitted` is not supported \
             (dirty reads are never allowed; use `repeatable read` or `serializable`)"
                .into(),
        )),
        other => Err(TakyonicError::Sql(format!(
            "invalid transaction_isolation `{other}` \
             (supported: read committed, repeatable read, serializable)"
        ))),
    }
}

fn transaction_isolation_name(level: &sqlparser::ast::TransactionIsolationLevel) -> Result<String> {
    use sqlparser::ast::TransactionIsolationLevel::*;
    let raw = match level {
        ReadUncommitted => "read uncommitted",
        ReadCommitted => "read committed",
        RepeatableRead | Snapshot => "repeatable read",
        Serializable => "serializable",
    };
    normalize_transaction_isolation(raw)
}




