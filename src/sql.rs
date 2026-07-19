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
    Action, Analyze, Array, AssignmentTarget, BinaryOperator, CreateIndex, CreateRole, CreateUser,
    Expr, FromTable, Function, FunctionArg, FunctionArgExpr, FunctionArguments, Grant, GrantObjects,
    Grantee, GranteeName, GroupByExpr, JoinConstraint, JoinOperator, LimitClause, ObjectName,
    ObjectNamePart, ObjectType, OrderBy, OrderByExpr, OrderByKind, Password,
    Privileges as AstPrivileges, Query, Revoke, SelectItem, SetExpr, Statement, TableFactor,
    TableObject, VacuumStatement, Value as SqlValue, ValueWithSpan,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use crate::error::{Result, TakyonicError};
use crate::query::{Filter, FilterOp};
use crate::rbac::Privilege;
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

/// One `ORDER BY` key: expression + direction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SortExpr {
    /// Sort key expression (column, aggregate result column, …).
    pub expr: Expression,
    /// `true` = ASC (default), `false` = DESC.
    pub asc: bool,
}

impl SortExpr {
    /// Build an ascending sort key.
    pub fn asc(expr: Expression) -> Self {
        Self { expr, asc: true }
    }

    /// Build a descending sort key.
    pub fn desc(expr: Expression) -> Self {
        Self { expr, asc: false }
    }
}

/// Stable output column name for an aggregate expression (`sum(salary)`, `count(*)`, …).
///
/// Matches the field names emitted by [`crate::executor::AggregateExec`].
pub fn aggregate_result_column(expr: &Expression) -> Option<String> {
    match expr {
        Expression::AggregateFunction { name, args } => {
            let lower = name.to_ascii_lowercase();
            if args.is_empty() {
                Some(format!("{lower}(*)"))
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
                Some(format!("{lower}({arg})"))
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
        other => other,
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
    /// Aggregate call (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`).
    ///
    /// Only valid inside [`LogicalPlan::Aggregate`]; scalar `evaluate` rejects these.
    AggregateFunction {
        /// Uppercase function name (`COUNT`, `SUM`, …).
        name: String,
        /// Function arguments (`COUNT(*)` → empty; `SUM(salary)` → `[Column("salary")]`).
        args: Vec<Expression>,
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
    /// `INSERT INTO table (cols...) VALUES (...)`.
    Insert {
        /// Target table.
        table: String,
        /// Explicit column list.
        columns: Vec<String>,
        /// One expression row per VALUES tuple (may contain `$n` parameters).
        values: Vec<Vec<Expression>>,
    },
    /// `UPDATE table SET col = expr, ... [WHERE ...]`.
    Update {
        /// Target table.
        table: String,
        /// Column → assignment expression.
        assignments: HashMap<String, Expression>,
        /// Optional WHERE predicate.
        selection: Option<Expression>,
    },
    /// `DELETE FROM table [WHERE ...]`.
    Delete {
        /// Target table.
        table: String,
        /// Optional WHERE predicate.
        selection: Option<Expression>,
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
    /// `LIMIT` / `OFFSET` — skip then fetch from the child stream.
    Limit {
        /// Child plan.
        input: Box<LogicalPlan>,
        /// Rows to skip (`OFFSET`).
        skip: usize,
        /// Max rows to yield (`LIMIT`); `None` = unbounded (OFFSET-only).
        fetch: Option<usize>,
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
    /// `DROP INDEX name`.
    DropIndex {
        /// Index name.
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
            other => Err(TakyonicError::Sql(format!(
                "unsupported statement: {other}"
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

        let select = match query.body.as_ref() {
            SetExpr::Select(s) => s.as_ref(),
            other => {
                return Err(TakyonicError::Sql(format!(
                    "unsupported query body: {other}"
                )));
            }
        };
        if select.from.len() != 1 {
            return Err(TakyonicError::Sql(
                "SELECT requires exactly one FROM table".into(),
            ));
        }
        let from = &select.from[0];

        // Resolve FROM (base table, CTE alias, or derived subquery).
        let mut plan = Self::plan_from_item(&from.relation, &ctes)?;
        for join in &from.joins {
            let (join_type, on) = plan_join_operator_ctx(&join.join_operator, &ctes, outer_columns)?;
            let right = Self::plan_from_item(&join.relation, &ctes)?;
            plan = LogicalPlan::Join {
                left: Box::new(plan),
                right: Box::new(right),
                on,
                join_type,
            };
        }

        // WHERE — may contain IN/EXISTS/scalar subqueries.
        let scope = {
            let mut s = outer_columns.to_vec();
            s.extend(collect_plan_output_hints(&plan));
            s
        };
        if let Some(selection) = &select.selection {
            let (filters, predicate) =
                plan_where_ctx(Some(selection), &ctes, &scope)?;
            plan = match plan {
                LogicalPlan::Select {
                    table,
                    filters: mut existing,
                    predicate: None,
                } if from.joins.is_empty() => {
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

        let (group_exprs, aggr_exprs, has_agg, having) =
            plan_projection_aggregates_ctx(select, &ctes, &scope)?;
        if has_agg || !group_exprs.is_empty() {
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

        if let Some(order_by) = &query.order_by {
            let exprs = plan_order_by_ctx(order_by, &ctes, &scope)?;
            if !exprs.is_empty() {
                plan = LogicalPlan::Sort {
                    input: Box::new(plan),
                    exprs,
                };
            }
        }

        if let Some(limit_clause) = &query.limit_clause {
            let (skip, fetch) = plan_limit_clause(limit_clause)?;
            if skip > 0 || fetch.is_some() {
                plan = LogicalPlan::Limit {
                    input: Box::new(plan),
                    skip,
                    fetch,
                };
            }
        }

        Ok(plan)
    }

    fn plan_from_item(
        factor: &TableFactor,
        ctes: &HashMap<String, LogicalPlan>,
    ) -> Result<LogicalPlan> {
        match factor {
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
                let inner = Self::plan_query(subquery, ctes, &[])?;
                let alias = alias
                    .as_ref()
                    .map(|a| a.name.value.clone())
                    .ok_or_else(|| {
                        TakyonicError::Sql(
                            "derived FROM subquery requires an alias".into(),
                        )
                    })?;
                Ok(LogicalPlan::SubqueryAlias {
                    alias,
                    input: Box::new(inner),
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
        let value_rows = match source.body.as_ref() {
            SetExpr::Values(v) => v,
            other => {
                return Err(TakyonicError::Sql(format!(
                    "INSERT supports VALUES only, got {other}"
                )));
            }
        };
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
        })
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
        Ok(LogicalPlan::Update {
            table,
            assignments,
            selection,
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
        Ok(LogicalPlan::Delete { table, selection })
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
        let privileges = ast_privileges_to_rbac(&grant.privileges)?;
        let table = grant_object_table(grant.objects.as_ref())?;
        let grantee = grantee_name(&grant.grantees)?;
        Ok(LogicalPlan::Grant {
            privileges,
            table,
            grantee,
        })
    }

    fn plan_revoke(revoke: &Revoke) -> Result<LogicalPlan> {
        let privileges = ast_privileges_to_rbac(&revoke.privileges)?;
        let table = grant_object_table(revoke.objects.as_ref())?;
        let grantee = grantee_name(&revoke.grantees)?;
        Ok(LogicalPlan::Revoke {
            privileges,
            table,
            grantee,
        })
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
    plan_where_ctx(selection, &HashMap::new(), &[])
}

fn plan_where_ctx(
    selection: Option<&Expr>,
    ctes: &HashMap<String, LogicalPlan>,
    outer_columns: &[String],
) -> Result<(Vec<Filter>, Option<Expression>)> {
    match selection {
        None => Ok((Vec::new(), None)),
        Some(expr) => {
            let predicate = expr_to_expression_ctx(expr, ctes, outer_columns)?;
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
        | LogicalPlan::Limit { input, .. } => collect_plan_output_hints(input),
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
        | Expression::VectorDistance { left, right, .. } => {
            expression_has_subquery(left) || expression_has_subquery(right)
        }
        Expression::InList { expr, .. } => expression_has_subquery(expr),
        Expression::AggregateFunction { args, .. } => args.iter().any(expression_has_subquery),
        Expression::Array(items) => items.iter().any(expression_has_subquery),
        Expression::Column(_) | Expression::Literal(_) | Expression::Parameter(_) => false,
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
        LogicalPlan::Limit { input, .. } | LogicalPlan::SubqueryAlias { input, .. } => {
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
        | Expression::VectorDistance { left, right, .. } => {
            walk_columns(left, f);
            walk_columns(right, f);
        }
        Expression::InSubquery { expr, .. } | Expression::InList { expr, .. } => {
            walk_columns(expr, f);
        }
        Expression::AggregateFunction { args, .. } => {
            for a in args {
                walk_columns(a, f);
            }
        }
        Expression::Array(items) => {
            for a in items {
                walk_columns(a, f);
            }
        }
        Expression::Exists { .. }
        | Expression::ScalarSubquery { .. }
        | Expression::Literal(_)
        | Expression::Parameter(_) => {}
    }
}

/// Rewrite `CREATE VECTOR INDEX` / `CREATE USER … PASSWORD` into forms sqlparser accepts.
fn preprocess_sql(sql: &str) -> String {
    let trimmed = sql.trim_start();
    let upper = trimmed.to_ascii_uppercase();
    if let Some(rest) = upper.strip_prefix("CREATE VECTOR INDEX") {
        let orig_rest = &trimmed[trimmed.len() - rest.len()..];
        format!("CREATE INDEX{orig_rest}")
    } else if let Some(rest) = upper.strip_prefix("EXPLAIN CREATE VECTOR INDEX") {
        let orig_rest = &trimmed[trimmed.len() - rest.len()..];
        format!("EXPLAIN CREATE INDEX{orig_rest}")
    } else if let Some(rewritten) = rewrite_create_user(trimmed) {
        rewritten
    } else {
        sql.to_string()
    }
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

/// First projected column name of a SELECT list (for IN/scalar subquery keys).
fn first_projection_column(
    select: &sqlparser::ast::Select,
    ctes: &HashMap<String, LogicalPlan>,
    outer_columns: &[String],
) -> Result<String> {
    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(e) => {
                let planned = expr_to_expression_ctx(e, ctes, outer_columns)?;
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

/// Split SELECT list + GROUP BY into grouping keys and aggregate expressions.
///
/// Returns `(group_exprs, aggr_exprs, has_aggregate_in_projection)`.
#[allow(dead_code)]
fn plan_projection_aggregates(
    select: &sqlparser::ast::Select,
) -> Result<(Vec<Expression>, Vec<Expression>, bool, Option<Expression>)> {
    plan_projection_aggregates_ctx(select, &HashMap::new(), &[])
}

fn plan_projection_aggregates_ctx(
    select: &sqlparser::ast::Select,
    ctes: &HashMap<String, LogicalPlan>,
    outer_columns: &[String],
) -> Result<(Vec<Expression>, Vec<Expression>, bool, Option<Expression>)> {
    let group_exprs = plan_group_by_ctx(&select.group_by, ctes, outer_columns)?;
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
        let planned = expr_to_expression_ctx(expr, ctes, outer_columns)?;
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
        Some(expr_to_expression_ctx(h, ctes, outer_columns)?)
    } else {
        None
    };

    Ok((group_exprs, aggr_exprs, has_agg, having))
}

#[allow(dead_code)]
fn plan_group_by(group_by: &GroupByExpr) -> Result<Vec<Expression>> {
    plan_group_by_ctx(group_by, &HashMap::new(), &[])
}

fn plan_group_by_ctx(
    group_by: &GroupByExpr,
    ctes: &HashMap<String, LogicalPlan>,
    outer_columns: &[String],
) -> Result<Vec<Expression>> {
    match group_by {
        GroupByExpr::Expressions(exprs, modifiers) => {
            if !modifiers.is_empty() {
                return Err(TakyonicError::Sql(
                    "GROUP BY modifiers (ROLLUP/CUBE/…) are unsupported".into(),
                ));
            }
            exprs
                .iter()
                .map(|e| expr_to_expression_ctx(e, ctes, outer_columns))
                .collect()
        }
        GroupByExpr::All(_) => Err(TakyonicError::Sql(
            "GROUP BY ALL is not supported".into(),
        )),
    }
}

#[allow(dead_code)]
fn plan_order_by(order_by: &OrderBy) -> Result<Vec<SortExpr>> {
    plan_order_by_ctx(order_by, &HashMap::new(), &[])
}

fn plan_order_by_ctx(
    order_by: &OrderBy,
    ctes: &HashMap<String, LogicalPlan>,
    outer_columns: &[String],
) -> Result<Vec<SortExpr>> {
    if order_by.interpolate.is_some() {
        return Err(TakyonicError::Sql(
            "ORDER BY INTERPOLATE is not supported".into(),
        ));
    }
    match &order_by.kind {
        OrderByKind::All(_) => Err(TakyonicError::Sql(
            "ORDER BY ALL is not supported".into(),
        )),
        OrderByKind::Expressions(exprs) => exprs
            .iter()
            .map(|e| plan_order_by_expr_ctx(e, ctes, outer_columns))
            .collect(),
    }
}

#[allow(dead_code)]
fn plan_order_by_expr(obe: &OrderByExpr) -> Result<SortExpr> {
    plan_order_by_expr_ctx(obe, &HashMap::new(), &[])
}

fn plan_order_by_expr_ctx(
    obe: &OrderByExpr,
    ctes: &HashMap<String, LogicalPlan>,
    outer_columns: &[String],
) -> Result<SortExpr> {
    if obe.with_fill.is_some() {
        return Err(TakyonicError::Sql(
            "ORDER BY WITH FILL is not supported".into(),
        ));
    }
    let expr = rewrite_sort_expr_for_output(expr_to_expression_ctx(
        &obe.expr,
        ctes,
        outer_columns,
    )?);
    let asc = obe.options.asc.unwrap_or(true);
    Ok(SortExpr { expr, asc })
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
        "COUNT" | "SUM" | "AVG" | "MIN" | "MAX"
    )
}

#[allow(dead_code)]
fn function_to_expression(func: &Function) -> Result<Expression> {
    function_to_expression_ctx(func, &HashMap::new(), &[])
}

fn function_to_expression_ctx(
    func: &Function,
    ctes: &HashMap<String, LogicalPlan>,
    outer_columns: &[String],
) -> Result<Expression> {
    let name = object_name_leaf(&func.name)?;
    let upper = name.to_ascii_uppercase();
    if !is_aggregate_fn(&upper) {
        return Err(TakyonicError::Sql(format!(
            "unsupported function `{name}` (only COUNT/SUM/AVG/MIN/MAX)"
        )));
    }
    if func.over.is_some() {
        return Err(TakyonicError::Sql(
            "window functions (OVER) are not supported".into(),
        ));
    }
    if func.filter.is_some() {
        return Err(TakyonicError::Sql(
            "FILTER (WHERE …) on aggregates is not supported".into(),
        ));
    }
    let args = match &func.args {
        FunctionArguments::None => Vec::new(),
        FunctionArguments::List(list) => {
            let mut out = Vec::with_capacity(list.args.len());
            for arg in &list.args {
                out.push(function_arg_to_expression_ctx(arg, ctes, outer_columns)?);
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
    Ok(Expression::AggregateFunction {
        name: upper,
        args,
    })
}

#[allow(dead_code)]
fn function_arg_to_expression(arg: &FunctionArg) -> Result<Expression> {
    function_arg_to_expression_ctx(arg, &HashMap::new(), &[])
}

fn function_arg_to_expression_ctx(
    arg: &FunctionArg,
    ctes: &HashMap<String, LogicalPlan>,
    outer_columns: &[String],
) -> Result<Expression> {
    match arg {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => {
            expr_to_expression_ctx(e, ctes, outer_columns)
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
    plan_join_operator_ctx(op, &HashMap::new(), &[])
}

fn plan_join_operator_ctx(
    op: &JoinOperator,
    ctes: &HashMap<String, LogicalPlan>,
    outer_columns: &[String],
) -> Result<(JoinType, Expression)> {
    let (join_type, constraint) = match op {
        JoinOperator::Join(c) | JoinOperator::Inner(c) => (JoinType::Inner, c),
        JoinOperator::Left(c) | JoinOperator::LeftOuter(c) => (JoinType::Left, c),
        JoinOperator::Right(c) | JoinOperator::RightOuter(c) => (JoinType::Right, c),
        JoinOperator::FullOuter(c) => (JoinType::Full, c),
        other => {
            return Err(TakyonicError::Sql(format!(
                "unsupported JOIN operator: {other:?}"
            )));
        }
    };
    let on = match constraint {
        JoinConstraint::On(expr) => expr_to_expression_ctx(expr, ctes, outer_columns)?,
        other => {
            return Err(TakyonicError::Sql(format!(
                "JOIN requires ON condition, got {other:?}"
            )));
        }
    };
    Ok((join_type, on))
}

/// Translate a SQL expression into our simplified [`Expression`] tree.
fn expr_to_expression(expr: &Expr) -> Result<Expression> {
    expr_to_expression_ctx(expr, &HashMap::new(), &[])
}

fn expr_to_expression_ctx(
    expr: &Expr,
    ctes: &HashMap<String, LogicalPlan>,
    outer_columns: &[String],
) -> Result<Expression> {
    match expr {
        Expr::Identifier(ident) => Ok(Expression::Column(ident.value.clone())),
        Expr::CompoundIdentifier(parts) => parts
            .last()
            .map(|i| Expression::Column(i.value.clone()))
            .ok_or_else(|| TakyonicError::Sql("empty compound identifier".into())),
        Expr::Value(ValueWithSpan { value, .. }) => match value {
            SqlValue::Placeholder(p) => Ok(Expression::Parameter(parse_placeholder(p)?)),
            other => Ok(Expression::Literal(sql_value_to_string(other)?)),
        },
        Expr::Nested(inner) => expr_to_expression_ctx(inner, ctes, outer_columns),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => Ok(Expression::And {
            left: Box::new(expr_to_expression_ctx(left, ctes, outer_columns)?),
            right: Box::new(expr_to_expression_ctx(right, ctes, outer_columns)?),
        }),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Or,
            right,
        } => Ok(Expression::Or {
            left: Box::new(expr_to_expression_ctx(left, ctes, outer_columns)?),
            right: Box::new(expr_to_expression_ctx(right, ctes, outer_columns)?),
        }),
        Expr::BinaryOp { left, op, right } => {
            let left_e = Box::new(expr_to_expression_ctx(left, ctes, outer_columns)?);
            let right_e = Box::new(expr_to_expression_ctx(right, ctes, outer_columns)?);
            if let Some(arith) = match op {
                BinaryOperator::Plus => Some(ArithOp::Add),
                BinaryOperator::Minus => Some(ArithOp::Sub),
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
            if matches!(op, BinaryOperator::LtDashGt) {
                return Ok(Expression::VectorDistance {
                    left: left_e,
                    right: right_e,
                    metric: DistanceMetric::Euclidean,
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
                items.push(expr_to_expression_ctx(e, ctes, outer_columns)?);
            }
            Ok(Expression::Array(items))
        }
        Expr::Function(func) => function_to_expression_ctx(func, ctes, outer_columns),
        Expr::InSubquery {
            expr: left,
            subquery,
            negated,
        } => {
            let left_expr = expr_to_expression_ctx(left, ctes, outer_columns)?;
            let sub_plan = LogicalPlanner::plan_query(subquery, ctes, outer_columns)?;
            let value_column = subquery_value_column(subquery, ctes, outer_columns)?;
            let correlated = plan_is_correlated(&sub_plan, outer_columns);
            Ok(Expression::InSubquery {
                expr: Box::new(left_expr),
                subquery: Box::new(sub_plan),
                value_column,
                negated: *negated,
                correlated,
            })
        }
        Expr::Exists { subquery, negated } => {
            let sub_plan = LogicalPlanner::plan_query(subquery, ctes, outer_columns)?;
            let correlated = plan_is_correlated(&sub_plan, outer_columns);
            Ok(Expression::Exists {
                subquery: Box::new(sub_plan),
                negated: *negated,
                correlated,
            })
        }
        Expr::Subquery(subquery) => {
            let sub_plan = LogicalPlanner::plan_query(subquery, ctes, outer_columns)?;
            let value_column = subquery_value_column(subquery, ctes, outer_columns)?;
            let correlated = plan_is_correlated(&sub_plan, outer_columns);
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
            let left_expr = expr_to_expression_ctx(left, ctes, outer_columns)?;
            let mut values = Vec::with_capacity(list.len());
            for item in list {
                match expr_to_expression_ctx(item, ctes, outer_columns)? {
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
        other => Err(TakyonicError::Sql(format!(
            "unsupported expression: {other}"
        ))),
    }
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
    fn parses_update_set_where() {
        let plan =
            LogicalPlanner::plan("UPDATE users SET age = 31 WHERE name = 'Ada'").unwrap();
        match plan {
            LogicalPlan::Update {
                table,
                assignments,
                selection,
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
    fn parses_delete_where() {
        let plan = LogicalPlanner::plan("DELETE FROM users WHERE age < 25").unwrap();
        match plan {
            LogicalPlan::Delete { table, selection } => {
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
                    }
                );
                assert_eq!(
                    aggr_exprs[1],
                    Expression::AggregateFunction {
                        name: "SUM".into(),
                        args: vec![Expression::Column("salary".into())],
                    }
                );
            }
            other => panic!("expected Aggregate, got {other:?}"),
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
                    }
                );
            }
            other => panic!("expected Aggregate for COUNT(*), got {other:?}"),
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
                input,
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
                input,
            } => match input.as_ref() {
                LogicalPlan::Sort { exprs, .. } => {
                    assert_eq!(exprs[0].expr, Expression::Column("name".into()));
                    assert!(exprs[0].asc);
                }
                other => panic!("expected Sort, got {other:?}"),
            },
            other => panic!("expected Limit with offset, got {other:?}"),
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
                // Subquery FROM top_depts resolves to SubqueryAlias → Limit → Aggregate.
                assert!(
                    matches!(
                        subquery.as_ref(),
                        LogicalPlan::Limit { .. }
                            | LogicalPlan::SubqueryAlias { .. }
                    ) || matches!(
                        subquery.as_ref(),
                        LogicalPlan::SubqueryAlias { input, .. }
                            if matches!(input.as_ref(), LogicalPlan::Limit { .. })
                    ),
                    "expected Limit/SubqueryAlias CTE body, got {subquery:?}"
                );
            }
            other => panic!("expected Select+InSubquery, got {other:?}"),
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
