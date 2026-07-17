//! SQL parser + logical planner — bridge from standard SQL into Takyonic's CBO / MVCC APIs.
//!
//! ```ignore
//! let plan = LogicalPlanner::plan(
//!     "SELECT * FROM users WHERE status = 'active' AND city = 'Ankara'"
//! )?;
//! // → LogicalPlan::Select { table: "users", filters: [status=active, city=Ankara] }
//! ```

use sqlparser::ast::{
    BinaryOperator, Expr, ObjectName, ObjectNamePart, Query, SetExpr, Statement, TableFactor,
    TableObject, Value as SqlValue, ValueWithSpan,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::error::{Result, TakyonicError};
use crate::query::{Filter, FilterOp};
use crate::schema::Record;

/// Logical plan produced by translating a SQL AST into Takyonic primitives.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LogicalPlan {
    /// `SELECT ... FROM table WHERE ...` → CBO filters.
    Select {
        /// Target table.
        table: String,
        /// Conjunctive equality/range predicates from WHERE.
        filters: Vec<Filter>,
    },
    /// `INSERT INTO table (cols...) VALUES (...)` → structured records.
    Insert {
        /// Target table.
        table: String,
        /// One record per VALUES row.
        records: Vec<Record>,
    },
}

/// SQL → AST → [`LogicalPlan`] translator.
pub struct LogicalPlanner;

impl LogicalPlanner {
    /// Parse a single SQL statement into a [`LogicalPlan`].
    pub fn plan(sql: &str) -> Result<LogicalPlan> {
        let dialect = GenericDialect {};
        let stmts = Parser::parse_sql(&dialect, sql)
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
            other => Err(TakyonicError::Sql(format!(
                "unsupported statement: {other}"
            ))),
        }
    }

    fn plan_select(query: &Query) -> Result<LogicalPlan> {
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
        let table = table_factor_name(&select.from[0].relation)?;
        let filters = match &select.selection {
            Some(expr) => flatten_and_predicates(expr)?,
            None => Vec::new(),
        };
        Ok(LogicalPlan::Select { table, filters })
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
        let values = match source.body.as_ref() {
            SetExpr::Values(v) => v,
            other => {
                return Err(TakyonicError::Sql(format!(
                    "INSERT supports VALUES only, got {other}"
                )));
            }
        };
        let mut records = Vec::with_capacity(values.rows.len());
        for row in &values.rows {
            if row.len() != columns.len() {
                return Err(TakyonicError::Sql(format!(
                    "INSERT row has {} values for {} columns",
                    row.len(),
                    columns.len()
                )));
            }
            let mut record = Record::new();
            for (col, expr) in columns.iter().zip(row.iter()) {
                record = record.set(col.clone(), expr_literal(expr)?);
            }
            records.push(record);
        }
        Ok(LogicalPlan::Insert { table, records })
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
            LogicalPlan::Select { table, filters } => {
                assert_eq!(table, "users");
                assert_eq!(filters.len(), 2);
                assert_eq!(filters[0].column, "status");
                assert_eq!(filters[0].op, FilterOp::Eq);
                assert_eq!(filters[0].value, "active");
                assert_eq!(filters[1].column, "city");
                assert_eq!(filters[1].value, "Ankara");
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
            LogicalPlan::Insert { table, records } => {
                assert_eq!(table, "users");
                assert_eq!(records.len(), 1);
                assert_eq!(records[0].get("id"), Some("1"));
                assert_eq!(records[0].get("name"), Some("Ada"));
                assert_eq!(records[0].get("city"), Some("Bursa"));
            }
            other => panic!("expected Insert, got {other:?}"),
        }
    }
}
