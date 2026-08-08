//! PostgreSQL wire-protocol (pgwire) facade over Takyonic's SQL / Smart Client stack.
//!
//! [`TakyonicPgBackend`] implements both [`SimpleQueryHandler`] and
//! [`ExtendedQueryHandler`] (Parse / Bind / Execute / Sync prepared-statement
//! flow). Session scaffolding lives in [`SessionState`].

use std::collections::HashMap;
use std::fmt::Debug;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use futures::sink::{Sink, SinkExt};
use futures::stream;
use parking_lot::Mutex;
use pgwire::api::auth::sasl::SASLAuthStartupHandler;
use pgwire::api::auth::sasl::scram::ScramAuth;
use pgwire::api::auth::{
    AuthSource, DefaultServerParameterProvider, LoginInfo, Password, StartupHandler,
};
use pgwire::api::copy::CopyHandler;
use pgwire::api::portal::Portal;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    CopyResponse, DataRowEncoder, DescribePortalResponse, DescribeStatementResponse, FieldFormat,
    FieldInfo, QueryResponse, Response, Tag,
};
use pgwire::api::stmt::{QueryParser, StoredStatement};
use pgwire::api::store::PortalStore;
use pgwire::api::{ClientInfo, METADATA_USER, PgWireServerHandlers, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::copy::{CopyData, CopyDone, CopyFail};
use pgwire::messages::response::CommandComplete;
use pgwire::messages::PgWireBackendMessage;
use tracing::debug;

use crate::client::TakyonicClient;
use crate::dtxn::{
    DistTxnOutcome, DistTxnRequest, ShardId, ShardParticipant, partition_txn_branches,
};
use crate::engine::TakyonicEngine;
use crate::executor::{self, ExecutionContext, affected_row_count};
use crate::partition::PartitionRouter;
use crate::rbac::{AuthCatalog, AuthContext, AuthorizationManager, SharedAuthCatalog};
use crate::schema::{ColumnSpec, Record};
use crate::sql::{ctas_output_columns, Expression, LogicalPlan, Returning, SqlEngine, Value};
use crate::txn::Transaction;

fn connect_remote_shard_blocking(
    id: ShardId,
    sock: SocketAddr,
) -> crate::error::Result<Arc<crate::twopc_service::RemoteShard>> {
    let connect = crate::twopc_service::RemoteShard::connect(id, sock);
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => Ok(tokio::task::block_in_place(|| handle.block_on(connect))?),
        Err(_) => {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| {
                    crate::error::TakyonicError::Network(format!("tokio runtime: {e}"))
                })?;
            Ok(rt.block_on(connect)?)
        }
    }
}

/// A portal: a prepared [`LogicalPlan`] bound to concrete parameter values.
#[derive(Clone, Debug)]
pub struct BoundPlan {
    /// Parsed logical plan.
    pub plan: LogicalPlan,
    /// Bind-time parameters decoded into engine [`Value`]s (`$1` = index 0).
    pub parameters: Vec<Value>,
}

/// Result of running a statement through [`SessionState`].
#[derive(Clone, Debug)]
pub struct SessionResult {
    /// PostgreSQL command tag stem (`BEGIN`, `INSERT`, `SELECT`, …).
    pub tag: &'static str,
    /// Rows for SELECT/JOIN; empty for DDL-ish / txn control / DML tags.
    pub rows: Vec<Record>,
    /// Affected-row count for INSERT/UPDATE/DELETE.
    pub affected: Option<u64>,
    /// SELECT-list column order when a [`LogicalPlan::Project`] wrapped the plan.
    pub column_order: Option<Vec<String>>,
}

impl SessionResult {
    fn command(tag: &'static str) -> Self {
        Self {
            tag,
            rows: Vec::new(),
            affected: None,
            column_order: None,
        }
    }

    fn from_data_plan(plan: &LogicalPlan, rows: Vec<Record>) -> Self {
        let column_order = projection_column_order(plan);
        match plan {
            LogicalPlan::Insert { returning, .. } => {
                if returning.is_some() {
                    Self {
                        tag: "INSERT",
                        affected: Some(rows.len() as u64),
                        rows,
                        column_order,
                    }
                } else {
                    Self {
                        tag: "INSERT",
                        affected: Some(affected_row_count(&rows)),
                        rows: Vec::new(),
                        column_order: None,
                    }
                }
            }
            LogicalPlan::Update { returning, .. } => {
                if returning.is_some() {
                    Self {
                        tag: "UPDATE",
                        affected: Some(rows.len() as u64),
                        rows,
                        column_order,
                    }
                } else {
                    Self {
                        tag: "UPDATE",
                        affected: Some(affected_row_count(&rows)),
                        rows: Vec::new(),
                        column_order: None,
                    }
                }
            }
            LogicalPlan::Delete { returning, .. } => {
                if returning.is_some() {
                    Self {
                        tag: "DELETE",
                        affected: Some(rows.len() as u64),
                        rows,
                        column_order,
                    }
                } else {
                    Self {
                        tag: "DELETE",
                        affected: Some(affected_row_count(&rows)),
                        rows: Vec::new(),
                        column_order: None,
                    }
                }
            }
            LogicalPlan::Truncate { .. } => Self {
                tag: "TRUNCATE TABLE",
                affected: Some(affected_row_count(&rows)),
                rows: Vec::new(),
                column_order: None,
            },
            LogicalPlan::Copy { .. } => Self {
                tag: "COPY",
                affected: Some(affected_row_count(&rows)),
                rows: Vec::new(),
                column_order: None,
            },
            LogicalPlan::Select { .. }
            | LogicalPlan::Join { .. }
            | LogicalPlan::DistributedJoin { .. }
            | LogicalPlan::Aggregate { .. }
            | LogicalPlan::DistributedAggregate { .. }
            | LogicalPlan::Sort { .. }
            | LogicalPlan::Limit { .. }
            | LogicalPlan::Project { .. }
            | LogicalPlan::Window { .. }
            | LogicalPlan::Union { .. }
            | LogicalPlan::Distinct { .. }
            | LogicalPlan::DistinctOn { .. }
            | LogicalPlan::Values { .. }
            | LogicalPlan::GenerateSeries { .. }
            | LogicalPlan::Unnest { .. }
            | LogicalPlan::JsonArrayElements { .. }
            | LogicalPlan::JsonEach { .. }
            | LogicalPlan::JsonObjectKeys { .. }
            | LogicalPlan::RegexpSplitToTable { .. }
            | LogicalPlan::RegexpMatches { .. }
            | LogicalPlan::Explain { .. }
            | LogicalPlan::Filter { .. }
            | LogicalPlan::SubqueryAlias { .. } => Self {
                tag: "SELECT",
                affected: None,
                rows,
                column_order,
            },
            LogicalPlan::CreateIndex { .. } => Self {
                tag: "CREATE INDEX",
                affected: None,
                rows: Vec::new(),
                column_order: None,
            },
            LogicalPlan::CreateTable { .. } | LogicalPlan::CreateTableAs { .. } => Self {
                tag: "CREATE TABLE",
                affected: None,
                rows: Vec::new(),
                column_order: None,
            },
            LogicalPlan::AlterTable { .. } => Self {
                tag: "ALTER TABLE",
                affected: None,
                rows: Vec::new(),
                column_order: None,
            },
            LogicalPlan::CreateRole { can_login, .. } => Self {
                tag: if *can_login { "CREATE USER" } else { "CREATE ROLE" },
                affected: None,
                rows: Vec::new(),
                column_order: None,
            },
            LogicalPlan::DropRole { .. } => Self {
                tag: "DROP ROLE",
                affected: None,
                rows: Vec::new(),
                column_order: None,
            },
            LogicalPlan::Grant { .. }
            | LogicalPlan::GrantSchema { .. }
            | LogicalPlan::GrantColumn { .. }
            | LogicalPlan::GrantRole { .. } => Self {
                tag: "GRANT",
                affected: None,
                rows: Vec::new(),
                column_order: None,
            },
            LogicalPlan::Revoke { .. }
            | LogicalPlan::RevokeSchema { .. }
            | LogicalPlan::RevokeColumn { .. } => Self {
                tag: "REVOKE",
                affected: None,
                rows: Vec::new(),
                column_order: None,
            },
            LogicalPlan::DropIndex { .. } => Self {
                tag: "DROP INDEX",
                affected: None,
                rows: Vec::new(),
                column_order: None,
            },
            LogicalPlan::DropTable { .. } => Self {
                tag: "DROP TABLE",
                affected: None,
                rows: Vec::new(),
                column_order: None,
            },
            LogicalPlan::Analyze { .. } => Self {
                tag: "ANALYZE",
                affected: None,
                rows: Vec::new(),
                column_order: None,
            },
            LogicalPlan::Vacuum { .. } => Self {
                tag: "VACUUM",
                affected: None,
                rows: Vec::new(),
                column_order: None,
            },
            LogicalPlan::Rebalance { .. } => Self {
                tag: "REBALANCE",
                affected: None,
                rows: Vec::new(),
                column_order: None,
            },
            LogicalPlan::Set { .. } => Self {
                tag: "SET",
                affected: None,
                rows: Vec::new(),
                column_order: None,
            },
            LogicalPlan::Show { .. } => Self {
                tag: "SELECT",
                affected: None,
                rows,
                column_order,
            },
            LogicalPlan::Comment { .. } => Self {
                tag: "COMMENT",
                affected: None,
                rows: Vec::new(),
                column_order: None,
            },
            LogicalPlan::Listen { .. } => Self {
                tag: "LISTEN",
                affected: None,
                rows: Vec::new(),
                column_order: None,
            },
            LogicalPlan::Unlisten { .. } => Self {
                tag: "UNLISTEN",
                affected: None,
                rows: Vec::new(),
                column_order: None,
            },
            LogicalPlan::Notify { .. } => Self {
                tag: "NOTIFY",
                affected: None,
                rows: Vec::new(),
                column_order: None,
            },
            LogicalPlan::CreateSequence { .. } => Self {
                tag: "CREATE SEQUENCE",
                affected: None,
                rows: Vec::new(),
                column_order: None,
            },
            LogicalPlan::DropSequence { .. } => Self {
                tag: "DROP SEQUENCE",
                affected: None,
                rows: Vec::new(),
                column_order: None,
            },
            LogicalPlan::AlterSequence { .. } => Self {
                tag: "ALTER SEQUENCE",
                affected: None,
                rows: Vec::new(),
                column_order: None,
            },
            LogicalPlan::Begin | LogicalPlan::Commit | LogicalPlan::Rollback => {
                unreachable!("txn control handled before data execution")
            }
        }
    }
}

fn projection_column_order(plan: &LogicalPlan) -> Option<Vec<String>> {
    match plan {
        LogicalPlan::Project { columns, .. } => {
            Some(columns.iter().map(|(n, _)| n.clone()).collect())
        }
        LogicalPlan::Insert {
            returning: Some(ret),
            ..
        }
        | LogicalPlan::Update {
            returning: Some(ret),
            ..
        }
        | LogicalPlan::Delete {
            returning: Some(ret),
            ..
        } => returning_column_order(ret),
        LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Filter { input, .. }
        | LogicalPlan::Window { input, .. }
        | LogicalPlan::SubqueryAlias { input, .. }
        | LogicalPlan::Distinct { input, .. }
        | LogicalPlan::Explain { plan: input } => projection_column_order(input),
        LogicalPlan::Union { left, .. } => projection_column_order(left),
        _ => None,
    }
}

fn returning_column_order(ret: &Returning) -> Option<Vec<String>> {
    match ret {
        Returning::Star => None,
        Returning::List(cols) => Some(cols.iter().map(|(n, _)| n.clone()).collect()),
    }
}

fn resolve_ctas_columns(
    engine: &TakyonicEngine,
    query: &LogicalPlan,
    explicit: &[String],
    rows: &[Record],
) -> crate::error::Result<Vec<String>> {
    if !explicit.is_empty() {
        return Ok(explicit.to_vec());
    }
    let mut names = ctas_output_columns(query)?;
    if names.is_empty() {
        if let Some(table) = ctas_source_table(query) {
            let schema = engine.table_schema(table)?;
            if !schema.columns.is_empty() {
                names = schema.columns.iter().map(|c| c.name.clone()).collect();
            } else {
                names = vec![schema.primary_key.clone()];
            }
        } else if let Some(row) = rows.first() {
            names = row.fields.keys().cloned().collect();
        }
    }
    if names.is_empty() {
        return Err(crate::error::TakyonicError::Sql(
            "CREATE TABLE AS SELECT could not determine output columns".into(),
        ));
    }
    Ok(names)
}

fn ctas_source_table(plan: &LogicalPlan) -> Option<&str> {
    match plan {
        LogicalPlan::Select { table, .. } => Some(table.as_str()),
        LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Filter { input, .. }
        | LogicalPlan::Distinct { input, .. }
        | LogicalPlan::DistinctOn { input, .. }
        | LogicalPlan::SubqueryAlias { input, .. }
        | LogicalPlan::Window { input, .. }
        | LogicalPlan::Project { input, .. } => ctas_source_table(input),
        _ => None,
    }
}

fn remap_ctas_row(
    row: &Record,
    source_names: &[String],
    dest_names: &[String],
) -> crate::error::Result<Record> {
    let mut out = Record::new();
    for (src, dst) in source_names.iter().zip(dest_names.iter()) {
        let val = row.get(src).unwrap_or("");
        out = out.set(dst.clone(), val);
    }
    Ok(out)
}

/// Row-description fields for Extended Query Describe (no execution).
fn describe_plan_fields(plan: &LogicalPlan, engine: &TakyonicEngine) -> Vec<FieldInfo> {
    describe_plan_columns(plan, engine)
        .into_iter()
        .map(|(name, ty)| FieldInfo::new(name, None, None, ty, FieldFormat::Text))
        .collect()
}

fn describe_plan_columns(plan: &LogicalPlan, engine: &TakyonicEngine) -> Vec<(String, Type)> {
    match plan {
        LogicalPlan::Project { columns, .. } => columns
            .iter()
            .map(|(name, expr)| (name.clone(), expr_pg_type(expr, engine)))
            .collect(),
        LogicalPlan::Select { table, .. } => table_star_columns(engine, table),
        LogicalPlan::Values { columns, .. } => columns
            .iter()
            .map(|c| (c.clone(), Type::VARCHAR))
            .collect(),
        LogicalPlan::GenerateSeries {
            column,
            ordinality_column,
            as_timestamp,
            ..
        } => {
            let ty = if *as_timestamp {
                Type::TIMESTAMP
            } else {
                Type::INT8
            };
            let mut cols = vec![(column.clone(), ty)];
            if let Some(o) = ordinality_column {
                cols.push((o.clone(), Type::INT8));
            }
            cols
        }
        LogicalPlan::Unnest {
            column,
            ordinality_column,
            ..
        } => {
            let mut cols = vec![(column.clone(), Type::VARCHAR)];
            if let Some(o) = ordinality_column {
                cols.push((o.clone(), Type::INT8));
            }
            cols
        }
        LogicalPlan::JsonArrayElements {
            column,
            ordinality_column,
            ..
        } => {
            let mut cols = vec![(column.clone(), Type::VARCHAR)];
            if let Some(o) = ordinality_column {
                cols.push((o.clone(), Type::INT8));
            }
            cols
        }
        LogicalPlan::JsonEach {
            key_column,
            value_column,
            ordinality_column,
            ..
        } => {
            let mut cols = vec![
                (key_column.clone(), Type::VARCHAR),
                (value_column.clone(), Type::VARCHAR),
            ];
            if let Some(o) = ordinality_column {
                cols.push((o.clone(), Type::INT8));
            }
            cols
        }
        LogicalPlan::JsonObjectKeys {
            column,
            ordinality_column,
            ..
        } => {
            let mut cols = vec![(column.clone(), Type::VARCHAR)];
            if let Some(o) = ordinality_column {
                cols.push((o.clone(), Type::INT8));
            }
            cols
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
            let mut cols = vec![(column.clone(), Type::VARCHAR)];
            if let Some(o) = ordinality_column {
                cols.push((o.clone(), Type::INT8));
            }
            cols
        }
        LogicalPlan::Join { left, right, .. }
        | LogicalPlan::DistributedJoin { left, right, .. } => {
            let mut cols = describe_plan_columns(left, engine);
            for (name, ty) in describe_plan_columns(right, engine) {
                if let Some((_, existing)) = cols.iter_mut().find(|(n, _)| n == &name) {
                    *existing = ty;
                } else {
                    cols.push((name, ty));
                }
            }
            cols
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
            let mut cols = Vec::new();
            for (i, g) in group_exprs.iter().enumerate() {
                let name = match g {
                    Expression::Column(c) => c.clone(),
                    _ => format!("group_{i}"),
                };
                cols.push((name, expr_pg_type(g, engine)));
            }
            for a in aggr_exprs {
                let name = crate::sql::aggregate_result_column(a).unwrap_or_else(|| "aggr".into());
                cols.push((name, expr_pg_type(a, engine)));
            }
            cols
        }
        LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Filter { input, .. }
        | LogicalPlan::Window { input, .. }
        | LogicalPlan::SubqueryAlias { input, .. }
        | LogicalPlan::Distinct { input, .. }
        | LogicalPlan::DistinctOn { input, .. }
        | LogicalPlan::Union { left: input, .. } => describe_plan_columns(input, engine),
        LogicalPlan::Explain { .. } => vec![("QUERY PLAN".into(), Type::VARCHAR)],
        LogicalPlan::Insert {
            table,
            returning: Some(ret),
            ..
        }
        | LogicalPlan::Update {
            table,
            returning: Some(ret),
            ..
        }
        | LogicalPlan::Delete {
            table,
            returning: Some(ret),
            ..
        } => describe_returning_columns(engine, table, ret),
        LogicalPlan::Insert { .. }
        | LogicalPlan::Update { .. }
        | LogicalPlan::Delete { .. }
        | LogicalPlan::Truncate { .. }
        | LogicalPlan::Copy { .. }
        | LogicalPlan::Begin
        | LogicalPlan::Commit
        | LogicalPlan::Rollback
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
        | LogicalPlan::Analyze { .. }
        | LogicalPlan::Vacuum { .. }
        | LogicalPlan::Rebalance { .. }
        | LogicalPlan::Set { .. }
        | LogicalPlan::Show { .. }
        | LogicalPlan::Comment { .. }
        | LogicalPlan::Listen { .. }
        | LogicalPlan::Unlisten { .. }
        | LogicalPlan::Notify { .. }
        | LogicalPlan::CreateSequence { .. }
        | LogicalPlan::DropSequence { .. }
        | LogicalPlan::AlterSequence { .. } => Vec::new(),
    }
}

fn describe_returning_columns(
    engine: &TakyonicEngine,
    table: &str,
    ret: &Returning,
) -> Vec<(String, Type)> {
    match ret {
        Returning::Star => table_star_columns(engine, table),
        Returning::List(cols) => cols
            .iter()
            .map(|(name, expr)| (name.clone(), expr_pg_type(expr, engine)))
            .collect(),
    }
}

fn table_star_columns(engine: &TakyonicEngine, table: &str) -> Vec<(String, Type)> {
    let Ok(schema) = engine.table_schema(table) else {
        return Vec::new();
    };
    if !schema.columns.is_empty() {
        return schema
            .columns
            .iter()
            .map(|c| (c.name.clone(), catalog_type_to_pg(&c.data_type)))
            .collect();
    }
    // Legacy API-registered tables: only PK is known at Describe time.
    vec![(schema.primary_key, Type::VARCHAR)]
}

fn lookup_column_pg_type(engine: &TakyonicEngine, name: &str) -> Type {
    for schema in engine.list_table_schemas() {
        if let Some(col) = schema.columns.iter().find(|c| c.name == name) {
            return catalog_type_to_pg(&col.data_type);
        }
    }
    Type::VARCHAR
}

fn expr_pg_type(expr: &Expression, engine: &TakyonicEngine) -> Type {
    match expr {
        Expression::Column(name) | Expression::OuterRef(name) => {
            lookup_column_pg_type(engine, name)
        }
        Expression::Literal(s) => match classify_value(s) {
            HeuristicKind::Int => Type::INT8,
            HeuristicKind::Bool => Type::BOOL,
            HeuristicKind::Float => Type::FLOAT8,
            HeuristicKind::Text => Type::VARCHAR,
        },
        Expression::Parameter(_) => Type::VARCHAR,
        Expression::AggregateFunction { name, args, .. } => match name.to_ascii_lowercase().as_str() {
            "count" => Type::INT8,
            "sum" | "avg" => Type::FLOAT8,
            "stddev" | "stddev_pop" | "stddev_samp" | "variance" | "var_pop" | "var_samp"
            | "corr" | "covar_pop" | "covar_samp"
            | "regr_slope" | "regr_intercept" | "regr_r2"
            | "regr_avgx" | "regr_avgy" | "regr_sxx" | "regr_syy" | "regr_sxy"
            | "percentile_cont" | "percentile_disc" => Type::FLOAT8,
            "regr_count" => Type::INT8,
            "bit_and" | "bit_or" => Type::INT8,
            "min" | "max" | "mode" if !args.is_empty() => expr_pg_type(&args[0], engine),
            "json_agg" | "jsonb_agg" | "json_object_agg" | "jsonb_object_agg"
            | "string_agg" | "array_agg" => Type::VARCHAR,
            "bool_and" | "bool_or" | "every" => Type::BOOL,
            _ => Type::VARCHAR,
        },
        Expression::Arith { .. } | Expression::VectorDistance { .. } => Type::FLOAT8,
        Expression::Array(_) => Type::VARCHAR,
        Expression::ArrayIndex { .. } => Type::VARCHAR,
        Expression::ScalarSubquery { value_column, .. } => {
            lookup_column_pg_type(engine, value_column)
        }
        Expression::BinaryOp { .. }
        | Expression::And { .. }
        | Expression::Or { .. }
        | Expression::InList { .. }
        | Expression::InSubquery { .. }
        | Expression::Exists { .. }
        | Expression::Like { .. }
        | Expression::SimilarTo { .. }
        | Expression::RegexMatch { .. }
        | Expression::AtTimeZone { .. }
        | Expression::IsNull { .. }
        | Expression::IsBoolTest { .. }
        | Expression::IsDistinctFrom { .. }
        | Expression::QuantifiedCmp { .. }
        | Expression::Not { .. } => Type::BOOL,
        Expression::Case {
            when_then,
            else_result,
        } => {
            if let Some((_, then)) = when_then.first() {
                return expr_pg_type(then, engine);
            }
            if let Some(e) = else_result {
                return expr_pg_type(e, engine);
            }
            Type::VARCHAR
        }
        Expression::Coalesce(args) => {
            if let Some(first) = args.first() {
                expr_pg_type(first, engine)
            } else {
                Type::VARCHAR
            }
        }
        Expression::Cast { target, .. } => match target {
            crate::sql::CastTarget::Text => Type::VARCHAR,
            crate::sql::CastTarget::Int => Type::INT8,
            crate::sql::CastTarget::Float => Type::FLOAT8,
            crate::sql::CastTarget::Bool => Type::BOOL,
            crate::sql::CastTarget::Json => Type::VARCHAR,
        },
        Expression::NullIf { left, .. } => expr_pg_type(left, engine),
        Expression::ScalarFunction { name, args } => match name.as_str() {
            "LENGTH" | "CHAR_LENGTH" | "CHARACTER_LENGTH" | "OCTET_LENGTH" | "BIT_LENGTH"
            | "STRPOS" | "POSITION" | "ASCII"
            | "WIDTH_BUCKET" | "DIV" | "EXTRACT" | "DATE_PART" | "ARRAY_LENGTH" | "CARDINALITY"
            | "JSON_ARRAY_LENGTH" | "JSONB_ARRAY_LENGTH" | "NUM_NONNULLS" | "NUM_NULLS" => {
                Type::INT8
            }
            "ABS" | "NEGATE" | "CEIL" | "CEILING" | "FLOOR" | "ROUND" | "TRUNC" | "SIGN" | "MOD"
            | "POWER" | "POW" | "SQRT" | "CBRT" | "LN" | "LOG" | "EXP" | "PI" | "SIN" | "COS"
            | "TAN" | "ASIN" | "ACOS" | "ATAN" | "ATAN2" | "RADIANS" | "DEGREES" | "TO_NUMBER"
            | "RANDOM" | "PG_NOTIFICATION_QUEUE_USAGE" => {
                Type::FLOAT8
            }
            "ARRAY_CONTAINS" | "ARRAY_CONTAINED_BY" | "ARRAY_OVERLAP" | "JSON_CONTAINS"
            | "JSON_CONTAINED_BY" | "IS_JSON" | "JSON_IS_VALID" | "JSONB_PATH_EXISTS"
            | "JSON_PATH_EXISTS" | "REGEXP_LIKE" | "STARTS_WITH" | "ENDS_WITH" | "ISFINITE"
            | "OVERLAPS" => {
                Type::BOOL
            }
            "JSON_GET_TEXT" | "JSON_TYPEOF" | "JSONB_TYPEOF" | "JSON_PATH_GET_TEXT"
            | "JSONB_EXTRACT_PATH_TEXT" | "JSON_EXTRACT_PATH_TEXT" => Type::VARCHAR,
            "JSON_GET" | "JSON_PATH_GET" | "JSON_CONCAT" | "JSONB_SET" | "JSON_SET"
            | "JSONB_EXTRACT_PATH" | "JSON_EXTRACT_PATH"
            | "JSONB_BUILD_OBJECT" | "JSON_BUILD_OBJECT" | "JSONB_BUILD_ARRAY"
            | "JSON_BUILD_ARRAY" | "JSONB_PRETTY" | "JSON_PRETTY" | "JSON_DELETE"
            | "JSON_PATH_DELETE" | "JSONB_INSERT" | "JSON_INSERT" | "JSONB_STRIP_NULLS"
            | "JSON_STRIP_NULLS" | "TO_JSON" | "TO_JSONB" | "ARRAY_TO_JSON" | "ROW_TO_JSON" => {
                Type::VARCHAR
            }
            "DATE_TRUNC" | "AGE" | "TO_CHAR" | "TO_TIMESTAMP" | "TO_DATE" | "MAKE_DATE" | "MAKE_TIME"
            | "MAKE_TIMESTAMP" | "MAKE_INTERVAL" | "TIMEZONE" | "DATE_BIN" | "JUSTIFY_HOURS"
            | "JUSTIFY_DAYS" | "JUSTIFY_INTERVAL" | "ARRAY_CAT" | "STRING_TO_ARRAY"
            | "ARRAY_TO_STRING" | "SPLIT_PART" | "REGEXP_SPLIT_TO_ARRAY" | "NOW"
            | "CURRENT_TIMESTAMP" | "CURRENT_DATE" | "CURRENT_TIME" | "LOCALTIMESTAMP"
            | "LOCALTIME" | "CLOCK_TIMESTAMP" | "TIMEOFDAY" | "STATEMENT_TIMESTAMP"
            | "TRANSACTION_TIMESTAMP" | "CURRENT_USER" | "SESSION_USER" | "USER"
            | "CURRENT_ROLE" | "CURRENT_SCHEMA" | "CURRENT_CATALOG" | "CURRENT_SCHEMAS"
            | "VERSION" | "GEN_RANDOM_UUID" | "PG_POSTMASTER_START_TIME"
            | "PG_CONF_LOAD_TIME"
            | "CURRENT_SETTING" => {
                Type::VARCHAR
            }
            "PG_BACKEND_PID" | "TXID_CURRENT" | "PG_CURRENT_XACT_ID" | "PG_COLUMN_SIZE"
            | "TO_REGCLASS" | "TO_REGROLE" | "TO_REGNAMESPACE" | "TO_REGTYPE"
            | "TO_REGPROC" | "TO_REGPROCEDURE" | "TO_REGOPER" | "TO_REGOPERATOR"
            | "TO_REGCOLLATION"
            | "PG_RELATION_SIZE" | "PG_TABLE_SIZE" | "PG_TOTAL_RELATION_SIZE"
            | "PG_INDEXES_SIZE" | "PG_DATABASE_SIZE" | "PG_RELATION_IS_UPDATABLE"
            | "PG_SNAPSHOT_XMIN" | "PG_SNAPSHOT_XMAX" | "PG_WAL_LSN_DIFF" | "PG_SIZE_BYTES"
            | "NEXTVAL" | "CURRVAL" | "LASTVAL" | "SETVAL" | "PG_SEQUENCE_LAST_VALUE" => {
                Type::INT8
            }
            "PG_IS_IN_RECOVERY" | "PG_TABLE_IS_VISIBLE" | "PG_TYPE_IS_VISIBLE"
            | "PG_FUNCTION_IS_VISIBLE" | "PG_COLUMN_IS_UPDATABLE" | "PG_OPERATOR_IS_VISIBLE"
            | "PG_COLLATION_IS_VISIBLE" | "PG_JIT_AVAILABLE" | "PG_TRY_ADVISORY_LOCK"
            | "PG_ADVISORY_UNLOCK" | "PG_TRY_ADVISORY_LOCK_SHARED"
            | "PG_ADVISORY_UNLOCK_SHARED" | "PG_TRY_ADVISORY_XACT_LOCK"
            | "PG_TRY_ADVISORY_XACT_LOCK_SHARED" | "PG_RELOAD_CONF"
            | "PG_ROTATE_LOGFILE" | "PG_VISIBLE_IN_SNAPSHOT" | "PG_CANCEL_BACKEND"
            | "PG_TERMINATE_BACKEND" | "PG_IS_WAL_REPLAY_PAUSED" | "PG_IS_IN_BACKUP"
            | "PG_PROMOTE" => {
                Type::BOOL
            }
            "PG_TYPEOF" | "GETDATABASEENCODING" | "PG_CLIENT_ENCODING" | "PG_SIZE_PRETTY"
            | "FORMAT_TYPE" | "PG_GET_USERBYID" | "PG_GET_INDEXDEF" | "PG_DESCRIBE_OBJECT"
            | "PG_IDENTIFY_OBJECT" | "CURRENT_QUERY" | "TXID_STATUS" | "PG_XACT_STATUS"
            | "PG_EXPORT_SNAPSHOT" | "PG_CURRENT_SNAPSHOT" | "TXID_CURRENT_SNAPSHOT"
            | "PG_CURRENT_WAL_LSN" | "PG_CURRENT_WAL_INSERT_LSN"
            | "PG_CURRENT_WAL_FLUSH_LSN" | "PG_WALFILE_NAME" | "PG_WALFILE_NAME_OFFSET"
            | "PG_SWITCH_WAL" | "PG_SWITCH_XLOG" | "PG_LAST_WAL_RECEIVE_LSN"
            | "PG_LAST_WAL_REPLAY_LSN" | "PG_LAST_XACT_REPLAY_TIMESTAMP"
            | "PG_BACKUP_START_TIME" | "PG_BACKUP_START" | "PG_START_BACKUP"
            | "PG_BACKUP_STOP" | "PG_STOP_BACKUP" | "PG_CREATE_RESTORE_POINT"
            | "PG_LISTENING_CHANNELS" | "PG_GET_SERIAL_SEQUENCE" => {
                Type::VARCHAR
            }
            "SETSEED" | "PG_SLEEP" | "PG_ADVISORY_LOCK" | "PG_ADVISORY_LOCK_SHARED"
            | "PG_ADVISORY_XACT_LOCK" | "PG_ADVISORY_XACT_LOCK_SHARED"
            | "PG_ADVISORY_UNLOCK_ALL" | "PG_NOTIFY" | "PG_WAL_REPLAY_PAUSE"
            | "PG_WAL_REPLAY_RESUME" => {
                Type::VARCHAR
            } // void; returned as NULL
            "GREATEST" | "LEAST" => {
                if let Some(first) = args.first() {
                    expr_pg_type(first, engine)
                } else {
                    Type::VARCHAR
                }
            }
            "LOWER" | "UPPER" | "TRIM" | "BTRIM" | "LTRIM" | "RTRIM" | "SUBSTRING" | "SUBSTR"
            | "CONCAT" | "CONCAT_WS" | "FORMAT" | "QUOTE_IDENT" | "QUOTE_LITERAL"
            | "QUOTE_NULLABLE" | "REPLACE"
            | "REGEXP_REPLACE" | "LPAD" | "RPAD" | "REPEAT" | "LEFT" | "RIGHT" | "REVERSE"
            | "INITCAP" | "CHR" | "MD5" | "ENCODE" | "DECODE" | "OVERLAY" | "TRANSLATE" => {
                Type::VARCHAR
            }
            _ if !args.is_empty() => expr_pg_type(&args[0], engine),
            _ => Type::VARCHAR,
        },
    }
}

/// Session transaction mode.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionTxnMode {
    /// No explicit transaction — each statement auto-commits.
    Idle,
    /// Inside `BEGIN` … `COMMIT`/`ROLLBACK`.
    InTransaction,
}

/// Per-session extended-query + transaction + authorization state.
///
/// Holds an optional active MVCC [`Transaction`] (owned via `Arc` engine) so
/// clients can group statements into one atomic snapshot-isolation unit.
pub struct SessionState {
    engine: Arc<TakyonicEngine>,
    /// Named prepared statements from `Parse`.
    pub prepared_statements: HashMap<String, LogicalPlan>,
    /// Named portals from `Bind`.
    pub portals: HashMap<String, BoundPlan>,
    /// Explicit transaction workspace when [`SessionTxnMode::InTransaction`].
    active_txn: Option<Transaction>,
    /// Authorization context after authentication (`current_user` + roles).
    auth: AuthContext,
    /// `search_path` GUC (comma-separated schema list).
    search_path: String,
    /// `transaction_isolation` GUC (`repeatable read` = SI+OCC; `serializable` = minimal SSI).
    transaction_isolation: String,
    /// `TimeZone` GUC (IANA name or fixed offset; default `UTC`).
    timezone: String,
    /// Prior session GUC values to restore when a `SET LOCAL` / `set_config(..., true)` txn ends.
    local_guc_restore: HashMap<String, String>,
    /// Local TCP address the server accepted on (`None` → Unix-socket / unset → NULL).
    inet_server_addr: Option<String>,
    inet_server_port: Option<i64>,
    /// Peer TCP address (`None` → Unix-socket / unset → NULL).
    inet_client_addr: Option<String>,
    inet_client_port: Option<i64>,
    /// Unique id for session-scoped advisory locks.
    session_id: u64,
    /// SQL text of the statement currently being executed (`current_query()`).
    current_query: Option<String>,
    /// Channels registered via `LISTEN` for this session.
    listening_channels: std::collections::BTreeSet<String>,
    /// Optional in-process 2PC participants (`shard_id` → participant).
    ///
    /// When set, multi-shard `COMMIT` uses [`TransactionCoordinator`] instead of
    /// local OCC `txn_batch`. Production nodes typically leave this empty and
    /// rely on `mpp_workers()` + `RemoteShard` (SocketAddr endpoints).
    dist_shards: Option<HashMap<ShardId, Arc<dyn ShardParticipant>>>,
    /// Active `COPY FROM STDIN` (table + columns) waiting for wire data.
    pending_copy_in: Option<(String, Vec<String>)>,
    /// Accumulated TSV bytes for [`Self::pending_copy_in`].
    copy_in_buffer: Vec<u8>,
}

impl SessionState {
    /// Bind this session to a shared engine as the bootstrap superuser.
    ///
    /// Unit / integration tests use this path so existing suites stay privileged.
    pub fn new(engine: Arc<TakyonicEngine>) -> Self {
        Self::as_user(engine, BOOTSTRAP_USER).expect("bootstrap user must exist")
    }

    /// Open a session authenticated as `user` (must exist in the AUTH catalog).
    pub fn as_user(engine: Arc<TakyonicEngine>, user: &str) -> crate::error::Result<Self> {
        let auth = engine.auth_catalog().read().auth_context(user)?;
        Ok(Self {
            engine,
            prepared_statements: HashMap::new(),
            portals: HashMap::new(),
            active_txn: None,
            auth,
            search_path: "public".into(),
            transaction_isolation: "repeatable read".into(),
            timezone: "UTC".into(),
            local_guc_restore: HashMap::new(),
            inet_server_addr: None,
            inet_server_port: None,
            inet_client_addr: None,
            inet_client_port: None,
            session_id: crate::sql::alloc_advisory_session_id(),
            current_query: None,
            listening_channels: std::collections::BTreeSet::new(),
            dist_shards: None,
            pending_copy_in: None,
            copy_in_buffer: Vec::new(),
        })
    }

    /// Record TCP endpoints for `inet_{server,client}_{addr,port}` (pgwire / tests).
    pub fn set_net_info(
        &mut self,
        server_addr: Option<String>,
        server_port: Option<i64>,
        client_addr: Option<String>,
        client_port: Option<i64>,
    ) {
        self.inet_server_addr = server_addr;
        self.inet_server_port = server_port;
        self.inet_client_addr = client_addr;
        self.inet_client_port = client_port;
    }

    /// Bulk-load TSV text as `COPY table FROM STDIN` (session / tests / ORM helpers).
    pub fn copy_from_tsv(
        &mut self,
        table: &str,
        columns: &[String],
        tsv: &str,
    ) -> crate::error::Result<u64> {
        copy_table_from_tsv(self, table, columns, tsv)
    }

    /// Dump table as TSV text (`COPY table TO STDOUT`).
    pub fn copy_to_tsv(
        &mut self,
        table: &str,
        columns: &[String],
    ) -> crate::error::Result<String> {
        copy_table_to_tsv(self, table, columns)
    }

    /// Append a `CopyData` chunk while `COPY FROM STDIN` is pending.
    pub fn append_copy_in_data(&mut self, data: &[u8]) -> crate::error::Result<()> {
        if self.pending_copy_in.is_none() {
            return Err(crate::error::TakyonicError::Sql(
                "no COPY FROM STDIN in progress".into(),
            ));
        }
        self.copy_in_buffer.extend_from_slice(data);
        Ok(())
    }

    /// Finish pending `COPY FROM STDIN` and apply buffered TSV rows.
    pub fn finish_copy_in(&mut self) -> crate::error::Result<u64> {
        let Some((table, columns)) = self.pending_copy_in.take() else {
            return Err(crate::error::TakyonicError::Sql(
                "no COPY FROM STDIN in progress".into(),
            ));
        };
        let text = String::from_utf8(std::mem::take(&mut self.copy_in_buffer)).map_err(|e| {
            crate::error::TakyonicError::Sql(format!("COPY FROM STDIN: invalid UTF-8: {e}"))
        })?;
        copy_table_from_tsv(self, &table, &columns, &text)
    }

    /// Abort a pending `COPY FROM STDIN` without applying rows.
    pub fn abort_copy_in(&mut self) {
        self.pending_copy_in = None;
        self.copy_in_buffer.clear();
    }

    /// Column count for the pending COPY IN (wire `CopyInResponse`).
    pub fn pending_copy_in_column_count(&self) -> Option<usize> {
        let (table, cols) = self.pending_copy_in.as_ref()?;
        if !cols.is_empty() {
            return Some(cols.len());
        }
        let schema = self.engine.table_schema(table).ok()?;
        Some(if schema.columns.is_empty() {
            1
        } else {
            schema.columns.len()
        })
    }

    /// Attach in-process 2PC shard participants for Session SQL multi-shard COMMIT.
    ///
    /// Call path: `COMMIT` → [`partition_txn_branches`] → [`TransactionCoordinator`]
    /// → [`ShardParticipant::prepare`] (Engine Raft `TxnPrepare`).
    pub fn attach_dist_shards(
        &mut self,
        shards: impl IntoIterator<Item = (ShardId, Arc<dyn ShardParticipant>)>,
    ) {
        let mut map = HashMap::new();
        for (id, shard) in shards {
            map.insert(id, shard);
        }
        self.dist_shards = if map.is_empty() { None } else { Some(map) };
    }

    /// Single-shard OCC commit, or multi-shard 2PC when participants cover the write-set.
    fn commit_active_txn(&self, txn: Transaction) -> crate::error::Result<()> {
        let default_shard = self.engine.shard_id().max(1);
        let workers = self.engine.mpp_workers();
        let router = PartitionRouter::new(workers.clone());
        let schema_of = |t: &str| self.engine.table_schema(t).ok();
        let branches = partition_txn_branches(
            txn.writes(),
            txn.reads(),
            &schema_of,
            &router,
            default_shard,
        )?;
        let shard_ids: Vec<ShardId> = branches.iter().map(|b| b.shard_id).collect();
        let multi = shard_ids.len() >= 2;

        if !multi || !self.have_dist_participants(&shard_ids, &workers) {
            txn.commit()?;
            return Ok(());
        }

        let participants = self.resolve_dist_participants(&shard_ids, &workers)?;
        let workspace = txn.into_dist_workspace();
        let req = DistTxnRequest {
            read_ts: workspace.read_ts,
            branches,
        };
        let tc = self.engine.txn_coordinator();
        // Clear prior shard registrations so this commit's participants win.
        for p in &participants {
            tc.register_shard(Arc::clone(p));
        }
        // Preserve the session SI snapshot (`workspace.read_ts`); do not let
        // `execute()` overwrite it with a fresh GlobalClock tick.
        let (txn_id, _) = tc.begin();
        match tc.commit(txn_id, req)? {
            DistTxnOutcome::Committed { .. } => {
                self.engine.apply_txn_stats_edits(&workspace.stats_edits);
                Ok(())
            }
            DistTxnOutcome::Aborted { reason, .. } => Err(crate::error::TakyonicError::Conflict(
                if reason.is_empty() {
                    "distributed transaction aborted".into()
                } else {
                    reason
                },
            )),
        }
    }

    fn have_dist_participants(
        &self,
        shard_ids: &[ShardId],
        workers: &[crate::mpp::WorkerEndpoint],
    ) -> bool {
        if let Some(map) = &self.dist_shards {
            return shard_ids.iter().all(|id| map.contains_key(id));
        }
        shard_ids.iter().all(|id| {
            workers.iter().any(|w| {
                w.node_id == *id && w.address.parse::<SocketAddr>().is_ok()
            })
        })
    }

    fn resolve_dist_participants(
        &self,
        shard_ids: &[ShardId],
        workers: &[crate::mpp::WorkerEndpoint],
    ) -> crate::error::Result<Vec<Arc<dyn ShardParticipant>>> {
        if let Some(map) = &self.dist_shards {
            let mut out = Vec::with_capacity(shard_ids.len());
            for id in shard_ids {
                let p = map.get(id).ok_or_else(|| {
                    crate::error::TakyonicError::Config(format!(
                        "missing dist shard participant `{id}`"
                    ))
                })?;
                out.push(Arc::clone(p));
            }
            return Ok(out);
        }

        // Production: connect RemoteShard on the same gRPC port as Raft/Twopc.
        let mut out = Vec::with_capacity(shard_ids.len());
        for id in shard_ids {
            let addr = workers
                .iter()
                .find(|w| w.node_id == *id)
                .map(|w| w.address.clone())
                .ok_or_else(|| {
                    crate::error::TakyonicError::Config(format!(
                        "no mpp worker endpoint for shard `{id}`"
                    ))
                })?;
            let sock: SocketAddr = addr.parse().map_err(|e| {
                crate::error::TakyonicError::Config(format!(
                    "bad twopc shard addr `{addr}`: {e}"
                ))
            })?;
            let remote = connect_remote_shard_blocking(*id, sock)?;
            out.push(remote as Arc<dyn ShardParticipant>);
        }
        Ok(out)
    }

    /// Current `search_path` setting.
    pub fn search_path(&self) -> &str {
        &self.search_path
    }

    /// Current `transaction_isolation` setting.
    pub fn transaction_isolation(&self) -> &str {
        &self.transaction_isolation
    }

    /// Current `TimeZone` setting.
    pub fn timezone(&self) -> &str {
        &self.timezone
    }

    /// Build an [`ExecutionContext`] from session GUCs + bind params.
    fn execution_context(&self, params: Vec<Value>) -> ExecutionContext {
        let mut ctx = ExecutionContext::for_session(
            params,
            self.auth.clone(),
            self.engine.auth_catalog(),
            self.search_path(),
            DEFAULT_DATABASE,
            self.transaction_isolation(),
            self.timezone(),
            self.active_txn.is_some(),
            self.inet_server_addr.clone(),
            self.inet_server_port,
            self.inet_client_addr.clone(),
            self.inet_client_port,
            Some(self.engine.comments()),
        );
        ctx.relation_catalog = Some(crate::oid::RelationCatalog::shared(
            &self.engine.list_table_schemas(),
        ));
        ctx.relation_sizes = Some(std::sync::Arc::new(self.engine.relation_size_catalog()));
        ctx.index_catalog = Some(crate::oid::IndexCatalog::shared(
            &self.engine.list_table_schemas(),
        ));
        ctx.session_id = self.session_id;
        ctx.current_query = self.current_query.clone();
        ctx.listening_channels = self.listening_channels.iter().cloned().collect();
        ctx
    }

    fn apply_guc(&mut self, name: &str, value: String) {
        match name {
            "search_path" => self.search_path = value,
            "transaction_isolation" => self.transaction_isolation = value,
            "timezone" | "time_zone" => self.timezone = value,
            _ => {}
        }
    }

    fn apply_guc_overlay(&mut self) {
        let (values, local) = crate::sql::take_guc_overlay();
        for (name, value) in values {
            if local.contains(&name) {
                let prior = match name.as_str() {
                    "search_path" => self.search_path.clone(),
                    "transaction_isolation" => self.transaction_isolation.clone(),
                    "timezone" | "time_zone" => self.timezone.clone(),
                    _ => continue,
                };
                self.local_guc_restore.entry(name.clone()).or_insert(prior);
            } else {
                self.local_guc_restore.remove(&name);
            }
            self.apply_guc(&name, value);
        }
    }

    fn restore_local_gucs(&mut self) {
        let restore = std::mem::take(&mut self.local_guc_restore);
        for (name, value) in restore {
            self.apply_guc(&name, value);
        }
    }

    /// Current SQL user name.
    pub fn current_user(&self) -> &str {
        &self.auth.user
    }

    /// Current authorization context.
    pub fn auth_context(&self) -> &AuthContext {
        &self.auth
    }

    /// Switch the session identity (after PgWire authentication).
    pub fn set_user(&mut self, user: &str) -> crate::error::Result<()> {
        self.auth = self.engine.auth_catalog().read().auth_context(user)?;
        Ok(())
    }

    /// Borrow the engine this session executes against.
    pub fn engine(&self) -> &Arc<TakyonicEngine> {
        &self.engine
    }

    /// Current transaction mode.
    pub fn txn_mode(&self) -> SessionTxnMode {
        if self.active_txn.is_some() {
            SessionTxnMode::InTransaction
        } else {
            SessionTxnMode::Idle
        }
    }

    /// `Parse`: SQL → [`LogicalPlan`], store under `name`.
    pub fn parse(&mut self, name: impl Into<String>, sql: &str) -> crate::error::Result<()> {
        let plan = SqlEngine::plan(sql)?;
        self.prepared_statements.insert(name.into(), plan);
        Ok(())
    }

    /// `Bind`: attach decoded parameters to a prepared statement → portal.
    pub fn bind(
        &mut self,
        portal_name: impl Into<String>,
        statement_name: &str,
        parameters: Vec<Value>,
    ) -> crate::error::Result<()> {
        let plan = self
            .prepared_statements
            .get(statement_name)
            .cloned()
            .ok_or_else(|| {
                crate::error::TakyonicError::Sql(format!(
                    "prepared statement `{statement_name}` not found"
                ))
            })?;
        self.portals.insert(
            portal_name.into(),
            BoundPlan { plan, parameters },
        );
        Ok(())
    }

    /// Run a single SQL string (parse → [`Self::run_plan`]).
    ///
    /// Catalog introspection (`pg_catalog` / `information_schema`, including
    /// psql `\dt`) is answered from the table catalog before planning.
    pub fn execute_sql(&mut self, sql: &str) -> crate::error::Result<SessionResult> {
        let tables = self.engine.list_table_schemas();
        if let Some(result) = crate::pg_catalog::try_handle(sql, &tables, self.auth.user.as_str())
        {
            return Ok(result);
        }
        let plan = SqlEngine::plan(sql)?;
        self.current_query = Some(sql.to_string());
        let result = self.run_plan(&plan, Vec::new());
        self.current_query = None;
        result
    }

    fn authorize(&self, plan: &LogicalPlan) -> crate::error::Result<()> {
        let catalog = self.engine.auth_catalog();
        AuthorizationManager::authorize(&catalog.read(), &self.auth, plan)
    }

    /// Execute a logical plan with optional bind parameters.
    ///
    /// * `BEGIN` / `COMMIT` / `ROLLBACK` manage [`Self::active_txn`].
    /// * DQL/DML in Idle mode auto-commit; in InTransaction mode they reuse
    ///   the open workspace and do **not** commit.
    pub fn run_plan(
        &mut self,
        plan: &LogicalPlan,
        params: Vec<Value>,
    ) -> crate::error::Result<SessionResult> {
        let result = self.run_plan_body(plan, params);
        // Transaction-scoped advisory locks release when not in an explicit txn
        // (COMMIT/ROLLBACK already cleared `active_txn`; auto-commit statements too).
        if self.active_txn.is_none() {
            let _ = crate::sql::pg_advisory_xact_unlock_all(self.session_id);
        }
        result
    }

    fn run_plan_body(
        &mut self,
        plan: &LogicalPlan,
        params: Vec<Value>,
    ) -> crate::error::Result<SessionResult> {
        self.authorize(plan)?;
        match plan {
            LogicalPlan::Begin => {
                if self.active_txn.is_some() {
                    return Err(crate::error::TakyonicError::Sql(
                        "there is already a transaction in progress".into(),
                    ));
                }
                let iso = crate::txn::IsolationLevel::from_guc(&self.transaction_isolation);
                self.active_txn = Some(self.engine.begin_with_isolation(iso)?);
                Ok(SessionResult::command("BEGIN"))
            }
            LogicalPlan::Commit => {
                let txn = self.active_txn.take().ok_or_else(|| {
                    crate::error::TakyonicError::Sql("there is no transaction in progress".into())
                })?;
                match self.commit_active_txn(txn) {
                    Ok(()) => {
                        self.restore_local_gucs();
                        Ok(SessionResult::command("COMMIT"))
                    }
                    Err(e) => {
                        self.restore_local_gucs();
                        Err(e)
                    }
                }
            }
            LogicalPlan::Rollback => {
                let txn = self.active_txn.take().ok_or_else(|| {
                    crate::error::TakyonicError::Sql("there is no transaction in progress".into())
                })?;
                txn.abort();
                self.restore_local_gucs();
                Ok(SessionResult::command("ROLLBACK"))
            }
            LogicalPlan::CreateIndex {
                name,
                table,
                column,
                if_not_exists,
                vector,
            } => {
                match vector {
                    Some(spec) => {
                        self.engine.create_vector_index(
                            name,
                            table,
                            column,
                            *if_not_exists,
                            spec.clone(),
                        )?;
                        Ok(SessionResult::command("CREATE VECTOR INDEX"))
                    }
                    None => {
                        self.engine
                            .create_index(name, table, column, *if_not_exists)?;
                        Ok(SessionResult::command("CREATE INDEX"))
                    }
                }
            }
            LogicalPlan::CreateTable {
                name,
                primary_key,
                columns,
                if_not_exists,
                serial_columns,
            } => {
                self.engine.create_table(
                    name,
                    primary_key,
                    columns.clone(),
                    *if_not_exists,
                )?;
                for col in serial_columns {
                    crate::sql::create_serial_sequence(name, col)?;
                }
                for col in columns {
                    if col.unique && col.name != *primary_key {
                        let idx = format!("uq_{name}_{}", col.name);
                        let _ = self.engine.create_index(&idx, name, &col.name, true);
                    }
                }
                Ok(SessionResult::command("CREATE TABLE"))
            }
            LogicalPlan::Copy {
                table,
                columns,
                to,
                target,
            } => {
                use crate::sql::CopyIoTarget;
                match (to, target) {
                    (true, CopyIoTarget::File(path)) => {
                        let n = copy_table_to_file(self, table, columns, path)?;
                        Ok(SessionResult {
                            tag: "COPY",
                            affected: Some(n),
                            rows: Vec::new(),
                            column_order: None,
                        })
                    }
                    (false, CopyIoTarget::File(path)) => {
                        let n = copy_table_from_file(self, table, columns, path)?;
                        Ok(SessionResult {
                            tag: "COPY",
                            affected: Some(n),
                            rows: Vec::new(),
                            column_order: None,
                        })
                    }
                    (false, CopyIoTarget::Stdin) => {
                        self.pending_copy_in = Some((table.clone(), columns.clone()));
                        self.copy_in_buffer.clear();
                        let ncols = if columns.is_empty() {
                            let schema = self.engine.table_schema(table)?;
                            if schema.columns.is_empty() {
                                1
                            } else {
                                schema.columns.len()
                            }
                        } else {
                            columns.len()
                        };
                        Ok(SessionResult {
                            tag: "COPY_IN",
                            affected: Some(ncols as u64),
                            rows: Vec::new(),
                            column_order: None,
                        })
                    }
                    (true, CopyIoTarget::Stdout) => {
                        let tsv = copy_table_to_tsv(self, table, columns)?;
                        Ok(SessionResult {
                            tag: "COPY_OUT",
                            affected: Some(tsv.lines().filter(|l| !l.is_empty()).count() as u64),
                            rows: vec![Record::new().set("__copy_tsv", tsv)],
                            column_order: Some(vec!["__copy_tsv".into()]),
                        })
                    }
                    (true, CopyIoTarget::Stdin) | (false, CopyIoTarget::Stdout) => {
                        Err(crate::error::TakyonicError::Sql(
                            "invalid COPY direction for STDIN/STDOUT".into(),
                        ))
                    }
                }
            }
            LogicalPlan::CreateTableAs {
                name,
                query,
                columns,
                if_not_exists,
            } => {
                if *if_not_exists && self.engine.table_schema(name).is_ok() {
                    return Ok(SessionResult::command("CREATE TABLE"));
                }
                let ctx = self.execution_context(Vec::new());
                let rows = if let Some(txn) = self.active_txn.as_mut() {
                    executor::execute_plan(query, &ctx, txn)?
                } else {
                    executor::execute_plan_autocommit(query, &ctx, self.begin_txn()?)?
                };
                let col_names = resolve_ctas_columns(self.engine.as_ref(), query, columns, &rows)?;
                let source_order = if columns.is_empty() {
                    col_names.clone()
                } else {
                    let mut src = ctas_output_columns(query)?;
                    if src.is_empty() {
                        src = resolve_ctas_columns(self.engine.as_ref(), query, &[], &rows)?;
                    }
                    if src.len() != col_names.len() {
                        return Err(crate::error::TakyonicError::Sql(format!(
                            "CREATE TABLE AS column count mismatch: {} AS names for {} query columns",
                            col_names.len(),
                            src.len()
                        )));
                    }
                    src
                };
                let primary_key = col_names[0].clone();
                let specs: Vec<_> = col_names
                    .iter()
                    .map(|c| ColumnSpec::new(c.clone(), "TEXT"))
                    .collect();
                self.engine
                    .create_table(name, &primary_key, specs, false)?;
                if !rows.is_empty() {
                    let mut txn = self.begin_txn()?;
                    for row in rows {
                        let record = remap_ctas_row(&row, &source_order, &col_names)?;
                        txn.put_record(name, record)?;
                    }
                    txn.commit()?;
                }
                Ok(SessionResult::command("CREATE TABLE"))
            }
            LogicalPlan::AlterTable { name, operations } => {
                self.engine.alter_table(name, operations)?;
                for op in operations {
                    if let crate::sql::AlterTableOp::AddColumn {
                        column,
                        is_serial: true,
                        ..
                    } = op
                    {
                        crate::sql::create_serial_sequence(name, &column.name)?;
                    }
                }
                Ok(SessionResult::command("ALTER TABLE"))
            }
            LogicalPlan::CreateRole {
                name,
                can_login,
                is_superuser,
                password,
                if_not_exists,
            } => {
                self.engine.create_role(
                    name,
                    *can_login,
                    *is_superuser,
                    password.as_deref(),
                    *if_not_exists,
                )?;
                let tag = if *can_login {
                    "CREATE USER"
                } else {
                    "CREATE ROLE"
                };
                Ok(SessionResult::command(tag))
            }
            LogicalPlan::DropRole { name, if_exists } => {
                self.engine.drop_role(name, *if_exists)?;
                Ok(SessionResult::command("DROP ROLE"))
            }
            LogicalPlan::Grant {
                privileges,
                table,
                grantee,
            } => {
                self.engine
                    .grant_privilege(grantee, table, privileges)?;
                Ok(SessionResult::command("GRANT"))
            }
            LogicalPlan::Revoke {
                privileges,
                table,
                grantee,
            } => {
                self.engine
                    .revoke_privilege(grantee, table, privileges)?;
                Ok(SessionResult::command("REVOKE"))
            }
            LogicalPlan::GrantSchema {
                privileges,
                schema,
                grantee,
            } => {
                self.engine
                    .grant_schema_privilege(grantee, schema, privileges)?;
                Ok(SessionResult::command("GRANT"))
            }
            LogicalPlan::RevokeSchema {
                privileges,
                schema,
                grantee,
            } => {
                self.engine
                    .revoke_schema_privilege(grantee, schema, privileges)?;
                Ok(SessionResult::command("REVOKE"))
            }
            LogicalPlan::GrantColumn {
                specs,
                table,
                grantee,
            } => {
                self.engine
                    .grant_column_privilege(grantee, table, specs)?;
                Ok(SessionResult::command("GRANT"))
            }
            LogicalPlan::RevokeColumn {
                specs,
                table,
                grantee,
            } => {
                self.engine
                    .revoke_column_privilege(grantee, table, specs)?;
                Ok(SessionResult::command("REVOKE"))
            }
            LogicalPlan::GrantRole { role, member } => {
                self.engine.grant_role_membership(role, member)?;
                Ok(SessionResult::command("GRANT"))
            }
            LogicalPlan::DropIndex { name, if_exists } => {
                self.engine.drop_index(name, *if_exists)?;
                Ok(SessionResult::command("DROP INDEX"))
            }
            LogicalPlan::DropTable { name, if_exists } => {
                self.engine.drop_table(name, *if_exists)?;
                Ok(SessionResult::command("DROP TABLE"))
            }
            LogicalPlan::Explain { plan } => {
                let rewritten = self.mpp_rewrite(plan.as_ref());
                let text = match peel_mpp_explain(&rewritten) {
                    Some(text) => text,
                    None => {
                        let physical = executor::optimize_with_catalog(
                            &rewritten,
                            &|t| self.engine.table_schema(t).ok(),
                            &|t| Some(self.engine.table_stats(t)),
                        )?;
                        executor::explain_physical(&physical)
                    }
                };
                Ok(SessionResult {
                    tag: "SELECT",
                    rows: vec![Record::new().set("QUERY PLAN", text)],
                    affected: None,
                    column_order: Some(vec!["QUERY PLAN".into()]),
                })
            }
            LogicalPlan::Vacuum { table } => {
                // Run without an open snapshot so the watermark can advance and
                // dead versions become reclaimable (SI readers still pin epochs).
                let stats = self.engine.vacuum_table(table)?;
                Ok(SessionResult {
                    tag: "VACUUM",
                    rows: vec![Record::new()
                        .set("table", stats.table)
                        .set("watermark", stats.watermark.to_string())
                        .set("removed", stats.memtable_removed.to_string())
                        .set("versions_before", stats.versions_before.to_string())
                        .set("versions_after", stats.versions_after.to_string())
                        .set("dead_heap", stats.dead_heap_versions.to_string())
                        .set("dead_index", stats.dead_index_versions.to_string())],
                    affected: None,
                    column_order: None,
                })
            }
            LogicalPlan::Rebalance { table } => {
                let mv = self.engine.rebalance_table(table)?;
                let row = match mv {
                    Some(m) => Record::new()
                        .set("table", table.as_str())
                        .set("moved", "1")
                        .set("partition_id", m.partition_id.to_string())
                        .set("from_node", m.from_node.to_string())
                        .set("to_node", m.to_node.to_string()),
                    None => Record::new()
                        .set("table", table.as_str())
                        .set("moved", "0")
                        .set("partition_id", "")
                        .set("from_node", "")
                        .set("to_node", ""),
                };
                Ok(SessionResult {
                    tag: "REBALANCE",
                    rows: vec![row],
                    affected: None,
                    column_order: Some(vec![
                        "table".into(),
                        "moved".into(),
                        "partition_id".into(),
                        "from_node".into(),
                        "to_node".into(),
                    ]),
                })
            }
            LogicalPlan::Set { name, value } => {
                match name.as_str() {
                    "search_path" => self.search_path = value.clone(),
                    "transaction_isolation" => self.transaction_isolation = value.clone(),
                    "timezone" | "time_zone" => self.timezone = value.clone(),
                    other => {
                        return Err(crate::error::TakyonicError::Sql(format!(
                            "unsupported SET variable `{other}`"
                        )));
                    }
                }
                Ok(SessionResult::command("SET"))
            }
            LogicalPlan::Show { name } => {
                let value = match name.as_str() {
                    "search_path" => self.search_path.as_str(),
                    "transaction_isolation" => self.transaction_isolation.as_str(),
                    "timezone" | "time_zone" => self.timezone.as_str(),
                    other => {
                        return Err(crate::error::TakyonicError::Sql(format!(
                            "unrecognized configuration parameter \"{other}\""
                        )));
                    }
                };
                Ok(SessionResult {
                    tag: "SELECT",
                    rows: vec![Record::new().set(name.clone(), value)],
                    affected: None,
                    column_order: Some(vec![name.clone()]),
                })
            }
            LogicalPlan::Comment {
                object_type,
                table,
                column,
                comment,
            } => {
                match object_type.as_str() {
                    "table" => self.engine.set_table_comment(table, comment.as_deref())?,
                    "column" => {
                        let col = column.as_deref().ok_or_else(|| {
                            crate::error::TakyonicError::Sql(
                                "COMMENT ON COLUMN requires a column name".into(),
                            )
                        })?;
                        self.engine
                            .set_column_comment(table, col, comment.as_deref())?;
                    }
                    "role" => self.engine.set_shared_comment("role", table, comment.as_deref())?,
                    "database" => {
                        self.engine
                            .set_shared_comment("database", table, comment.as_deref())?
                    }
                    other => {
                        return Err(crate::error::TakyonicError::Sql(format!(
                            "unsupported COMMENT ON {other}"
                        )));
                    }
                }
                Ok(SessionResult::command("COMMENT"))
            }
            LogicalPlan::Listen { channel } => {
                self.listening_channels.insert(channel.clone());
                crate::sql::register_listen(self.session_id, channel);
                Ok(SessionResult::command("LISTEN"))
            }
            LogicalPlan::Unlisten { channel } => {
                match channel {
                    Some(ch) => {
                        self.listening_channels.remove(ch);
                        crate::sql::register_unlisten(self.session_id, Some(ch.as_str()));
                    }
                    None => {
                        self.listening_channels.clear();
                        crate::sql::register_unlisten(self.session_id, None);
                    }
                }
                Ok(SessionResult::command("UNLISTEN"))
            }
            LogicalPlan::Notify { channel, payload } => {
                crate::sql::pg_notify(channel, payload)?;
                Ok(SessionResult::command("NOTIFY"))
            }
            LogicalPlan::CreateSequence {
                name,
                if_not_exists,
                start,
                increment,
            } => {
                crate::sql::create_sequence(name, *if_not_exists, *start, *increment)?;
                Ok(SessionResult::command("CREATE SEQUENCE"))
            }
            LogicalPlan::DropSequence { name, if_exists } => {
                crate::sql::drop_sequence(name, *if_exists)?;
                Ok(SessionResult::command("DROP SEQUENCE"))
            }
            LogicalPlan::AlterSequence {
                name,
                restart,
                increment,
                owned_by,
                rename_to,
            } => {
                crate::sql::alter_sequence(
                    name,
                    *restart,
                    *increment,
                    owned_by.clone(),
                    rename_to.as_deref(),
                )?;
                Ok(SessionResult::command("ALTER SEQUENCE"))
            }
            other => {
                crate::sql::clear_guc_overlay();
                if let Some(rows) = self.try_mpp_execute(other, params.clone())? {
                    self.apply_guc_overlay();
                    return Ok(SessionResult::from_data_plan(other, rows));
                }
                let ctx = self.execution_context(params);
                let rows = if let Some(txn) = self.active_txn.as_mut() {
                    // Explicit transaction: mutate/read workspace, do not commit.
                    executor::execute_plan(other, &ctx, txn)?
                } else {
                    // Auto-commit: fresh txn, commit DML / abort after SELECT.
                    executor::execute_plan_autocommit(other, &ctx, self.begin_txn()?)?
                };
                self.apply_guc_overlay();
                Ok(SessionResult::from_data_plan(other, rows))
            }
        }
    }

    /// Begin a transaction honoring the current `transaction_isolation` GUC.
    fn begin_txn(&self) -> crate::error::Result<crate::txn::Transaction> {
        let iso = crate::txn::IsolationLevel::from_guc(&self.transaction_isolation);
        self.engine.begin_with_isolation(iso)
    }

    /// When `mpp_enabled`, lift Aggregates/Joins into Distributed* forms.
    fn mpp_rewrite(&self, plan: &LogicalPlan) -> LogicalPlan {
        if !self.engine.config().mpp_enabled {
            return plan.clone();
        }
        let n = self.engine.mpp_worker_count();
        crate::mpp::maybe_distribute(plan.clone(), n)
    }

    /// Execute DistributedAggregate / pruned DistributedScan / DistributedJoin /
    /// partitioned INSERT via [`crate::mpp::Coordinator`].
    fn try_mpp_execute(
        &self,
        plan: &LogicalPlan,
        params: Vec<Value>,
    ) -> crate::error::Result<Option<Vec<Record>>> {
        if !self.engine.config().mpp_enabled {
            return Ok(None);
        }
        let rewritten = self.mpp_rewrite(plan);
        match &rewritten {
            LogicalPlan::Project { columns, input } => {
                let Some(rows) = self.try_mpp_execute(input, params)? else {
                    return Ok(None);
                };
                Ok(Some(project_mpp_rows(rows, columns)?))
            }
            LogicalPlan::Filter { input, predicate } => {
                let Some(rows) = self.try_mpp_execute(input, params)? else {
                    return Ok(None);
                };
                let ctx = self.execution_context(Vec::new());
                let mut out = Vec::new();
                for row in rows {
                    if executor::evaluate_bool(predicate, &row, &ctx)? {
                        out.push(row);
                    }
                }
                Ok(Some(out))
            }
            LogicalPlan::DistributedAggregate {
                input,
                group_exprs,
                aggr_exprs,
            } => {
                let Ok((table, group_col, agg)) =
                    dist_agg_params(input, group_exprs, aggr_exprs)
                else {
                    // Unsupported shape → local Volcano (EXPLAIN stays local via
                    // [`maybe_distribute`] filter; this is a safety net).
                    return Ok(None);
                };
                let coord = self.engine.mpp_coordinator()?;
                let rows =
                    coord.execute_distributed_aggregate(&table, &group_col, agg)?;
                Ok(Some(rows))
            }
            LogicalPlan::DistributedJoin {
                left,
                right,
                on,
                ..
            } => {
                let (left_table, left_pred) = mpp_scan_source(left)?;
                let (right_table, right_pred) = mpp_scan_source(right)?;
                let coord = self.engine.mpp_coordinator()?;
                let rows = coord.execute_distributed_join(
                    &left_table,
                    &right_table,
                    on,
                    left_pred.as_ref(),
                    right_pred.as_ref(),
                )?;
                Ok(Some(rows))
            }
            LogicalPlan::Select {
                table,
                predicate,
                ..
            } => {
                // Partitioned tables: RemoteWorker fetch via Coordinator (C2).
                let schema = match self.engine.table_schema(table) {
                    Ok(s) => s,
                    Err(_) => return Ok(None),
                };
                if matches!(
                    schema.partitioning,
                    crate::partition::PartitioningStrategy::None
                ) {
                    return Ok(None);
                }
                let coord = self.engine.mpp_coordinator()?;
                let rows =
                    coord.execute_distributed_scan(table, predicate.as_ref())?;
                Ok(Some(rows))
            }
            LogicalPlan::Insert {
                table,
                columns,
                values,
                query,
                returning: _,
                ..
            } => {
                // Explicit txn keeps local InsertExec so writes stay in the
                // session snapshot; auto-commit partitioned INSERT uses ownership
                // routing (C4) — one worker per row, no broadcast.
                // INSERT…SELECT stays on the local volcano path.
                if self.active_txn.is_some() || query.is_some() {
                    return Ok(None);
                }
                let schema = match self.engine.table_schema(table) {
                    Ok(s) => s,
                    Err(_) => return Ok(None),
                };
                if matches!(
                    schema.partitioning,
                    crate::partition::PartitioningStrategy::None
                ) {
                    return Ok(None);
                }
                let ctx = self.execution_context(params);
                let records =
                    executor::materialize_insert_records(columns, values, &ctx)?;
                let coord = self.engine.mpp_coordinator()?;
                let n = coord.execute_insert_rows(table, records)?;
                Ok(Some(vec![Record::new().set("rows", n.to_string())]))
            }
            _ => Ok(None),
        }
    }

    /// `Execute` a named portal (extended query).
    pub fn execute(&mut self, portal_name: &str) -> crate::error::Result<SessionResult> {
        let portal = self.portals.get(portal_name).cloned().ok_or_else(|| {
            crate::error::TakyonicError::Sql(format!("portal `{portal_name}` not found"))
        })?;
        self.run_plan(&portal.plan, portal.parameters)
    }

    /// `Execute` over an in-memory row set (unit tests / stub TableScan).
    ///
    /// Ignores the session transaction — used only for Values-based filters.
    pub fn execute_with_rows(
        &mut self,
        portal_name: &str,
        rows: Vec<Record>,
    ) -> crate::error::Result<Vec<Record>> {
        let portal = self.portals.get(portal_name).cloned().ok_or_else(|| {
            crate::error::TakyonicError::Sql(format!("portal `{portal_name}` not found"))
        })?;
        let ctx = self.execution_context(portal.parameters.clone());
        execute_bound_plan(&portal.plan, &ctx, rows)
    }

    /// `Sync`: drop unnamed prepared state and portals (keeps explicit txn open).
    pub fn sync(&mut self) {
        self.portals.clear();
        self.prepared_statements.remove("");
        self.prepared_statements
            .remove(pgwire::api::DEFAULT_NAME);
    }
}

/// EXPLAIN text for MPP-rewritten plans (peels Project/Filter wrappers).
fn peel_mpp_explain(plan: &LogicalPlan) -> Option<String> {
    match plan {
        LogicalPlan::DistributedAggregate {
            group_exprs,
            aggr_exprs,
            ..
        } => Some(format!(
            "DistributedAggregate(groups={}, aggs={})",
            group_exprs.len(),
            aggr_exprs.len(),
        )),
        LogicalPlan::DistributedJoin { distribution, .. } => Some(format!(
            "DistributedJoin(distribution={distribution:?})"
        )),
        LogicalPlan::Project { input, .. }
        | LogicalPlan::Filter { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. } => peel_mpp_explain(input),
        _ => None,
    }
}

/// Project MPP result rows through a SELECT-list (name, expr) pairs.
fn project_mpp_rows(
    rows: Vec<Record>,
    columns: &[(String, Expression)],
) -> crate::error::Result<Vec<Record>> {
    let ctx = executor::ExecutionContext::new();
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let mut projected = Record::new();
        for (name, expr) in columns {
            let val = executor::evaluate(expr, &row, &ctx)?;
            projected = projected.set(name, executor::value_to_field(&val));
        }
        out.push(projected);
    }
    Ok(out)
}

/// Extract `(table, group_column, DistAggKind)` for MPP distributed aggregate.
fn dist_agg_params(
    input: &LogicalPlan,
    group_exprs: &[Expression],
    aggr_exprs: &[Expression],
) -> crate::error::Result<(String, String, crate::mpp::DistAggKind)> {
    let table = match input {
        LogicalPlan::Select { table, .. } => table.clone(),
        LogicalPlan::Project { input, .. } | LogicalPlan::Filter { input, .. } => {
            match input.as_ref() {
                LogicalPlan::Select { table, .. } => table.clone(),
                other => {
                    return Err(crate::error::TakyonicError::Sql(format!(
                        "DistributedAggregate input must be a table scan, got {other:?}"
                    )));
                }
            }
        }
        other => {
            return Err(crate::error::TakyonicError::Sql(format!(
                "DistributedAggregate input must be a table scan, got {other:?}"
            )));
        }
    };
    let (group_col, agg) = crate::mpp::extract_simple_agg(group_exprs, aggr_exprs)
        .ok_or_else(|| {
            crate::error::TakyonicError::Sql(
                "DistributedAggregate requires single GROUP BY col + SUM|COUNT|MIN|MAX|AVG"
                    .into(),
            )
        })?;
    Ok((table, group_col, agg))
}

/// Table + optional predicate for an MPP scan leaf (unwrap Project/Filter).
fn mpp_scan_source(
    plan: &LogicalPlan,
) -> crate::error::Result<(String, Option<Expression>)> {
    match plan {
        LogicalPlan::Select {
            table, predicate, ..
        } => Ok((table.clone(), predicate.clone())),
        LogicalPlan::Filter { input, predicate } => {
            let (table, inner) = mpp_scan_source(input)?;
            let pred = match (inner, Some(predicate.clone())) {
                (Some(a), Some(b)) => Some(Expression::And {
                    left: Box::new(a),
                    right: Box::new(b),
                }),
                (a, b) => a.or(b),
            };
            Ok((table, pred))
        }
        LogicalPlan::Project { input, .. } => mpp_scan_source(input),
        other => Err(crate::error::TakyonicError::Sql(format!(
            "DistributedJoin side must resolve to a table scan, got {other:?}"
        ))),
    }
}

/// Execute a bound logical plan via the Volcano optimizer + [`ExecutionContext`].
fn execute_bound_plan(
    plan: &LogicalPlan,
    ctx: &ExecutionContext,
    rows: Vec<Record>,
) -> crate::error::Result<Vec<Record>> {
    match plan {
        LogicalPlan::Select { .. } if !rows.is_empty() || plan_has_predicate(plan) => {
            let physical = if rows.is_empty() {
                let _ = executor::optimize(plan)?;
                return Ok(Vec::new());
            } else {
                executor::optimize_with_values(plan, rows)?
            };
            executor::collect_rows(executor::open_executor(physical, ctx)?.as_mut())
        }
        LogicalPlan::Select { .. } => Ok(Vec::new()),
        LogicalPlan::Join { .. }
        | LogicalPlan::DistributedJoin { .. }
        | LogicalPlan::Aggregate { .. }
        | LogicalPlan::DistributedAggregate { .. }
        | LogicalPlan::Sort { .. }
        | LogicalPlan::Limit { .. }
        | LogicalPlan::Project { .. }
        | LogicalPlan::Window { .. }
        | LogicalPlan::Union { .. }
        | LogicalPlan::Distinct { .. }
        | LogicalPlan::DistinctOn { .. }
        | LogicalPlan::Values { .. }
        | LogicalPlan::GenerateSeries { .. }
        | LogicalPlan::Unnest { .. }
        | LogicalPlan::JsonArrayElements { .. }
        | LogicalPlan::JsonEach { .. }
        | LogicalPlan::JsonObjectKeys { .. }
        | LogicalPlan::RegexpSplitToTable { .. }
        | LogicalPlan::RegexpMatches { .. }
        | LogicalPlan::Filter { .. }
        | LogicalPlan::SubqueryAlias { .. } => {
            let _physical = executor::optimize(plan)?;
            Ok(Vec::new())
        }
        LogicalPlan::Insert { .. }
        | LogicalPlan::Update { .. }
        | LogicalPlan::Delete { .. }
        | LogicalPlan::Truncate { .. }
        | LogicalPlan::Copy { .. }
        | LogicalPlan::Begin
        | LogicalPlan::Commit
        | LogicalPlan::Rollback
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
        | LogicalPlan::Explain { .. }
        | LogicalPlan::Analyze { .. }
        | LogicalPlan::Vacuum { .. }
        | LogicalPlan::Rebalance { .. }
        | LogicalPlan::Set { .. }
        | LogicalPlan::Show { .. }
        | LogicalPlan::Comment { .. }
        | LogicalPlan::Listen { .. }
        | LogicalPlan::Unlisten { .. }
        | LogicalPlan::Notify { .. }
        | LogicalPlan::CreateSequence { .. }
        | LogicalPlan::DropSequence { .. }
        | LogicalPlan::AlterSequence { .. } => {
            let _ = plan;
            Ok(Vec::new())
        }
    }
}

fn plan_has_predicate(plan: &LogicalPlan) -> bool {
    matches!(
        plan,
        LogicalPlan::Select {
            predicate: Some(_),
            ..
        }
    )
}

/// Decode pgwire bind parameter bytes into engine [`Value`]s.
///
/// Text format is UTF-8; type OID (when known) guides Int/Bool/String casting.
/// Unknown types infer Int when the text parses as `i64`, else String.
pub fn decode_bind_parameters(
    raw: &[Option<bytes::Bytes>],
    param_types: &[Option<Type>],
    format: &pgwire::api::portal::Format,
) -> crate::error::Result<Vec<Value>> {
    let mut out = Vec::with_capacity(raw.len());
    for (i, slot) in raw.iter().enumerate() {
        match slot {
            None => out.push(Value::Null),
            Some(buf) => {
                let ty = param_types.get(i).and_then(|t| t.as_ref());
                let field_fmt = format.format_for(i);
                out.push(decode_one_param(buf, ty, field_fmt)?);
            }
        }
    }
    Ok(out)
}

fn decode_one_param(
    buf: &bytes::Bytes,
    ty: Option<&Type>,
    format: pgwire::api::results::FieldFormat,
) -> crate::error::Result<Value> {
    // Binary INT8/INT4/INT2: big-endian. Everything else treated as text for now.
    if format == pgwire::api::results::FieldFormat::Binary {
        if let Some(t) = ty {
            if *t == Type::INT8 && buf.len() == 8 {
                let mut arr = [0u8; 8];
                arr.copy_from_slice(buf);
                return Ok(Value::Int(i64::from_be_bytes(arr)));
            }
            if *t == Type::INT4 && buf.len() == 4 {
                let mut arr = [0u8; 4];
                arr.copy_from_slice(buf);
                return Ok(Value::Int(i32::from_be_bytes(arr) as i64));
            }
            if *t == Type::INT2 && buf.len() == 2 {
                let mut arr = [0u8; 2];
                arr.copy_from_slice(buf);
                return Ok(Value::Int(i16::from_be_bytes(arr) as i64));
            }
            if *t == Type::BOOL && buf.len() == 1 {
                return Ok(Value::Bool(buf[0] != 0));
            }
        }
    }

    let s = std::str::from_utf8(buf).map_err(|e| {
        crate::error::TakyonicError::Sql(format!("parameter is not valid UTF-8: {e}"))
    })?;

    if let Some(t) = ty {
        if *t == Type::INT2 || *t == Type::INT4 || *t == Type::INT8 {
            let n: i64 = s.parse().map_err(|e| {
                crate::error::TakyonicError::Sql(format!("invalid integer parameter `{s}`: {e}"))
            })?;
            return Ok(Value::Int(n));
        }
        if *t == Type::BOOL {
            return Ok(match s.to_ascii_lowercase().as_str() {
                "t" | "true" | "1" | "yes" | "on" => Value::Bool(true),
                "f" | "false" | "0" | "no" | "off" => Value::Bool(false),
                other => {
                    return Err(crate::error::TakyonicError::Sql(format!(
                        "invalid boolean parameter `{other}`"
                    )));
                }
            });
        }
        if *t == Type::TEXT || *t == Type::VARCHAR || *t == Type::BPCHAR {
            return Ok(Value::String(s.to_string()));
        }
    }

    Ok(Value::from_text(s))
}

fn bind_param_format(codes: &[i16]) -> pgwire::api::portal::Format {
    use pgwire::api::portal::Format;
    if codes.is_empty() {
        Format::UnifiedText
    } else if codes.len() == 1 {
        Format::from(codes[0])
    } else {
        Format::Individual(codes.to_vec())
    }
}

/// Stages of the PgWire SCRAM-SHA-256 (SASL) handshake.
///
/// Maps onto pgwire's internal [`pgwire::api::auth::sasl::SASLState`] owned by
/// each per-connection [`SASLAuthStartupHandler`]:
/// - [`AuthStage::Initial`] → await `StartupMessage`, then offer `SCRAM-SHA-256`
/// - [`AuthStage::AwaitingSaslInitialResponse`] → parse client-first + nonce
/// - [`AuthStage::AwaitingSaslResponse`] → verify `ClientProof`, emit server-final
/// - [`AuthStage::Authenticated`] → `AuthenticationOk` + `ReadyForQuery`
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthStage {
    /// No startup message yet.
    Initial,
    /// Sent `AuthenticationSASL`; waiting for `SASLInitialResponse`.
    AwaitingSaslInitialResponse,
    /// Sent `AuthenticationSASLContinue`; waiting for `SASLResponse`.
    AwaitingSaslResponse,
    /// Handshake complete; session may run queries.
    Authenticated,
}

/// Re-export bootstrap / SCRAM types for the public pgwire API.
pub use crate::rbac::{BOOTSTRAP_PASSWORD, BOOTSTRAP_USER, SCRAM_ITERATIONS, ScramCredential};

/// Sole database name accepted on pgwire startup (`psql -d …`).
///
/// Takyonic is single-database: any other `-d` / startup `database` is rejected
/// with SQLSTATE `3D000` (Postgres "invalid_catalog_name").
pub const DEFAULT_DATABASE: &str = "postgres";

/// Whether a startup `database` name is allowed (missing/`postgres` only).
pub fn database_allowed(name: Option<&str>) -> bool {
    match name {
        None | Some("") => true,
        Some(d) => d.eq_ignore_ascii_case(DEFAULT_DATABASE),
    }
}

fn reject_unknown_database(name: &str) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "FATAL".into(),
        "3D000".into(),
        format!(
            "database \"{name}\" does not exist (Takyonic is single-database: use `{DEFAULT_DATABASE}`)"
        ),
    )))
}

/// Auth catalog backed by the engine's durable RBAC store (SCRAM-SHA-256).
#[derive(Clone, Debug)]
pub struct TakyonicAuthSource {
    catalog: SharedAuthCatalog,
    iterations: usize,
}

impl TakyonicAuthSource {
    /// Wrap a shared AUTH catalog (typically [`TakyonicEngine::auth_catalog`]).
    pub fn new(catalog: SharedAuthCatalog) -> Self {
        Self {
            catalog,
            iterations: SCRAM_ITERATIONS,
        }
    }

    /// Catalog with the bootstrap `postgres` / `password` role (standalone tests).
    pub fn with_bootstrap_user() -> Self {
        Self::new(Arc::new(parking_lot::RwLock::new(AuthCatalog::with_bootstrap())))
    }

    /// PBKDF2 iteration count advertised to SCRAM clients.
    pub fn iterations(&self) -> usize {
        self.iterations
    }

    /// Insert or replace a SCRAM credential for `username` (test helper).
    pub fn upsert(&mut self, username: impl Into<String>, credential: ScramCredential) {
        self.catalog.write().upsert_scram(username, credential);
    }
}

#[async_trait]
impl AuthSource for TakyonicAuthSource {
    async fn get_password(&self, login: &LoginInfo) -> PgWireResult<Password> {
        if !database_allowed(login.database()) {
            let name = login.database().unwrap_or("");
            debug!(database = name, "rejecting non-default database");
            return Err(reject_unknown_database(name));
        }
        let user = login.user().unwrap_or("");
        match self.catalog.read().scram_credential(user) {
            Some(cred) => {
                debug!(user, "SCRAM credential lookup ok");
                Ok(Password::new(
                    Some(cred.salt.clone()),
                    cred.salted_password.clone(),
                ))
            }
            None => {
                debug!(user, "SCRAM credential lookup miss");
                Err(PgWireError::InvalidPassword(user.to_owned()))
            }
        }
    }
}

fn default_server_params(iterations: usize) -> DefaultServerParameterProvider {
    let mut params = DefaultServerParameterProvider::default();
    params.server_version = "16.0 (Takyonic)".into();
    params.server_encoding = "UTF8".into();
    params.client_encoding = Some("UTF8".into());
    params.scram_iterations = iterations;
    params
}

/// Build a fresh SCRAM-SHA-256 SASL startup handler (one per TCP connection).
fn scram_startup_handler(
    auth_source: Arc<dyn AuthSource>,
    params: Arc<DefaultServerParameterProvider>,
    iterations: usize,
) -> Arc<SASLAuthStartupHandler<DefaultServerParameterProvider>> {
    let mut scram = ScramAuth::new(auth_source);
    scram.set_iterations(iterations);
    Arc::new(SASLAuthStartupHandler::new(params).with_scram(scram))
}

/// Parse SQL into a [`LogicalPlan`] for the extended query protocol.
#[derive(Debug, Default)]
pub struct TakyonicQueryParser;

#[async_trait]
impl QueryParser for TakyonicQueryParser {
    type Statement = LogicalPlan;

    async fn parse_sql<C>(
        &self,
        _client: &C,
        sql: &str,
        _types: &[Option<Type>],
    ) -> PgWireResult<Self::Statement>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let sql = sql.trim().trim_end_matches(';').trim();
        if sql.is_empty() {
            return Err(sql_err(crate::error::TakyonicError::Sql(
                "empty query".into(),
            )));
        }
        SqlEngine::plan(sql).map_err(sql_err)
    }
}

/// pgwire query backend: local Volcano session + optional Smart Client.
pub struct TakyonicPgBackend {
    #[allow(dead_code)]
    client: TakyonicClient,
    query_parser: Arc<TakyonicQueryParser>,
    engine: Arc<TakyonicEngine>,
    /// Per-connection sessions keyed by pgwire backend pid.
    sessions: DashMap<i32, Arc<Mutex<SessionState>>>,
    /// TCP address the pgwire listener is bound to (`inet_server_*`).
    listen_addr: parking_lot::RwLock<Option<SocketAddr>>,
}

impl TakyonicPgBackend {
    /// Wrap a connected Smart Client and a local engine for Volcano / txn control.
    pub fn new(client: TakyonicClient, engine: Arc<TakyonicEngine>) -> Self {
        Self {
            client,
            query_parser: Arc::new(TakyonicQueryParser),
            engine,
            sessions: DashMap::new(),
            listen_addr: parking_lot::RwLock::new(None),
        }
    }

    /// Record the pgwire listen address for `inet_server_addr` / `inet_server_port`.
    pub fn set_listen_addr(&self, addr: SocketAddr) {
        *self.listen_addr.write() = Some(addr);
    }

    /// Test helper: pre-seed a session for `pid` authenticated as `user`.
    pub fn new_for_test(engine: Arc<TakyonicEngine>, pid: i32, user: &str) -> Self {
        let backend = Self {
            client: TakyonicClient::new(std::iter::empty::<String>()),
            query_parser: Arc::new(TakyonicQueryParser),
            engine: Arc::clone(&engine),
            sessions: DashMap::new(),
            listen_addr: parking_lot::RwLock::new(None),
        };
        backend.sessions.insert(
            pid,
            Arc::new(Mutex::new(
                SessionState::as_user(engine, user).expect("test user must exist"),
            )),
        );
        backend
    }

    /// Session handle for `pid` (tests / introspection).
    pub fn session_arc_for_pid(&self, pid: i32) -> Arc<Mutex<SessionState>> {
        self.sessions
            .get(&pid)
            .map(|r| Arc::clone(r.value()))
            .unwrap_or_else(|| panic!("no SessionState for pid {pid}"))
    }

    /// Resolve (or create) the SessionState for this TCP connection.
    fn session_arc_for<C: ClientInfo>(&self, client: &C) -> Arc<Mutex<SessionState>> {
        let (pid, _) = client.pid_and_secret_key();
        if let Some(existing) = self.sessions.get(&pid) {
            return Arc::clone(existing.value());
        }
        let user = client
            .metadata()
            .get(METADATA_USER)
            .map(String::as_str)
            .unwrap_or(BOOTSTRAP_USER);
        let mut session = SessionState::as_user(Arc::clone(&self.engine), user)
            .unwrap_or_else(|_| SessionState::new(Arc::clone(&self.engine)));
        let (sa, sp, ca, cp) =
            net_info_from_endpoints(*self.listen_addr.read(), client.socket_addr());
        session.set_net_info(sa, sp, ca, cp);
        let state = Arc::new(Mutex::new(session));
        match self.sessions.entry(pid) {
            dashmap::mapref::entry::Entry::Occupied(o) => Arc::clone(o.get()),
            dashmap::mapref::entry::Entry::Vacant(v) => {
                v.insert(Arc::clone(&state));
                state
            }
        }
    }
}

/// Build `inet_{server,client}_{addr,port}` values from listen + peer sockets.
///
/// Unspecified listen IPs (`0.0.0.0` / `::`) yield a NULL server address (port still set).
pub fn net_info_from_endpoints(
    listen: Option<SocketAddr>,
    peer: SocketAddr,
) -> (Option<String>, Option<i64>, Option<String>, Option<i64>) {
    let (server_addr, server_port) = match listen {
        Some(sa) => {
            let addr = if sa.ip().is_unspecified() {
                None
            } else {
                Some(format_inet_ip(sa.ip()))
            };
            (addr, Some(i64::from(sa.port())))
        }
        None => (None, None),
    };
    (
        server_addr,
        server_port,
        Some(format_inet_ip(peer.ip())),
        Some(i64::from(peer.port())),
    )
}

fn format_inet_ip(ip: IpAddr) -> String {
    ip.to_string()
}

fn session_result_to_response_with_hints(
    result: SessionResult,
    type_hints: &std::collections::HashMap<String, String>,
) -> PgWireResult<Response> {
    match result.tag {
        "COPY_IN" => {
            let ncols = result
                .affected
                .map(|n| n as usize)
                .unwrap_or(1)
                .max(1);
            Ok(Response::CopyIn(CopyResponse::new(
                0,
                ncols,
                vec![0; ncols],
            )))
        }
        "COPY_OUT" => {
            // Handled specially in SimpleQueryHandler (streams CopyData).
            Ok(Response::Execution(Tag::new("COPY").with_rows(
                result.affected.unwrap_or(0) as usize,
            )))
        }
        "SELECT" => encode_select_response(
            result.rows,
            type_hints,
            result.column_order.as_deref(),
        ),
        "INSERT" | "UPDATE" | "DELETE" if !result.rows.is_empty() => {
            let mut resp = encode_select_response(
                result.rows,
                type_hints,
                result.column_order.as_deref(),
            )?;
            if let Response::Query(ref mut q) = resp {
                let tag = match result.tag {
                    "INSERT" => "INSERT 0",
                    other => other,
                };
                q.set_command_tag(tag);
            }
            Ok(resp)
        }
        "INSERT" => Ok(Response::Execution(
            Tag::new("INSERT")
                .with_oid(0)
                .with_rows(result.affected.unwrap_or(0) as usize),
        )),
        "UPDATE" => Ok(Response::Execution(
            Tag::new("UPDATE").with_rows(result.affected.unwrap_or(0) as usize),
        )),
        "DELETE" => Ok(Response::Execution(
            Tag::new("DELETE").with_rows(result.affected.unwrap_or(0) as usize),
        )),
        other => Ok(Response::Execution(Tag::new(other))),
    }
}

#[async_trait]
impl SimpleQueryHandler for TakyonicPgBackend {
    async fn do_query<C>(&self, client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo
            + pgwire::api::ClientPortalStore
            + Sink<PgWireBackendMessage>
            + Unpin
            + Send
            + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let sql = query.trim().trim_end_matches(';').trim();
        if sql.is_empty() {
            return Ok(vec![Response::EmptyQuery]);
        }

        let session = self.session_arc_for(client);
        let result = session.lock().execute_sql(sql).map_err(sql_err)?;
        if result.tag == "COPY_OUT" {
            let tsv = result
                .rows
                .first()
                .and_then(|r| r.get("__copy_tsv"))
                .unwrap_or("")
                .to_string();
            let n = result.affected.unwrap_or(0) as usize;
            let ncols = 1usize;
            client
                .feed(PgWireBackendMessage::CopyOutResponse(
                    pgwire::messages::copy::CopyOutResponse::new(0, ncols as i16, vec![0]),
                ))
                .await?;
            if !tsv.is_empty() {
                client
                    .feed(PgWireBackendMessage::CopyData(CopyData::new(
                        bytes::Bytes::from(tsv.into_bytes()),
                    )))
                    .await?;
            }
            client
                .feed(PgWireBackendMessage::CopyDone(CopyDone::new()))
                .await?;
            client
                .feed(PgWireBackendMessage::CommandComplete(CommandComplete::new(
                    format!("COPY {n}"),
                )))
                .await?;
            return Ok(vec![]);
        }
        let hints = self.engine.column_type_hints();
        Ok(vec![session_result_to_response_with_hints(result, &hints)?])
    }
}

#[async_trait]
impl CopyHandler for TakyonicPgBackend {
    async fn on_copy_data<C>(&self, client: &mut C, copy_data: CopyData) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let session = self.session_arc_for(client);
        session
            .lock()
            .append_copy_in_data(copy_data.data.as_ref())
            .map_err(sql_err)?;
        Ok(())
    }

    async fn on_copy_done<C>(&self, client: &mut C, _done: CopyDone) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let session = self.session_arc_for(client);
        let n = session.lock().finish_copy_in().map_err(sql_err)?;
        client
            .send(PgWireBackendMessage::CommandComplete(CommandComplete::new(
                format!("COPY {n}"),
            )))
            .await?;
        Ok(())
    }

    async fn on_copy_fail<C>(&self, client: &mut C, fail: CopyFail) -> PgWireError
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        self.session_arc_for(client).lock().abort_copy_in();
        PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            "XX000".to_owned(),
            format!("COPY IN mode terminated by the user: {}", fail.message),
        )))
    }
}

#[async_trait]
impl ExtendedQueryHandler for TakyonicPgBackend {
    type Statement = LogicalPlan;
    type QueryParser = TakyonicQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        self.query_parser.clone()
    }

    /// `Parse`: SQL → [`LogicalPlan`], store in session + pgwire portal store.
    async fn on_parse<C>(
        &self,
        client: &mut C,
        message: pgwire::messages::extendedquery::Parse,
    ) -> PgWireResult<()>
    where
        C: ClientInfo
            + pgwire::api::ClientPortalStore
            + Sink<PgWireBackendMessage>
            + Unpin
            + Send
            + Sync,
        C::PortalStore: pgwire::api::store::PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let name = message
            .name
            .clone()
            .unwrap_or_else(|| pgwire::api::DEFAULT_NAME.to_owned());
        {
            let session = self.session_arc_for(client);
            session
                .lock()
                .parse(&name, &message.query)
                .map_err(sql_err)?;
        }

        let types: Vec<Option<Type>> = message
            .type_oids
            .iter()
            .map(|oid| Type::from_oid(*oid))
            .collect();
        let plan = self
            .query_parser()
            .parse_sql(client, &message.query, &types)
            .await?;
        let stmt = StoredStatement::new(name, plan, types);
        client.portal_store().put_statement(Arc::new(stmt));
        client
            .send(PgWireBackendMessage::ParseComplete(
                pgwire::messages::extendedquery::ParseComplete::new(),
            ))
            .await?;
        Ok(())
    }

    /// `Bind`: attach parameters → portal in session + pgwire store.
    async fn on_bind<C>(
        &self,
        client: &mut C,
        message: pgwire::messages::extendedquery::Bind,
    ) -> PgWireResult<()>
    where
        C: ClientInfo
            + pgwire::api::ClientPortalStore
            + Sink<PgWireBackendMessage>
            + Unpin
            + Send
            + Sync,
        C::PortalStore: pgwire::api::store::PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let statement_name = message
            .statement_name
            .as_deref()
            .unwrap_or(pgwire::api::DEFAULT_NAME);
        let portal_name = message
            .portal_name
            .clone()
            .unwrap_or_else(|| pgwire::api::DEFAULT_NAME.to_owned());

        let stored = client
            .portal_store()
            .get_statement(statement_name)
            .ok_or_else(|| PgWireError::StatementNotFound(statement_name.to_owned()))?;

        let format = bind_param_format(&message.parameter_format_codes);
        let parameters = decode_bind_parameters(
            &message.parameters,
            &stored.parameter_types,
            &format,
        )
        .map_err(sql_err)?;

        {
            let session = self.session_arc_for(client);
            session
                .lock()
                .bind(&portal_name, statement_name, parameters)
                .map_err(sql_err)?;
        }

        let portal = Portal::try_new(&message, stored)?;
        client.portal_store().put_portal(Arc::new(portal));
        client
            .send(PgWireBackendMessage::BindComplete(
                pgwire::messages::extendedquery::BindComplete::new(),
            ))
            .await?;
        Ok(())
    }

    /// `Sync`: ReadyForQuery + clear unnamed session state.
    async fn on_sync<C>(
        &self,
        client: &mut C,
        _message: pgwire::messages::extendedquery::Sync,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        self.session_arc_for(client).lock().sync();
        client
            .send(PgWireBackendMessage::ReadyForQuery(
                pgwire::messages::response::ReadyForQuery::new(client.transaction_status()),
            ))
            .await?;
        client.flush().await?;
        Ok(())
    }

    async fn do_query<C>(
        &self,
        client: &mut C,
        portal: &Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let plan = portal.statement.statement.clone();
        let session = self.session_arc_for(client);
        let params = {
            let guard = session.lock();
            guard
                .portals
                .get(&portal.name)
                .map(|b| b.parameters.clone())
                .unwrap_or_else(|| {
                    decode_bind_parameters(
                        &portal.parameters,
                        &portal.statement.parameter_types,
                        &portal.parameter_format,
                    )
                    .unwrap_or_default()
                })
        };

        let result = session.lock().run_plan(&plan, params).map_err(sql_err)?;
        let hints = self.engine.column_type_hints();
        session_result_to_response_with_hints(result, &hints)
    }

    async fn do_describe_statement<C>(
        &self,
        _client: &mut C,
        stmt: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let param_types: Vec<Type> = stmt
            .parameter_types
            .iter()
            .map(|t| t.clone().unwrap_or(Type::UNKNOWN))
            .collect();
        let fields = describe_plan_fields(&stmt.statement, &self.engine);
        Ok(DescribeStatementResponse::new(param_types, fields))
    }

    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        portal: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let fields = describe_plan_fields(&portal.statement.statement, &self.engine);
        Ok(DescribePortalResponse::new(fields))
    }
}

fn sql_err(e: crate::error::TakyonicError) -> PgWireError {
    let sqlstate = match &e {
        crate::error::TakyonicError::PermissionDenied(_) => "42501",
        _ => "XX000",
    };
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".into(),
        sqlstate.into(),
        e.to_string(),
    )))
}

fn encode_select_response(
    rows: Vec<Record>,
    type_hints: &std::collections::HashMap<String, String>,
    column_order: Option<&[String]>,
) -> PgWireResult<Response> {
    let columns = infer_columns(&rows, type_hints, column_order);
    if columns.is_empty() {
        let schema = Arc::new(Vec::new());
        let stream = stream::iter(Vec::<PgWireResult<_>>::new());
        return Ok(Response::Query(QueryResponse::new(schema, stream)));
    }

    let fields: Vec<FieldInfo> = columns
        .iter()
        .map(|(name, ty)| FieldInfo::new(name.clone(), None, None, ty.clone(), FieldFormat::Text))
        .collect();
    let schema = Arc::new(fields);

    let mut encoded = Vec::with_capacity(rows.len());
    for record in &rows {
        let mut encoder = DataRowEncoder::new(schema.clone());
        for (name, ty) in &columns {
            match record.get(name) {
                None => encoder.encode_field(&None::<&str>)?,
                Some(s) => match *ty {
                    Type::INT8 | Type::INT4 | Type::INT2 => {
                        let n: i64 = s.parse().unwrap_or(0);
                        encoder.encode_field(&n)?;
                    }
                    Type::FLOAT8 | Type::FLOAT4 => {
                        let n: f64 = s.parse().unwrap_or(0.0);
                        encoder.encode_field(&n)?;
                    }
                    Type::BOOL => {
                        let b = matches!(s.to_ascii_lowercase().as_str(), "t" | "true" | "1");
                        encoder.encode_field(&b)?;
                    }
                    _ => encoder.encode_field(&s)?,
                },
            }
        }
        encoded.push(encoder.finish());
    }

    Ok(Response::Query(QueryResponse::new(
        schema,
        stream::iter(encoded),
    )))
}

/// Prefer catalog `type_hints`; else heuristic INT8/BOOL/FLOAT8/VARCHAR.
/// Column order follows first-seen field order across rows.
fn infer_columns(
    rows: &[Record],
    type_hints: &std::collections::HashMap<String, String>,
    column_order: Option<&[String]>,
) -> Vec<(String, Type)> {
    use std::collections::BTreeMap;
    let mut seen: BTreeMap<String, HeuristicKind> = BTreeMap::new();
    for record in rows {
        for (k, v) in &record.fields {
            let kind = classify_value(v);
            seen.entry(k.clone())
                .and_modify(|all| *all = all.merge(kind))
                .or_insert(kind);
        }
    }
    let names: Vec<String> = if let Some(order) = column_order {
        order.to_vec()
    } else if rows.is_empty() {
        seen.keys().cloned().collect()
    } else {
        // Preserve first-row BTree order as a stable fallback.
        rows[0].fields.keys().cloned().collect()
    };
    names
        .into_iter()
        .filter_map(|name| {
            let ty = if let Some(hint) = type_hints.get(&name) {
                catalog_type_to_pg(hint)
            } else {
                match seen.get(&name).copied().unwrap_or(HeuristicKind::Text) {
                    HeuristicKind::Int => Type::INT8,
                    HeuristicKind::Bool => Type::BOOL,
                    HeuristicKind::Float => Type::FLOAT8,
                    HeuristicKind::Text => Type::VARCHAR,
                }
            };
            Some((name, ty))
        })
        .collect()
}

#[derive(Clone, Copy)]
enum HeuristicKind {
    Int,
    Bool,
    Float,
    Text,
}

impl HeuristicKind {
    fn merge(self, other: Self) -> Self {
        use HeuristicKind::*;
        match (self, other) {
            (Text, _) | (_, Text) => Text,
            (Bool, Bool) => Bool,
            (Int, Int) => Int,
            (Float, Float) | (Int, Float) | (Float, Int) => Float,
            (Bool, _) | (_, Bool) => Text,
        }
    }
}

fn classify_value(v: &str) -> HeuristicKind {
    let lower = v.to_ascii_lowercase();
    if matches!(lower.as_str(), "t" | "f" | "true" | "false") {
        return HeuristicKind::Bool;
    }
    if v.parse::<i64>().is_ok() {
        return HeuristicKind::Int;
    }
    if v.parse::<f64>().is_ok() {
        return HeuristicKind::Float;
    }
    HeuristicKind::Text
}

fn catalog_type_to_pg(data_type: &str) -> Type {
    let upper = data_type.to_ascii_uppercase();
    // Catalog tokens may use `_` for spaces (`TIMESTAMP_WITH_TIME_ZONE`) and
    // optional `(…)` precision (`NUMERIC(10,2)`).
    let base = upper
        .split('(')
        .next()
        .unwrap_or(&upper)
        .trim()
        .trim_matches('"');
    match base {
        "SMALLINT" | "INT2" => Type::INT2,
        "INT" | "INTEGER" | "INT4" => Type::INT4,
        "BIGINT" | "INT8" => Type::INT8,
        "BOOL" | "BOOLEAN" => Type::BOOL,
        "FLOAT" | "REAL" | "FLOAT4" => Type::FLOAT4,
        "DOUBLE" | "FLOAT8" | "DOUBLE_PRECISION" | "DOUBLE PRECISION" => Type::FLOAT8,
        "TEXT" | "VARCHAR" | "BPCHAR" | "CHAR" | "CHARACTER" | "CHARACTER_VARYING"
        | "CHARACTER VARYING" => Type::VARCHAR,
        "UUID" => Type::UUID,
        "BYTEA" => Type::BYTEA,
        "NUMERIC" | "DECIMAL" => Type::NUMERIC,
        "DATE" => Type::DATE,
        "TIME" | "TIME_WITHOUT_TIME_ZONE" | "TIME WITHOUT TIME ZONE" => Type::TIME,
        "TIMESTAMP" | "TIMESTAMP_WITHOUT_TIME_ZONE" | "TIMESTAMP WITHOUT TIME ZONE" => {
            Type::TIMESTAMP
        }
        "TIMESTAMPTZ"
        | "TIMESTAMP_WITH_TIME_ZONE"
        | "TIMESTAMP WITH TIME ZONE" => Type::TIMESTAMPTZ,
        "JSON" => Type::JSON,
        "JSONB" => Type::JSONB,
        "INTERVAL" => Type::INTERVAL,
        _ => Type::VARCHAR,
    }
}

/// Factory wiring startup + simple + extended query handlers.
pub struct TakyonicPgFactory {
    backend: Arc<TakyonicPgBackend>,
    /// Shared SCRAM credential catalog (salt + SaltedPassword per role).
    auth_source: Arc<dyn AuthSource>,
    params: Arc<DefaultServerParameterProvider>,
    iterations: usize,
}

impl TakyonicPgFactory {
    /// Build handlers around a connected Smart Client and local engine.
    ///
    /// Startup uses SCRAM-SHA-256 with the bootstrap [`BOOTSTRAP_USER`] role.
    /// A fresh SASL state machine is created per connection (see
    /// [`PgWireServerHandlers::startup_handler`]).
    pub fn new(client: TakyonicClient, engine: Arc<TakyonicEngine>) -> Self {
        let auth = TakyonicAuthSource::new(engine.auth_catalog());
        let iterations = auth.iterations();
        Self {
            backend: Arc::new(TakyonicPgBackend::new(client, engine)),
            auth_source: Arc::new(auth),
            params: Arc::new(default_server_params(iterations)),
            iterations,
        }
    }

    /// Override the auth catalog (tests / custom role injection).
    pub fn with_auth_source(mut self, auth: Arc<dyn AuthSource>, iterations: usize) -> Self {
        self.auth_source = auth;
        self.iterations = iterations;
        self.params = Arc::new(default_server_params(iterations));
        self
    }

    /// Set the pgwire listen address used by `inet_server_*` on new sessions.
    pub fn set_listen_addr(&self, addr: SocketAddr) {
        self.backend.set_listen_addr(addr);
    }
}

impl PgWireServerHandlers for TakyonicPgFactory {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        self.backend.clone()
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        self.backend.clone()
    }

    fn copy_handler(&self) -> Arc<impl CopyHandler> {
        self.backend.clone()
    }

    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        // New handler (and Mutex<SASLState>) per TCP session — never share
        // SASLAuthStartupHandler across concurrent connections.
        scram_startup_handler(
            Arc::clone(&self.auth_source),
            Arc::clone(&self.params),
            self.iterations,
        )
    }
}

fn copy_table_to_file(
    session: &mut SessionState,
    table: &str,
    columns: &[String],
    path: &str,
) -> crate::error::Result<u64> {
    let out = copy_table_to_tsv(session, table, columns)?;
    let n = out.lines().filter(|l| !l.is_empty()).count() as u64;
    std::fs::write(path, out).map_err(|e| {
        crate::error::TakyonicError::Engine(format!("COPY TO `{path}`: {e}"))
    })?;
    Ok(n)
}

fn copy_table_to_tsv(
    session: &mut SessionState,
    table: &str,
    columns: &[String],
) -> crate::error::Result<String> {
    let schema = session.engine.table_schema(table)?;
    let cols: Vec<String> = if columns.is_empty() {
        if schema.columns.is_empty() {
            vec![schema.primary_key.clone()]
        } else {
            schema.columns.iter().map(|c| c.name.clone()).collect()
        }
    } else {
        columns.to_vec()
    };
    let rows = session
        .execute_sql(&format!("SELECT * FROM {table}"))?
        .rows;
    let mut out = String::new();
    for row in &rows {
        let mut fields = Vec::with_capacity(cols.len());
        for c in &cols {
            let v = row.get(c).unwrap_or("");
            fields.push(escape_copy_field(v));
        }
        out.push_str(&fields.join("\t"));
        out.push('\n');
    }
    Ok(out)
}

fn copy_table_from_file(
    session: &mut SessionState,
    table: &str,
    columns: &[String],
    path: &str,
) -> crate::error::Result<u64> {
    let text = std::fs::read_to_string(path).map_err(|e| {
        crate::error::TakyonicError::Engine(format!("COPY FROM `{path}`: {e}"))
    })?;
    copy_table_from_tsv(session, table, columns, &text)
}

fn copy_table_from_tsv(
    session: &mut SessionState,
    table: &str,
    columns: &[String],
    text: &str,
) -> crate::error::Result<u64> {
    let schema = session.engine.table_schema(table)?;
    let cols: Vec<String> = if columns.is_empty() {
        if schema.columns.is_empty() {
            vec![schema.primary_key.clone()]
        } else {
            schema.columns.iter().map(|c| c.name.clone()).collect()
        }
    } else {
        columns.to_vec()
    };
    let mut n = 0u64;
    for line in text.lines() {
        if line.is_empty() || line.starts_with('\\') {
            continue;
        }
        let fields: Vec<&str> = line.split('\t').collect();
        if fields.len() != cols.len() {
            return Err(crate::error::TakyonicError::Sql(format!(
                "COPY FROM: expected {} columns, got {}",
                cols.len(),
                fields.len()
            )));
        }
        let col_list = cols.join(", ");
        let val_list: Vec<String> = fields
            .iter()
            .map(|v| {
                if *v == "\\N" {
                    "NULL".into()
                } else {
                    format!("'{}'", v.replace('\'', "''"))
                }
            })
            .collect();
        let sql = format!(
            "INSERT INTO {table} ({col_list}) VALUES ({})",
            val_list.join(", ")
        );
        session.execute_sql(&sql)?;
        n += 1;
    }
    Ok(n)
}

fn escape_copy_field(v: &str) -> String {
    if v.is_empty() {
        return "\\N".into();
    }
    v.replace('\\', "\\\\")
        .replace('\t', "\\t")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::schema::{ColumnSpec, IndexDef, Record, TableSchema};
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_session(name: &str) -> (SessionState, std::path::PathBuf) {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("takyonic-pg-{name}-{nanos}"));
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
        engine
            .register_table(TableSchema::new(
                "users",
                "id",
                vec![IndexDef::new("age", "age")],
            ))
            .unwrap();
        (SessionState::new(engine), root)
    }

    fn temp_session_mpp(name: &str) -> (SessionState, std::path::PathBuf) {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("takyonic-pg-mpp-{name}-{nanos}"));
        let config = Config::default()
            .data_dir(root.join("data"))
            .wal_dir(root.join("wal"))
            .memtable_size_bytes(64 * 1024 * 1024)
            .l0_rapid_pool_threads(1)
            .ln_haul_pool_threads(1)
            .compaction_write_bytes_per_sec(1024 * 1024 * 1024)
            .mpp_enabled(true)
            .metrics_enabled(true)
            .metrics_bind("127.0.0.1:0");
        let engine = Arc::new(TakyonicEngine::open(config).unwrap());
        (SessionState::new(engine), root)
    }

    #[test]
    fn session_mpp_group_by_uses_coordinator_not_local_fallback() {
        let (mut session, root) = temp_session_mpp("c1-agg");
        session
            .engine()
            .register_table(
                TableSchema::new("employees", "id", Vec::new()).with_columns(vec![
                    crate::schema::ColumnSpec::new("id", "TEXT"),
                    crate::schema::ColumnSpec::new("department", "TEXT"),
                    crate::schema::ColumnSpec::new("salary", "INT"),
                ]),
            )
            .unwrap();
        for (id, dept, sal) in [
            ("1", "Engineering", "100"),
            ("2", "Engineering", "150"),
            ("3", "Sales", "90"),
            ("4", "Sales", "110"),
        ] {
            session
                .execute_sql(&format!(
                    "INSERT INTO employees (id, department, salary) VALUES ('{id}', '{dept}', '{sal}')"
                ))
                .unwrap();
        }

        let explain = session
            .execute_sql(
                "EXPLAIN SELECT department, SUM(salary) FROM employees GROUP BY department",
            )
            .unwrap();
        let plan_text = explain.rows[0].get("QUERY PLAN").unwrap_or("");
        assert!(
            plan_text.contains("DistributedAggregate"),
            "mpp_enabled EXPLAIN must show DistributedAggregate, got: {plan_text}"
        );

        let rows = session
            .execute_sql("SELECT department, SUM(salary) FROM employees GROUP BY department")
            .unwrap();
        assert_eq!(rows.rows.len(), 2);
        let mut by_dept = std::collections::BTreeMap::new();
        for r in &rows.rows {
            by_dept.insert(
                r.get("department").unwrap_or("").to_string(),
                r.get("SUM(salary)").unwrap_or("").to_string(),
            );
        }
        assert_eq!(by_dept.get("Engineering").map(String::as_str), Some("250"));
        assert_eq!(by_dept.get("Sales").map(String::as_str), Some("200"));
        assert!(session.engine().metrics().mpp_shuffle_sent() > 0);

        drop(session);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_mpp_min_max_avg_distributed() {
        let (mut session, root) = temp_session_mpp("c1-mma");
        session
            .engine()
            .register_table(
                TableSchema::new("employees", "id", Vec::new()).with_columns(vec![
                    crate::schema::ColumnSpec::new("id", "TEXT"),
                    crate::schema::ColumnSpec::new("department", "TEXT"),
                    crate::schema::ColumnSpec::new("salary", "INT"),
                ]),
            )
            .unwrap();
        for (id, dept, sal) in [
            ("1", "Engineering", "100"),
            ("2", "Engineering", "150"),
            ("3", "Sales", "90"),
            ("4", "Sales", "110"),
        ] {
            session
                .execute_sql(&format!(
                    "INSERT INTO employees (id, department, salary) VALUES ('{id}', '{dept}', '{sal}')"
                ))
                .unwrap();
        }

        for (sql, needle) in [
            (
                "EXPLAIN SELECT department, MIN(salary) FROM employees GROUP BY department",
                "DistributedAggregate",
            ),
            (
                "EXPLAIN SELECT department, MAX(salary) FROM employees GROUP BY department",
                "DistributedAggregate",
            ),
            (
                "EXPLAIN SELECT department, AVG(salary) FROM employees GROUP BY department",
                "DistributedAggregate",
            ),
        ] {
            let plan_text = session.execute_sql(sql).unwrap().rows[0]
                .get("QUERY PLAN")
                .unwrap_or("")
                .to_string();
            assert!(
                plan_text.contains(needle),
                "expected {needle} in {plan_text}"
            );
        }

        let min_rows = session
            .execute_sql("SELECT department, MIN(salary) FROM employees GROUP BY department")
            .unwrap();
        let max_rows = session
            .execute_sql("SELECT department, MAX(salary) FROM employees GROUP BY department")
            .unwrap();
        let avg_rows = session
            .execute_sql("SELECT department, AVG(salary) FROM employees GROUP BY department")
            .unwrap();
        let mut mins = std::collections::BTreeMap::new();
        for r in &min_rows.rows {
            mins.insert(
                r.get("department").unwrap_or("").to_string(),
                r.get("MIN(salary)").unwrap_or("").to_string(),
            );
        }
        let mut maxs = std::collections::BTreeMap::new();
        for r in &max_rows.rows {
            maxs.insert(
                r.get("department").unwrap_or("").to_string(),
                r.get("MAX(salary)").unwrap_or("").to_string(),
            );
        }
        let mut avgs = std::collections::BTreeMap::new();
        for r in &avg_rows.rows {
            avgs.insert(
                r.get("department").unwrap_or("").to_string(),
                r.get("AVG(salary)").unwrap_or("").to_string(),
            );
        }
        assert_eq!(mins.get("Engineering").map(String::as_str), Some("100"));
        assert_eq!(maxs.get("Engineering").map(String::as_str), Some("150"));
        assert_eq!(avgs.get("Engineering").map(String::as_str), Some("125"));
        assert_eq!(mins.get("Sales").map(String::as_str), Some("90"));
        assert_eq!(maxs.get("Sales").map(String::as_str), Some("110"));
        assert_eq!(avgs.get("Sales").map(String::as_str), Some("100"));

        drop(session);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_rebalance_table_moves_hot_partition() {
        use crate::partition::{PartitionMap, PartitioningStrategy};

        let (mut session, root) = temp_session_mpp("rebal");
        // Force all partitions onto node 1 initially so load is skewed once we
        // insert many keys hashing to those buckets.
        session
            .engine()
            .register_table(
                TableSchema::new("users", "user_id", Vec::new())
                    .with_columns(vec![
                        crate::schema::ColumnSpec::new("user_id", "TEXT"),
                        crate::schema::ColumnSpec::new("name", "TEXT"),
                    ])
                    .with_partitioning(PartitioningStrategy::Hash {
                        column: "user_id".into(),
                        bucket_count: 4,
                    })
                    .with_partition_map(PartitionMap {
                        // hot=1 owns three slots; cold=2 owns one empty-ish slot
                        assignments: vec![1, 1, 1, 2],
                    }),
            )
            .unwrap();

        // Insert enough rows that node 1's observed load is ≥ 2× node 2.
        for i in 0..40 {
            session
                .execute_sql(&format!(
                    "INSERT INTO users (user_id, name) VALUES ('u{i}', 'n{i}')"
                ))
                .unwrap();
        }

        let before = session
            .engine()
            .table_schema("users")
            .unwrap()
            .partition_map
            .assignments
            .clone();
        let plan = crate::sql::LogicalPlanner::plan("REBALANCE TABLE users").unwrap();
        assert!(matches!(plan, LogicalPlan::Rebalance { .. }));

        let result = session.execute_sql("REBALANCE TABLE users").unwrap();
        assert_eq!(result.tag, "REBALANCE");
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("moved"), Some("1"));
        let after = session
            .engine()
            .table_schema("users")
            .unwrap()
            .partition_map
            .assignments;
        assert_ne!(before, after, "PMAP should change after a successful move");
        assert_eq!(
            after.iter().filter(|&&n| n == 2).count(),
            before.iter().filter(|&&n| n == 2).count() + 1,
            "cold node should gain one partition"
        );

        drop(session);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_mpp_distributed_join_e2e() {
        let (mut session, root) = temp_session_mpp("c1-join");
        session
            .engine()
            .register_table(
                TableSchema::new("users", "id", Vec::new()).with_columns(vec![
                    crate::schema::ColumnSpec::new("id", "TEXT"),
                    crate::schema::ColumnSpec::new("name", "TEXT"),
                ]),
            )
            .unwrap();
        session
            .engine()
            .register_table(
                TableSchema::new("orders", "order_id", Vec::new()).with_columns(vec![
                    crate::schema::ColumnSpec::new("order_id", "TEXT"),
                    crate::schema::ColumnSpec::new("user_id", "TEXT"),
                    crate::schema::ColumnSpec::new("amt", "INT"),
                ]),
            )
            .unwrap();
        for (id, name) in [("1", "Ada"), ("2", "Bob"), ("3", "Cy")] {
            session
                .execute_sql(&format!(
                    "INSERT INTO users (id, name) VALUES ('{id}', '{name}')"
                ))
                .unwrap();
        }
        for (oid, uid, amt) in [("10", "1", "5"), ("11", "1", "7"), ("20", "2", "3")] {
            session
                .execute_sql(&format!(
                    "INSERT INTO orders (order_id, user_id, amt) VALUES ('{oid}', '{uid}', '{amt}')"
                ))
                .unwrap();
        }

        let explain = session
            .execute_sql(
                "EXPLAIN SELECT users.name, orders.amt FROM users \
                 INNER JOIN orders ON users.id = orders.user_id",
            )
            .unwrap();
        let plan_text = explain.rows[0].get("QUERY PLAN").unwrap_or("");
        assert!(
            plan_text.contains("DistributedJoin"),
            "mpp_enabled EXPLAIN must show DistributedJoin, got: {plan_text}"
        );

        let rows = session
            .execute_sql(
                "SELECT users.name, orders.amt FROM users \
                 INNER JOIN orders ON users.id = orders.user_id",
            )
            .unwrap();
        assert_eq!(rows.rows.len(), 3, "Ada×2 + Bob×1");
        let mut names: Vec<_> = rows
            .rows
            .iter()
            .map(|r| r.get("name").unwrap_or("").to_string())
            .collect();
        names.sort();
        assert_eq!(names, vec!["Ada", "Ada", "Bob"]);
        assert!(
            session.engine().metrics().mpp_fragments() > 0,
            "join must dispatch remote fragments"
        );

        drop(session);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_mpp_partitioned_scan_uses_coordinator_remote_fetch() {
        use crate::partition::{PartitionMap, PartitioningStrategy};

        let (mut session, root) = temp_session_mpp("c2-scan");
        session
            .engine()
            .register_table(
                TableSchema::new("users", "user_id", Vec::new())
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
        for i in 0..12 {
            session
                .execute_sql(&format!(
                    "INSERT INTO users (user_id, name) VALUES ('{i}', 'u{i}')"
                ))
                .unwrap();
        }

        let explain = session
            .execute_sql("EXPLAIN SELECT * FROM users WHERE user_id = '7'")
            .unwrap();
        let plan_text = explain.rows[0].get("QUERY PLAN").unwrap_or("");
        assert!(
            plan_text.contains("DistributedScan(users)"),
            "partitioned EXPLAIN must show DistributedScan, got: {plan_text}"
        );
        assert_eq!(
            plan_text.matches("RemoteWorker(").count(),
            1,
            "pruned EXPLAIN must show one RemoteWorker, got: {plan_text}"
        );

        let rows = session
            .execute_sql("SELECT * FROM users WHERE user_id = '7'")
            .unwrap();
        assert_eq!(rows.rows.len(), 1);
        assert_eq!(rows.rows[0].get("user_id"), Some("7"));
        assert!(session.engine().metrics().mpp_fragments() > 0);

        drop(session);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_mpp_partitioned_insert_routes_without_broadcast() {
        use crate::mpp::{FragmentDispatcher, FragmentSpec, Worker};
        use crate::partition::{PartitionMap, PartitioningStrategy};
        use crate::shuffle::ShuffleManager;
        use std::sync::Mutex as StdMutex;

        let (mut session, root) = temp_session_mpp("c4-ins");
        session
            .engine()
            .register_table(
                TableSchema::new("orders", "id", Vec::new())
                    .with_columns(vec![
                        crate::schema::ColumnSpec::new("id", "TEXT"),
                        crate::schema::ColumnSpec::new("amt", "INT"),
                    ])
                    .with_partitioning(PartitioningStrategy::Hash {
                        column: "id".into(),
                        bucket_count: 3,
                    })
                    .with_partition_map(PartitionMap::round_robin(&[1, 2, 3], 3)),
            )
            .unwrap();

        let shuffle = Arc::new(ShuffleManager::new(
            32,
            Some(Arc::clone(session.engine().metrics())),
        ));
        let contacted: Arc<StdMutex<Vec<u64>>> = Arc::new(StdMutex::new(Vec::new()));
        struct HitDispatch {
            inner: Worker,
            contacted: Arc<StdMutex<Vec<u64>>>,
        }
        impl FragmentDispatcher for HitDispatch {
            fn execute_remote(
                &self,
                node_id: u64,
                fragment: &FragmentSpec,
            ) -> crate::error::Result<Vec<Record>> {
                self.contacted.lock().unwrap().push(node_id);
                self.inner.execute_fragment(fragment)
            }
        }
        session.engine().set_mpp_dispatcher(Some(Arc::new(HitDispatch {
            inner: Worker::new(
                Arc::clone(session.engine()),
                Arc::clone(&shuffle),
                Arc::clone(session.engine().metrics()),
            ),
            contacted: Arc::clone(&contacted),
        })));

        let mut owners = std::collections::HashSet::new();
        for i in 0..30 {
            let r = session
                .execute_sql(&format!(
                    "INSERT INTO orders (id, amt) VALUES ('{i}', '{i}')"
                ))
                .unwrap();
            assert_eq!(r.tag, "INSERT");
            assert_eq!(r.affected, Some(1));
        }
        let hits = contacted.lock().unwrap().clone();
        assert_eq!(
            hits.len(),
            30,
            "each INSERT must contact exactly one worker (no broadcast), got {hits:?}"
        );
        for h in &hits {
            owners.insert(*h);
        }
        assert_eq!(
            owners.len(),
            3,
            "hash inserts must hit all 3 owners over 30 keys, got {owners:?}"
        );

        // Clear dispatcher for teardown cleanliness.
        session.engine().set_mpp_dispatcher(None);
        drop(session);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_state_parse_bind_sync() {
        let (mut session, root) = temp_session("parse");
        session
            .parse("s1", "SELECT * FROM users WHERE status = 'active'")
            .unwrap();
        assert!(session.prepared_statements.contains_key("s1"));

        session.bind("p1", "s1", vec![]).unwrap();
        assert!(session.portals.contains_key("p1"));

        let result = session.execute("p1").unwrap();
        assert_eq!(result.tag, "SELECT");
        assert!(result.rows.is_empty());

        session.sync();
        assert!(session.portals.is_empty());
        assert!(session.prepared_statements.contains_key("s1"));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_state_parse_join() {
        let (mut session, root) = temp_session("join");
        session
            .parse(
                "j1",
                "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id",
            )
            .unwrap();
        match session.prepared_statements.get("j1") {
            Some(LogicalPlan::Join { join_type, .. }) => {
                assert_eq!(*join_type, crate::sql::JoinType::Inner);
            }
            other => panic!("expected Join plan, got {other:?}"),
        }
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn parse_bind_execute_parameterized_filter() {
        let (mut session, root) = temp_session("bind");
        session
            .parse("s_age", "SELECT * FROM users WHERE age > $1")
            .unwrap();
        session
            .bind("p_age", "s_age", vec![Value::Int(25)])
            .unwrap();
        assert_eq!(
            session.portals.get("p_age").unwrap().parameters,
            vec![Value::Int(25)]
        );

        let dataset = vec![
            Record::new().set("name", "Ada").set("age", "30"),
            Record::new().set("name", "Bob").set("age", "20"),
            Record::new().set("name", "Cy").set("age", "25"),
            Record::new().set("name", "Di").set("age", "40"),
        ];
        let out = session.execute_with_rows("p_age", dataset).unwrap();
        assert_eq!(out.len(), 2);
        let names: Vec<_> = out.iter().map(|r| r.get("name").unwrap()).collect();
        assert_eq!(names, vec!["Ada", "Di"]);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn explicit_txn_isolation_and_rollback() {
        let (mut session, root) = temp_session("txn");
        let engine = Arc::clone(session.engine());

        // --- COMMIT path: isolation then visibility ---
        assert_eq!(session.txn_mode(), SessionTxnMode::Idle);
        let begin = session.execute_sql("BEGIN").unwrap();
        assert_eq!(begin.tag, "BEGIN");
        assert_eq!(session.txn_mode(), SessionTxnMode::InTransaction);

        let ins = session
            .execute_sql(
                "INSERT INTO users (id, name, age) VALUES (99, 'TxnTest', 50)",
            )
            .unwrap();
        assert_eq!(ins.tag, "INSERT");
        assert_eq!(ins.affected, Some(1));

        // Second client snapshot must NOT see uncommitted row.
        let mut outsider = engine.begin().unwrap();
        let peek = outsider.get_record("users", "99").unwrap();
        assert!(peek.is_none(), "uncommitted insert must be invisible");
        outsider.abort();

        let commit = session.execute_sql("COMMIT").unwrap();
        assert_eq!(commit.tag, "COMMIT");
        assert_eq!(session.txn_mode(), SessionTxnMode::Idle);

        let mut outsider = engine.begin().unwrap();
        let peek = outsider.get_record("users", "99").unwrap();
        assert_eq!(peek.as_ref().and_then(|r| r.get("name")), Some("TxnTest"));
        outsider.abort();

        // --- ROLLBACK path: workspace discarded ---
        session.execute_sql("BEGIN").unwrap();
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (100, 'Gone', 1)")
            .unwrap();
        // Visible inside the same session txn.
        let inside = session
            .execute_sql("SELECT * FROM users WHERE id = 100")
            .unwrap();
        assert_eq!(inside.rows.len(), 1);

        let rollback = session.execute_sql("ROLLBACK").unwrap();
        assert_eq!(rollback.tag, "ROLLBACK");
        assert_eq!(session.txn_mode(), SessionTxnMode::Idle);

        let after = session
            .execute_sql("SELECT * FROM users WHERE id = 100")
            .unwrap();
        assert!(after.rows.is_empty(), "rolled-back insert must be gone");

        // id=99 from the committed txn still present.
        let kept = session
            .execute_sql("SELECT * FROM users WHERE id = 99")
            .unwrap();
        assert_eq!(kept.rows.len(), 1);

        engine.close().unwrap();
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn bootstrap_scram_credential_is_deterministic() {
        let a = ScramCredential::from_password(
            BOOTSTRAP_PASSWORD,
            b"takyonic-scram!!",
            SCRAM_ITERATIONS,
        );
        let b = ScramCredential::from_password(
            BOOTSTRAP_PASSWORD,
            b"takyonic-scram!!",
            SCRAM_ITERATIONS,
        );
        assert_eq!(a.salt, b.salt);
        assert_eq!(a.salted_password, b.salted_password);
        assert_eq!(a.salted_password.len(), 32); // SHA-256 output
    }

    #[tokio::test]
    async fn auth_source_accepts_postgres_rejects_unknown() {
        let auth = TakyonicAuthSource::with_bootstrap_user();
        let ok = auth
            .get_password(&LoginInfo::new(Some(BOOTSTRAP_USER), Some("postgres"), "127.0.0.1".into()))
            .await
            .unwrap();
        assert!(ok.salt().is_some());
        assert_eq!(ok.password().len(), 32);

        let err = auth
            .get_password(&LoginInfo::new(Some("nope"), Some("postgres"), "127.0.0.1".into()))
            .await
            .unwrap_err();
        assert!(matches!(err, PgWireError::InvalidPassword(_)));
    }

    #[tokio::test]
    async fn auth_source_rejects_non_default_database() {
        assert!(database_allowed(None));
        assert!(database_allowed(Some("")));
        assert!(database_allowed(Some("postgres")));
        assert!(database_allowed(Some("POSTGRES")));
        assert!(!database_allowed(Some("appdb")));

        let auth = TakyonicAuthSource::with_bootstrap_user();
        // Default / missing database still authenticates.
        auth.get_password(&LoginInfo::new(
            Some(BOOTSTRAP_USER),
            None,
            "127.0.0.1".into(),
        ))
        .await
        .unwrap();

        let err = auth
            .get_password(&LoginInfo::new(
                Some(BOOTSTRAP_USER),
                Some("appdb"),
                "127.0.0.1".into(),
            ))
            .await
            .unwrap_err();
        match err {
            PgWireError::UserError(info) => {
                assert_eq!(info.code, "3D000");
                assert!(info.message.contains("appdb"));
                assert!(info.message.contains(DEFAULT_DATABASE));
            }
            other => panic!("expected UserError 3D000, got {other:?}"),
        }
    }

    #[test]
    fn factory_builds_scram_startup_handler() {
        // Smoke: constructing the factory + asking for a startup handler must
        // succeed without panicking (fresh SASL state per call).
        let (session, root) = temp_session("scram-factory");
        let engine = Arc::clone(session.engine());
        let client = crate::client::TakyonicClient::new(vec!["127.0.0.1:1".to_string()]);
        let factory = TakyonicPgFactory::new(client, engine);
        let _h1 = factory.startup_handler();
        let _h2 = factory.startup_handler();
        assert_eq!(AuthStage::Initial, AuthStage::Initial);
        assert_ne!(AuthStage::Initial, AuthStage::Authenticated);
        drop(_h1);
        drop(_h2);
        drop(factory);
        drop(session);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_group_by_order_by_limit_topn() {
        let (mut session, root) = temp_session("topn-session");
        session
            .engine()
            .register_table(TableSchema::new(
                "employees",
                "id",
                vec![
                    IndexDef::new("department", "department"),
                    IndexDef::new("salary", "salary"),
                ],
            ))
            .unwrap();

        session
            .execute_sql(
                "INSERT INTO employees (id, department, salary) VALUES \
                 (1, 'Sales', 5000), (2, 'Sales', 7000), (3, 'Engineering', 9000)",
            )
            .unwrap();

        let sql = "SELECT department, SUM(salary) FROM employees GROUP BY department \
                   ORDER BY SUM(salary) DESC LIMIT 1";
        let plan = SqlEngine::plan(sql).unwrap();
        let physical = executor::optimize_with_catalog(
            &plan,
            &|t| session.engine().table_schema(t).ok(),
            &|_| None,
        )
        .unwrap();
        assert!(
            matches!(physical, crate::executor::PhysicalPlan::TopN { fetch: 1, .. }),
            "session path must see TopN plan, got {physical:?}"
        );

        let result = session.execute_sql(sql).unwrap();
        assert_eq!(result.tag, "SELECT");
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("department"), Some("Sales"));
        assert_eq!(result.rows[0].get("sum(salary)"), Some("12000"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_group_by_all_e2e() {
        let (mut session, root) = temp_session("group-by-all");
        session
            .engine()
            .register_table(TableSchema::new(
                "employees",
                "id",
                vec![
                    IndexDef::new("department", "department"),
                    IndexDef::new("city", "city"),
                ],
            ))
            .unwrap();

        session
            .execute_sql(
                "INSERT INTO employees (id, department, city, salary) VALUES \
                 (1, 'Sales', 'A', 5000), (2, 'Sales', 'A', 7000), \
                 (3, 'Sales', 'B', 6000), (4, 'Engineering', 'A', 9000)",
            )
            .unwrap();

        let result = session
            .execute_sql(
                "SELECT department, city, COUNT(*) FROM employees GROUP BY ALL \
                 ORDER BY department, city",
            )
            .unwrap();
        assert_eq!(result.tag, "SELECT");
        assert_eq!(result.rows.len(), 3);
        assert_eq!(result.rows[0].get("department"), Some("Engineering"));
        assert_eq!(result.rows[0].get("city"), Some("A"));
        assert_eq!(result.rows[0].get("count(*)"), Some("1"));
        assert_eq!(result.rows[1].get("department"), Some("Sales"));
        assert_eq!(result.rows[1].get("city"), Some("A"));
        assert_eq!(result.rows[1].get("count(*)"), Some("2"));
        assert_eq!(result.rows[2].get("department"), Some("Sales"));
        assert_eq!(result.rows[2].get("city"), Some("B"));
        assert_eq!(result.rows[2].get("count(*)"), Some("1"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_order_by_all_e2e() {
        let (mut session, root) = temp_session("order-by-all");
        session
            .execute_sql(
                "INSERT INTO users (id, name, age) VALUES \
                 (1, 'Bob', 30), (2, 'Ada', 20), (3, 'Ada', 25)",
            )
            .unwrap();

        let result = session
            .execute_sql("SELECT name, age FROM users ORDER BY ALL")
            .unwrap();
        assert_eq!(result.tag, "SELECT");
        assert_eq!(result.rows.len(), 3);
        assert_eq!(result.rows[0].get("name"), Some("Ada"));
        assert_eq!(result.rows[0].get("age"), Some("20"));
        assert_eq!(result.rows[1].get("name"), Some("Ada"));
        assert_eq!(result.rows[1].get("age"), Some("25"));
        assert_eq!(result.rows[2].get("name"), Some("Bob"));
        assert_eq!(result.rows[2].get("age"), Some("30"));

        let desc = session
            .execute_sql("SELECT name, age FROM users ORDER BY ALL DESC")
            .unwrap();
        assert_eq!(desc.rows[0].get("name"), Some("Bob"));
        assert_eq!(desc.rows[2].get("name"), Some("Ada"));
        assert_eq!(desc.rows[2].get("age"), Some("20"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_alter_table_add_drop_column() {
        let (mut session, root) = temp_session("alter-col");
        session
            .execute_sql("CREATE TABLE items (id BIGINT PRIMARY KEY, name TEXT)")
            .unwrap();
        let add = session
            .execute_sql("ALTER TABLE items ADD COLUMN qty INT")
            .unwrap();
        assert_eq!(add.tag, "ALTER TABLE");
        let schema = session.engine().table_schema("items").unwrap();
        assert_eq!(schema.columns.len(), 3);
        assert_eq!(schema.columns[2].name, "qty");
        assert_eq!(schema.columns[2].data_type, "INT");

        session
            .execute_sql("INSERT INTO items (id, name, qty) VALUES (1, 'a', 5)")
            .unwrap();
        let sel = session
            .execute_sql("SELECT qty FROM items WHERE id = 1")
            .unwrap();
        assert_eq!(sel.rows[0].get("qty"), Some("5"));

        let drop = session
            .execute_sql("ALTER TABLE items DROP COLUMN qty")
            .unwrap();
        assert_eq!(drop.tag, "ALTER TABLE");
        let schema = session.engine().table_schema("items").unwrap();
        assert_eq!(schema.columns.len(), 2);
        assert!(schema.columns.iter().all(|c| c.name != "qty"));

        assert!(session
            .execute_sql("ALTER TABLE items DROP COLUMN id")
            .unwrap_err()
            .to_string()
            .contains("primary key"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_alter_table_rename_e2e() {
        let (mut session, root) = temp_session("alter-rename");
        session
            .execute_sql("CREATE TABLE items (id BIGINT PRIMARY KEY, name TEXT)")
            .unwrap();
        session
            .execute_sql("INSERT INTO items (id, name) VALUES (1, 'Ada'), (2, 'Bob')")
            .unwrap();

        let ren_col = session
            .execute_sql("ALTER TABLE items RENAME COLUMN name TO title")
            .unwrap();
        assert_eq!(ren_col.tag, "ALTER TABLE");
        let schema = session.engine().table_schema("items").unwrap();
        assert!(schema.columns.iter().any(|c| c.name == "title"));
        assert!(schema.columns.iter().all(|c| c.name != "name"));

        let sel = session
            .execute_sql("SELECT id, title FROM items ORDER BY id")
            .unwrap();
        assert_eq!(sel.rows.len(), 2);
        assert_eq!(sel.rows[0].get("title"), Some("Ada"));
        assert_eq!(sel.rows[1].get("title"), Some("Bob"));

        let ren_pk = session
            .execute_sql("ALTER TABLE items RENAME COLUMN id TO item_id")
            .unwrap();
        assert_eq!(ren_pk.tag, "ALTER TABLE");
        let schema = session.engine().table_schema("items").unwrap();
        assert_eq!(schema.primary_key, "item_id");
        let by_pk = session
            .execute_sql("SELECT title FROM items WHERE item_id = 1")
            .unwrap();
        assert_eq!(by_pk.rows[0].get("title"), Some("Ada"));

        let ren_tbl = session
            .execute_sql("ALTER TABLE items RENAME TO products")
            .unwrap();
        assert_eq!(ren_tbl.tag, "ALTER TABLE");
        assert!(session.engine().table_schema("items").is_err());
        let products = session
            .execute_sql("SELECT item_id, title FROM products ORDER BY item_id")
            .unwrap();
        assert_eq!(products.rows.len(), 2);
        assert_eq!(products.rows[0].get("title"), Some("Ada"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_alter_column_type_e2e() {
        let (mut session, root) = temp_session("alter-col-type");
        session
            .execute_sql("CREATE TABLE items (id BIGINT PRIMARY KEY, qty INT, note TEXT)")
            .unwrap();
        session
            .execute_sql("INSERT INTO items (id, qty, note) VALUES (1, 10, 'a')")
            .unwrap();

        let alt = session
            .execute_sql("ALTER TABLE items ALTER COLUMN qty TYPE BIGINT")
            .unwrap();
        assert_eq!(alt.tag, "ALTER TABLE");
        let schema = session.engine().table_schema("items").unwrap();
        let qty = schema.columns.iter().find(|c| c.name == "qty").unwrap();
        assert_eq!(qty.data_type, "BIGINT");

        let set = session
            .execute_sql("ALTER TABLE items ALTER COLUMN note SET DATA TYPE VARCHAR")
            .unwrap();
        assert_eq!(set.tag, "ALTER TABLE");
        let schema = session.engine().table_schema("items").unwrap();
        let note = schema.columns.iter().find(|c| c.name == "note").unwrap();
        assert_eq!(note.data_type, "TEXT");

        let row = session
            .execute_sql("SELECT qty, note FROM items WHERE id = 1")
            .unwrap();
        assert_eq!(row.rows[0].get("qty"), Some("10"));
        assert_eq!(row.rows[0].get("note"), Some("a"));

        assert!(session
            .execute_sql("ALTER TABLE items ALTER COLUMN missing TYPE INT")
            .unwrap_err()
            .to_string()
            .contains("does not exist"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_select_projection_returns_only_named_columns() {
        let (mut session, root) = temp_session("proj");
        session
            .execute_sql(
                "CREATE TABLE items (id BIGINT PRIMARY KEY, name TEXT, qty INT)",
            )
            .unwrap();
        session
            .execute_sql("INSERT INTO items (id, name, qty) VALUES (1, 'widget', 10)")
            .unwrap();

        let sel = session
            .execute_sql("SELECT name, qty FROM items WHERE id = 1")
            .unwrap();
        assert_eq!(sel.rows.len(), 1);
        assert_eq!(sel.rows[0].get("name"), Some("widget"));
        assert_eq!(sel.rows[0].get("qty"), Some("10"));
        assert!(sel.rows[0].get("id").is_none(), "id must be projected away");

        let aliased = session
            .execute_sql("SELECT name AS n FROM items")
            .unwrap();
        assert_eq!(aliased.rows[0].get("n"), Some("widget"));
        assert!(aliased.rows[0].get("name").is_none());

        let explain = session
            .execute_sql("EXPLAIN SELECT name FROM items")
            .unwrap();
        let plan = explain.rows[0].get("QUERY PLAN").unwrap();
        assert!(
            plan.contains("Project(name)"),
            "EXPLAIN should show Project, got {plan}"
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_create_table_then_insert_select_and_drop() {
        let (mut session, root) = temp_session("create-table");

        let create = session
            .execute_sql(
                "CREATE TABLE items (id BIGINT PRIMARY KEY, name TEXT, qty INT)",
            )
            .unwrap();
        assert_eq!(create.tag, "CREATE TABLE");

        let schema = session.engine().table_schema("items").unwrap();
        assert_eq!(schema.primary_key, "id");
        assert_eq!(schema.columns.len(), 3);
        assert_eq!(schema.columns[0].data_type, "BIGINT");

        session
            .execute_sql("INSERT INTO items (id, name, qty) VALUES (1, 'widget', 10)")
            .unwrap();
        let sel = session
            .execute_sql("SELECT * FROM items WHERE id = 1")
            .unwrap();
        assert_eq!(sel.rows.len(), 1);
        assert_eq!(sel.rows[0].get("name"), Some("widget"));

        let drop = session.execute_sql("DROP TABLE items").unwrap();
        assert_eq!(drop.tag, "DROP TABLE");
        assert!(session.engine().table_schema("items").is_err());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_create_table_as_select_e2e() {
        let (mut session, root) = temp_session("ctas");
        session
            .execute_sql(
                "INSERT INTO users (id, name, age) VALUES \
                 (1, 'Ada', 30), (2, 'Bob', 20), (3, 'Cy', 25)",
            )
            .unwrap();

        let create = session
            .execute_sql(
                "CREATE TABLE adults AS SELECT id, name FROM users WHERE age >= 25",
            )
            .unwrap();
        assert_eq!(create.tag, "CREATE TABLE");

        let schema = session.engine().table_schema("adults").unwrap();
        assert_eq!(schema.primary_key, "id");
        assert_eq!(schema.columns.len(), 2);

        let rows = session
            .execute_sql("SELECT id, name FROM adults ORDER BY id")
            .unwrap();
        assert_eq!(rows.rows.len(), 2);
        assert_eq!(rows.rows[0].get("name"), Some("Ada"));
        assert_eq!(rows.rows[1].get("name"), Some("Cy"));

        let renamed = session
            .execute_sql(
                "CREATE TABLE kids (kid_id TEXT, kid_name TEXT) AS \
                 SELECT id, name FROM users WHERE age < 25",
            )
            .unwrap();
        assert_eq!(renamed.tag, "CREATE TABLE");
        let kid_schema = session.engine().table_schema("kids").unwrap();
        assert_eq!(kid_schema.primary_key, "kid_id");
        let kids = session
            .execute_sql("SELECT kid_id, kid_name FROM kids")
            .unwrap();
        assert_eq!(kids.rows.len(), 1);
        assert_eq!(kids.rows[0].get("kid_name"), Some("Bob"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_insert_select_e2e() {
        let (mut session, root) = temp_session("insert-select");
        session
            .execute_sql(
                "CREATE TABLE dest (id BIGINT PRIMARY KEY, name TEXT)",
            )
            .unwrap();
        session
            .execute_sql(
                "INSERT INTO users (id, name, age) VALUES \
                 (1, 'Ada', 30), (2, 'Bob', 20), (3, 'Cy', 25)",
            )
            .unwrap();

        let ins = session
            .execute_sql(
                "INSERT INTO dest (id, name) SELECT id, name FROM users WHERE age >= 25",
            )
            .unwrap();
        assert_eq!(ins.tag, "INSERT");
        assert_eq!(ins.affected, Some(2));

        let rows = session
            .execute_sql("SELECT id, name FROM dest ORDER BY id")
            .unwrap();
        assert_eq!(rows.rows.len(), 2);
        assert_eq!(rows.rows[0].get("name"), Some("Ada"));
        assert_eq!(rows.rows[1].get("name"), Some("Cy"));

        let ret = session
            .execute_sql(
                "INSERT INTO dest (id, name) SELECT id, name FROM users WHERE id = 2 \
                 RETURNING id, name",
            )
            .unwrap();
        assert_eq!(ret.rows.len(), 1);
        assert_eq!(ret.rows[0].get("name"), Some("Bob"));

        let skip = session
            .execute_sql(
                "INSERT INTO dest (id, name) SELECT id, name FROM users WHERE id = 1 \
                 ON CONFLICT DO NOTHING",
            )
            .unwrap();
        assert_eq!(skip.affected, Some(0));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_create_index_explain_index_scan() {
        let (mut session, root) = temp_session("sec-idx");
        session
            .engine()
            .register_table(TableSchema::new("employees", "id", vec![]))
            .unwrap();

        session
            .execute_sql(
                "INSERT INTO employees (id, department, salary) VALUES \
                 (1, 'Sales', 5000), (2, 'Sales', 7000), (3, 'Engineering', 9000)",
            )
            .unwrap();

        let create = session
            .execute_sql("CREATE INDEX idx_dept ON employees(department)")
            .unwrap();
        assert_eq!(create.tag, "CREATE INDEX");

        let schema = session.engine().table_schema("employees").unwrap();
        assert_eq!(schema.indexes.len(), 1);
        assert_eq!(schema.indexes[0].name, "idx_dept");

        let explain = session
            .execute_sql("EXPLAIN SELECT * FROM employees WHERE department = 'Engineering'")
            .unwrap();
        assert_eq!(explain.tag, "SELECT");
        assert_eq!(explain.rows.len(), 1);
        let plan_text = explain.rows[0].get("QUERY PLAN").unwrap_or("");
        assert!(
            plan_text.contains("IndexScan(idx_dept)"),
            "CBO must choose secondary IndexScan, got:\n{plan_text}"
        );
        assert!(
            !plan_text.contains("TableScan(employees)"),
            "must not fall back to TableScan when index is cheaper, got:\n{plan_text}"
        );

        let result = session
            .execute_sql("SELECT * FROM employees WHERE department = 'Engineering'")
            .unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("id"), Some("3"));
        assert_eq!(result.rows[0].get("department"), Some("Engineering"));
        assert_eq!(result.rows[0].get("salary"), Some("9000"));

        let drop = session.execute_sql("DROP INDEX idx_dept").unwrap();
        assert_eq!(drop.tag, "DROP INDEX");
        assert!(session.engine().table_schema("employees").unwrap().indexes.is_empty());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn crash_abandon_recovers_committed_sql_from_wal() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("takyonic-pg-aries-{nanos}"));
        let data_dir = root.join("data");
        let wal_dir = root.join("wal");
        let config = Config::default()
            .data_dir(&data_dir)
            .wal_dir(&wal_dir)
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

        {
            let engine = Arc::new(TakyonicEngine::open(config).unwrap());
            engine
                .register_table(TableSchema::new("employees", "id", vec![]))
                .unwrap();
            let mut session = SessionState::new(Arc::clone(&engine));
            session
                .execute_sql(
                    "INSERT INTO employees (id, department, salary) VALUES \
                     (1, 'Sales', 5000), (3, 'Engineering', 9000)",
                )
                .unwrap();
            session
                .execute_sql(
                    "UPDATE employees SET salary = 9500 WHERE id = 3",
                )
                .unwrap();
            // Crash before graceful close / SST flush.
            engine.abandon_for_crash_test().unwrap();
            std::mem::forget(session);
            std::mem::forget(engine);
        }

        let engine = Arc::new(
            TakyonicEngine::open(
                Config::default()
                    .data_dir(&data_dir)
                    .wal_dir(&wal_dir)
                    .memtable_size_bytes(64 * 1024 * 1024)
                    .l0_rapid_pool_threads(1)
                    .ln_haul_pool_threads(1)
                    .compaction_write_bytes_per_sec(1024 * 1024 * 1024),
            )
            .unwrap(),
        );
        let mut session = SessionState::new(engine);
        let result = session
            .execute_sql("SELECT * FROM employees WHERE department = 'Engineering'")
            .unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("id"), Some("3"));
        assert_eq!(result.rows[0].get("salary"), Some("9500"));

        let all = session.execute_sql("SELECT * FROM employees").unwrap();
        assert_eq!(all.rows.len(), 2);

        session.engine().close().unwrap();
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn cte_in_subquery_unnests_to_hash_semi_join() {
        let (mut session, root) = temp_session("cte-semi");
        session
            .engine()
            .register_table(TableSchema::new("employees", "id", vec![]))
            .unwrap();
        session
            .execute_sql(
                "INSERT INTO employees (id, department, salary) VALUES \
                 (1, 'Sales', 5000), (2, 'Sales', 7000), (3, 'Engineering', 9000), \
                 (4, 'HR', 4000)",
            )
            .unwrap();

        let sql = "WITH top_depts AS (\
            SELECT department FROM employees GROUP BY department LIMIT 2\
        ) SELECT * FROM employees WHERE department IN (SELECT department FROM top_depts)";

        let explain = session.execute_sql(&format!("EXPLAIN {sql}")).unwrap();
        let plan_text = explain.rows[0].get("QUERY PLAN").unwrap_or("");
        assert!(
            plan_text.contains("HashSemiJoin"),
            "CBO must unnest IN subquery to HashSemiJoin, got:\n{plan_text}"
        );

        let result = session.execute_sql(sql).unwrap();
        assert!(
            result.rows.len() >= 2,
            "expected rows from top 2 departments, got {result:?}"
        );
        // LIMIT 2 on grouped departments is order-dependent; every returned
        // department must appear in the result set consistently.
        let depts: std::collections::HashSet<_> = result
            .rows
            .iter()
            .filter_map(|r| r.get("department").map(str::to_string))
            .collect();
        assert!(
            depts.len() <= 2,
            "IN (LIMIT 2 groups) must restrict to ≤2 departments, got {depts:?}"
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_analyze_explain_switches_plan_on_skew() {
        let (mut session, root) = temp_session("analyze-skew");
        session
            .engine()
            .register_table(TableSchema::new("employees", "id", vec![]))
            .unwrap();

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
        session.execute_sql(&values).unwrap();
        session
            .execute_sql("CREATE INDEX idx_dept ON employees(department)")
            .unwrap();

        let before = session
            .execute_sql("EXPLAIN SELECT * FROM employees WHERE department = 'Sales'")
            .unwrap();
        let before_text = before.rows[0].get("QUERY PLAN").unwrap_or("");
        assert!(
            before_text.contains("IndexScan(idx_dept)"),
            "pre-ANALYZE prefers index, got:\n{before_text}"
        );

        let analyze = session.execute_sql("ANALYZE employees").unwrap();
        assert_eq!(analyze.tag, "ANALYZE");

        let frequent = session
            .execute_sql("EXPLAIN SELECT * FROM employees WHERE department = 'Sales'")
            .unwrap();
        let freq_text = frequent.rows[0].get("QUERY PLAN").unwrap_or("");
        assert!(
            freq_text.contains("TableScan(employees)"),
            "after ANALYZE, frequent predicate → Full Scan, got:\n{freq_text}"
        );

        let rare = session
            .execute_sql("EXPLAIN SELECT * FROM employees WHERE department = 'Engineering'")
            .unwrap();
        let rare_text = rare.rows[0].get("QUERY PLAN").unwrap_or("");
        assert!(
            rare_text.contains("IndexScan(idx_dept)"),
            "after ANALYZE, rare predicate → IndexScan, got:\n{rare_text}"
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_vacuum_reclaims_dead_versions_after_updates() {
        let (mut session, root) = temp_session("vacuum-e2e");
        session
            .engine()
            .register_table(TableSchema::new("items", "id", vec![]))
            .unwrap();

        const N: usize = 10_000;
        // Bulk load in chunks so admission burst is not exceeded.
        {
            let engine = session.engine();
            for chunk_start in (1..=N).step_by(200) {
                let chunk_end = (chunk_start + 199).min(N);
                let mut txn = engine.begin().unwrap();
                for i in chunk_start..=chunk_end {
                    txn.put_record(
                        "items",
                        Record::new()
                            .set("id", i.to_string())
                            .set("status", "old")
                            .set("payload", format!("x{i}")),
                    )
                    .unwrap();
                }
                txn.commit().unwrap();
            }
        }
        session
            .execute_sql("CREATE INDEX idx_status ON items(status)")
            .unwrap();
        session.engine().force_flush().unwrap();

        let versions_before_update = session.engine().table_version_count("items").unwrap();
        let bytes_before_update = session.engine().sst_total_bytes();

        {
            let engine = session.engine();
            for chunk_start in (1..=N).step_by(200) {
                let chunk_end = (chunk_start + 199).min(N);
                let mut txn = engine.begin().unwrap();
                for i in chunk_start..=chunk_end {
                    txn.put_record(
                        "items",
                        Record::new()
                            .set("id", i.to_string())
                            .set("status", "new")
                            .set("payload", format!("y{i}")),
                    )
                    .unwrap();
                }
                txn.commit().unwrap();
            }
        }
        session.engine().force_flush().unwrap();

        let versions_bloated = session.engine().table_version_count("items").unwrap();
        let bytes_bloated = session.engine().sst_total_bytes();
        assert!(
            versions_bloated > versions_before_update,
            "updates must create dead versions: before={versions_before_update} after={versions_bloated}"
        );
        assert!(
            bytes_bloated >= bytes_before_update,
            "SST bytes should not shrink before VACUUM"
        );

        let vac = session.execute_sql("VACUUM items").unwrap();
        assert_eq!(vac.tag, "VACUUM");
        assert!(!vac.rows.is_empty());
        let dead_heap: u64 = vac.rows[0]
            .get("dead_heap")
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let dead_index: u64 = vac.rows[0]
            .get("dead_index")
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let removed: u64 = vac.rows[0]
            .get("removed")
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        assert!(
            dead_heap + dead_index > 0 || removed > 0 || versions_bloated > session.engine().table_version_count("items").unwrap(),
            "VACUUM must identify or reclaim dead versions (dead_heap={dead_heap} dead_index={dead_index} removed={removed})"
        );

        let versions_after = session.engine().table_version_count("items").unwrap();
        let bytes_after = session.engine().sst_total_bytes();
        assert!(
            versions_after < versions_bloated,
            "VACUUM must reclaim versions: bloated={versions_bloated} after={versions_after}"
        );
        assert!(
            bytes_after <= bytes_bloated,
            "VACUUM must not grow SST footprint: bloated={bytes_bloated} after={bytes_after}"
        );

        let live = session
            .execute_sql("SELECT * FROM items WHERE id = 1")
            .unwrap();
        assert_eq!(live.rows.len(), 1);
        assert_eq!(live.rows[0].get("status"), Some("new"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn vacuum_respects_long_running_snapshot() {
        let (mut session, root) = temp_session("vacuum-si");
        session
            .engine()
            .register_table(TableSchema::new("kv", "id", vec![]))
            .unwrap();
        session
            .execute_sql("INSERT INTO kv (id, v) VALUES (1, 'v1')")
            .unwrap();

        // Long-running reader pins the watermark at the insert snapshot.
        let mut reader = session.engine().begin().unwrap();
        let seen = reader
            .get_record("kv", "1")
            .unwrap()
            .expect("row visible to reader");
        assert_eq!(seen.get("v"), Some("v1"));

        // Concurrent writer creates a new version.
        session
            .execute_sql("UPDATE kv SET v = 'v2' WHERE id = 1")
            .unwrap();

        // VACUUM must not drop the version the reader still needs.
        let vac = session.execute_sql("VACUUM kv").unwrap();
        assert_eq!(vac.tag, "VACUUM");
        let still = reader
            .get_record("kv", "1")
            .unwrap()
            .expect("snapshot must still see v1");
        assert_eq!(still.get("v"), Some("v1"));

        // After the reader ends, VACUUM can reclaim the old version.
        reader.abort();
        session.execute_sql("VACUUM kv").unwrap();
        let versions = session.engine().table_version_count("kv").unwrap();
        // Only the live row version should remain (possibly plus floor tombstones).
        assert!(
            versions <= 2,
            "after reader ends, old version should be GC'd; versions={versions}"
        );

        let live = session
            .execute_sql("SELECT * FROM kv WHERE id = 1")
            .unwrap();
        assert_eq!(live.rows[0].get("v"), Some("v2"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_bpm_caches_sst_reads_after_flush() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("takyonic-pg-bpm-{nanos}"));
        let config = Config::default()
            .data_dir(root.join("data"))
            .wal_dir(root.join("wal"))
            .memtable_size_bytes(4 * 1024) // force early flush
            .block_size_bytes(64)
            .l0_soft_limit(8)
            .l0_hard_limit(32)
            .l0_rapid_pool_threads(1)
            .ln_haul_pool_threads(1)
            .compaction_write_bytes_per_sec(1024 * 1024 * 1024)
            .write_admission_ops_per_sec(100_000)
            .write_admission_min_ops_per_sec(1_000)
            .write_admission_burst(10_000)
            .bpm_pool_size(64)
            .bpm_page_size(4096)
            .bpm_lru_k(2);
        let engine = Arc::new(TakyonicEngine::open(config).unwrap());
        assert!(engine.buffer_pool().is_some());
        engine
            .register_table(TableSchema::new("items", "id", vec![]))
            .unwrap();
        let mut session = SessionState::new(Arc::clone(&engine));

        for i in 1..=200 {
            session
                .execute_sql(&format!(
                    "INSERT INTO items (id, v) VALUES ({i}, 'payload-{i}')"
                ))
                .unwrap();
        }
        engine.force_flush().unwrap();

        let bpm = engine.buffer_pool().unwrap();
        let before = bpm.stats();
        let t0 = std::time::Instant::now();
        for _ in 0..3 {
            let rows = session.execute_sql("SELECT * FROM items WHERE id = 1").unwrap();
            assert_eq!(rows.rows.len(), 1);
        }
        let elapsed = t0.elapsed();
        let after = bpm.stats();
        assert!(
            after.misses > before.misses || after.hits > before.hits,
            "BPM should observe SST page traffic: before={before:?} after={after:?}"
        );
        // Latency smoke: three point lookups should complete quickly with the pool.
        assert!(
            elapsed.as_millis() < 5_000,
            "BPM-backed lookups too slow: {elapsed:?}"
        );

        engine.close().unwrap();
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn decode_text_int_parameter() {
        let raw = vec![Some(bytes::Bytes::from_static(b"25"))];
        let vals =
            decode_bind_parameters(&raw, &[Some(Type::INT4)], &pgwire::api::portal::Format::UnifiedText)
                .unwrap();
        assert_eq!(vals, vec![Value::Int(25)]);
    }

    #[test]
    fn session_jit_olap_sum_filter_via_sql() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("takyonic-pg-jit-{nanos}"));
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
        engine
            .register_table(TableSchema::new("employees", "id", Vec::new()))
            .unwrap();
        let mut session = SessionState::new(engine);
        session
            .execute_sql(
                "INSERT INTO employees (id, age, salary, tax_rate) VALUES \
                 (1, 25, 90, 1), (2, 35, 100, 2), (3, 40, 200, 3), (4, 28, 80, 1), (5, 50, 150, 2)",
            )
            .unwrap();
        let explain = session
            .execute_sql("EXPLAIN SELECT SUM(salary * tax_rate) FROM employees WHERE age > 30")
            .unwrap();
        let plan_text = explain.rows[0].get("QUERY PLAN").unwrap_or("");
        assert!(
            plan_text.contains("JitExec"),
            "EXPLAIN must show JitExec, got:\n{plan_text}"
        );
        let result = session
            .execute_sql("SELECT SUM(salary * tax_rate) FROM employees WHERE age > 30")
            .unwrap();
        assert_eq!(result.rows.len(), 1);
        let sum = result.rows[0]
            .fields
            .values()
            .next()
            .expect("sum value");
        assert_eq!(sum, "1100");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_vector_index_hnsw_explain_and_knn() {
        let (mut session, root) = temp_session("vec-hnsw");
        session
            .engine()
            .register_table(TableSchema::new("docs", "id", vec![]))
            .unwrap();

        // Five unit-ish 128-d embeddings with distinct peaks.
        fn emb(peak_at: usize) -> String {
            let mut v = vec![0.0f32; 128];
            v[peak_at] = 1.0;
            v[peak_at.saturating_add(1) % 128] = 0.1;
            format!(
                "[{}]",
                v.iter()
                    .map(|f| format!("{f}"))
                    .collect::<Vec<_>>()
                    .join(",")
            )
        }
        for (id, peak) in [(1usize, 0usize), (2, 10), (3, 40), (4, 80), (5, 120)] {
            let sql = format!(
                "INSERT INTO docs (id, title, vec) VALUES ({id}, 'doc{id}', '{}')",
                emb(peak)
            );
            session.execute_sql(&sql).unwrap();
        }

        let create = session
            .execute_sql(
                "CREATE VECTOR INDEX idx_v ON docs(vec) WITH (DIMENSION=128, TYPE=HNSW)",
            )
            .unwrap();
        assert_eq!(create.tag, "CREATE VECTOR INDEX");
        assert!(session.engine().hnsw_index("idx_v").is_some());
        assert_eq!(session.engine().hnsw_index("idx_v").unwrap().len(), 5);

        // Query near peak index 40 → expect doc3 first.
        let q = emb(40);
        let array_lit = format!(
            "ARRAY[{}]",
            q.trim_start_matches('[').trim_end_matches(']')
        );
        let explain_sql = format!(
            "EXPLAIN SELECT * FROM docs ORDER BY vec <-> {array_lit} LIMIT 5"
        );
        let explain = session.execute_sql(&explain_sql).unwrap();
        let plan_text = explain.rows[0].get("QUERY PLAN").unwrap_or("");
        assert!(
            plan_text.contains("VectorIndexScanExec"),
            "EXPLAIN must show VectorIndexScanExec, got:\n{plan_text}"
        );

        let select_sql = format!(
            "SELECT * FROM docs ORDER BY vec <-> {array_lit} LIMIT 2"
        );
        let result = session.execute_sql(&select_sql).unwrap();
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0].get("id"), Some("3"));
        assert_eq!(result.rows[0].get("title"), Some("doc3"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn two_pids_on_same_backend_do_not_share_active_txn() {
        let root = std::env::temp_dir().join(format!(
            "takyonic-sess-iso-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let config = Config::default()
            .data_dir(root.join("data"))
            .wal_dir(root.join("wal"));
        let engine = Arc::new(crate::engine::TakyonicEngine::open(config).unwrap());
        let backend = TakyonicPgBackend::new(
            TakyonicClient::new(std::iter::empty::<String>()),
            Arc::clone(&engine),
        );
        // Seed two connection pids on the same shared backend (factory pattern).
        backend.sessions.insert(
            101,
            Arc::new(Mutex::new(SessionState::new(Arc::clone(&engine)))),
        );
        backend.sessions.insert(
            202,
            Arc::new(Mutex::new(SessionState::new(Arc::clone(&engine)))),
        );

        backend
            .session_arc_for_pid(101)
            .lock()
            .execute_sql("BEGIN")
            .unwrap();
        assert_eq!(
            backend.session_arc_for_pid(101).lock().txn_mode(),
            SessionTxnMode::InTransaction
        );
        assert_eq!(
            backend.session_arc_for_pid(202).lock().txn_mode(),
            SessionTxnMode::Idle,
            "second connection must not inherit first connection's txn"
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn backend_binds_authenticated_user_not_bootstrap() {
        let (mut admin, root) = temp_session("rbac-bind");
        admin
            .engine()
            .register_table(TableSchema::new("employees", "id", vec![]))
            .unwrap();
        admin
            .execute_sql("CREATE USER analyst WITH PASSWORD 'secret'")
            .unwrap();
        admin
            .execute_sql("GRANT SELECT ON employees TO analyst")
            .unwrap();
        let engine = Arc::clone(admin.engine());
        let backend = TakyonicPgBackend::new_for_test(engine, 7, "analyst");
        assert_eq!(
            backend.session_arc_for_pid(7).lock().current_user(),
            "analyst"
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_rbac_analyst_select_ok_delete_denied() {
        let (mut admin, root) = temp_session("rbac-e2e");
        admin
            .engine()
            .register_table(TableSchema::new("employees", "id", vec![]))
            .unwrap();
        admin
            .execute_sql(
                "INSERT INTO employees (id, name, department) VALUES \
                 (1, 'Ada', 'Engineering'), (2, 'Grace', 'Sales')",
            )
            .unwrap();

        let create = admin
            .execute_sql("CREATE USER analyst WITH PASSWORD 'secret'")
            .unwrap();
        assert_eq!(create.tag, "CREATE USER");

        let grant = admin
            .execute_sql("GRANT SELECT ON employees TO analyst")
            .unwrap();
        assert_eq!(grant.tag, "GRANT");

        // Analyst may SELECT.
        let mut analyst = SessionState::as_user(Arc::clone(admin.engine()), "analyst").unwrap();
        assert_eq!(analyst.current_user(), "analyst");
        let sel = analyst
            .execute_sql("SELECT * FROM employees WHERE id = 1")
            .unwrap();
        assert_eq!(sel.rows.len(), 1);
        assert_eq!(sel.rows[0].get("name"), Some("Ada"));

        // Analyst may NOT DELETE.
        let denied = analyst.execute_sql("DELETE FROM employees WHERE id = 1");
        assert!(
            matches!(
                denied,
                Err(crate::error::TakyonicError::PermissionDenied(_))
            ),
            "expected PermissionDenied, got {denied:?}"
        );

        // Superuser still can DELETE.
        let deleted = admin
            .execute_sql("DELETE FROM employees WHERE id = 1")
            .unwrap();
        assert_eq!(deleted.tag, "DELETE");
        assert_eq!(deleted.affected, Some(1));

        // VACUUM requires SUPERUSER — analyst denied.
        let vac = analyst.execute_sql("VACUUM employees");
        assert!(matches!(
            vac,
            Err(crate::error::TakyonicError::PermissionDenied(_))
        ));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_has_table_privilege_e2e() {
        let (mut admin, root) = temp_session("has-table-priv");
        admin
            .engine()
            .register_table(TableSchema::new("employees", "id", vec![]))
            .unwrap();
        admin
            .execute_sql("INSERT INTO employees (id, name) VALUES (1, 'Ada')")
            .unwrap();
        admin
            .execute_sql("CREATE USER analyst WITH PASSWORD 'secret'")
            .unwrap();
        admin
            .execute_sql("GRANT SELECT ON employees TO analyst")
            .unwrap();

        let mut analyst = SessionState::as_user(Arc::clone(admin.engine()), "analyst").unwrap();
        let row = analyst
            .execute_sql(
                "SELECT has_table_privilege('employees', 'SELECT') AS sel, \
                 has_table_privilege('employees', 'DELETE') AS del, \
                 has_table_privilege('analyst', 'employees', 'SELECT') AS u_sel \
                 FROM employees WHERE id = 1",
            )
            .unwrap();
        assert_eq!(row.rows[0].get("sel"), Some("true"));
        assert_eq!(row.rows[0].get("del"), Some("false"));
        assert_eq!(row.rows[0].get("u_sel"), Some("true"));

        let su = admin
            .execute_sql(
                "SELECT has_table_privilege('employees', 'DELETE') AS d FROM employees WHERE id = 1",
            )
            .unwrap();
        assert_eq!(su.rows[0].get("d"), Some("true"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_has_column_privilege_e2e() {
        let (mut admin, root) = temp_session("has-col-priv");
        admin
            .engine()
            .register_table(TableSchema::new("employees", "id", vec![]))
            .unwrap();
        admin
            .execute_sql("INSERT INTO employees (id, name) VALUES (1, 'Ada')")
            .unwrap();
        admin
            .execute_sql("CREATE USER analyst WITH PASSWORD 'secret'")
            .unwrap();
        admin
            .execute_sql("GRANT SELECT ON employees TO analyst")
            .unwrap();

        let mut analyst = SessionState::as_user(Arc::clone(admin.engine()), "analyst").unwrap();
        let row = analyst
            .execute_sql(
                "SELECT has_column_privilege('employees', 'name', 'SELECT') AS sel, \
                 has_column_privilege('employees', 'name', 'UPDATE') AS upd, \
                 has_column_privilege('analyst', 'employees', 'id', 'SELECT') AS u_sel \
                 FROM employees WHERE id = 1",
            )
            .unwrap();
        assert_eq!(row.rows[0].get("sel"), Some("true"));
        assert_eq!(row.rows[0].get("upd"), Some("false"));
        assert_eq!(row.rows[0].get("u_sel"), Some("true"));

        let su = admin
            .execute_sql(
                "SELECT has_column_privilege('employees', 'id', 'REFERENCES') AS r \
                 FROM employees WHERE id = 1",
            )
            .unwrap();
        assert_eq!(su.rows[0].get("r"), Some("true"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_grant_column_acl_e2e() {
        let (mut admin, root) = temp_session("grant-col-acl");
        admin
            .engine()
            .register_table(TableSchema::new("employees", "id", vec![]))
            .unwrap();
        admin
            .execute_sql("INSERT INTO employees (id, name) VALUES (1, 'Ada')")
            .unwrap();
        admin
            .execute_sql("CREATE USER analyst WITH PASSWORD 'secret'")
            .unwrap();

        let before = admin
            .execute_sql(
                "SELECT has_column_privilege('analyst', 'employees', 'name', 'UPDATE') AS u \
                 FROM employees WHERE id = 1",
            )
            .unwrap();
        assert_eq!(before.rows[0].get("u"), Some("false"));

        admin
            .execute_sql("GRANT UPDATE (name) ON employees TO analyst")
            .unwrap();
        let after = admin
            .execute_sql(
                "SELECT has_column_privilege('analyst', 'employees', 'name', 'UPDATE') AS name_u, \
                 has_column_privilege('analyst', 'employees', 'id', 'UPDATE') AS id_u, \
                 has_any_column_privilege('analyst', 'employees', 'UPDATE') AS any_u \
                 FROM employees WHERE id = 1",
            )
            .unwrap();
        assert_eq!(after.rows[0].get("name_u"), Some("true"));
        assert_eq!(after.rows[0].get("id_u"), Some("false"));
        assert_eq!(after.rows[0].get("any_u"), Some("true"));

        admin
            .execute_sql("REVOKE UPDATE (name) ON employees FROM analyst")
            .unwrap();
        let revoked = admin
            .execute_sql(
                "SELECT has_column_privilege('analyst', 'employees', 'name', 'UPDATE') AS u \
                 FROM employees WHERE id = 1",
            )
            .unwrap();
        assert_eq!(revoked.rows[0].get("u"), Some("false"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_has_any_column_privilege_e2e() {
        let (mut admin, root) = temp_session("has-any-col-priv");
        admin
            .engine()
            .register_table(TableSchema::new("employees", "id", vec![]))
            .unwrap();
        admin
            .execute_sql("INSERT INTO employees (id, name) VALUES (1, 'Ada')")
            .unwrap();
        admin
            .execute_sql("CREATE USER analyst WITH PASSWORD 'secret'")
            .unwrap();
        admin
            .execute_sql("GRANT SELECT ON employees TO analyst")
            .unwrap();

        let mut analyst = SessionState::as_user(Arc::clone(admin.engine()), "analyst").unwrap();
        let row = analyst
            .execute_sql(
                "SELECT has_any_column_privilege('employees', 'SELECT') AS sel, \
                 has_any_column_privilege('employees', 'UPDATE') AS upd, \
                 has_any_column_privilege('analyst', 'employees', 'SELECT') AS u_sel \
                 FROM employees WHERE id = 1",
            )
            .unwrap();
        assert_eq!(row.rows[0].get("sel"), Some("true"));
        assert_eq!(row.rows[0].get("upd"), Some("false"));
        assert_eq!(row.rows[0].get("u_sel"), Some("true"));

        let su = admin
            .execute_sql(
                "SELECT has_any_column_privilege('employees', 'REFERENCES') AS r \
                 FROM employees WHERE id = 1",
            )
            .unwrap();
        assert_eq!(su.rows[0].get("r"), Some("true"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_comment_obj_col_description_e2e() {
        let (mut session, root) = temp_session("comment-desc");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();

        let none = session
            .execute_sql(
                "SELECT obj_description('users') AS od, col_description('users', 'name') AS cd \
                 FROM users WHERE id = 1",
            )
            .unwrap();
        assert_eq!(none.rows[0].get("od"), Some(""));
        assert_eq!(none.rows[0].get("cd"), Some(""));

        let c = session
            .execute_sql("COMMENT ON TABLE users IS 'people table'")
            .unwrap();
        assert_eq!(c.tag, "COMMENT");
        session
            .execute_sql("COMMENT ON COLUMN users.name IS 'display name'")
            .unwrap();

        let row = session
            .execute_sql(
                "SELECT obj_description('users') AS od, col_description('users', 'name') AS cd \
                 FROM users WHERE id = 1",
            )
            .unwrap();
        assert_eq!(row.rows[0].get("od"), Some("people table"));
        assert_eq!(row.rows[0].get("cd"), Some("display name"));

        session
            .execute_sql("COMMENT ON TABLE users IS NULL")
            .unwrap();
        let cleared = session
            .execute_sql("SELECT obj_description('users') AS od FROM users WHERE id = 1")
            .unwrap();
        assert_eq!(cleared.rows[0].get("od"), Some(""));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_oid_obj_col_description_e2e() {
        let (mut session, root) = temp_session("oid-desc");
        session
            .execute_sql(
                "CREATE TABLE items (id BIGINT PRIMARY KEY, name TEXT, note TEXT)",
            )
            .unwrap();
        session
            .execute_sql("INSERT INTO items (id, name, note) VALUES (1, 'a', 'n')")
            .unwrap();
        session
            .execute_sql("COMMENT ON TABLE items IS 'stuff'")
            .unwrap();
        session
            .execute_sql("COMMENT ON COLUMN items.name IS 'label'")
            .unwrap();

        let row = session
            .execute_sql(
                "SELECT to_regclass('items') AS oid, \
                 obj_description(to_regclass('items')) AS od, \
                 obj_description(to_regclass('items'), 'pg_class') AS od2, \
                 col_description(to_regclass('items'), 2) AS cd, \
                 to_regclass('missing') AS miss \
                 FROM items WHERE id = 1",
            )
            .unwrap();
        let oid = row.rows[0].get("oid").unwrap();
        assert!(!oid.is_empty() && oid != "NULL", "oid={oid}");
        assert_eq!(row.rows[0].get("od"), Some("stuff"));
        assert_eq!(row.rows[0].get("od2"), Some("stuff"));
        assert_eq!(row.rows[0].get("cd"), Some("label"));
        assert_eq!(row.rows[0].get("miss"), Some(""));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_format_type_pg_get_userbyid_e2e() {
        let (mut session, root) = temp_session("format-type-userbyid");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();
        session
            .execute_sql("CREATE USER analyst WITH PASSWORD 'secret'")
            .unwrap();

        let row = session
            .execute_sql(
                "SELECT format_type(20, NULL) AS bi, \
                 format_type(1043, 54) AS vc, \
                 format_type(999999, NULL) AS unk, \
                 pg_get_userbyid(1) AS missing \
                 FROM users WHERE id = 1",
            )
            .unwrap();
        assert_eq!(row.rows[0].get("bi"), Some("bigint"));
        assert_eq!(row.rows[0].get("vc"), Some("character varying(50)"));
        assert_eq!(row.rows[0].get("unk"), Some("??? (999999)"));
        assert_eq!(row.rows[0].get("missing"), Some(""));

        let oid = crate::oid::role_oid("analyst");
        let byid = session
            .execute_sql(&format!(
                "SELECT pg_get_userbyid({oid}) AS u FROM users WHERE id = 1"
            ))
            .unwrap();
        assert_eq!(byid.rows[0].get("u"), Some("analyst"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_to_regrole_e2e() {
        let (mut session, root) = temp_session("to-regrole");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();
        session
            .execute_sql("CREATE USER analyst WITH PASSWORD 'secret'")
            .unwrap();

        let row = session
            .execute_sql(
                "SELECT to_regrole('analyst') AS oid, \
                 pg_get_userbyid(to_regrole('analyst')) AS name, \
                 to_regrole('missing') AS miss, \
                 to_regrole('postgres') IS NOT NULL AS has_pg \
                 FROM users WHERE id = 1",
            )
            .unwrap();
        let oid = row.rows[0].get("oid").unwrap();
        assert_eq!(oid, crate::oid::role_oid("analyst").to_string());
        assert_eq!(row.rows[0].get("name"), Some("analyst"));
        assert_eq!(row.rows[0].get("miss"), Some(""));
        assert_eq!(row.rows[0].get("has_pg"), Some("true"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_pg_relation_size_e2e() {
        let (mut session, root) = temp_session("pg-rel-size");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)")
            .unwrap();

        let row = session
            .execute_sql(
                "SELECT pg_relation_size('users') AS heap, \
                 pg_table_size('users') AS tbl, \
                 pg_total_relation_size('users') AS total, \
                 pg_relation_size(to_regclass('users')) AS by_oid, \
                 pg_relation_size('missing') AS miss \
                 FROM users WHERE id = 1",
            )
            .unwrap();
        let heap: i64 = row.rows[0].get("heap").unwrap().parse().unwrap();
        let tbl: i64 = row.rows[0].get("tbl").unwrap().parse().unwrap();
        let total: i64 = row.rows[0].get("total").unwrap().parse().unwrap();
        let by_oid: i64 = row.rows[0].get("by_oid").unwrap().parse().unwrap();
        assert!(heap > 0, "heap={heap}");
        assert_eq!(heap, tbl);
        assert!(total >= heap, "total={total} heap={heap}");
        assert_eq!(heap, by_oid);
        assert_eq!(row.rows[0].get("miss"), Some(""));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_pg_indexes_database_size_e2e() {
        let (mut session, root) = temp_session("pg-idx-db-size");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)")
            .unwrap();

        let row = session
            .execute_sql(
                "SELECT pg_indexes_size('users') AS idx, \
                 pg_relation_size('users') AS heap, \
                 pg_total_relation_size('users') AS total, \
                 pg_database_size('postgres') AS db, \
                 pg_database_size('missing') AS miss \
                 FROM users WHERE id = 1",
            )
            .unwrap();
        let idx: i64 = row.rows[0].get("idx").unwrap().parse().unwrap();
        let heap: i64 = row.rows[0].get("heap").unwrap().parse().unwrap();
        let total: i64 = row.rows[0].get("total").unwrap().parse().unwrap();
        let db: i64 = row.rows[0].get("db").unwrap().parse().unwrap();
        assert_eq!(idx, total - heap);
        assert!(db >= total, "db={db} total={total}");
        assert_eq!(row.rows[0].get("miss"), Some(""));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_to_regnamespace_regtype_e2e() {
        let (mut session, root) = temp_session("to-regns-type");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();

        let row = session
            .execute_sql(
                "SELECT to_regnamespace('public') AS ns, \
                 to_regnamespace('missing') AS ns_miss, \
                 to_regtype('bigint') AS bi, \
                 to_regtype('INT4') AS i4, \
                 format_type(to_regtype('varchar'), NULL) AS vc, \
                 to_regtype('nope') AS miss \
                 FROM users WHERE id = 1",
            )
            .unwrap();
        let ns: i64 = row.rows[0].get("ns").unwrap().parse().unwrap();
        assert_eq!(ns, i64::from(crate::oid::namespace_oid("public")));
        assert_eq!(row.rows[0].get("ns_miss"), Some(""));
        assert_eq!(row.rows[0].get("bi"), Some("20"));
        assert_eq!(row.rows[0].get("i4"), Some("23"));
        assert_eq!(row.rows[0].get("vc"), Some("character varying"));
        assert_eq!(row.rows[0].get("miss"), Some(""));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_shobj_description_e2e() {
        let (mut session, root) = temp_session("shobj-desc");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();
        session
            .execute_sql("CREATE USER analyst WITH PASSWORD 'secret'")
            .unwrap();
        session
            .execute_sql("COMMENT ON ROLE analyst IS 'read-only analyst'")
            .unwrap();
        session
            .execute_sql("COMMENT ON DATABASE postgres IS 'default db'")
            .unwrap();

        let row = session
            .execute_sql(
                "SELECT shobj_description('analyst', 'pg_authid') AS role_c, \
                 shobj_description('postgres', 'pg_database') AS db_c, \
                 shobj_description('missing', 'pg_authid') AS none \
                 FROM users WHERE id = 1",
            )
            .unwrap();
        assert_eq!(row.rows[0].get("role_c"), Some("read-only analyst"));
        assert_eq!(row.rows[0].get("db_c"), Some("default db"));
        assert_eq!(row.rows[0].get("none"), Some(""));

        let err = session
            .execute_sql(
                "SELECT shobj_description('x', 'pg_class') FROM users WHERE id = 1",
            )
            .unwrap_err();
        assert!(
            err.to_string().contains("unsupported shobj_description catalog"),
            "unexpected: {err}"
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_has_schema_privilege_e2e() {
        let (mut admin, root) = temp_session("has-schema-priv");
        admin
            .engine()
            .register_table(TableSchema::new("employees", "id", vec![]))
            .unwrap();
        admin
            .execute_sql("INSERT INTO employees (id, name) VALUES (1, 'Ada')")
            .unwrap();
        admin
            .execute_sql("CREATE USER analyst WITH PASSWORD 'secret'")
            .unwrap();
        admin
            .execute_sql("GRANT SELECT ON employees TO analyst")
            .unwrap();

        let mut analyst = SessionState::as_user(Arc::clone(admin.engine()), "analyst").unwrap();
        let row = analyst
            .execute_sql(
                "SELECT has_schema_privilege('public', 'USAGE') AS u, \
                 has_schema_privilege('public', 'CREATE') AS c, \
                 has_schema_privilege('analyst', 'public', 'USAGE') AS au \
                 FROM employees WHERE id = 1",
            )
            .unwrap();
        assert_eq!(row.rows[0].get("u"), Some("true"));
        assert_eq!(row.rows[0].get("c"), Some("false"));
        assert_eq!(row.rows[0].get("au"), Some("true"));

        let su = admin
            .execute_sql(
                "SELECT has_schema_privilege('public', 'CREATE') AS c FROM employees WHERE id = 1",
            )
            .unwrap();
        assert_eq!(su.rows[0].get("c"), Some("true"));

        let err = analyst
            .execute_sql(
                "SELECT has_schema_privilege('missing', 'USAGE') FROM employees WHERE id = 1",
            )
            .unwrap_err();
        assert!(
            err.to_string().contains("does not exist"),
            "unexpected: {err}"
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_grant_on_schema_e2e() {
        let (mut admin, root) = temp_session("grant-on-schema");
        admin
            .engine()
            .register_table(TableSchema::new("employees", "id", vec![]))
            .unwrap();
        admin
            .execute_sql("INSERT INTO employees (id, name) VALUES (1, 'Ada')")
            .unwrap();
        admin
            .execute_sql("CREATE USER analyst WITH PASSWORD 'secret'")
            .unwrap();
        admin
            .execute_sql("GRANT SELECT ON employees TO analyst")
            .unwrap();

        let mut analyst = SessionState::as_user(Arc::clone(admin.engine()), "analyst").unwrap();
        let before = analyst
            .execute_sql(
                "SELECT has_schema_privilege('public', 'CREATE') AS c FROM employees WHERE id = 1",
            )
            .unwrap();
        assert_eq!(before.rows[0].get("c"), Some("false"));

        admin
            .execute_sql("GRANT CREATE ON SCHEMA public TO analyst")
            .unwrap();
        let after = analyst
            .execute_sql(
                "SELECT has_schema_privilege('public', 'CREATE') AS c FROM employees WHERE id = 1",
            )
            .unwrap();
        assert_eq!(after.rows[0].get("c"), Some("true"));

        admin
            .execute_sql("REVOKE CREATE ON SCHEMA public FROM analyst")
            .unwrap();
        let revoked = analyst
            .execute_sql(
                "SELECT has_schema_privilege('public', 'CREATE') AS c FROM employees WHERE id = 1",
            )
            .unwrap();
        assert_eq!(revoked.rows[0].get("c"), Some("false"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_has_database_privilege_e2e() {
        let (mut admin, root) = temp_session("has-db-priv");
        admin
            .engine()
            .register_table(TableSchema::new("employees", "id", vec![]))
            .unwrap();
        admin
            .execute_sql("INSERT INTO employees (id, name) VALUES (1, 'Ada')")
            .unwrap();
        admin
            .execute_sql("CREATE USER analyst WITH PASSWORD 'secret'")
            .unwrap();
        admin
            .execute_sql("GRANT SELECT ON employees TO analyst")
            .unwrap();

        let mut analyst = SessionState::as_user(Arc::clone(admin.engine()), "analyst").unwrap();
        let row = analyst
            .execute_sql(
                "SELECT has_database_privilege('postgres', 'CONNECT') AS c, \
                 has_database_privilege('postgres', 'CREATE') AS cr, \
                 has_database_privilege('analyst', 'postgres', 'TEMP') AS t \
                 FROM employees WHERE id = 1",
            )
            .unwrap();
        assert_eq!(row.rows[0].get("c"), Some("true"));
        assert_eq!(row.rows[0].get("cr"), Some("false"));
        assert_eq!(row.rows[0].get("t"), Some("false"));

        let su = admin
            .execute_sql(
                "SELECT has_database_privilege('postgres', 'CREATE') AS cr FROM employees WHERE id = 1",
            )
            .unwrap();
        assert_eq!(su.rows[0].get("cr"), Some("true"));

        let err = analyst
            .execute_sql(
                "SELECT has_database_privilege('otherdb', 'CONNECT') FROM employees WHERE id = 1",
            )
            .unwrap_err();
        assert!(
            err.to_string().contains("does not exist"),
            "unexpected: {err}"
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_has_tablespace_privilege_e2e() {
        let (mut admin, root) = temp_session("has-tablespace-priv");
        admin
            .engine()
            .register_table(TableSchema::new("employees", "id", vec![]))
            .unwrap();
        admin
            .execute_sql("INSERT INTO employees (id, name) VALUES (1, 'Ada')")
            .unwrap();
        admin
            .execute_sql("CREATE USER analyst WITH PASSWORD 'secret'")
            .unwrap();
        admin
            .execute_sql("GRANT SELECT ON employees TO analyst")
            .unwrap();

        let mut analyst = SessionState::as_user(Arc::clone(admin.engine()), "analyst").unwrap();
        let row = analyst
            .execute_sql(
                "SELECT has_tablespace_privilege('pg_default', 'CREATE') AS c, \
                 has_tablespace_privilege('analyst', 'pg_default', 'ALL') AS u \
                 FROM employees WHERE id = 1",
            )
            .unwrap();
        assert_eq!(row.rows[0].get("c"), Some("false"));
        assert_eq!(row.rows[0].get("u"), Some("false"));

        let su = admin
            .execute_sql(
                "SELECT has_tablespace_privilege('pg_default', 'CREATE') AS c, \
                 has_tablespace_privilege('pg_global', 'CREATE') AS g \
                 FROM employees WHERE id = 1",
            )
            .unwrap();
        assert_eq!(su.rows[0].get("c"), Some("true"));
        assert_eq!(su.rows[0].get("g"), Some("true"));

        let err = analyst
            .execute_sql(
                "SELECT has_tablespace_privilege('no_such_ts', 'CREATE') FROM employees WHERE id = 1",
            )
            .unwrap_err();
        assert!(
            err.to_string().contains("does not exist"),
            "unexpected: {err}"
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_pg_tablespace_location_e2e() {
        let (mut session, root) = temp_session("pg-tablespace-location");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();

        let row = session
            .execute_sql(
                "SELECT pg_tablespace_location('pg_default') AS d, \
                 pg_tablespace_location('pg_global') AS g, \
                 pg_tablespace_location(1663) AS oid_d \
                 FROM users WHERE id = 1",
            )
            .unwrap();
        assert_eq!(row.rows[0].get("d"), Some(""));
        assert_eq!(row.rows[0].get("g"), Some(""));
        assert_eq!(row.rows[0].get("oid_d"), Some(""));

        let err = session
            .execute_sql(
                "SELECT pg_tablespace_location('no_such_ts') FROM users WHERE id = 1",
            )
            .unwrap_err();
        assert!(
            err.to_string().contains("does not exist"),
            "unexpected: {err}"
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_has_function_privilege_e2e() {
        let (mut admin, root) = temp_session("has-fn-priv");
        admin
            .engine()
            .register_table(TableSchema::new("employees", "id", vec![]))
            .unwrap();
        admin
            .execute_sql("INSERT INTO employees (id, name) VALUES (1, 'Ada')")
            .unwrap();
        admin
            .execute_sql("CREATE USER analyst WITH PASSWORD 'secret'")
            .unwrap();
        admin
            .execute_sql("GRANT SELECT ON employees TO analyst")
            .unwrap();

        let mut analyst = SessionState::as_user(Arc::clone(admin.engine()), "analyst").unwrap();
        let row = analyst
            .execute_sql(
                "SELECT has_function_privilege('format_type', 'EXECUTE') AS ok, \
                 has_function_privilege('format_type(regtype,integer)', 'EXECUTE') AS sig, \
                 has_function_privilege('nope_fn', 'EXECUTE') AS bad, \
                 has_function_privilege('analyst', 'lower', 'EXECUTE') AS u_ok \
                 FROM employees WHERE id = 1",
            )
            .unwrap();
        assert_eq!(row.rows[0].get("ok"), Some("true"));
        assert_eq!(row.rows[0].get("sig"), Some("true"));
        assert_eq!(row.rows[0].get("bad"), Some("false"));
        assert_eq!(row.rows[0].get("u_ok"), Some("true"));

        let su = admin
            .execute_sql(
                "SELECT has_function_privilege('nope_fn', 'EXECUTE') AS any \
                 FROM employees WHERE id = 1",
            )
            .unwrap();
        assert_eq!(su.rows[0].get("any"), Some("true"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_pg_has_role_e2e() {
        let (mut admin, root) = temp_session("pg-has-role");
        admin
            .engine()
            .register_table(TableSchema::new("employees", "id", vec![]))
            .unwrap();
        admin
            .execute_sql("INSERT INTO employees (id, name) VALUES (1, 'Ada')")
            .unwrap();
        admin
            .execute_sql("CREATE ROLE analysts")
            .unwrap();
        admin
            .execute_sql("CREATE USER analyst WITH PASSWORD 'secret'")
            .unwrap();
        admin
            .execute_sql("GRANT SELECT ON employees TO analyst")
            .unwrap();

        let mut analyst = SessionState::as_user(Arc::clone(admin.engine()), "analyst").unwrap();
        let before = analyst
            .execute_sql(
                "SELECT pg_has_role('analysts', 'MEMBER') AS m, \
                 pg_has_role('analyst', 'SET') AS self_ok \
                 FROM employees WHERE id = 1",
            )
            .unwrap();
        assert_eq!(before.rows[0].get("m"), Some("false"));
        assert_eq!(before.rows[0].get("self_ok"), Some("true"));

        admin.execute_sql("GRANT analysts TO analyst").unwrap();
        let after = analyst
            .execute_sql(
                "SELECT pg_has_role('analysts', 'MEMBER') AS m, \
                 pg_has_role('analysts', 'USAGE') AS u, \
                 pg_has_role('analyst', 'analysts', 'SET') AS named, \
                 pg_has_role(to_regrole('analyst'), to_regrole('analysts'), 'MEMBER') AS oids \
                 FROM employees WHERE id = 1",
            )
            .unwrap();
        assert_eq!(after.rows[0].get("m"), Some("true"));
        assert_eq!(after.rows[0].get("u"), Some("true"));
        assert_eq!(after.rows[0].get("named"), Some("true"));
        assert_eq!(after.rows[0].get("oids"), Some("true"));

        let su = admin
            .execute_sql(
                "SELECT pg_has_role('analysts', 'MEMBER') AS any \
                 FROM employees WHERE id = 1",
            )
            .unwrap();
        assert_eq!(su.rows[0].get("any"), Some("true"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_has_type_privilege_e2e() {
        let (mut admin, root) = temp_session("has-type-priv");
        admin
            .engine()
            .register_table(TableSchema::new("employees", "id", vec![]))
            .unwrap();
        admin
            .execute_sql("INSERT INTO employees (id, name) VALUES (1, 'Ada')")
            .unwrap();
        admin
            .execute_sql("CREATE USER analyst WITH PASSWORD 'secret'")
            .unwrap();
        admin
            .execute_sql("GRANT SELECT ON employees TO analyst")
            .unwrap();

        let mut analyst = SessionState::as_user(Arc::clone(admin.engine()), "analyst").unwrap();
        let row = analyst
            .execute_sql(
                "SELECT has_type_privilege('integer', 'USAGE') AS ok, \
                 has_type_privilege(to_regtype('text'), 'USAGE') AS oid_ok, \
                 has_type_privilege('analyst', 'jsonb', 'USAGE') AS u_ok \
                 FROM employees WHERE id = 1",
            )
            .unwrap();
        assert_eq!(row.rows[0].get("ok"), Some("true"));
        assert_eq!(row.rows[0].get("oid_ok"), Some("true"));
        assert_eq!(row.rows[0].get("u_ok"), Some("true"));

        let err = analyst.execute_sql(
            "SELECT has_type_privilege('nope_type', 'USAGE') FROM employees WHERE id = 1",
        );
        assert!(err.is_err(), "expected missing type error, got {err:?}");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_inet_addr_port_e2e() {
        let (mut session, root) = temp_session("inet-addr-port");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();

        let none = session
            .execute_sql(
                "SELECT inet_server_addr() AS sa, inet_server_port() AS sp, \
                 inet_client_addr() AS ca, inet_client_port() AS cp FROM users WHERE id = 1",
            )
            .unwrap();
        assert_eq!(none.rows[0].get("sa"), Some(""));
        assert_eq!(none.rows[0].get("sp"), Some(""));
        assert_eq!(none.rows[0].get("ca"), Some(""));
        assert_eq!(none.rows[0].get("cp"), Some(""));

        session.set_net_info(
            Some("127.0.0.1".into()),
            Some(5433),
            Some("10.0.0.5".into()),
            Some(50123),
        );
        let row = session
            .execute_sql(
                "SELECT inet_server_addr() AS sa, inet_server_port() AS sp, \
                 inet_client_addr() AS ca, inet_client_port() AS cp FROM users WHERE id = 1",
            )
            .unwrap();
        assert_eq!(row.rows[0].get("sa"), Some("127.0.0.1"));
        assert_eq!(row.rows[0].get("sp"), Some("5433"));
        assert_eq!(row.rows[0].get("ca"), Some("10.0.0.5"));
        assert_eq!(row.rows[0].get("cp"), Some("50123"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn net_info_from_endpoints_listen_and_unspecified() {
        let peer: SocketAddr = "10.0.0.5:50123".parse().unwrap();
        let (sa, sp, ca, cp) =
            net_info_from_endpoints(Some("127.0.0.1:5433".parse().unwrap()), peer);
        assert_eq!(sa.as_deref(), Some("127.0.0.1"));
        assert_eq!(sp, Some(5433));
        assert_eq!(ca.as_deref(), Some("10.0.0.5"));
        assert_eq!(cp, Some(50123));

        let (sa2, sp2, _, _) =
            net_info_from_endpoints(Some("0.0.0.0:5433".parse().unwrap()), peer);
        assert_eq!(sa2, None);
        assert_eq!(sp2, Some(5433));

        let (sa3, sp3, ca3, _) = net_info_from_endpoints(None, peer);
        assert_eq!(sa3, None);
        assert_eq!(sp3, None);
        assert_eq!(ca3.as_deref(), Some("10.0.0.5"));
    }

    #[test]
    fn backend_listen_addr_feeds_session_net_info() {
        let (mut session, root) = temp_session("inet-backend-listen");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();
        let backend = TakyonicPgBackend::new(
            TakyonicClient::new(std::iter::empty::<String>()),
            Arc::clone(session.engine()),
        );
        backend.set_listen_addr("192.168.1.10:5433".parse().unwrap());
        let (sa, sp, ca, cp) = net_info_from_endpoints(
            *backend.listen_addr.read(),
            "10.1.2.3:40000".parse().unwrap(),
        );
        session.set_net_info(sa, sp, ca, cp);
        let row = session
            .execute_sql(
                "SELECT inet_server_addr() AS sa, inet_server_port() AS sp, \
                 inet_client_addr() AS ca, inet_client_port() AS cp FROM users WHERE id = 1",
            )
            .unwrap();
        assert_eq!(row.rows[0].get("sa"), Some("192.168.1.10"));
        assert_eq!(row.rows[0].get("sp"), Some("5433"));
        assert_eq!(row.rows[0].get("ca"), Some("10.1.2.3"));
        assert_eq!(row.rows[0].get("cp"), Some("40000"));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_psql_dt_and_information_schema() {
        let (mut session, root) = temp_session("pg-catalog-dt");
        session
            .execute_sql("CREATE TABLE items (id BIGINT PRIMARY KEY, name TEXT)")
            .unwrap();

        let dt_sql = r#"SELECT n.nspname as "Schema",
  c.relname as "Name",
  CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' END as "Type",
  pg_catalog.pg_get_userbyid(c.relowner) as "Owner"
FROM pg_catalog.pg_class c
     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r','p','')
      AND n.nspname <> 'pg_catalog'
      AND n.nspname <> 'information_schema'
      AND n.nspname !~ '^pg_toast'
  AND pg_catalog.pg_table_is_visible(c.oid)
ORDER BY 1,2;"#;
        let dt = session.execute_sql(dt_sql).unwrap();
        assert_eq!(dt.tag, "SELECT");
        let names: Vec<_> = dt
            .rows
            .iter()
            .filter_map(|r| r.get("Name"))
            .collect();
        assert!(
            names.contains(&"items") && names.contains(&"users"),
            "expected items+users in \\dt rows, got {names:?}"
        );
        assert_eq!(dt.rows.iter().find(|r| r.get("Name") == Some("items"))
            .and_then(|r| r.get("Type")), Some("table"));

        let info = session
            .execute_sql(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'",
            )
            .unwrap();
        assert!(
            info.rows.iter().any(|r| r.get("table_name") == Some("items")),
            "information_schema.tables missing items: {info:?}"
        );

        let cols = session
            .execute_sql(
                "SELECT column_name, data_type FROM information_schema.columns \
                 WHERE table_name = 'items'",
            )
            .unwrap();
        assert_eq!(cols.rows.len(), 2);
        assert_eq!(cols.rows[0].get("column_name"), Some("id"));
        assert_eq!(cols.rows[1].get("column_name"), Some("name"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_psql_d_describe_table() {
        let (mut session, root) = temp_session("pg-catalog-d");
        session
            .execute_sql("CREATE TABLE items (id BIGINT PRIMARY KEY, name TEXT, qty INT)")
            .unwrap();

        let d_sql = r#"SELECT a.attname AS "Column",
  pg_catalog.format_type(a.atttypid, a.atttypmod) AS "Type",
  '' AS "Collation",
  CASE WHEN a.attnotnull THEN 'not null' ELSE '' END AS "Nullable",
  '' AS "Default"
FROM pg_catalog.pg_attribute a
     JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
WHERE c.relname = 'items'
  AND a.attnum > 0
  AND NOT a.attisdropped
ORDER BY a.attnum;"#;
        let d = session.execute_sql(d_sql).unwrap();
        assert_eq!(d.tag, "SELECT");
        assert_eq!(d.rows.len(), 3);
        assert_eq!(d.rows[0].get("Column"), Some("id"));
        assert_eq!(d.rows[0].get("Type"), Some("BIGINT"));
        assert_eq!(d.rows[0].get("Nullable"), Some("not null"));
        assert_eq!(d.rows[1].get("Column"), Some("name"));
        assert_eq!(d.rows[1].get("Type"), Some("TEXT"));
        assert_eq!(d.rows[2].get("Column"), Some("qty"));
        assert_eq!(d.rows[2].get("Type"), Some("INT"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn describe_plan_fields_for_select_project_and_aggregate() {
        let (mut session, root) = temp_session("describe-fields");
        session
            .execute_sql("CREATE TABLE items (id BIGINT PRIMARY KEY, name TEXT, qty INT)")
            .unwrap();

        let star = SqlEngine::plan("SELECT * FROM items").unwrap();
        let star_fields = describe_plan_columns(&star, session.engine());
        assert_eq!(
            star_fields,
            vec![
                ("id".into(), Type::INT8),
                ("name".into(), Type::VARCHAR),
                ("qty".into(), Type::INT4),
            ]
        );

        let proj = SqlEngine::plan("SELECT name, id FROM items WHERE id = $1").unwrap();
        let proj_fields = describe_plan_columns(&proj, session.engine());
        assert_eq!(
            proj_fields,
            vec![("name".into(), Type::VARCHAR), ("id".into(), Type::INT8)]
        );
        let wire = describe_plan_fields(&proj, session.engine());
        assert_eq!(wire.len(), 2);
        assert_eq!(wire[0].name(), "name");
        assert_eq!(wire[1].name(), "id");

        let agg = SqlEngine::plan("SELECT COUNT(*) FROM items").unwrap();
        let agg_fields = describe_plan_columns(&agg, session.engine());
        assert_eq!(agg_fields, vec![("count(*)".into(), Type::INT8)]);

        let dml = SqlEngine::plan("INSERT INTO items (id, name, qty) VALUES (1, 'a', 2)").unwrap();
        assert!(describe_plan_columns(&dml, session.engine()).is_empty());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_orm_types_describe_and_insert_e2e() {
        let (mut session, root) = temp_session("orm-types");
        session
            .execute_sql(
                "CREATE TABLE typed (
                    id UUID PRIMARY KEY,
                    blob BYTEA,
                    amount NUMERIC,
                    ts TIMESTAMPTZ
                )",
            )
            .unwrap();
        let plan = SqlEngine::plan("SELECT * FROM typed").unwrap();
        let fields = describe_plan_columns(&plan, session.engine());
        assert_eq!(
            fields,
            vec![
                ("id".into(), Type::UUID),
                ("blob".into(), Type::BYTEA),
                ("amount".into(), Type::NUMERIC),
                ("ts".into(), Type::TIMESTAMPTZ),
            ]
        );
        session
            .execute_sql(
                "INSERT INTO typed (id, blob, amount, ts) VALUES (
                    '550e8400-e29b-41d4-a716-446655440000',
                    '\\xDEAD',
                    '12.50',
                    '2026-08-07 12:00:00+00'
                )",
            )
            .unwrap();
        let bad = session.execute_sql(
            "INSERT INTO typed (id, blob, amount, ts) VALUES ('not-a-uuid', '\\x00', '1', '2026-01-01')",
        );
        assert!(bad.is_err(), "invalid uuid must fail");

        let cols = session
            .execute_sql(
                "SELECT column_name, data_type, udt_name, is_nullable \
                 FROM information_schema.columns WHERE table_name = 'typed' \
                 ORDER BY ordinal_position",
            )
            .unwrap();
        assert_eq!(cols.rows.len(), 4);
        assert_eq!(cols.rows[0].get("udt_name"), Some("uuid"));
        assert_eq!(cols.rows[1].get("udt_name"), Some("bytea"));
        assert_eq!(cols.rows[2].get("udt_name"), Some("numeric"));
        assert_eq!(cols.rows[3].get("udt_name"), Some("timestamptz"));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_copy_stdin_stdout_tsv_e2e() {
        let (mut session, root) = temp_session("copy-stdio");
        let table = format!("cpy_io_{}", session.session_id);
        session
            .execute_sql(&format!(
                "CREATE TABLE {table} (id BIGINT PRIMARY KEY, name TEXT)"
            ))
            .unwrap();
        let n = session
            .copy_from_tsv(
                &table,
                &[],
                "1\tAda\n2\tGrace\n",
            )
            .unwrap();
        assert_eq!(n, 2);

        // Wire-shaped: SQL COPY FROM STDIN arms pending buffer.
        let arm = session
            .execute_sql(&format!("COPY {table} FROM STDIN"))
            .unwrap();
        assert_eq!(arm.tag, "COPY_IN");
        session
            .append_copy_in_data(b"3\tLin\n")
            .unwrap();
        let n2 = session.finish_copy_in().unwrap();
        assert_eq!(n2, 1);

        let out = session.copy_to_tsv(&table, &[]).unwrap();
        assert!(out.contains("Ada"));
        assert!(out.contains("Lin"));

        let stdout = session
            .execute_sql(&format!("COPY {table} TO STDOUT"))
            .unwrap();
        assert_eq!(stdout.tag, "COPY_OUT");
        let tsv = stdout.rows[0].get("__copy_tsv").unwrap();
        assert!(tsv.contains("Grace"));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_left_outer_join_e2e() {
        let (mut session, root) = temp_session("left-join-e2e");
        session
            .execute_sql("CREATE TABLE orders (order_id BIGINT PRIMARY KEY, user_id BIGINT)")
            .unwrap();
        session
            .execute_sql(
                "INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30), (2, 'Bob', 20), (3, 'Cy', 25)",
            )
            .unwrap();
        session
            .execute_sql("INSERT INTO orders (order_id, user_id) VALUES (10, 1), (20, 2)")
            .unwrap();

        let result = session
            .execute_sql(
                "SELECT name, order_id FROM users LEFT JOIN orders ON users.id = orders.user_id",
            )
            .unwrap();
        assert_eq!(result.tag, "SELECT");
        assert_eq!(result.rows.len(), 3);

        let mut pairs: Vec<(String, String)> = result
            .rows
            .iter()
            .map(|r| {
                (
                    r.get("name").unwrap_or("").to_string(),
                    r.get("order_id").unwrap_or("").to_string(),
                )
            })
            .collect();
        pairs.sort();
        assert_eq!(
            pairs,
            vec![
                ("Ada".into(), "10".into()),
                ("Bob".into(), "20".into()),
                ("Cy".into(), "".into()),
            ]
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_right_outer_join_e2e() {
        let (mut session, root) = temp_session("right-join-e2e");
        session
            .execute_sql("CREATE TABLE orders (order_id BIGINT PRIMARY KEY, user_id BIGINT)")
            .unwrap();
        session
            .execute_sql(
                "INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30), (2, 'Bob', 20)",
            )
            .unwrap();
        session
            .execute_sql(
                "INSERT INTO orders (order_id, user_id) VALUES (10, 1), (20, 2), (30, 99)",
            )
            .unwrap();

        let result = session
            .execute_sql(
                "SELECT name, order_id FROM users RIGHT JOIN orders ON users.id = orders.user_id",
            )
            .unwrap();
        assert_eq!(result.tag, "SELECT");
        assert_eq!(result.rows.len(), 3);

        let mut pairs: Vec<(String, String)> = result
            .rows
            .iter()
            .map(|r| {
                (
                    r.get("name").unwrap_or("").to_string(),
                    r.get("order_id").unwrap_or("").to_string(),
                )
            })
            .collect();
        pairs.sort();
        assert_eq!(
            pairs,
            vec![
                ("".into(), "30".into()),
                ("Ada".into(), "10".into()),
                ("Bob".into(), "20".into()),
            ]
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_full_outer_join_e2e() {
        let (mut session, root) = temp_session("full-join-e2e");
        session
            .execute_sql("CREATE TABLE orders (order_id BIGINT PRIMARY KEY, user_id BIGINT)")
            .unwrap();
        session
            .execute_sql(
                "INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30), (2, 'Bob', 20), (3, 'Cy', 25)",
            )
            .unwrap();
        session
            .execute_sql(
                "INSERT INTO orders (order_id, user_id) VALUES (10, 1), (20, 2), (30, 99)",
            )
            .unwrap();

        let result = session
            .execute_sql(
                "SELECT name, order_id FROM users FULL OUTER JOIN orders ON users.id = orders.user_id",
            )
            .unwrap();
        assert_eq!(result.tag, "SELECT");
        assert_eq!(result.rows.len(), 4);

        let mut pairs: Vec<(String, String)> = result
            .rows
            .iter()
            .map(|r| {
                (
                    r.get("name").unwrap_or("").to_string(),
                    r.get("order_id").unwrap_or("").to_string(),
                )
            })
            .collect();
        pairs.sort();
        assert_eq!(
            pairs,
            vec![
                ("".into(), "30".into()),
                ("Ada".into(), "10".into()),
                ("Bob".into(), "20".into()),
                ("Cy".into(), "".into()),
            ]
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_union_and_distinct_e2e() {
        let (mut session, root) = temp_session("union-distinct");
        session
            .execute_sql(
                "INSERT INTO users (id, name, age) VALUES \
                 (1, 'Ada', 30), (2, 'Bob', 20), (3, 'Ada', 25)",
            )
            .unwrap();

        let distinct = session
            .execute_sql("SELECT DISTINCT name FROM users")
            .unwrap();
        assert_eq!(distinct.tag, "SELECT");
        let mut names: Vec<_> = distinct
            .rows
            .iter()
            .filter_map(|r| r.get("name").map(str::to_string))
            .collect();
        names.sort();
        assert_eq!(names, vec!["Ada".to_string(), "Bob".to_string()]);

        let union_all = session
            .execute_sql(
                "SELECT name FROM users WHERE id = 1 \
                 UNION ALL \
                 SELECT name FROM users WHERE id = 3",
            )
            .unwrap();
        assert_eq!(union_all.rows.len(), 2); // Ada + Ada

        let union_distinct = session
            .execute_sql(
                "SELECT name FROM users WHERE id = 1 \
                 UNION \
                 SELECT name FROM users WHERE id = 3",
            )
            .unwrap();
        assert_eq!(union_distinct.rows.len(), 1);
        assert_eq!(union_distinct.rows[0].get("name"), Some("Ada"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_intersect_and_except_e2e() {
        let (mut session, root) = temp_session("intersect-except");
        session
            .execute_sql(
                "INSERT INTO users (id, name, age) VALUES \
                 (1, 'Ada', 30), (2, 'Bob', 20), (3, 'Ada', 25), (4, 'Cy', 40)",
            )
            .unwrap();

        let inter = session
            .execute_sql(
                "SELECT name FROM users WHERE id IN (1, 2) \
                 INTERSECT \
                 SELECT name FROM users WHERE id IN (3, 4)",
            )
            .unwrap();
        assert_eq!(inter.rows.len(), 1);
        assert_eq!(inter.rows[0].get("name"), Some("Ada"));

        let except = session
            .execute_sql(
                "SELECT name FROM users WHERE id IN (1, 2, 4) \
                 EXCEPT \
                 SELECT name FROM users WHERE id = 3",
            )
            .unwrap();
        let mut names: Vec<_> = except
            .rows
            .iter()
            .filter_map(|r| r.get("name").map(str::to_string))
            .collect();
        names.sort();
        assert_eq!(names, vec!["Bob".to_string(), "Cy".to_string()]);

        let except_all = session
            .execute_sql(
                "SELECT name FROM users WHERE id IN (1, 3) \
                 EXCEPT ALL \
                 SELECT name FROM users WHERE id = 1",
            )
            .unwrap();
        assert_eq!(except_all.rows.len(), 1);
        assert_eq!(except_all.rows[0].get("name"), Some("Ada"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_like_and_ilike_e2e() {
        let (mut session, root) = temp_session("like-ilike");
        session
            .execute_sql(
                "INSERT INTO users (id, name, age) VALUES \
                 (1, 'Ada', 30), (2, 'Bob', 20), (3, 'ada', 25)",
            )
            .unwrap();

        let like = session
            .execute_sql("SELECT name FROM users WHERE name LIKE 'A%'")
            .unwrap();
        assert_eq!(like.rows.len(), 1);
        assert_eq!(like.rows[0].get("name"), Some("Ada"));

        let ilike = session
            .execute_sql("SELECT name FROM users WHERE name ILIKE 'a%'")
            .unwrap();
        let mut names: Vec<_> = ilike
            .rows
            .iter()
            .filter_map(|r| r.get("name").map(str::to_string))
            .collect();
        names.sort();
        assert_eq!(names, vec!["Ada".to_string(), "ada".to_string()]);

        let not_like = session
            .execute_sql("SELECT name FROM users WHERE name NOT LIKE '%a' AND name NOT LIKE '%A'")
            .unwrap();
        assert_eq!(not_like.rows.len(), 1);
        assert_eq!(not_like.rows[0].get("name"), Some("Bob"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_like_any_e2e() {
        let (mut session, root) = temp_session("like-any");
        session
            .execute_sql(
                "INSERT INTO users (id, name, age) VALUES \
                 (1, 'Ada', 30), (2, 'Bob', 20), (3, 'ada', 25), (4, 'Cy', 22)",
            )
            .unwrap();

        let like_any = session
            .execute_sql(
                "SELECT name FROM users WHERE name LIKE ANY (ARRAY['A%', 'B%']) ORDER BY id",
            )
            .unwrap();
        assert_eq!(like_any.rows.len(), 2);
        assert_eq!(like_any.rows[0].get("name"), Some("Ada"));
        assert_eq!(like_any.rows[1].get("name"), Some("Bob"));

        let ilike_any = session
            .execute_sql(
                "SELECT name FROM users WHERE name ILIKE ANY (ARRAY['%ADA%']) ORDER BY id",
            )
            .unwrap();
        assert_eq!(ilike_any.rows.len(), 2);
        assert_eq!(ilike_any.rows[0].get("name"), Some("Ada"));
        assert_eq!(ilike_any.rows[1].get("name"), Some("ada"));

        let not_any = session
            .execute_sql(
                "SELECT name FROM users WHERE name NOT LIKE ANY (ARRAY['A%', 'B%']) ORDER BY id",
            )
            .unwrap();
        assert_eq!(not_any.rows.len(), 2);
        assert_eq!(not_any.rows[0].get("name"), Some("ada"));
        assert_eq!(not_any.rows[1].get("name"), Some("Cy"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_similar_to_e2e() {
        let (mut session, root) = temp_session("similar-to");
        session
            .execute_sql(
                "INSERT INTO users (id, name, age) VALUES \
                 (1, 'Ada', 30), (2, 'Bob', 20), (3, 'Cy', 25), (4, 'Ann', 22)",
            )
            .unwrap();

        let similar = session
            .execute_sql("SELECT name FROM users WHERE name SIMILAR TO 'A%' ORDER BY id")
            .unwrap();
        assert_eq!(similar.rows.len(), 2);
        assert_eq!(similar.rows[0].get("name"), Some("Ada"));
        assert_eq!(similar.rows[1].get("name"), Some("Ann"));

        let alt = session
            .execute_sql(
                "SELECT name FROM users WHERE name SIMILAR TO '(B|C)%' ORDER BY id",
            )
            .unwrap();
        assert_eq!(alt.rows.len(), 2);
        assert_eq!(alt.rows[0].get("name"), Some("Bob"));
        assert_eq!(alt.rows[1].get("name"), Some("Cy"));

        let not_sim = session
            .execute_sql(
                "SELECT name FROM users WHERE name NOT SIMILAR TO 'A%' ORDER BY id",
            )
            .unwrap();
        assert_eq!(not_sim.rows.len(), 2);
        assert_eq!(not_sim.rows[0].get("name"), Some("Bob"));
        assert_eq!(not_sim.rows[1].get("name"), Some("Cy"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_regex_match_ops_e2e() {
        let (mut session, root) = temp_session("regex-ops");
        session
            .execute_sql(
                "INSERT INTO users (id, name, age) VALUES \
                 (1, 'Ada', 30), (2, 'Bob', 20), (3, 'ada', 25)",
            )
            .unwrap();

        let tilde = session
            .execute_sql("SELECT name FROM users WHERE name ~ '^A' ORDER BY id")
            .unwrap();
        assert_eq!(tilde.rows.len(), 1);
        assert_eq!(tilde.rows[0].get("name"), Some("Ada"));

        let icase = session
            .execute_sql("SELECT name FROM users WHERE name ~* '^ada$' ORDER BY id")
            .unwrap();
        assert_eq!(icase.rows.len(), 2);
        assert_eq!(icase.rows[0].get("name"), Some("Ada"));
        assert_eq!(icase.rows[1].get("name"), Some("ada"));

        let not_re = session
            .execute_sql("SELECT name FROM users WHERE name !~ 'a' ORDER BY id")
            .unwrap();
        assert_eq!(not_re.rows.len(), 1);
        assert_eq!(not_re.rows[0].get("name"), Some("Bob"));

        let not_icase = session
            .execute_sql("SELECT name FROM users WHERE name !~* 'ADA' ORDER BY id")
            .unwrap();
        assert_eq!(not_icase.rows.len(), 1);
        assert_eq!(not_icase.rows[0].get("name"), Some("Bob"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_dml_returning_e2e() {
        let (mut session, root) = temp_session("dml-returning");

        let ins = session
            .execute_sql(
                "INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30) \
                 RETURNING id, name",
            )
            .unwrap();
        assert_eq!(ins.tag, "INSERT");
        assert_eq!(ins.affected, Some(1));
        assert_eq!(ins.rows.len(), 1);
        assert_eq!(ins.rows[0].get("id"), Some("1"));
        assert_eq!(ins.rows[0].get("name"), Some("Ada"));
        assert_eq!(
            ins.column_order.as_deref(),
            Some(["id".to_string(), "name".to_string()].as_slice())
        );

        let upd = session
            .execute_sql("UPDATE users SET age = 31 WHERE id = 1 RETURNING id, age")
            .unwrap();
        assert_eq!(upd.tag, "UPDATE");
        assert_eq!(upd.affected, Some(1));
        assert_eq!(upd.rows.len(), 1);
        assert_eq!(upd.rows[0].get("id"), Some("1"));
        assert_eq!(upd.rows[0].get("age"), Some("31"));

        let star = session
            .execute_sql(
                "INSERT INTO users (id, name, age) VALUES (2, 'Bob', 20) RETURNING *",
            )
            .unwrap();
        assert_eq!(star.tag, "INSERT");
        assert_eq!(star.affected, Some(1));
        assert_eq!(star.rows.len(), 1);
        assert_eq!(star.rows[0].get("id"), Some("2"));
        assert_eq!(star.rows[0].get("name"), Some("Bob"));
        assert_eq!(star.rows[0].get("age"), Some("20"));

        let del = session
            .execute_sql("DELETE FROM users WHERE id = 2 RETURNING id, name")
            .unwrap();
        assert_eq!(del.tag, "DELETE");
        assert_eq!(del.affected, Some(1));
        assert_eq!(del.rows.len(), 1);
        assert_eq!(del.rows[0].get("id"), Some("2"));
        assert_eq!(del.rows[0].get("name"), Some("Bob"));

        let left = session
            .execute_sql("SELECT id, name, age FROM users ORDER BY id")
            .unwrap();
        assert_eq!(left.rows.len(), 1);
        assert_eq!(left.rows[0].get("name"), Some("Ada"));
        assert_eq!(left.rows[0].get("age"), Some("31"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_insert_on_conflict_do_nothing_e2e() {
        let (mut session, root) = temp_session("on-conflict");

        let first = session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();
        assert_eq!(first.affected, Some(1));

        let skip = session
            .execute_sql(
                "INSERT INTO users (id, name, age) VALUES (1, 'Other', 99) \
                 ON CONFLICT DO NOTHING",
            )
            .unwrap();
        assert_eq!(skip.tag, "INSERT");
        assert_eq!(skip.affected, Some(0));

        let mixed = session
            .execute_sql(
                "INSERT INTO users (id, name, age) VALUES \
                 (1, 'Skip', 1), (2, 'Bob', 20) ON CONFLICT DO NOTHING RETURNING id, name",
            )
            .unwrap();
        assert_eq!(mixed.affected, Some(1));
        assert_eq!(mixed.rows.len(), 1);
        assert_eq!(mixed.rows[0].get("id"), Some("2"));
        assert_eq!(mixed.rows[0].get("name"), Some("Bob"));

        let left = session
            .execute_sql("SELECT id, name, age FROM users ORDER BY id")
            .unwrap();
        assert_eq!(left.rows.len(), 2);
        assert_eq!(left.rows[0].get("name"), Some("Ada"));
        assert_eq!(left.rows[0].get("age"), Some("30"));
        assert_eq!(left.rows[1].get("name"), Some("Bob"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_insert_on_conflict_do_update_e2e() {
        let (mut session, root) = temp_session("on-conflict-update");

        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();

        let upsert = session
            .execute_sql(
                "INSERT INTO users (id, name, age) VALUES (1, 'Ada Lovelace', 31) \
                 ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, age = EXCLUDED.age \
                 RETURNING id, name, age",
            )
            .unwrap();
        assert_eq!(upsert.tag, "INSERT");
        assert_eq!(upsert.affected, Some(1));
        assert_eq!(upsert.rows.len(), 1);
        assert_eq!(upsert.rows[0].get("name"), Some("Ada Lovelace"));
        assert_eq!(upsert.rows[0].get("age"), Some("31"));

        let skipped = session
            .execute_sql(
                "INSERT INTO users (id, name, age) VALUES (1, 'Nope', 99) \
                 ON CONFLICT DO UPDATE SET name = EXCLUDED.name WHERE age < 20 \
                 RETURNING id",
            )
            .unwrap();
        assert_eq!(skipped.affected, Some(0));
        assert!(skipped.rows.is_empty());

        let left = session
            .execute_sql("SELECT id, name, age FROM users WHERE id = 1")
            .unwrap();
        assert_eq!(left.rows[0].get("name"), Some("Ada Lovelace"));
        assert_eq!(left.rows[0].get("age"), Some("31"));

        let insert_new = session
            .execute_sql(
                "INSERT INTO users (id, name, age) VALUES (2, 'Bob', 20) \
                 ON CONFLICT DO UPDATE SET name = EXCLUDED.name RETURNING id, name",
            )
            .unwrap();
        assert_eq!(insert_new.affected, Some(1));
        assert_eq!(insert_new.rows[0].get("name"), Some("Bob"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_values_clause_e2e() {
        let (mut session, root) = temp_session("values-clause");

        let bare = session
            .execute_sql("VALUES (1, 'Ada'), (2, 'Bob')")
            .unwrap();
        assert_eq!(bare.rows.len(), 2);
        assert_eq!(bare.rows[0].get("column1"), Some("1"));
        assert_eq!(bare.rows[0].get("column2"), Some("Ada"));
        assert_eq!(bare.rows[1].get("column1"), Some("2"));
        assert_eq!(bare.rows[1].get("column2"), Some("Bob"));

        let aliased = session
            .execute_sql(
                "SELECT id, name FROM (VALUES (1, 'Ada'), (2, 'Bob')) AS t(id, name) \
                 ORDER BY id",
            )
            .unwrap();
        assert_eq!(aliased.rows.len(), 2);
        assert_eq!(aliased.rows[0].get("id"), Some("1"));
        assert_eq!(aliased.rows[0].get("name"), Some("Ada"));
        assert_eq!(aliased.rows[1].get("name"), Some("Bob"));

        // Join VALUES against a real table.
        session
            .execute_sql(
                "INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30), (2, 'Bob', 20)",
            )
            .unwrap();
        let joined = session
            .execute_sql(
                "SELECT u.name, v.extra FROM users u \
                 JOIN (VALUES (1, 'x'), (2, 'y')) AS v(id, extra) ON u.id = v.id \
                 ORDER BY u.id",
            )
            .unwrap();
        assert_eq!(joined.rows.len(), 2);
        assert_eq!(joined.rows[0].get("extra"), Some("x"));
        assert_eq!(joined.rows[1].get("extra"), Some("y"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_between_e2e() {
        let (mut session, root) = temp_session("between");
        session
            .execute_sql(
                "INSERT INTO users (id, name, age) VALUES \
                 (1, 'Ada', 30), (2, 'Bob', 20), (3, 'Cy', 40)",
            )
            .unwrap();

        let between = session
            .execute_sql("SELECT name FROM users WHERE age BETWEEN 20 AND 30")
            .unwrap();
        let mut names: Vec<_> = between
            .rows
            .iter()
            .filter_map(|r| r.get("name").map(str::to_string))
            .collect();
        names.sort();
        assert_eq!(names, vec!["Ada".to_string(), "Bob".to_string()]);

        let not_between = session
            .execute_sql("SELECT name FROM users WHERE age NOT BETWEEN 25 AND 35")
            .unwrap();
        let mut names: Vec<_> = not_between
            .rows
            .iter()
            .filter_map(|r| r.get("name").map(str::to_string))
            .collect();
        names.sort();
        assert_eq!(names, vec!["Bob".to_string(), "Cy".to_string()]);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_is_true_false_unknown_e2e() {
        let (mut session, root) = temp_session("is-bool-test");
        session
            .execute_sql(
                "INSERT INTO users (id, name, age) VALUES \
                 (1, 'Ada', 30), (2, 'Bob', NULL), (3, 'Cy', 10)",
            )
            .unwrap();

        let is_true = session
            .execute_sql("SELECT name FROM users WHERE (age > 20) IS TRUE ORDER BY id")
            .unwrap();
        assert_eq!(is_true.rows.len(), 1);
        assert_eq!(is_true.rows[0].get("name"), Some("Ada"));

        let is_false = session
            .execute_sql("SELECT name FROM users WHERE (age > 20) IS FALSE ORDER BY id")
            .unwrap();
        assert_eq!(is_false.rows.len(), 1);
        assert_eq!(is_false.rows[0].get("name"), Some("Cy"));

        let is_unknown = session
            .execute_sql("SELECT name FROM users WHERE (age > 20) IS UNKNOWN ORDER BY id")
            .unwrap();
        assert_eq!(is_unknown.rows.len(), 1);
        assert_eq!(is_unknown.rows[0].get("name"), Some("Bob"));

        let not_true = session
            .execute_sql("SELECT name FROM users WHERE (age > 20) IS NOT TRUE ORDER BY id")
            .unwrap();
        assert_eq!(not_true.rows.len(), 2);
        assert_eq!(not_true.rows[0].get("name"), Some("Bob"));
        assert_eq!(not_true.rows[1].get("name"), Some("Cy"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_binary_op_null_propagates_e2e() {
        let (mut session, root) = temp_session("binop-null");
        session
            .execute_sql(
                "INSERT INTO users (id, name, age) VALUES \
                 (1, 'Ada', 30), (2, 'Bob', NULL), (3, 'Cy', 10)",
            )
            .unwrap();

        let gt = session
            .execute_sql("SELECT name FROM users WHERE age > 20 ORDER BY id")
            .unwrap();
        assert_eq!(gt.rows.len(), 1);
        assert_eq!(gt.rows[0].get("name"), Some("Ada"));

        let proj = session
            .execute_sql("SELECT name, (age = 30) AS eq30 FROM users ORDER BY id")
            .unwrap();
        assert_eq!(proj.rows[0].get("eq30"), Some("true"));
        assert_eq!(proj.rows[1].get("eq30"), Some(""));
        assert_eq!(proj.rows[2].get("eq30"), Some("false"));

        let not_gt = session
            .execute_sql("SELECT name FROM users WHERE NOT (age > 20) ORDER BY id")
            .unwrap();
        assert_eq!(not_gt.rows.len(), 1);
        assert_eq!(not_gt.rows[0].get("name"), Some("Cy"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_any_all_quantified_e2e() {
        let (mut session, root) = temp_session("any-all");
        session
            .execute_sql(
                "INSERT INTO users (id, name, age) VALUES \
                 (1, 'Ada', 30), (2, 'Bob', NULL), (3, 'Cy', 10)",
            )
            .unwrap();

        let any_eq = session
            .execute_sql(
                "SELECT name FROM users WHERE age = ANY(ARRAY[10, 30]) ORDER BY id",
            )
            .unwrap();
        assert_eq!(any_eq.rows.len(), 2);
        assert_eq!(any_eq.rows[0].get("name"), Some("Ada"));
        assert_eq!(any_eq.rows[1].get("name"), Some("Cy"));

        let some = session
            .execute_sql(
                "SELECT name FROM users WHERE age = SOME(ARRAY[30]) ORDER BY id",
            )
            .unwrap();
        assert_eq!(some.rows.len(), 1);
        assert_eq!(some.rows[0].get("name"), Some("Ada"));

        // > ALL — only Ada (30 > 15 and 30 > 20); Cy fails 10 > 15
        let all_gt = session
            .execute_sql(
                "SELECT name FROM users WHERE age > ALL(ARRAY[15, 20]) ORDER BY id",
            )
            .unwrap();
        assert_eq!(all_gt.rows.len(), 1);
        assert_eq!(all_gt.rows[0].get("name"), Some("Ada"));

        let any_gt = session
            .execute_sql(
                "SELECT name FROM users WHERE age > ANY(ARRAY[20, 25]) ORDER BY id",
            )
            .unwrap();
        assert_eq!(any_gt.rows.len(), 1);
        assert_eq!(any_gt.rows[0].get("name"), Some("Ada"));

        let sub = session
            .execute_sql(
                "SELECT name FROM users WHERE age = ANY(SELECT age FROM users WHERE name = 'Ada') \
                 ORDER BY id",
            )
            .unwrap();
        assert_eq!(sub.rows.len(), 1);
        assert_eq!(sub.rows[0].get("name"), Some("Ada"));

        let not_all = session
            .execute_sql(
                "SELECT name FROM users WHERE age <> ALL(SELECT age FROM users WHERE name = 'Ada') \
                 ORDER BY id",
            )
            .unwrap();
        let names: Vec<_> = not_all
            .rows
            .iter()
            .filter_map(|r| r.get("name"))
            .collect();
        assert_eq!(names, vec!["Cy"], "got {names:?}");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_distinct_on_e2e() {
        let (mut session, root) = temp_session("distinct-on");
        session
            .execute_sql(
                "INSERT INTO users (id, name, age) VALUES \
                 (1, 'Ada', 30), (2, 'Ada', 20), (3, 'Bob', 25)",
            )
            .unwrap();

        let rows = session
            .execute_sql(
                "SELECT DISTINCT ON (name) name, age FROM users ORDER BY name, age",
            )
            .unwrap();
        assert_eq!(rows.rows.len(), 2);
        assert_eq!(rows.rows[0].get("name"), Some("Ada"));
        assert_eq!(rows.rows[0].get("age"), Some("20"));
        assert_eq!(rows.rows[1].get("name"), Some("Bob"));
        assert_eq!(rows.rows[1].get("age"), Some("25"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_window_exclude_e2e() {
        let (mut session, root) = temp_session("window-exclude");
        session
            .execute_sql("CREATE TABLE emp (id BIGINT PRIMARY KEY, name TEXT, salary BIGINT)")
            .unwrap();
        for (id, name, sal) in [(1, "A", 10), (2, "B", 20), (3, "C", 30)] {
            session
                .execute_sql(&format!(
                    "INSERT INTO emp (id, name, salary) VALUES ({id}, '{name}', {sal})"
                ))
                .unwrap();
        }

        // Running SUM excluding current row: A=0/NULL→empty, B=10, C=10+20=30
        let rows = session
            .execute_sql(
                "SELECT name, SUM(salary) OVER (\
                   ORDER BY id \
                   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW \
                   EXCLUDE CURRENT ROW\
                 ) AS s FROM emp ORDER BY id",
            )
            .unwrap();
        assert_eq!(rows.rows.len(), 3);
        assert_eq!(rows.rows[0].get("s"), Some(""));
        assert_eq!(rows.rows[1].get("s"), Some("10"));
        assert_eq!(rows.rows[2].get("s"), Some("30"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_fetch_with_ties_e2e() {
        let (mut session, root) = temp_session("fetch-ties");
        session
            .execute_sql(
                "INSERT INTO users (id, name, age) VALUES \
                 (1, 'Ada', 10), (2, 'Bob', 20), (3, 'Cy', 20), (4, 'Di', 30)",
            )
            .unwrap();

        // First 2 rows by age are 10 and 20; WITH TIES keeps the other age=20 peer.
        // ORDER BY age only (not id) so peers share the tie key.
        let rows = session
            .execute_sql(
                "SELECT name, age FROM users ORDER BY age \
                 FETCH FIRST 2 ROWS WITH TIES",
            )
            .unwrap();
        assert_eq!(rows.rows.len(), 3);
        let names: Vec<_> = rows.rows.iter().filter_map(|r| r.get("name")).collect();
        assert!(names.contains(&"Ada"));
        assert!(names.contains(&"Bob"));
        assert!(names.contains(&"Cy"));

        let only = session
            .execute_sql(
                "SELECT name FROM users ORDER BY age, id FETCH FIRST 2 ROWS ONLY",
            )
            .unwrap();
        assert_eq!(only.rows.len(), 2);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_order_by_nulls_first_last_e2e() {
        let (mut session, root) = temp_session("nulls-order");
        session
            .execute_sql(
                "INSERT INTO users (id, name, age) VALUES \
                 (1, 'Ada', 30), (2, 'Bob', NULL), (3, 'Cy', 10)",
            )
            .unwrap();

        let first = session
            .execute_sql("SELECT name FROM users ORDER BY age NULLS FIRST, id")
            .unwrap();
        assert_eq!(
            first
                .rows
                .iter()
                .filter_map(|r| r.get("name"))
                .collect::<Vec<_>>(),
            vec!["Bob", "Cy", "Ada"]
        );

        let last = session
            .execute_sql("SELECT name FROM users ORDER BY age NULLS LAST, id")
            .unwrap();
        assert_eq!(
            last
                .rows
                .iter()
                .filter_map(|r| r.get("name"))
                .collect::<Vec<_>>(),
            vec!["Cy", "Ada", "Bob"]
        );

        // PG default ASC → NULLS LAST
        let asc = session
            .execute_sql("SELECT name FROM users ORDER BY age, id")
            .unwrap();
        assert_eq!(asc.rows[2].get("name"), Some("Bob"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_truncate_table_e2e() {
        let (mut session, root) = temp_session("truncate");
        session
            .execute_sql(
                "INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30), (2, 'Bob', 20)",
            )
            .unwrap();

        let trunc = session.execute_sql("TRUNCATE TABLE users").unwrap();
        assert_eq!(trunc.tag, "TRUNCATE TABLE");

        let left = session.execute_sql("SELECT name FROM users").unwrap();
        assert!(left.rows.is_empty());

        let if_exists = session
            .execute_sql("TRUNCATE TABLE IF EXISTS ghost_table")
            .unwrap();
        assert_eq!(if_exists.tag, "TRUNCATE TABLE");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_is_distinct_from_e2e() {
        let (mut session, root) = temp_session("is-distinct");
        session
            .execute_sql(
                "INSERT INTO users (id, name, age) VALUES \
                 (1, 'Ada', 30), (2, 'Bob', NULL), (3, 'Cy', 10)",
            )
            .unwrap();

        let both_null = session
            .execute_sql(
                "SELECT name FROM users WHERE age IS NOT DISTINCT FROM NULL ORDER BY id",
            )
            .unwrap();
        assert_eq!(both_null.rows.len(), 1);
        assert_eq!(both_null.rows[0].get("name"), Some("Bob"));

        let dist = session
            .execute_sql(
                "SELECT name FROM users WHERE age IS DISTINCT FROM 30 ORDER BY id",
            )
            .unwrap();
        assert_eq!(dist.rows.len(), 2);
        assert_eq!(dist.rows[0].get("name"), Some("Bob"));
        assert_eq!(dist.rows[1].get("name"), Some("Cy"));

        let eqish = session
            .execute_sql(
                "SELECT name FROM users WHERE age IS NOT DISTINCT FROM 30 ORDER BY id",
            )
            .unwrap();
        assert_eq!(eqish.rows.len(), 1);
        assert_eq!(eqish.rows[0].get("name"), Some("Ada"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_case_when_e2e() {
        let (mut session, root) = temp_session("case-when");
        session
            .execute_sql(
                "INSERT INTO users (id, name, age) VALUES \
                 (1, 'Ada', 30), (2, 'Bob', 20), (3, 'Cy', 40)",
            )
            .unwrap();

        let searched = session
            .execute_sql(
                "SELECT name, \
                 CASE WHEN age >= 30 THEN 'senior' ELSE 'junior' END AS band \
                 FROM users ORDER BY name",
            )
            .unwrap();
        assert_eq!(searched.rows.len(), 3);
        let by_name: std::collections::BTreeMap<_, _> = searched
            .rows
            .iter()
            .filter_map(|r| {
                Some((
                    r.get("name")?.to_string(),
                    r.get("band")?.to_string(),
                ))
            })
            .collect();
        assert_eq!(by_name.get("Ada").map(String::as_str), Some("senior"));
        assert_eq!(by_name.get("Bob").map(String::as_str), Some("junior"));
        assert_eq!(by_name.get("Cy").map(String::as_str), Some("senior"));

        let simple = session
            .execute_sql(
                "SELECT CASE name WHEN 'Ada' THEN 'founder' ELSE 'other' END AS role \
                 FROM users WHERE id = 1",
            )
            .unwrap();
        assert_eq!(simple.rows.len(), 1);
        assert_eq!(simple.rows[0].get("role"), Some("founder"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_is_null_and_coalesce_e2e() {
        let (mut session, root) = temp_session("isnull-coalesce");
        session
            .execute_sql(
                "INSERT INTO users (id, name, age) VALUES \
                 (1, 'Ada', 30), (2, 'Bob', 20)",
            )
            .unwrap();

        let not_null = session
            .execute_sql("SELECT name FROM users WHERE name IS NOT NULL")
            .unwrap();
        assert_eq!(not_null.rows.len(), 2);

        let null_probe = session
            .execute_sql(
                "SELECT name FROM users \
                 WHERE CASE WHEN age > 100 THEN 'x' END IS NULL",
            )
            .unwrap();
        assert_eq!(null_probe.rows.len(), 2);

        let coal = session
            .execute_sql(
                "SELECT COALESCE(CASE WHEN age > 100 THEN 'x' END, name) AS n \
                 FROM users WHERE id = 1",
            )
            .unwrap();
        assert_eq!(coal.rows.len(), 1);
        assert_eq!(coal.rows[0].get("n"), Some("Ada"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_cast_and_nullif_e2e() {
        let (mut session, root) = temp_session("cast-nullif");
        session
            .execute_sql(
                "INSERT INTO users (id, name, age) VALUES \
                 (1, 'Ada', 30), (2, 'Bob', 20)",
            )
            .unwrap();

        let casted = session
            .execute_sql("SELECT CAST(age AS TEXT) AS a FROM users WHERE id = 1")
            .unwrap();
        assert_eq!(casted.rows[0].get("a"), Some("30"));

        let dcolon = session
            .execute_sql("SELECT (age + 0)::INT AS n FROM users WHERE id = 2")
            .unwrap();
        assert_eq!(dcolon.rows[0].get("n"), Some("20"));

        let nullif = session
            .execute_sql(
                "SELECT NULLIF(name, 'Ada') AS n FROM users WHERE id = 1",
            )
            .unwrap();
        assert!(
            nullif.rows[0].get("n").unwrap_or("").is_empty(),
            "NULLIF matching should yield NULL/empty"
        );

        let keep = session
            .execute_sql(
                "SELECT NULLIF(name, 'Ada') AS n FROM users WHERE id = 2",
            )
            .unwrap();
        assert_eq!(keep.rows[0].get("n"), Some("Bob"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_string_scalars_e2e() {
        let (mut session, root) = temp_session("string-scalars");
        session
            .execute_sql(
                "INSERT INTO users (id, name, age) VALUES \
                 (1, ' Ada ', 30), (2, 'Bob', 20)",
            )
            .unwrap();

        let row = session
            .execute_sql(
                "SELECT LOWER(name) AS lo, UPPER(TRIM(name)) AS up, LENGTH(TRIM(name)) AS n, \
                 SUBSTRING(TRIM(name) FROM 1 FOR 2) AS pref \
                 FROM users WHERE id = 1",
            )
            .unwrap();
        assert_eq!(row.rows.len(), 1);
        assert_eq!(row.rows[0].get("lo"), Some(" ada "));
        assert_eq!(row.rows[0].get("up"), Some("ADA"));
        assert_eq!(row.rows[0].get("n"), Some("3"));
        assert_eq!(row.rows[0].get("pref"), Some("Ad"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_octet_bit_length_e2e() {
        let (mut session, root) = temp_session("octet-bit-length");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();

        let row = session
            .execute_sql(
                "SELECT LENGTH('café') AS chars, \
                 OCTET_LENGTH('café') AS bytes, \
                 BIT_LENGTH('café') AS bits, \
                 OCTET_LENGTH('Ada') AS ascii \
                 FROM users WHERE id = 1",
            )
            .unwrap();
        // 'café' = 4 Unicode chars; é is 2 UTF-8 bytes → 5 octets, 40 bits
        assert_eq!(row.rows[0].get("chars"), Some("4"));
        assert_eq!(row.rows[0].get("bytes"), Some("5"));
        assert_eq!(row.rows[0].get("bits"), Some("40"));
        assert_eq!(row.rows[0].get("ascii"), Some("3"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_concat_replace_position_e2e() {
        let (mut session, root) = temp_session("concat-replace");
        session
            .execute_sql(
                "INSERT INTO users (id, name, age) VALUES \
                 (1, 'Ada', 30), (2, 'Bob', 20)",
            )
            .unwrap();

        let row = session
            .execute_sql(
                "SELECT CONCAT(name, '-', CAST(age AS TEXT)) AS c, \
                 name || '!' AS bang, \
                 REPLACE(name, 'a', 'A') AS r, \
                 POSITION('d' IN name) AS p, \
                 STRPOS(name, 'o') AS s \
                 FROM users WHERE id = 1",
            )
            .unwrap();
        assert_eq!(row.rows[0].get("c"), Some("Ada-30"));
        assert_eq!(row.rows[0].get("bang"), Some("Ada!"));
        assert_eq!(row.rows[0].get("r"), Some("AdA"));
        assert_eq!(row.rows[0].get("p"), Some("2"));

        let bob = session
            .execute_sql(
                "SELECT STRPOS(name, 'o') AS s FROM users WHERE id = 2",
            )
            .unwrap();
        assert_eq!(bob.rows[0].get("s"), Some("2"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_math_scalars_e2e() {
        let (mut session, root) = temp_session("math-scalars");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();

        let row = session
            .execute_sql(
                "SELECT ABS(0 - age) AS a, ROUND(age / 2.0) AS r, \
                 CEIL(age / 4.0) AS c, FLOOR(age / 4.0) AS f, \
                 MOD(age, 7) AS m, POWER(2, 3) AS p \
                 FROM users WHERE id = 1",
            )
            .unwrap();
        assert_eq!(row.rows[0].get("a"), Some("30"));
        assert_eq!(row.rows[0].get("r"), Some("15"));
        assert_eq!(row.rows[0].get("c"), Some("8"));
        assert_eq!(row.rows[0].get("f"), Some("7"));
        assert_eq!(row.rows[0].get("m"), Some("2"));
        assert_eq!(row.rows[0].get("p"), Some("8"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_not_and_now_e2e() {
        let (mut session, root) = temp_session("not-now");
        session
            .execute_sql(
                "INSERT INTO users (id, name, age) VALUES \
                 (1, 'Ada', 30), (2, 'Bob', 15)",
            )
            .unwrap();

        let adults = session
            .execute_sql("SELECT name FROM users WHERE NOT (age < 18)")
            .unwrap();
        assert_eq!(adults.rows.len(), 1);
        assert_eq!(adults.rows[0].get("name"), Some("Ada"));

        let now = session
            .execute_sql("SELECT NOW() AS t, CURRENT_DATE AS d FROM users WHERE id = 1")
            .unwrap();
        let t = now.rows[0].get("t").unwrap();
        let d = now.rows[0].get("d").unwrap();
        assert!(t.len() >= 19, "NOW() too short: {t}");
        assert_eq!(d.len(), 10);
        assert!(t.starts_with(d), "NOW should start with CURRENT_DATE: {t} vs {d}");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_clock_statement_timestamps_e2e() {
        let (mut session, root) = temp_session("clock-stmt-ts");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();

        let row = session
            .execute_sql(
                "SELECT NOW() AS n, \
                 STATEMENT_TIMESTAMP() AS s, \
                 TRANSACTION_TIMESTAMP() AS x, \
                 CLOCK_TIMESTAMP() AS c, \
                 CURRENT_DATE AS d, \
                 CURRENT_TIME AS t \
                 FROM users WHERE id = 1",
            )
            .unwrap();
        let n = row.rows[0].get("n").unwrap();
        let s = row.rows[0].get("s").unwrap();
        let x = row.rows[0].get("x").unwrap();
        let c = row.rows[0].get("c").unwrap();
        let d = row.rows[0].get("d").unwrap();
        let t = row.rows[0].get("t").unwrap();
        assert_eq!(n, s, "NOW should equal STATEMENT_TIMESTAMP");
        assert_eq!(s, x, "STATEMENT_TIMESTAMP should equal TRANSACTION_TIMESTAMP");
        assert!(c.len() >= 19, "CLOCK_TIMESTAMP too short: {c}");
        assert!(n.starts_with(d), "NOW vs CURRENT_DATE: {n} / {d}");
        assert!(t.contains(':'), "CURRENT_TIME unexpected: {t}");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_timeofday_e2e() {
        let (mut session, root) = temp_session("timeofday");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();

        let row = session
            .execute_sql("SELECT TIMEOFDAY() AS t FROM users WHERE id = 1")
            .unwrap();
        let t = row.rows[0].get("t").unwrap();
        assert!(t.ends_with(" UTC"), "TIMEOFDAY should end with UTC: {t}");
        assert!(t.contains(':'), "TIMEOFDAY missing time: {t}");
        assert!(
            t.len() >= 28,
            "TIMEOFDAY too short (expected weekday+date+time): {t}"
        );
        let year = &t[t.len() - 8..t.len() - 4];
        assert!(
            year.chars().all(|c| c.is_ascii_digit()),
            "TIMEOFDAY year missing: {t}"
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_current_user_schema_catalog_e2e() {
        let (mut session, root) = temp_session("session-identity");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();

        let row = session
            .execute_sql(
                "SELECT CURRENT_USER AS u, SESSION_USER AS s, USER AS usr, \
                 CURRENT_SCHEMA() AS sch, CURRENT_CATALOG AS cat \
                 FROM users WHERE id = 1",
            )
            .unwrap();
        assert_eq!(row.rows[0].get("u"), Some("postgres"));
        assert_eq!(row.rows[0].get("s"), Some("postgres"));
        assert_eq!(row.rows[0].get("usr"), Some("postgres"));
        assert_eq!(row.rows[0].get("sch"), Some("public"));
        assert_eq!(row.rows[0].get("cat"), Some("postgres"));

        session
            .execute_sql("SET search_path TO myschema, public")
            .unwrap();
        let sch = session
            .execute_sql("SELECT CURRENT_SCHEMA() AS sch FROM users WHERE id = 1")
            .unwrap();
        assert_eq!(sch.rows[0].get("sch"), Some("myschema"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_current_schemas_e2e() {
        let (mut session, root) = temp_session("current-schemas");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();

        let def = session
            .execute_sql(
                "SELECT current_schemas(false) AS s, current_schemas(true) AS i \
                 FROM users WHERE id = 1",
            )
            .unwrap();
        assert_eq!(def.rows[0].get("s"), Some("[public]"));
        assert_eq!(def.rows[0].get("i"), Some("[pg_catalog,public]"));

        session
            .execute_sql("SET search_path TO myschema, public")
            .unwrap();
        let multi = session
            .execute_sql(
                "SELECT current_schemas(false) AS s, current_schemas(true) AS i \
                 FROM users WHERE id = 1",
            )
            .unwrap();
        assert_eq!(multi.rows[0].get("s"), Some("[myschema,public]"));
        assert_eq!(
            multi.rows[0].get("i"),
            Some("[pg_catalog,myschema,public]")
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_pg_backend_pid_recovery_e2e() {
        let (mut session, root) = temp_session("pg-backend-pid");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();

        let row = session
            .execute_sql(
                "SELECT pg_backend_pid() AS pid, pg_is_in_recovery() AS rec \
                 FROM users WHERE id = 1",
            )
            .unwrap();
        let pid: i64 = row.rows[0].get("pid").unwrap().parse().unwrap();
        assert!(pid > 0, "pg_backend_pid should be positive, got {pid}");
        assert_eq!(
            pid,
            std::process::id() as i64,
            "pg_backend_pid should match OS pid"
        );
        assert_eq!(row.rows[0].get("rec"), Some("false"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_pg_jit_available_e2e() {
        let (mut session, root) = temp_session("pg-jit-available");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();

        let row = session
            .execute_sql(
                "SELECT pg_jit_available() AS jit FROM users WHERE id = 1",
            )
            .unwrap();
        assert_eq!(row.rows[0].get("jit"), Some("true"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_current_query_e2e() {
        let (mut session, root) = temp_session("current-query");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();

        let sql = "SELECT current_query() AS q FROM users WHERE id = 1";
        let row = session.execute_sql(sql).unwrap();
        let q = row.rows[0].get("q").unwrap();
        assert!(
            q.to_lowercase().contains("current_query"),
            "current_query should echo statement text, got {q}"
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_pg_reload_rotate_logfile_e2e() {
        let (mut session, root) = temp_session("pg-reload-rotate");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();

        let row = session
            .execute_sql(
                "SELECT pg_reload_conf() AS reload, pg_rotate_logfile() AS rot \
                 FROM users WHERE id = 1",
            )
            .unwrap();
        assert_eq!(row.rows[0].get("reload"), Some("true"));
        assert_eq!(row.rows[0].get("rot"), Some("true"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_pg_conf_load_time_e2e() {
        let (mut session, root) = temp_session("pg-conf-load-time");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();

        let before = session
            .execute_sql("SELECT pg_conf_load_time() AS t FROM users WHERE id = 1")
            .unwrap()
            .rows[0]
            .get("t")
            .unwrap()
            .to_string();
        assert!(!before.is_empty());

        session
            .execute_sql("SELECT pg_reload_conf() FROM users WHERE id = 1")
            .unwrap();

        let after = session
            .execute_sql("SELECT pg_conf_load_time() AS t FROM users WHERE id = 1")
            .unwrap()
            .rows[0]
            .get("t")
            .unwrap()
            .to_string();
        assert_ne!(before, after);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_sequence_nextval_e2e() {
        let (mut session, root) = temp_session("pg-sequence-nextval");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();
        let seq = format!("orders_id_seq_{}", session.session_id);

        let row = session
            .execute_sql(&format!(
                "SELECT nextval('{seq}') AS a, nextval('{seq}') AS b, \
                 currval('{seq}') AS c, lastval() AS d \
                 FROM users WHERE id = 1",
            ))
            .unwrap();
        assert_eq!(row.rows[0].get("a"), Some("1"));
        assert_eq!(row.rows[0].get("b"), Some("2"));
        assert_eq!(row.rows[0].get("c"), Some("2"));
        assert_eq!(row.rows[0].get("d"), Some("2"));

        let row = session
            .execute_sql(&format!(
                "SELECT setval('{seq}', 50) AS s, nextval('{seq}') AS n \
                 FROM users WHERE id = 1",
            ))
            .unwrap();
        assert_eq!(row.rows[0].get("s"), Some("50"));
        assert_eq!(row.rows[0].get("n"), Some("51"));

        let row = session
            .execute_sql(&format!(
                "SELECT setval('{seq}', 7, false) AS s, nextval('{seq}') AS n \
                 FROM users WHERE id = 1",
            ))
            .unwrap();
        assert_eq!(row.rows[0].get("s"), Some("7"));
        assert_eq!(row.rows[0].get("n"), Some("7"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_create_drop_sequence_e2e() {
        let (mut session, root) = temp_session("pg-create-drop-sequence");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();
        let seq = format!("ddl_seq_{}", session.session_id);

        assert_eq!(
            session
                .execute_sql(&format!(
                    "CREATE SEQUENCE {seq} START WITH 10 INCREMENT BY 5"
                ))
                .unwrap()
                .tag,
            "CREATE SEQUENCE"
        );
        assert!(session
            .execute_sql(&format!("CREATE SEQUENCE {seq}"))
            .is_err());
        session
            .execute_sql(&format!("CREATE SEQUENCE IF NOT EXISTS {seq}"))
            .unwrap();

        let row = session
            .execute_sql(&format!(
                "SELECT nextval('{seq}') AS a, nextval('{seq}') AS b FROM users WHERE id = 1"
            ))
            .unwrap();
        assert_eq!(row.rows[0].get("a"), Some("10"));
        assert_eq!(row.rows[0].get("b"), Some("15"));

        assert_eq!(
            session
                .execute_sql(&format!("DROP SEQUENCE {seq}"))
                .unwrap()
                .tag,
            "DROP SEQUENCE"
        );
        session
            .execute_sql(&format!("DROP SEQUENCE IF EXISTS {seq}"))
            .unwrap();
        assert!(session
            .execute_sql(&format!("DROP SEQUENCE {seq}"))
            .is_err());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_alter_sequence_serial_e2e() {
        let (mut session, root) = temp_session("pg-alter-sequence-serial");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();
        let seq = format!("serial_seq_{}", session.session_id);
        let owner_table = format!("t{}", session.session_id);

        session
            .execute_sql(&format!("CREATE SEQUENCE {seq} START WITH 1"))
            .unwrap();
        session
            .execute_sql(&format!(
                "ALTER SEQUENCE {seq} RESTART WITH 20 INCREMENT BY 3 OWNED BY {owner_table}.id"
            ))
            .unwrap();

        let row = session
            .execute_sql(&format!(
                "SELECT pg_get_serial_sequence('{owner_table}', 'id') AS s, \
                 pg_get_serial_sequence('{owner_table}', 'name') AS miss \
                 FROM users WHERE id = 1",
            ))
            .unwrap();
        assert_eq!(
            row.rows[0].get("s"),
            Some(format!("public.{seq}").as_str())
        );
        assert_eq!(row.rows[0].get("miss"), Some(""));

        let row = session
            .execute_sql(&format!(
                "SELECT nextval('{seq}') AS a, nextval('{seq}') AS b FROM users WHERE id = 1"
            ))
            .unwrap();
        assert_eq!(row.rows[0].get("a"), Some("20"));
        assert_eq!(row.rows[0].get("b"), Some("23"));

        session
            .execute_sql(&format!("ALTER SEQUENCE {seq} OWNED BY NONE"))
            .unwrap();
        let row = session
            .execute_sql(&format!(
                "SELECT pg_get_serial_sequence('{owner_table}', 'id') AS s FROM users WHERE id = 1",
            ))
            .unwrap();
        assert_eq!(row.rows[0].get("s"), Some(""));

        session.execute_sql(&format!("DROP SEQUENCE {seq}")).unwrap();
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_create_table_serial_e2e() {
        let (mut session, root) = temp_session("pg-create-table-serial");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();
        let table = format!("serials_{}", session.session_id);
        session
            .execute_sql(&format!(
                "CREATE TABLE {table} (id SERIAL PRIMARY KEY, name TEXT)"
            ))
            .unwrap();

        let schema = session.engine.table_schema(&table).unwrap();
        assert_eq!(schema.columns[0].data_type, "INT");

        let row = session
            .execute_sql(&format!(
                "SELECT pg_get_serial_sequence('{table}', 'id') AS s \
                 FROM users WHERE id = 1"
            ))
            .unwrap();
        let expect = format!("public.{table}_id_seq");
        assert_eq!(row.rows[0].get("s"), Some(expect.as_str()));

        let row = session
            .execute_sql(&format!(
                "SELECT nextval('{table}_id_seq') AS a, nextval('{table}_id_seq') AS b \
                 FROM users WHERE id = 1"
            ))
            .unwrap();
        assert_eq!(row.rows[0].get("a"), Some("1"));
        assert_eq!(row.rows[0].get("b"), Some("2"));

        session.execute_sql(&format!("DROP TABLE {table}")).unwrap();
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_serial_survives_engine_reopen() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("takyonic-pg-serial-reopen-{nanos}"));
        let _ = fs::remove_dir_all(&root);
        let table = format!("ser_{nanos}");
        {
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
            let mut session = SessionState::new(engine);
            session
                .execute_sql(&format!(
                    "CREATE TABLE {table} (id SERIAL PRIMARY KEY, name TEXT)"
                ))
                .unwrap();
            session
                .execute_sql(&format!(
                    "INSERT INTO {table} (name) VALUES ('a'), ('b')"
                ))
                .unwrap();
            let rows = session
                .execute_sql(&format!("SELECT id FROM {table} ORDER BY id"))
                .unwrap();
            assert_eq!(rows.rows[0].get("id"), Some("1"));
            assert_eq!(rows.rows[1].get("id"), Some("2"));
        }
        {
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
            let mut session = SessionState::new(engine);
            let rows = session
                .execute_sql(&format!(
                    "INSERT INTO {table} (name) VALUES ('c') RETURNING id"
                ))
                .unwrap();
            assert_eq!(rows.rows[0].get("id"), Some("3"));
        }
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_default_not_null_unique_e2e() {
        let (mut session, root) = temp_session("pg-default-nn-uq");
        let table = format!("defs_{}", session.session_id);
        session
            .execute_sql(&format!(
                "CREATE TABLE {table} (\
                    id BIGINT PRIMARY KEY, \
                    email TEXT NOT NULL UNIQUE, \
                    label TEXT DEFAULT 'x'\
                )"
            ))
            .unwrap();
        session
            .execute_sql(&format!(
                "INSERT INTO {table} (id, email) VALUES (1, 'a@b.c')"
            ))
            .unwrap();
        let rows = session
            .execute_sql(&format!("SELECT id, email, label FROM {table}"))
            .unwrap();
        assert_eq!(rows.rows[0].get("label"), Some("x"));
        let err = session
            .execute_sql(&format!(
                "INSERT INTO {table} (id, email) VALUES (2, NULL)"
            ))
            .unwrap_err();
        assert!(
            err.to_string().contains("not-null") || err.to_string().contains("null value"),
            "{err}"
        );
        let err = session
            .execute_sql(&format!(
                "INSERT INTO {table} (id, email) VALUES (3, 'a@b.c')"
            ))
            .unwrap_err();
        assert!(
            err.to_string().to_ascii_lowercase().contains("unique"),
            "{err}"
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_copy_file_roundtrip_e2e() {
        let (mut session, root) = temp_session("pg-copy-file");
        let table = format!("cpy_{}", session.session_id);
        session
            .execute_sql(&format!(
                "CREATE TABLE {table} (id BIGINT PRIMARY KEY, name TEXT)"
            ))
            .unwrap();
        session
            .execute_sql(&format!(
                "INSERT INTO {table} (id, name) VALUES (1, 'Ada'), (2, 'Grace')"
            ))
            .unwrap();
        let path = root.join("out.tsv");
        let path_s = path.to_string_lossy();
        session
            .execute_sql(&format!("COPY {table} TO '{path_s}'"))
            .unwrap();
        session
            .execute_sql(&format!("DELETE FROM {table}"))
            .unwrap();
        session
            .execute_sql(&format!("COPY {table} FROM '{path_s}'"))
            .unwrap();
        let rows = session
            .execute_sql(&format!("SELECT id, name FROM {table} ORDER BY id"))
            .unwrap();
        assert_eq!(rows.rows.len(), 2);
        assert_eq!(rows.rows[0].get("name"), Some("Ada"));
        assert_eq!(rows.rows[1].get("name"), Some("Grace"));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_serial_insert_default_and_drop_e2e() {
        let (mut session, root) = temp_session("pg-serial-insert-drop");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();
        let table = format!("auto_{}", session.session_id);
        session
            .execute_sql(&format!(
                "CREATE TABLE {table} (id SERIAL PRIMARY KEY, name TEXT)"
            ))
            .unwrap();

        session
            .execute_sql(&format!(
                "INSERT INTO {table} (name) VALUES ('Ada'), ('Grace')"
            ))
            .unwrap();

        let rows = session
            .execute_sql(&format!(
                "SELECT id, name FROM {table} ORDER BY id"
            ))
            .unwrap();
        assert_eq!(rows.rows.len(), 2);
        assert_eq!(rows.rows[0].get("id"), Some("1"));
        assert_eq!(rows.rows[0].get("name"), Some("Ada"));
        assert_eq!(rows.rows[1].get("id"), Some("2"));
        assert_eq!(rows.rows[1].get("name"), Some("Grace"));

        let seq = format!("{table}_id_seq");
        assert_eq!(
            crate::sql::pg_get_serial_sequence(&table, "id"),
            Some(format!("public.{seq}"))
        );
        session.execute_sql(&format!("DROP TABLE {table}")).unwrap();
        assert_eq!(crate::sql::pg_get_serial_sequence(&table, "id"), None);
        // Owned sequence was removed with the table.
        crate::sql::create_sequence(&seq, false, 1, 1).unwrap();
        crate::sql::drop_sequence(&seq, false).unwrap();

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_alter_add_drop_serial_column_e2e() {
        let (mut session, root) = temp_session("pg-alter-add-serial");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();
        let table = format!("t_{}", session.session_id);
        session
            .execute_sql(&format!(
                "CREATE TABLE {table} (id BIGINT PRIMARY KEY, name TEXT)"
            ))
            .unwrap();

        session
            .execute_sql(&format!("ALTER TABLE {table} ADD COLUMN sid SERIAL"))
            .unwrap();
        let schema = session.engine.table_schema(&table).unwrap();
        let sid = schema.columns.iter().find(|c| c.name == "sid").unwrap();
        assert_eq!(sid.data_type, "INT");
        assert_eq!(
            crate::sql::pg_get_serial_sequence(&table, "sid"),
            Some(format!("public.{table}_sid_seq"))
        );

        session
            .execute_sql(&format!("INSERT INTO {table} (id, name) VALUES (1, 'a')"))
            .unwrap();
        let row = session
            .execute_sql(&format!(
                "SELECT sid, name FROM {table} WHERE id = 1"
            ))
            .unwrap();
        assert_eq!(row.rows[0].get("sid"), Some("1"));
        assert_eq!(row.rows[0].get("name"), Some("a"));

        session
            .execute_sql(&format!("ALTER TABLE {table} DROP COLUMN sid"))
            .unwrap();
        assert_eq!(crate::sql::pg_get_serial_sequence(&table, "sid"), None);

        session.execute_sql(&format!("DROP TABLE {table}")).unwrap();
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_pg_sequence_last_value_e2e() {
        let (mut session, root) = temp_session("pg-sequence-last-value");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();
        let seq = format!("lastv_{}", session.session_id);
        session
            .execute_sql(&format!("CREATE SEQUENCE {seq}"))
            .unwrap();

        let row = session
            .execute_sql(&format!(
                "SELECT pg_sequence_last_value('{seq}') AS v FROM users WHERE id = 1"
            ))
            .unwrap();
        assert_eq!(row.rows[0].get("v"), Some(""));

        session
            .execute_sql(&format!(
                "SELECT nextval('{seq}') FROM users WHERE id = 1"
            ))
            .unwrap();
        let row = session
            .execute_sql(&format!(
                "SELECT pg_sequence_last_value('{seq}') AS v, nextval('{seq}') AS n \
                 FROM users WHERE id = 1"
            ))
            .unwrap();
        assert_eq!(row.rows[0].get("v"), Some("1"));
        assert_eq!(row.rows[0].get("n"), Some("2"));

        let row = session
            .execute_sql(&format!(
                "SELECT pg_sequence_last_value('{seq}') AS v FROM users WHERE id = 1"
            ))
            .unwrap();
        assert_eq!(row.rows[0].get("v"), Some("2"));

        session.execute_sql(&format!("DROP SEQUENCE {seq}")).unwrap();
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_alter_sequence_rename_e2e() {
        let (mut session, root) = temp_session("pg-alter-sequence-rename");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();
        let seq = format!("ren_{}", session.session_id);
        let seq2 = format!("ren2_{}", session.session_id);
        session
            .execute_sql(&format!("CREATE SEQUENCE {seq} START WITH 5"))
            .unwrap();
        session
            .execute_sql(&format!("ALTER SEQUENCE {seq} RENAME TO {seq2}"))
            .unwrap();

        let row = session
            .execute_sql(&format!(
                "SELECT nextval('{seq2}') AS n FROM users WHERE id = 1"
            ))
            .unwrap();
        assert_eq!(row.rows[0].get("n"), Some("5"));
        assert!(crate::sql::pg_sequence_last_value(&seq).is_err());

        session.execute_sql(&format!("DROP SEQUENCE {seq2}")).unwrap();
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_has_sequence_privilege_e2e() {
        let (mut session, root) = temp_session("pg-has-sequence-privilege");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();
        let seq = format!("priv_{}", session.session_id);
        session
            .execute_sql(&format!("CREATE SEQUENCE {seq}"))
            .unwrap();

        let row = session
            .execute_sql(&format!(
                "SELECT has_sequence_privilege('{seq}', 'USAGE') AS u, \
                 has_sequence_privilege('{seq}', 'SELECT, UPDATE') AS su, \
                 has_sequence_privilege('postgres', '{seq}', 'ALL') AS allp \
                 FROM users WHERE id = 1"
            ))
            .unwrap();
        assert_eq!(row.rows[0].get("u"), Some("true"));
        assert_eq!(row.rows[0].get("su"), Some("true"));
        assert_eq!(row.rows[0].get("allp"), Some("true"));

        assert!(session
            .execute_sql(
                "SELECT has_sequence_privilege('no_such_seq', 'USAGE') FROM users WHERE id = 1"
            )
            .is_err());

        session.execute_sql(&format!("DROP SEQUENCE {seq}")).unwrap();
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_pg_notify_queue_usage_e2e() {
        let (mut session, root) = temp_session("pg-notify-queue");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();

        let row = session
            .execute_sql(
                "SELECT pg_notify('ch', 'hello') AS n, \
                 pg_notification_queue_usage() AS q, \
                 pg_listening_channels() AS chans \
                 FROM users WHERE id = 1",
            )
            .unwrap();
        assert_eq!(row.rows[0].get("n"), Some(""));
        assert_eq!(row.rows[0].get("q"), Some("0"));
        assert_eq!(row.rows[0].get("chans"), Some("[]"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_listen_unlisten_e2e() {
        let (mut session, root) = temp_session("pg-listen-unlisten");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();

        assert_eq!(
            session.execute_sql("LISTEN alerts").unwrap().tag,
            "LISTEN"
        );
        let row = session
            .execute_sql("SELECT pg_listening_channels() AS chans FROM users WHERE id = 1")
            .unwrap();
        assert_eq!(row.rows[0].get("chans"), Some("[alerts]"));

        session.execute_sql("LISTEN jobs").unwrap();
        let row = session
            .execute_sql("SELECT pg_listening_channels() AS chans FROM users WHERE id = 1")
            .unwrap();
        assert_eq!(row.rows[0].get("chans"), Some("[alerts,jobs]"));

        session.execute_sql("UNLISTEN alerts").unwrap();
        let row = session
            .execute_sql("SELECT pg_listening_channels() AS chans FROM users WHERE id = 1")
            .unwrap();
        assert_eq!(row.rows[0].get("chans"), Some("[jobs]"));

        session.execute_sql("UNLISTEN *").unwrap();
        let row = session
            .execute_sql("SELECT pg_listening_channels() AS chans FROM users WHERE id = 1")
            .unwrap();
        assert_eq!(row.rows[0].get("chans"), Some("[]"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_notify_delivery_e2e() {
        let (mut session, root) = temp_session("pg-notify-delivery");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();

        assert_eq!(
            session
                .execute_sql("SELECT pg_notification_queue_usage() AS q FROM users WHERE id = 1")
                .unwrap()
                .rows[0]
                .get("q"),
            Some("0")
        );

        session.execute_sql("LISTEN alerts").unwrap();
        assert_eq!(
            session
                .execute_sql("NOTIFY alerts, 'hello'")
                .unwrap()
                .tag,
            "NOTIFY"
        );

        let row = session
            .execute_sql("SELECT pg_notification_queue_usage() AS q FROM users WHERE id = 1")
            .unwrap();
        assert_eq!(row.rows[0].get("q"), Some("0.001"));

        session
            .execute_sql("SELECT pg_notify('alerts', 'via-fn') FROM users WHERE id = 1")
            .unwrap();
        let row = session
            .execute_sql("SELECT pg_notification_queue_usage() AS q FROM users WHERE id = 1")
            .unwrap();
        assert_eq!(row.rows[0].get("q"), Some("0.002"));

        session.execute_sql("UNLISTEN *").unwrap();
        let _ = crate::sql::drain_notifications(session.session_id);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_pg_signal_backend_e2e() {
        let (mut session, root) = temp_session("pg-signal-backend");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();

        let row = session
            .execute_sql(
                "SELECT pg_cancel_backend(pg_backend_pid()) AS self_c, \
                 pg_terminate_backend(pg_backend_pid()) AS self_t, \
                 pg_cancel_backend(0) AS z, \
                 pg_terminate_backend(999999999) AS miss \
                 FROM users WHERE id = 1",
            )
            .unwrap();
        assert_eq!(row.rows[0].get("self_c"), Some("true"));
        assert_eq!(row.rows[0].get("self_t"), Some("true"));
        assert_eq!(row.rows[0].get("z"), Some("false"));
        assert_eq!(row.rows[0].get("miss"), Some("false"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_pg_wal_lsn_e2e() {
        let (mut session, root) = temp_session("pg-wal-lsn");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();

        let row = session
            .execute_sql(
                "SELECT pg_current_wal_lsn() AS cur, \
                 pg_current_wal_insert_lsn() AS ins, \
                 pg_current_wal_flush_lsn() AS flush, \
                 pg_wal_lsn_diff(pg_current_wal_lsn(), pg_current_wal_lsn()) AS d0, \
                 pg_wal_lsn_diff('0/01000010', '0/01000000') AS d \
                 FROM users WHERE id = 1",
            )
            .unwrap();
        let cur = row.rows[0].get("cur").unwrap();
        assert!(cur.contains('/'), "lsn format: {cur}");
        assert_eq!(row.rows[0].get("ins"), Some(cur));
        assert_eq!(row.rows[0].get("flush"), Some(cur));
        assert_eq!(row.rows[0].get("d0"), Some("0"));
        assert_eq!(row.rows[0].get("d"), Some("16"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_pg_walfile_name_e2e() {
        let (mut session, root) = temp_session("pg-walfile-name");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();

        let row = session
            .execute_sql(
                "SELECT pg_walfile_name(pg_current_wal_lsn()) AS f, \
                 pg_walfile_name_offset('0/01000010') AS fo, \
                 pg_walfile_name('bad') AS miss \
                 FROM users WHERE id = 1",
            )
            .unwrap();
        let f = row.rows[0].get("f").unwrap();
        assert_eq!(f.len(), 24, "walfile name should be 24 hex chars: {f}");
        assert!(f.chars().all(|c| c.is_ascii_hexdigit()), "hex: {f}");
        assert_eq!(
            row.rows[0].get("fo"),
            Some("000000010000000000000001,16")
        );
        assert_eq!(row.rows[0].get("miss"), Some(""));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_pg_switch_wal_e2e() {
        let (mut session, root) = temp_session("pg-switch-wal");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();

        let before = session
            .execute_sql("SELECT pg_current_wal_lsn() AS c FROM users WHERE id = 1")
            .unwrap();
        let b = before.rows[0].get("c").unwrap().to_string();

        let row = session
            .execute_sql(
                "SELECT pg_switch_wal() AS sw, \
                 pg_wal_lsn_diff(pg_current_wal_lsn(), pg_current_wal_lsn()) AS z \
                 FROM users WHERE id = 1",
            )
            .unwrap();
        let sw = row.rows[0].get("sw").unwrap();
        assert_ne!(sw, b.as_str(), "switch should advance LSN from {b}");
        assert!(sw.contains('/'));
        assert_eq!(row.rows[0].get("z"), Some("0"));

        let after = session
            .execute_sql("SELECT pg_current_wal_lsn() AS c FROM users WHERE id = 1")
            .unwrap();
        assert_eq!(after.rows[0].get("c"), Some(sw));

        let alias = session
            .execute_sql("SELECT pg_switch_xlog() AS x FROM users WHERE id = 1")
            .unwrap();
        assert_ne!(alias.rows[0].get("x"), Some(sw));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_pg_standby_wal_e2e() {
        let (mut session, root) = temp_session("pg-standby-wal");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();

        let row = session
            .execute_sql(
                "SELECT pg_last_wal_receive_lsn() AS recv, \
                 pg_last_wal_replay_lsn() AS replay, \
                 pg_last_xact_replay_timestamp() AS ts, \
                 pg_is_wal_replay_paused() AS paused \
                 FROM users WHERE id = 1",
            )
            .unwrap();
        // Primary / non-standby: receive/replay LSNs and replay timestamp are NULL.
        assert_eq!(row.rows[0].get("recv"), Some(""));
        assert_eq!(row.rows[0].get("replay"), Some(""));
        assert_eq!(row.rows[0].get("ts"), Some(""));
        assert_eq!(row.rows[0].get("paused"), Some("false"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_pg_wal_replay_pause_e2e() {
        let (mut session, root) = temp_session("pg-wal-replay-pause");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();

        // Ensure clean state (other tests may have toggled the process-global flag).
        session
            .execute_sql("SELECT pg_wal_replay_resume() FROM users WHERE id = 1")
            .unwrap();
        let off = session
            .execute_sql(
                "SELECT pg_is_wal_replay_paused() AS p FROM users WHERE id = 1",
            )
            .unwrap();
        assert_eq!(off.rows[0].get("p"), Some("false"));

        session
            .execute_sql("SELECT pg_wal_replay_pause() FROM users WHERE id = 1")
            .unwrap();
        let on = session
            .execute_sql(
                "SELECT pg_is_wal_replay_paused() AS p FROM users WHERE id = 1",
            )
            .unwrap();
        assert_eq!(on.rows[0].get("p"), Some("true"));

        session
            .execute_sql("SELECT pg_wal_replay_resume() FROM users WHERE id = 1")
            .unwrap();
        let again = session
            .execute_sql(
                "SELECT pg_is_wal_replay_paused() AS p FROM users WHERE id = 1",
            )
            .unwrap();
        assert_eq!(again.rows[0].get("p"), Some("false"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_pg_backup_e2e() {
        let (mut session, root) = temp_session("pg-backup");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();

        // Clear any leftover process-global backup state.
        let _ = session.execute_sql("SELECT pg_backup_stop() FROM users WHERE id = 1");

        let idle = session
            .execute_sql(
                "SELECT pg_is_in_backup() AS b, pg_backup_start_time() AS t \
                 FROM users WHERE id = 1",
            )
            .unwrap();
        assert_eq!(idle.rows[0].get("b"), Some("false"));
        assert_eq!(idle.rows[0].get("t"), Some(""));

        let start = session
            .execute_sql(
                "SELECT pg_backup_start('tick162') AS lsn, pg_is_in_backup() AS b \
                 FROM users WHERE id = 1",
            )
            .unwrap();
        assert!(start.rows[0].get("lsn").unwrap().contains('/'));
        assert_eq!(start.rows[0].get("b"), Some("true"));

        let during = session
            .execute_sql(
                "SELECT pg_backup_start_time() AS t FROM users WHERE id = 1",
            )
            .unwrap();
        assert!(!during.rows[0].get("t").unwrap().is_empty());

        let stop = session
            .execute_sql(
                "SELECT pg_stop_backup() AS lsn, pg_is_in_backup() AS b \
                 FROM users WHERE id = 1",
            )
            .unwrap();
        assert!(stop.rows[0].get("lsn").unwrap().contains('/'));
        assert_eq!(stop.rows[0].get("b"), Some("false"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_pg_create_restore_point_e2e() {
        let (mut session, root) = temp_session("pg-create-restore-point");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();

        let row = session
            .execute_sql(
                "SELECT pg_create_restore_point('tick163') AS rp, \
                 pg_current_wal_lsn() AS cur \
                 FROM users WHERE id = 1",
            )
            .unwrap();
        let rp = row.rows[0].get("rp").unwrap();
        let cur = row.rows[0].get("cur").unwrap();
        assert!(rp.contains('/'), "restore point LSN: {rp}");
        assert_eq!(rp, cur);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_pg_promote_e2e() {
        let (mut session, root) = temp_session("pg-promote");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();

        let row = session
            .execute_sql(
                "SELECT pg_promote() AS p0, pg_promote(false) AS p1, \
                 pg_is_in_recovery() AS rec FROM users WHERE id = 1",
            )
            .unwrap();
        assert_eq!(row.rows[0].get("p0"), Some("false"));
        assert_eq!(row.rows[0].get("p1"), Some("false"));
        assert_eq!(row.rows[0].get("rec"), Some("false"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_pg_typeof_encoding_e2e() {
        let (mut session, root) = temp_session("pg-typeof");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();

        let row = session
            .execute_sql(
                "SELECT pg_typeof(42) AS i, pg_typeof(1.5) AS f, pg_typeof(true) AS b, \
                 pg_typeof('hi') AS t, pg_typeof(NULL) AS n, \
                 pg_typeof(INTERVAL '1 day') AS iv, \
                 getdatabaseencoding() AS enc \
                 FROM users WHERE id = 1",
            )
            .unwrap();
        assert_eq!(row.rows[0].get("i"), Some("bigint"));
        assert_eq!(row.rows[0].get("f"), Some("double precision"));
        assert_eq!(row.rows[0].get("b"), Some("boolean"));
        assert_eq!(row.rows[0].get("t"), Some("text"));
        assert_eq!(row.rows[0].get("n"), Some(""));
        assert_eq!(row.rows[0].get("iv"), Some("interval"));
        assert_eq!(row.rows[0].get("enc"), Some("UTF8"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_pg_encoding_char_e2e() {
        let (mut session, root) = temp_session("pg-encoding-char");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();

        let row = session
            .execute_sql(
                "SELECT pg_encoding_to_char(6) AS utf, \
                 pg_encoding_to_char(0) AS ascii, \
                 pg_encoding_to_char(999) AS bad, \
                 pg_char_to_encoding('UTF8') AS id, \
                 pg_char_to_encoding(getdatabaseencoding()) AS round, \
                 pg_char_to_encoding('nope') AS miss \
                 FROM users WHERE id = 1",
            )
            .unwrap();
        assert_eq!(row.rows[0].get("utf"), Some("UTF8"));
        assert_eq!(row.rows[0].get("ascii"), Some("SQL_ASCII"));
        assert_eq!(row.rows[0].get("bad"), Some(""));
        assert_eq!(row.rows[0].get("id"), Some("6"));
        assert_eq!(row.rows[0].get("round"), Some("6"));
        assert_eq!(row.rows[0].get("miss"), Some("-1"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_pg_table_type_is_visible_e2e() {
        let (mut session, root) = temp_session("pg-is-visible");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();

        let row = session
            .execute_sql(
                "SELECT pg_table_is_visible('users') AS t, \
                 pg_table_is_visible(to_regclass('users')) AS oid_t, \
                 pg_table_is_visible('missing') AS miss, \
                 pg_type_is_visible('integer') AS ty, \
                 pg_type_is_visible(to_regtype('text')) AS ty_oid, \
                 pg_type_is_visible('nope') AS ty_miss \
                 FROM users WHERE id = 1",
            )
            .unwrap();
        assert_eq!(row.rows[0].get("t"), Some("true"));
        assert_eq!(row.rows[0].get("oid_t"), Some("true"));
        assert_eq!(row.rows[0].get("miss"), Some("false"));
        assert_eq!(row.rows[0].get("ty"), Some("true"));
        assert_eq!(row.rows[0].get("ty_oid"), Some("true"));
        assert_eq!(row.rows[0].get("ty_miss"), Some("false"));

        session
            .execute_sql("SET search_path TO myschema")
            .unwrap();
        let hidden = session
            .execute_sql(
                "SELECT pg_table_is_visible('users') AS t, \
                 pg_type_is_visible('integer') AS ty FROM users WHERE id = 1",
            )
            .unwrap();
        assert_eq!(hidden.rows[0].get("t"), Some("false"));
        assert_eq!(hidden.rows[0].get("ty"), Some("true"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_to_regproc_function_visible_e2e() {
        let (mut session, root) = temp_session("to-regproc-vis");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();

        let row = session
            .execute_sql(
                "SELECT pg_function_is_visible('lower') AS ok, \
                 pg_function_is_visible('nope_fn') AS bad, \
                 to_regproc('format_type') IS NOT NULL AS reg, \
                 to_regproc('nope_fn') IS NULL AS miss, \
                 pg_function_is_visible(to_regproc('lower')) AS oid_ok, \
                 to_regprocedure('upper') IS NOT NULL AS proc \
                 FROM users WHERE id = 1",
            )
            .unwrap();
        assert_eq!(row.rows[0].get("ok"), Some("true"));
        assert_eq!(row.rows[0].get("bad"), Some("false"));
        assert_eq!(row.rows[0].get("reg"), Some("true"));
        assert_eq!(row.rows[0].get("miss"), Some("true"));
        assert_eq!(row.rows[0].get("oid_ok"), Some("true"));
        assert_eq!(row.rows[0].get("proc"), Some("true"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_pg_relation_is_updatable_e2e() {
        let (mut session, root) = temp_session("rel-updatable");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();

        let row = session
            .execute_sql(
                "SELECT pg_relation_is_updatable('users', true) AS bits, \
                 pg_relation_is_updatable(to_regclass('users'), false) AS oid_bits, \
                 pg_relation_is_updatable('missing', true) AS miss \
                 FROM users WHERE id = 1",
            )
            .unwrap();
        assert_eq!(row.rows[0].get("bits"), Some("28"));
        assert_eq!(row.rows[0].get("oid_bits"), Some("28"));
        assert_eq!(row.rows[0].get("miss"), Some("0"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_pg_column_is_updatable_e2e() {
        let (mut session, root) = temp_session("col-updatable");
        session
            .engine()
            .register_table(
                TableSchema::new("items", "id", vec![]).with_columns(vec![
                    ColumnSpec::new("id", "BIGINT"),
                    ColumnSpec::new("name", "TEXT"),
                ]),
            )
            .unwrap();
        session
            .execute_sql("INSERT INTO items (id, name) VALUES (1, 'Ada')")
            .unwrap();

        let row = session
            .execute_sql(
                "SELECT pg_column_is_updatable('items', 'name', true) AS ok, \
                 pg_column_is_updatable(to_regclass('items'), 1, false) AS att, \
                 pg_column_is_updatable('items', 'missing', true) AS miss, \
                 pg_column_is_updatable('nope', 'id', true) AS bad_tbl \
                 FROM items WHERE id = 1",
            )
            .unwrap();
        assert_eq!(row.rows[0].get("ok"), Some("true"));
        assert_eq!(row.rows[0].get("att"), Some("true"));
        assert_eq!(row.rows[0].get("miss"), Some("false"));
        assert_eq!(row.rows[0].get("bad_tbl"), Some("false"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_pg_get_indexdef_e2e() {
        let (mut session, root) = temp_session("get-indexdef");
        session
            .engine()
            .register_table(
                TableSchema::new("employees", "id", vec![]).with_columns(vec![
                    ColumnSpec::new("id", "BIGINT"),
                    ColumnSpec::new("department", "TEXT"),
                ]),
            )
            .unwrap();
        session
            .execute_sql("INSERT INTO employees (id, department) VALUES (1, 'Eng')")
            .unwrap();
        session
            .execute_sql("CREATE INDEX idx_dept ON employees(department)")
            .unwrap();

        let row = session
            .execute_sql(
                "SELECT pg_get_indexdef('idx_dept') AS def, \
                 pg_get_indexdef('idx_dept', 0, true) AS pretty, \
                 pg_get_indexdef('missing') AS miss \
                 FROM employees WHERE id = 1",
            )
            .unwrap();
        assert_eq!(
            row.rows[0].get("def"),
            Some("CREATE INDEX idx_dept ON employees USING btree (department)")
        );
        assert_eq!(
            row.rows[0].get("pretty"),
            Some("CREATE INDEX idx_dept ON employees USING btree (department)")
        );
        assert_eq!(row.rows[0].get("miss"), Some(""));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_pg_describe_object_e2e() {
        let (mut session, root) = temp_session("describe-object");
        session
            .engine()
            .register_table(
                TableSchema::new("items", "id", vec![]).with_columns(vec![
                    ColumnSpec::new("id", "BIGINT"),
                    ColumnSpec::new("name", "TEXT"),
                ]),
            )
            .unwrap();
        session
            .execute_sql("INSERT INTO items (id, name) VALUES (1, 'Ada')")
            .unwrap();
        session
            .execute_sql("CREATE INDEX idx_items_name ON items(name)")
            .unwrap();

        let row = session
            .execute_sql(
                "SELECT pg_describe_object(1259, to_regclass('items'), 0) AS tbl, \
                 pg_describe_object(1259, to_regclass('items'), 2) AS col, \
                 pg_describe_object(1247, to_regtype('integer'), 0) AS ty, \
                 pg_describe_object(2615, to_regnamespace('public'), 0) AS ns, \
                 pg_describe_object(1260, to_regrole('postgres'), 0) AS role, \
                 pg_describe_object(1255, to_regproc('lower'), 0) AS fn, \
                 pg_describe_object(1259, 1, 0) AS miss \
                 FROM items WHERE id = 1",
            )
            .unwrap();
        assert_eq!(row.rows[0].get("tbl"), Some("table items"));
        assert_eq!(row.rows[0].get("col"), Some("column name of table items"));
        assert_eq!(row.rows[0].get("ty"), Some("type integer"));
        assert_eq!(row.rows[0].get("ns"), Some("schema public"));
        assert_eq!(row.rows[0].get("role"), Some("role postgres"));
        assert_eq!(row.rows[0].get("fn"), Some("function lower"));
        assert_eq!(row.rows[0].get("miss"), Some(""));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_pg_identify_object_e2e() {
        let (mut session, root) = temp_session("identify-object");
        session
            .engine()
            .register_table(
                TableSchema::new("items", "id", vec![]).with_columns(vec![
                    ColumnSpec::new("id", "BIGINT"),
                    ColumnSpec::new("name", "TEXT"),
                ]),
            )
            .unwrap();
        session
            .execute_sql("INSERT INTO items (id, name) VALUES (1, 'Ada')")
            .unwrap();

        let row = session
            .execute_sql(
                "SELECT pg_identify_object(1259, to_regclass('items'), 0) AS tbl, \
                 pg_identify_object(1259, to_regclass('items'), 2) AS col, \
                 pg_identify_object(1247, to_regtype('text'), 0) AS ty, \
                 pg_identify_object(1255, to_regproc('lower'), 0) AS fn, \
                 pg_identify_object(1259, 1, 0) AS miss \
                 FROM items WHERE id = 1",
            )
            .unwrap();
        assert_eq!(row.rows[0].get("tbl"), Some("table public.items"));
        assert_eq!(
            row.rows[0].get("col"),
            Some("column name of table public.items")
        );
        assert_eq!(row.rows[0].get("ty"), Some("type text"));
        assert_eq!(row.rows[0].get("fn"), Some("function public.lower()"));
        assert_eq!(row.rows[0].get("miss"), Some(""));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_to_regoper_operator_visible_e2e() {
        let (mut session, root) = temp_session("to-regoper");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();

        let row = session
            .execute_sql(
                "SELECT pg_operator_is_visible('=') AS eq, \
                 pg_operator_is_visible('<->') AS dist, \
                 pg_operator_is_visible('@@@') AS bad, \
                 to_regoper('||') IS NOT NULL AS reg, \
                 to_regoper('nope') IS NULL AS miss, \
                 pg_operator_is_visible(to_regoper('=')) AS oid_ok, \
                 to_regoperator('->') IS NOT NULL AS arrow \
                 FROM users WHERE id = 1",
            )
            .unwrap();
        assert_eq!(row.rows[0].get("eq"), Some("true"));
        assert_eq!(row.rows[0].get("dist"), Some("true"));
        assert_eq!(row.rows[0].get("bad"), Some("false"));
        assert_eq!(row.rows[0].get("reg"), Some("true"));
        assert_eq!(row.rows[0].get("miss"), Some("true"));
        assert_eq!(row.rows[0].get("oid_ok"), Some("true"));
        assert_eq!(row.rows[0].get("arrow"), Some("true"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_to_regcollation_visible_e2e() {
        let (mut session, root) = temp_session("to-regcollation");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();

        let row = session
            .execute_sql(
                "SELECT pg_collation_is_visible('C') AS c, \
                 pg_collation_is_visible('default') AS def, \
                 pg_collation_is_visible('en_US') AS bad, \
                 to_regcollation('POSIX') IS NOT NULL AS reg, \
                 to_regcollation('nope') IS NULL AS miss, \
                 pg_collation_is_visible(to_regcollation('ucs_basic')) AS oid_ok \
                 FROM users WHERE id = 1",
            )
            .unwrap();
        assert_eq!(row.rows[0].get("c"), Some("true"));
        assert_eq!(row.rows[0].get("def"), Some("true"));
        assert_eq!(row.rows[0].get("bad"), Some("false"));
        assert_eq!(row.rows[0].get("reg"), Some("true"));
        assert_eq!(row.rows[0].get("miss"), Some("true"));
        assert_eq!(row.rows[0].get("oid_ok"), Some("true"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_pg_advisory_lock_e2e() {
        let (mut admin, root) = temp_session("advisory-lock");
        admin
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();
        admin
            .execute_sql("CREATE USER peer WITH PASSWORD 'secret'")
            .unwrap();
        admin
            .execute_sql("GRANT SELECT ON users TO peer")
            .unwrap();

        let got = admin
            .execute_sql(
                "SELECT pg_try_advisory_lock(9001) AS ok FROM users WHERE id = 1",
            )
            .unwrap();
        assert_eq!(got.rows[0].get("ok"), Some("true"));

        let mut peer = SessionState::as_user(Arc::clone(admin.engine()), "peer").unwrap();
        let blocked = peer
            .execute_sql(
                "SELECT pg_try_advisory_lock(9001) AS ok FROM users WHERE id = 1",
            )
            .unwrap();
        assert_eq!(blocked.rows[0].get("ok"), Some("false"));

        let pair = admin
            .execute_sql(
                "SELECT pg_try_advisory_lock(7, 8) AS ok FROM users WHERE id = 1",
            )
            .unwrap();
        assert_eq!(pair.rows[0].get("ok"), Some("true"));

        admin
            .execute_sql("SELECT pg_advisory_unlock_all() FROM users WHERE id = 1")
            .unwrap();
        let free = peer
            .execute_sql(
                "SELECT pg_try_advisory_lock(9001) AS ok FROM users WHERE id = 1",
            )
            .unwrap();
        assert_eq!(free.rows[0].get("ok"), Some("true"));
        peer.execute_sql("SELECT pg_advisory_unlock_all() FROM users WHERE id = 1")
            .unwrap();

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_pg_advisory_lock_shared_e2e() {
        let (mut admin, root) = temp_session("advisory-lock-shared");
        admin
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();
        admin
            .execute_sql("CREATE USER peer WITH PASSWORD 'secret'")
            .unwrap();
        admin
            .execute_sql("GRANT SELECT ON users TO peer")
            .unwrap();

        let got = admin
            .execute_sql(
                "SELECT pg_try_advisory_lock_shared(9002) AS ok FROM users WHERE id = 1",
            )
            .unwrap();
        assert_eq!(got.rows[0].get("ok"), Some("true"));

        let mut peer = SessionState::as_user(Arc::clone(admin.engine()), "peer").unwrap();
        let also = peer
            .execute_sql(
                "SELECT pg_try_advisory_lock_shared(9002) AS ok FROM users WHERE id = 1",
            )
            .unwrap();
        assert_eq!(also.rows[0].get("ok"), Some("true"));

        let excl = peer
            .execute_sql(
                "SELECT pg_try_advisory_lock(9002) AS ok FROM users WHERE id = 1",
            )
            .unwrap();
        assert_eq!(excl.rows[0].get("ok"), Some("false"));

        admin
            .execute_sql(
                "SELECT pg_advisory_unlock_shared(9002) FROM users WHERE id = 1",
            )
            .unwrap();
        peer.execute_sql("SELECT pg_advisory_unlock_all() FROM users WHERE id = 1")
            .unwrap();
        let free = admin
            .execute_sql(
                "SELECT pg_try_advisory_lock(9002) AS ok FROM users WHERE id = 1",
            )
            .unwrap();
        assert_eq!(free.rows[0].get("ok"), Some("true"));
        admin
            .execute_sql("SELECT pg_advisory_unlock_all() FROM users WHERE id = 1")
            .unwrap();

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_pg_advisory_xact_lock_e2e() {
        let (mut admin, root) = temp_session("advisory-xact-lock");
        admin
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();
        admin
            .execute_sql("CREATE USER peer WITH PASSWORD 'secret'")
            .unwrap();
        admin
            .execute_sql("GRANT SELECT ON users TO peer")
            .unwrap();

        admin.execute_sql("BEGIN").unwrap();
        let got = admin
            .execute_sql(
                "SELECT pg_try_advisory_xact_lock(9100) AS ok FROM users WHERE id = 1",
            )
            .unwrap();
        assert_eq!(got.rows[0].get("ok"), Some("true"));

        let mut peer = SessionState::as_user(Arc::clone(admin.engine()), "peer").unwrap();
        let blocked = peer
            .execute_sql(
                "SELECT pg_try_advisory_xact_lock(9100) AS ok FROM users WHERE id = 1",
            )
            .unwrap();
        assert_eq!(blocked.rows[0].get("ok"), Some("false"));

        admin.execute_sql("COMMIT").unwrap();
        let free = peer
            .execute_sql(
                "SELECT pg_try_advisory_xact_lock(9100) AS ok FROM users WHERE id = 1",
            )
            .unwrap();
        assert_eq!(free.rows[0].get("ok"), Some("true"));
        // Auto-commit releases xact lock after the statement.
        let again = admin
            .execute_sql(
                "SELECT pg_try_advisory_xact_lock(9100) AS ok FROM users WHERE id = 1",
            )
            .unwrap();
        assert_eq!(again.rows[0].get("ok"), Some("true"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_pg_advisory_xact_lock_shared_e2e() {
        let (mut admin, root) = temp_session("advisory-xact-lock-shared");
        admin
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();
        admin
            .execute_sql("CREATE USER peer WITH PASSWORD 'secret'")
            .unwrap();
        admin
            .execute_sql("GRANT SELECT ON users TO peer")
            .unwrap();

        admin.execute_sql("BEGIN").unwrap();
        let got = admin
            .execute_sql(
                "SELECT pg_try_advisory_xact_lock_shared(9101) AS ok FROM users WHERE id = 1",
            )
            .unwrap();
        assert_eq!(got.rows[0].get("ok"), Some("true"));

        let mut peer = SessionState::as_user(Arc::clone(admin.engine()), "peer").unwrap();
        let also = peer
            .execute_sql(
                "SELECT pg_try_advisory_xact_lock_shared(9101) AS ok FROM users WHERE id = 1",
            )
            .unwrap();
        assert_eq!(also.rows[0].get("ok"), Some("true"));

        let excl = peer
            .execute_sql(
                "SELECT pg_try_advisory_xact_lock(9101) AS ok FROM users WHERE id = 1",
            )
            .unwrap();
        assert_eq!(excl.rows[0].get("ok"), Some("false"));

        admin.execute_sql("COMMIT").unwrap();
        // Peer auto-commit already released its shared xact lock; admin released on COMMIT.
        let free = peer
            .execute_sql(
                "SELECT pg_try_advisory_xact_lock(9101) AS ok FROM users WHERE id = 1",
            )
            .unwrap();
        assert_eq!(free.rows[0].get("ok"), Some("true"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_pg_size_pretty_encoding_e2e() {
        let (mut session, root) = temp_session("pg-size-pretty");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();

        let row = session
            .execute_sql(
                "SELECT pg_size_pretty(1024) AS b, \
                 pg_size_pretty(10240) AS kb, \
                 pg_size_pretty(1048576) AS mb, \
                 pg_client_encoding() AS enc \
                 FROM users WHERE id = 1",
            )
            .unwrap();
        assert_eq!(row.rows[0].get("b"), Some("1024 bytes"));
        assert_eq!(row.rows[0].get("kb"), Some("10 kB"));
        assert_eq!(row.rows[0].get("mb"), Some("1024 kB"));
        assert_eq!(row.rows[0].get("enc"), Some("UTF8"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_pg_size_bytes_e2e() {
        let (mut session, root) = temp_session("pg-size-bytes");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();

        let row = session
            .execute_sql(
                "SELECT pg_size_bytes('1024 bytes') AS b, \
                 pg_size_bytes('10 kB') AS kb, \
                 pg_size_bytes(pg_size_pretty(1048576)) AS roundtrip \
                 FROM users WHERE id = 1",
            )
            .unwrap();
        assert_eq!(row.rows[0].get("b"), Some("1024"));
        assert_eq!(row.rows[0].get("kb"), Some("10240"));
        // pretty(1048576) → "1024 kB" → 1024*1024
        assert_eq!(row.rows[0].get("roundtrip"), Some("1048576"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_version_e2e() {
        let (mut session, root) = temp_session("version-fn");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();

        let row = session
            .execute_sql("SELECT VERSION() AS v FROM users WHERE id = 1")
            .unwrap();
        let v = row.rows[0].get("v").unwrap();
        assert!(
            v.starts_with("PostgreSQL 16.0 (Takyonic "),
            "VERSION banner unexpected: {v}"
        );
        assert!(v.contains(env!("CARGO_PKG_VERSION")), "missing crate version: {v}");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_current_setting_e2e() {
        let (mut session, root) = temp_session("current-setting");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();

        let row = session
            .execute_sql(
                "SELECT current_setting('search_path') AS sp, \
                 current_setting('transaction_isolation') AS iso, \
                 current_setting('server_encoding') AS enc, \
                 current_setting('no_such_guc', true) AS missing \
                 FROM users WHERE id = 1",
            )
            .unwrap();
        assert_eq!(row.rows[0].get("sp"), Some("public"));
        assert_eq!(row.rows[0].get("iso"), Some("repeatable read"));
        assert_eq!(row.rows[0].get("enc"), Some("UTF8"));
        assert_eq!(row.rows[0].get("missing"), Some(""));

        session
            .execute_sql("SET search_path TO myschema, public")
            .unwrap();
        let sp = session
            .execute_sql("SELECT current_setting('search_path') AS sp FROM users WHERE id = 1")
            .unwrap();
        assert_eq!(sp.rows[0].get("sp"), Some("myschema, public"));

        let err = session
            .execute_sql("SELECT current_setting('no_such_guc') FROM users WHERE id = 1")
            .unwrap_err();
        assert!(
            err.to_string().contains("unrecognized configuration"),
            "unexpected: {err}"
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_set_config_e2e() {
        let (mut session, root) = temp_session("set-config");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();

        let row = session
            .execute_sql(
                "SELECT set_config('search_path', 'myschema, public', false) AS sc, \
                 current_setting('search_path') AS sp FROM users WHERE id = 1",
            )
            .unwrap();
        assert_eq!(row.rows[0].get("sc"), Some("myschema, public"));
        assert_eq!(row.rows[0].get("sp"), Some("myschema, public"));

        let sp = session
            .execute_sql("SELECT current_setting('search_path') AS sp FROM users WHERE id = 1")
            .unwrap();
        assert_eq!(sp.rows[0].get("sp"), Some("myschema, public"));

        let err = session
            .execute_sql(
                "SELECT set_config('search_path', 'tmp', true) FROM users WHERE id = 1",
            )
            .unwrap_err();
        assert!(
            err.to_string().contains("SET LOCAL can only be used in transaction blocks"),
            "unexpected: {err}"
        );

        session.execute_sql("BEGIN").unwrap();
        session
            .execute_sql(
                "SELECT set_config('search_path', 'local_only', true) FROM users WHERE id = 1",
            )
            .unwrap();
        assert_eq!(session.search_path(), "local_only");
        session.execute_sql("ROLLBACK").unwrap();
        assert_eq!(session.search_path(), "myschema, public");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_txid_postmaster_start_e2e() {
        let (mut session, root) = temp_session("txid-postmaster");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();

        let row = session
            .execute_sql(
                "SELECT txid_current() AS t1, pg_current_xact_id() AS t2, \
                 pg_postmaster_start_time() AS start \
                 FROM users WHERE id = 1",
            )
            .unwrap();
        let t1 = row.rows[0].get("t1").unwrap();
        let t2 = row.rows[0].get("t2").unwrap();
        assert_eq!(t1, t2, "txid aliases should match within a statement");
        let n: i64 = t1.parse().unwrap();
        assert!(n > 0, "txid should be positive: {n}");
        let start = row.rows[0].get("start").unwrap();
        assert!(start.len() >= 19, "start time too short: {start}");
        assert!(start.contains('+'), "expected tz suffix: {start}");

        let again = session
            .execute_sql("SELECT pg_postmaster_start_time() AS s FROM users WHERE id = 1")
            .unwrap();
        assert_eq!(again.rows[0].get("s").as_deref(), Some(start));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_txid_status_e2e() {
        let (mut session, root) = temp_session("txid-status");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();

        let row = session
            .execute_sql(
                "SELECT txid_current() AS xid, \
                 txid_status(txid_current()) AS cur, \
                 pg_xact_status(txid_current()) AS alias, \
                 txid_status(1) AS old, \
                 txid_status(0) AS z, \
                 txid_status(999999999) AS future \
                 FROM users WHERE id = 1",
            )
            .unwrap();
        assert_eq!(row.rows[0].get("cur"), Some("in progress"));
        assert_eq!(row.rows[0].get("alias"), Some("in progress"));
        assert_eq!(row.rows[0].get("old"), Some("committed"));
        assert_eq!(row.rows[0].get("z"), Some(""));
        assert_eq!(row.rows[0].get("future"), Some(""));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_pg_snapshot_e2e() {
        let (mut session, root) = temp_session("pg-snapshot");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();

        let row = session
            .execute_sql(
                "SELECT txid_current() AS xid, \
                 pg_export_snapshot() AS exp, \
                 pg_current_snapshot() AS cur, \
                 txid_current_snapshot() AS alias \
                 FROM users WHERE id = 1",
            )
            .unwrap();
        let xid = row.rows[0].get("xid").unwrap();
        let exp = row.rows[0].get("exp").unwrap();
        let cur = row.rows[0].get("cur").unwrap();
        let alias = row.rows[0].get("alias").unwrap();
        assert!(
            exp.contains('-') && exp.contains('1'),
            "export snapshot unexpected: {exp}"
        );
        assert_eq!(cur, alias);
        assert!(
            cur.starts_with(&format!("{xid}:")),
            "snapshot should start with xmin={xid}, got {cur}"
        );
        assert!(cur.ends_with(':'), "expected empty xip list: {cur}");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_pg_snapshot_inspect_e2e() {
        let (mut session, root) = temp_session("pg-snapshot-inspect");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();

        let row = session
            .execute_sql(
                "SELECT pg_snapshot_xmin(pg_current_snapshot()) AS xmin, \
                 pg_snapshot_xmax(pg_current_snapshot()) AS xmax, \
                 pg_visible_in_snapshot(1, pg_current_snapshot()) AS old_ok, \
                 pg_visible_in_snapshot(txid_current(), pg_current_snapshot()) AS cur_ok, \
                 pg_visible_in_snapshot(999999999, pg_current_snapshot()) AS future_ok \
                 FROM users WHERE id = 1",
            )
            .unwrap();
        let xmin: i64 = row.rows[0].get("xmin").unwrap().parse().unwrap();
        let xmax: i64 = row.rows[0].get("xmax").unwrap().parse().unwrap();
        assert!(xmax == xmin + 1, "xmax should be xmin+1, got {xmin}/{xmax}");
        assert_eq!(row.rows[0].get("old_ok"), Some("true"));
        assert_eq!(row.rows[0].get("cur_ok"), Some("true"));
        assert_eq!(row.rows[0].get("future_ok"), Some("false"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_pg_sleep_column_size_e2e() {
        let (mut session, root) = temp_session("pg-sleep-colsize");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();

        let row = session
            .execute_sql(
                "SELECT pg_sleep(0) AS z, \
                 pg_column_size('Ada') AS t, \
                 pg_column_size(42) AS i, \
                 pg_column_size(true) AS b, \
                 pg_column_size(NULL) AS n \
                 FROM users WHERE id = 1",
            )
            .unwrap();
        assert_eq!(row.rows[0].get("z"), Some(""));
        assert_eq!(row.rows[0].get("t"), Some("3"));
        assert_eq!(row.rows[0].get("i"), Some("8"));
        assert_eq!(row.rows[0].get("b"), Some("1"));
        assert_eq!(row.rows[0].get("n"), Some(""));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_current_role_gen_random_uuid_e2e() {
        let (mut session, root) = temp_session("current-role-uuid");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();

        let row = session
            .execute_sql(
                "SELECT CURRENT_ROLE AS role, CURRENT_USER AS usr, \
                 gen_random_uuid() AS u1, gen_random_uuid() AS u2 \
                 FROM users WHERE id = 1",
            )
            .unwrap();
        assert_eq!(row.rows[0].get("role"), Some("postgres"));
        assert_eq!(row.rows[0].get("usr"), Some("postgres"));
        let u1 = row.rows[0].get("u1").unwrap();
        let u2 = row.rows[0].get("u2").unwrap();
        assert_eq!(u1.len(), 36, "uuid length: {u1}");
        assert_eq!(u1.chars().filter(|c| *c == '-').count(), 4);
        assert_eq!(&u1[14..15], "4", "UUID version nibble: {u1}");
        assert!(
            matches!(u1.as_bytes()[19], b'8' | b'9' | b'a' | b'b'),
            "UUID variant: {u1}"
        );
        assert_ne!(u1, u2, "successive UUIDs should differ");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_random_setseed_e2e() {
        let (mut session, root) = temp_session("random-setseed");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();

        session
            .execute_sql("SELECT setseed(0.42) FROM users WHERE id = 1")
            .unwrap();
        let a = session
            .execute_sql("SELECT random() AS r FROM users WHERE id = 1")
            .unwrap();
        let r1: f64 = a.rows[0].get("r").unwrap().parse().unwrap();
        assert!((0.0..1.0).contains(&r1), "random out of range: {r1}");

        session
            .execute_sql("SELECT setseed(0.42) FROM users WHERE id = 1")
            .unwrap();
        let b = session
            .execute_sql("SELECT random() AS r FROM users WHERE id = 1")
            .unwrap();
        let r2: f64 = b.rows[0].get("r").unwrap().parse().unwrap();
        assert_eq!(r1, r2, "setseed should make random reproducible");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_num_nulls_nonnulls_e2e() {
        let (mut session, root) = temp_session("num-nulls");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();

        let row = session
            .execute_sql(
                "SELECT num_nonnulls(1, NULL, 2) AS nn, \
                 num_nulls(1, NULL, 2) AS n, \
                 num_nonnulls(NULL, NULL) AS alln, \
                 num_nulls(1, 2, 3) AS none \
                 FROM users WHERE id = 1",
            )
            .unwrap();
        assert_eq!(row.rows[0].get("nn"), Some("2"));
        assert_eq!(row.rows[0].get("n"), Some("1"));
        assert_eq!(row.rows[0].get("alln"), Some("0"));
        assert_eq!(row.rows[0].get("none"), Some("0"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_greatest_least_extract_e2e() {
        let (mut session, root) = temp_session("greatest-extract");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();

        let row = session
            .execute_sql(
                "SELECT GREATEST(age, 25) AS g, LEAST(age, 25) AS l, \
                 EXTRACT(YEAR FROM CURRENT_DATE) AS y, \
                 EXTRACT(MONTH FROM CURRENT_DATE) AS m \
                 FROM users WHERE id = 1",
            )
            .unwrap();
        assert_eq!(row.rows[0].get("g"), Some("30"));
        assert_eq!(row.rows[0].get("l"), Some("25"));
        let y = row.rows[0].get("y").unwrap();
        assert_eq!(y.len(), 4);
        assert!(y.chars().all(|c| c.is_ascii_digit()));
        let m: u32 = row.rows[0].get("m").unwrap().parse().unwrap();
        assert!((1..=12).contains(&m));

        let skip_null = session
            .execute_sql(
                r#"SELECT greatest(10, NULL, 30) AS g, least(10, NULL, 30) AS l
                   FROM generate_series(1, 1)"#,
            )
            .unwrap();
        assert_eq!(skip_null.rows[0].get("g"), Some("30"));
        assert_eq!(skip_null.rows[0].get("l"), Some("10"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_interval_arith_e2e() {
        let (mut session, root) = temp_session("interval-arith");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();

        let row = session
            .execute_sql(
                "SELECT INTERVAL '1' DAY AS i, \
                 INTERVAL '2 hours' AS h, \
                 '2026-01-15' + INTERVAL '1' DAY AS d, \
                 '2026-01-15 10:00:00' - INTERVAL '2 hours' AS t \
                 FROM users WHERE id = 1",
            )
            .unwrap();
        assert_eq!(row.rows[0].get("i"), Some("1 day"));
        assert_eq!(row.rows[0].get("h"), Some("02:00:00"));
        assert_eq!(row.rows[0].get("d"), Some("2026-01-16"));
        assert_eq!(row.rows[0].get("t"), Some("2026-01-15 08:00:00+00"));

        let scaled = session
            .execute_sql(
                "SELECT (INTERVAL '1' DAY * 2) AS two FROM users WHERE id = 1",
            )
            .unwrap();
        assert_eq!(scaled.rows[0].get("two"), Some("2 days"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_date_trunc_e2e() {
        let (mut session, root) = temp_session("date-trunc");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();

        let row = session
            .execute_sql(
                "SELECT DATE_TRUNC('day', '2026-08-02 15:30:45') AS d, \
                 DATE_TRUNC('hour', '2026-08-02 15:30:45') AS h, \
                 DATE_TRUNC('month', '2026-08-02 15:30:45') AS m, \
                 DATE_TRUNC('year', CURRENT_DATE) AS y \
                 FROM users WHERE id = 1",
            )
            .unwrap();
        assert_eq!(row.rows[0].get("d"), Some("2026-08-02 00:00:00+00"));
        assert_eq!(row.rows[0].get("h"), Some("2026-08-02 15:00:00+00"));
        assert_eq!(row.rows[0].get("m"), Some("2026-08-01 00:00:00+00"));
        let y = row.rows[0].get("y").unwrap();
        assert!(y.ends_with("-01-01 00:00:00+00"), "unexpected year trunc: {y}");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_make_date_time_timestamp_e2e() {
        let (mut session, root) = temp_session("make-date");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();

        let row = session
            .execute_sql(
                "SELECT MAKE_DATE(2026, 8, 2) AS d, \
                 MAKE_TIME(15, 30, 45) AS t, \
                 MAKE_TIMESTAMP(2026, 8, 2, 15, 30, 45) AS ts \
                 FROM users WHERE id = 1",
            )
            .unwrap();
        assert_eq!(row.rows[0].get("d"), Some("2026-08-02"));
        assert_eq!(row.rows[0].get("t"), Some("15:30:45"));
        assert_eq!(row.rows[0].get("ts"), Some("2026-08-02 15:30:45+00"));

        let err = session
            .execute_sql("SELECT MAKE_DATE(2026, 2, 30) FROM users WHERE id = 1")
            .unwrap_err();
        assert!(
            err.to_string().contains("out of range"),
            "unexpected: {err}"
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_make_interval_e2e() {
        let (mut session, root) = temp_session("make-interval");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();

        let row = session
            .execute_sql(
                "SELECT MAKE_INTERVAL(0, 0, 0, 1, 0, 0, 0) AS day, \
                 MAKE_INTERVAL(0, 0, 0, 0, 2, 30, 0) AS hm, \
                 MAKE_INTERVAL(0, 0, 0, 1, 2, 0, 0) AS combo, \
                 '2026-01-15' + MAKE_INTERVAL(0, 0, 0, 1) AS next_day \
                 FROM users WHERE id = 1",
            )
            .unwrap();
        assert_eq!(row.rows[0].get("day"), Some("1 day"));
        assert_eq!(row.rows[0].get("hm"), Some("02:30:00"));
        assert_eq!(row.rows[0].get("combo"), Some("1 day 02:00:00"));
        assert_eq!(row.rows[0].get("next_day"), Some("2026-01-16"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_justify_interval_e2e() {
        let (mut session, root) = temp_session("justify-interval");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();

        let row = session
            .execute_sql(
                "SELECT JUSTIFY_HOURS(INTERVAL '25 hours') AS h, \
                 JUSTIFY_DAYS(INTERVAL '40 days') AS d, \
                 JUSTIFY_INTERVAL(INTERVAL '40 days 25 hours') AS both \
                 FROM users WHERE id = 1",
            )
            .unwrap();
        assert_eq!(row.rows[0].get("h"), Some("1 day 01:00:00"));
        assert_eq!(row.rows[0].get("d"), Some("1 mon 10 days"));
        assert_eq!(row.rows[0].get("both"), Some("1 mon 11 days 01:00:00"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_extract_epoch_e2e() {
        let (mut session, root) = temp_session("extract-epoch");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();

        let row = session
            .execute_sql(
                "SELECT EXTRACT(EPOCH FROM '1970-01-01 00:00:00') AS e0, \
                 EXTRACT(EPOCH FROM '2026-01-15 12:00:00+00') AS e1, \
                 EXTRACT(EPOCH FROM '1970-01-01 01:00:00+01') AS e_off, \
                 EXTRACT(EPOCH FROM INTERVAL '1 day') AS iv, \
                 DATE_PART('epoch', '1970-01-01 00:00:00') AS dp \
                 FROM users WHERE id = 1",
            )
            .unwrap();
        assert_eq!(row.rows[0].get("e0"), Some("0"));
        assert_eq!(row.rows[0].get("e1"), Some("1768478400"));
        assert_eq!(row.rows[0].get("e_off"), Some("0"));
        assert_eq!(row.rows[0].get("iv"), Some("86400"));
        assert_eq!(row.rows[0].get("dp"), Some("0"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_overlaps_e2e() {
        let (mut session, root) = temp_session("overlaps");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();

        let row = session
            .execute_sql(
                "SELECT ('2001-02-16', '2001-12-21') OVERLAPS \
                 ('2001-10-30', '2002-10-30') AS yes, \
                 ('2001-02-16', '2001-12-21') OVERLAPS \
                 ('2002-01-01', '2002-10-30') AS no, \
                 ('2001-01-01', '2001-01-10') OVERLAPS \
                 ('2001-01-10', '2001-01-20') AS touch, \
                 ('2001-02-16', INTERVAL '100 days') OVERLAPS \
                 ('2001-10-30', '2002-10-30') AS iv \
                 FROM users WHERE id = 1",
            )
            .unwrap();
        assert_eq!(row.rows[0].get("yes"), Some("true"));
        assert_eq!(row.rows[0].get("no"), Some("false"));
        assert_eq!(row.rows[0].get("touch"), Some("false"));
        assert_eq!(row.rows[0].get("iv"), Some("false"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_isfinite_e2e() {
        let (mut session, root) = temp_session("isfinite");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();

        let row = session
            .execute_sql(
                "SELECT ISFINITE('2026-08-02') AS d, \
                 ISFINITE('2026-08-02 15:30:45') AS ts, \
                 ISFINITE('infinity') AS inf, \
                 ISFINITE('-infinity') AS ninf, \
                 ISFINITE(INTERVAL '1 day') AS iv \
                 FROM users WHERE id = 1",
            )
            .unwrap();
        assert_eq!(row.rows[0].get("d"), Some("true"));
        assert_eq!(row.rows[0].get("ts"), Some("true"));
        assert_eq!(row.rows[0].get("inf"), Some("false"));
        assert_eq!(row.rows[0].get("ninf"), Some("false"));
        assert_eq!(row.rows[0].get("iv"), Some("true"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_at_time_zone_e2e() {
        let (mut session, root) = temp_session("at-time-zone");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();

        let row = session
            .execute_sql(
                "SELECT \
                 '2026-08-02 12:00:00+00' AT TIME ZONE '+03' AS with_off, \
                 '2026-08-02 15:00:00' AT TIME ZONE '+03' AS local_as, \
                 '2026-08-02 12:00:00Z' AT TIME ZONE 'UTC' AS utc_z, \
                 '2026-08-02 12:00:00+00' AT TIME ZONE 'Europe/Istanbul' AS istanbul, \
                 '2026-01-15 12:00:00+00' AT TIME ZONE 'America/Denver' AS denver_winter, \
                 '2026-07-15 12:00:00+00' AT TIME ZONE 'America/Denver' AS denver_summer \
                 FROM users WHERE id = 1",
            )
            .unwrap();
        assert_eq!(row.rows[0].get("with_off"), Some("2026-08-02 15:00:00"));
        assert_eq!(row.rows[0].get("local_as"), Some("2026-08-02 12:00:00+00"));
        assert_eq!(row.rows[0].get("utc_z"), Some("2026-08-02 12:00:00"));
        assert_eq!(row.rows[0].get("istanbul"), Some("2026-08-02 15:00:00"));
        assert_eq!(row.rows[0].get("denver_winter"), Some("2026-01-15 05:00:00"));
        assert_eq!(row.rows[0].get("denver_summer"), Some("2026-07-15 06:00:00"));

        let err = session
            .execute_sql(
                "SELECT '2026-08-02 12:00:00+00' AT TIME ZONE 'NotA/RealZone' \
                 FROM users WHERE id = 1",
            )
            .unwrap_err();
        assert!(
            err.to_string().contains("unsupported time zone")
                || err.to_string().contains("not recognized"),
            "unexpected: {err}"
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_timezone_guc_e2e() {
        let (mut session, root) = temp_session("timezone-guc");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();
        assert_eq!(session.timezone(), "UTC");
        session
            .execute_sql("SET TimeZone TO 'Europe/Istanbul'")
            .unwrap();
        assert_eq!(session.timezone(), "Europe/Istanbul");
        let show = session.execute_sql("SHOW timezone").unwrap();
        assert_eq!(show.rows[0].get("timezone"), Some("Europe/Istanbul"));
        let cur = session
            .execute_sql("SELECT current_setting('TimeZone') AS tz FROM users WHERE id = 1")
            .unwrap();
        assert_eq!(cur.rows[0].get("tz"), Some("Europe/Istanbul"));

        let local = session
            .execute_sql("SELECT LOCALTIMESTAMP AS loc FROM users WHERE id = 1")
            .unwrap();
        // LOCALTIMESTAMP is live wall clock in session TZ — no +00 suffix when TZ ≠ UTC.
        let loc = local.rows[0].get("loc").unwrap();
        assert!(
            !loc.ends_with("+00"),
            "LOCALTIMESTAMP should be session-local wall clock, got {loc}"
        );

        let bad = session
            .execute_sql("SET TimeZone TO 'NotA/RealZone'")
            .unwrap_err();
        assert!(
            bad.to_string().contains("TimeZone") || bad.to_string().contains("not recognized"),
            "unexpected: {bad}"
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_timezone_fn_e2e() {
        let (mut session, root) = temp_session("timezone-fn");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();

        let row = session
            .execute_sql(
                "SELECT \
                 TIMEZONE('+03', '2026-08-02 12:00:00+00') AS with_off, \
                 TIMEZONE('+03', '2026-08-02 15:00:00') AS local_as, \
                 TIMEZONE('UTC', '2026-08-02 12:00:00Z') AS utc_z, \
                 ('2026-08-02 12:00:00+00' AT TIME ZONE '+03') AS via_at \
                 FROM users WHERE id = 1",
            )
            .unwrap();
        assert_eq!(row.rows[0].get("with_off"), Some("2026-08-02 15:00:00"));
        assert_eq!(row.rows[0].get("local_as"), Some("2026-08-02 12:00:00+00"));
        assert_eq!(row.rows[0].get("utc_z"), Some("2026-08-02 12:00:00"));
        assert_eq!(row.rows[0].get("via_at"), Some("2026-08-02 15:00:00"));
        assert_eq!(
            row.rows[0].get("with_off"),
            row.rows[0].get("via_at"),
            "TIMEZONE(zone, ts) must match ts AT TIME ZONE zone"
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_date_bin_e2e() {
        let (mut session, root) = temp_session("date-bin");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();

        let row = session
            .execute_sql(
                "SELECT \
                 DATE_BIN(INTERVAL '15 minutes', '2026-08-02 15:37:00') AS q15, \
                 DATE_BIN(INTERVAL '1 hour', '2026-08-02 15:37:00', '2026-08-02') AS h, \
                 DATE_BIN(INTERVAL '1 day', '2026-08-02 15:37:00') AS d \
                 FROM users WHERE id = 1",
            )
            .unwrap();
        assert_eq!(row.rows[0].get("q15"), Some("2026-08-02 15:30:00+00"));
        assert_eq!(row.rows[0].get("h"), Some("2026-08-02 15:00:00+00"));
        assert_eq!(row.rows[0].get("d"), Some("2026-08-02 00:00:00+00"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_age_e2e() {
        let (mut session, root) = temp_session("age-fn");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();

        let row = session
            .execute_sql(
                "SELECT AGE('2026-08-02', '2026-08-01') AS a, \
                 AGE('2026-01-15 12:00:00', '2026-01-15 10:00:00') AS b \
                 FROM users WHERE id = 1",
            )
            .unwrap();
        assert_eq!(row.rows[0].get("a"), Some("1 day"));
        assert_eq!(row.rows[0].get("b"), Some("02:00:00"));

        let one = session
            .execute_sql("SELECT AGE('1970-01-01') AS from_epoch FROM users WHERE id = 1")
            .unwrap();
        let s = one.rows[0].get("from_epoch").unwrap();
        assert!(!s.is_empty(), "AGE(ts) should return a non-empty interval");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_to_char_to_timestamp_e2e() {
        let (mut session, root) = temp_session("to-char-ts");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();

        let row = session
            .execute_sql(
                "SELECT TO_CHAR('2026-08-02 15:30:45', 'YYYY-MM-DD HH24:MI:SS') AS c, \
                 TO_TIMESTAMP('2026-01-15 08:00:00', 'YYYY-MM-DD HH24:MI:SS') AS t \
                 FROM users WHERE id = 1",
            )
            .unwrap();
        assert_eq!(row.rows[0].get("c"), Some("2026-08-02 15:30:45"));
        assert_eq!(row.rows[0].get("t"), Some("2026-01-15 08:00:00+00"));

        let round = session
            .execute_sql(
                "SELECT TO_CHAR(TO_TIMESTAMP('2026-08-02', 'YYYY-MM-DD'), 'YYYY/MM/DD') AS r \
                 FROM users WHERE id = 1",
            )
            .unwrap();
        assert_eq!(round.rows[0].get("r"), Some("2026/08/02"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_to_date_e2e() {
        let (mut session, root) = temp_session("to-date");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();

        let row = session
            .execute_sql(
                "SELECT TO_DATE('2026-08-02', 'YYYY-MM-DD') AS d, \
                 TO_DATE('15/01/2026', 'DD/MM/YYYY') AS e, \
                 TO_DATE('2026-08-02 15:30:45', 'YYYY-MM-DD HH24:MI:SS') AS f \
                 FROM users WHERE id = 1",
            )
            .unwrap();
        assert_eq!(row.rows[0].get("d"), Some("2026-08-02"));
        assert_eq!(row.rows[0].get("e"), Some("2026-01-15"));
        assert_eq!(row.rows[0].get("f"), Some("2026-08-02"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_to_number_e2e() {
        let (mut session, root) = temp_session("to-number");
        session
            .execute_sql("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 30)")
            .unwrap();

        let row = session
            .execute_sql(
                "SELECT TO_NUMBER('1234.56', '9999.99') AS a, \
                 TO_NUMBER('1,234.56', '9,999.99') AS b, \
                 TO_NUMBER('1,234.56', '9G999D99') AS c, \
                 TO_NUMBER('-42', 'S999') AS d, \
                 TO_NUMBER('42', '999') AS e \
                 FROM users WHERE id = 1",
            )
            .unwrap();
        assert_eq!(row.rows[0].get("a"), Some("1234.56"));
        assert_eq!(row.rows[0].get("b"), Some("1234.56"));
        assert_eq!(row.rows[0].get("c"), Some("1234.56"));
        assert_eq!(row.rows[0].get("d"), Some("-42"));
        assert_eq!(row.rows[0].get("e"), Some("42"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_generate_series_e2e() {
        let (mut session, root) = temp_session("generate-series");

        let rows = session
            .execute_sql("SELECT * FROM generate_series(1, 5)")
            .unwrap();
        assert_eq!(rows.rows.len(), 5);
        assert_eq!(rows.rows[0].get("generate_series"), Some("1"));
        assert_eq!(rows.rows[4].get("generate_series"), Some("5"));

        let stepped = session
            .execute_sql("SELECT n FROM generate_series(2, 8, 3) AS g(n)")
            .unwrap();
        let vals: Vec<_> = stepped
            .rows
            .iter()
            .map(|r| r.get("n").unwrap().to_string())
            .collect();
        assert_eq!(vals, vec!["2", "5", "8"]);

        let filtered = session
            .execute_sql(
                "SELECT generate_series FROM generate_series(1, 10) WHERE generate_series > 7",
            )
            .unwrap();
        assert_eq!(filtered.rows.len(), 3);

        let ord = session
            .execute_sql(
                "SELECT n, ord FROM generate_series(10, 12) WITH ORDINALITY AS t(n, ord)",
            )
            .unwrap();
        assert_eq!(ord.rows.len(), 3);
        assert_eq!(ord.rows[0].get("n"), Some("10"));
        assert_eq!(ord.rows[0].get("ord"), Some("1"));
        assert_eq!(ord.rows[2].get("n"), Some("12"));
        assert_eq!(ord.rows[2].get("ord"), Some("3"));

        let days = session
            .execute_sql(
                "SELECT d FROM generate_series('2026-01-01', '2026-01-03', INTERVAL '1 day') AS g(d)",
            )
            .unwrap();
        let day_vals: Vec<_> = days
            .rows
            .iter()
            .map(|r| r.get("d").unwrap().to_string())
            .collect();
        assert_eq!(day_vals, vec!["2026-01-01", "2026-01-02", "2026-01-03"]);

        let hours = session
            .execute_sql(
                "SELECT ts FROM generate_series('2026-01-01 00:00:00', '2026-01-01 02:00:00', INTERVAL '1 hour') AS g(ts)",
            )
            .unwrap();
        assert_eq!(hours.rows.len(), 3);
        assert_eq!(hours.rows[0].get("ts"), Some("2026-01-01 00:00:00+00"));
        assert_eq!(hours.rows[2].get("ts"), Some("2026-01-01 02:00:00+00"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_unnest_e2e() {
        let (mut session, root) = temp_session("unnest-array");

        let rows = session
            .execute_sql("SELECT * FROM unnest(ARRAY[1, 2, 3])")
            .unwrap();
        assert_eq!(rows.rows.len(), 3);
        assert_eq!(rows.rows[0].get("unnest"), Some("1"));
        assert_eq!(rows.rows[2].get("unnest"), Some("3"));

        let aliased = session
            .execute_sql("SELECT x FROM unnest(ARRAY['Ada', 'Di']) AS t(x)")
            .unwrap();
        assert_eq!(aliased.rows[0].get("x"), Some("Ada"));
        assert_eq!(aliased.rows[1].get("x"), Some("Di"));

        let filtered = session
            .execute_sql(
                "SELECT unnest FROM unnest(ARRAY[1, 2, 3, 4]) WHERE unnest > 2",
            )
            .unwrap();
        assert_eq!(filtered.rows.len(), 2);

        let ord = session
            .execute_sql(
                "SELECT x, i FROM unnest(ARRAY[10, 20, 30]) WITH ORDINALITY AS t(x, i)",
            )
            .unwrap();
        assert_eq!(ord.rows.len(), 3);
        assert_eq!(ord.rows[0].get("x"), Some("10"));
        assert_eq!(ord.rows[0].get("i"), Some("1"));
        assert_eq!(ord.rows[2].get("i"), Some("3"));

        let offset = session
            .execute_sql(
                "SELECT numbers, offset FROM UNNEST(ARRAY[10, 20, 30]) AS numbers WITH OFFSET",
            )
            .unwrap();
        assert_eq!(offset.rows.len(), 3);
        assert_eq!(offset.rows[0].get("numbers"), Some("10"));
        assert_eq!(offset.rows[0].get("offset"), Some("0"));
        assert_eq!(offset.rows[2].get("offset"), Some("2"));

        let named = session
            .execute_sql(
                "SELECT x, off FROM UNNEST(ARRAY['a', 'b']) AS t(x) WITH OFFSET AS off",
            )
            .unwrap();
        assert_eq!(named.rows[0].get("x"), Some("a"));
        assert_eq!(named.rows[0].get("off"), Some("0"));
        assert_eq!(named.rows[1].get("off"), Some("1"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_array_ops_e2e() {
        let (mut session, root) = temp_session("array-ops");

        let row = session
            .execute_sql(
                "SELECT array_length(ARRAY[1, 2, 3], 1) AS n, \
                 cardinality(ARRAY[10, 20]) AS c, \
                 ARRAY[1, 2, 3][2] AS mid, \
                 (ARRAY[1, 2] || ARRAY[3, 4]) AS cat \
                 FROM generate_series(1, 1)",
            )
            .unwrap();
        assert_eq!(row.rows[0].get("n"), Some("3"));
        assert_eq!(row.rows[0].get("c"), Some("2"));
        assert_eq!(row.rows[0].get("mid"), Some("2"));
        assert_eq!(row.rows[0].get("cat"), Some("[1,2,3,4]"));

        let oob = session
            .execute_sql("SELECT ARRAY[1, 2][9] AS x FROM generate_series(1, 1)")
            .unwrap();
        assert_eq!(oob.rows[0].get("x"), Some(""));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_array_contains_e2e() {
        let (mut session, root) = temp_session("array-contains");

        let row = session
            .execute_sql(
                "SELECT (ARRAY[1, 2, 3] @> ARRAY[2, 1]) AS contains, \
                 (ARRAY[1] <@ ARRAY[1, 2, 3]) AS contained, \
                 (ARRAY[1, 2] && ARRAY[2, 9]) AS overlap, \
                 (ARRAY[1, 2] && ARRAY[8, 9]) AS no_overlap \
                 FROM generate_series(1, 1)",
            )
            .unwrap();
        assert_eq!(row.rows[0].get("contains"), Some("true"));
        assert_eq!(row.rows[0].get("contained"), Some("true"));
        assert_eq!(row.rows[0].get("overlap"), Some("true"));
        assert_eq!(row.rows[0].get("no_overlap"), Some("false"));

        let filtered = session
            .execute_sql(
                "SELECT generate_series FROM generate_series(1, 5) \
                 WHERE ARRAY[1, 2, 3] @> ARRAY[2]",
            )
            .unwrap();
        assert_eq!(filtered.rows.len(), 5);

        let none = session
            .execute_sql(
                "SELECT generate_series FROM generate_series(1, 5) \
                 WHERE ARRAY[1, 2] && ARRAY[9]",
            )
            .unwrap();
        assert!(none.rows.is_empty());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_json_arrow_e2e() {
        let (mut session, root) = temp_session("json-arrow");

        let row = session
            .execute_sql(
                "SELECT ('{\"a\":1,\"b\":{\"c\":2}}'::json -> 'a') AS a, \
                 ('{\"a\":1,\"b\":{\"c\":2}}'::jsonb -> 'b' ->> 'c') AS c, \
                 ('[10,20,30]'::json -> 1) AS idx, \
                 jsonb_typeof('{\"a\":1}'::jsonb) AS t \
                 FROM generate_series(1, 1)",
            )
            .unwrap();
        assert_eq!(row.rows[0].get("a"), Some("1"));
        assert_eq!(row.rows[0].get("c"), Some("2"));
        assert_eq!(row.rows[0].get("idx"), Some("20"));
        assert_eq!(row.rows[0].get("t"), Some("object"));

        let missing = session
            .execute_sql(
                "SELECT ('{\"a\":1}'::json ->> 'z') AS z FROM generate_series(1, 1)",
            )
            .unwrap();
        assert_eq!(missing.rows[0].get("z"), Some(""));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_json_path_contains_e2e() {
        let (mut session, root) = temp_session("json-path-contains");

        let row = session
            .execute_sql(
                r#"SELECT ('{"a":{"b":2}}'::json #> '{a,b}') AS path,
                 ('{"a":{"b":2}}'::jsonb #>> '{a,b}') AS path_text,
                 ('{"a":1,"b":2}'::jsonb @> '{"a":1}'::jsonb) AS contains,
                 ('{"a":1}'::jsonb <@ '{"a":1,"b":2}'::jsonb) AS contained
                 FROM generate_series(1, 1)"#,
            )
            .unwrap();
        assert_eq!(row.rows[0].get("path"), Some("2"));
        assert_eq!(row.rows[0].get("path_text"), Some("2"));
        assert_eq!(row.rows[0].get("contains"), Some("true"));
        assert_eq!(row.rows[0].get("contained"), Some("true"));

        // Regression: SQL ARRAY @> still works.
        let arr = session
            .execute_sql(
                "SELECT (ARRAY[1, 2, 3] @> ARRAY[2]) AS ok FROM generate_series(1, 1)",
            )
            .unwrap();
        assert_eq!(arr.rows[0].get("ok"), Some("true"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_jsonb_set_concat_e2e() {
        let (mut session, root) = temp_session("jsonb-set-concat");

        let row = session
            .execute_sql(
                r#"SELECT jsonb_set('{"a":1}'::jsonb, '{b}', '2'::jsonb) AS set,
                 ('{"a":1}'::jsonb || '{"b":2}'::jsonb) AS cat,
                 jsonb_set('{"a":{"b":1}}'::jsonb, '{a,b}', '9'::jsonb) AS nested
                 FROM generate_series(1, 1)"#,
            )
            .unwrap();
        let set = row.rows[0].get("set").unwrap();
        assert!(set.contains("\"a\":1") && set.contains("\"b\":2"), "unexpected set: {set}");
        let cat = row.rows[0].get("cat").unwrap();
        assert!(cat.contains("\"a\":1") && cat.contains("\"b\":2"), "unexpected cat: {cat}");
        assert_eq!(
            row.rows[0].get("nested").map(|s| {
                // extract via another query would be heavy; just check substring
                s.contains("\"b\":9")
            }),
            Some(true)
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_jsonb_build_object_array_e2e() {
        let (mut session, root) = temp_session("jsonb-build");

        let row = session
            .execute_sql(
                r#"SELECT jsonb_build_object('a', 1, 'b', true) AS obj,
                 jsonb_build_array(1, 'x', NULL) AS arr,
                 jsonb_build_object('nested', '{"k":2}'::jsonb) AS nest
                 FROM generate_series(1, 1)"#,
            )
            .unwrap();
        let obj = row.rows[0].get("obj").unwrap();
        assert!(obj.contains("\"a\":1") && obj.contains("\"b\":true"), "obj={obj}");
        assert_eq!(row.rows[0].get("arr"), Some(r#"[1,"x",null]"#));
        let nest = row.rows[0].get("nest").unwrap();
        assert!(nest.contains("\"k\":2"), "nest={nest}");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_jsonb_pretty_delete_e2e() {
        let (mut session, root) = temp_session("jsonb-pretty-delete");

        let row = session
            .execute_sql(
                r#"SELECT jsonb_pretty('{"a":1,"b":2}'::jsonb) AS pretty,
                 ('{"a":1,"b":2}'::jsonb - 'a') AS del,
                 ('[10,20,30]'::jsonb - 1) AS adel,
                 ('{"a":{"b":1,"c":2}}'::jsonb #- '{a,b}') AS pdel
                 FROM generate_series(1, 1)"#,
            )
            .unwrap();
        let pretty = row.rows[0].get("pretty").unwrap();
        assert!(pretty.contains('\n') && pretty.contains("\"a\""), "pretty={pretty}");
        let del = row.rows[0].get("del").unwrap();
        assert!(del.contains("\"b\":2") && !del.contains("\"a\""), "del={del}");
        assert_eq!(row.rows[0].get("adel"), Some("[10,30]"));
        let pdel = row.rows[0].get("pdel").unwrap();
        assert!(pdel.contains("\"c\":2") && !pdel.contains("\"b\""), "pdel={pdel}");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_jsonb_insert_strip_nulls_e2e() {
        let (mut session, root) = temp_session("jsonb-insert-strip");

        let row = session
            .execute_sql(
                r#"SELECT jsonb_insert('{"a":[1,2]}'::jsonb, '{a,1}', '9'::jsonb) AS ins,
                 jsonb_insert('[1,2]'::jsonb, '{1}', '9'::jsonb, true) AS after,
                 jsonb_strip_nulls('{"a":1,"b":null}'::jsonb) AS strip
                 FROM generate_series(1, 1)"#,
            )
            .unwrap();
        let ins = row.rows[0].get("ins").unwrap();
        assert!(ins.contains("[1,9,2]") || (ins.contains('9') && ins.contains('1')), "ins={ins}");
        assert_eq!(row.rows[0].get("after"), Some("[1,2,9]"));
        let strip = row.rows[0].get("strip").unwrap();
        assert!(strip.contains("\"a\":1") && !strip.contains("\"b\""), "strip={strip}");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_to_json_e2e() {
        let (mut session, root) = temp_session("to-json");

        let row = session
            .execute_sql(
                r#"SELECT to_json(1) AS n,
                 to_jsonb(true) AS b,
                 to_json('hi') AS s,
                 array_to_json(ARRAY[1, 2]) AS a,
                 to_json(NULL) AS z
                 FROM generate_series(1, 1)"#,
            )
            .unwrap();
        assert_eq!(row.rows[0].get("n"), Some("1"));
        assert_eq!(row.rows[0].get("b"), Some("true"));
        assert_eq!(row.rows[0].get("s"), Some("\"hi\""));
        assert_eq!(row.rows[0].get("a"), Some("[1,2]"));
        assert_eq!(row.rows[0].get("z"), Some("null"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_json_agg_e2e() {
        let (mut session, root) = temp_session("json-agg");
        session
            .execute_sql(
                "CREATE TABLE emp (id BIGINT PRIMARY KEY, department TEXT, name TEXT)",
            )
            .unwrap();
        for (id, dept, name) in [
            (1, "Eng", "Ada"),
            (2, "Eng", "Di"),
            (3, "Sales", "Bob"),
        ] {
            session
                .execute_sql(&format!(
                    "INSERT INTO emp (id, department, name) VALUES ({id}, '{dept}', '{name}')"
                ))
                .unwrap();
        }

        let rows = session
            .execute_sql(
                "SELECT department, json_agg(name) FROM emp GROUP BY department",
            )
            .unwrap();
        assert_eq!(rows.rows.len(), 2);
        let mut by_dept = std::collections::BTreeMap::new();
        for r in &rows.rows {
            by_dept.insert(
                r.get("department").unwrap_or("").to_string(),
                r.get("json_agg(name)").unwrap_or("").to_string(),
            );
        }
        let eng = by_dept.get("Eng").unwrap();
        assert!(
            eng.contains("\"Ada\"") && eng.contains("\"Di\""),
            "eng names={eng}"
        );
        let sales = by_dept.get("Sales").unwrap();
        assert!(sales.contains("\"Bob\""), "sales={sales}");

        let global = session
            .execute_sql("SELECT jsonb_agg(id) FROM emp")
            .unwrap();
        let ids = global.rows[0].get("jsonb_agg(id)").unwrap();
        assert!(ids.starts_with('[') && ids.contains('1') && ids.contains('3'), "ids={ids}");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_json_object_agg_e2e() {
        let (mut session, root) = temp_session("json-object-agg");
        session
            .execute_sql(
                "CREATE TABLE emp (id BIGINT PRIMARY KEY, department TEXT, name TEXT)",
            )
            .unwrap();
        for (id, dept, name) in [
            (1, "Eng", "Ada"),
            (2, "Eng", "Di"),
            (3, "Sales", "Bob"),
        ] {
            session
                .execute_sql(&format!(
                    "INSERT INTO emp (id, department, name) VALUES ({id}, '{dept}', '{name}')"
                ))
                .unwrap();
        }

        let rows = session
            .execute_sql(
                "SELECT department, json_object_agg(name, id) FROM emp GROUP BY department",
            )
            .unwrap();
        assert_eq!(rows.rows.len(), 2);
        let mut by_dept = std::collections::BTreeMap::new();
        for r in &rows.rows {
            by_dept.insert(
                r.get("department").unwrap_or("").to_string(),
                r.get("json_object_agg(name)").unwrap_or("").to_string(),
            );
        }
        let eng = by_dept.get("Eng").unwrap();
        assert!(
            eng.contains("\"Ada\":1") && eng.contains("\"Di\":2"),
            "eng={eng}"
        );
        let sales = by_dept.get("Sales").unwrap();
        assert!(sales.contains("\"Bob\":3"), "sales={sales}");

        let global = session
            .execute_sql("SELECT jsonb_object_agg(name, id) FROM emp")
            .unwrap();
        let obj = global.rows[0].get("jsonb_object_agg(name)").unwrap();
        assert!(
            obj.contains("\"Ada\":1") && obj.contains("\"Bob\":3"),
            "obj={obj}"
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_string_agg_array_agg_e2e() {
        let (mut session, root) = temp_session("string-array-agg");
        session
            .execute_sql(
                "CREATE TABLE emp (id BIGINT PRIMARY KEY, department TEXT, name TEXT)",
            )
            .unwrap();
        for (id, dept, name) in [
            (1, "Eng", "Ada"),
            (2, "Eng", "Di"),
            (3, "Sales", "Bob"),
        ] {
            session
                .execute_sql(&format!(
                    "INSERT INTO emp (id, department, name) VALUES ({id}, '{dept}', '{name}')"
                ))
                .unwrap();
        }

        let rows = session
            .execute_sql(
                "SELECT department, string_agg(name, ',') FROM emp GROUP BY department",
            )
            .unwrap();
        assert_eq!(rows.rows.len(), 2);
        let mut by_dept = std::collections::BTreeMap::new();
        for r in &rows.rows {
            by_dept.insert(
                r.get("department").unwrap_or("").to_string(),
                r.get("string_agg(name)").unwrap_or("").to_string(),
            );
        }
        let eng = by_dept.get("Eng").unwrap();
        assert!(
            (eng == "Ada,Di" || eng == "Di,Ada"),
            "eng names={eng}"
        );
        assert_eq!(by_dept.get("Sales").map(String::as_str), Some("Bob"));

        let arrays = session
            .execute_sql(
                "SELECT department, array_agg(name) FROM emp GROUP BY department",
            )
            .unwrap();
        let mut arr_by = std::collections::BTreeMap::new();
        for r in &arrays.rows {
            arr_by.insert(
                r.get("department").unwrap_or("").to_string(),
                r.get("array_agg(name)").unwrap_or("").to_string(),
            );
        }
        let eng_arr = arr_by.get("Eng").unwrap();
        assert!(
            eng_arr == "[Ada,Di]" || eng_arr == "[Di,Ada]",
            "eng_arr={eng_arr}"
        );
        assert_eq!(arr_by.get("Sales").map(String::as_str), Some("[Bob]"));

        let global = session
            .execute_sql("SELECT string_agg(id, '|') FROM emp")
            .unwrap();
        let ids = global.rows[0].get("string_agg(id)").unwrap();
        assert!(ids.contains('1') && ids.contains('2') && ids.contains('3'), "ids={ids}");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_bool_and_or_e2e() {
        let (mut session, root) = temp_session("bool-and-or");
        session
            .execute_sql(
                "CREATE TABLE flags (id BIGINT PRIMARY KEY, grp TEXT, ok BOOLEAN)",
            )
            .unwrap();
        for (id, grp, ok) in [
            (1, "A", "true"),
            (2, "A", "true"),
            (3, "B", "true"),
            (4, "B", "false"),
            (5, "C", "false"),
        ] {
            session
                .execute_sql(&format!(
                    "INSERT INTO flags (id, grp, ok) VALUES ({id}, '{grp}', {ok})"
                ))
                .unwrap();
        }

        let rows = session
            .execute_sql(
                "SELECT grp, bool_and(ok), bool_or(ok) FROM flags GROUP BY grp",
            )
            .unwrap();
        assert_eq!(rows.rows.len(), 3);
        let mut by = std::collections::BTreeMap::new();
        for r in &rows.rows {
            by.insert(
                r.get("grp").unwrap_or("").to_string(),
                (
                    r.get("bool_and(ok)").unwrap_or("").to_string(),
                    r.get("bool_or(ok)").unwrap_or("").to_string(),
                ),
            );
        }
        assert_eq!(by.get("A").map(|(a, o)| (a.as_str(), o.as_str())), Some(("true", "true")));
        assert_eq!(by.get("B").map(|(a, o)| (a.as_str(), o.as_str())), Some(("false", "true")));
        assert_eq!(by.get("C").map(|(a, o)| (a.as_str(), o.as_str())), Some(("false", "false")));

        let every = session
            .execute_sql("SELECT every(ok) FROM flags WHERE grp = 'A'")
            .unwrap();
        assert_eq!(every.rows[0].get("bool_and(ok)"), Some("true"));

        let empty = session
            .execute_sql("SELECT bool_or(ok) FROM flags WHERE grp = 'Z'")
            .unwrap();
        assert!(
            empty.rows[0].get("bool_or(ok)").unwrap_or("x").is_empty()
                || empty.rows[0].get("bool_or(ok)") == Some(""),
            "empty group should be NULL, got {:?}",
            empty.rows[0].get("bool_or(ok)")
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_bit_and_or_e2e() {
        let (mut session, root) = temp_session("bit-and-or");
        session
            .execute_sql(
                "CREATE TABLE bits (id BIGINT PRIMARY KEY, grp TEXT, flags BIGINT)",
            )
            .unwrap();
        for (id, grp, flags) in [
            (1, "A", 0b111),
            (2, "A", 0b101),
            (3, "B", 0b010),
            (4, "B", 0b100),
        ] {
            session
                .execute_sql(&format!(
                    "INSERT INTO bits (id, grp, flags) VALUES ({id}, '{grp}', {flags})"
                ))
                .unwrap();
        }

        let rows = session
            .execute_sql(
                "SELECT grp, bit_and(flags), bit_or(flags) FROM bits GROUP BY grp",
            )
            .unwrap();
        assert_eq!(rows.rows.len(), 2);
        let mut by = std::collections::BTreeMap::new();
        for r in &rows.rows {
            by.insert(
                r.get("grp").unwrap_or("").to_string(),
                (
                    r.get("bit_and(flags)").unwrap_or("").to_string(),
                    r.get("bit_or(flags)").unwrap_or("").to_string(),
                ),
            );
        }
        assert_eq!(by.get("A").map(|(a, o)| (a.as_str(), o.as_str())), Some(("5", "7")));
        assert_eq!(by.get("B").map(|(a, o)| (a.as_str(), o.as_str())), Some(("0", "6")));

        let global = session
            .execute_sql("SELECT bit_and(flags), bit_or(flags) FROM bits")
            .unwrap();
        assert_eq!(global.rows[0].get("bit_and(flags)"), Some("0"));
        assert_eq!(global.rows[0].get("bit_or(flags)"), Some("7"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_aggregate_filter_e2e() {
        let (mut session, root) = temp_session("agg-filter");
        session
            .execute_sql(
                "CREATE TABLE emp (id BIGINT PRIMARY KEY, department TEXT, salary BIGINT)",
            )
            .unwrap();
        for (id, dept, salary) in [
            (1, "Eng", 120),
            (2, "Eng", 80),
            (3, "Sales", 150),
            (4, "Sales", 90),
        ] {
            session
                .execute_sql(&format!(
                    "INSERT INTO emp (id, department, salary) VALUES ({id}, '{dept}', {salary})"
                ))
                .unwrap();
        }

        let rows = session
            .execute_sql(
                "SELECT department, COUNT(*) FILTER (WHERE salary > 100), COUNT(*) \
                 FROM emp GROUP BY department",
            )
            .unwrap();
        assert_eq!(rows.rows.len(), 2);
        let mut by = std::collections::BTreeMap::new();
        for r in &rows.rows {
            by.insert(
                r.get("department").unwrap_or("").to_string(),
                (
                    r.get("count(*) filter").unwrap_or("").to_string(),
                    r.get("count(*)").unwrap_or("").to_string(),
                ),
            );
        }
        assert_eq!(by.get("Eng").map(|(a, b)| (a.as_str(), b.as_str())), Some(("1", "2")));
        assert_eq!(by.get("Sales").map(|(a, b)| (a.as_str(), b.as_str())), Some(("1", "2")));

        let sum = session
            .execute_sql(
                "SELECT SUM(salary) FILTER (WHERE department = 'Eng') FROM emp",
            )
            .unwrap();
        assert_eq!(sum.rows[0].get("sum(salary) filter"), Some("200"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_count_distinct_e2e() {
        let (mut session, root) = temp_session("count-distinct");
        session
            .execute_sql(
                "CREATE TABLE emp (id BIGINT PRIMARY KEY, department TEXT, name TEXT)",
            )
            .unwrap();
        for (id, dept, name) in [
            (1, "Eng", "Ada"),
            (2, "Eng", "Di"),
            (3, "Sales", "Bob"),
            (4, "Eng", "Ada"),
        ] {
            session
                .execute_sql(&format!(
                    "INSERT INTO emp (id, department, name) VALUES ({id}, '{dept}', '{name}')"
                ))
                .unwrap();
        }

        let global = session
            .execute_sql("SELECT COUNT(DISTINCT department), COUNT(DISTINCT name), COUNT(*) FROM emp")
            .unwrap();
        assert_eq!(global.rows[0].get("count(distinct department)"), Some("2"));
        assert_eq!(global.rows[0].get("count(distinct name)"), Some("3"));
        assert_eq!(global.rows[0].get("count(*)"), Some("4"));

        let grouped = session
            .execute_sql(
                "SELECT department, COUNT(DISTINCT name) FROM emp GROUP BY department",
            )
            .unwrap();
        let mut by = std::collections::BTreeMap::new();
        for r in &grouped.rows {
            by.insert(
                r.get("department").unwrap_or("").to_string(),
                r.get("count(distinct name)").unwrap_or("").to_string(),
            );
        }
        assert_eq!(by.get("Eng").map(String::as_str), Some("2"));
        assert_eq!(by.get("Sales").map(String::as_str), Some("1"));

        let filtered = session
            .execute_sql(
                "SELECT COUNT(DISTINCT name) FILTER (WHERE department = 'Eng') FROM emp",
            )
            .unwrap();
        assert_eq!(
            filtered.rows[0].get("count(distinct name) filter"),
            Some("2")
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_string_agg_order_by_e2e() {
        let (mut session, root) = temp_session("string-agg-order");
        session
            .execute_sql(
                "CREATE TABLE emp (id BIGINT PRIMARY KEY, department TEXT, name TEXT)",
            )
            .unwrap();
        for (id, dept, name) in [
            (1, "Eng", "Di"),
            (2, "Eng", "Ada"),
            (3, "Sales", "Zoe"),
            (4, "Sales", "Bob"),
        ] {
            session
                .execute_sql(&format!(
                    "INSERT INTO emp (id, department, name) VALUES ({id}, '{dept}', '{name}')"
                ))
                .unwrap();
        }

        let rows = session
            .execute_sql(
                "SELECT department, string_agg(name, ',' ORDER BY name) FROM emp GROUP BY department",
            )
            .unwrap();
        let mut by = std::collections::BTreeMap::new();
        for r in &rows.rows {
            by.insert(
                r.get("department").unwrap_or("").to_string(),
                r.get("string_agg(name)").unwrap_or("").to_string(),
            );
        }
        assert_eq!(by.get("Eng").map(String::as_str), Some("Ada,Di"));
        assert_eq!(by.get("Sales").map(String::as_str), Some("Bob,Zoe"));

        let desc = session
            .execute_sql(
                "SELECT string_agg(name, '|' ORDER BY name DESC) FROM emp WHERE department = 'Eng'",
            )
            .unwrap();
        assert_eq!(desc.rows[0].get("string_agg(name)"), Some("Di|Ada"));

        let arr = session
            .execute_sql(
                "SELECT array_agg(name ORDER BY id) FROM emp WHERE department = 'Eng'",
            )
            .unwrap();
        assert_eq!(arr.rows[0].get("array_agg(name)"), Some("[Di,Ada]"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_stddev_variance_e2e() {
        let (mut session, root) = temp_session("stddev-var");
        session
            .execute_sql("CREATE TABLE nums (id BIGINT PRIMARY KEY, x BIGINT)")
            .unwrap();
        for (id, x) in [(1, 2), (2, 4), (3, 4), (4, 6)] {
            session
                .execute_sql(&format!("INSERT INTO nums (id, x) VALUES ({id}, {x})"))
                .unwrap();
        }
        // values 2,4,4,6 — mean 4; sample var = ((4+0+0+4)/3)=8/3; stddev=sqrt(8/3)
        // pop var = 8/4=2; stddev_pop=sqrt(2)

        let row = session
            .execute_sql(
                "SELECT stddev_samp(x), stddev_pop(x), var_samp(x), var_pop(x), \
                 stddev(x), variance(x) FROM nums",
            )
            .unwrap();
        let samp = row.rows[0].get("stddev_samp(x)").unwrap().parse::<f64>().unwrap();
        let pop = row.rows[0].get("stddev_pop(x)").unwrap().parse::<f64>().unwrap();
        let vs = row.rows[0].get("var_samp(x)").unwrap().parse::<f64>().unwrap();
        let vp = row.rows[0].get("var_pop(x)").unwrap().parse::<f64>().unwrap();
        assert!((vs - 8.0 / 3.0).abs() < 1e-9, "var_samp={vs}");
        assert!((vp - 2.0).abs() < 1e-9, "var_pop={vp}");
        assert!((samp - (8.0_f64 / 3.0).sqrt()).abs() < 1e-9, "stddev_samp={samp}");
        assert!((pop - 2.0_f64.sqrt()).abs() < 1e-9, "stddev_pop={pop}");
        // Aliases rewrite to *_SAMP names in the result column.
        assert!(row.rows[0].get("stddev_samp(x)").is_some());
        assert!(row.rows[0].get("var_samp(x)").is_some());

        let single = session
            .execute_sql("SELECT stddev(x) FROM nums WHERE id = 1")
            .unwrap();
        assert!(
            single.rows[0].get("stddev_samp(x)").unwrap_or("x").is_empty(),
            "sample stddev of 1 row is NULL, got {:?}",
            single.rows[0]
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_corr_covar_e2e() {
        let (mut session, root) = temp_session("corr-covar");
        session
            .execute_sql("CREATE TABLE pts (id BIGINT PRIMARY KEY, y BIGINT, x BIGINT)")
            .unwrap();
        // Perfect line y = 2x: (1,2), (2,4), (3,6) → corr=1
        for (id, x, y) in [(1, 1, 2), (2, 2, 4), (3, 3, 6)] {
            session
                .execute_sql(&format!(
                    "INSERT INTO pts (id, y, x) VALUES ({id}, {y}, {x})"
                ))
                .unwrap();
        }

        let row = session
            .execute_sql(
                "SELECT corr(y, x), covar_pop(y, x), covar_samp(y, x) FROM pts",
            )
            .unwrap();
        let corr = row.rows[0].get("corr(y)").unwrap().parse::<f64>().unwrap();
        let cp = row.rows[0].get("covar_pop(y)").unwrap().parse::<f64>().unwrap();
        let cs = row.rows[0].get("covar_samp(y)").unwrap().parse::<f64>().unwrap();
        assert!((corr - 1.0).abs() < 1e-9, "corr={corr}");
        // mean_x=2, mean_y=4; C = sum (x-2)(y-4) = (-1)(-2)+(0)(0)+(1)(2)=4
        // covar_pop=4/3, covar_samp=4/2=2
        assert!((cp - 4.0 / 3.0).abs() < 1e-9, "covar_pop={cp}");
        assert!((cs - 2.0).abs() < 1e-9, "covar_samp={cs}");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_regr_e2e() {
        let (mut session, root) = temp_session("regr-stats");
        session
            .execute_sql("CREATE TABLE pts (id BIGINT PRIMARY KEY, y BIGINT, x BIGINT)")
            .unwrap();
        // y = 2x + 1 → (1,3), (2,5), (3,7)
        for (id, x, y) in [(1, 1, 3), (2, 2, 5), (3, 3, 7)] {
            session
                .execute_sql(&format!(
                    "INSERT INTO pts (id, y, x) VALUES ({id}, {y}, {x})"
                ))
                .unwrap();
        }

        let row = session
            .execute_sql(
                "SELECT regr_slope(y, x), regr_intercept(y, x), regr_r2(y, x), \
                 regr_count(y, x), regr_avgx(y, x), regr_avgy(y, x), \
                 regr_sxx(y, x), regr_syy(y, x), regr_sxy(y, x) FROM pts",
            )
            .unwrap();
        let slope = row.rows[0]
            .get("regr_slope(y)")
            .unwrap()
            .parse::<f64>()
            .unwrap();
        let intercept = row.rows[0]
            .get("regr_intercept(y)")
            .unwrap()
            .parse::<f64>()
            .unwrap();
        let r2 = row.rows[0]
            .get("regr_r2(y)")
            .unwrap()
            .parse::<f64>()
            .unwrap();
        assert!((slope - 2.0).abs() < 1e-9, "slope={slope}");
        assert!((intercept - 1.0).abs() < 1e-9, "intercept={intercept}");
        assert!((r2 - 1.0).abs() < 1e-9, "r2={r2}");
        assert_eq!(row.rows[0].get("regr_count(y)"), Some("3"));
        let avgx = row.rows[0].get("regr_avgx(y)").unwrap().parse::<f64>().unwrap();
        let avgy = row.rows[0].get("regr_avgy(y)").unwrap().parse::<f64>().unwrap();
        assert!((avgx - 2.0).abs() < 1e-9, "avgx={avgx}");
        assert!((avgy - 5.0).abs() < 1e-9, "avgy={avgy}");
        let sxx = row.rows[0].get("regr_sxx(y)").unwrap().parse::<f64>().unwrap();
        let syy = row.rows[0].get("regr_syy(y)").unwrap().parse::<f64>().unwrap();
        let sxy = row.rows[0].get("regr_sxy(y)").unwrap().parse::<f64>().unwrap();
        assert!((sxx - 2.0).abs() < 1e-9, "sxx={sxx}");
        assert!((syy - 8.0).abs() < 1e-9, "syy={syy}");
        assert!((sxy - 4.0).abs() < 1e-9, "sxy={sxy}");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_mode_e2e() {
        let (mut session, root) = temp_session("mode-agg");
        session
            .execute_sql(
                "CREATE TABLE votes (id BIGINT PRIMARY KEY, grp TEXT, color TEXT)",
            )
            .unwrap();
        for (id, grp, color) in [
            (1, "A", "red"),
            (2, "A", "blue"),
            (3, "A", "red"),
            (4, "B", "green"),
            (5, "B", "green"),
            (6, "B", "yellow"),
        ] {
            session
                .execute_sql(&format!(
                    "INSERT INTO votes (id, grp, color) VALUES ({id}, '{grp}', '{color}')"
                ))
                .unwrap();
        }

        let global = session
            .execute_sql("SELECT mode(color) FROM votes")
            .unwrap();
        // red×2, green×2, blue×1, yellow×1 → tie red/green → ASC picks green
        assert_eq!(global.rows[0].get("mode(color)"), Some("green"));

        let grouped = session
            .execute_sql("SELECT grp, mode(color) FROM votes GROUP BY grp")
            .unwrap();
        let mut by = std::collections::BTreeMap::new();
        for r in &grouped.rows {
            by.insert(
                r.get("grp").unwrap_or("").to_string(),
                r.get("mode(color)").unwrap_or("").to_string(),
            );
        }
        assert_eq!(by.get("A").map(String::as_str), Some("red"));
        assert_eq!(by.get("B").map(String::as_str), Some("green"));

        let wg = session
            .execute_sql("SELECT mode() WITHIN GROUP (ORDER BY color DESC) FROM votes")
            .unwrap();
        // tie red/green → DESC picks red
        assert_eq!(wg.rows[0].get("mode(color)"), Some("red"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_row_number_e2e() {
        let (mut session, root) = temp_session("row-number");
        session
            .execute_sql(
                "CREATE TABLE emp (id BIGINT PRIMARY KEY, name TEXT, salary BIGINT)",
            )
            .unwrap();
        for (id, name, sal) in [(1, "Ada", 200), (2, "Di", 100), (3, "Bob", 300)] {
            session
                .execute_sql(&format!(
                    "INSERT INTO emp (id, name, salary) VALUES ({id}, '{name}', {sal})"
                ))
                .unwrap();
        }

        let rows = session
            .execute_sql(
                "SELECT name, ROW_NUMBER() OVER (ORDER BY salary DESC) AS rn FROM emp",
            )
            .unwrap();
        assert_eq!(rows.rows.len(), 3);
        assert_eq!(rows.rows[0].get("name"), Some("Bob"));
        assert_eq!(rows.rows[0].get("rn"), Some("1"));
        assert_eq!(rows.rows[1].get("name"), Some("Ada"));
        assert_eq!(rows.rows[1].get("rn"), Some("2"));
        assert_eq!(rows.rows[2].get("name"), Some("Di"));
        assert_eq!(rows.rows[2].get("rn"), Some("3"));

        let plain = session
            .execute_sql("SELECT ROW_NUMBER() OVER () AS n FROM emp WHERE id = 1")
            .unwrap();
        assert_eq!(plain.rows.len(), 1);
        assert_eq!(plain.rows[0].get("n"), Some("1"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_rank_dense_rank_e2e() {
        let (mut session, root) = temp_session("rank-dense");
        session
            .execute_sql(
                "CREATE TABLE emp (id BIGINT PRIMARY KEY, name TEXT, salary BIGINT)",
            )
            .unwrap();
        // salaries: 100,100,200 → RANK 1,1,3 and DENSE_RANK 1,1,2
        for (id, name, sal) in [(1, "Ada", 100), (2, "Di", 100), (3, "Bob", 200)] {
            session
                .execute_sql(&format!(
                    "INSERT INTO emp (id, name, salary) VALUES ({id}, '{name}', {sal})"
                ))
                .unwrap();
        }

        let rows = session
            .execute_sql(
                "SELECT name, RANK() OVER (ORDER BY salary) AS r, \
                 DENSE_RANK() OVER (ORDER BY salary) AS d FROM emp",
            )
            .unwrap();
        assert_eq!(rows.rows.len(), 3);
        let mut by_name = std::collections::BTreeMap::new();
        for row in &rows.rows {
            by_name.insert(
                row.get("name").unwrap_or("").to_string(),
                (
                    row.get("r").unwrap_or("").to_string(),
                    row.get("d").unwrap_or("").to_string(),
                ),
            );
        }
        assert_eq!(by_name.get("Ada").map(|(r, d)| (r.as_str(), d.as_str())), Some(("1", "1")));
        assert_eq!(by_name.get("Di").map(|(r, d)| (r.as_str(), d.as_str())), Some(("1", "1")));
        assert_eq!(by_name.get("Bob").map(|(r, d)| (r.as_str(), d.as_str())), Some(("3", "2")));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_window_partition_by_e2e() {
        let (mut session, root) = temp_session("window-partition");
        session
            .execute_sql(
                "CREATE TABLE emp (id BIGINT PRIMARY KEY, dept TEXT, name TEXT, salary BIGINT)",
            )
            .unwrap();
        for (id, dept, name, sal) in [
            (1, "Eng", "Ada", 200),
            (2, "Eng", "Di", 100),
            (3, "Sales", "Bob", 50),
            (4, "Sales", "Zoe", 150),
        ] {
            session
                .execute_sql(&format!(
                    "INSERT INTO emp (id, dept, name, salary) VALUES ({id}, '{dept}', '{name}', {sal})"
                ))
                .unwrap();
        }

        let rows = session
            .execute_sql(
                "SELECT dept, name, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn \
                 FROM emp",
            )
            .unwrap();
        assert_eq!(rows.rows.len(), 4);
        let mut by = std::collections::BTreeMap::new();
        for r in &rows.rows {
            by.insert(
                (
                    r.get("dept").unwrap_or("").to_string(),
                    r.get("name").unwrap_or("").to_string(),
                ),
                r.get("rn").unwrap_or("").to_string(),
            );
        }
        assert_eq!(by.get(&("Eng".into(), "Ada".into())).map(String::as_str), Some("1"));
        assert_eq!(by.get(&("Eng".into(), "Di".into())).map(String::as_str), Some("2"));
        assert_eq!(by.get(&("Sales".into(), "Zoe".into())).map(String::as_str), Some("1"));
        assert_eq!(by.get(&("Sales".into(), "Bob".into())).map(String::as_str), Some("2"));

        let ranked = session
            .execute_sql(
                "SELECT name, RANK() OVER (PARTITION BY dept ORDER BY salary) AS r FROM emp \
                 WHERE dept = 'Eng'",
            )
            .unwrap();
        assert_eq!(ranked.rows.len(), 2);
        let mut eng = std::collections::BTreeMap::new();
        for r in &ranked.rows {
            eng.insert(
                r.get("name").unwrap_or("").to_string(),
                r.get("r").unwrap_or("").to_string(),
            );
        }
        assert_eq!(eng.get("Di").map(String::as_str), Some("1"));
        assert_eq!(eng.get("Ada").map(String::as_str), Some("2"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_lag_lead_e2e() {
        let (mut session, root) = temp_session("lag-lead");
        session
            .execute_sql(
                "CREATE TABLE emp (id BIGINT PRIMARY KEY, dept TEXT, name TEXT, salary BIGINT)",
            )
            .unwrap();
        for (id, dept, name, sal) in [
            (1, "Eng", "Ada", 100),
            (2, "Eng", "Di", 200),
            (3, "Sales", "Bob", 50),
        ] {
            session
                .execute_sql(&format!(
                    "INSERT INTO emp (id, dept, name, salary) VALUES ({id}, '{dept}', '{name}', {sal})"
                ))
                .unwrap();
        }

        let rows = session
            .execute_sql(
                "SELECT name, LAG(salary) OVER (ORDER BY id) AS prev, \
                 LEAD(salary) OVER (ORDER BY id) AS next FROM emp",
            )
            .unwrap();
        assert_eq!(rows.rows.len(), 3);
        let mut by = std::collections::BTreeMap::new();
        for r in &rows.rows {
            by.insert(
                r.get("name").unwrap_or("").to_string(),
                (
                    r.get("prev").unwrap_or("").to_string(),
                    r.get("next").unwrap_or("").to_string(),
                ),
            );
        }
        assert_eq!(by.get("Ada").map(|(p, n)| (p.as_str(), n.as_str())), Some(("", "200")));
        assert_eq!(by.get("Di").map(|(p, n)| (p.as_str(), n.as_str())), Some(("100", "50")));
        assert_eq!(by.get("Bob").map(|(p, n)| (p.as_str(), n.as_str())), Some(("200", "")));

        let with_default = session
            .execute_sql(
                "SELECT name, LAG(salary, 1, -1) OVER (PARTITION BY dept ORDER BY id) AS prev \
                 FROM emp WHERE dept = 'Eng'",
            )
            .unwrap();
        let mut eng = std::collections::BTreeMap::new();
        for r in &with_default.rows {
            eng.insert(
                r.get("name").unwrap_or("").to_string(),
                r.get("prev").unwrap_or("").to_string(),
            );
        }
        assert_eq!(eng.get("Ada").map(String::as_str), Some("-1"));
        assert_eq!(eng.get("Di").map(String::as_str), Some("100"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_ntile_e2e() {
        let (mut session, root) = temp_session("ntile");
        session
            .execute_sql("CREATE TABLE emp (id BIGINT PRIMARY KEY, name TEXT, salary BIGINT)")
            .unwrap();
        for (id, name, sal) in [
            (1, "A", 10),
            (2, "B", 20),
            (3, "C", 30),
            (4, "D", 40),
            (5, "E", 50),
        ] {
            session
                .execute_sql(&format!(
                    "INSERT INTO emp (id, name, salary) VALUES ({id}, '{name}', {sal})"
                ))
                .unwrap();
        }

        let rows = session
            .execute_sql(
                "SELECT name, NTILE(3) OVER (ORDER BY salary) AS bucket FROM emp ORDER BY salary",
            )
            .unwrap();
        assert_eq!(rows.rows.len(), 5);
        // PG: with 5 rows / 3 buckets → sizes 2,2,1
        let expected = [("A", "1"), ("B", "1"), ("C", "2"), ("D", "2"), ("E", "3")];
        for (i, (name, bucket)) in expected.iter().enumerate() {
            assert_eq!(rows.rows[i].get("name").unwrap_or(""), *name);
            assert_eq!(rows.rows[i].get("bucket").unwrap_or(""), *bucket);
        }

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_first_last_value_e2e() {
        let (mut session, root) = temp_session("first-last-value");
        session
            .execute_sql(
                "CREATE TABLE emp (id BIGINT PRIMARY KEY, dept TEXT, name TEXT, salary BIGINT)",
            )
            .unwrap();
        for (id, dept, name, sal) in [
            (1, "Eng", "Ada", 100),
            (2, "Eng", "Di", 200),
            (3, "Sales", "Bob", 50),
            (4, "Sales", "Zoe", 150),
        ] {
            session
                .execute_sql(&format!(
                    "INSERT INTO emp (id, dept, name, salary) VALUES ({id}, '{dept}', '{name}', {sal})"
                ))
                .unwrap();
        }

        let rows = session
            .execute_sql(
                "SELECT name, \
                 FIRST_VALUE(name) OVER (PARTITION BY dept ORDER BY salary) AS first_name, \
                 LAST_VALUE(salary) OVER (PARTITION BY dept ORDER BY salary) AS last_sal \
                 FROM emp",
            )
            .unwrap();
        assert_eq!(rows.rows.len(), 4);
        let mut by = std::collections::BTreeMap::new();
        for r in &rows.rows {
            by.insert(
                r.get("name").unwrap_or("").to_string(),
                (
                    r.get("first_name").unwrap_or("").to_string(),
                    r.get("last_sal").unwrap_or("").to_string(),
                ),
            );
        }
        // Full-partition semantics (frames unsupported): Eng → Ada/200, Sales → Bob/150
        assert_eq!(
            by.get("Ada").map(|(f, l)| (f.as_str(), l.as_str())),
            Some(("Ada", "200"))
        );
        assert_eq!(
            by.get("Di").map(|(f, l)| (f.as_str(), l.as_str())),
            Some(("Ada", "200"))
        );
        assert_eq!(
            by.get("Bob").map(|(f, l)| (f.as_str(), l.as_str())),
            Some(("Bob", "150"))
        );
        assert_eq!(
            by.get("Zoe").map(|(f, l)| (f.as_str(), l.as_str())),
            Some(("Bob", "150"))
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_nth_value_e2e() {
        let (mut session, root) = temp_session("nth-value");
        session
            .execute_sql(
                "CREATE TABLE emp (id BIGINT PRIMARY KEY, dept TEXT, name TEXT, salary BIGINT)",
            )
            .unwrap();
        for (id, dept, name, sal) in [
            (1, "Eng", "Ada", 100),
            (2, "Eng", "Di", 200),
            (3, "Eng", "Eve", 300),
            (4, "Sales", "Bob", 50),
        ] {
            session
                .execute_sql(&format!(
                    "INSERT INTO emp (id, dept, name, salary) VALUES ({id}, '{dept}', '{name}', {sal})"
                ))
                .unwrap();
        }

        let rows = session
            .execute_sql(
                "SELECT name, NTH_VALUE(name, 2) OVER (PARTITION BY dept ORDER BY salary) AS second \
                 FROM emp",
            )
            .unwrap();
        assert_eq!(rows.rows.len(), 4);
        let mut by = std::collections::BTreeMap::new();
        for r in &rows.rows {
            by.insert(
                r.get("name").unwrap_or("").to_string(),
                r.get("second").unwrap_or("").to_string(),
            );
        }
        // Eng ordered by salary: Ada, Di, Eve → 2nd is Di for all Eng rows
        assert_eq!(by.get("Ada").map(String::as_str), Some("Di"));
        assert_eq!(by.get("Di").map(String::as_str), Some("Di"));
        assert_eq!(by.get("Eve").map(String::as_str), Some("Di"));
        // Sales only has Bob → 2nd is NULL
        assert_eq!(by.get("Bob").map(String::as_str), Some(""));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_percent_rank_cume_dist_e2e() {
        let (mut session, root) = temp_session("percent-rank-cume");
        session
            .execute_sql("CREATE TABLE emp (id BIGINT PRIMARY KEY, name TEXT, salary BIGINT)")
            .unwrap();
        for (id, name, sal) in [
            (1, "A", 100),
            (2, "B", 200),
            (3, "C", 200),
            (4, "D", 300),
        ] {
            session
                .execute_sql(&format!(
                    "INSERT INTO emp (id, name, salary) VALUES ({id}, '{name}', {sal})"
                ))
                .unwrap();
        }

        let rows = session
            .execute_sql(
                "SELECT name, PERCENT_RANK() OVER (ORDER BY salary) AS pr, \
                 CUME_DIST() OVER (ORDER BY salary) AS cd FROM emp ORDER BY salary, name",
            )
            .unwrap();
        assert_eq!(rows.rows.len(), 4);
        let parse = |s: &str| s.parse::<f64>().unwrap_or(f64::NAN);
        // RANK 1,2,2,4 → PR 0, 1/3, 1/3, 1; CD 0.25, 0.75, 0.75, 1
        assert_eq!(rows.rows[0].get("name").unwrap_or(""), "A");
        assert!((parse(rows.rows[0].get("pr").unwrap_or("")) - 0.0).abs() < 1e-9);
        assert!((parse(rows.rows[0].get("cd").unwrap_or("")) - 0.25).abs() < 1e-9);
        assert!((parse(rows.rows[1].get("pr").unwrap_or("")) - (1.0 / 3.0)).abs() < 1e-9);
        assert!((parse(rows.rows[1].get("cd").unwrap_or("")) - 0.75).abs() < 1e-9);
        assert!((parse(rows.rows[2].get("pr").unwrap_or("")) - (1.0 / 3.0)).abs() < 1e-9);
        assert!((parse(rows.rows[2].get("cd").unwrap_or("")) - 0.75).abs() < 1e-9);
        assert!((parse(rows.rows[3].get("pr").unwrap_or("")) - 1.0).abs() < 1e-9);
        assert!((parse(rows.rows[3].get("cd").unwrap_or("")) - 1.0).abs() < 1e-9);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_window_rows_frame_e2e() {
        let (mut session, root) = temp_session("window-rows-frame");
        session
            .execute_sql("CREATE TABLE emp (id BIGINT PRIMARY KEY, name TEXT, salary BIGINT)")
            .unwrap();
        for (id, name, sal) in [(1, "A", 10), (2, "B", 20), (3, "C", 30)] {
            session
                .execute_sql(&format!(
                    "INSERT INTO emp (id, name, salary) VALUES ({id}, '{name}', {sal})"
                ))
                .unwrap();
        }

        // Default LAST_VALUE (no frame) = full partition → always 30
        let full = session
            .execute_sql(
                "SELECT name, LAST_VALUE(salary) OVER (ORDER BY id) AS last_sal FROM emp ORDER BY id",
            )
            .unwrap();
        assert_eq!(full.rows[0].get("last_sal").unwrap_or(""), "30");
        assert_eq!(full.rows[2].get("last_sal").unwrap_or(""), "30");

        // ROWS … CURRENT ROW → running last = current salary
        let running = session
            .execute_sql(
                "SELECT name, LAST_VALUE(salary) OVER (\
                   ORDER BY id \
                   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\
                 ) AS last_sal FROM emp ORDER BY id",
            )
            .unwrap();
        assert_eq!(running.rows[0].get("last_sal").unwrap_or(""), "10");
        assert_eq!(running.rows[1].get("last_sal").unwrap_or(""), "20");
        assert_eq!(running.rows[2].get("last_sal").unwrap_or(""), "30");

        // 1 PRECEDING → FIRST_VALUE within [i-1, i]
        let prev = session
            .execute_sql(
                "SELECT name, FIRST_VALUE(name) OVER (\
                   ORDER BY id \
                   ROWS BETWEEN 1 PRECEDING AND CURRENT ROW\
                 ) AS first_name FROM emp ORDER BY id",
            )
            .unwrap();
        assert_eq!(prev.rows[0].get("first_name").unwrap_or(""), "A");
        assert_eq!(prev.rows[1].get("first_name").unwrap_or(""), "A");
        assert_eq!(prev.rows[2].get("first_name").unwrap_or(""), "B");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_window_range_frame_e2e() {
        let (mut session, root) = temp_session("window-range-frame");
        session
            .execute_sql("CREATE TABLE emp (id BIGINT PRIMARY KEY, name TEXT, salary BIGINT)")
            .unwrap();
        for (id, name, sal) in [(1, "A", 10), (2, "B", 20), (3, "C", 20), (4, "D", 30)] {
            session
                .execute_sql(&format!(
                    "INSERT INTO emp (id, name, salary) VALUES ({id}, '{name}', {sal})"
                ))
                .unwrap();
        }

        // RANGE … CURRENT ROW includes the whole peer group of equal ORDER BY keys.
        // Running SUM: A=10, B=10+20+20=50, C=50, D=50+30=80
        let rows = session
            .execute_sql(
                "SELECT name, SUM(salary) OVER (\
                   ORDER BY salary \
                   RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\
                 ) AS running FROM emp ORDER BY salary, name",
            )
            .unwrap();
        assert_eq!(rows.rows.len(), 4);
        assert_eq!(rows.rows[0].get("name").unwrap_or(""), "A");
        assert_eq!(rows.rows[0].get("running").unwrap_or(""), "10");
        assert_eq!(rows.rows[1].get("running").unwrap_or(""), "50");
        assert_eq!(rows.rows[2].get("running").unwrap_or(""), "50");
        assert_eq!(rows.rows[3].get("running").unwrap_or(""), "80");

        // Value offset: RANGE 20 PRECEDING includes rows within salary-20 … salary
        let offset = session
            .execute_sql(
                "SELECT name, SUM(salary) OVER (\
                   ORDER BY salary \
                   RANGE BETWEEN 20 PRECEDING AND CURRENT ROW\
                 ) AS win FROM emp ORDER BY salary, name",
            )
            .unwrap();
        // A(10): [10]=10; B/C(20): [10,20,20]=50; D(30): [10,20,20,30]=80
        assert_eq!(offset.rows[0].get("win").unwrap_or(""), "10");
        assert_eq!(offset.rows[1].get("win").unwrap_or(""), "50");
        assert_eq!(offset.rows[2].get("win").unwrap_or(""), "50");
        assert_eq!(offset.rows[3].get("win").unwrap_or(""), "80");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_window_groups_frame_e2e() {
        let (mut session, root) = temp_session("window-groups-frame");
        session
            .execute_sql("CREATE TABLE emp (id BIGINT PRIMARY KEY, name TEXT, salary BIGINT)")
            .unwrap();
        // Groups by salary: {10}, {20,20}, {40}
        for (id, name, sal) in [(1, "A", 10), (2, "B", 20), (3, "C", 20), (4, "D", 40)] {
            session
                .execute_sql(&format!(
                    "INSERT INTO emp (id, name, salary) VALUES ({id}, '{name}', {sal})"
                ))
                .unwrap();
        }

        // GROUPS 1 PRECEDING: current group + previous group
        // A: {10}=10; B/C: {10}+{20,20}=50; D: {20,20}+{40}=80
        let rows = session
            .execute_sql(
                "SELECT name, SUM(salary) OVER (\
                   ORDER BY salary \
                   GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW\
                 ) AS win FROM emp ORDER BY salary, name",
            )
            .unwrap();
        assert_eq!(rows.rows.len(), 4);
        assert_eq!(rows.rows[0].get("win").unwrap_or(""), "10");
        assert_eq!(rows.rows[1].get("win").unwrap_or(""), "50");
        assert_eq!(rows.rows[2].get("win").unwrap_or(""), "50");
        assert_eq!(rows.rows[3].get("win").unwrap_or(""), "80");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_named_window_e2e() {
        let (mut session, root) = temp_session("named-window");
        session
            .execute_sql(
                "CREATE TABLE emp (id BIGINT PRIMARY KEY, dept TEXT, name TEXT, salary BIGINT)",
            )
            .unwrap();
        for (id, dept, name, sal) in [
            (1, "Eng", "Ada", 100),
            (2, "Eng", "Di", 200),
            (3, "Sales", "Bob", 50),
        ] {
            session
                .execute_sql(&format!(
                    "INSERT INTO emp (id, dept, name, salary) VALUES ({id}, '{dept}', '{name}', {sal})"
                ))
                .unwrap();
        }

        let rows = session
            .execute_sql(
                "SELECT name, ROW_NUMBER() OVER w AS rn FROM emp \
                 WINDOW w AS (PARTITION BY dept ORDER BY salary DESC)",
            )
            .unwrap();
        assert_eq!(rows.rows.len(), 3);
        let mut by = std::collections::BTreeMap::new();
        for r in &rows.rows {
            by.insert(
                r.get("name").unwrap_or("").to_string(),
                r.get("rn").unwrap_or("").to_string(),
            );
        }
        assert_eq!(by.get("Di").map(String::as_str), Some("1"));
        assert_eq!(by.get("Ada").map(String::as_str), Some("2"));
        assert_eq!(by.get("Bob").map(String::as_str), Some("1"));

        let refined = session
            .execute_sql(
                "SELECT name, RANK() OVER (w ORDER BY salary) AS r FROM emp WHERE dept = 'Eng' \
                 WINDOW w AS (PARTITION BY dept)",
            )
            .unwrap();
        let mut eng = std::collections::BTreeMap::new();
        for r in &refined.rows {
            eng.insert(
                r.get("name").unwrap_or("").to_string(),
                r.get("r").unwrap_or("").to_string(),
            );
        }
        assert_eq!(eng.get("Ada").map(String::as_str), Some("1"));
        assert_eq!(eng.get("Di").map(String::as_str), Some("2"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_window_agg_e2e() {
        let (mut session, root) = temp_session("window-agg");
        session
            .execute_sql(
                "CREATE TABLE emp (id BIGINT PRIMARY KEY, dept TEXT, name TEXT, salary BIGINT)",
            )
            .unwrap();
        for (id, dept, name, sal) in [
            (1, "Eng", "Ada", 100),
            (2, "Eng", "Di", 200),
            (3, "Sales", "Bob", 50),
        ] {
            session
                .execute_sql(&format!(
                    "INSERT INTO emp (id, dept, name, salary) VALUES ({id}, '{dept}', '{name}', {sal})"
                ))
                .unwrap();
        }

        // Full-partition COUNT / SUM
        let part = session
            .execute_sql(
                "SELECT name, COUNT(*) OVER (PARTITION BY dept) AS n, \
                 SUM(salary) OVER (PARTITION BY dept) AS total FROM emp",
            )
            .unwrap();
        let mut by = std::collections::BTreeMap::new();
        for r in &part.rows {
            by.insert(
                r.get("name").unwrap_or("").to_string(),
                (
                    r.get("n").unwrap_or("").to_string(),
                    r.get("total").unwrap_or("").to_string(),
                ),
            );
        }
        assert_eq!(by.get("Ada").map(|(n, t)| (n.as_str(), t.as_str())), Some(("2", "300")));
        assert_eq!(by.get("Di").map(|(n, t)| (n.as_str(), t.as_str())), Some(("2", "300")));
        assert_eq!(by.get("Bob").map(|(n, t)| (n.as_str(), t.as_str())), Some(("1", "50")));

        // Running SUM with ROWS frame
        let run = session
            .execute_sql(
                "SELECT name, SUM(salary) OVER (\
                   ORDER BY id \
                   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\
                 ) AS running FROM emp ORDER BY id",
            )
            .unwrap();
        assert_eq!(run.rows[0].get("running").unwrap_or(""), "100");
        assert_eq!(run.rows[1].get("running").unwrap_or(""), "300");
        assert_eq!(run.rows[2].get("running").unwrap_or(""), "350");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_window_string_array_agg_e2e() {
        let (mut session, root) = temp_session("window-string-array-agg");
        session
            .execute_sql(
                "CREATE TABLE emp (id BIGINT PRIMARY KEY, dept TEXT, name TEXT, salary BIGINT)",
            )
            .unwrap();
        for (id, dept, name, sal) in [
            (1, "Eng", "Ada", 100),
            (2, "Eng", "Di", 200),
            (3, "Sales", "Bob", 50),
        ] {
            session
                .execute_sql(&format!(
                    "INSERT INTO emp (id, dept, name, salary) VALUES ({id}, '{dept}', '{name}', {sal})"
                ))
                .unwrap();
        }

        let rows = session
            .execute_sql(
                "SELECT name, \
                 STRING_AGG(name, ',') OVER (PARTITION BY dept ORDER BY id) AS names, \
                 ARRAY_AGG(salary) OVER (PARTITION BY dept ORDER BY id) AS sals \
                 FROM emp",
            )
            .unwrap();
        let mut by = std::collections::BTreeMap::new();
        for r in &rows.rows {
            by.insert(
                r.get("name").unwrap_or("").to_string(),
                (
                    r.get("names").unwrap_or("").to_string(),
                    r.get("sals").unwrap_or("").to_string(),
                ),
            );
        }
        // No frame → full partition; Eng ordered Ada then Di
        assert_eq!(
            by.get("Ada").map(|(n, s)| (n.as_str(), s.as_str())),
            Some(("Ada,Di", "[100,200]"))
        );
        assert_eq!(
            by.get("Di").map(|(n, s)| (n.as_str(), s.as_str())),
            Some(("Ada,Di", "[100,200]"))
        );
        assert_eq!(
            by.get("Bob").map(|(n, s)| (n.as_str(), s.as_str())),
            Some(("Bob", "[50]"))
        );

        // Running STRING_AGG with ROWS frame
        let run = session
            .execute_sql(
                "SELECT name, STRING_AGG(name, '-') OVER (\
                   ORDER BY id \
                   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\
                 ) AS path FROM emp ORDER BY id",
            )
            .unwrap();
        assert_eq!(run.rows[0].get("path").unwrap_or(""), "Ada");
        assert_eq!(run.rows[1].get("path").unwrap_or(""), "Ada-Di");
        assert_eq!(run.rows[2].get("path").unwrap_or(""), "Ada-Di-Bob");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_bare_having_e2e() {
        let (mut session, root) = temp_session("bare-having");
        session
            .execute_sql(
                "CREATE TABLE emp (id BIGINT PRIMARY KEY, department TEXT, salary BIGINT)",
            )
            .unwrap();
        for (id, dept, sal) in [
            (1, "Eng", 100),
            (2, "Eng", 200),
            (3, "Sales", 50),
        ] {
            session
                .execute_sql(&format!(
                    "INSERT INTO emp (id, department, salary) VALUES ({id}, '{dept}', {sal})"
                ))
                .unwrap();
        }

        let ok = session
            .execute_sql("SELECT COUNT(*) FROM emp HAVING COUNT(*) > 2")
            .unwrap();
        assert_eq!(ok.rows.len(), 1);
        assert_eq!(ok.rows[0].get("count(*)"), Some("3"));

        let filtered_out = session
            .execute_sql("SELECT COUNT(*) FROM emp HAVING COUNT(*) > 10")
            .unwrap();
        assert!(filtered_out.rows.is_empty());

        let having_sum = session
            .execute_sql("SELECT COUNT(*) FROM emp HAVING SUM(salary) > 300")
            .unwrap();
        assert_eq!(having_sum.rows.len(), 1);
        assert_eq!(having_sum.rows[0].get("count(*)"), Some("3"));

        let having_sum_fail = session
            .execute_sql("SELECT COUNT(*) FROM emp HAVING SUM(salary) > 1000")
            .unwrap();
        assert!(having_sum_fail.rows.is_empty());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_percentile_e2e() {
        let (mut session, root) = temp_session("percentile-agg");
        session
            .execute_sql("CREATE TABLE nums (id BIGINT PRIMARY KEY, grp TEXT, x BIGINT)")
            .unwrap();
        for (id, grp, x) in [
            (1, "A", 10),
            (2, "A", 20),
            (3, "A", 30),
            (4, "A", 40),
            (5, "B", 1),
            (6, "B", 2),
            (7, "B", 3),
        ] {
            session
                .execute_sql(&format!(
                    "INSERT INTO nums (id, grp, x) VALUES ({id}, '{grp}', {x})"
                ))
                .unwrap();
        }

        let row = session
            .execute_sql(
                "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY x), \
                 percentile_disc(0.5) WITHIN GROUP (ORDER BY x) FROM nums WHERE grp = 'A'",
            )
            .unwrap();
        let cont = row.rows[0]
            .get("percentile_cont(0.5)")
            .unwrap()
            .parse::<f64>()
            .unwrap();
        let disc = row.rows[0]
            .get("percentile_disc(0.5)")
            .unwrap()
            .parse::<f64>()
            .unwrap();
        assert!((cont - 25.0).abs() < 1e-9, "cont={cont}");
        assert!((disc - 20.0).abs() < 1e-9, "disc={disc}");

        let grouped = session
            .execute_sql(
                "SELECT grp, percentile_cont(0.5) WITHIN GROUP (ORDER BY x) FROM nums GROUP BY grp",
            )
            .unwrap();
        let mut by = std::collections::BTreeMap::new();
        for r in &grouped.rows {
            by.insert(
                r.get("grp").unwrap_or("").to_string(),
                r.get("percentile_cont(0.5)")
                    .unwrap()
                    .parse::<f64>()
                    .unwrap(),
            );
        }
        assert!((by["A"] - 25.0).abs() < 1e-9);
        assert!((by["B"] - 2.0).abs() < 1e-9);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_row_to_json_e2e() {
        let (mut session, root) = temp_session("row-to-json");
        session
            .execute_sql("CREATE TABLE emp (id BIGINT PRIMARY KEY, name TEXT, dept TEXT)")
            .unwrap();
        session
            .execute_sql("INSERT INTO emp (id, name, dept) VALUES (1, 'Ada', 'Eng')")
            .unwrap();

        let whole = session
            .execute_sql("SELECT row_to_json(emp) FROM emp")
            .unwrap();
        let j = whole.rows[0].fields.values().next().unwrap();
        assert!(
            j.contains("\"id\":1") && j.contains("\"name\":\"Ada\"") && j.contains("\"dept\""),
            "whole={j}"
        );

        let ctor = session
            .execute_sql("SELECT row_to_json(ROW(id, name)) FROM emp")
            .unwrap();
        let c = ctor.rows[0].fields.values().next().unwrap();
        assert!(c.contains("\"id\":1") && c.contains("\"name\":\"Ada\""), "ctor={c}");
        assert!(!c.contains("dept"), "ROW() must not include dept: {c}");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_json_array_elements_each_e2e() {
        let (mut session, root) = temp_session("json-srf");

        let arr = session
            .execute_sql(r#"SELECT * FROM jsonb_array_elements('[1, 2, 3]'::jsonb)"#)
            .unwrap();
        assert_eq!(arr.rows.len(), 3);
        assert_eq!(arr.rows[0].get("value"), Some("1"));
        assert_eq!(arr.rows[2].get("value"), Some("3"));

        let text = session
            .execute_sql(
                r#"SELECT x FROM jsonb_array_elements_text('["Ada","Di"]'::jsonb) AS t(x)"#,
            )
            .unwrap();
        assert_eq!(text.rows.len(), 2);
        assert_eq!(text.rows[0].get("x"), Some("Ada"));
        assert_eq!(text.rows[1].get("x"), Some("Di"));

        let each = session
            .execute_sql(r#"SELECT * FROM json_each('{"a":1,"b":true}'::json)"#)
            .unwrap();
        assert_eq!(each.rows.len(), 2);
        let mut map = std::collections::BTreeMap::new();
        for r in &each.rows {
            map.insert(
                r.get("key").unwrap_or("").to_string(),
                r.get("value").unwrap_or("").to_string(),
            );
        }
        assert_eq!(map.get("a").map(String::as_str), Some("1"));
        assert_eq!(map.get("b").map(String::as_str), Some("true"));

        let ord = session
            .execute_sql(
                r#"SELECT x, i FROM jsonb_array_elements('[10,20,30]'::jsonb)
                   WITH ORDINALITY AS t(x, i)"#,
            )
            .unwrap();
        assert_eq!(ord.rows.len(), 3);
        assert_eq!(ord.rows[0].get("x"), Some("10"));
        assert_eq!(ord.rows[0].get("i"), Some("1"));
        assert_eq!(ord.rows[2].get("i"), Some("3"));

        let keys = session
            .execute_sql(
                r#"SELECT k, i FROM jsonb_object_keys('{"b":1,"a":2}'::jsonb)
                   WITH ORDINALITY AS t(k, i) ORDER BY k"#,
            )
            .unwrap();
        assert_eq!(keys.rows.len(), 2);
        // BTreeMap iteration order in materialize is key order of serde_json Map (insertion)
        // After ORDER BY k: a then b
        assert_eq!(keys.rows[0].get("k"), Some("a"));
        assert_eq!(keys.rows[1].get("k"), Some("b"));

        let each_ord = session
            .execute_sql(
                r#"SELECT k, v, i FROM jsonb_each('{"z":9,"a":1}'::jsonb)
                   WITH ORDINALITY AS e(k, v, i) ORDER BY k"#,
            )
            .unwrap();
        assert_eq!(each_ord.rows.len(), 2);
        assert_eq!(each_ord.rows[0].get("k"), Some("a"));
        assert_eq!(each_ord.rows[0].get("v"), Some("1"));
        assert_eq!(each_ord.rows[1].get("k"), Some("z"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_json_array_length_e2e() {
        let (mut session, root) = temp_session("json-array-length");

        let row = session
            .execute_sql(
                r#"SELECT json_array_length('[1,2,3]'::json) AS n,
                 jsonb_array_length('[]'::jsonb) AS z,
                 jsonb_array_length('[1,null,"x"]'::jsonb) AS m
                 FROM generate_series(1, 1)"#,
            )
            .unwrap();
        assert_eq!(row.rows[0].get("n"), Some("3"));
        assert_eq!(row.rows[0].get("z"), Some("0"));
        assert_eq!(row.rows[0].get("m"), Some("3"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_cross_join_lateral_json_srf_e2e() {
        let (mut session, root) = temp_session("lateral-json-srf");

        let rows = session
            .execute_sql(
                r#"SELECT n, x FROM generate_series(1, 2) AS g(n)
                   CROSS JOIN LATERAL jsonb_array_elements('[10,20]'::jsonb) AS t(x)
                   ORDER BY n, x"#,
            )
            .unwrap();
        assert_eq!(rows.rows.len(), 4);
        assert_eq!(rows.rows[0].get("n"), Some("1"));
        assert_eq!(rows.rows[0].get("x"), Some("10"));
        assert_eq!(rows.rows[3].get("n"), Some("2"));
        assert_eq!(rows.rows[3].get("x"), Some("20"));

        session
            .execute_sql("CREATE TABLE docs (id BIGINT PRIMARY KEY, tags TEXT)")
            .unwrap();
        session
            .execute_sql(
                "INSERT INTO docs (id, tags) VALUES (1, '[10,20]'), (2, '[30]')",
            )
            .unwrap();
        let corr = session
            .execute_sql(
                r#"SELECT id, x FROM docs
                   CROSS JOIN LATERAL jsonb_array_elements(tags::jsonb) AS t(x)
                   ORDER BY id, x"#,
            )
            .unwrap();
        assert_eq!(corr.rows.len(), 3);
        assert_eq!(corr.rows[0].get("id"), Some("1"));
        assert_eq!(corr.rows[0].get("x"), Some("10"));
        assert_eq!(corr.rows[1].get("id"), Some("1"));
        assert_eq!(corr.rows[1].get("x"), Some("20"));
        assert_eq!(corr.rows[2].get("id"), Some("2"));
        assert_eq!(corr.rows[2].get("x"), Some("30"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_cross_join_lateral_json_each_keys_e2e() {
        let (mut session, root) = temp_session("lateral-json-each");

        session
            .execute_sql("CREATE TABLE objs (id BIGINT PRIMARY KEY, j TEXT)")
            .unwrap();
        session
            .execute_sql(
                r#"INSERT INTO objs (id, j) VALUES (1, '{"a":1,"b":2}'), (2, '{"z":9}')"#,
            )
            .unwrap();

        let each = session
            .execute_sql(
                r#"SELECT id, k, v FROM objs
                   CROSS JOIN LATERAL jsonb_each(j::jsonb) AS e(k, v)
                   ORDER BY id, k"#,
            )
            .unwrap();
        assert_eq!(each.rows.len(), 3);
        assert_eq!(each.rows[0].get("id"), Some("1"));
        assert_eq!(each.rows[0].get("k"), Some("a"));
        assert_eq!(each.rows[0].get("v"), Some("1"));
        assert_eq!(each.rows[2].get("id"), Some("2"));
        assert_eq!(each.rows[2].get("k"), Some("z"));
        assert_eq!(each.rows[2].get("v"), Some("9"));

        let keys = session
            .execute_sql(
                r#"SELECT id, k FROM objs
                   CROSS JOIN LATERAL jsonb_object_keys(j::jsonb) AS t(k)
                   ORDER BY id, k"#,
            )
            .unwrap();
        assert_eq!(keys.rows.len(), 3);
        assert_eq!(keys.rows[0].get("k"), Some("a"));
        assert_eq!(keys.rows[1].get("k"), Some("b"));
        assert_eq!(keys.rows[2].get("k"), Some("z"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_cross_join_lateral_unnest_e2e() {
        let (mut session, root) = temp_session("lateral-unnest");

        let lit = session
            .execute_sql(
                r#"SELECT n, x FROM generate_series(1, 2) AS g(n)
                   CROSS JOIN LATERAL unnest(ARRAY[10, 20]) AS t(x)
                   ORDER BY n, x"#,
            )
            .unwrap();
        assert_eq!(lit.rows.len(), 4);
        assert_eq!(lit.rows[0].get("n"), Some("1"));
        assert_eq!(lit.rows[0].get("x"), Some("10"));

        session
            .execute_sql("CREATE TABLE arrs (id BIGINT PRIMARY KEY, tags TEXT)")
            .unwrap();
        session
            .execute_sql(
                "INSERT INTO arrs (id, tags) VALUES (1, '[10,20]'), (2, '[30]')",
            )
            .unwrap();
        let corr = session
            .execute_sql(
                r#"SELECT id, x FROM arrs
                   CROSS JOIN LATERAL unnest(tags) AS t(x)
                   ORDER BY id, x"#,
            )
            .unwrap();
        assert_eq!(corr.rows.len(), 3);
        assert_eq!(corr.rows[0].get("id"), Some("1"));
        assert_eq!(corr.rows[0].get("x"), Some("10"));
        assert_eq!(corr.rows[1].get("x"), Some("20"));
        assert_eq!(corr.rows[2].get("id"), Some("2"));
        assert_eq!(corr.rows[2].get("x"), Some("30"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_cross_join_lateral_regexp_srf_e2e() {
        let (mut session, root) = temp_session("lateral-regexp-srf");

        session
            .execute_sql("CREATE TABLE docs (id BIGINT PRIMARY KEY, body TEXT)")
            .unwrap();
        session
            .execute_sql(
                "INSERT INTO docs (id, body) VALUES (1, 'a-b-c'), (2, 'x|y')",
            )
            .unwrap();

        let split = session
            .execute_sql(
                r#"SELECT id, part FROM docs
                   CROSS JOIN LATERAL regexp_split_to_table(body, '[-|]') AS t(part)
                   ORDER BY id, part"#,
            )
            .unwrap();
        assert_eq!(split.rows.len(), 5);
        assert_eq!(split.rows[0].get("id"), Some("1"));
        assert_eq!(split.rows[0].get("part"), Some("a"));
        assert_eq!(split.rows[1].get("part"), Some("b"));
        assert_eq!(split.rows[2].get("part"), Some("c"));
        assert_eq!(split.rows[3].get("id"), Some("2"));
        assert_eq!(split.rows[3].get("part"), Some("x"));
        assert_eq!(split.rows[4].get("part"), Some("y"));

        let matches = session
            .execute_sql(
                r#"SELECT id, m FROM docs
                   CROSS JOIN LATERAL regexp_matches(body, '[a-z]', 'g') AS t(m)
                   WHERE id = 1
                   ORDER BY m"#,
            )
            .unwrap();
        assert_eq!(matches.rows.len(), 3);
        assert_eq!(matches.rows[0].get("m"), Some("[a]"));
        assert_eq!(matches.rows[1].get("m"), Some("[b]"));
        assert_eq!(matches.rows[2].get("m"), Some("[c]"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_is_json_e2e() {
        let (mut session, root) = temp_session("is-json");

        let row = session
            .execute_sql(
                r#"SELECT is_json('[]') AS ok,
                 json_is_valid('{') AS bad,
                 is_json(NULL) AS z
                 FROM generate_series(1, 1)"#,
            )
            .unwrap();
        assert_eq!(row.rows[0].get("ok"), Some("true"));
        assert_eq!(row.rows[0].get("bad"), Some("false"));
        // NULL → empty display / nullish
        let z = row.rows[0].get("z").unwrap_or("");
        assert!(z.is_empty() || z == "null", "z={z}");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_jsonb_path_exists_e2e() {
        let (mut session, root) = temp_session("jsonb-path-exists");

        let row = session
            .execute_sql(
                r#"SELECT jsonb_path_exists('{"a":{"b":1}}'::jsonb, '{a,b}') AS ok,
                 jsonb_path_exists('{"a":1}'::jsonb, '{z}') AS missing,
                 jsonb_path_exists('[10,20]'::jsonb, '{1}') AS idx
                 FROM generate_series(1, 1)"#,
            )
            .unwrap();
        assert_eq!(row.rows[0].get("ok"), Some("true"));
        assert_eq!(row.rows[0].get("missing"), Some("false"));
        assert_eq!(row.rows[0].get("idx"), Some("true"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_jsonb_extract_path_e2e() {
        let (mut session, root) = temp_session("jsonb-extract-path");

        let row = session
            .execute_sql(
                r#"SELECT jsonb_extract_path('{"a":{"b":9}}'::jsonb, 'a', 'b') AS j,
                 jsonb_extract_path_text('{"a":{"b":"x"}}'::jsonb, 'a', 'b') AS t,
                 jsonb_extract_path('[10,20]'::jsonb, '1') AS i
                 FROM generate_series(1, 1)"#,
            )
            .unwrap();
        assert_eq!(row.rows[0].get("j"), Some("9"));
        assert_eq!(row.rows[0].get("t"), Some("x"));
        assert_eq!(row.rows[0].get("i"), Some("20"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_jsonb_object_keys_e2e() {
        let (mut session, root) = temp_session("jsonb-object-keys");

        let rows = session
            .execute_sql(r#"SELECT * FROM jsonb_object_keys('{"z":1,"a":2}'::jsonb)"#)
            .unwrap();
        assert_eq!(rows.rows.len(), 2);
        let mut keys: Vec<_> = rows
            .rows
            .iter()
            .map(|r| r.get("jsonb_object_keys").unwrap_or("").to_string())
            .collect();
        keys.sort();
        assert_eq!(keys, vec!["a".to_string(), "z".to_string()]);

        let aliased = session
            .execute_sql(
                r#"SELECT k FROM json_object_keys('{"x":true}'::json) AS t(k)"#,
            )
            .unwrap();
        assert_eq!(aliased.rows.len(), 1);
        assert_eq!(aliased.rows[0].get("k"), Some("x"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_string_array_convert_e2e() {
        let (mut session, root) = temp_session("string-array-convert");

        let row = session
            .execute_sql(
                r#"SELECT string_to_array('a,b,c', ',') AS a,
                 array_to_string(ARRAY[1, 2, 3], '-') AS s,
                 array_to_string(string_to_array('x|y', '|'), ',') AS roundtrip
                 FROM generate_series(1, 1)"#,
            )
            .unwrap();
        assert_eq!(row.rows[0].get("a"), Some("[a,b,c]"));
        assert_eq!(row.rows[0].get("s"), Some("1-2-3"));
        assert_eq!(row.rows[0].get("roundtrip"), Some("x,y"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_split_part_e2e() {
        let (mut session, root) = temp_session("split-part");

        let row = session
            .execute_sql(
                r#"SELECT split_part('a.b.c', '.', 2) AS mid,
                 split_part('a.b.c', '.', 9) AS missing,
                 split_part('a.b.c', '.', -1) AS last
                 FROM generate_series(1, 1)"#,
            )
            .unwrap();
        assert_eq!(row.rows[0].get("mid"), Some("b"));
        assert_eq!(row.rows[0].get("missing"), Some(""));
        assert_eq!(row.rows[0].get("last"), Some("c"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_regexp_split_to_array_e2e() {
        let (mut session, root) = temp_session("regexp-split");

        let row = session
            .execute_sql(
                r#"SELECT regexp_split_to_array('hello world', '\s+') AS parts,
                 regexp_split_to_array('aXbXc', 'x', 'i') AS ci,
                 array_to_string(regexp_split_to_array('1-2-3', '-'), ',') AS joined
                 FROM generate_series(1, 1)"#,
            )
            .unwrap();
        assert_eq!(row.rows[0].get("parts"), Some("[hello,world]"));
        assert_eq!(row.rows[0].get("ci"), Some("[a,b,c]"));
        assert_eq!(row.rows[0].get("joined"), Some("1,2,3"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_regexp_split_to_table_e2e() {
        let (mut session, root) = temp_session("regexp-split-table");

        let rows = session
            .execute_sql(
                r#"SELECT regexp_split_to_table AS part
                   FROM regexp_split_to_table('hello world', '\s+')"#,
            )
            .unwrap();
        assert_eq!(rows.rows.len(), 2);
        assert_eq!(rows.rows[0].get("part"), Some("hello"));
        assert_eq!(rows.rows[1].get("part"), Some("world"));

        let aliased = session
            .execute_sql(
                r#"SELECT x FROM regexp_split_to_table('aXbXc', 'x', 'i') AS t(x)"#,
            )
            .unwrap();
        assert_eq!(aliased.rows.len(), 3);
        assert_eq!(aliased.rows[0].get("x"), Some("a"));
        assert_eq!(aliased.rows[2].get("x"), Some("c"));

        let ordinal = session
            .execute_sql(
                r#"SELECT part, ordinality
                   FROM regexp_split_to_table('hello world', '\s+') WITH ORDINALITY AS t(part, ordinality)"#,
            )
            .unwrap();
        assert_eq!(ordinal.rows.len(), 2);
        assert_eq!(ordinal.rows[0].get("part"), Some("hello"));
        assert_eq!(ordinal.rows[0].get("ordinality"), Some("1"));
        assert_eq!(ordinal.rows[1].get("part"), Some("world"));
        assert_eq!(ordinal.rows[1].get("ordinality"), Some("2"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_regexp_replace_e2e() {
        let (mut session, root) = temp_session("regexp-replace");

        let row = session
            .execute_sql(
                r#"SELECT regexp_replace('foobarbaz', 'b..', 'X') AS first,
                 regexp_replace('foobarbaz', 'b..', 'X', 'g') AS allm,
                 regexp_replace('AaA', 'a', 'z', 'gi') AS cig
                 FROM generate_series(1, 1)"#,
            )
            .unwrap();
        assert_eq!(row.rows[0].get("first"), Some("fooXbaz"));
        assert_eq!(row.rows[0].get("allm"), Some("fooXX"));
        assert_eq!(row.rows[0].get("cig"), Some("zzz"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_regexp_like_and_matches_e2e() {
        let (mut session, root) = temp_session("regexp-like-matches");

        let row = session
            .execute_sql(
                r#"SELECT regexp_like('hello', 'h.*o') AS ok,
                 regexp_like('hello', 'xyz') AS no
                 FROM generate_series(1, 1)"#,
            )
            .unwrap();
        assert_eq!(row.rows[0].get("ok"), Some("true"));
        assert_eq!(row.rows[0].get("no"), Some("false"));

        let matches = session
            .execute_sql(
                r#"SELECT regexp_matches AS m
                   FROM regexp_matches('foobarbaz', 'b(..)', 'g')"#,
            )
            .unwrap();
        assert_eq!(matches.rows.len(), 2);
        assert_eq!(matches.rows[0].get("m"), Some("[ar]"));
        assert_eq!(matches.rows[1].get("m"), Some("[az]"));

        let ordinal = session
            .execute_sql(
                r#"SELECT m, n
                   FROM regexp_matches('foobarbaz', 'b(..)', 'g') WITH ORDINALITY AS t(m, n)"#,
            )
            .unwrap();
        assert_eq!(ordinal.rows.len(), 2);
        assert_eq!(ordinal.rows[0].get("m"), Some("[ar]"));
        assert_eq!(ordinal.rows[0].get("n"), Some("1"));
        assert_eq!(ordinal.rows[1].get("m"), Some("[az]"));
        assert_eq!(ordinal.rows[1].get("n"), Some("2"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_lpad_rpad_repeat_e2e() {
        let (mut session, root) = temp_session("lpad-rpad-repeat");

        let row = session
            .execute_sql(
                r#"SELECT lpad('hi', 5, 'xy') AS lp,
                 rpad('hi', 5, '*') AS rp,
                 repeat('ab', 3) AS rp3,
                 lpad('hello', 3) AS trunc
                 FROM generate_series(1, 1)"#,
            )
            .unwrap();
        assert_eq!(row.rows[0].get("lp"), Some("xyxhi"));
        assert_eq!(row.rows[0].get("rp"), Some("hi***"));
        assert_eq!(row.rows[0].get("rp3"), Some("ababab"));
        assert_eq!(row.rows[0].get("trunc"), Some("hel"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_left_right_reverse_e2e() {
        let (mut session, root) = temp_session("left-right-reverse");

        let row = session
            .execute_sql(
                r#"SELECT left('abcde', 2) AS l,
                 right('abcde', 2) AS r,
                 reverse('abc') AS rev
                 FROM generate_series(1, 1)"#,
            )
            .unwrap();
        assert_eq!(row.rows[0].get("l"), Some("ab"));
        assert_eq!(row.rows[0].get("r"), Some("de"));
        assert_eq!(row.rows[0].get("rev"), Some("cba"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_initcap_ascii_chr_e2e() {
        let (mut session, root) = temp_session("initcap-ascii-chr");

        let row = session
            .execute_sql(
                r#"SELECT initcap('hello world') AS title,
                 ascii('A') AS code,
                 chr(65) AS ch
                 FROM generate_series(1, 1)"#,
            )
            .unwrap();
        assert_eq!(row.rows[0].get("title"), Some("Hello World"));
        assert_eq!(row.rows[0].get("code"), Some("65"));
        assert_eq!(row.rows[0].get("ch"), Some("A"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_md5_encode_decode_e2e() {
        let (mut session, root) = temp_session("md5-encode-decode");

        let row = session
            .execute_sql(
                r#"SELECT md5('abc') AS dig,
                 encode('hi', 'hex') AS hx,
                 decode('6869', 'hex') AS plain,
                 decode(encode('ok', 'base64'), 'base64') AS round
                 FROM generate_series(1, 1)"#,
            )
            .unwrap();
        assert_eq!(
            row.rows[0].get("dig"),
            Some("900150983cd24fb0d6963f7d28e17f72")
        );
        assert_eq!(row.rows[0].get("hx"), Some("6869"));
        assert_eq!(row.rows[0].get("plain"), Some("hi"));
        assert_eq!(row.rows[0].get("round"), Some("ok"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_starts_with_overlay_e2e() {
        let (mut session, root) = temp_session("starts-with-overlay");

        let row = session
            .execute_sql(
                r#"SELECT starts_with('hello', 'he') AS ok,
                 starts_with('hello', 'lo') AS no,
                 ends_with('hello', 'lo') AS eok,
                 ends_with('hello', 'he') AS eno,
                 overlay('Txxxxas' placing 'hom' from 2 for 4) AS name
                 FROM generate_series(1, 1)"#,
            )
            .unwrap();
        assert_eq!(row.rows[0].get("ok"), Some("true"));
        assert_eq!(row.rows[0].get("no"), Some("false"));
        assert_eq!(row.rows[0].get("eok"), Some("true"));
        assert_eq!(row.rows[0].get("eno"), Some("false"));
        assert_eq!(row.rows[0].get("name"), Some("Thomas"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_translate_btrim_e2e() {
        let (mut session, root) = temp_session("translate-btrim");

        let row = session
            .execute_sql(
                r#"SELECT translate('12345', '14', 'ax') AS tr,
                 btrim('xyxHelloxyx', 'xy') AS bt,
                 ltrim('  hi') AS lt,
                 rtrim('hi***', '*') AS rt
                 FROM generate_series(1, 1)"#,
            )
            .unwrap();
        assert_eq!(row.rows[0].get("tr"), Some("a23x5"));
        assert_eq!(row.rows[0].get("bt"), Some("Hello"));
        assert_eq!(row.rows[0].get("lt"), Some("hi"));
        assert_eq!(row.rows[0].get("rt"), Some("hi"));

        let sql_trim = session
            .execute_sql(
                r#"SELECT TRIM(BOTH 'x' FROM 'xhellox') AS both,
                 TRIM(LEADING 'xy' FROM 'xyhello') AS lead,
                 TRIM(TRAILING '*' FROM 'hi***') AS trail
                 FROM generate_series(1, 1)"#,
            )
            .unwrap();
        assert_eq!(sql_trim.rows[0].get("both"), Some("hello"));
        assert_eq!(sql_trim.rows[0].get("lead"), Some("hello"));
        assert_eq!(sql_trim.rows[0].get("trail"), Some("hi"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_concat_ws_format_e2e() {
        let (mut session, root) = temp_session("concat-ws-format");

        let row = session
            .execute_sql(
                r#"SELECT concat_ws('-', 'a', NULL, 'b') AS j,
                 format('Hello %s!', 'Ada') AS f,
                 format('%I', 'Foo') AS ident
                 FROM generate_series(1, 1)"#,
            )
            .unwrap();
        assert_eq!(row.rows[0].get("j"), Some("a-b"));
        assert_eq!(row.rows[0].get("f"), Some("Hello Ada!"));
        assert_eq!(row.rows[0].get("ident"), Some("\"Foo\""));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_quote_ident_literal_e2e() {
        let (mut session, root) = temp_session("quote-ident-literal");

        let row = session
            .execute_sql(
                r#"SELECT quote_ident('Foo') AS qi,
                 quote_literal('a''b') AS ql,
                 quote_ident('x"y') AS qiq
                 FROM generate_series(1, 1)"#,
            )
            .unwrap();
        assert_eq!(row.rows[0].get("qi"), Some("\"Foo\""));
        assert_eq!(row.rows[0].get("ql"), Some("'a''b'"));
        assert_eq!(row.rows[0].get("qiq"), Some("\"x\"\"y\""));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_quote_nullable_width_bucket_e2e() {
        let (mut session, root) = temp_session("quote-nullable-width-bucket");

        let row = session
            .execute_sql(
                r#"SELECT quote_nullable(NULL) AS qn,
                 quote_nullable('hi') AS ql,
                 width_bucket(5.35, 0.024, 10.06, 5) AS wb
                 FROM generate_series(1, 1)"#,
            )
            .unwrap();
        assert_eq!(row.rows[0].get("qn"), Some("NULL"));
        assert_eq!(row.rows[0].get("ql"), Some("'hi'"));
        assert_eq!(row.rows[0].get("wb"), Some("3"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_sign_trunc_div_e2e() {
        let (mut session, root) = temp_session("sign-trunc-div");

        let row = session
            .execute_sql(
                r#"SELECT sign(-3) AS s,
                 trunc(42.8) AS t,
                 trunc(42.89, 1) AS td,
                 div(9, 4) AS d
                 FROM generate_series(1, 1)"#,
            )
            .unwrap();
        assert_eq!(row.rows[0].get("s"), Some("-1"));
        assert_eq!(row.rows[0].get("t"), Some("42"));
        assert_eq!(row.rows[0].get("td"), Some("42.8"));
        assert_eq!(row.rows[0].get("d"), Some("2"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_pi_sqrt_cbrt_log_e2e() {
        let (mut session, root) = temp_session("pi-sqrt-cbrt-log");

        let row = session
            .execute_sql(
                r#"SELECT sqrt(9) AS s,
                 cbrt(8) AS c,
                 ln(1) AS n,
                 log(100) AS l10,
                 log(2, 8) AS l2,
                 trunc(pi(), 2) AS p
                 FROM generate_series(1, 1)"#,
            )
            .unwrap();
        assert_eq!(row.rows[0].get("s"), Some("3"));
        assert_eq!(row.rows[0].get("c"), Some("2"));
        assert_eq!(row.rows[0].get("n"), Some("0"));
        assert_eq!(row.rows[0].get("l10"), Some("2"));
        assert_eq!(row.rows[0].get("l2"), Some("3"));
        assert_eq!(row.rows[0].get("p"), Some("3.14"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_trig_radians_degrees_e2e() {
        let (mut session, root) = temp_session("trig-radians-degrees");

        let row = session
            .execute_sql(
                r#"SELECT trunc(sin(radians(90)), 6) AS s,
                 trunc(cos(0), 6) AS c,
                 trunc(degrees(pi()/2), 6) AS d,
                 trunc(atan2(1, 1), 6) AS a
                 FROM generate_series(1, 1)"#,
            )
            .unwrap();
        assert_eq!(row.rows[0].get("s"), Some("1"));
        assert_eq!(row.rows[0].get("c"), Some("1"));
        assert_eq!(row.rows[0].get("d"), Some("90"));
        // π/4 ≈ 0.785398
        let a = row.rows[0].get("a").unwrap();
        assert!(a.starts_with("0.7853"), "atan2≈π/4 got {a}");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_set_show_search_path_and_isolation() {
        let (mut session, root) = temp_session("set-show-guc");
        assert_eq!(session.search_path(), "public");
        assert_eq!(session.transaction_isolation(), "repeatable read");

        let set = session.execute_sql("SET search_path TO myschema").unwrap();
        assert_eq!(set.tag, "SET");
        assert_eq!(session.search_path(), "myschema");

        let show = session.execute_sql("SHOW search_path").unwrap();
        assert_eq!(show.tag, "SELECT");
        assert_eq!(show.rows[0].get("search_path"), Some("myschema"));

        session
            .execute_sql("SET transaction_isolation TO 'read committed'")
            .unwrap();
        assert_eq!(session.transaction_isolation(), "read committed");
        let show_iso = session.execute_sql("SHOW transaction_isolation").unwrap();
        assert_eq!(
            show_iso.rows[0].get("transaction_isolation"),
            Some("read committed")
        );

        session
            .execute_sql("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            .unwrap();
        assert_eq!(session.transaction_isolation(), "repeatable read");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_accepts_serializable_rejects_read_uncommitted() {
        let (mut session, root) = temp_session("iso-boundary");
        session
            .execute_sql("SET transaction_isolation TO 'serializable'")
            .unwrap();
        assert_eq!(session.transaction_isolation(), "serializable");
        let show = session.execute_sql("SHOW transaction_isolation").unwrap();
        assert_eq!(
            show.rows[0].get("transaction_isolation"),
            Some("serializable")
        );

        let err = session
            .execute_sql("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
            .expect_err("dirty reads must be rejected");
        assert!(
            err.to_string().contains("read uncommitted"),
            "unexpected: {err}"
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_ssi_write_skew_aborts_second_committer() {
        let (mut session, root) = temp_session("ssi-skew");
        session
            .execute_sql(
                "CREATE TABLE accounts (id TEXT PRIMARY KEY, balance INT NOT NULL)",
            )
            .unwrap();
        session
            .execute_sql(
                "INSERT INTO accounts (id, balance) VALUES ('A', '100'), ('B', '100')",
            )
            .unwrap();
        session
            .execute_sql("SET transaction_isolation TO 'serializable'")
            .unwrap();

        // T1: read A, write B (classic write-skew setup).
        session.execute_sql("BEGIN").unwrap();
        let a = session
            .execute_sql("SELECT balance FROM accounts WHERE id = 'A'")
            .unwrap();
        assert_eq!(a.rows[0].get("balance"), Some("100"));
        session
            .execute_sql("UPDATE accounts SET balance = '90' WHERE id = 'B'")
            .unwrap();

        // T2 on a second session against the same engine.
        let mut s2 = SessionState::new(Arc::clone(session.engine()));
        s2.execute_sql("SET transaction_isolation TO 'serializable'")
            .unwrap();
        s2.execute_sql("BEGIN").unwrap();
        let b = s2
            .execute_sql("SELECT balance FROM accounts WHERE id = 'B'")
            .unwrap();
        assert_eq!(b.rows[0].get("balance"), Some("100"));
        s2.execute_sql("UPDATE accounts SET balance = '90' WHERE id = 'A'")
            .unwrap();

        // First committer wins; second is doomed by SSI rw-antidependency.
        session.execute_sql("COMMIT").unwrap();
        let err = s2.execute_sql("COMMIT").expect_err("write skew must abort");
        assert!(
            err.to_string().contains("SSI") || err.to_string().contains("conflict"),
            "unexpected: {err}"
        );

        // Survivor: A unchanged (100), B=90.
        let rows = session
            .execute_sql("SELECT id, balance FROM accounts ORDER BY id")
            .unwrap();
        let mut map = std::collections::BTreeMap::new();
        for r in &rows.rows {
            map.insert(
                r.get("id").unwrap_or("").to_string(),
                r.get("balance").unwrap_or("").to_string(),
            );
        }
        assert_eq!(map.get("A").map(String::as_str), Some("100"));
        assert_eq!(map.get("B").map(String::as_str), Some("90"));

        drop(session);
        drop(s2);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_repeatable_read_write_skew_aborts_via_occ_not_ssi() {
        // Same A/B pattern under SI+OCC: second commit fails on read-set OCC,
        // not the SSI doom path (RR must not register SSI).
        let (mut session, root) = temp_session("rr-skew");
        session
            .execute_sql(
                "CREATE TABLE accounts (id TEXT PRIMARY KEY, balance INT NOT NULL)",
            )
            .unwrap();
        session
            .execute_sql(
                "INSERT INTO accounts (id, balance) VALUES ('A', '100'), ('B', '100')",
            )
            .unwrap();
        assert_eq!(session.transaction_isolation(), "repeatable read");

        session.execute_sql("BEGIN").unwrap();
        let _ = session
            .execute_sql("SELECT balance FROM accounts WHERE id = 'A'")
            .unwrap();
        session
            .execute_sql("UPDATE accounts SET balance = '90' WHERE id = 'B'")
            .unwrap();

        let mut s2 = SessionState::new(Arc::clone(session.engine()));
        s2.execute_sql("BEGIN").unwrap();
        let _ = s2
            .execute_sql("SELECT balance FROM accounts WHERE id = 'B'")
            .unwrap();
        s2.execute_sql("UPDATE accounts SET balance = '90' WHERE id = 'A'")
            .unwrap();

        session.execute_sql("COMMIT").unwrap();
        let err = s2.execute_sql("COMMIT").expect_err("OCC must abort");
        let msg = err.to_string();
        assert!(
            msg.contains("committed at") || msg.contains("Conflict"),
            "expected OCC conflict, got: {msg}"
        );
        assert!(
            !msg.contains("SSI"),
            "RR path must not report SSI doom: {msg}"
        );

        drop(session);
        drop(s2);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_correlated_exists_uses_apply() {
        let (mut session, root) = temp_session("apply-exists");
        session
            .execute_sql("CREATE TABLE dept_budget (dept TEXT PRIMARY KEY, budget INT)")
            .unwrap();
        session
            .execute_sql(
                "INSERT INTO users (id, name, age) VALUES \
                 (1, 'Ada', 30), (2, 'Bob', 20)",
            )
            .unwrap();
        // Reuse users.name as a stand-in "dept" correlation key via a tiny budget table.
        session
            .execute_sql("INSERT INTO dept_budget (dept, budget) VALUES ('Ada', 1)")
            .unwrap();

        let explain = session
            .execute_sql(
                "EXPLAIN SELECT id FROM users u WHERE EXISTS (
                    SELECT 1 FROM dept_budget d WHERE d.dept = u.name
                )",
            )
            .unwrap();
        let plan_text = explain.rows[0].get("QUERY PLAN").unwrap_or("");
        assert!(
            plan_text.contains("HashSemiJoin"),
            "expected HashSemiJoin in EXPLAIN, got:\n{plan_text}"
        );

        let rows = session
            .execute_sql(
                "SELECT id FROM users u WHERE EXISTS (
                    SELECT 1 FROM dept_budget d WHERE d.dept = u.name
                )",
            )
            .unwrap();
        assert_eq!(rows.rows.len(), 1);
        assert_eq!(rows.rows[0].get("id"), Some("1"));

        let _ = fs::remove_dir_all(root);
    }

    /// Session `COMMIT` → partition → TC → EngineShard prepare (in-process).
    #[test]
    fn session_multi_shard_commit_uses_2pc() {
        use crate::dtxn::EngineShard;
        use crate::partition::{PartitionMap, PartitioningStrategy};
        use crate::schema::data_key;

        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("takyonic-pg-session-2pc-{nanos}"));
        let mk = |name: &str| {
            let cfg = Config::default()
                .data_dir(root.join(name).join("data"))
                .wal_dir(root.join(name).join("wal"))
                .memtable_size_bytes(64 * 1024 * 1024)
                .l0_rapid_pool_threads(1)
                .ln_haul_pool_threads(1)
                .compaction_write_bytes_per_sec(1024 * 1024 * 1024)
                .write_admission_ops_per_sec(100_000)
                .write_admission_min_ops_per_sec(1_000)
                .write_admission_burst(10_000);
            Arc::new(TakyonicEngine::open(cfg).unwrap())
        };
        let e1 = mk("s1");
        let e2 = mk("s2");
        e1.set_shard_id(1);
        e2.set_shard_id(2);
        e1.set_mpp_workers(vec![
            crate::mpp::WorkerEndpoint {
                node_id: 1,
                address: "local-0".into(),
                slot: 0,
            },
            crate::mpp::WorkerEndpoint {
                node_id: 2,
                address: "local-1".into(),
                slot: 1,
            },
        ]);

        let schema = TableSchema::new("accounts", "id", vec![])
            .with_partitioning(PartitioningStrategy::Hash {
                column: "id".into(),
                bucket_count: 2,
            })
            .with_partition_map(PartitionMap::round_robin(&[1, 2], 2));
        e1.register_table(schema.clone()).unwrap();
        e2.register_table(schema).unwrap();

        // Pick two PKs that hash to different partitions / nodes.
        let router = PartitionRouter::new(e1.mpp_workers());
        let sch = e1.table_schema("accounts").unwrap();
        let mut pk_a = None;
        let mut pk_b = None;
        for i in 1..200 {
            let (_, node) = router.route_key(&sch, &i.to_string()).unwrap();
            if node == 1 && pk_a.is_none() {
                pk_a = Some(i);
            } else if node == 2 && pk_b.is_none() {
                pk_b = Some(i);
            }
            if pk_a.is_some() && pk_b.is_some() {
                break;
            }
        }
        let a = pk_a.expect("pk on shard 1");
        let b = pk_b.expect("pk on shard 2");

        let mut session = SessionState::new(Arc::clone(&e1));
        session.attach_dist_shards([
            (1, EngineShard::new(Arc::clone(&e1), 1) as Arc<dyn ShardParticipant>),
            (2, EngineShard::new(Arc::clone(&e2), 2) as Arc<dyn ShardParticipant>),
        ]);

        session.execute_sql("BEGIN").unwrap();
        session
            .execute_sql(&format!(
                "INSERT INTO accounts (id, bal) VALUES ({a}, 100), ({b}, 200)"
            ))
            .unwrap();
        session.execute_sql("COMMIT").unwrap();

        let va = e1
            .get(&data_key("accounts", &a.to_string()))
            .unwrap()
            .expect("shard1 should hold pk A after 2PC");
        let vb = e2
            .get(&data_key("accounts", &b.to_string()))
            .unwrap()
            .expect("shard2 should hold pk B after 2PC");
        assert!(!va.as_bytes().is_empty());
        assert!(!vb.as_bytes().is_empty());
        // Owning shard only: A must not land solely on the wrong engine as a
        // local txn_batch of both keys.
        let a_on_2 = e2.get(&data_key("accounts", &a.to_string())).unwrap();
        let b_on_1 = e1.get(&data_key("accounts", &b.to_string())).unwrap();
        assert!(a_on_2.is_none(), "pk A should not live on shard 2");
        assert!(b_on_1.is_none(), "pk B should not live on shard 1");

        e1.close().unwrap();
        e2.close().unwrap();
        let _ = fs::remove_dir_all(root);
    }
}
