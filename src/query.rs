//! Cost-based query planner and execution over secondary indexes.
//!
//! ```ignore
//! engine.query("users")
//!     .filter("status", FilterOp::Eq, "active")
//!     .filter("city", FilterOp::Eq, "X")
//!     .explain()  // shows chosen index + costs
//!     .execute()?;
//! ```

use std::fmt::Write as _;

use crate::engine::TakyonicEngine;
use crate::error::{Result, TakyonicError};
use crate::schema::{
    Record, TableSchema, data_key, decode_sortable_int, encode_sortable_int, index_column_prefix,
    index_eq_prefix, parse_index_suffix, pk_from_index_key,
};
use crate::stats::TableStats;
use crate::types::CommitTs;

/// Comparison operator in a filter predicate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FilterOp {
    /// Equality.
    Eq,
    /// Not equal (residual only — not index-driving).
    Ne,
    /// Greater than.
    Gt,
    /// Greater or equal.
    Gte,
    /// Less than.
    Lt,
    /// Less or equal.
    Lte,
}

impl FilterOp {
    /// Parse from a string like `"=="`, `">"`, `"<"`.
    pub fn parse(s: &str) -> Result<Self> {
        match s {
            "==" | "=" | "eq" => Ok(Self::Eq),
            "!=" | "<>" | "ne" => Ok(Self::Ne),
            ">" | "gt" => Ok(Self::Gt),
            ">=" | "gte" => Ok(Self::Gte),
            "<" | "lt" => Ok(Self::Lt),
            "<=" | "lte" => Ok(Self::Lte),
            other => Err(TakyonicError::Engine(format!("unknown filter op {other}"))),
        }
    }

    /// Whether this op can drive an index range/point scan.
    pub fn indexable(self) -> bool {
        !matches!(self, Self::Ne)
    }
}

/// One filter clause: `column op value`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Filter {
    /// Record field name.
    pub column: String,
    /// Comparison.
    pub op: FilterOp,
    /// Literal value (string form; numeric columns use sortable encoding in indexes).
    pub value: String,
}

/// Candidate access path evaluated by the CBO.
#[derive(Clone, Debug)]
pub struct IndexCandidate {
    /// Index name.
    pub index: String,
    /// Column the index covers.
    pub column: String,
    /// Estimated rows the index scan would return.
    pub estimated_rows: u64,
    /// Selectivity used in the estimate.
    pub selectivity: f64,
    /// Filter this candidate would apply as the driving predicate.
    pub filter: Filter,
}

/// Chosen physical plan.
#[derive(Clone, Debug)]
pub struct ExecutionPlan {
    /// Table name.
    pub table: String,
    /// Driving index (None ⇒ full table scan of data keys).
    pub driving_index: Option<String>,
    /// Driving filter applied via the index.
    pub driving_filter: Option<Filter>,
    /// Remaining filters evaluated after PK fetch.
    pub residual_filters: Vec<Filter>,
    /// All candidates considered (for EXPLAIN).
    pub candidates: Vec<IndexCandidate>,
    /// Estimated rows from the driving path.
    pub estimated_rows: u64,
}

impl ExecutionPlan {
    /// Human-readable EXPLAIN text.
    pub fn explain(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(out, "EXPLAIN for table `{}`", self.table);
        let _ = writeln!(out, "  candidates:");
        for c in &self.candidates {
            let _ = writeln!(
                out,
                "    - index=`{}` column=`{}` filter=`{} {:?} \"{}\"` selectivity={:.6} est_rows={}",
                c.index,
                c.column,
                c.filter.column,
                c.filter.op,
                c.filter.value,
                c.selectivity,
                c.estimated_rows
            );
        }
        match &self.driving_index {
            Some(idx) => {
                let _ = writeln!(
                    out,
                    "  chosen: IndexScan({}) est_rows={}",
                    idx, self.estimated_rows
                );
            }
            None => {
                let _ = writeln!(out, "  chosen: TableScan est_rows={}", self.estimated_rows);
            }
        }
        if let Some(f) = &self.driving_filter {
            let _ = writeln!(out, "  drive: {} {:?} \"{}\"", f.column, f.op, f.value);
        }
        if !self.residual_filters.is_empty() {
            let _ = writeln!(out, "  residual filters:");
            for f in &self.residual_filters {
                let _ = writeln!(out, "    - {} {:?} \"{}\"", f.column, f.op, f.value);
            }
        }
        out
    }
}

/// Fluent query builder bound to an engine + table.
pub struct Query<'a> {
    engine: &'a TakyonicEngine,
    table: String,
    filters: Vec<Filter>,
    plan: Option<ExecutionPlan>,
}

impl<'a> Query<'a> {
    pub(crate) fn new(engine: &'a TakyonicEngine, table: impl Into<String>) -> Self {
        Self {
            engine,
            table: table.into(),
            filters: Vec::new(),
            plan: None,
        }
    }

    /// Add a filter (`column`, op string, value).
    pub fn filter(
        mut self,
        column: impl Into<String>,
        op: &str,
        value: impl Into<String>,
    ) -> Result<Self> {
        self.filters.push(Filter {
            column: column.into(),
            op: FilterOp::parse(op)?,
            value: value.into(),
        });
        self.plan = None;
        Ok(self)
    }

    /// Add a typed filter.
    pub fn filter_op(
        mut self,
        column: impl Into<String>,
        op: FilterOp,
        value: impl Into<String>,
    ) -> Self {
        self.filters.push(Filter {
            column: column.into(),
            op,
            value: value.into(),
        });
        self.plan = None;
        self
    }

    /// Run the cost-based optimizer and cache the plan.
    pub fn plan(&mut self) -> Result<&ExecutionPlan> {
        if self.plan.is_none() {
            let schema = self.engine.table_schema(&self.table)?.clone();
            let stats = self.engine.table_stats(&self.table);
            self.plan = Some(optimize(&schema, &stats, &self.filters));
        }
        Ok(self.plan.as_ref().expect("plan just set"))
    }

    /// EXPLAIN text for the chosen plan (plans first if needed).
    pub fn explain(&mut self) -> Result<String> {
        Ok(self.plan()?.explain())
    }

    /// Execute the plan and return matching records.
    pub fn execute(mut self) -> Result<Vec<Record>> {
        let plan = self.plan()?.clone();
        execute_plan(self.engine, &plan)
    }
}

/// Pick the lowest-cost indexable filter as the driving access path.
pub fn optimize(schema: &TableSchema, stats: &TableStats, filters: &[Filter]) -> ExecutionPlan {
    let mut candidates = Vec::new();
    for filter in filters {
        if !filter.op.indexable() {
            continue;
        }
        let Some(index) = schema.indexes.iter().find(|i| i.column == filter.column) else {
            continue;
        };
        let (estimated_rows, selectivity) = match filter.op {
            FilterOp::Eq => (
                stats.eq_cost(&index.name),
                stats.eq_selectivity(&index.name),
            ),
            // Range: rough half-table unless we have histograms; still prefer
            // indexed path over full scan when NDV is high.
            FilterOp::Gt | FilterOp::Gte | FilterOp::Lt | FilterOp::Lte => {
                let sel = 0.5;
                ((((stats.row_count as f64) * sel).ceil() as u64).max(1), sel)
            }
            FilterOp::Ne => continue,
        };
        candidates.push(IndexCandidate {
            index: index.name.clone(),
            column: index.column.clone(),
            estimated_rows,
            selectivity,
            filter: filter.clone(),
        });
    }

    candidates.sort_by_key(|c| c.estimated_rows);

    let (driving_index, driving_filter, estimated_rows) = if let Some(best) = candidates.first() {
        (
            Some(best.index.clone()),
            Some(best.filter.clone()),
            best.estimated_rows,
        )
    } else {
        (None, None, stats.row_count.max(1))
    };

    let residual_filters: Vec<Filter> = filters
        .iter()
        .filter(|f| driving_filter.as_ref() != Some(*f))
        .cloned()
        .collect();

    ExecutionPlan {
        table: schema.name.clone(),
        driving_index,
        driving_filter,
        residual_filters,
        candidates,
        estimated_rows,
    }
}

fn execute_plan(engine: &TakyonicEngine, plan: &ExecutionPlan) -> Result<Vec<Record>> {
    let schema = engine.table_schema(&plan.table)?;
    let read_ts = engine.last_applied();

    let pks: Vec<String> = match (&plan.driving_index, &plan.driving_filter) {
        (Some(index), Some(filter)) => {
            collect_pks_from_index(engine, &schema.name, index, filter, read_ts)?
        }
        _ => collect_all_pks(engine, &schema.name, read_ts)?,
    };

    let mut out = Vec::new();
    for pk in pks {
        let key = data_key(&schema.name, &pk);
        let Some(val) = engine.get_at_with_ts(&key, read_ts)?.0 else {
            continue;
        };
        let record = Record::decode(&val)?;
        if plan
            .residual_filters
            .iter()
            .all(|f| matches_filter(&record, f))
        {
            // Also verify driving filter in case of stale index (should match).
            if let Some(df) = &plan.driving_filter
                && !matches_filter(&record, df)
            {
                continue;
            }
            out.push(record);
        }
    }
    Ok(out)
}

fn index_probe_value(raw: &str) -> String {
    if let Ok(n) = raw.parse::<i64>() {
        encode_sortable_int(n)
    } else {
        raw.to_string()
    }
}

fn collect_pks_from_index(
    engine: &TakyonicEngine,
    table: &str,
    index: &str,
    filter: &Filter,
    read_ts: CommitTs,
) -> Result<Vec<String>> {
    match filter.op {
        FilterOp::Eq => {
            let encoded = index_probe_value(&filter.value);
            let prefix = index_eq_prefix(table, index, &encoded);
            let keys = engine.scan_prefix_keys(&prefix, read_ts)?;
            Ok(keys
                .into_iter()
                .filter_map(|k| pk_from_index_key(&k, &prefix))
                .collect())
        }
        FilterOp::Gt | FilterOp::Gte | FilterOp::Lt | FilterOp::Lte => {
            let col_prefix = index_column_prefix(table, index);
            let keys = engine.scan_prefix_keys(&col_prefix, read_ts)?;
            let mut pks = Vec::new();
            let bound = index_probe_value(&filter.value);
            for key in keys {
                let Some((value, pk)) = parse_index_suffix(&key, &col_prefix) else {
                    continue;
                };
                if range_match_encoded(&value, filter.op, &bound, &filter.value) {
                    pks.push(pk);
                }
            }
            Ok(pks)
        }
        FilterOp::Ne => Ok(Vec::new()),
    }
}

fn collect_all_pks(engine: &TakyonicEngine, table: &str, read_ts: CommitTs) -> Result<Vec<String>> {
    let prefix = {
        let mut p = Vec::from(b"Data_".as_slice());
        p.extend_from_slice(table.as_bytes());
        p.push(b'_');
        bytes::Bytes::from(p)
    };
    let keys = engine.scan_prefix_keys(&prefix, read_ts)?;
    Ok(keys
        .into_iter()
        .filter_map(|k| {
            let bytes = k.as_bytes();
            if bytes.len() <= prefix.len() {
                return None;
            }
            String::from_utf8(bytes[prefix.len()..].to_vec()).ok()
        })
        .collect())
}

fn range_match_encoded(
    index_value: &str,
    op: FilterOp,
    encoded_bound: &str,
    raw_bound: &str,
) -> bool {
    if let (Some(iv), Ok(fv)) = (
        decode_sortable_int(index_value).or_else(|| index_value.parse().ok()),
        raw_bound.parse::<i64>(),
    ) {
        return match op {
            FilterOp::Gt => iv > fv,
            FilterOp::Gte => iv >= fv,
            FilterOp::Lt => iv < fv,
            FilterOp::Lte => iv <= fv,
            FilterOp::Eq => iv == fv,
            FilterOp::Ne => iv != fv,
        };
    }
    match op {
        FilterOp::Gt => index_value > encoded_bound,
        FilterOp::Gte => index_value >= encoded_bound,
        FilterOp::Lt => index_value < encoded_bound,
        FilterOp::Lte => index_value <= encoded_bound,
        FilterOp::Eq => index_value == encoded_bound,
        FilterOp::Ne => index_value != encoded_bound,
    }
}

fn matches_filter(record: &Record, filter: &Filter) -> bool {
    let Some(actual) = record.get(&filter.column) else {
        return false;
    };
    // Numeric-aware when possible.
    if let (Ok(a), Ok(b)) = (actual.parse::<i64>(), filter.value.parse::<i64>()) {
        return match filter.op {
            FilterOp::Eq => a == b,
            FilterOp::Ne => a != b,
            FilterOp::Gt => a > b,
            FilterOp::Gte => a >= b,
            FilterOp::Lt => a < b,
            FilterOp::Lte => a <= b,
        };
    }
    match filter.op {
        FilterOp::Eq => actual == filter.value,
        FilterOp::Ne => actual != filter.value,
        FilterOp::Gt => actual > filter.value.as_str(),
        FilterOp::Gte => actual >= filter.value.as_str(),
        FilterOp::Lt => actual < filter.value.as_str(),
        FilterOp::Lte => actual <= filter.value.as_str(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::IndexDef;
    use crate::stats::TableStats;
    use std::collections::BTreeMap;

    #[test]
    fn cbo_picks_high_ndv_index() {
        let schema = TableSchema::new(
            "users",
            "id",
            vec![
                IndexDef::new("status", "status"),
                IndexDef::new("city", "city"),
            ],
        );
        let mut distinct = BTreeMap::new();
        distinct.insert("status".into(), 2);
        distinct.insert("city".into(), 200);
        let stats = TableStats {
            row_count: 10_000,
            distinct,
        };
        let filters = vec![
            Filter {
                column: "status".into(),
                op: FilterOp::Eq,
                value: "active".into(),
            },
            Filter {
                column: "city".into(),
                op: FilterOp::Eq,
                value: "X".into(),
            },
        ];
        let plan = optimize(&schema, &stats, &filters);
        assert_eq!(plan.driving_index.as_deref(), Some("city"));
        assert!(plan.explain().contains("chosen: IndexScan(city)"));
    }
}
