//! Stub `pg_catalog` / `information_schema` responses for psql introspection.
//!
//! Takyonic's SQL planner does not execute joins / CASE / catalog functions, so
//! common introspection queries (notably psql `\dt` / `\d`) are recognized by
//! shape and answered from the in-memory table catalog.

use crate::pg::SessionResult;
use crate::schema::{Record, TableSchema};

/// Try to answer a catalog / information_schema introspection query.
///
/// Returns [`None`] when `sql` is ordinary user SQL (caller should plan normally).
pub fn try_handle(
    sql: &str,
    tables: &[TableSchema],
    owner: &str,
) -> Option<SessionResult> {
    let n = normalize(sql);
    if looks_like_psql_list_tables(&n) {
        return Some(list_relations_dt(tables, owner));
    }
    if looks_like_psql_describe_table(&n) {
        return Some(describe_table(tables, &n));
    }
    if n.contains("information_schema.columns") {
        return Some(info_schema_columns(tables, &n));
    }
    if n.contains("information_schema.tables") {
        return Some(info_schema_tables(tables));
    }
    if n.contains("pg_catalog.pg_tables")
        || n.contains(" from pg_tables ")
        || n.ends_with(" from pg_tables")
    {
        return Some(pg_tables(tables, owner));
    }
    None
}

fn normalize(sql: &str) -> String {
    sql.trim()
        .trim_end_matches(';')
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase()
}

/// psql `\dt` / `listTables` shape: `pg_class` + `relkind` + namespace join.
fn looks_like_psql_list_tables(n: &str) -> bool {
    n.contains("pg_catalog.pg_class")
        && n.contains("relkind")
        && (n.contains("nspname") || n.contains("\"schema\""))
        && !n.contains("pg_catalog.pg_attribute")
        && !n.contains("pg_attribute")
}

/// psql `\d table` / column describe: `pg_attribute` (+ usually `pg_class`).
fn looks_like_psql_describe_table(n: &str) -> bool {
    (n.contains("pg_catalog.pg_attribute") || n.contains(" from pg_attribute"))
        && (n.contains("attname") || n.contains("\"column\""))
}

fn describe_table(tables: &[TableSchema], normalized: &str) -> SessionResult {
    let filter = extract_relname_filter(normalized)
        .or_else(|| extract_quoted_filter(normalized, "table_name"));
    let columns = vec![
        "Column".into(),
        "Type".into(),
        "Collation".into(),
        "Nullable".into(),
        "Default".into(),
    ];
    let mut rows = Vec::new();
    for table in tables {
        if let Some(want) = filter.as_deref() {
            if !table.name.eq_ignore_ascii_case(want) {
                continue;
            }
        }
        for col in effective_columns(table) {
            let nullable = if col.name == table.primary_key {
                "not null"
            } else {
                ""
            };
            rows.push(
                Record::new()
                    .set("Column", col.name.as_str())
                    .set("Type", col.data_type.as_str())
                    .set("Collation", "")
                    .set("Nullable", nullable)
                    .set("Default", ""),
            );
        }
    }
    SessionResult {
        tag: "SELECT",
        rows,
        affected: None,
        column_order: Some(columns),
    }
}

fn list_relations_dt(tables: &[TableSchema], owner: &str) -> SessionResult {
    let columns = vec![
        "Schema".into(),
        "Name".into(),
        "Type".into(),
        "Owner".into(),
    ];
    let mut rows: Vec<Record> = tables
        .iter()
        .map(|t| {
            Record::new()
                .set("Schema", "public")
                .set("Name", t.name.as_str())
                .set("Type", "table")
                .set("Owner", owner)
        })
        .collect();
    rows.sort_by(|a, b| a.get("Name").cmp(&b.get("Name")));
    SessionResult {
        tag: "SELECT",
        rows,
        affected: None,
        column_order: Some(columns),
    }
}

fn info_schema_tables(tables: &[TableSchema]) -> SessionResult {
    let columns = vec![
        "table_schema".into(),
        "table_name".into(),
        "table_type".into(),
    ];
    let mut rows: Vec<Record> = tables
        .iter()
        .map(|t| {
            Record::new()
                .set("table_schema", "public")
                .set("table_name", t.name.as_str())
                .set("table_type", "BASE TABLE")
        })
        .collect();
    rows.sort_by(|a, b| a.get("table_name").cmp(&b.get("table_name")));
    SessionResult {
        tag: "SELECT",
        rows,
        affected: None,
        column_order: Some(columns),
    }
}

fn info_schema_columns(tables: &[TableSchema], normalized: &str) -> SessionResult {
    let filter = extract_quoted_filter(normalized, "table_name");
    let columns = vec![
        "table_schema".into(),
        "table_name".into(),
        "column_name".into(),
        "ordinal_position".into(),
        "column_default".into(),
        "is_nullable".into(),
        "data_type".into(),
        "udt_name".into(),
    ];
    let mut rows = Vec::new();
    for table in tables {
        if let Some(want) = filter.as_deref() {
            if !table.name.eq_ignore_ascii_case(want) {
                continue;
            }
        }
        let specs = effective_columns(table);
        for (i, col) in specs.iter().enumerate() {
            let (data_type, udt) = info_schema_type_names(&col.data_type);
            rows.push(
                Record::new()
                    .set("table_schema", "public")
                    .set("table_name", table.name.as_str())
                    .set("column_name", col.name.as_str())
                    .set("ordinal_position", (i + 1).to_string())
                    .set(
                        "column_default",
                        col.default_sql.as_deref().unwrap_or(""),
                    )
                    .set(
                        "is_nullable",
                        if col.nullable { "YES" } else { "NO" },
                    )
                    .set("data_type", data_type.as_str())
                    .set("udt_name", udt.as_str()),
            );
        }
    }
    SessionResult {
        tag: "SELECT",
        rows,
        affected: None,
        column_order: Some(columns),
    }
}

/// Map catalog tokens → (`information_schema.data_type`, `udt_name`).
fn info_schema_type_names(catalog_ty: &str) -> (String, String) {
    let upper = catalog_ty.to_ascii_uppercase();
    let base = upper.split('(').next().unwrap_or(&upper).trim();
    let udt = match base {
        "SMALLINT" | "INT2" => "int2",
        "INT" | "INTEGER" | "INT4" => "int4",
        "BIGINT" | "INT8" => "int8",
        "BOOL" | "BOOLEAN" => "bool",
        "FLOAT" | "REAL" | "FLOAT4" => "float4",
        "DOUBLE" | "FLOAT8" | "DOUBLE_PRECISION" => "float8",
        "TEXT" | "VARCHAR" | "BPCHAR" | "CHAR" | "CHARACTER" | "CHARACTER_VARYING" => "text",
        "UUID" => "uuid",
        "BYTEA" => "bytea",
        "NUMERIC" | "DECIMAL" => "numeric",
        "DATE" => "date",
        "TIME" | "TIME_WITHOUT_TIME_ZONE" => "time",
        "TIMESTAMP" | "TIMESTAMP_WITHOUT_TIME_ZONE" => "timestamp",
        "TIMESTAMPTZ" | "TIMESTAMP_WITH_TIME_ZONE" => "timestamptz",
        "JSON" => "json",
        "JSONB" => "jsonb",
        "INTERVAL" => "interval",
        other => {
            return (
                other.to_ascii_lowercase(),
                other.to_ascii_lowercase(),
            );
        }
    };
    let data_type = match udt {
        "int2" => "smallint",
        "int4" => "integer",
        "int8" => "bigint",
        "bool" => "boolean",
        "float4" => "real",
        "float8" => "double precision",
        "text" => "text",
        "uuid" => "uuid",
        "bytea" => "bytea",
        "numeric" => "numeric",
        "date" => "date",
        "time" => "time without time zone",
        "timestamp" => "timestamp without time zone",
        "timestamptz" => "timestamp with time zone",
        "json" => "json",
        "jsonb" => "jsonb",
        "interval" => "interval",
        other => other,
    };
    (data_type.into(), udt.into())
}

fn pg_tables(tables: &[TableSchema], owner: &str) -> SessionResult {
    let columns = vec![
        "schemaname".into(),
        "tablename".into(),
        "tableowner".into(),
    ];
    let mut rows: Vec<Record> = tables
        .iter()
        .map(|t| {
            Record::new()
                .set("schemaname", "public")
                .set("tablename", t.name.as_str())
                .set("tableowner", owner)
        })
        .collect();
    rows.sort_by(|a, b| a.get("tablename").cmp(&b.get("tablename")));
    SessionResult {
        tag: "SELECT",
        rows,
        affected: None,
        column_order: Some(columns),
    }
}

fn effective_columns(table: &TableSchema) -> Vec<crate::schema::ColumnSpec> {
    if !table.columns.is_empty() {
        return table.columns.clone();
    }
    // Legacy API-registered tables: expose PK as a single TEXT column.
    vec![crate::schema::ColumnSpec::new(
        table.primary_key.as_str(),
        "TEXT",
    )]
}

/// Pull `table_name = 'foo'` / `= "foo"` style filters from normalized SQL.
fn extract_quoted_filter(normalized: &str, column: &str) -> Option<String> {
    let needle = format!("{column} = ");
    let idx = normalized.find(&needle)?;
    let rest = &normalized[idx + needle.len()..];
    let quote = rest.chars().next()?;
    if quote != '\'' && quote != '"' {
        return None;
    }
    let end = rest[1..].find(quote)?;
    Some(rest[1..1 + end].to_string())
}

/// Extract relation name from `\d` catalog SQL (`c.relname = 't'`, `relname ~ '^(t)$'`).
fn extract_relname_filter(normalized: &str) -> Option<String> {
    for key in ["c.relname = ", "relname = "] {
        if let Some(v) = extract_after_eq_quote(normalized, key) {
            return Some(v);
        }
    }
    // psql often uses: c.relname ~ '^(users)$'  (or ~* )
    for key in ["c.relname ~ ", "relname ~ ", "c.relname ~* ", "relname ~* "] {
        if let Some(raw) = extract_after_eq_quote(normalized, key) {
            let trimmed = raw.trim_matches(|c| c == '^' || c == '$' || c == '(' || c == ')');
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }
    }
    None
}

fn extract_after_eq_quote(normalized: &str, needle: &str) -> Option<String> {
    let idx = normalized.find(needle)?;
    let rest = &normalized[idx + needle.len()..];
    let quote = rest.chars().next()?;
    if quote != '\'' && quote != '"' {
        return None;
    }
    let end = rest[1..].find(quote)?;
    Some(rest[1..1 + end].to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{ColumnSpec, TableSchema};

    fn sample_tables() -> Vec<TableSchema> {
        vec![
            TableSchema::new("users", "id", vec![]).with_columns(vec![
                ColumnSpec::new("id", "BIGINT"),
                ColumnSpec::new("name", "TEXT"),
            ]),
            TableSchema::new("orders", "id", vec![]).with_columns(vec![
                ColumnSpec::new("id", "BIGINT"),
            ]),
        ]
    }

    #[test]
    fn psql_dt_shape_lists_public_tables() {
        let sql = r#"SELECT n.nspname as "Schema",
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
        let result = try_handle(sql, &sample_tables(), "postgres").expect("handled");
        assert_eq!(result.tag, "SELECT");
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0].get("Name"), Some("orders"));
        assert_eq!(result.rows[0].get("Schema"), Some("public"));
        assert_eq!(result.rows[0].get("Type"), Some("table"));
        assert_eq!(result.rows[0].get("Owner"), Some("postgres"));
        assert_eq!(result.rows[1].get("Name"), Some("users"));
        assert_eq!(
            result.column_order.as_deref(),
            Some(
                [
                    "Schema".to_string(),
                    "Name".to_string(),
                    "Type".to_string(),
                    "Owner".to_string()
                ]
                .as_slice()
            )
        );
    }

    #[test]
    fn information_schema_tables_and_columns() {
        let tables = sample_tables();
        let t = try_handle(
            "SELECT table_schema, table_name, table_type FROM information_schema.tables",
            &tables,
            "postgres",
        )
        .unwrap();
        assert_eq!(t.rows.len(), 2);

        let c = try_handle(
            "SELECT * FROM information_schema.columns WHERE table_name = 'users'",
            &tables,
            "postgres",
        )
        .unwrap();
        assert_eq!(c.rows.len(), 2);
        assert_eq!(c.rows[0].get("column_name"), Some("id"));
        assert_eq!(c.rows[1].get("column_name"), Some("name"));
        assert_eq!(c.rows[0].get("data_type"), Some("bigint"));
        assert_eq!(c.rows[0].get("udt_name"), Some("int8"));
        assert_eq!(c.rows[0].get("is_nullable"), Some("YES"));
        assert_eq!(c.rows[1].get("data_type"), Some("text"));
        assert_eq!(c.rows[1].get("udt_name"), Some("text"));
    }

    #[test]
    fn psql_d_describe_shape_lists_columns() {
        let sql = r#"SELECT a.attname AS "Column",
  pg_catalog.format_type(a.atttypid, a.atttypmod) AS "Type",
  a.attnotnull AS "Nullable"
FROM pg_catalog.pg_attribute a
JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
WHERE c.relname = 'users' AND a.attnum > 0 AND NOT a.attisdropped
ORDER BY a.attnum;"#;
        let result = try_handle(sql, &sample_tables(), "postgres").expect("handled");
        assert_eq!(result.tag, "SELECT");
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0].get("Column"), Some("id"));
        assert_eq!(result.rows[0].get("Type"), Some("BIGINT"));
        assert_eq!(result.rows[0].get("Nullable"), Some("not null"));
        assert_eq!(result.rows[1].get("Column"), Some("name"));
        assert_eq!(result.rows[1].get("Type"), Some("TEXT"));
    }

    #[test]
    fn psql_d_relname_regex_filter() {
        let sql = r#"SELECT a.attname AS "Column", pg_catalog.format_type(a.atttypid, a.atttypmod) AS "Type"
FROM pg_catalog.pg_attribute a, pg_catalog.pg_class c
WHERE a.attrelid = c.oid AND c.relname ~ '^(orders)$' AND a.attnum > 0;"#;
        let result = try_handle(sql, &sample_tables(), "postgres").unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("Column"), Some("id"));
    }

    #[test]
    fn ordinary_select_not_intercepted() {
        assert!(try_handle("SELECT * FROM users", &sample_tables(), "postgres").is_none());
    }
}
