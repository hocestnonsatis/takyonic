//! Persistent system catalog for table schemas and secondary / vector indexes.
//!
//! On-disk format (`data_dir/CATALOG`):
//! ```text
//! TABLE <name> <primary_key> [LSM|BTREE]
//! COLUMN <table> <column_name> <data_type>
//! PARTITION HASH <column> <bucket_count>
//! PARTITION RANGE <column> <bound0> <bound1> …
//! PMAP <node0> <node1> …
//! INDEX <table> <index_name> <column>
//! VINDEX <table> <index_name> <column> <dimension> <metric> <type>
//! ```
//!
//! Loaded into memory at engine open; rewritten atomically on every DDL change.

use std::collections::BTreeMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::error::{Result, TakyonicError};
use crate::partition::{PartitionMap, PartitioningStrategy};
use crate::schema::{ColumnSpec, IndexDef, TableSchema};
use crate::vector::{DistanceMetric, VectorIndexSpec};

const CATALOG_FILE: &str = "CATALOG";

/// Path to the durable catalog file under `data_dir`.
pub fn catalog_path(data_dir: &Path) -> PathBuf {
    data_dir.join(CATALOG_FILE)
}

/// Load all table schemas from `data_dir/CATALOG` (empty map if missing).
pub fn load_catalog(data_dir: &Path) -> Result<BTreeMap<String, TableSchema>> {
    let path = catalog_path(data_dir);
    if !path.exists() {
        return Ok(BTreeMap::new());
    }
    let text = fs::read_to_string(&path)?;
    let mut tables: BTreeMap<String, TableSchema> = BTreeMap::new();
    let mut last_table: Option<String> = None;
    for (lineno, raw) in text.lines().enumerate() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut parts = line.split_whitespace();
        let tag = parts.next().ok_or_else(|| {
            TakyonicError::Engine(format!("catalog line {}: empty", lineno + 1))
        })?;
        match tag {
            "TABLE" => {
                let name = parts.next().ok_or_else(|| {
                    TakyonicError::Engine(format!("catalog line {}: TABLE missing name", lineno + 1))
                })?;
                let pk = parts.next().ok_or_else(|| {
                    TakyonicError::Engine(format!(
                        "catalog line {}: TABLE missing primary key",
                        lineno + 1
                    ))
                })?;
                let mut schema = TableSchema::new(name, pk, Vec::new());
                if let Some(eng) = parts.next() {
                    if let Some(kind) = crate::storage::StorageEngineKind::parse(eng) {
                        schema.storage_engine = kind;
                    }
                }
                last_table = Some(name.to_string());
                tables.insert(name.to_string(), schema);
            }
            "COLUMN" => {
                let table = parts.next().ok_or_else(|| {
                    TakyonicError::Engine(format!(
                        "catalog line {}: COLUMN missing table",
                        lineno + 1
                    ))
                })?;
                let col_name = parts.next().ok_or_else(|| {
                    TakyonicError::Engine(format!(
                        "catalog line {}: COLUMN missing name",
                        lineno + 1
                    ))
                })?;
                let data_type = parts.next().ok_or_else(|| {
                    TakyonicError::Engine(format!(
                        "catalog line {}: COLUMN missing data type",
                        lineno + 1
                    ))
                })?;
                let schema = tables.get_mut(table).ok_or_else(|| {
                    TakyonicError::Engine(format!(
                        "catalog line {}: COLUMN for unknown table `{table}`",
                        lineno + 1
                    ))
                })?;
                let mut spec = ColumnSpec::new(col_name, data_type);
                for flag in parts {
                    match flag {
                        "NOTNULL" => spec.nullable = false,
                        "UNIQUE" => spec.unique = true,
                        other => {
                            return Err(TakyonicError::Engine(format!(
                                "catalog line {}: unknown COLUMN flag `{other}`",
                                lineno + 1
                            )));
                        }
                    }
                }
                schema.columns.push(spec);
            }
            "COLDEFAULT" => {
                let table = parts.next().ok_or_else(|| {
                    TakyonicError::Engine(format!(
                        "catalog line {}: COLDEFAULT missing table",
                        lineno + 1
                    ))
                })?;
                let col_name = parts.next().ok_or_else(|| {
                    TakyonicError::Engine(format!(
                        "catalog line {}: COLDEFAULT missing column",
                        lineno + 1
                    ))
                })?;
                let expr = parts.collect::<Vec<_>>().join(" ");
                if expr.is_empty() {
                    return Err(TakyonicError::Engine(format!(
                        "catalog line {}: COLDEFAULT missing expression",
                        lineno + 1
                    )));
                }
                let schema = tables.get_mut(table).ok_or_else(|| {
                    TakyonicError::Engine(format!(
                        "catalog line {}: COLDEFAULT for unknown table `{table}`",
                        lineno + 1
                    ))
                })?;
                let col = schema
                    .columns
                    .iter_mut()
                    .find(|c| c.name == col_name)
                    .ok_or_else(|| {
                        TakyonicError::Engine(format!(
                            "catalog line {}: COLDEFAULT unknown column `{col_name}`",
                            lineno + 1
                        ))
                    })?;
                col.default_sql = Some(expr);
            }
            "PARTITION" => {
                let table = last_table.as_deref().ok_or_else(|| {
                    TakyonicError::Engine(format!(
                        "catalog line {}: PARTITION without TABLE",
                        lineno + 1
                    ))
                })?;
                let kind = parts.next().ok_or_else(|| {
                    TakyonicError::Engine(format!(
                        "catalog line {}: PARTITION missing kind",
                        lineno + 1
                    ))
                })?;
                let column = parts.next().ok_or_else(|| {
                    TakyonicError::Engine(format!(
                        "catalog line {}: PARTITION missing column",
                        lineno + 1
                    ))
                })?;
                let strategy = match kind.to_ascii_uppercase().as_str() {
                    "HASH" => {
                        let buckets: u32 = parts
                            .next()
                            .and_then(|s| s.parse().ok())
                            .unwrap_or(1)
                            .max(1);
                        PartitioningStrategy::Hash {
                            column: column.to_string(),
                            bucket_count: buckets,
                        }
                    }
                    "RANGE" => {
                        let bounds: Vec<String> = parts.map(str::to_string).collect();
                        PartitioningStrategy::Range {
                            column: column.to_string(),
                            bounds,
                        }
                    }
                    other => {
                        return Err(TakyonicError::Engine(format!(
                            "catalog line {}: unknown PARTITION kind `{other}`",
                            lineno + 1
                        )));
                    }
                };
                let schema = tables.get_mut(table).unwrap();
                schema.partitioning = strategy;
            }
            "PMAP" => {
                let table = last_table.as_deref().ok_or_else(|| {
                    TakyonicError::Engine(format!(
                        "catalog line {}: PMAP without TABLE",
                        lineno + 1
                    ))
                })?;
                let assignments: Result<Vec<u64>> = parts
                    .map(|s| {
                        s.parse::<u64>().map_err(|_| {
                            TakyonicError::Engine(format!(
                                "catalog line {}: bad PMAP node id `{s}`",
                                lineno + 1
                            ))
                        })
                    })
                    .collect();
                tables.get_mut(table).unwrap().partition_map = PartitionMap {
                    assignments: assignments?,
                };
            }
            "INDEX" => {
                let table = parts.next().ok_or_else(|| {
                    TakyonicError::Engine(format!(
                        "catalog line {}: INDEX missing table",
                        lineno + 1
                    ))
                })?;
                let index = parts.next().ok_or_else(|| {
                    TakyonicError::Engine(format!(
                        "catalog line {}: INDEX missing name",
                        lineno + 1
                    ))
                })?;
                let column = parts.next().ok_or_else(|| {
                    TakyonicError::Engine(format!(
                        "catalog line {}: INDEX missing column",
                        lineno + 1
                    ))
                })?;
                let schema = tables.get_mut(table).ok_or_else(|| {
                    TakyonicError::Engine(format!(
                        "catalog line {}: INDEX for unknown table `{table}`",
                        lineno + 1
                    ))
                })?;
                schema.indexes.push(IndexDef::new(index, column));
            }
            "VINDEX" => {
                let table = parts.next().ok_or_else(|| {
                    TakyonicError::Engine(format!(
                        "catalog line {}: VINDEX missing table",
                        lineno + 1
                    ))
                })?;
                let index = parts.next().ok_or_else(|| {
                    TakyonicError::Engine(format!(
                        "catalog line {}: VINDEX missing name",
                        lineno + 1
                    ))
                })?;
                let column = parts.next().ok_or_else(|| {
                    TakyonicError::Engine(format!(
                        "catalog line {}: VINDEX missing column",
                        lineno + 1
                    ))
                })?;
                let dim: usize = parts
                    .next()
                    .ok_or_else(|| {
                        TakyonicError::Engine(format!(
                            "catalog line {}: VINDEX missing dimension",
                            lineno + 1
                        ))
                    })?
                    .parse()
                    .map_err(|_| {
                        TakyonicError::Engine(format!(
                            "catalog line {}: VINDEX bad dimension",
                            lineno + 1
                        ))
                    })?;
                let metric_s = parts.next().unwrap_or("EUCLIDEAN");
                let metric = match metric_s.to_ascii_uppercase().as_str() {
                    "COSINE" => DistanceMetric::Cosine,
                    _ => DistanceMetric::Euclidean,
                };
                let index_type = parts.next().unwrap_or("HNSW").to_string();
                let schema = tables.get_mut(table).ok_or_else(|| {
                    TakyonicError::Engine(format!(
                        "catalog line {}: VINDEX for unknown table `{table}`",
                        lineno + 1
                    ))
                })?;
                let mut spec = VectorIndexSpec::hnsw(dim);
                spec.metric = metric;
                spec.index_type = index_type;
                schema.indexes.push(IndexDef::vector(index, column, spec));
            }
            other => {
                return Err(TakyonicError::Engine(format!(
                    "catalog line {}: unknown tag `{other}`",
                    lineno + 1
                )));
            }
        }
    }
    Ok(tables)
}

/// Atomically rewrite the catalog file from the in-memory schema map.
pub fn save_catalog(data_dir: &Path, schemas: &BTreeMap<String, TableSchema>) -> Result<()> {
    fs::create_dir_all(data_dir)?;
    let path = catalog_path(data_dir);
    let tmp = data_dir.join(format!("{CATALOG_FILE}.tmp"));
    {
        let mut f = fs::File::create(&tmp)?;
        writeln!(f, "# Takyonic system catalog")?;
        for schema in schemas.values() {
            writeln!(
                f,
                "TABLE {} {} {}",
                schema.name,
                schema.primary_key,
                schema.storage_engine.as_str()
            )?;
            for col in &schema.columns {
                let mut line = format!(
                    "COLUMN {} {} {}",
                    schema.name, col.name, col.data_type
                );
                if !col.nullable {
                    line.push_str(" NOTNULL");
                }
                if col.unique {
                    line.push_str(" UNIQUE");
                }
                writeln!(f, "{line}")?;
                if let Some(def) = &col.default_sql {
                    writeln!(
                        f,
                        "COLDEFAULT {} {} {}",
                        schema.name, col.name, def
                    )?;
                }
            }
            match &schema.partitioning {
                PartitioningStrategy::None => {}
                PartitioningStrategy::Hash {
                    column,
                    bucket_count,
                } => {
                    writeln!(f, "PARTITION HASH {column} {bucket_count}")?;
                }
                PartitioningStrategy::Range { column, bounds } => {
                    write!(f, "PARTITION RANGE {column}")?;
                    for b in bounds {
                        write!(f, " {b}")?;
                    }
                    writeln!(f)?;
                }
            }
            if !schema.partition_map.assignments.is_empty()
                && !matches!(schema.partitioning, PartitioningStrategy::None)
            {
                write!(f, "PMAP")?;
                for n in &schema.partition_map.assignments {
                    write!(f, " {n}")?;
                }
                writeln!(f)?;
            }
            for idx in &schema.indexes {
                if let Some(spec) = &idx.vector {
                    let metric = match spec.metric {
                        DistanceMetric::Euclidean => "EUCLIDEAN",
                        DistanceMetric::Cosine => "COSINE",
                    };
                    writeln!(
                        f,
                        "VINDEX {} {} {} {} {} {}",
                        schema.name,
                        idx.name,
                        idx.column,
                        spec.dimension,
                        metric,
                        spec.index_type
                    )?;
                } else {
                    writeln!(f, "INDEX {} {} {}", schema.name, idx.name, idx.column)?;
                }
            }
        }
        f.sync_all()?;
    }
    fs::rename(&tmp, &path)?;
    if let Ok(dir) = fs::File::open(data_dir) {
        let _ = dir.sync_all();
    }
    Ok(())
}

/// Serialize a single table's catalog lines (Raft `CatalogUpsert` payload).
pub fn encode_table_block(schema: &TableSchema) -> String {
    let mut out = String::new();
    out.push_str(&format!(
        "TABLE {} {} {}\n",
        schema.name,
        schema.primary_key,
        schema.storage_engine.as_str()
    ));
    for col in &schema.columns {
        let mut line = format!(
            "COLUMN {} {} {}",
            schema.name, col.name, col.data_type
        );
        if !col.nullable {
            line.push_str(" NOTNULL");
        }
        if col.unique {
            line.push_str(" UNIQUE");
        }
        out.push_str(&line);
        out.push('\n');
        if let Some(def) = &col.default_sql {
            out.push_str(&format!(
                "COLDEFAULT {} {} {}\n",
                schema.name, col.name, def
            ));
        }
    }
    match &schema.partitioning {
        PartitioningStrategy::None => {}
        PartitioningStrategy::Hash {
            column,
            bucket_count,
        } => {
            out.push_str(&format!("PARTITION HASH {column} {bucket_count}\n"));
        }
        PartitioningStrategy::Range { column, bounds } => {
            out.push_str(&format!("PARTITION RANGE {column}"));
            for b in bounds {
                out.push(' ');
                out.push_str(b);
            }
            out.push('\n');
        }
    }
    if !schema.partition_map.assignments.is_empty()
        && !matches!(schema.partitioning, PartitioningStrategy::None)
    {
        out.push_str("PMAP");
        for n in &schema.partition_map.assignments {
            out.push(' ');
            out.push_str(&n.to_string());
        }
        out.push('\n');
    }
    for idx in &schema.indexes {
        if let Some(spec) = &idx.vector {
            let metric = match spec.metric {
                DistanceMetric::Euclidean => "EUCLIDEAN",
                DistanceMetric::Cosine => "COSINE",
            };
            out.push_str(&format!(
                "VINDEX {} {} {} {} {} {}\n",
                schema.name,
                idx.name,
                idx.column,
                spec.dimension,
                metric,
                spec.index_type
            ));
        } else {
            out.push_str(&format!(
                "INDEX {} {} {}\n",
                schema.name, idx.name, idx.column
            ));
        }
    }
    out
}

/// Parse a single-table catalog block (inverse of [`encode_table_block`]).
pub fn parse_table_block(text: &str) -> Result<TableSchema> {
    let loaded = load_catalog_from_text(text)?;
    if loaded.len() != 1 {
        return Err(TakyonicError::Engine(format!(
            "catalog block must contain exactly one TABLE, got {}",
            loaded.len()
        )));
    }
    Ok(loaded.into_values().next().unwrap())
}

fn load_catalog_from_text(text: &str) -> Result<BTreeMap<String, TableSchema>> {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("takyonic-cat-block-{nanos}"));
    fs::create_dir_all(&dir)?;
    fs::write(catalog_path(&dir), text)?;
    let loaded = load_catalog(&dir);
    let _ = fs::remove_dir_all(&dir);
    loaded
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn catalog_roundtrip() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("takyonic-catalog-{nanos}"));
        fs::create_dir_all(&root).unwrap();

        let mut schemas = BTreeMap::new();
        schemas.insert(
            "employees".into(),
            TableSchema::new(
                "employees",
                "id",
                vec![IndexDef::new("idx_dept", "department")],
            )
            .with_columns(vec![
                ColumnSpec::new("id", "BIGINT"),
                ColumnSpec::new("department", "TEXT"),
            ]),
        );
        schemas.insert(
            "docs".into(),
            TableSchema::new(
                "docs",
                "id",
                vec![IndexDef::vector("idx_v", "vec", VectorIndexSpec::hnsw(128))],
            ),
        );
        schemas.insert(
            "users".into(),
            TableSchema::new("users", "user_id", vec![])
                .with_partitioning(PartitioningStrategy::Hash {
                    column: "user_id".into(),
                    bucket_count: 3,
                })
                .with_partition_map(PartitionMap::round_robin(&[1, 2, 3], 3)),
        );
        save_catalog(&root, &schemas).unwrap();
        let loaded = load_catalog(&root).unwrap();
        assert_eq!(loaded.get("employees").unwrap().primary_key, "id");
        assert_eq!(
            loaded.get("employees").unwrap().columns,
            vec![
                ColumnSpec::new("id", "BIGINT"),
                ColumnSpec::new("department", "TEXT"),
            ]
        );
        assert_eq!(
            loaded.get("employees").unwrap().indexes[0].name,
            "idx_dept"
        );
        assert_eq!(
            loaded.get("employees").unwrap().indexes[0].column,
            "department"
        );
        let v = &loaded.get("docs").unwrap().indexes[0];
        assert!(v.is_vector());
        assert_eq!(v.vector.as_ref().unwrap().dimension, 128);
        let u = loaded.get("users").unwrap();
        assert!(matches!(
            u.partitioning,
            PartitioningStrategy::Hash {
                bucket_count: 3,
                ..
            }
        ));
        assert_eq!(u.partition_map.assignments, vec![1, 2, 3]);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn encode_parse_table_block_roundtrip() {
        let schema = TableSchema::new(
            "t",
            "id",
            vec![IndexDef::new("idx_a", "a")],
        )
        .with_columns(vec![
            ColumnSpec::new("id", "BIGINT"),
            ColumnSpec::new("a", "TEXT"),
        ]);
        let block = encode_table_block(&schema);
        let parsed = parse_table_block(&block).unwrap();
        assert_eq!(parsed.name, "t");
        assert_eq!(parsed.primary_key, "id");
        assert_eq!(parsed.columns, schema.columns);
        assert_eq!(parsed.indexes.len(), 1);
        assert_eq!(parsed.indexes[0].name, "idx_a");
    }
}
