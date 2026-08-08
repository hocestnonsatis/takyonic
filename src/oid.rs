//! Synthetic relation / role OIDs and PostgreSQL catalog scalar helpers
//! (`to_regclass`, `obj_description(oid)`, `format_type`, `pg_get_userbyid`).

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::schema::TableSchema;

/// Snapshot of table OID → name / column attnums for scalar helpers.
#[derive(Clone, Debug, Default)]
pub struct RelationCatalog {
    by_oid: BTreeMap<u32, RelationOidEntry>,
    by_name: BTreeMap<String, u32>,
}

/// One relation entry (synthetic `pg_class` row).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RelationOidEntry {
    /// Lowercased table name.
    pub name: String,
    /// Column names in attnum order (1-based).
    pub columns: Vec<String>,
}

impl RelationCatalog {
    /// Build from engine table schemas.
    pub fn from_schemas(schemas: &[TableSchema]) -> Self {
        let mut cat = Self::default();
        for s in schemas {
            let name = s.name.trim().to_ascii_lowercase();
            let oid = relation_oid(&name);
            let columns = if s.columns.is_empty() {
                vec![s.primary_key.trim().to_ascii_lowercase()]
            } else {
                s.columns
                    .iter()
                    .map(|c| c.name.trim().to_ascii_lowercase())
                    .collect()
            };
            cat.by_name.insert(name.clone(), oid);
            cat.by_oid.insert(
                oid,
                RelationOidEntry {
                    name,
                    columns,
                },
            );
        }
        cat
    }

    /// Shared handle for [`crate::executor::ExecutionContext`].
    pub fn shared(schemas: &[TableSchema]) -> Arc<Self> {
        Arc::new(Self::from_schemas(schemas))
    }

    /// OID for a registered table name, if present.
    pub fn oid_of(&self, table: &str) -> Option<u32> {
        let leaf = table
            .rsplit('.')
            .next()
            .unwrap_or(table)
            .trim()
            .trim_matches('"')
            .to_ascii_lowercase();
        self.by_name.get(&leaf).copied()
    }

    /// Resolve OID → relation entry.
    pub fn by_oid(&self, oid: u32) -> Option<&RelationOidEntry> {
        self.by_oid.get(&oid)
    }

    /// Column name for 1-based `attnum`, if in range.
    pub fn column_at(&self, oid: u32, attnum: i64) -> Option<&str> {
        if attnum < 1 {
            return None;
        }
        let entry = self.by_oid(oid)?;
        entry.columns.get((attnum as usize) - 1).map(String::as_str)
    }

    /// 1-based `attnum` for a column name on `oid`, if present.
    pub fn attnum_of(&self, oid: u32, column: &str) -> Option<i64> {
        let leaf = column
            .trim()
            .trim_matches('"')
            .to_ascii_lowercase();
        let entry = self.by_oid(oid)?;
        entry
            .columns
            .iter()
            .position(|c| c == &leaf)
            .map(|i| (i + 1) as i64)
    }
}

/// Stable synthetic OID for a relation / role name (FNV-1a 32-bit; never 0).
pub fn relation_oid(table: &str) -> u32 {
    let mut h: u32 = 2_166_136_261;
    for b in table.trim().to_ascii_lowercase().bytes() {
        h ^= u32::from(b);
        h = h.wrapping_mul(16_777_619);
    }
    if h == 0 {
        16_384
    } else {
        h
    }
}

/// Alias for role OIDs (`pg_authid` / `pg_get_userbyid`).
pub fn role_oid(role: &str) -> u32 {
    relation_oid(role)
}

/// Alias for namespace OIDs (`pg_namespace` / `to_regnamespace`).
pub fn namespace_oid(schema: &str) -> u32 {
    relation_oid(schema)
}

/// `to_regtype(name)` — common PostgreSQL type name → OID (aliases included).
pub fn type_oid_from_name(name: &str) -> Option<u32> {
    let n = name
        .trim()
        .trim_matches('"')
        .to_ascii_lowercase()
        .replace("  ", " ");
    // Strip schema qualifier (`pg_catalog.int8`).
    let leaf = n.rsplit('.').next().unwrap_or(&n).trim();
    Some(match leaf {
        "bool" | "boolean" => 16,
        "int8" | "bigint" => 20,
        "int2" | "smallint" => 21,
        "int4" | "integer" | "int" => 23,
        "text" => 25,
        "float4" | "real" => 700,
        "float8" | "double precision" => 701,
        "bpchar" | "character" | "char" => 1042,
        "varchar" | "character varying" => 1043,
        "date" => 1082,
        "time" | "time without time zone" => 1083,
        "timestamp" | "timestamp without time zone" => 1114,
        "timestamptz" | "timestamp with time zone" => 1184,
        "interval" => 1186,
        "numeric" | "decimal" => 1700,
        "uuid" => 2950,
        "bytea" => 17,
        "json" => 114,
        "jsonb" => 3802,
        _ => return None,
    })
}

/// Known builtin schemas for `to_regnamespace`.
pub fn namespace_exists(name: &str) -> bool {
    matches!(
        name.trim().trim_matches('"').to_ascii_lowercase().as_str(),
        "public" | "pg_catalog" | "information_schema"
    )
}

/// `format_type(type_oid, typmod)` — common PostgreSQL type OIDs.
///
/// `typmod < 0` means “no typmod” (PG `NULL` typmod).
pub fn format_type(type_oid: i64, typmod: i64) -> String {
    match type_oid {
        16 => "boolean".into(),
        20 => "bigint".into(),
        21 => "smallint".into(),
        23 => "integer".into(),
        25 => "text".into(),
        700 => "real".into(),
        701 => "double precision".into(),
        1042 => {
            // `character(n)` — typmod encodes length+4 when present.
            if typmod < 0 {
                "character".into()
            } else if typmod >= 4 {
                format!("character({})", typmod - 4)
            } else {
                "character".into()
            }
        }
        1043 => {
            if typmod < 0 {
                "character varying".into()
            } else if typmod >= 4 {
                format!("character varying({})", typmod - 4)
            } else {
                "character varying".into()
            }
        }
        1082 => "date".into(),
        1083 => "time without time zone".into(),
        1114 => "timestamp without time zone".into(),
        1184 => "timestamp with time zone".into(),
        1186 => "interval".into(),
        1700 => "numeric".into(),
        2950 => "uuid".into(),
        17 => "bytea".into(),
        114 => "json".into(),
        3802 => "jsonb".into(),
        other => format!("??? ({other})"),
    }
}

/// Resolve `pg_get_userbyid(oid)` against a list of role names.
pub fn user_by_oid<'a>(oid: u32, roles: impl IntoIterator<Item = &'a str>) -> Option<&'a str> {
    roles.into_iter().find(|r| role_oid(r) == oid)
}

/// Whether `schema` appears on a comma-separated `search_path` (`pg_catalog` always).
pub fn schema_on_search_path(search_path: &str, schema: &str) -> bool {
    let schema = schema.trim().trim_matches('"').to_ascii_lowercase();
    if schema.is_empty() {
        return false;
    }
    if schema == "pg_catalog" {
        return true;
    }
    search_path.split(',').any(|part| {
        let p = part.trim().trim_matches('"').to_ascii_lowercase();
        !p.is_empty() && p == schema
    })
}

/// Split `schema.rel` (default schema `public`).
pub fn relation_schema_and_name(spec: &str) -> (String, String) {
    let s = spec.trim().trim_matches('"');
    if let Some((schema, name)) = s.rsplit_once('.') {
        (
            schema.trim().trim_matches('"').to_ascii_lowercase(),
            name.trim().trim_matches('"').to_ascii_lowercase(),
        )
    } else {
        (
            "public".into(),
            s.trim().trim_matches('"').to_ascii_lowercase(),
        )
    }
}

/// `pg_table_is_visible(name)` — registered relation whose schema is on `search_path`.
pub fn pg_table_is_visible_name(
    search_path: &str,
    catalog: &RelationCatalog,
    spec: &str,
) -> bool {
    let (schema, name) = relation_schema_and_name(spec);
    catalog.oid_of(&name).is_some() && schema_on_search_path(search_path, &schema)
}

/// `pg_table_is_visible(oid)` — OID maps to a relation in `public` on `search_path`.
pub fn pg_table_is_visible_oid(search_path: &str, catalog: &RelationCatalog, oid: u32) -> bool {
    if catalog.by_oid(oid).is_none() {
        return false;
    }
    schema_on_search_path(search_path, "public")
}

/// `pg_type_is_visible` — known catalog type (always under `pg_catalog`).
pub fn pg_type_is_visible(type_spec: &str) -> bool {
    let leaf = type_spec
        .trim()
        .trim_matches('"')
        .rsplit('.')
        .next()
        .unwrap_or(type_spec)
        .trim()
        .to_ascii_lowercase();
    if leaf.is_empty() {
        return false;
    }
    if type_oid_from_name(&leaf).is_some() {
        return true;
    }
    if let Ok(oid) = leaf.parse::<u32>() {
        let formatted = format_type(i64::from(oid), -1);
        return !formatted.starts_with("???");
    }
    false
}

/// PG `pg_relation_is_updatable` bits: UPDATE (4) | DELETE (8) | INSERT (16).
pub const RELATION_UPDATABLE_ALL: i64 = 4 | 8 | 16;

/// `pg_relation_is_updatable(oid|name, include_triggers)` — ordinary tables → all DML bits.
///
/// `include_triggers` is accepted for signature compatibility (no triggers yet).
pub fn pg_relation_is_updatable(
    catalog: &RelationCatalog,
    name_or_oid: NameOrOid<'_>,
    _include_triggers: bool,
) -> i64 {
    let exists = match name_or_oid {
        NameOrOid::Name(name) => catalog.oid_of(name).is_some(),
        NameOrOid::Oid(oid) => catalog.by_oid(oid).is_some(),
    };
    if exists {
        RELATION_UPDATABLE_ALL
    } else {
        0
    }
}

/// Column identity for `pg_column_is_updatable` (attnum or name).
#[derive(Clone, Copy, Debug)]
pub enum ColumnRef<'a> {
    /// 1-based attribute number.
    Attnum(i64),
    /// Column name.
    Name(&'a str),
}

/// `pg_column_is_updatable(table, column, include_triggers)` — true when column exists
/// on an ordinary updatable table (`include_triggers` ignored; no triggers yet).
pub fn pg_column_is_updatable(
    catalog: &RelationCatalog,
    table: NameOrOid<'_>,
    column: ColumnRef<'_>,
    include_triggers: bool,
) -> bool {
    if pg_relation_is_updatable(catalog, table, include_triggers) == 0 {
        return false;
    }
    let oid = match table {
        NameOrOid::Name(name) => match catalog.oid_of(name) {
            Some(o) => o,
            None => return false,
        },
        NameOrOid::Oid(oid) => oid,
    };
    match column {
        ColumnRef::Attnum(n) => catalog.column_at(oid, n).is_some(),
        ColumnRef::Name(name) => catalog.attnum_of(oid, name).is_some(),
    }
}

/// Approximate on-disk sizes for `pg_relation_size` / `pg_table_size` /
/// `pg_total_relation_size` (64 bytes × MVCC version count heuristic).
#[derive(Clone, Debug, Default)]
pub struct RelationSizeCatalog {
    by_name: BTreeMap<String, RelationSizeEntry>,
    by_oid: BTreeMap<u32, RelationSizeEntry>,
}

/// Heap vs total (heap+index) byte estimates for one relation.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RelationSizeEntry {
    /// `pg_relation_size` / `pg_table_size` (heap; no TOAST yet).
    pub heap_bytes: u64,
    /// `pg_total_relation_size` (heap + indexes).
    pub total_bytes: u64,
}

impl RelationSizeEntry {
    /// `pg_indexes_size` — index bytes only (`total - heap`).
    pub fn index_bytes(self) -> u64 {
        self.total_bytes.saturating_sub(self.heap_bytes)
    }
}

impl RelationSizeCatalog {
    /// Insert a size entry for `table` (lowercased).
    pub fn insert(&mut self, table: &str, entry: RelationSizeEntry) {
        let name = table.trim().to_ascii_lowercase();
        let oid = relation_oid(&name);
        self.by_name.insert(name, entry);
        self.by_oid.insert(oid, entry);
    }

    /// Shared empty catalog.
    pub fn shared_empty() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Sum of all relation totals (`pg_database_size` heuristic).
    pub fn database_bytes(&self) -> u64 {
        self.by_name.values().map(|e| e.total_bytes).sum()
    }

    /// Look up by table name or OID.
    pub fn get(&self, name_or_oid: NameOrOid<'_>) -> Option<RelationSizeEntry> {
        match name_or_oid {
            NameOrOid::Name(name) => {
                let leaf = name
                    .rsplit('.')
                    .next()
                    .unwrap_or(name)
                    .trim()
                    .trim_matches('"')
                    .to_ascii_lowercase();
                self.by_name.get(&leaf).copied()
            }
            NameOrOid::Oid(oid) => self.by_oid.get(&oid).copied(),
        }
    }
}

/// Argument to size helpers: relation name or OID.
#[derive(Clone, Copy, Debug)]
pub enum NameOrOid<'a> {
    /// Relation name (`users` / `public.users`).
    Name(&'a str),
    /// Synthetic relation OID.
    Oid(u32),
}

/// Bytes-per-version heuristic for size estimates.
pub const RELATION_SIZE_BYTES_PER_VERSION: u64 = 64;

/// `pg_class` catalog OID (`classid` for relations / indexes).
pub const CLASS_PG_CLASS: u32 = 1259;
/// `pg_type` catalog OID.
pub const CLASS_PG_TYPE: u32 = 1247;
/// `pg_authid` catalog OID.
pub const CLASS_PG_AUTHID: u32 = 1260;
/// `pg_namespace` catalog OID.
pub const CLASS_PG_NAMESPACE: u32 = 2615;
/// `pg_proc` catalog OID.
pub const CLASS_PG_PROC: u32 = 1255;

/// `pg_describe_object(classid, objid, objsubid)` — human-readable object identity.
///
/// `proc_name` supplies a resolved function name when `classid = pg_proc`.
pub fn pg_describe_object(
    classid: u32,
    objid: u32,
    objsubid: i32,
    relations: Option<&RelationCatalog>,
    indexes: Option<&IndexCatalog>,
    role_names: impl IntoIterator<Item = impl AsRef<str>>,
    proc_name: Option<&str>,
) -> Option<String> {
    match classid {
        CLASS_PG_CLASS => {
            if objsubid > 0 {
                let rel = relations?.by_oid(objid)?;
                let col = relations?.column_at(objid, i64::from(objsubid))?;
                return Some(format!("column {} of table {}", col, rel.name));
            }
            if let Some(rel) = relations.and_then(|c| c.by_oid(objid)) {
                return Some(format!("table {}", rel.name));
            }
            if let Some(idx) = indexes.and_then(|c| c.by_oid(objid)) {
                return Some(format!("index {}", idx.name));
            }
            None
        }
        CLASS_PG_TYPE => {
            let formatted = format_type(i64::from(objid), -1);
            if formatted.starts_with("???") {
                None
            } else {
                Some(format!("type {formatted}"))
            }
        }
        CLASS_PG_NAMESPACE => {
            for name in ["public", "pg_catalog", "information_schema"] {
                if namespace_oid(name) == objid {
                    return Some(format!("schema {name}"));
                }
            }
            None
        }
        CLASS_PG_AUTHID => {
            let names: Vec<String> = role_names
                .into_iter()
                .map(|s| s.as_ref().to_string())
                .collect();
            let role = user_by_oid(objid, names.iter().map(String::as_str))?;
            Some(format!("role {role}"))
        }
        CLASS_PG_PROC => {
            let name = proc_name?;
            Some(format!("function {name}"))
        }
        _ => None,
    }
}

/// `pg_identify_object(classid, objid, objsubid)` — PG `identity` field (schema-qualified).
///
/// Record `(type, schema, name, identity)` is not returned; this scalar yields `identity` only.
pub fn pg_identify_object(
    classid: u32,
    objid: u32,
    objsubid: i32,
    relations: Option<&RelationCatalog>,
    indexes: Option<&IndexCatalog>,
    role_names: impl IntoIterator<Item = impl AsRef<str>>,
    proc_name: Option<&str>,
) -> Option<String> {
    match classid {
        CLASS_PG_CLASS => {
            if objsubid > 0 {
                let rel = relations?.by_oid(objid)?;
                let col = relations?.column_at(objid, i64::from(objsubid))?;
                return Some(format!(
                    "column {col} of table public.{}",
                    rel.name
                ));
            }
            if let Some(rel) = relations.and_then(|c| c.by_oid(objid)) {
                return Some(format!("table public.{}", rel.name));
            }
            if let Some(idx) = indexes.and_then(|c| c.by_oid(objid)) {
                return Some(format!("index public.{}", idx.name));
            }
            None
        }
        CLASS_PG_TYPE => {
            let formatted = format_type(i64::from(objid), -1);
            if formatted.starts_with("???") {
                None
            } else {
                Some(format!("type {formatted}"))
            }
        }
        CLASS_PG_NAMESPACE => {
            for name in ["public", "pg_catalog", "information_schema"] {
                if namespace_oid(name) == objid {
                    return Some(format!("schema {name}"));
                }
            }
            None
        }
        CLASS_PG_AUTHID => {
            let names: Vec<String> = role_names
                .into_iter()
                .map(|s| s.as_ref().to_string())
                .collect();
            let role = user_by_oid(objid, names.iter().map(String::as_str))?;
            Some(format!("role {role}"))
        }
        CLASS_PG_PROC => {
            let name = proc_name?;
            Some(format!("function public.{name}()"))
        }
        _ => None,
    }
}

/// One secondary index entry for `pg_get_indexdef` / synthetic index OIDs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexOidEntry {
    /// Index name.
    pub name: String,
    /// Owning table name.
    pub table: String,
    /// Indexed column.
    pub column: String,
    /// True for HNSW / vector indexes.
    pub is_vector: bool,
}

/// Snapshot of secondary indexes for catalog scalars.
#[derive(Clone, Debug, Default)]
pub struct IndexCatalog {
    by_name: BTreeMap<String, IndexOidEntry>,
    by_oid: BTreeMap<u32, IndexOidEntry>,
}

impl IndexCatalog {
    /// Build from table schemas' secondary indexes.
    pub fn from_schemas(schemas: &[crate::schema::TableSchema]) -> Self {
        let mut cat = Self::default();
        for s in schemas {
            let table = s.name.trim().to_ascii_lowercase();
            for idx in &s.indexes {
                let name = idx.name.trim().to_ascii_lowercase();
                let entry = IndexOidEntry {
                    name: name.clone(),
                    table: table.clone(),
                    column: idx.column.trim().to_ascii_lowercase(),
                    is_vector: idx.is_vector(),
                };
                let oid = relation_oid(&name);
                cat.by_name.insert(name, entry.clone());
                cat.by_oid.insert(oid, entry);
            }
        }
        cat
    }

    /// Shared handle for [`crate::executor::ExecutionContext`].
    pub fn shared(schemas: &[crate::schema::TableSchema]) -> Arc<Self> {
        Arc::new(Self::from_schemas(schemas))
    }

    /// Lookup by index name.
    pub fn by_name(&self, name: &str) -> Option<&IndexOidEntry> {
        let leaf = name
            .rsplit('.')
            .next()
            .unwrap_or(name)
            .trim()
            .trim_matches('"')
            .to_ascii_lowercase();
        self.by_name.get(&leaf)
    }

    /// Lookup by synthetic OID (`relation_oid(index_name)`).
    pub fn by_oid(&self, oid: u32) -> Option<&IndexOidEntry> {
        self.by_oid.get(&oid)
    }

    /// OID for a registered index name, if present.
    pub fn oid_of(&self, name: &str) -> Option<u32> {
        self.by_name(name).map(|e| relation_oid(&e.name))
    }
}

/// `pg_get_indexdef` — reconstruct a `CREATE INDEX` statement.
pub fn pg_get_indexdef(entry: &IndexOidEntry) -> String {
    let method = if entry.is_vector { "hnsw" } else { "btree" };
    format!(
        "CREATE INDEX {} ON {} USING {} ({})",
        entry.name, entry.table, method, entry.column
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{ColumnSpec, TableSchema};

    #[test]
    fn relation_oid_stable_and_catalog_roundtrip() {
        assert_eq!(relation_oid("Users"), relation_oid("users"));
        assert_ne!(relation_oid("users"), 0);
        let schemas = vec![TableSchema::new("users", "id", vec![]).with_columns(vec![
            ColumnSpec::new("id", "BIGINT"),
            ColumnSpec::new("name", "TEXT"),
        ])];
        let cat = RelationCatalog::from_schemas(&schemas);
        let oid = cat.oid_of("users").unwrap();
        assert_eq!(oid, relation_oid("users"));
        assert_eq!(cat.by_oid(oid).unwrap().name, "users");
        assert_eq!(cat.column_at(oid, 1), Some("id"));
        assert_eq!(cat.column_at(oid, 2), Some("name"));
        assert_eq!(cat.column_at(oid, 3), None);
        assert!(cat.oid_of("missing").is_none());
    }

    #[test]
    fn format_type_common_oids() {
        assert_eq!(format_type(20, -1), "bigint");
        assert_eq!(format_type(25, -1), "text");
        assert_eq!(format_type(16, -1), "boolean");
        assert_eq!(format_type(1043, -1), "character varying");
        assert_eq!(format_type(1043, 54), "character varying(50)");
        assert_eq!(format_type(999_999, -1), "??? (999999)");
    }

    #[test]
    fn pg_get_userbyid_resolves_role_oid() {
        let oid = role_oid("postgres");
        assert_eq!(
            user_by_oid(oid, ["analyst", "postgres", "reader"]),
            Some("postgres")
        );
        assert_eq!(user_by_oid(1, ["postgres"]), None);
    }

    #[test]
    fn to_regtype_and_namespace_oids() {
        assert_eq!(type_oid_from_name("bigint"), Some(20));
        assert_eq!(type_oid_from_name("INT8"), Some(20));
        assert_eq!(type_oid_from_name("pg_catalog.int4"), Some(23));
        assert_eq!(type_oid_from_name("character varying"), Some(1043));
        assert_eq!(type_oid_from_name("nope"), None);
        assert!(namespace_exists("public"));
        assert!(namespace_exists("PG_CATALOG"));
        assert!(!namespace_exists("missing"));
        assert_ne!(namespace_oid("public"), 0);
    }

    #[test]
    fn table_and_type_visibility() {
        let schemas = vec![TableSchema::new("users", "id", vec![])];
        let cat = RelationCatalog::from_schemas(&schemas);
        let oid = cat.oid_of("users").unwrap();
        assert!(pg_table_is_visible_name("public", &cat, "users"));
        assert!(pg_table_is_visible_name("myschema, public", &cat, "public.users"));
        assert!(!pg_table_is_visible_name("myschema", &cat, "users"));
        assert!(pg_table_is_visible_oid("public", &cat, oid));
        assert!(!pg_table_is_visible_oid("myschema", &cat, oid));
        assert!(!pg_table_is_visible_oid("public", &cat, 1));
        assert!(pg_type_is_visible("integer"));
        assert!(pg_type_is_visible("23"));
        assert!(!pg_type_is_visible("nope_type"));
        assert!(schema_on_search_path("public", "pg_catalog"));
    }

    #[test]
    fn relation_is_updatable_bitmask() {
        let schemas = vec![TableSchema::new("users", "id", vec![])];
        let cat = RelationCatalog::from_schemas(&schemas);
        let oid = cat.oid_of("users").unwrap();
        assert_eq!(
            pg_relation_is_updatable(&cat, NameOrOid::Name("users"), true),
            RELATION_UPDATABLE_ALL
        );
        assert_eq!(
            pg_relation_is_updatable(&cat, NameOrOid::Oid(oid), false),
            RELATION_UPDATABLE_ALL
        );
        assert_eq!(
            pg_relation_is_updatable(&cat, NameOrOid::Name("missing"), true),
            0
        );
        assert_eq!(RELATION_UPDATABLE_ALL, 28);
    }

    #[test]
    fn column_is_updatable() {
        let schemas = vec![TableSchema::new("users", "id", vec![]).with_columns(vec![
            ColumnSpec::new("id", "BIGINT"),
            ColumnSpec::new("name", "TEXT"),
        ])];
        let cat = RelationCatalog::from_schemas(&schemas);
        let oid = cat.oid_of("users").unwrap();
        assert_eq!(cat.attnum_of(oid, "name"), Some(2));
        assert!(pg_column_is_updatable(
            &cat,
            NameOrOid::Name("users"),
            ColumnRef::Name("name"),
            true
        ));
        assert!(pg_column_is_updatable(
            &cat,
            NameOrOid::Oid(oid),
            ColumnRef::Attnum(1),
            false
        ));
        assert!(!pg_column_is_updatable(
            &cat,
            NameOrOid::Name("users"),
            ColumnRef::Name("missing"),
            true
        ));
        assert!(!pg_column_is_updatable(
            &cat,
            NameOrOid::Name("nope"),
            ColumnRef::Name("id"),
            true
        ));
    }

    #[test]
    fn get_indexdef_from_catalog() {
        use crate::schema::IndexDef;
        let schemas = vec![TableSchema::new(
            "employees",
            "id",
            vec![IndexDef::new("idx_dept", "department")],
        )];
        let cat = IndexCatalog::from_schemas(&schemas);
        let e = cat.by_name("idx_dept").unwrap();
        assert_eq!(
            pg_get_indexdef(e),
            "CREATE INDEX idx_dept ON employees USING btree (department)"
        );
        let oid = cat.oid_of("idx_dept").unwrap();
        assert_eq!(cat.by_oid(oid).unwrap().column, "department");
        assert!(cat.by_name("missing").is_none());
    }

    #[test]
    fn describe_object_common_classes() {
        use crate::schema::IndexDef;
        let schemas = vec![
            TableSchema::new("users", "id", vec![])
                .with_columns(vec![
                    ColumnSpec::new("id", "BIGINT"),
                    ColumnSpec::new("name", "TEXT"),
                ]),
            TableSchema::new(
                "employees",
                "id",
                vec![IndexDef::new("idx_dept", "department")],
            ),
        ];
        let rel = RelationCatalog::from_schemas(&schemas);
        let idx = IndexCatalog::from_schemas(&schemas);
        let users = rel.oid_of("users").unwrap();
        assert_eq!(
            pg_describe_object(
                CLASS_PG_CLASS,
                users,
                0,
                Some(&rel),
                Some(&idx),
                std::iter::empty::<&str>(),
                None
            )
            .as_deref(),
            Some("table users")
        );
        assert_eq!(
            pg_describe_object(
                CLASS_PG_CLASS,
                users,
                2,
                Some(&rel),
                Some(&idx),
                std::iter::empty::<&str>(),
                None
            )
            .as_deref(),
            Some("column name of table users")
        );
        let ioid = idx.oid_of("idx_dept").unwrap();
        assert_eq!(
            pg_describe_object(
                CLASS_PG_CLASS,
                ioid,
                0,
                Some(&rel),
                Some(&idx),
                std::iter::empty::<&str>(),
                None
            )
            .as_deref(),
            Some("index idx_dept")
        );
        assert_eq!(
            pg_describe_object(
                CLASS_PG_TYPE,
                23,
                0,
                None,
                None,
                std::iter::empty::<&str>(),
                None
            )
            .as_deref(),
            Some("type integer")
        );
        assert_eq!(
            pg_describe_object(
                CLASS_PG_AUTHID,
                role_oid("postgres"),
                0,
                None,
                None,
                ["postgres", "analyst"],
                None
            )
            .as_deref(),
            Some("role postgres")
        );
        assert_eq!(
            pg_describe_object(
                CLASS_PG_NAMESPACE,
                namespace_oid("public"),
                0,
                None,
                None,
                std::iter::empty::<&str>(),
                None
            )
            .as_deref(),
            Some("schema public")
        );
        assert_eq!(
            pg_describe_object(
                CLASS_PG_PROC,
                1,
                0,
                None,
                None,
                std::iter::empty::<&str>(),
                Some("lower")
            )
            .as_deref(),
            Some("function lower")
        );
    }

    #[test]
    fn identify_object_identity_strings() {
        let schemas = vec![TableSchema::new("users", "id", vec![])
            .with_columns(vec![
                ColumnSpec::new("id", "BIGINT"),
                ColumnSpec::new("name", "TEXT"),
            ])];
        let rel = RelationCatalog::from_schemas(&schemas);
        let idx = IndexCatalog::from_schemas(&schemas);
        let users = rel.oid_of("users").unwrap();
        assert_eq!(
            pg_identify_object(
                CLASS_PG_CLASS,
                users,
                0,
                Some(&rel),
                Some(&idx),
                std::iter::empty::<&str>(),
                None
            )
            .as_deref(),
            Some("table public.users")
        );
        assert_eq!(
            pg_identify_object(
                CLASS_PG_CLASS,
                users,
                2,
                Some(&rel),
                Some(&idx),
                std::iter::empty::<&str>(),
                None
            )
            .as_deref(),
            Some("column name of table public.users")
        );
        assert_eq!(
            pg_identify_object(
                CLASS_PG_PROC,
                1,
                0,
                None,
                None,
                std::iter::empty::<&str>(),
                Some("lower")
            )
            .as_deref(),
            Some("function public.lower()")
        );
        assert_eq!(
            pg_identify_object(
                CLASS_PG_TYPE,
                25,
                0,
                None,
                None,
                std::iter::empty::<&str>(),
                None
            )
            .as_deref(),
            Some("type text")
        );
    }
}
