//! Relational projection onto Takyonic's flat MVCC key space.
//!
//! **Data key:** `Data_<table>_<pk>` → serialized record  
//! **Index key:** `Idx_<table>_<index>_<value>_<pk>` → empty value (PK is in the key)

use std::collections::BTreeMap;

use bytes::{BufMut, Bytes, BytesMut};

use crate::error::{Result, TakyonicError};
use crate::types::{Key, Value};
use crate::vector::VectorIndexSpec;

/// Storage key prefix for heap/table rows (`Data_<table>_<pk>`).
pub const DATA_PREFIX: &[u8] = b"Data_";
/// Storage key prefix for secondary index entries (`Idx_<table>_…`).
pub const IDX_PREFIX: &[u8] = b"Idx_";
const SEP: u8 = b'_';

/// A typed column declared in `CREATE TABLE` / catalog `COLUMN` lines.
///
/// Storage remains a string-field [`Record`] until typed coercion lands (Roadmap A2);
/// this metadata is durable catalog state for planning and client Describe.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnSpec {
    /// Column / field name.
    pub name: String,
    /// Canonical type token without whitespace (`BIGINT`, `TEXT`, `BOOL`, …).
    pub data_type: String,
    /// When false, INSERT/UPDATE must supply a non-NULL value.
    pub nullable: bool,
    /// Optional SQL default expression text (`now()`, `'x'`, `gen_random_uuid()`, …).
    pub default_sql: Option<String>,
    /// When true, values must be unique across the table (secondary unique index).
    pub unique: bool,
}

impl ColumnSpec {
    /// Construct a nullable column specification (no default / unique).
    pub fn new(name: impl Into<String>, data_type: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            data_type: data_type.into(),
            nullable: true,
            default_sql: None,
            unique: false,
        }
    }

    /// Builder: NOT NULL.
    pub fn not_null(mut self) -> Self {
        self.nullable = false;
        self
    }

    /// Builder: column DEFAULT expression (SQL text).
    pub fn with_default(mut self, expr: impl Into<String>) -> Self {
        self.default_sql = Some(expr.into());
        self
    }

    /// Builder: UNIQUE constraint.
    pub fn unique(mut self) -> Self {
        self.unique = true;
        self
    }
}

/// A secondary (B-Tree) or vector (HNSW) index on one column.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexDef {
    /// Index name (also used in key encoding / HNSW snapshot name).
    pub name: String,
    /// Column / field name in the record.
    pub column: String,
    /// When set, this is an HNSW vector index (no `Idx_` B-Tree keys).
    pub vector: Option<VectorIndexSpec>,
}

impl IndexDef {
    /// Construct a B-Tree secondary index definition.
    pub fn new(name: impl Into<String>, column: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            column: column.into(),
            vector: None,
        }
    }

    /// Construct an HNSW vector index definition.
    pub fn vector(
        name: impl Into<String>,
        column: impl Into<String>,
        spec: VectorIndexSpec,
    ) -> Self {
        Self {
            name: name.into(),
            column: column.into(),
            vector: Some(spec),
        }
    }

    /// True when this is an HNSW / ANN index.
    pub fn is_vector(&self) -> bool {
        self.vector.is_some()
    }
}

/// Table schema: primary key field + secondary indexes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableSchema {
    /// Logical table name.
    pub name: String,
    /// Primary-key field name inside the record.
    pub primary_key: String,
    /// Declared columns from DDL (may be empty for legacy API-registered tables).
    pub columns: Vec<ColumnSpec>,
    /// Declared secondary indexes.
    pub indexes: Vec<IndexDef>,
    /// Physical storage engine (LSM default, or B-Tree).
    pub storage_engine: crate::storage::StorageEngineKind,
    /// Horizontal partitioning strategy (`None` = unreplicated single placement).
    pub partitioning: crate::partition::PartitioningStrategy,
    /// Partition id → owning cluster node id.
    pub partition_map: crate::partition::PartitionMap,
}

impl TableSchema {
    /// Construct a table schema (default LSM storage, no partitioning).
    pub fn new(
        name: impl Into<String>,
        primary_key: impl Into<String>,
        indexes: Vec<IndexDef>,
    ) -> Self {
        Self {
            name: name.into(),
            primary_key: primary_key.into(),
            columns: Vec::new(),
            indexes,
            storage_engine: crate::storage::StorageEngineKind::Lsm,
            partitioning: crate::partition::PartitioningStrategy::None,
            partition_map: crate::partition::PartitionMap::default(),
        }
    }

    /// Builder: attach typed column metadata from `CREATE TABLE`.
    pub fn with_columns(mut self, columns: Vec<ColumnSpec>) -> Self {
        self.columns = columns;
        self
    }

    /// Builder: select LSM or B-Tree storage for this table.
    pub fn with_engine(mut self, engine: crate::storage::StorageEngineKind) -> Self {
        self.storage_engine = engine;
        self
    }

    /// Builder: attach a hashing / range partitioning strategy.
    pub fn with_partitioning(
        mut self,
        strategy: crate::partition::PartitioningStrategy,
    ) -> Self {
        let n = strategy.partition_count();
        self.partitioning = strategy;
        if self.partition_map.assignments.is_empty() {
            self.partition_map = crate::partition::PartitionMap::round_robin(&[], n);
        }
        self
    }

    /// Builder: set explicit partition → node assignments.
    pub fn with_partition_map(mut self, map: crate::partition::PartitionMap) -> Self {
        self.partition_map = map;
        self
    }
}

/// Structured record: string field map (document projection).
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct Record {
    /// Field name → UTF-8 value.
    pub fields: BTreeMap<String, String>,
}

impl Record {
    /// Empty record.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set a field.
    pub fn set(mut self, field: impl Into<String>, value: impl Into<String>) -> Self {
        self.fields.insert(field.into(), value.into());
        self
    }

    /// Borrow a field value.
    pub fn get(&self, field: &str) -> Option<&str> {
        self.fields.get(field).map(String::as_str)
    }

    /// Encode for storage in the data key's value.
    pub fn encode(&self) -> Value {
        let mut buf = BytesMut::new();
        buf.put_u32_le(self.fields.len() as u32);
        for (k, v) in &self.fields {
            let kb = k.as_bytes();
            let vb = v.as_bytes();
            buf.put_u32_le(kb.len() as u32);
            buf.put_slice(kb);
            buf.put_u32_le(vb.len() as u32);
            buf.put_slice(vb);
        }
        Value::new(buf.freeze())
    }

    /// Decode a stored record value.
    pub fn decode(value: &Value) -> Result<Self> {
        let mut data = value.as_bytes();
        if data.len() < 4 {
            return Err(TakyonicError::Engine("record truncated".into()));
        }
        let n = u32::from_le_bytes(data[..4].try_into().unwrap()) as usize;
        data = &data[4..];
        let mut fields = BTreeMap::new();
        for _ in 0..n {
            if data.len() < 4 {
                return Err(TakyonicError::Engine("record key len truncated".into()));
            }
            let klen = u32::from_le_bytes(data[..4].try_into().unwrap()) as usize;
            data = &data[4..];
            if data.len() < klen + 4 {
                return Err(TakyonicError::Engine("record key truncated".into()));
            }
            let key = String::from_utf8(data[..klen].to_vec())
                .map_err(|e| TakyonicError::Engine(format!("record key utf8: {e}")))?;
            data = &data[klen..];
            let vlen = u32::from_le_bytes(data[..4].try_into().unwrap()) as usize;
            data = &data[4..];
            if data.len() < vlen {
                return Err(TakyonicError::Engine("record value truncated".into()));
            }
            let val = String::from_utf8(data[..vlen].to_vec())
                .map_err(|e| TakyonicError::Engine(format!("record value utf8: {e}")))?;
            data = &data[vlen..];
            fields.insert(key, val);
        }
        Ok(Self { fields })
    }
}

/// Build `Data_<table>_` prefix for full-table data scans.
pub fn data_table_prefix(table: &str) -> Bytes {
    let mut buf = BytesMut::with_capacity(DATA_PREFIX.len() + table.len() + 1);
    buf.put_slice(DATA_PREFIX);
    buf.put_slice(table.as_bytes());
    buf.put_u8(SEP);
    buf.freeze()
}

/// Build `Idx_<table>_` prefix for secondary-index Vacuum scans.
pub fn index_table_prefix(table: &str) -> Bytes {
    let mut buf = BytesMut::with_capacity(IDX_PREFIX.len() + table.len() + 1);
    buf.put_slice(IDX_PREFIX);
    buf.put_slice(table.as_bytes());
    buf.put_u8(SEP);
    buf.freeze()
}

/// Extract table name from a `Data_<table>_…` or `Idx_<table>_…` user key.
pub fn table_from_user_key(key: &Key) -> Option<String> {
    let bytes = key.as_bytes();
    let rest = bytes
        .strip_prefix(DATA_PREFIX)
        .or_else(|| bytes.strip_prefix(IDX_PREFIX))?;
    let end = rest.iter().position(|&b| b == SEP)?;
    Some(String::from_utf8_lossy(&rest[..end]).into_owned())
}

/// Build `Data_<table>_<pk>`.
pub fn data_key(table: &str, pk: &str) -> Key {
    let mut buf = BytesMut::with_capacity(DATA_PREFIX.len() + table.len() + 1 + pk.len());
    buf.put_slice(DATA_PREFIX);
    buf.put_slice(table.as_bytes());
    buf.put_u8(SEP);
    buf.put_slice(pk.as_bytes());
    Key::new(buf.freeze())
}

/// Extract the primary-key suffix from a `Data_<table>_<pk>` key.
pub fn pk_from_data_key(key: &Key, table: &str) -> Option<String> {
    let prefix = data_table_prefix(table);
    let bytes = key.as_bytes();
    if !bytes.starts_with(prefix.as_ref()) {
        return None;
    }
    String::from_utf8(bytes[prefix.len()..].to_vec()).ok()
}

/// Build `Idx_<table>_<index>_<value>_<pk>`.
pub fn index_key(table: &str, index: &str, value: &str, pk: &str) -> Key {
    let mut buf = BytesMut::with_capacity(
        IDX_PREFIX.len() + table.len() + index.len() + value.len() + pk.len() + 3,
    );
    buf.put_slice(IDX_PREFIX);
    buf.put_slice(table.as_bytes());
    buf.put_u8(SEP);
    buf.put_slice(index.as_bytes());
    buf.put_u8(SEP);
    buf.put_slice(value.as_bytes());
    buf.put_u8(SEP);
    buf.put_slice(pk.as_bytes());
    Key::new(buf.freeze())
}

/// Prefix for all index entries of `(table, index, value)` (equality probe).
pub fn index_eq_prefix(table: &str, index: &str, value: &str) -> Bytes {
    let mut buf = BytesMut::new();
    buf.put_slice(IDX_PREFIX);
    buf.put_slice(table.as_bytes());
    buf.put_u8(SEP);
    buf.put_slice(index.as_bytes());
    buf.put_u8(SEP);
    buf.put_slice(value.as_bytes());
    buf.put_u8(SEP);
    buf.freeze()
}

/// Prefix for the entire index column (range scans).
pub fn index_column_prefix(table: &str, index: &str) -> Bytes {
    let mut buf = BytesMut::new();
    buf.put_slice(IDX_PREFIX);
    buf.put_slice(table.as_bytes());
    buf.put_u8(SEP);
    buf.put_slice(index.as_bytes());
    buf.put_u8(SEP);
    buf.freeze()
}

/// Extract PK from an index key that starts with `prefix` (equality prefix).
pub fn pk_from_index_key(key: &Key, eq_prefix: &[u8]) -> Option<String> {
    let bytes = key.as_bytes();
    if !bytes.starts_with(eq_prefix) {
        return None;
    }
    String::from_utf8(bytes[eq_prefix.len()..].to_vec()).ok()
}

/// Parse index value + pk from a key under `index_column_prefix`.
///
/// Key layout after column prefix: `<value>_<pk>`.
pub fn parse_index_suffix(key: &Key, column_prefix: &[u8]) -> Option<(String, String)> {
    let bytes = key.as_bytes();
    if !bytes.starts_with(column_prefix) {
        return None;
    }
    let rest = &bytes[column_prefix.len()..];
    let sep = rest.iter().rposition(|&b| b == SEP)?;
    let value = String::from_utf8(rest[..sep].to_vec()).ok()?;
    let pk = String::from_utf8(rest[sep + 1..].to_vec()).ok()?;
    Some((value, pk))
}

/// Encode a numeric age (or similar) for lexicographic range order.
pub fn encode_sortable_int(n: i64) -> String {
    // Bias into unsigned domain for byte-wise ordering.
    format!("{:020}", n.wrapping_sub(i64::MIN) as u64)
}

/// Decode [`encode_sortable_int`].
pub fn decode_sortable_int(s: &str) -> Option<i64> {
    let u: u64 = s.parse().ok()?;
    Some(u.wrapping_add(i64::MIN as u64) as i64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_roundtrip() {
        let r = Record::new().set("id", "1").set("city", "Bursa");
        let decoded = Record::decode(&r.encode()).unwrap();
        assert_eq!(decoded.get("city"), Some("Bursa"));
    }

    #[test]
    fn index_key_prefix_extracts_pk() {
        let k = index_key("users", "city", "X", "42");
        let p = index_eq_prefix("users", "city", "X");
        assert_eq!(pk_from_index_key(&k, &p).as_deref(), Some("42"));
    }

    #[test]
    fn sortable_int_orders() {
        assert!(encode_sortable_int(25) < encode_sortable_int(26));
        assert_eq!(decode_sortable_int(&encode_sortable_int(-3)), Some(-3));
    }
}
