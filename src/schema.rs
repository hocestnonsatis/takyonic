//! Relational projection onto Takyonic's flat MVCC key space.
//!
//! **Data key:** `Data_<table>_<pk>` → serialized record  
//! **Index key:** `Idx_<table>_<index>_<value>_<pk>` → empty value (PK is in the key)

use std::collections::BTreeMap;

use bytes::{BufMut, Bytes, BytesMut};

use crate::error::{Result, TakyonicError};
use crate::types::{Key, Value};

const DATA_PREFIX: &[u8] = b"Data_";
const IDX_PREFIX: &[u8] = b"Idx_";
const SEP: u8 = b'_';

/// A secondary index on one column.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexDef {
    /// Index name (also used in key encoding).
    pub name: String,
    /// Column / field name in the record.
    pub column: String,
}

impl IndexDef {
    /// Construct an index definition.
    pub fn new(name: impl Into<String>, column: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            column: column.into(),
        }
    }
}

/// Table schema: primary key field + secondary indexes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableSchema {
    /// Logical table name.
    pub name: String,
    /// Primary-key field name inside the record.
    pub primary_key: String,
    /// Declared secondary indexes.
    pub indexes: Vec<IndexDef>,
}

impl TableSchema {
    /// Construct a table schema.
    pub fn new(
        name: impl Into<String>,
        primary_key: impl Into<String>,
        indexes: Vec<IndexDef>,
    ) -> Self {
        Self {
            name: name.into(),
            primary_key: primary_key.into(),
            indexes,
        }
    }
}

/// Structured record: string field map (document projection).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
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

/// Build `Data_<table>_<pk>`.
pub fn data_key(table: &str, pk: &str) -> Key {
    let mut buf = BytesMut::with_capacity(DATA_PREFIX.len() + table.len() + 1 + pk.len());
    buf.put_slice(DATA_PREFIX);
    buf.put_slice(table.as_bytes());
    buf.put_u8(SEP);
    buf.put_slice(pk.as_bytes());
    Key::new(buf.freeze())
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
