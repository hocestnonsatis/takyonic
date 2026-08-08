//! Role-Based Access Control (RBAC) and authorization.
//!
//! Durable catalog (`data_dir/AUTH`) stores:
//! - login roles (users) with Argon2id password hashes + SCRAM-SHA-256 credentials
//! - non-login roles
//! - role memberships
//! - table-level `GRANT` / `REVOKE` privileges
//!
//! [`AuthorizationManager`] validates [`crate::sql::LogicalPlan`] nodes against
//! an [`AuthContext`] (current user + inherited roles) before execution.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use argon2::{
    Argon2,
    password_hash::{
        PasswordHash, PasswordHasher, PasswordVerifier, SaltString,
        rand_core::{OsRng, RngCore},
    },
};
use parking_lot::RwLock;
use pgwire::api::auth::sasl::scram::gen_salted_password;

use crate::error::{Result, TakyonicError};
use crate::sql::LogicalPlan;

const AUTH_FILE: &str = "AUTH";
/// Bootstrap role used by the demo server and SCRAM integration tests.
pub const BOOTSTRAP_USER: &str = "postgres";
/// Cleartext password for [`BOOTSTRAP_USER`] (hashed at catalog seed time).
pub const BOOTSTRAP_PASSWORD: &str = "password";
const BOOTSTRAP_SALT: &[u8; 16] = b"takyonic-scram!!";
/// Default SCRAM PBKDF2 iteration count (matches pgwire).
pub const SCRAM_ITERATIONS: usize = 4096;

/// Pre-hashed SCRAM-SHA-256 credential (salt + PBKDF2 `SaltedPassword`).
#[derive(Clone, Debug)]
pub struct ScramCredential {
    /// Random (or fixed) salt bytes sent in server-first.
    pub salt: Vec<u8>,
    /// `Hi(Normalize(password), salt, iterations)` — pgwire derives StoredKey/ServerKey.
    pub salted_password: Vec<u8>,
}

impl ScramCredential {
    /// Hash a cleartext password into SCRAM storage form.
    pub fn from_password(password: &str, salt: &[u8], iterations: usize) -> Self {
        Self {
            salt: salt.to_vec(),
            salted_password: gen_salted_password(password, salt, iterations),
        }
    }
}

/// Table-level privilege bits.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Privilege {
    /// `SELECT`
    Select,
    /// `INSERT`
    Insert,
    /// `UPDATE`
    Update,
    /// `DELETE`
    Delete,
}

impl Privilege {
    /// Parse a privilege keyword.
    pub fn parse(s: &str) -> Result<Self> {
        match s.to_ascii_uppercase().as_str() {
            "SELECT" => Ok(Self::Select),
            "INSERT" => Ok(Self::Insert),
            "UPDATE" => Ok(Self::Update),
            "DELETE" => Ok(Self::Delete),
            // PG has a separate TRUNCATE privilege; map to Delete until GRANT TRUNCATE exists.
            "TRUNCATE" => Ok(Self::Delete),
            other => Err(TakyonicError::Sql(format!(
                "unrecognized privilege type: \"{other}\""
            ))),
        }
    }

    /// Parse a PG-style privilege list (`SELECT,INSERT` / optional `WITH GRANT OPTION`).
    pub fn parse_list(s: &str) -> Result<Vec<Self>> {
        let mut out = Vec::new();
        for part in s.split(',') {
            let mut p = part.trim();
            if p.is_empty() {
                continue;
            }
            let upper = p.to_ascii_uppercase();
            p = upper.trim();
            if let Some(stripped) = p.strip_suffix("WITH GRANT OPTION") {
                p = stripped.trim();
            }
            out.push(Self::parse(p)?);
        }
        if out.is_empty() {
            return Err(TakyonicError::Sql(
                "privilege list must contain at least one privilege".into(),
            ));
        }
        Ok(out)
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Select => "SELECT",
            Self::Insert => "INSERT",
            Self::Update => "UPDATE",
            Self::Delete => "DELETE",
        }
    }
}

/// Schema-level privilege for `has_schema_privilege` / `GRANT ON SCHEMA` (`CREATE` / `USAGE`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SchemaPrivilege {
    /// Create objects in the schema.
    Create,
    /// Look up / use objects in the schema.
    Usage,
}

impl SchemaPrivilege {
    /// Privilege keyword for AUTH serialization.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Create => "CREATE",
            Self::Usage => "USAGE",
        }
    }

    /// Parse a schema privilege keyword.
    pub fn parse(s: &str) -> Result<Self> {
        match s.to_ascii_uppercase().as_str() {
            "CREATE" => Ok(Self::Create),
            "USAGE" => Ok(Self::Usage),
            other => Err(TakyonicError::Sql(format!(
                "unrecognized privilege type: \"{other}\""
            ))),
        }
    }

    /// Parse a PG-style privilege list (`USAGE,CREATE` / optional `WITH GRANT OPTION`).
    pub fn parse_list(s: &str) -> Result<Vec<Self>> {
        let mut out = Vec::new();
        for part in s.split(',') {
            let mut p = part.trim();
            if p.is_empty() {
                continue;
            }
            let upper = p.to_ascii_uppercase();
            p = upper.trim();
            if let Some(stripped) = p.strip_suffix("WITH GRANT OPTION") {
                p = stripped.trim();
            }
            out.push(Self::parse(p)?);
        }
        if out.is_empty() {
            return Err(TakyonicError::Sql(
                "privilege list must contain at least one privilege".into(),
            ));
        }
        Ok(out)
    }
}

/// Normalize a schema identifier (`public`, `"Foo"`, `pg_catalog.public` → leaf).
pub fn schema_name_leaf(schema: &str) -> String {
    schema
        .rsplit('.')
        .next()
        .unwrap_or(schema)
        .trim()
        .trim_matches('"')
        .to_ascii_lowercase()
}

/// Builtin / search_path schemas currently recognized without a schema catalog.
pub fn schema_exists(schema: &str, search_path: &str) -> bool {
    let leaf = schema_name_leaf(schema);
    matches!(
        leaf.as_str(),
        "public" | "pg_catalog" | "information_schema"
    ) || search_path
        .split(',')
        .any(|p| schema_name_leaf(p) == leaf)
}

/// Default schema ACL (plus optional `GRANT ON SCHEMA` rows in AUTH):
/// - superuser: all
/// - others: `USAGE` on `public` / `pg_catalog` / `information_schema`; no `CREATE`
/// - additional `USAGE` / `CREATE` via [`AuthCatalog`] schema grants
pub fn has_schema_privilege(
    is_superuser: bool,
    schema: &str,
    search_path: &str,
    privs: &[SchemaPrivilege],
) -> Result<bool> {
    let leaf = schema_name_leaf(schema);
    if !schema_exists(&leaf, search_path) {
        return Err(TakyonicError::Sql(format!(
            "schema \"{leaf}\" does not exist"
        )));
    }
    Ok(privs
        .iter()
        .any(|p| has_one_schema_privilege(is_superuser, &leaf, *p)))
}

fn has_one_schema_privilege(is_superuser: bool, leaf: &str, priv_: SchemaPrivilege) -> bool {
    if is_superuser {
        return true;
    }
    match priv_ {
        SchemaPrivilege::Usage => {
            matches!(leaf, "public" | "pg_catalog" | "information_schema")
        }
        SchemaPrivilege::Create => false,
    }
}

/// Database-level privilege for `has_database_privilege` (`CREATE` / `CONNECT` / `TEMP`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DatabasePrivilege {
    /// Create schemas / publications in the database.
    Create,
    /// Connect to the database.
    Connect,
    /// Create temporary tables.
    Temporary,
}

impl DatabasePrivilege {
    /// Parse a database privilege keyword (`TEMP` ≡ `TEMPORARY`).
    pub fn parse(s: &str) -> Result<Self> {
        match s.to_ascii_uppercase().as_str() {
            "CREATE" => Ok(Self::Create),
            "CONNECT" => Ok(Self::Connect),
            "TEMP" | "TEMPORARY" => Ok(Self::Temporary),
            other => Err(TakyonicError::Sql(format!(
                "unrecognized privilege type: \"{other}\""
            ))),
        }
    }

    /// Parse a PG-style privilege list (`CONNECT,CREATE` / optional `WITH GRANT OPTION`).
    pub fn parse_list(s: &str) -> Result<Vec<Self>> {
        let mut out = Vec::new();
        for part in s.split(',') {
            let mut p = part.trim();
            if p.is_empty() {
                continue;
            }
            let upper = p.to_ascii_uppercase();
            p = upper.trim();
            if let Some(stripped) = p.strip_suffix("WITH GRANT OPTION") {
                p = stripped.trim();
            }
            out.push(Self::parse(p)?);
        }
        if out.is_empty() {
            return Err(TakyonicError::Sql(
                "privilege list must contain at least one privilege".into(),
            ));
        }
        Ok(out)
    }
}

/// Normalize a database name (`"Postgres"` → `postgres`).
pub fn database_name_leaf(name: &str) -> String {
    name.trim().trim_matches('"').to_ascii_lowercase()
}

/// Takyonic is single-database: only [`crate::pg::DEFAULT_DATABASE`] (`postgres`) exists.
pub fn database_exists(name: &str) -> bool {
    database_name_leaf(name) == "postgres"
}

/// Default database ACL (no `GRANT ON DATABASE` catalog yet):
/// - superuser: all
/// - others: `CONNECT` on `postgres`; no `CREATE` / `TEMPORARY`
pub fn has_database_privilege(
    is_superuser: bool,
    database: &str,
    privs: &[DatabasePrivilege],
) -> Result<bool> {
    let leaf = database_name_leaf(database);
    if !database_exists(&leaf) {
        return Err(TakyonicError::Sql(format!(
            "database \"{leaf}\" does not exist"
        )));
    }
    Ok(privs
        .iter()
        .any(|p| has_one_database_privilege(is_superuser, *p)))
}

fn has_one_database_privilege(is_superuser: bool, priv_: DatabasePrivilege) -> bool {
    if is_superuser {
        return true;
    }
    match priv_ {
        DatabasePrivilege::Connect => true,
        DatabasePrivilege::Create | DatabasePrivilege::Temporary => false,
    }
}

/// Tablespace-level privilege for `has_tablespace_privilege` (`CREATE`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TablespacePrivilege {
    /// Create objects in the tablespace.
    Create,
}

impl TablespacePrivilege {
    /// Privilege keyword.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Create => "CREATE",
        }
    }

    /// Parse a tablespace privilege keyword.
    pub fn parse(s: &str) -> Result<Self> {
        match s.to_ascii_uppercase().as_str() {
            "CREATE" | "ALL" | "ALL PRIVILEGES" => Ok(Self::Create),
            other => Err(TakyonicError::Sql(format!(
                "unrecognized privilege type: \"{other}\""
            ))),
        }
    }

    /// Parse a PG-style privilege list.
    pub fn parse_list(s: &str) -> Result<Vec<Self>> {
        let mut out = Vec::new();
        for part in s.split(',') {
            let mut p = part.trim();
            if p.is_empty() {
                continue;
            }
            let upper = p.to_ascii_uppercase();
            p = upper.trim();
            if let Some(stripped) = p.strip_suffix("WITH GRANT OPTION") {
                p = stripped.trim();
            }
            out.push(Self::parse(p)?);
        }
        if out.is_empty() {
            return Err(TakyonicError::Sql(
                "privilege list must contain at least one privilege".into(),
            ));
        }
        Ok(out)
    }
}

/// Leaf tablespace name (`pg_catalog.pg_default` → `pg_default`).
pub fn tablespace_name_leaf(name: &str) -> String {
    name.trim()
        .trim_matches('"')
        .rsplit('.')
        .next()
        .unwrap_or(name)
        .trim_matches('"')
        .to_ascii_lowercase()
}

/// Built-in tablespaces: `pg_default` (and alias `pg_global` as catalog name only).
pub fn tablespace_exists(name: &str) -> bool {
    matches!(tablespace_name_leaf(name).as_str(), "pg_default" | "pg_global")
}

/// PostgreSQL built-in tablespace OIDs (`pg_default`=1663, `pg_global`=1664).
pub fn tablespace_name_for_oid(oid: i64) -> Option<&'static str> {
    match oid {
        1663 => Some("pg_default"),
        1664 => Some("pg_global"),
        _ => None,
    }
}

/// `pg_tablespace_location(name|oid)` — path string; empty for built-in tablespaces.
pub fn pg_tablespace_location(arg: &str) -> Result<String> {
    let trimmed = arg.trim().trim_matches('"');
    if trimmed.is_empty() {
        return Ok(String::new());
    }
    if let Ok(oid) = trimmed.parse::<i64>() {
        let Some(name) = tablespace_name_for_oid(oid) else {
            return Err(TakyonicError::Sql(format!(
                "tablespace with OID {oid} does not exist"
            )));
        };
        let _ = name;
        return Ok(String::new());
    }
    let leaf = tablespace_name_leaf(trimmed);
    if !tablespace_exists(&leaf) {
        return Err(TakyonicError::Sql(format!(
            "tablespace \"{leaf}\" does not exist"
        )));
    }
    // Built-in tablespaces have no absolute path in PostgreSQL either.
    Ok(String::new())
}

/// Default tablespace ACL (no `GRANT ON TABLESPACE` yet):
/// - superuser: `CREATE` on known tablespaces
/// - others: no `CREATE`
pub fn has_tablespace_privilege(
    is_superuser: bool,
    tablespace: &str,
    privs: &[TablespacePrivilege],
) -> Result<bool> {
    let leaf = tablespace_name_leaf(tablespace);
    if !tablespace_exists(&leaf) {
        return Err(TakyonicError::Sql(format!(
            "tablespace \"{leaf}\" does not exist"
        )));
    }
    Ok(privs.iter().any(|p| {
        matches!(p, TablespacePrivilege::Create) && is_superuser
    }))
}

/// Function-level privilege for `has_function_privilege` (`EXECUTE`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum FunctionPrivilege {
    /// Call / execute the function.
    Execute,
}

impl FunctionPrivilege {
    /// Privilege keyword.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Execute => "EXECUTE",
        }
    }

    /// Parse a function privilege keyword.
    pub fn parse(s: &str) -> Result<Self> {
        match s.to_ascii_uppercase().as_str() {
            "EXECUTE" | "ALL" | "ALL PRIVILEGES" => Ok(Self::Execute),
            other => Err(TakyonicError::Sql(format!(
                "unrecognized privilege type: \"{other}\""
            ))),
        }
    }

    /// Parse a PG-style privilege list.
    pub fn parse_list(s: &str) -> Result<Vec<Self>> {
        let mut out = Vec::new();
        for part in s.split(',') {
            let mut p = part.trim();
            if p.is_empty() {
                continue;
            }
            let upper = p.to_ascii_uppercase();
            p = upper.trim();
            if let Some(stripped) = p.strip_suffix("WITH GRANT OPTION") {
                p = stripped.trim();
            }
            out.push(Self::parse(p)?);
        }
        if out.is_empty() {
            return Err(TakyonicError::Sql(
                "privilege list must contain at least one privilege".into(),
            ));
        }
        Ok(out)
    }
}

/// Normalize `has_function_privilege` function identity to a bare name.
///
/// Accepts `format_type`, `pg_catalog.format_type`, or `format_type(regtype,integer)`.
pub fn function_name_leaf(spec: &str) -> String {
    let before_paren = spec
        .split('(')
        .next()
        .unwrap_or(spec)
        .trim()
        .trim_matches('"');
    before_paren
        .rsplit('.')
        .next()
        .unwrap_or(before_paren)
        .trim()
        .to_ascii_lowercase()
}

/// Default function ACL: superuser all; others `EXECUTE` only on known SQL scalars.
pub fn has_function_privilege(
    is_superuser: bool,
    function: &str,
    privs: &[FunctionPrivilege],
    is_known: impl Fn(&str) -> bool,
) -> bool {
    if is_superuser {
        return true;
    }
    let leaf = function_name_leaf(function);
    if leaf.is_empty() {
        return false;
    }
    privs.iter().any(|p| match p {
        FunctionPrivilege::Execute => is_known(&leaf),
    })
}

/// Role-membership privilege for `pg_has_role` (`MEMBER` / `USAGE` / `SET`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RolePrivilege {
    /// Direct or inherited role membership.
    Member,
    /// Privileges of the role are available (`INHERIT`; treated as membership here).
    Usage,
    /// May `SET ROLE` to the role (treated as membership until SET flags exist).
    Set,
}

impl RolePrivilege {
    /// Privilege keyword.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Member => "MEMBER",
            Self::Usage => "USAGE",
            Self::Set => "SET",
        }
    }

    /// Parse a `pg_has_role` privilege keyword.
    pub fn parse(s: &str) -> Result<Self> {
        match s.to_ascii_uppercase().as_str() {
            "MEMBER" => Ok(Self::Member),
            "USAGE" => Ok(Self::Usage),
            "SET" => Ok(Self::Set),
            other => Err(TakyonicError::Sql(format!(
                "unrecognized privilege type: \"{other}\""
            ))),
        }
    }

    /// Parse a PG-style privilege list (`MEMBER, USAGE`).
    pub fn parse_list(s: &str) -> Result<Vec<Self>> {
        let mut out = Vec::new();
        for part in s.split(',') {
            let mut p = part.trim();
            if p.is_empty() {
                continue;
            }
            let upper = p.to_ascii_uppercase();
            p = upper.trim();
            if let Some(stripped) = p.strip_suffix("WITH ADMIN OPTION") {
                p = stripped.trim();
            } else if let Some(stripped) = p.strip_suffix("WITH GRANT OPTION") {
                p = stripped.trim();
            }
            out.push(Self::parse(p)?);
        }
        if out.is_empty() {
            return Err(TakyonicError::Sql(
                "privilege list must contain at least one privilege".into(),
            ));
        }
        Ok(out)
    }
}

/// Normalize a role identity for `pg_has_role` (`"Analyst"` → `analyst`).
pub fn role_name_leaf(spec: &str) -> String {
    spec.trim()
        .trim_matches('"')
        .rsplit('.')
        .next()
        .unwrap_or(spec)
        .trim()
        .to_ascii_lowercase()
}

/// Type-level privilege for `has_type_privilege` (`USAGE`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TypePrivilege {
    /// Use the type in table definitions / casts.
    Usage,
}

impl TypePrivilege {
    /// Privilege keyword.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Usage => "USAGE",
        }
    }

    /// Parse a type privilege keyword.
    pub fn parse(s: &str) -> Result<Self> {
        match s.to_ascii_uppercase().as_str() {
            "USAGE" | "ALL" | "ALL PRIVILEGES" => Ok(Self::Usage),
            other => Err(TakyonicError::Sql(format!(
                "unrecognized privilege type: \"{other}\""
            ))),
        }
    }

    /// Parse a PG-style privilege list.
    pub fn parse_list(s: &str) -> Result<Vec<Self>> {
        let mut out = Vec::new();
        for part in s.split(',') {
            let mut p = part.trim();
            if p.is_empty() {
                continue;
            }
            let upper = p.to_ascii_uppercase();
            p = upper.trim();
            if let Some(stripped) = p.strip_suffix("WITH GRANT OPTION") {
                p = stripped.trim();
            }
            out.push(Self::parse(p)?);
        }
        if out.is_empty() {
            return Err(TakyonicError::Sql(
                "privilege list must contain at least one privilege".into(),
            ));
        }
        Ok(out)
    }
}

/// Normalize `has_type_privilege` type identity to a bare name.
pub fn type_name_leaf(spec: &str) -> String {
    let s = spec.trim().trim_matches('"');
    s.rsplit('.')
        .next()
        .unwrap_or(s)
        .trim()
        .to_ascii_lowercase()
}

/// Default type ACL: superuser all; others `USAGE` on every known type.
pub fn has_type_privilege(
    is_superuser: bool,
    type_spec: &str,
    privs: &[TypePrivilege],
    type_exists: impl Fn(&str) -> bool,
) -> Result<bool> {
    let leaf = type_name_leaf(type_spec);
    if leaf.is_empty() || !type_exists(&leaf) {
        return Err(TakyonicError::Sql(format!(
            "type \"{leaf}\" does not exist"
        )));
    }
    if is_superuser {
        return Ok(true);
    }
    // Public USAGE on catalog types (no per-type GRANT yet).
    Ok(privs.iter().any(|p| match p {
        TypePrivilege::Usage => true,
    }))
}

/// Sequence-level privilege for `has_sequence_privilege` (`USAGE` / `SELECT` / `UPDATE`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SequencePrivilege {
    /// Use `nextval` / `currval` / `setval`.
    Usage,
    /// Read sequence metadata / `currval`-style access.
    Select,
    /// Advance / set the sequence (`nextval` / `setval`).
    Update,
}

impl SequencePrivilege {
    /// Privilege keyword.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Usage => "USAGE",
            Self::Select => "SELECT",
            Self::Update => "UPDATE",
        }
    }

    /// Parse a sequence privilege keyword.
    pub fn parse(s: &str) -> Result<Self> {
        match s.to_ascii_uppercase().as_str() {
            "USAGE" => Ok(Self::Usage),
            "SELECT" => Ok(Self::Select),
            "UPDATE" => Ok(Self::Update),
            "ALL" | "ALL PRIVILEGES" => Ok(Self::Usage), // ALL expands in parse_list
            other => Err(TakyonicError::Sql(format!(
                "unrecognized privilege type: \"{other}\""
            ))),
        }
    }

    /// Parse a PG-style privilege list (`USAGE, SELECT`).
    pub fn parse_list(s: &str) -> Result<Vec<Self>> {
        let mut out = Vec::new();
        for part in s.split(',') {
            let mut p = part.trim();
            if p.is_empty() {
                continue;
            }
            let upper = p.to_ascii_uppercase();
            p = upper.trim();
            if let Some(stripped) = p.strip_suffix("WITH GRANT OPTION") {
                p = stripped.trim();
            }
            if matches!(p, "ALL" | "ALL PRIVILEGES") {
                out.extend([Self::Usage, Self::Select, Self::Update]);
                continue;
            }
            out.push(Self::parse(p)?);
        }
        if out.is_empty() {
            return Err(TakyonicError::Sql(
                "privilege list must contain at least one privilege".into(),
            ));
        }
        out.sort();
        out.dedup();
        Ok(out)
    }
}

/// Default sequence ACL: superuser all; others `USAGE`/`SELECT`/`UPDATE` on existing sequences.
pub fn has_sequence_privilege(
    is_superuser: bool,
    sequence: &str,
    privs: &[SequencePrivilege],
    sequence_exists: impl Fn(&str) -> bool,
) -> Result<bool> {
    let leaf = type_name_leaf(sequence); // same schema-strip / lowercase
    if leaf.is_empty() || !sequence_exists(&leaf) {
        return Err(TakyonicError::Sql(format!(
            "relation \"{leaf}\" does not exist"
        )));
    }
    if is_superuser {
        return Ok(true);
    }
    // Public full sequence ACL until GRANT ON SEQUENCE exists.
    Ok(privs.iter().any(|p| {
        matches!(
            p,
            SequencePrivilege::Usage | SequencePrivilege::Select | SequencePrivilege::Update
        )
    }))
}

/// Column-level privilege for `has_column_privilege` (`SELECT` / `INSERT` / `UPDATE` / `REFERENCES`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ColumnPrivilege {
    /// Read column values.
    Select,
    /// Insert into the column.
    Insert,
    /// Update the column.
    Update,
    /// Create FK references to the column.
    References,
}

impl ColumnPrivilege {
    /// Privilege keyword for AUTH serialization.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Select => "SELECT",
            Self::Insert => "INSERT",
            Self::Update => "UPDATE",
            Self::References => "REFERENCES",
        }
    }

    /// Parse a column privilege keyword.
    pub fn parse(s: &str) -> Result<Self> {
        match s.to_ascii_uppercase().as_str() {
            "SELECT" => Ok(Self::Select),
            "INSERT" => Ok(Self::Insert),
            "UPDATE" => Ok(Self::Update),
            "REFERENCES" => Ok(Self::References),
            other => Err(TakyonicError::Sql(format!(
                "unrecognized privilege type: \"{other}\""
            ))),
        }
    }

    /// Parse a PG-style privilege list (`SELECT,UPDATE` / optional `WITH GRANT OPTION`).
    pub fn parse_list(s: &str) -> Result<Vec<Self>> {
        let mut out = Vec::new();
        for part in s.split(',') {
            let mut p = part.trim();
            if p.is_empty() {
                continue;
            }
            let upper = p.to_ascii_uppercase();
            p = upper.trim();
            if let Some(stripped) = p.strip_suffix("WITH GRANT OPTION") {
                p = stripped.trim();
            }
            out.push(Self::parse(p)?);
        }
        if out.is_empty() {
            return Err(TakyonicError::Sql(
                "privilege list must contain at least one privilege".into(),
            ));
        }
        Ok(out)
    }

    fn as_table_privilege(self) -> Option<Privilege> {
        match self {
            Self::Select => Some(Privilege::Select),
            Self::Insert => Some(Privilege::Insert),
            Self::Update => Some(Privilege::Update),
            Self::References => None,
        }
    }
}

/// One column-targeted privilege in `GRANT SELECT (col) ON …`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnGrantSpec {
    /// Privilege being granted/revoked.
    pub privilege: ColumnPrivilege,
    /// Column names (lowercased at grant time).
    pub columns: Vec<String>,
}

/// A role or login user in the auth catalog.
#[derive(Clone, Debug)]
pub struct RoleDef {
    /// Role / user name.
    pub name: String,
    /// May authenticate via PgWire (`CREATE USER` / `LOGIN`).
    pub can_login: bool,
    /// Bypass all privilege checks; may run DDL / VACUUM / ANALYZE.
    pub is_superuser: bool,
    /// Argon2id PHC string (only for login roles with passwords).
    pub argon2_hash: Option<String>,
    /// SCRAM-SHA-256 storage for the wire protocol.
    pub scram: Option<ScramCredential>,
}

/// Session authorization snapshot after authentication.
#[derive(Clone, Debug)]
pub struct AuthContext {
    /// Authenticated role name.
    pub user: String,
    /// Direct + inherited role names (includes `user`).
    pub roles: BTreeSet<String>,
    /// True when `user` (or an inherited role) is a superuser.
    pub is_superuser: bool,
}

impl AuthContext {
    /// Bootstrap superuser context (`postgres`).
    pub fn superuser() -> Self {
        let mut roles = BTreeSet::new();
        roles.insert(BOOTSTRAP_USER.to_string());
        Self {
            user: BOOTSTRAP_USER.to_string(),
            roles,
            is_superuser: true,
        }
    }

    /// Unauthenticated / anonymous (no privileges).
    pub fn anonymous() -> Self {
        Self {
            user: String::new(),
            roles: BTreeSet::new(),
            is_superuser: false,
        }
    }
}

/// Durable in-memory RBAC catalog.
#[derive(Clone, Debug, Default)]
pub struct AuthCatalog {
    roles: BTreeMap<String, RoleDef>,
    /// `member` → set of role names granted to them.
    memberships: BTreeMap<String, BTreeSet<String>>,
    /// `(grantee, table)` → privileges.
    grants: BTreeMap<(String, String), BTreeSet<Privilege>>,
    /// `(grantee, schema)` → schema privileges (`GRANT ON SCHEMA`).
    schema_grants: BTreeMap<(String, String), BTreeSet<SchemaPrivilege>>,
    /// `(grantee, table, column)` → column privileges (`GRANT SELECT (col) ON …`).
    column_grants: BTreeMap<(String, String, String), BTreeSet<ColumnPrivilege>>,
}

impl AuthCatalog {
    /// Empty catalog (caller should seed bootstrap).
    pub fn new() -> Self {
        Self::default()
    }

    /// Catalog with the bootstrap `postgres` superuser.
    pub fn with_bootstrap() -> Self {
        let mut cat = Self::new();
        let scram = ScramCredential::from_password(
            BOOTSTRAP_PASSWORD,
            BOOTSTRAP_SALT,
            SCRAM_ITERATIONS,
        );
        let argon2_hash = hash_password(BOOTSTRAP_PASSWORD).expect("argon2 bootstrap");
        cat.roles.insert(
            BOOTSTRAP_USER.to_string(),
            RoleDef {
                name: BOOTSTRAP_USER.to_string(),
                can_login: true,
                is_superuser: true,
                argon2_hash: Some(argon2_hash),
                scram: Some(scram),
            },
        );
        cat
    }

    /// Path to the durable AUTH file.
    pub fn path(data_dir: &Path) -> PathBuf {
        data_dir.join(AUTH_FILE)
    }

    /// Load from disk, or seed bootstrap when missing.
    pub fn load(data_dir: &Path) -> Result<Self> {
        let path = Self::path(data_dir);
        if !path.exists() {
            let cat = Self::with_bootstrap();
            cat.save(data_dir)?;
            return Ok(cat);
        }
        let text = fs::read_to_string(&path)?;
        let cat = Self::parse(&text)?;
        if cat.roles.is_empty() {
            let cat = Self::with_bootstrap();
            cat.save(data_dir)?;
            return Ok(cat);
        }
        Ok(cat)
    }

    /// Parse AUTH catalog text (same format as the durable file).
    pub fn parse(text: &str) -> Result<Self> {
        let mut cat = Self::new();
        for (lineno, raw) in text.lines().enumerate() {
            let line = raw.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let mut parts = line.split('\t');
            let tag = parts.next().unwrap_or("");
            match tag {
                "ROLE" => {
                    let name = parts.next().unwrap_or("").to_string();
                    let can_login = parts.next() == Some("1");
                    let is_superuser = parts.next() == Some("1");
                    let argon = parts.next().filter(|s| !s.is_empty()).map(str::to_string);
                    let salt_b64 = parts.next().unwrap_or("");
                    let key_b64 = parts.next().unwrap_or("");
                    let scram = if salt_b64.is_empty() || key_b64.is_empty() {
                        None
                    } else {
                        Some(ScramCredential {
                            salt: b64_decode(salt_b64)?,
                            salted_password: b64_decode(key_b64)?,
                        })
                    };
                    cat.roles.insert(
                        name.clone(),
                        RoleDef {
                            name,
                            can_login,
                            is_superuser,
                            argon2_hash: argon,
                            scram,
                        },
                    );
                }
                "MEMBER" => {
                    let member = parts.next().unwrap_or("").to_string();
                    let role = parts.next().unwrap_or("").to_string();
                    cat.memberships
                        .entry(member)
                        .or_default()
                        .insert(role);
                }
                "GRANT" => {
                    let grantee = parts.next().unwrap_or("").to_string();
                    let table = parts.next().unwrap_or("").to_string();
                    let priv_s = parts.next().unwrap_or("");
                    let privilege = Privilege::parse(priv_s)?;
                    cat.grants
                        .entry((grantee, table))
                        .or_default()
                        .insert(privilege);
                }
                "SGRANT" => {
                    let grantee = parts.next().unwrap_or("").to_string();
                    let schema = schema_name_leaf(parts.next().unwrap_or(""));
                    let priv_s = parts.next().unwrap_or("");
                    let privilege = SchemaPrivilege::parse(priv_s)?;
                    cat.schema_grants
                        .entry((grantee, schema))
                        .or_default()
                        .insert(privilege);
                }
                "CGRANT" => {
                    let grantee = parts.next().unwrap_or("").to_string();
                    let table = table_name_leaf(parts.next().unwrap_or("")).to_string();
                    let column = parts.next().unwrap_or("").trim().to_ascii_lowercase();
                    let priv_s = parts.next().unwrap_or("");
                    let privilege = ColumnPrivilege::parse(priv_s)?;
                    cat.column_grants
                        .entry((grantee, table, column))
                        .or_default()
                        .insert(privilege);
                }
                other => {
                    return Err(TakyonicError::Engine(format!(
                        "AUTH line {}: unknown tag `{other}`",
                        lineno + 1
                    )));
                }
            }
        }
        Ok(cat)
    }

    /// Serialize to AUTH file text (Raft `AuthReplace` payload).
    pub fn encode(&self) -> String {
        let mut out = String::from("# Takyonic AUTH catalog\n");
        for role in self.roles.values() {
            let argon = role.argon2_hash.as_deref().unwrap_or("");
            let (salt, key) = match &role.scram {
                Some(s) => (b64_encode(&s.salt), b64_encode(&s.salted_password)),
                None => (String::new(), String::new()),
            };
            out.push_str(&format!(
                "ROLE\t{}\t{}\t{}\t{}\t{}\t{}\n",
                role.name,
                if role.can_login { "1" } else { "0" },
                if role.is_superuser { "1" } else { "0" },
                argon,
                salt,
                key
            ));
        }
        for (member, roles) in &self.memberships {
            for role in roles {
                out.push_str(&format!("MEMBER\t{member}\t{role}\n"));
            }
        }
        for ((grantee, table), privs) in &self.grants {
            for p in privs {
                out.push_str(&format!("GRANT\t{grantee}\t{table}\t{}\n", p.as_str()));
            }
        }
        for ((grantee, schema), privs) in &self.schema_grants {
            for p in privs {
                out.push_str(&format!("SGRANT\t{grantee}\t{schema}\t{}\n", p.as_str()));
            }
        }
        for ((grantee, table, column), privs) in &self.column_grants {
            for p in privs {
                out.push_str(&format!(
                    "CGRANT\t{grantee}\t{table}\t{column}\t{}\n",
                    p.as_str()
                ));
            }
        }
        out
    }

    /// Atomically rewrite the AUTH file.
    pub fn save(&self, data_dir: &Path) -> Result<()> {
        fs::create_dir_all(data_dir)?;
        let path = Self::path(data_dir);
        let tmp = data_dir.join(format!("{AUTH_FILE}.tmp"));
        {
            let mut f = fs::File::create(&tmp)?;
            f.write_all(self.encode().as_bytes())?;
            f.sync_all()?;
        }
        fs::rename(&tmp, &path)?;
        Ok(())
    }

    /// Look up a role definition.
    pub fn get_role(&self, name: &str) -> Option<&RoleDef> {
        self.roles.get(name)
    }

    /// All role names.
    pub fn role_names(&self) -> impl Iterator<Item = &str> {
        self.roles.keys().map(String::as_str)
    }

    /// Create a role / user.
    pub fn create_role(
        &mut self,
        name: &str,
        can_login: bool,
        is_superuser: bool,
        password: Option<&str>,
        if_not_exists: bool,
    ) -> Result<()> {
        if self.roles.contains_key(name) {
            if if_not_exists {
                return Ok(());
            }
            return Err(TakyonicError::Sql(format!("role `{name}` already exists")));
        }
        let (argon2_hash, scram) = if let Some(pwd) = password {
            let argon = hash_password(pwd)?;
            let mut salt = [0u8; 16];
            OsRng.fill_bytes(&mut salt);
            let scram = ScramCredential::from_password(pwd, &salt, SCRAM_ITERATIONS);
            (Some(argon), Some(scram))
        } else {
            (None, None)
        };
        if can_login && password.is_none() {
            return Err(TakyonicError::Sql(
                "login role requires PASSWORD".into(),
            ));
        }
        self.roles.insert(
            name.to_string(),
            RoleDef {
                name: name.to_string(),
                can_login,
                is_superuser,
                argon2_hash,
                scram,
            },
        );
        Ok(())
    }

    /// Drop a role / user.
    pub fn drop_role(&mut self, name: &str, if_exists: bool) -> Result<()> {
        if name == BOOTSTRAP_USER {
            return Err(TakyonicError::Sql(
                "cannot drop the bootstrap superuser".into(),
            ));
        }
        if !self.roles.contains_key(name) {
            if if_exists {
                return Ok(());
            }
            return Err(TakyonicError::Sql(format!("role `{name}` does not exist")));
        }
        self.roles.remove(name);
        self.memberships.remove(name);
        for members in self.memberships.values_mut() {
            members.remove(name);
        }
        self.grants.retain(|(g, _), _| g != name);
        Ok(())
    }

    /// Grant role membership (`GRANT role TO member`).
    pub fn grant_membership(&mut self, role: &str, member: &str) -> Result<()> {
        if !self.roles.contains_key(role) {
            return Err(TakyonicError::Sql(format!("role `{role}` does not exist")));
        }
        if !self.roles.contains_key(member) {
            return Err(TakyonicError::Sql(format!(
                "role `{member}` does not exist"
            )));
        }
        self.memberships
            .entry(member.to_string())
            .or_default()
            .insert(role.to_string());
        Ok(())
    }

    /// Grant table privileges.
    pub fn grant(
        &mut self,
        grantee: &str,
        table: &str,
        privileges: &[Privilege],
    ) -> Result<()> {
        if !self.roles.contains_key(grantee) {
            return Err(TakyonicError::Sql(format!(
                "role `{grantee}` does not exist"
            )));
        }
        let entry = self
            .grants
            .entry((grantee.to_string(), table.to_string()))
            .or_default();
        for p in privileges {
            entry.insert(*p);
        }
        Ok(())
    }

    /// Revoke table privileges.
    pub fn revoke(
        &mut self,
        grantee: &str,
        table: &str,
        privileges: &[Privilege],
    ) -> Result<()> {
        let key = (grantee.to_string(), table.to_string());
        if let Some(entry) = self.grants.get_mut(&key) {
            for p in privileges {
                entry.remove(p);
            }
            if entry.is_empty() {
                self.grants.remove(&key);
            }
        }
        Ok(())
    }

    /// `GRANT … ON SCHEMA` privileges.
    pub fn grant_schema(
        &mut self,
        grantee: &str,
        schema: &str,
        privileges: &[SchemaPrivilege],
    ) -> Result<()> {
        if !self.roles.contains_key(grantee) {
            return Err(TakyonicError::Sql(format!(
                "role `{grantee}` does not exist"
            )));
        }
        let leaf = schema_name_leaf(schema);
        if !schema_exists(&leaf, "public") {
            return Err(TakyonicError::Sql(format!(
                "schema \"{leaf}\" does not exist"
            )));
        }
        let entry = self
            .schema_grants
            .entry((grantee.to_string(), leaf))
            .or_default();
        for p in privileges {
            entry.insert(*p);
        }
        Ok(())
    }

    /// `REVOKE … ON SCHEMA` privileges.
    pub fn revoke_schema(
        &mut self,
        grantee: &str,
        schema: &str,
        privileges: &[SchemaPrivilege],
    ) -> Result<()> {
        let key = (grantee.to_string(), schema_name_leaf(schema));
        if let Some(entry) = self.schema_grants.get_mut(&key) {
            for p in privileges {
                entry.remove(p);
            }
            if entry.is_empty() {
                self.schema_grants.remove(&key);
            }
        }
        Ok(())
    }

    /// `GRANT SELECT (col) ON table` (and INSERT/UPDATE/REFERENCES column lists).
    pub fn grant_columns(
        &mut self,
        grantee: &str,
        table: &str,
        specs: &[ColumnGrantSpec],
    ) -> Result<()> {
        if !self.roles.contains_key(grantee) {
            return Err(TakyonicError::Sql(format!(
                "role `{grantee}` does not exist"
            )));
        }
        let table = table_name_leaf(table).to_string();
        for spec in specs {
            for col in &spec.columns {
                let column = col.trim().to_ascii_lowercase();
                if column.is_empty() {
                    return Err(TakyonicError::Sql(
                        "GRANT column list must not contain empty names".into(),
                    ));
                }
                self.column_grants
                    .entry((grantee.to_string(), table.clone(), column))
                    .or_default()
                    .insert(spec.privilege);
            }
        }
        Ok(())
    }

    /// `REVOKE SELECT (col) ON table`.
    pub fn revoke_columns(
        &mut self,
        grantee: &str,
        table: &str,
        specs: &[ColumnGrantSpec],
    ) -> Result<()> {
        let table = table_name_leaf(table).to_string();
        for spec in specs {
            for col in &spec.columns {
                let column = col.trim().to_ascii_lowercase();
                let key = (grantee.to_string(), table.clone(), column);
                if let Some(entry) = self.column_grants.get_mut(&key) {
                    entry.remove(&spec.privilege);
                    if entry.is_empty() {
                        self.column_grants.remove(&key);
                    }
                }
            }
        }
        Ok(())
    }

    fn has_column_grant(
        &self,
        ctx: &AuthContext,
        table: &str,
        column: &str,
        priv_: ColumnPrivilege,
    ) -> bool {
        let table = table_name_leaf(table);
        let column = column.trim().to_ascii_lowercase();
        for role in &ctx.roles {
            if let Some(set) = self
                .column_grants
                .get(&(role.clone(), table.to_string(), column.clone()))
            {
                if set.contains(&priv_) {
                    return true;
                }
            }
        }
        false
    }

    fn has_column_grant_on_any(
        &self,
        ctx: &AuthContext,
        table: &str,
        priv_: ColumnPrivilege,
    ) -> bool {
        let table = table_name_leaf(table);
        for role in &ctx.roles {
            for ((g, t, _), set) in &self.column_grants {
                if g == role && t == table && set.contains(&priv_) {
                    return true;
                }
            }
        }
        false
    }

    /// True when `ctx` holds schema privilege `priv` (defaults + `SGRANT` rows).
    pub fn has_schema_grant(
        &self,
        ctx: &AuthContext,
        schema: &str,
        priv_: SchemaPrivilege,
    ) -> bool {
        if ctx.is_superuser {
            return true;
        }
        let leaf = schema_name_leaf(schema);
        for role in &ctx.roles {
            if let Some(set) = self.schema_grants.get(&(role.clone(), leaf.clone())) {
                if set.contains(&priv_) {
                    return true;
                }
            }
        }
        false
    }

    /// `has_schema_privilege` with AUTH schema grants layered on defaults.
    pub fn has_any_schema_privilege(
        &self,
        ctx: &AuthContext,
        schema: &str,
        search_path: &str,
        privs: &[SchemaPrivilege],
    ) -> Result<bool> {
        let leaf = schema_name_leaf(schema);
        if !schema_exists(&leaf, search_path) {
            return Err(TakyonicError::Sql(format!(
                "schema \"{leaf}\" does not exist"
            )));
        }
        Ok(privs.iter().any(|p| {
            has_one_schema_privilege(ctx.is_superuser, &leaf, *p)
                || self.has_schema_grant(ctx, &leaf, *p)
        }))
    }

    /// Build an [`AuthContext`] for `user` (expanding memberships).
    pub fn auth_context(&self, user: &str) -> Result<AuthContext> {
        let role = self.roles.get(user).ok_or_else(|| {
            TakyonicError::PermissionDenied(format!("unknown user `{user}`"))
        })?;
        let mut roles = BTreeSet::new();
        roles.insert(user.to_string());
        if let Some(m) = self.memberships.get(user) {
            roles.extend(m.iter().cloned());
        }
        let mut is_superuser = role.is_superuser;
        for r in &roles {
            if self
                .roles
                .get(r)
                .map(|d| d.is_superuser)
                .unwrap_or(false)
            {
                is_superuser = true;
            }
        }
        Ok(AuthContext {
            user: user.to_string(),
            roles,
            is_superuser,
        })
    }

    /// `pg_has_role` — true if `user` holds any of `privs` on `role` (membership-based).
    pub fn has_role_privilege(
        &self,
        user: &str,
        role: &str,
        privs: &[RolePrivilege],
    ) -> Result<bool> {
        let user = role_name_leaf(user);
        let role = role_name_leaf(role);
        if self.get_role(&role).is_none() {
            return Err(TakyonicError::Sql(format!(
                "role \"{role}\" does not exist"
            )));
        }
        let ctx = self.auth_context(&user).map_err(|_| {
            TakyonicError::Sql(format!("role \"{user}\" does not exist"))
        })?;
        if ctx.is_superuser {
            return Ok(true);
        }
        // MEMBER / USAGE / SET all reduce to membership until INHERIT/SET flags exist.
        let member = ctx.roles.iter().any(|r| r.eq_ignore_ascii_case(&role));
        Ok(privs.iter().any(|_| member))
    }

    /// True when `ctx` holds `priv` on `table`.
    pub fn has_privilege(&self, ctx: &AuthContext, table: &str, priv_: Privilege) -> bool {
        if ctx.is_superuser {
            return true;
        }
        for role in &ctx.roles {
            if let Some(set) = self.grants.get(&(role.clone(), table.to_string())) {
                if set.contains(&priv_) {
                    return true;
                }
            }
        }
        false
    }

    /// `has_table_privilege` — true if **any** privilege in `privs` is held (PG comma-list semantics).
    pub fn has_any_table_privilege(
        &self,
        ctx: &AuthContext,
        table: &str,
        privs: &[Privilege],
    ) -> bool {
        let table = table_name_leaf(table);
        privs
            .iter()
            .any(|p| self.has_privilege(ctx, table, *p))
    }

    /// `has_column_privilege` — table grants apply to all columns; plus `CGRANT` rows.
    /// Empty `column` means “any column” (`has_any_column_privilege`).
    pub fn has_any_column_privilege(
        &self,
        ctx: &AuthContext,
        table: &str,
        column: &str,
        privs: &[ColumnPrivilege],
    ) -> bool {
        if ctx.is_superuser {
            return true;
        }
        let table = table_name_leaf(table);
        let col = column.trim().to_ascii_lowercase();
        privs.iter().any(|p| {
            if let Some(tp) = p.as_table_privilege() {
                if self.has_privilege(ctx, table, tp) {
                    return true;
                }
            }
            if col.is_empty() {
                self.has_column_grant_on_any(ctx, table, *p)
            } else {
                self.has_column_grant(ctx, table, &col, *p)
            }
        })
    }

    /// Verify Argon2id password for a login role.
    pub fn verify_password(&self, user: &str, password: &str) -> bool {
        let Some(role) = self.roles.get(user) else {
            return false;
        };
        if !role.can_login {
            return false;
        }
        let Some(hash) = &role.argon2_hash else {
            return false;
        };
        verify_password(password, hash)
    }

    /// SCRAM credential for PgWire (if login role).
    pub fn scram_credential(&self, user: &str) -> Option<ScramCredential> {
        self.roles
            .get(user)
            .filter(|r| r.can_login)
            .and_then(|r| r.scram.clone())
    }

    /// Test / AuthSource helper: upsert a SCRAM credential for `user`.
    pub fn upsert_scram(&mut self, user: impl Into<String>, credential: ScramCredential) {
        let name = user.into();
        if let Some(role) = self.roles.get_mut(&name) {
            role.scram = Some(credential);
            role.can_login = true;
        } else {
            self.roles.insert(
                name.clone(),
                RoleDef {
                    name,
                    can_login: true,
                    is_superuser: false,
                    argon2_hash: None,
                    scram: Some(credential),
                },
            );
        }
    }
}

/// Shared mutable auth catalog handle.
pub type SharedAuthCatalog = Arc<RwLock<AuthCatalog>>;

/// Gatekeeper for logical plans (AccessControlRule).
pub struct AuthorizationManager;

impl AuthorizationManager {
    /// Reject the plan when `ctx` lacks required privileges.
    pub fn authorize(catalog: &AuthCatalog, ctx: &AuthContext, plan: &LogicalPlan) -> Result<()> {
        if ctx.is_superuser {
            return Ok(());
        }
        // DDL / maintenance require SUPERUSER.
        match plan {
            LogicalPlan::CreateIndex { .. }
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
            | LogicalPlan::Rebalance { .. } => {
                return Err(TakyonicError::PermissionDenied(
                    "permission denied: SUPERUSER required".into(),
                ));
            }
            LogicalPlan::Begin | LogicalPlan::Commit | LogicalPlan::Rollback => return Ok(()),
            LogicalPlan::Set { .. } | LogicalPlan::Show { .. } => return Ok(()),
            LogicalPlan::Listen { .. }
            | LogicalPlan::Unlisten { .. }
            | LogicalPlan::Notify { .. }
            | LogicalPlan::CreateSequence { .. }
            | LogicalPlan::DropSequence { .. }
            | LogicalPlan::AlterSequence { .. } => return Ok(()),
            LogicalPlan::Comment { .. } => {
                if !ctx.is_superuser {
                    return Err(TakyonicError::PermissionDenied(
                        "permission denied: SUPERUSER required for COMMENT".into(),
                    ));
                }
                return Ok(());
            }
            LogicalPlan::Explain { plan } => {
                return Self::authorize(catalog, ctx, plan);
            }
            _ => {}
        }

        for (table, priv_) in required_privileges(plan) {
            if !catalog.has_privilege(ctx, &table, priv_) {
                return Err(TakyonicError::PermissionDenied(format!(
                    "permission denied for table `{table}` ({})",
                    priv_.as_str()
                )));
            }
        }
        Ok(())
    }
}

/// Collect `(table, privilege)` requirements from a plan tree.
fn table_name_leaf(table: &str) -> &str {
    table
        .rsplit('.')
        .next()
        .unwrap_or(table)
        .trim()
        .trim_matches('"')
}

fn required_privileges(plan: &LogicalPlan) -> Vec<(String, Privilege)> {
    let mut out = Vec::new();
    walk_privs(plan, &mut out);
    out.sort();
    out.dedup();
    out
}

fn walk_privs(plan: &LogicalPlan, out: &mut Vec<(String, Privilege)>) {
    match plan {
        LogicalPlan::Select { table, .. } => {
            out.push((table.clone(), Privilege::Select));
        }
        LogicalPlan::Insert { table, .. } => {
            out.push((table.clone(), Privilege::Insert));
        }
        LogicalPlan::Update { table, selection, .. } => {
            out.push((table.clone(), Privilege::Update));
            if selection.is_some() {
                out.push((table.clone(), Privilege::Select));
            }
        }
        LogicalPlan::Delete { table, selection, .. } => {
            out.push((table.clone(), Privilege::Delete));
            if selection.is_some() {
                out.push((table.clone(), Privilege::Select));
            }
        }
        LogicalPlan::Truncate { table, .. } => {
            // PG has a separate TRUNCATE privilege; map to Delete until GRANT TRUNCATE exists.
            out.push((table.clone(), Privilege::Delete));
        }
        LogicalPlan::Copy { table, to, .. } => {
            if *to {
                out.push((table.clone(), Privilege::Select));
            } else {
                out.push((table.clone(), Privilege::Insert));
            }
        }
        LogicalPlan::Join { left, right, .. }
        | LogicalPlan::DistributedJoin { left, right, .. } => {
            walk_privs(left, out);
            walk_privs(right, out);
        }
        LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::DistributedAggregate { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Filter { input, .. }
        | LogicalPlan::Project { input, .. }
        | LogicalPlan::Distinct { input, .. }
        | LogicalPlan::DistinctOn { input, .. }
        | LogicalPlan::SubqueryAlias { input, .. } => walk_privs(input, out),
        LogicalPlan::Union { left, right, .. } => {
            walk_privs(left, out);
            walk_privs(right, out);
        }
        LogicalPlan::Explain { plan } => walk_privs(plan, out),
        _ => {}
    }
}

/// Hash a cleartext password with Argon2id (PHC string).
pub fn hash_password(password: &str) -> Result<String> {
    let salt = SaltString::generate(&mut OsRng);
    let argon2 = Argon2::default();
    argon2
        .hash_password(password.as_bytes(), &salt)
        .map(|h| h.to_string())
        .map_err(|e| TakyonicError::Engine(format!("argon2 hash failed: {e}")))
}

/// Verify cleartext against an Argon2id PHC string.
pub fn verify_password(password: &str, phc: &str) -> bool {
    let Ok(parsed) = PasswordHash::new(phc) else {
        return false;
    };
    Argon2::default()
        .verify_password(password.as_bytes(), &parsed)
        .is_ok()
}

fn b64_encode(bytes: &[u8]) -> String {
    // URL-safe-ish alphabet without padding — enough for salt/key storage.
    const T: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = chunk.get(1).copied().unwrap_or(0) as u32;
        let b2 = chunk.get(2).copied().unwrap_or(0) as u32;
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(T[((n >> 18) & 63) as usize] as char);
        out.push(T[((n >> 12) & 63) as usize] as char);
        if chunk.len() > 1 {
            out.push(T[((n >> 6) & 63) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(T[(n & 63) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
}

fn b64_decode(s: &str) -> Result<Vec<u8>> {
    fn val(c: u8) -> Result<u8> {
        match c {
            b'A'..=b'Z' => Ok(c - b'A'),
            b'a'..=b'z' => Ok(c - b'a' + 26),
            b'0'..=b'9' => Ok(c - b'0' + 52),
            b'+' => Ok(62),
            b'/' => Ok(63),
            _ => Err(TakyonicError::Integrity("bad base64 in AUTH".into())),
        }
    }
    let bytes = s.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i + 3 < bytes.len() || (i < bytes.len() && bytes[i] != b'=') {
        if i + 3 >= bytes.len() {
            break;
        }
        let a = val(bytes[i])?;
        let b = val(bytes[i + 1])?;
        let c = if bytes[i + 2] == b'=' {
            0
        } else {
            val(bytes[i + 2])?
        };
        let d = if bytes[i + 3] == b'=' {
            0
        } else {
            val(bytes[i + 3])?
        };
        out.push((a << 2) | (b >> 4));
        if bytes[i + 2] != b'=' {
            out.push((b << 4) | (c >> 2));
        }
        if bytes[i + 3] != b'=' {
            out.push((c << 6) | d);
        }
        i += 4;
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn argon2_roundtrip() {
        let h = hash_password("s3cret").unwrap();
        assert!(verify_password("s3cret", &h));
        assert!(!verify_password("wrong", &h));
    }

    #[test]
    fn grant_select_denies_delete() {
        let mut cat = AuthCatalog::with_bootstrap();
        cat.create_role("analyst", true, false, Some("pw"), false)
            .unwrap();
        cat.grant("analyst", "employees", &[Privilege::Select])
            .unwrap();
        let ctx = cat.auth_context("analyst").unwrap();
        assert!(cat.has_privilege(&ctx, "employees", Privilege::Select));
        assert!(!cat.has_privilege(&ctx, "employees", Privilege::Delete));
        assert!(cat.has_any_table_privilege(
            &ctx,
            "public.employees",
            &Privilege::parse_list("SELECT, DELETE").unwrap()
        ));
        assert!(!cat.has_any_table_privilege(
            &ctx,
            "employees",
            &Privilege::parse_list("DELETE").unwrap()
        ));
        let del = LogicalPlan::Delete {
            table: "employees".into(),
            selection: None,
            returning: None,
        };
        let err = AuthorizationManager::authorize(&cat, &ctx, &del).unwrap_err();
        assert!(
            matches!(err, TakyonicError::PermissionDenied(_)),
            "got {err:?}"
        );
        let sel = LogicalPlan::Select {
            table: "employees".into(),
            filters: vec![],
            predicate: None,
        };
        AuthorizationManager::authorize(&cat, &ctx, &sel).unwrap();
    }

    #[test]
    fn schema_privilege_defaults() {
        assert!(has_schema_privilege(
            false,
            "public",
            "public",
            &[SchemaPrivilege::Usage]
        )
        .unwrap());
        assert!(!has_schema_privilege(
            false,
            "public",
            "public",
            &[SchemaPrivilege::Create]
        )
        .unwrap());
        assert!(has_schema_privilege(
            true,
            "public",
            "public",
            &[SchemaPrivilege::Create]
        )
        .unwrap());
        let err =
            has_schema_privilege(false, "nope", "public", &[SchemaPrivilege::Usage]).unwrap_err();
        assert!(err.to_string().contains("does not exist"));
        assert!(!has_schema_privilege(
            false,
            "myschema",
            "myschema, public",
            &[SchemaPrivilege::Usage]
        )
        .unwrap());
    }

    #[test]
    fn schema_grant_create_roundtrip() {
        let mut cat = AuthCatalog::with_bootstrap();
        cat.create_role("analyst", true, false, Some("pw"), false)
            .unwrap();
        cat.grant_schema("analyst", "public", &[SchemaPrivilege::Create])
            .unwrap();
        let ctx = cat.auth_context("analyst").unwrap();
        assert!(cat
            .has_any_schema_privilege(&ctx, "public", "public", &[SchemaPrivilege::Create])
            .unwrap());
        let text = cat.encode();
        assert!(text.contains("SGRANT\tanalyst\tpublic\tCREATE"));
        let mut parsed = AuthCatalog::parse(&text).unwrap();
        let ctx2 = parsed.auth_context("analyst").unwrap();
        assert!(parsed
            .has_any_schema_privilege(&ctx2, "public", "public", &[SchemaPrivilege::Create])
            .unwrap());
        parsed
            .revoke_schema("analyst", "public", &[SchemaPrivilege::Create])
            .unwrap();
        let ctx3 = parsed.auth_context("analyst").unwrap();
        assert!(!parsed
            .has_any_schema_privilege(&ctx3, "public", "public", &[SchemaPrivilege::Create])
            .unwrap());
    }

    #[test]
    fn database_privilege_defaults() {
        assert!(has_database_privilege(
            false,
            "postgres",
            &[DatabasePrivilege::Connect]
        )
        .unwrap());
        assert!(!has_database_privilege(
            false,
            "postgres",
            &[DatabasePrivilege::Create]
        )
        .unwrap());
        assert!(!has_database_privilege(
            false,
            "postgres",
            &[DatabasePrivilege::Temporary]
        )
        .unwrap());
        assert!(has_database_privilege(
            true,
            "postgres",
            &[DatabasePrivilege::Create, DatabasePrivilege::Temporary]
        )
        .unwrap());
        let err = has_database_privilege(
            false,
            "otherdb",
            &[DatabasePrivilege::Connect],
        )
        .unwrap_err();
        assert!(err.to_string().contains("does not exist"));
        assert_eq!(
            DatabasePrivilege::parse_list("CONNECT, TEMP").unwrap(),
            vec![DatabasePrivilege::Connect, DatabasePrivilege::Temporary]
        );
    }

    #[test]
    fn function_privilege_known_scalars() {
        assert_eq!(
            function_name_leaf("pg_catalog.format_type(regtype,integer)"),
            "format_type"
        );
        assert_eq!(function_name_leaf("LOWER"), "lower");
        assert!(has_function_privilege(
            false,
            "format_type",
            &[FunctionPrivilege::Execute],
            |n| n == "format_type"
        ));
        assert!(!has_function_privilege(
            false,
            "nope_fn",
            &[FunctionPrivilege::Execute],
            |n| n == "format_type"
        ));
        assert!(has_function_privilege(
            true,
            "nope_fn",
            &[FunctionPrivilege::Execute],
            |_| false
        ));
    }

    #[test]
    fn pg_has_role_membership() {
        let mut cat = AuthCatalog::with_bootstrap();
        cat.create_role("analysts", false, false, None, false)
            .unwrap();
        cat.create_role("analyst", true, false, Some("pw"), false)
            .unwrap();
        assert!(!cat
            .has_role_privilege("analyst", "analysts", &[RolePrivilege::Member])
            .unwrap());
        cat.grant_membership("analysts", "analyst").unwrap();
        assert!(cat
            .has_role_privilege("analyst", "analysts", &[RolePrivilege::Member])
            .unwrap());
        assert!(cat
            .has_role_privilege("analyst", "analysts", &[RolePrivilege::Usage])
            .unwrap());
        assert!(cat
            .has_role_privilege("analyst", "analyst", &[RolePrivilege::Set])
            .unwrap());
        assert!(cat
            .has_role_privilege("postgres", "analysts", &[RolePrivilege::Member])
            .unwrap());
        let err = cat
            .has_role_privilege("analyst", "nope", &[RolePrivilege::Member])
            .unwrap_err();
        assert!(err.to_string().contains("does not exist"));
        assert_eq!(
            RolePrivilege::parse_list("MEMBER WITH ADMIN OPTION, USAGE").unwrap(),
            vec![RolePrivilege::Member, RolePrivilege::Usage]
        );
    }

    #[test]
    fn type_privilege_known_types() {
        assert_eq!(type_name_leaf("pg_catalog.int4"), "int4");
        let exists = |n: &str| crate::oid::type_oid_from_name(n).is_some();
        assert!(has_type_privilege(false, "integer", &[TypePrivilege::Usage], exists).unwrap());
        assert!(has_type_privilege(true, "integer", &[TypePrivilege::Usage], exists).unwrap());
        let err = has_type_privilege(false, "nope_type", &[TypePrivilege::Usage], exists)
            .unwrap_err();
        assert!(err.to_string().contains("does not exist"));
    }

    #[test]
    fn column_privilege_follows_table_grants() {
        let mut cat = AuthCatalog::with_bootstrap();
        cat.create_role("analyst", true, false, Some("pw"), false)
            .unwrap();
        cat.grant("analyst", "employees", &[Privilege::Select])
            .unwrap();
        let ctx = cat.auth_context("analyst").unwrap();
        assert!(cat.has_any_column_privilege(
            &ctx,
            "employees",
            "name",
            &ColumnPrivilege::parse_list("SELECT").unwrap()
        ));
        assert!(!cat.has_any_column_privilege(
            &ctx,
            "employees",
            "name",
            &ColumnPrivilege::parse_list("UPDATE").unwrap()
        ));
        assert!(!cat.has_any_column_privilege(
            &ctx,
            "employees",
            "id",
            &ColumnPrivilege::parse_list("REFERENCES").unwrap()
        ));
        let su = cat.auth_context(BOOTSTRAP_USER).unwrap();
        assert!(cat.has_any_column_privilege(
            &su,
            "employees",
            "id",
            &ColumnPrivilege::parse_list("REFERENCES").unwrap()
        ));
    }

    #[test]
    fn column_grant_acl_roundtrip() {
        let mut cat = AuthCatalog::with_bootstrap();
        cat.create_role("analyst", true, false, Some("pw"), false)
            .unwrap();
        cat.grant_columns(
            "analyst",
            "employees",
            &[ColumnGrantSpec {
                privilege: ColumnPrivilege::Update,
                columns: vec!["name".into()],
            }],
        )
        .unwrap();
        let ctx = cat.auth_context("analyst").unwrap();
        assert!(cat.has_any_column_privilege(
            &ctx,
            "employees",
            "name",
            &[ColumnPrivilege::Update]
        ));
        assert!(!cat.has_any_column_privilege(
            &ctx,
            "employees",
            "id",
            &[ColumnPrivilege::Update]
        ));
        assert!(cat.has_any_column_privilege(
            &ctx,
            "employees",
            "",
            &[ColumnPrivilege::Update]
        ));
        let text = cat.encode();
        assert!(text.contains("CGRANT\tanalyst\temployees\tname\tUPDATE"));
        let mut parsed = AuthCatalog::parse(&text).unwrap();
        parsed
            .revoke_columns(
                "analyst",
                "employees",
                &[ColumnGrantSpec {
                    privilege: ColumnPrivilege::Update,
                    columns: vec!["name".into()],
                }],
            )
            .unwrap();
        let ctx2 = parsed.auth_context("analyst").unwrap();
        assert!(!parsed.has_any_column_privilege(
            &ctx2,
            "employees",
            "name",
            &[ColumnPrivilege::Update]
        ));
    }

    #[test]
    fn superuser_bypasses() {
        let cat = AuthCatalog::with_bootstrap();
        let ctx = cat.auth_context(BOOTSTRAP_USER).unwrap();
        assert!(ctx.is_superuser);
        let del = LogicalPlan::Delete {
            table: "employees".into(),
            selection: None,
            returning: None,
        };
        AuthorizationManager::authorize(&cat, &ctx, &del).unwrap();
    }

    #[test]
    fn auth_catalog_persists() {
        let dir = std::env::temp_dir().join(format!(
            "takyonic-auth-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&dir).unwrap();
        let mut cat = AuthCatalog::with_bootstrap();
        cat.create_role("analyst", true, false, Some("secret"), false)
            .unwrap();
        cat.grant("analyst", "employees", &[Privilege::Select])
            .unwrap();
        cat.save(&dir).unwrap();
        let loaded = AuthCatalog::load(&dir).unwrap();
        assert!(loaded.get_role("analyst").is_some());
        assert!(loaded.verify_password("analyst", "secret"));
        let ctx = loaded.auth_context("analyst").unwrap();
        assert!(loaded.has_privilege(&ctx, "employees", Privilege::Select));
        let _ = fs::remove_dir_all(dir);
    }
}
