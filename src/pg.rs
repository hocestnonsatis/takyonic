//! PostgreSQL wire-protocol (pgwire) facade over Takyonic's SQL / Smart Client stack.
//!
//! [`TakyonicPgBackend`] implements both [`SimpleQueryHandler`] and
//! [`ExtendedQueryHandler`] (Parse / Bind / Execute / Sync prepared-statement
//! flow). Session scaffolding lives in [`SessionState`].

use std::collections::HashMap;
use std::fmt::Debug;
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
use pgwire::api::portal::Portal;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    DataRowEncoder, DescribePortalResponse, DescribeStatementResponse, FieldFormat, FieldInfo,
    QueryResponse, Response, Tag,
};
use pgwire::api::stmt::{QueryParser, StoredStatement};
use pgwire::api::store::PortalStore;
use pgwire::api::{ClientInfo, METADATA_USER, PgWireServerHandlers, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;
use tracing::debug;

use crate::client::TakyonicClient;
use crate::engine::TakyonicEngine;
use crate::executor::{self, ExecutionContext, affected_row_count};
use crate::rbac::{AuthCatalog, AuthContext, AuthorizationManager, SharedAuthCatalog};
use crate::schema::Record;
use crate::sql::{LogicalPlan, SqlEngine, Value};
use crate::txn::Transaction;

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
}

impl SessionResult {
    fn command(tag: &'static str) -> Self {
        Self {
            tag,
            rows: Vec::new(),
            affected: None,
        }
    }

    fn from_data_plan(plan: &LogicalPlan, rows: Vec<Record>) -> Self {
        match plan {
            LogicalPlan::Insert { .. } => Self {
                tag: "INSERT",
                affected: Some(affected_row_count(&rows)),
                rows: Vec::new(),
            },
            LogicalPlan::Update { .. } => Self {
                tag: "UPDATE",
                affected: Some(affected_row_count(&rows)),
                rows: Vec::new(),
            },
            LogicalPlan::Delete { .. } => Self {
                tag: "DELETE",
                affected: Some(affected_row_count(&rows)),
                rows: Vec::new(),
            },
            LogicalPlan::Select { .. }
            | LogicalPlan::Join { .. }
            | LogicalPlan::DistributedJoin { .. }
            | LogicalPlan::Aggregate { .. }
            | LogicalPlan::DistributedAggregate { .. }
            | LogicalPlan::Sort { .. }
            | LogicalPlan::Limit { .. }
            | LogicalPlan::Explain { .. }
            | LogicalPlan::Filter { .. }
            | LogicalPlan::SubqueryAlias { .. } => Self {
                tag: "SELECT",
                affected: None,
                rows,
            },
            LogicalPlan::CreateIndex { .. } => Self {
                tag: "CREATE INDEX",
                affected: None,
                rows: Vec::new(),
            },
            LogicalPlan::CreateRole { can_login, .. } => Self {
                tag: if *can_login { "CREATE USER" } else { "CREATE ROLE" },
                affected: None,
                rows: Vec::new(),
            },
            LogicalPlan::DropRole { .. } => Self {
                tag: "DROP ROLE",
                affected: None,
                rows: Vec::new(),
            },
            LogicalPlan::Grant { .. } | LogicalPlan::GrantRole { .. } => Self {
                tag: "GRANT",
                affected: None,
                rows: Vec::new(),
            },
            LogicalPlan::Revoke { .. } => Self {
                tag: "REVOKE",
                affected: None,
                rows: Vec::new(),
            },
            LogicalPlan::DropIndex { .. } => Self {
                tag: "DROP INDEX",
                affected: None,
                rows: Vec::new(),
            },
            LogicalPlan::Analyze { .. } => Self {
                tag: "ANALYZE",
                affected: None,
                rows: Vec::new(),
            },
            LogicalPlan::Vacuum { .. } => Self {
                tag: "VACUUM",
                affected: None,
                rows: Vec::new(),
            },
            LogicalPlan::Begin | LogicalPlan::Commit | LogicalPlan::Rollback => {
                unreachable!("txn control handled before data execution")
            }
        }
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
        })
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
    pub fn execute_sql(&mut self, sql: &str) -> crate::error::Result<SessionResult> {
        let plan = SqlEngine::plan(sql)?;
        self.run_plan(&plan, Vec::new())
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
        self.authorize(plan)?;
        match plan {
            LogicalPlan::Begin => {
                if self.active_txn.is_some() {
                    return Err(crate::error::TakyonicError::Sql(
                        "there is already a transaction in progress".into(),
                    ));
                }
                self.active_txn = Some(self.engine.begin()?);
                Ok(SessionResult::command("BEGIN"))
            }
            LogicalPlan::Commit => {
                let txn = self.active_txn.take().ok_or_else(|| {
                    crate::error::TakyonicError::Sql("there is no transaction in progress".into())
                })?;
                match txn.commit() {
                    Ok(_) => Ok(SessionResult::command("COMMIT")),
                    Err(e) => {
                        // OCC conflict (or other commit failure): workspace is
                        // dropped via Transaction::Drop → abort semantics.
                        Err(e)
                    }
                }
            }
            LogicalPlan::Rollback => {
                let txn = self.active_txn.take().ok_or_else(|| {
                    crate::error::TakyonicError::Sql("there is no transaction in progress".into())
                })?;
                txn.abort();
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
            LogicalPlan::GrantRole { role, member } => {
                self.engine.grant_role_membership(role, member)?;
                Ok(SessionResult::command("GRANT"))
            }
            LogicalPlan::DropIndex { name, if_exists } => {
                self.engine.drop_index(name, *if_exists)?;
                Ok(SessionResult::command("DROP INDEX"))
            }
            LogicalPlan::Explain { plan } => {
                let physical = executor::optimize_with_catalog(
                    plan,
                    &|t| self.engine.table_schema(t).ok(),
                    &|t| Some(self.engine.table_stats(t)),
                )?;
                let text = executor::explain_physical(&physical);
                Ok(SessionResult {
                    tag: "SELECT",
                    rows: vec![Record::new().set("QUERY PLAN", text)],
                    affected: None,
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
                })
            }
            other => {
                let ctx = ExecutionContext::with_params(params);
                let rows = if let Some(txn) = self.active_txn.as_mut() {
                    // Explicit transaction: mutate/read workspace, do not commit.
                    executor::execute_plan(other, &ctx, txn)?
                } else {
                    // Auto-commit: fresh txn, commit DML / abort after SELECT.
                    executor::execute_plan_autocommit(other, &ctx, self.engine.begin()?)?
                };
                Ok(SessionResult::from_data_plan(other, rows))
            }
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
        let ctx = ExecutionContext::with_params(portal.parameters.clone());
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
        | LogicalPlan::Filter { .. }
        | LogicalPlan::SubqueryAlias { .. } => {
            let _physical = executor::optimize(plan)?;
            Ok(Vec::new())
        }
        LogicalPlan::Insert { .. }
        | LogicalPlan::Update { .. }
        | LogicalPlan::Delete { .. }
        | LogicalPlan::Begin
        | LogicalPlan::Commit
        | LogicalPlan::Rollback
        | LogicalPlan::CreateIndex { .. }
        | LogicalPlan::DropIndex { .. }
        | LogicalPlan::CreateRole { .. }
        | LogicalPlan::DropRole { .. }
        | LogicalPlan::Grant { .. }
        | LogicalPlan::Revoke { .. }
        | LogicalPlan::GrantRole { .. }
        | LogicalPlan::Explain { .. }
        | LogicalPlan::Analyze { .. }
        | LogicalPlan::Vacuum { .. } => {
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
}

impl TakyonicPgBackend {
    /// Wrap a connected Smart Client and a local engine for Volcano / txn control.
    pub fn new(client: TakyonicClient, engine: Arc<TakyonicEngine>) -> Self {
        Self {
            client,
            query_parser: Arc::new(TakyonicQueryParser),
            engine,
            sessions: DashMap::new(),
        }
    }

    /// Test helper: pre-seed a session for `pid` authenticated as `user`.
    pub fn new_for_test(engine: Arc<TakyonicEngine>, pid: i32, user: &str) -> Self {
        let backend = Self {
            client: TakyonicClient::new(std::iter::empty::<String>()),
            query_parser: Arc::new(TakyonicQueryParser),
            engine: Arc::clone(&engine),
            sessions: DashMap::new(),
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
        let state = Arc::new(Mutex::new(
            SessionState::as_user(Arc::clone(&self.engine), user)
                .unwrap_or_else(|_| SessionState::new(Arc::clone(&self.engine))),
        ));
        match self.sessions.entry(pid) {
            dashmap::mapref::entry::Entry::Occupied(o) => Arc::clone(o.get()),
            dashmap::mapref::entry::Entry::Vacant(v) => {
                v.insert(Arc::clone(&state));
                state
            }
        }
    }
}

fn session_result_to_response(result: SessionResult) -> PgWireResult<Response> {
    match result.tag {
        "SELECT" => encode_select_response(result.rows),
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
        C: ClientInfo + Unpin + Send + Sync,
    {
        let sql = query.trim().trim_end_matches(';').trim();
        if sql.is_empty() {
            return Ok(vec![Response::EmptyQuery]);
        }

        // Session housekeeping we don't implement yet.
        let upper = sql.to_ascii_uppercase();
        if upper.starts_with("SET ") || upper.starts_with("SHOW ") {
            let tag = if upper.starts_with("SHOW") {
                "SHOW"
            } else {
                "SET"
            };
            return Ok(vec![Response::Execution(Tag::new(tag))]);
        }

        let session = self.session_arc_for(client);
        let result = session.lock().execute_sql(sql).map_err(sql_err)?;
        Ok(vec![session_result_to_response(result)?])
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
        session_result_to_response(result)
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
        match &stmt.statement {
            LogicalPlan::Insert { .. }
            | LogicalPlan::Update { .. }
            | LogicalPlan::Delete { .. }
            | LogicalPlan::Begin
            | LogicalPlan::Commit
            | LogicalPlan::Rollback => {
                Ok(DescribeStatementResponse::new(param_types, Vec::new()))
            }
            LogicalPlan::Select { .. }
            | LogicalPlan::Join { .. }
            | LogicalPlan::DistributedJoin { .. }
            | LogicalPlan::Aggregate { .. }
            | LogicalPlan::DistributedAggregate { .. }
            | LogicalPlan::Sort { .. }
            | LogicalPlan::Limit { .. }
            | LogicalPlan::CreateIndex { .. }
            | LogicalPlan::DropIndex { .. }
            | LogicalPlan::CreateRole { .. }
            | LogicalPlan::DropRole { .. }
            | LogicalPlan::Grant { .. }
            | LogicalPlan::Revoke { .. }
            | LogicalPlan::GrantRole { .. }
            | LogicalPlan::Explain { .. }
            | LogicalPlan::Analyze { .. }
            | LogicalPlan::Vacuum { .. }
            | LogicalPlan::Filter { .. }
            | LogicalPlan::SubqueryAlias { .. } => {
                Ok(DescribeStatementResponse::new(param_types, Vec::new()))
            }
        }
    }

    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        _portal: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        Ok(DescribePortalResponse::new(Vec::new()))
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

fn encode_select_response(rows: Vec<Record>) -> PgWireResult<Response> {
    let columns = infer_columns(&rows);
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
                Some(s) => {
                    if *ty == Type::INT8 {
                        let n: i64 = s.parse().unwrap_or(0);
                        encoder.encode_field(&n)?;
                    } else {
                        encoder.encode_field(&s)?;
                    }
                }
            }
        }
        encoded.push(encoder.finish());
    }

    Ok(Response::Query(QueryResponse::new(
        schema,
        stream::iter(encoded),
    )))
}

/// Column order follows BTreeMap field order. INT8 if every non-null value
/// parses as i64, else VARCHAR.
fn infer_columns(rows: &[Record]) -> Vec<(String, Type)> {
    use std::collections::BTreeMap;
    let mut seen: BTreeMap<String, bool> = BTreeMap::new();
    for record in rows {
        for (k, v) in &record.fields {
            let is_int = v.parse::<i64>().is_ok();
            seen.entry(k.clone())
                .and_modify(|all| *all = *all && is_int)
                .or_insert(is_int);
        }
    }
    seen.into_iter()
        .map(|(name, all_int)| {
            let ty = if all_int { Type::INT8 } else { Type::VARCHAR };
            (name, ty)
        })
        .collect()
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
}

impl PgWireServerHandlers for TakyonicPgFactory {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        self.backend.clone()
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::schema::{IndexDef, Record, TableSchema};
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
}
