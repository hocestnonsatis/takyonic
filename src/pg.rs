//! PostgreSQL wire-protocol (pgwire) facade over Takyonic's SQL / Smart Client stack.
//!
//! [`TakyonicPgBackend`] implements [`SimpleQueryHandler`]: raw SQL from `psql`
//! is parsed by [`crate::sql::SqlEngine`], executed via [`TakyonicClient::execute_sql`],
//! and result rows are encoded as Postgres `DataRow` messages.

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use futures::sink::{Sink, SinkExt};
use futures::stream;
use pgwire::api::auth::{
    DefaultServerParameterProvider, StartupHandler, finish_authentication, protocol_negotiation,
    save_startup_parameters_to_metadata,
};
use pgwire::api::query::SimpleQueryHandler;
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response, Tag};
use pgwire::api::{
    ClientInfo, PgWireConnectionState, PgWireServerHandlers, PidSecretKeyGenerator,
    RandomPidSecretKeyGenerator, Type,
};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::startup::Authentication;
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};
use tracing::debug;

use crate::client::TakyonicClient;
use crate::schema::Record;
use crate::sql::{LogicalPlan, SqlEngine};

/// Cleartext password handshake that always succeeds (any username/password).
pub struct AcceptAnyCleartext {
    params: DefaultServerParameterProvider,
    pid_gen: RandomPidSecretKeyGenerator,
}

impl AcceptAnyCleartext {
    fn new() -> Self {
        let mut params = DefaultServerParameterProvider::default();
        params.server_version = "16.0 (Takyonic)".into();
        params.server_encoding = "UTF8".into();
        params.client_encoding = Some("UTF8".into());
        Self {
            params,
            pid_gen: RandomPidSecretKeyGenerator::default(),
        }
    }
}

#[async_trait]
impl StartupHandler for AcceptAnyCleartext {
    async fn on_startup<C>(
        &self,
        client: &mut C,
        message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        match message {
            PgWireFrontendMessage::Startup(ref startup) => {
                protocol_negotiation(client, startup).await?;
                save_startup_parameters_to_metadata(client, startup);
                client.set_state(PgWireConnectionState::AuthenticationInProgress);
                client
                    .send(PgWireBackendMessage::Authentication(
                        Authentication::CleartextPassword,
                    ))
                    .await?;
            }
            PgWireFrontendMessage::PasswordMessageFamily(pwd) => {
                let _ = pwd.into_password()?;
                debug!("pgwire cleartext auth — accepting credentials");
                let (pid, secret_key) = self.pid_gen.generate(client);
                client.set_pid_and_secret_key(pid, secret_key);
                finish_authentication(client, &self.params).await?;
            }
            _ => {}
        }
        Ok(())
    }
}

/// pgwire query backend bridged to [`TakyonicClient::execute_sql`].
pub struct TakyonicPgBackend {
    client: TakyonicClient,
}

impl TakyonicPgBackend {
    /// Wrap an already-connected Smart Client.
    pub fn new(client: TakyonicClient) -> Self {
        Self { client }
    }
}

#[async_trait]
impl SimpleQueryHandler for TakyonicPgBackend {
    async fn do_query<C>(&self, _client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let sql = query.trim().trim_end_matches(';').trim();
        if sql.is_empty() {
            return Ok(vec![Response::EmptyQuery]);
        }

        // psql / drivers often emit session housekeeping we can no-op.
        let upper = sql.to_ascii_uppercase();
        if upper.starts_with("SET ")
            || upper.starts_with("SHOW ")
            || upper == "BEGIN"
            || upper == "COMMIT"
            || upper == "ROLLBACK"
            || upper.starts_with("BEGIN ")
            || upper.starts_with("COMMIT ")
            || upper.starts_with("ROLLBACK ")
        {
            let tag = if upper.starts_with("BEGIN") {
                "BEGIN"
            } else if upper.starts_with("COMMIT") {
                "COMMIT"
            } else if upper.starts_with("ROLLBACK") {
                "ROLLBACK"
            } else if upper.starts_with("SHOW") {
                "SHOW"
            } else {
                "SET"
            };
            return Ok(vec![Response::Execution(Tag::new(tag))]);
        }

        let plan = SqlEngine::plan(sql).map_err(sql_err)?;
        match plan {
            LogicalPlan::Insert { records, .. } => {
                let n = records.len();
                self.client.execute_sql(sql).await.map_err(sql_err)?;
                Ok(vec![Response::Execution(
                    Tag::new("INSERT").with_oid(0).with_rows(n),
                )])
            }
            LogicalPlan::Select { .. } => {
                let rows = self.client.execute_sql(sql).await.map_err(sql_err)?;
                Ok(vec![encode_select_response(rows)?])
            }
        }
    }
}

fn sql_err(e: crate::error::TakyonicError) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".into(),
        "XX000".into(),
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
        encoded.push(Ok(encoder.take_row()));
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

/// Factory wiring startup + simple-query handlers for [`pgwire::tokio::process_socket`].
pub struct TakyonicPgFactory {
    backend: Arc<TakyonicPgBackend>,
    startup: Arc<AcceptAnyCleartext>,
}

impl TakyonicPgFactory {
    /// Build handlers around a connected Smart Client.
    pub fn new(client: TakyonicClient) -> Self {
        Self {
            backend: Arc::new(TakyonicPgBackend::new(client)),
            startup: Arc::new(AcceptAnyCleartext::new()),
        }
    }
}

impl PgWireServerHandlers for TakyonicPgFactory {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        self.backend.clone()
    }

    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        self.startup.clone()
    }
}
