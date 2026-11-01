// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! MySQL protocol dispatch over Query Application session contracts.

use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use opensrv_mysql::{
    AsyncMysqlIntermediary, AsyncMysqlShim, CapabilityFlags, ErrorKind, InitWriter, ParamParser,
    QueryResultWriter, StatementMetaWriter,
};
use tokio::io::AsyncWrite;
use tokio::net::TcpStream;
use tracing::{info, warn};

use novarocks_query_application::cancellation::QueryCancellationReason;
use novarocks_query_application::client_connection::{
    ClientConnectionTerminationReason, ClientConnectionToken,
};
use novarocks_query_application::protocol_delivery::QuerySessionOutput as StatementResult;
use novarocks_query_application::session::{
    QuerySession, QuerySessionFactory, QuerySessionOpenRequest,
};
use novarocks_query_application::session_error::{QueryServiceError, QueryServiceErrorKind};
use novarocks_query_application::sql::admission::negotiated_query_statements;

use crate::connection_registry::{MysqlClientConnectionRegistration, MysqlConnectionClass};
use crate::{ClientDisconnectWatcher, MysqlClientConnectionRegistry, spawn_disconnect_watcher};

#[cfg(feature = "mem-1-m07-exact-mysql-write")]
fn write_exact_mysql_bound_marker_for_hook<W: std::io::Write>(
    writer: &mut W,
    hook: Option<&crate::mysql_write_gate::late_binding::MysqlWriteRelayHook>,
    connection: ClientConnectionToken,
    statement: novarocks_query_application::session_control::StatementToken,
    sql_sha256: [u8; 32],
) -> io::Result<()> {
    write_original_mysql_bound_marker(
        writer,
        hook.is_some(),
        "NOVAROCKS_EXACT_MYSQL_TARGET_BOUND",
        connection,
        statement,
        sql_sha256,
    )
}

/// One bounded source observation, not a gate snapshot or a token capability.
/// The caller passes the exact raw inputs of its successful original bind.
#[cfg(any(
    feature = "mem-1-m07-exact-mysql-write",
    feature = "mem-1-m07-closing-pressure"
))]
fn write_original_mysql_bound_marker<W: std::io::Write>(
    writer: &mut W,
    bound: bool,
    marker: &'static str,
    connection: ClientConnectionToken,
    statement: novarocks_query_application::session_control::StatementToken,
    exact_sql_sha256: [u8; 32],
) -> io::Result<()> {
    // No-hook, non-target and follow-up paths produce no observation or IO.
    if !bound {
        return Ok(());
    }
    use std::fmt::Write as _;
    struct Line {
        bytes: [u8; 384],
        length: usize,
    }
    impl std::fmt::Write for Line {
        fn write_str(&mut self, value: &str) -> std::fmt::Result {
            let end = self
                .length
                .checked_add(value.len())
                .ok_or(std::fmt::Error)?;
            if !value.is_ascii() || end > self.bytes.len() {
                return Err(std::fmt::Error);
            }
            self.bytes[self.length..end].copy_from_slice(value.as_bytes());
            self.length = end;
            Ok(())
        }
    }
    fn invalid(_: std::fmt::Error) -> io::Error {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "exact MySQL bound marker exceeds its fixed ASCII line",
        )
    }
    let mut line = Line {
        bytes: [0; 384],
        length: 0,
    };
    write!(&mut line,
        "{} connection_id={} connection_generation={} session_connection_id={} session_key={} statement_generation={} sql_sha256=",
        marker, connection.connection_id(), connection.generation(), connection.connection_id(),
        statement.session().key().get(), statement.generation(),
    ).map_err(invalid)?;
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in exact_sql_sha256 {
        let pair = [HEX[(byte >> 4) as usize], HEX[(byte & 15) as usize]];
        line.write_str(std::str::from_utf8(&pair).expect("fixed ASCII hex table"))
            .map_err(invalid)?;
    }
    line.write_str("\n").map_err(invalid)?;
    // Preserve the original write/flush error object. No println panic,
    // generic error replacement, gate snapshot or unbounded formatted String.
    writer.write_all(&line.bytes[..line.length])?;
    writer.flush()
}

async fn write_negotiated_statement<'writer, W: AsyncWrite + Unpin>(
    statement: StatementResult,
    results: QueryResultWriter<'writer, W>,
    more_results: bool,
) -> io::Result<crate::MysqlStatementWriteOutcome<'writer, W>> {
    match statement {
        StatementResult::Query(_) => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "MySQL query result has no original governed owner",
        )),
        StatementResult::GovernedQuery(result) => {
            crate::local_result_writer::write_local_result_one(result, results, more_results).await
        }
        StatementResult::StreamingQuery(result) => {
            let result = *result;
            crate::governed_result_writer::write_streaming_query_result_with_more(
                result,
                results,
                more_results,
            )
            .await
        }
        StatementResult::GovernedCompletion(result) => {
            let (completion, protocol) = result.into_parts();
            crate::terminal::write_governed_terminal_ok_with_more(
                completion,
                protocol,
                results,
                more_results,
            )
            .await
        }
        StatementResult::GovernedError(result) => {
            let (error, protocol) = result.into_parts();
            crate::write_governed_terminal_error(error, protocol, results)
                .await
                .map(|_| crate::MysqlStatementWriteOutcome::Terminated)
        }
        StatementResult::Ok => crate::write_terminal_ok_one(results)
            .await
            .map(crate::MysqlStatementWriteOutcome::Continue),
    }
}

/// Default upper bound for draining protocol tasks during an immediate
/// application shutdown.
pub const QUERY_APPLICATION_MYSQL_SESSION_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

/// Pure settings shared by the original caller and its startup projection.
/// Authentication starts at registry admission; command timing starts at the
/// first byte inside the original intermediary. No clock is started here.
pub(crate) struct MysqlInputPolicy {
    pub(crate) limits: opensrv_mysql::ProtocolLimits,
    pub(crate) auth_timeout: Duration,
    pub(crate) command_timeout: Duration,
    pub(crate) response_write_timeout: Duration,
}

pub(crate) fn mysql_input_policy(class: MysqlConnectionClass) -> MysqlInputPolicy {
    let mut limits = opensrv_mysql::ProtocolLimits::default();
    if class == MysqlConnectionClass::Control {
        limits.command_bytes = limits.diagnostic_bytes;
    }
    MysqlInputPolicy {
        limits,
        auth_timeout: Duration::from_secs(10),
        command_timeout: Duration::from_secs(10),
        response_write_timeout: Duration::from_secs(30),
    }
}

/// Runs a ready Query Application session factory until shutdown.
///
/// The adapter owns protocol-task draining; the caller supplies the already
/// composed session factory and the role-specific readiness action.
pub async fn serve_query_application_mysql_until_shutdown<F, R>(
    settings: crate::ResolvedMysqlListenerSettings,
    server_version: String,
    session_factory: Arc<dyn QuerySessionFactory>,
    connections: Arc<MysqlClientConnectionRegistry>,
    shutdown: F,
    on_ready: R,
) -> Result<(), String>
where
    F: Future<Output = ()> + Send,
    R: FnOnce(SocketAddr),
{
    let shutdown_factory = Arc::clone(&session_factory);
    let shutdown_connections = Arc::clone(&connections);
    serve_query_application_mysql_until_drain_then_shutdown(
        settings,
        server_version,
        session_factory,
        connections,
        async move {
            shutdown.await;
        },
        async move {
            shutdown_factory.cancel_all(QueryCancellationReason::ServerShutdown);
            shutdown_connections.terminate_all(ClientConnectionTerminationReason::ServerShutdown);
        },
        QUERY_APPLICATION_MYSQL_SESSION_DRAIN_TIMEOUT,
        on_ready,
    )
    .await
}

/// Stops accepting new sockets at `drain`, runs role-owned finalization, then
/// drains established Query Application protocol tasks.
pub async fn serve_query_application_mysql_until_drain_then_shutdown<F, G, R>(
    settings: crate::ResolvedMysqlListenerSettings,
    server_version: String,
    session_factory: Arc<dyn QuerySessionFactory>,
    connections: Arc<MysqlClientConnectionRegistry>,
    drain: F,
    finalize: G,
    cleanup_timeout: Duration,
    on_ready: R,
) -> Result<(), String>
where
    F: Future<Output = ()> + Send,
    G: Future<Output = ()> + Send,
    R: FnOnce(SocketAddr),
{
    serve_query_application_mysql_kernel(
        settings,
        server_version,
        session_factory,
        connections,
        drain,
        finalize,
        cleanup_timeout,
        on_ready,
        #[cfg(feature = "mem-1-m07-exact-mysql-write")]
        None,
        #[cfg(feature = "mem-1-m07-closing-pressure")]
        None,
    )
    .await
}

/// Opt-in binding stays inside the adapter; it carries no public Hub capability.
#[cfg(feature = "mem-1-m07-exact-mysql-write")]
pub async fn serve_query_application_mysql_until_drain_then_shutdown_fixture<F, G, R>(
    settings: crate::ResolvedMysqlListenerSettings,
    server_version: String,
    session_factory: Arc<dyn QuerySessionFactory>,
    connections: Arc<MysqlClientConnectionRegistry>,
    drain: F,
    finalize: G,
    cleanup_timeout: Duration,
    on_ready: R,
    binding: crate::exact_mysql_write_fixture::MysqlWriteFixtureListenerBinding,
) -> Result<(), String>
where
    F: Future<Output = ()> + Send,
    G: Future<Output = ()> + Send,
    R: FnOnce(SocketAddr),
{
    serve_query_application_mysql_kernel(
        settings,
        server_version,
        session_factory,
        connections,
        drain,
        finalize,
        cleanup_timeout,
        on_ready,
        Some(binding),
        #[cfg(feature = "mem-1-m07-closing-pressure")]
        None,
    )
    .await
}

#[cfg(feature = "mem-1-m07-closing-pressure")]
pub async fn serve_query_application_mysql_until_drain_then_shutdown_pressure<F, G, R>(
    settings: crate::ResolvedMysqlListenerSettings,
    server_version: String,
    session_factory: Arc<dyn QuerySessionFactory>,
    connections: Arc<MysqlClientConnectionRegistry>,
    drain: F,
    finalize: G,
    cleanup_timeout: Duration,
    on_ready: R,
    binding: crate::closing_pressure_fixture::ClosingPressureListenerBinding,
) -> Result<(), String>
where
    F: Future<Output = ()> + Send,
    G: Future<Output = ()> + Send,
    R: FnOnce(SocketAddr),
{
    serve_query_application_mysql_kernel(
        settings,
        server_version,
        session_factory,
        connections,
        drain,
        finalize,
        cleanup_timeout,
        on_ready,
        #[cfg(feature = "mem-1-m07-exact-mysql-write")]
        None,
        Some(binding),
    )
    .await
}

async fn serve_query_application_mysql_kernel<F, G, R>(
    settings: crate::ResolvedMysqlListenerSettings,
    server_version: String,
    session_factory: Arc<dyn QuerySessionFactory>,
    connections: Arc<MysqlClientConnectionRegistry>,
    drain: F,
    finalize: G,
    cleanup_timeout: Duration,
    on_ready: R,
    #[cfg(feature = "mem-1-m07-exact-mysql-write")] binding: Option<
        crate::exact_mysql_write_fixture::MysqlWriteFixtureListenerBinding,
    >,
    #[cfg(feature = "mem-1-m07-closing-pressure")] pressure_binding: Option<
        crate::closing_pressure_fixture::ClosingPressureListenerBinding,
    >,
) -> Result<(), String>
where
    F: Future<Output = ()> + Send,
    G: Future<Output = ()> + Send,
    R: FnOnce(SocketAddr),
{
    let (bind_addr, session_user) = settings.into_parts();
    let drain_registry = Arc::clone(&connections);
    #[cfg(any(
        feature = "mem-1-m07-exact-mysql-write",
        feature = "mem-1-m07-closing-pressure"
    ))]
    let mut fixture_joins = None;
    #[cfg(feature = "mem-1-m07-exact-mysql-write")]
    let fixture_hub = match binding {
        Some(binding) => {
            fixture_joins = Some(binding.joins);
            Some(binding.hub)
        }
        None => None,
    };
    #[cfg(feature = "mem-1-m07-closing-pressure")]
    let pressure_owner = match pressure_binding {
        Some(binding) => {
            if fixture_joins.is_some() {
                binding
                    .owner
                    .fail(crate::closing_pressure_gate::Failure::Identity);
                return Err("independent MySQL fixtures cannot share an active listener".into());
            }
            fixture_joins = Some(binding.joins);
            Some(binding.owner)
        }
        None => None,
    };
    #[cfg(any(
        feature = "mem-1-m07-exact-mysql-write",
        feature = "mem-1-m07-closing-pressure"
    ))]
    let fixture_watchers = fixture_joins.clone();
    let handler = move |stream, peer_addr| {
        // Acquire a finite position before creating the task, watcher or
        // intermediary and its protocol buffers. Full admission closes IO.
        let registration = connections.register().ok()?;
        #[cfg(any(
            feature = "mem-1-m07-exact-mysql-write",
            feature = "mem-1-m07-closing-pressure"
        ))]
        let watcher_permit = match &fixture_watchers {
            Some(joins) => Some(joins.reserve_watcher(registration.retain_owner()).ok()?),
            None => None,
        };
        Some(serve_registered_mysql_connection(
            session_user.clone(),
            server_version.clone(),
            Arc::clone(&session_factory),
            registration,
            stream,
            peer_addr,
            #[cfg(feature = "mem-1-m07-exact-mysql-write")]
            fixture_hub.clone(),
            #[cfg(any(
                feature = "mem-1-m07-exact-mysql-write",
                feature = "mem-1-m07-closing-pressure"
            ))]
            watcher_permit,
            #[cfg(any(
                feature = "mem-1-m07-exact-mysql-write",
                feature = "mem-1-m07-closing-pressure"
            ))]
            fixture_watchers.clone(),
            #[cfg(feature = "mem-1-m07-closing-pressure")]
            pressure_owner.clone(),
        ))
    };
    #[cfg(any(
        feature = "mem-1-m07-exact-mysql-write",
        feature = "mem-1-m07-closing-pressure"
    ))]
    let serve_result = match fixture_joins {
        Some(joins) => {
            crate::listener::serve_tcp_until_drain_then_shutdown_admitted_observed(
                bind_addr,
                drain,
                finalize,
                handler,
                on_ready,
                cleanup_timeout,
                joins,
            )
            .await
        }
        None => {
            crate::listener::serve_tcp_until_drain_then_shutdown_admitted(
                bind_addr,
                drain,
                finalize,
                handler,
                on_ready,
                cleanup_timeout,
            )
            .await
        }
    };
    #[cfg(not(any(
        feature = "mem-1-m07-exact-mysql-write",
        feature = "mem-1-m07-closing-pressure"
    )))]
    let serve_result = crate::listener::serve_tcp_until_drain_then_shutdown_admitted(
        bind_addr,
        drain,
        finalize,
        handler,
        on_ready,
        cleanup_timeout,
    )
    .await;
    tokio::time::timeout(cleanup_timeout, drain_registry.wait_drained())
        .await
        .map_err(|_| {
            "MySQL connection owners did not drain before the cleanup deadline".to_string()
        })?;
    serve_result
}

pub async fn serve_query_application_mysql_connection(
    user: String,
    server_version: String,
    session_factory: Arc<dyn QuerySessionFactory>,
    connections: Arc<MysqlClientConnectionRegistry>,
    stream: TcpStream,
    peer_addr: SocketAddr,
) {
    let registration = match connections.register() {
        Ok(registration) => registration,
        Err(error) => {
            warn!(
                "reject standalone mysql connection because the connection registry is exhausted: peer={}, error={:?}",
                peer_addr, error
            );
            return;
        }
    };
    serve_registered_mysql_connection(
        user,
        server_version,
        session_factory,
        registration,
        stream,
        peer_addr,
        #[cfg(feature = "mem-1-m07-exact-mysql-write")]
        None,
        #[cfg(any(
            feature = "mem-1-m07-exact-mysql-write",
            feature = "mem-1-m07-closing-pressure"
        ))]
        None,
        #[cfg(any(
            feature = "mem-1-m07-exact-mysql-write",
            feature = "mem-1-m07-closing-pressure"
        ))]
        None,
        #[cfg(feature = "mem-1-m07-closing-pressure")]
        None,
    )
    .await;
}

#[cfg(any(
    feature = "mem-1-m07-exact-mysql-write",
    feature = "mem-1-m07-closing-pressure"
))]
enum FixtureMysqlWriter {
    Raw(tokio::net::tcp::OwnedWriteHalf),
    #[cfg(feature = "mem-1-m07-exact-mysql-write")]
    Gated(
        crate::mysql_write_gate::late_binding::InitiallyRawMysqlWriter<
            tokio::net::tcp::OwnedWriteHalf,
        >,
    ),
    #[cfg(feature = "mem-1-m07-closing-pressure")]
    Pressure(
        crate::closing_pressure_gate::relay::InitiallyRawPressureWriter<
            tokio::net::tcp::OwnedWriteHalf,
        >,
    ),
}
#[cfg(any(
    feature = "mem-1-m07-exact-mysql-write",
    feature = "mem-1-m07-closing-pressure"
))]
impl AsyncWrite for FixtureMysqlWriter {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bytes: &[u8],
    ) -> std::task::Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Raw(io) => std::pin::Pin::new(io).poll_write(cx, bytes),
            #[cfg(feature = "mem-1-m07-exact-mysql-write")]
            Self::Gated(io) => std::pin::Pin::new(io).poll_write(cx, bytes),
            #[cfg(feature = "mem-1-m07-closing-pressure")]
            Self::Pressure(io) => std::pin::Pin::new(io).poll_write(cx, bytes),
        }
    }
    fn poll_write_vectored(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bytes: &[io::IoSlice<'_>],
    ) -> std::task::Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Raw(io) => std::pin::Pin::new(io).poll_write_vectored(cx, bytes),
            #[cfg(feature = "mem-1-m07-exact-mysql-write")]
            Self::Gated(io) => std::pin::Pin::new(io).poll_write_vectored(cx, bytes),
            #[cfg(feature = "mem-1-m07-closing-pressure")]
            Self::Pressure(io) => std::pin::Pin::new(io).poll_write_vectored(cx, bytes),
        }
    }
    fn is_write_vectored(&self) -> bool {
        match self {
            Self::Raw(io) => io.is_write_vectored(),
            #[cfg(feature = "mem-1-m07-exact-mysql-write")]
            Self::Gated(io) => io.is_write_vectored(),
            #[cfg(feature = "mem-1-m07-closing-pressure")]
            Self::Pressure(io) => io.is_write_vectored(),
        }
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Raw(io) => std::pin::Pin::new(io).poll_flush(cx),
            #[cfg(feature = "mem-1-m07-exact-mysql-write")]
            Self::Gated(io) => std::pin::Pin::new(io).poll_flush(cx),
            #[cfg(feature = "mem-1-m07-closing-pressure")]
            Self::Pressure(io) => std::pin::Pin::new(io).poll_flush(cx),
        }
    }
    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Raw(io) => std::pin::Pin::new(io).poll_shutdown(cx),
            #[cfg(feature = "mem-1-m07-exact-mysql-write")]
            Self::Gated(io) => std::pin::Pin::new(io).poll_shutdown(cx),
            #[cfg(feature = "mem-1-m07-closing-pressure")]
            Self::Pressure(io) => std::pin::Pin::new(io).poll_shutdown(cx),
        }
    }
}

#[cfg(any(
    feature = "mem-1-m07-exact-mysql-write",
    feature = "mem-1-m07-closing-pressure"
))]
fn fixture_original_writer(
    writer: tokio::net::tcp::OwnedWriteHalf,
    connection: ClientConnectionToken,
    class: MysqlConnectionClass,
    #[cfg(feature = "mem-1-m07-exact-mysql-write")] legacy: Option<
        Arc<crate::mysql_write_gate::late_binding::MysqlWriteGateHub>,
    >,
    #[cfg(feature = "mem-1-m07-closing-pressure")] pressure: Option<
        Arc<crate::closing_pressure_gate::PressureOwner>,
    >,
) -> FixtureMysqlWriter {
    if class == MysqlConnectionClass::Control {
        return FixtureMysqlWriter::Raw(writer);
    }
    #[cfg(feature = "mem-1-m07-closing-pressure")]
    if let Some(owner) = pressure {
        return FixtureMysqlWriter::Pressure(
            crate::closing_pressure_gate::relay::InitiallyRawPressureWriter::new(
                writer, connection, owner,
            ),
        );
    }
    #[cfg(feature = "mem-1-m07-exact-mysql-write")]
    if let Some(hub) = legacy {
        return FixtureMysqlWriter::Gated(
            crate::mysql_write_gate::late_binding::InitiallyRawMysqlWriter::new(
                writer, connection, hub,
            ),
        );
    }
    FixtureMysqlWriter::Raw(writer)
}

async fn serve_registered_mysql_connection(
    user: String,
    server_version: String,
    session_factory: Arc<dyn QuerySessionFactory>,
    mut registration: MysqlClientConnectionRegistration,
    stream: TcpStream,
    peer_addr: SocketAddr,
    #[cfg(feature = "mem-1-m07-exact-mysql-write")] fixture_hub: Option<
        Arc<crate::mysql_write_gate::late_binding::MysqlWriteGateHub>,
    >,
    #[cfg(any(
        feature = "mem-1-m07-exact-mysql-write",
        feature = "mem-1-m07-closing-pressure"
    ))]
    watcher_permit: Option<crate::listener::WatcherPermit>,
    #[cfg(any(
        feature = "mem-1-m07-exact-mysql-write",
        feature = "mem-1-m07-closing-pressure"
    ))]
    fixture_protocol: Option<Arc<crate::listener::MysqlFixtureSessionJoins>>,
    #[cfg(feature = "mem-1-m07-closing-pressure")] pressure_owner: Option<
        Arc<crate::closing_pressure_gate::PressureOwner>,
    >,
) {
    let connection = registration.token();
    let session: Arc<OnceLock<Arc<dyn QuerySession>>> = Arc::new(OnceLock::new());
    let session_for_disconnect = Arc::clone(&session);
    let watcher_owner = registration.retain_owner();
    let disconnect_watcher = spawn_disconnect_watcher(&stream, move || {
        let _owner = &watcher_owner;
        if let Some(session) = session_for_disconnect.get() {
            session.cancel_current(QueryCancellationReason::ClientDisconnected);
        }
    });
    #[cfg(any(
        feature = "mem-1-m07-exact-mysql-write",
        feature = "mem-1-m07-closing-pressure"
    ))]
    let disconnect_watcher = match watcher_permit {
        Some(permit) => permit.attach(disconnect_watcher),
        None => disconnect_watcher,
    };
    let shim = QueryApplicationMysqlShim::new(
        user,
        connection,
        session_factory,
        Arc::clone(&session),
        disconnect_watcher,
        server_version,
    )
    .with_connection_class(registration.class());
    #[cfg(feature = "mem-1-m07-exact-mysql-write")]
    let shim = shim.with_fixture_hub(if registration.class() == MysqlConnectionClass::Ordinary {
        fixture_hub.clone()
    } else {
        None
    });
    #[cfg(feature = "mem-1-m07-closing-pressure")]
    let shim =
        shim.with_pressure_owner(if registration.class() == MysqlConnectionClass::Ordinary {
            pressure_owner.clone()
        } else {
            None
        });
    let (reader, writer) = stream.into_split();
    #[cfg(any(
        feature = "mem-1-m07-exact-mysql-write",
        feature = "mem-1-m07-closing-pressure"
    ))]
    let writer = fixture_original_writer(
        writer,
        connection,
        registration.class(),
        #[cfg(feature = "mem-1-m07-exact-mysql-write")]
        fixture_hub,
        #[cfg(feature = "mem-1-m07-closing-pressure")]
        pressure_owner,
    );
    let result = {
        let input_policy = mysql_input_policy(registration.class());
        let intermediary = AsyncMysqlIntermediary::run_with_input_deadlines(
            shim,
            reader,
            writer,
            &crate::MYSQL_INTERMEDIARY_OPTIONS,
            input_policy.limits,
            tokio::time::Instant::from_std(registration.admitted_at()) + input_policy.auth_timeout,
            input_policy.command_timeout,
            input_policy.response_write_timeout,
        );
        tokio::pin!(intermediary);
        tokio::select! {
            termination = registration.termination_receiver() => {
                match termination {
                    Ok(reason) => {
                        if let Some(session) = session.get() {
                            session.cancel_current(query_cancellation_reason_for_connection_termination(&reason));
                        }
                        info!(
                            "terminate standalone mysql connection: peer={}, connection_id={}, reason={:?}",
                            peer_addr,
                            connection.connection_id(),
                            reason
                        );
                    }
                    Err(error) => {
                        warn!(
                            "standalone mysql connection termination signal closed unexpectedly: peer={}, connection_id={}, error={}",
                            peer_addr,
                            connection.connection_id(),
                            error
                        );
                    }
                }
                None
            }
            result = &mut intermediary => Some(result),
        }
    };
    if let Some(Err(err)) = result {
        #[cfg(feature = "mem-1-m07-closing-pressure")]
        if fixture_protocol
            .as_ref()
            .is_some_and(|observer| observer.is_closing_pressure())
        {
            observe_pressure_protocol_failure(
                fixture_protocol.as_ref().unwrap(),
                connection,
                registration.class(),
                peer_addr,
                err,
            );
            return;
        }
        warn!(
            "standalone mysql connection failed: peer={}, connection_id={}, err={}",
            peer_addr,
            connection.connection_id(),
            err
        );
        #[cfg(any(
            feature = "mem-1-m07-exact-mysql-write",
            feature = "mem-1-m07-closing-pressure"
        ))]
        if let Some(observation) = fixture_protocol {
            observation.observe_protocol_failure(connection, registration.class(), err);
        }
    }
}

#[cfg(feature = "mem-1-m07-closing-pressure")]
fn observe_pressure_protocol_failure(
    observation: &crate::listener::MysqlFixtureSessionJoins,
    connection: ClientConnectionToken,
    class: MysqlConnectionClass,
    peer: SocketAddr,
    original: io::Error,
) {
    // Never execute unknown source formatters before the original error is retained.
    let kind = original.kind();
    let raw_os = original.raw_os_error();
    observation.observe_protocol_failure(connection, class, original);
    warn!(
        "Closing pressure original MySQL IO: peer={} connection_id={} generation={} class={:?} kind={:?} raw_os={:?}",
        peer,
        connection.connection_id(),
        connection.generation(),
        class,
        kind,
        raw_os
    );
}

#[cfg(feature = "mem-1-m07-closing-pressure")]
struct PressureQueryRejection {
    original: QueryServiceError,
    guard: io::Error,
}
#[cfg(feature = "mem-1-m07-closing-pressure")]
impl std::fmt::Display for PressureQueryRejection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "pressure query rejected: original_kind={:?} guard_kind={:?}",
            self.original.kind(),
            self.guard.kind()
        )
    }
}
#[cfg(feature = "mem-1-m07-closing-pressure")]
impl std::fmt::Debug for PressureQueryRejection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}
#[cfg(feature = "mem-1-m07-closing-pressure")]
impl std::error::Error for PressureQueryRejection {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.original)
    }
}

fn query_cancellation_reason_for_connection_termination(
    reason: &ClientConnectionTerminationReason,
) -> QueryCancellationReason {
    match reason {
        ClientConnectionTerminationReason::ExplicitKillConnection { requester_session } => {
            QueryCancellationReason::ExplicitKillConnection {
                requester_session: *requester_session,
            }
        }
        ClientConnectionTerminationReason::ServerShutdown => {
            QueryCancellationReason::ServerShutdown
        }
    }
}

pub struct QueryApplicationMysqlShim {
    user: String,
    connection: ClientConnectionToken,
    session_factory: Arc<dyn QuerySessionFactory>,
    session: Arc<OnceLock<Arc<dyn QuerySession>>>,
    _disconnect_watcher: ClientDisconnectWatcher,
    server_version: String,
    connection_class: MysqlConnectionClass,
    #[cfg(feature = "mem-1-m07-exact-mysql-write")]
    fixture_hub: Option<Arc<crate::mysql_write_gate::late_binding::MysqlWriteGateHub>>,
    #[cfg(feature = "mem-1-m07-closing-pressure")]
    pressure_owner: Option<Arc<crate::closing_pressure_gate::PressureOwner>>,
}

impl QueryApplicationMysqlShim {
    pub fn new(
        user: String,
        connection: ClientConnectionToken,
        session_factory: Arc<dyn QuerySessionFactory>,
        session: Arc<OnceLock<Arc<dyn QuerySession>>>,
        disconnect_watcher: ClientDisconnectWatcher,
        server_version: String,
    ) -> Self {
        Self {
            user,
            connection,
            session_factory,
            session,
            _disconnect_watcher: disconnect_watcher,
            server_version,
            connection_class: MysqlConnectionClass::Ordinary,
            #[cfg(feature = "mem-1-m07-exact-mysql-write")]
            fixture_hub: None,
            #[cfg(feature = "mem-1-m07-closing-pressure")]
            pressure_owner: None,
        }
    }

    fn with_connection_class(mut self, class: MysqlConnectionClass) -> Self {
        self.connection_class = class;
        self
    }

    #[cfg(feature = "mem-1-m07-exact-mysql-write")]
    fn with_fixture_hub(
        mut self,
        hub: Option<Arc<crate::mysql_write_gate::late_binding::MysqlWriteGateHub>>,
    ) -> Self {
        self.fixture_hub = hub;
        self
    }

    #[cfg(feature = "mem-1-m07-closing-pressure")]
    fn with_pressure_owner(
        mut self,
        owner: Option<Arc<crate::closing_pressure_gate::PressureOwner>>,
    ) -> Self {
        self.pressure_owner = owner;
        self
    }

    #[cfg(any(
        feature = "mem-1-m07-exact-mysql-write",
        feature = "mem-1-m07-closing-pressure"
    ))]
    fn reject_fixture_non_streaming(&self, sql_sha256: [u8; 32]) -> io::Result<()> {
        #[cfg(feature = "mem-1-m07-exact-mysql-write")]
        if let Some(hub) = &self.fixture_hub {
            hub.reject_non_streaming_target(self.connection, sql_sha256)?;
        }
        #[cfg(feature = "mem-1-m07-closing-pressure")]
        if let Some(owner) = &self.pressure_owner {
            owner.reject_non_streaming(self.connection, sql_sha256)?;
        }
        Ok(())
    }

    #[cfg(any(
        feature = "mem-1-m07-exact-mysql-write",
        feature = "mem-1-m07-closing-pressure"
    ))]
    fn retain_fixture_query_error(
        &self,
        sql_sha256: [u8; 32],
        original: QueryServiceError,
    ) -> io::Result<QueryServiceError> {
        match self.reject_fixture_non_streaming(sql_sha256) {
            Ok(()) => Ok(original),
            Err(guard) => {
                #[cfg(feature = "mem-1-m07-closing-pressure")]
                if self.pressure_owner.is_some() {
                    return Err(io::Error::new(
                        guard.kind(),
                        PressureQueryRejection { original, guard },
                    ));
                }
                Err(guard)
            }
        }
    }

    fn session(&self) -> Result<&Arc<dyn QuerySession>, QueryServiceError> {
        self.session.get().ok_or_else(|| {
            QueryServiceError::new(
                QueryServiceErrorKind::PermissionDenied,
                "session is not authenticated",
            )
        })
    }
}

impl Drop for QueryApplicationMysqlShim {
    fn drop(&mut self) {
        if let Some(session) = self.session.get() {
            session.close();
        }
    }
}

#[async_trait]
impl<W: AsyncWrite + Send + Unpin> AsyncMysqlShim<W> for QueryApplicationMysqlShim {
    type Error = io::Error;

    fn permits_query_shortcuts(&self) -> bool {
        #[cfg(feature = "mem-1-m07-closing-pressure")]
        if self
            .pressure_owner
            .as_ref()
            .is_some_and(|owner| owner.is_selected_connection(self.connection))
        {
            return false;
        }
        #[cfg(feature = "mem-1-m07-exact-mysql-write")]
        if self
            .fixture_hub
            .as_ref()
            .is_some_and(|hub| hub.is_selected_connection(self.connection))
        {
            return false;
        }
        self.connection_class == MysqlConnectionClass::Ordinary
    }

    fn version(&self) -> String {
        format!("{}-standalone-mysql", self.server_version)
    }

    fn connect_id(&self) -> u32 {
        self.connection.connection_id()
    }

    async fn authenticate(
        &self,
        auth_plugin: &str,
        username: &[u8],
        salt: &[u8],
        auth_data: &[u8],
    ) -> bool {
        if !crate::authenticate_empty_password(&self.user, auth_plugin, username, salt, auth_data) {
            return false;
        }
        let session = match self
            .session_factory
            .open_session(QuerySessionOpenRequest::new(
                self.connection,
                self.user.clone(),
            )) {
            Ok(session) => session,
            Err(error) => {
                warn!(
                    "failed to open frontend query session for connection_id={}: {}",
                    self.connection.connection_id(),
                    error
                );
                return false;
            }
        };
        self.session.set(session).is_ok()
    }

    async fn on_prepare<'a>(
        &'a mut self,
        _query: &'a str,
        info: StatementMetaWriter<'a, W>,
    ) -> io::Result<()> {
        if self.connection_class == MysqlConnectionClass::Control {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "control connection only accepts KILL",
            ));
        }
        info.error(
            ErrorKind::ER_NOT_SUPPORTED_YET,
            b"prepared statements are not supported in standalone server v1",
        )
        .await
    }

    async fn on_execute<'a>(
        &'a mut self,
        _id: u32,
        _params: ParamParser<'a>,
        results: QueryResultWriter<'a, W>,
    ) -> io::Result<()> {
        if self.connection_class == MysqlConnectionClass::Control {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "control connection only accepts KILL",
            ));
        }
        results
            .error(
                ErrorKind::ER_NOT_SUPPORTED_YET,
                b"prepared statements are not supported in standalone server v1",
            )
            .await
    }

    async fn on_close<'a>(&'a mut self, _stmt: u32) {}

    async fn on_init<'a>(
        &'a mut self,
        schema: &'a str,
        writer: InitWriter<'a, W>,
    ) -> io::Result<()> {
        if self.connection_class == MysqlConnectionClass::Control {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "control connection only accepts KILL",
            ));
        }
        let session = match self.session() {
            Ok(session) => session,
            Err(error) => {
                return writer
                    .error(crate::mysql_error_kind(&error), error.message().as_bytes())
                    .await;
            }
        };
        let (statement, terminal) = match session
            .init_database(&crate::normalize_init_database_schema(schema))
            .await
        {
            Ok(statement) => statement.into_parts(),
            Err(error) => {
                writer
                    .error(crate::mysql_error_kind(&error), error.message().as_bytes())
                    .await?;
                return Ok(());
            }
        };
        let outcome = match statement {
            StatementResult::Ok => writer.ok().await,
            StatementResult::GovernedCompletion(result) => {
                let (completion, protocol) = result.into_parts();
                crate::write_governed_init_ok(completion, protocol, writer).await
            }
            StatementResult::GovernedError(result) => {
                let (error, protocol) = result.into_parts();
                crate::write_governed_init_error(error, protocol, writer).await
            }
            StatementResult::Query(_)
            | StatementResult::GovernedQuery(_)
            | StatementResult::StreamingQuery(_) => {
                writer
                    .error(
                        ErrorKind::ER_UNKNOWN_ERROR,
                        b"COM_INIT_DB returned a non-terminal query result",
                    )
                    .await
            }
        };
        terminal.complete();
        outcome
    }

    async fn on_query<'a>(
        &'a mut self,
        query: &'a str,
        results: QueryResultWriter<'a, W>,
    ) -> io::Result<()> {
        #[cfg(any(
            feature = "mem-1-m07-exact-mysql-write",
            feature = "mem-1-m07-closing-pressure"
        ))]
        let fixture_sql_sha256: [u8; 32] = {
            use sha2::{Digest, Sha256};
            Sha256::digest(query.as_bytes()).into()
        };
        #[cfg(feature = "mem-1-m07-exact-mysql-write")]
        if let Some(hub) = &self.fixture_hub {
            if hub.is_selected_connection(self.connection)
                && negotiated_query_statements(query).is_ok_and(|statements| statements.len() > 1)
            {
                hub.reject_unsupported_batch(self.connection)?;
            }
        }
        #[cfg(feature = "mem-1-m07-closing-pressure")]
        if let Some(owner) = &self.pressure_owner {
            if negotiated_query_statements(query).is_ok_and(|statements| statements.len() > 1) {
                owner.reject_unsupported_batch(self.connection)?;
            }
        }
        if self.connection_class == MysqlConnectionClass::Control
            && let Err(error) =
                novarocks_query_application::sql::admission::admit_control_connection_batch(query)
        {
            return results
                .reject_connection(crate::mysql_error_kind(&error), error.message().as_bytes())
                .await;
        }
        let session = match self.session() {
            Ok(session) => session,
            Err(error) => {
                #[cfg(any(
                    feature = "mem-1-m07-exact-mysql-write",
                    feature = "mem-1-m07-closing-pressure"
                ))]
                let error = self.retain_fixture_query_error(fixture_sql_sha256, error)?;
                return results
                    .error(crate::mysql_error_kind(&error), error.message().as_bytes())
                    .await;
            }
        };
        let capabilities = results.client_capabilities();
        let multi_results_negotiated = capabilities
            .contains(CapabilityFlags::CLIENT_MULTI_STATEMENTS)
            && capabilities.contains(CapabilityFlags::CLIENT_MULTI_RESULTS);
        let statements = if multi_results_negotiated {
            match negotiated_query_statements(query) {
                Ok(statements) => statements,
                Err(error) => {
                    #[cfg(any(
                        feature = "mem-1-m07-exact-mysql-write",
                        feature = "mem-1-m07-closing-pressure"
                    ))]
                    let error = self.retain_fixture_query_error(fixture_sql_sha256, error)?;
                    return results
                        .error(crate::mysql_error_kind(&error), error.message().as_bytes())
                        .await;
                }
            }
        } else {
            Vec::new()
        };
        if statements.len() > 1 {
            #[cfg(feature = "mem-1-m07-exact-mysql-write")]
            if let Some(hub) = &self.fixture_hub {
                hub.reject_unsupported_batch(self.connection)?;
            }
            let mut results = results;
            let count = statements.len();
            for (index, statement_sql) in statements.into_iter().enumerate() {
                let statement = match session.execute_statement(statement_sql).await {
                    Ok(statement) => statement,
                    Err(error) => {
                        return results
                            .error(crate::mysql_error_kind(&error), error.message().as_bytes())
                            .await;
                    }
                };
                let (statement, terminal) = statement.into_parts();
                let outcome =
                    write_negotiated_statement(statement, results, index + 1 < count).await;
                terminal.complete();
                match outcome? {
                    crate::MysqlStatementWriteOutcome::Continue(next) => results = next,
                    crate::MysqlStatementWriteOutcome::Terminated => return Ok(()),
                }
            }
            return results.no_more_results().await;
        }
        let (statement, terminal) = match session.execute_batch(query).await {
            Ok(statement) => statement.into_parts(),
            Err(error) => {
                #[cfg(any(
                    feature = "mem-1-m07-exact-mysql-write",
                    feature = "mem-1-m07-closing-pressure"
                ))]
                let error = self.retain_fixture_query_error(fixture_sql_sha256, error)?;
                return results
                    .error(crate::mysql_error_kind(&error), error.message().as_bytes())
                    .await;
            }
        };
        #[cfg(any(
            feature = "mem-1-m07-exact-mysql-write",
            feature = "mem-1-m07-closing-pressure"
        ))]
        if !matches!(&statement, StatementResult::StreamingQuery(_)) {
            if let Err(error) = self.reject_fixture_non_streaming(fixture_sql_sha256) {
                drop(statement);
                terminal.complete();
                return Err(error);
            }
        }
        let outcome = match statement {
            StatementResult::Query(_) => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "MySQL query result has no original governed owner",
            )),
            StatementResult::GovernedQuery(result) => {
                crate::write_governed_query_result(result, results).await
            }
            StatementResult::StreamingQuery(result) => {
                let result = *result;
                async {
                    #[cfg(feature = "mem-1-m07-closing-pressure")]
                    if let Some(owner) = &self.pressure_owner {
                        let token = result.statement_token().ok_or_else(|| {
                            owner.fail(crate::closing_pressure_gate::Failure::Identity);
                            io::Error::new(io::ErrorKind::InvalidData, "pressure streaming result has no original statement token")
                        })?;
                        let hook = owner.bind_relay(self.connection, self.session.get().and_then(|session| session.connection_session(self.connection)).ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "fixture connection has no original session association"))?, token, fixture_sql_sha256)?;
                        if hook.is_some() {
                            owner.validate_streaming_owner(&result)?;
                            let stdout = std::io::stdout();
                            let mut output = stdout.lock();
                            if let Err(source) = write_original_mysql_bound_marker(
                                &mut output, true, "NOVAROCKS_CLOSING_PRESSURE_TARGET_BOUND",
                                self.connection, token, fixture_sql_sha256,
                            ) {
                                owner.fail(crate::closing_pressure_gate::Failure::Transition);
                                return Err(source);
                            }
                        }
                        let outcome = crate::governed_result_writer::write_streaming_query_result_with_pressure(result, results, hook).await;
                        if outcome.as_ref().is_err_and(|source| {
                            !crate::closing_pressure_gate::relay::PressureCapacityEof::from_error(source)
                                .is_some_and(|eof| eof.matches(self.connection))
                        }) {
                            owner.fail(crate::closing_pressure_gate::Failure::Transition);
                        }
                        return outcome;
                    }
                #[cfg(feature = "mem-1-m07-exact-mysql-write")]
                {
                    // Keep the original terminal completion below on every fixture error.
                    async {
                        if let Some(hub) = &self.fixture_hub {
                            let token = result.statement_token().ok_or_else(|| {
                                hub.fail_selected(self.connection, crate::mysql_write_gate::GateFailure::Identity);
                                io::Error::new(io::ErrorKind::InvalidData, "fixture streaming result has no original statement token")
                            })?;
                            let hook = hub.bind_statement(self.connection, self.session.get().and_then(|session| session.connection_session(self.connection)).ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "fixture connection has no original session association"))?, token, fixture_sql_sha256)?;
                            if hook.is_some() {
                                // Copy only the raw inputs of this ONE successful original bind.
                                // Stdout failure follows the existing retained protocol IO path.
                                let stdout = std::io::stdout();
                                let mut output = stdout.lock();
                                if let Err(error) = write_exact_mysql_bound_marker_for_hook(
                                    &mut output, hook.as_ref(), self.connection, token, fixture_sql_sha256,
                                ) {
                                    hub.fail_selected(self.connection, crate::mysql_write_gate::GateFailure::Transition);
                                    return Err(error);
                                }
                            }
                            let outcome = crate::governed_result_writer::write_streaming_query_result_with_gate(
                                result, results, hook,
                            ).await;
                            if outcome.as_ref().is_err_and(|error| {
                                !crate::mysql_write_gate::late_binding::PrescribedRelayEof::from_error(error)
                                    .is_some_and(|eof| eof.matches(self.connection, Some(token)))
                            }) {
                                // A bounded fixture summary keeps original scope first cause;
                                // the original typed IO error is still returned unchanged.
                                hub.fail_selected(self.connection, crate::mysql_write_gate::GateFailure::Transition);
                            }
                            outcome
                        } else {
                            crate::write_streaming_query_result(result, results).await
                        }
                    }.await
                }
                #[cfg(not(feature = "mem-1-m07-exact-mysql-write"))]
                crate::write_streaming_query_result(result, results).await
                }.await
            }
            StatementResult::GovernedCompletion(result) => {
                let (completion, protocol) = result.into_parts();
                crate::write_governed_terminal_ok(completion, protocol, results).await
            }
            StatementResult::GovernedError(result) => {
                let (error, protocol) = result.into_parts();
                crate::write_governed_terminal_error(error, protocol, results).await
            }
            StatementResult::Ok => crate::write_terminal_ok(results).await,
        };
        terminal.complete();
        outcome
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;

    struct RawResultFactory;
    struct RawResultSession;
    impl QuerySessionFactory for RawResultFactory {
        fn open_statement_session(
            &self,
            _: novarocks_query_application::session::StatementSessionRequest,
        ) -> Result<
            Arc<dyn QuerySession>,
            novarocks_query_application::session_value::StatementSessionError,
        > {
            unreachable!("this fixture exercises the connection session entry")
        }

        fn open_session(
            &self,
            _: QuerySessionOpenRequest,
        ) -> Result<Arc<dyn QuerySession>, QueryServiceError> {
            Ok(Arc::new(RawResultSession))
        }
        fn cancel_all(&self, _: QueryCancellationReason) {}
    }
    #[async_trait::async_trait]
    impl QuerySession for RawResultSession {
        #[cfg(novarocks_dev_controls)]
        fn connection_session(
            &self,
            _: ClientConnectionToken,
        ) -> Option<novarocks_query_application::session_control::SessionToken> {
            None
        }

        async fn init_database(
            &self,
            _: &str,
        ) -> Result<novarocks_query_application::session::QuerySessionStatement, QueryServiceError>
        {
            Ok(
                novarocks_query_application::session::QuerySessionStatement::output_owned(
                    StatementResult::Ok,
                    novarocks_query_application::statement_kind::StatementKind::Use,
                ),
            )
        }
        async fn execute_statement(
            &self,
            _: &str,
        ) -> Result<novarocks_query_application::session::QuerySessionStatement, QueryServiceError>
        {
            Ok(
                novarocks_query_application::session::QuerySessionStatement::output_owned(
                    StatementResult::Query(
                        novarocks_query_application::api::build_string_query_result(
                            "raw",
                            vec!["must not be published".to_string()],
                        )
                        .unwrap(),
                    ),
                    novarocks_query_application::statement_kind::StatementKind::Query,
                ),
            )
        }
        async fn execute_batch(
            &self,
            sql: &str,
        ) -> Result<novarocks_query_application::session::QuerySessionStatement, QueryServiceError>
        {
            self.execute_statement(sql).await
        }
        fn cancel_current(&self, _: QueryCancellationReason) {}
        fn close(&self) {}
    }

    #[tokio::test]
    async fn raw_result_without_original_owner_is_refused_before_metadata_in_both_protocol_modes() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::{TcpListener, TcpStream};
        async fn send(stream: &mut TcpStream, sequence: u8, body: &[u8]) {
            let mut header = (body.len() as u32).to_le_bytes();
            header[3] = sequence;
            stream.write_all(&header).await.unwrap();
            stream.write_all(body).await.unwrap();
        }
        async fn read(stream: &mut TcpStream) -> Vec<u8> {
            let mut header = [0; 4];
            stream.read_exact(&mut header).await.unwrap();
            header[3] = 0;
            let length = u32::from_le_bytes(header) as usize;
            assert!(length < 4096);
            let mut body = vec![0; length];
            stream.read_exact(&mut body).await.unwrap();
            body
        }
        tokio::time::timeout(Duration::from_secs(5), async {
            for negotiated in [false, true] {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let address = listener.local_addr().unwrap();
                let server = tokio::spawn(async move {
                    let (stream, _) = listener.accept().await.unwrap();
                    let (read, write) = stream.into_split();
                    opensrv_mysql::AsyncMysqlIntermediary::run_with_options(
                        QueryApplicationMysqlShim::new(
                            "root".into(),
                            ClientConnectionToken::new(91, 1).unwrap(),
                            Arc::new(RawResultFactory),
                            Arc::new(OnceLock::new()),
                            ClientDisconnectWatcher::inactive(),
                            "test".into(),
                        ),
                        read,
                        write,
                        &crate::MYSQL_INTERMEDIARY_OPTIONS,
                    )
                    .await
                });
                let mut client = TcpStream::connect(address).await.unwrap();
                assert_eq!(read(&mut client).await[0], 10);
                let mut flags = CapabilityFlags::CLIENT_PROTOCOL_41
                    | CapabilityFlags::CLIENT_SECURE_CONNECTION
                    | CapabilityFlags::CLIENT_PLUGIN_AUTH;
                if negotiated {
                    flags |= CapabilityFlags::CLIENT_MULTI_STATEMENTS
                        | CapabilityFlags::CLIENT_MULTI_RESULTS;
                }
                let mut auth = Vec::new();
                auth.extend_from_slice(&flags.bits().to_le_bytes());
                auth.extend_from_slice(&(64_u32 * 1024 * 1024).to_le_bytes());
                auth.push(33);
                auth.extend_from_slice(&[0; 23]);
                auth.extend_from_slice(b"root\0");
                auth.push(0);
                auth.extend_from_slice(b"mysql_native_password\0");
                send(&mut client, 1, &auth).await;
                assert_eq!(read(&mut client).await[0], 0);
                send(&mut client, 0, b"\x03SELECT raw_result").await;
                let mut first = [0; 1];
                assert_eq!(
                    client.read(&mut first).await.unwrap(),
                    0,
                    "raw schema/data must never reach the socket"
                );
                let error = server.await.unwrap().unwrap_err();
                assert_eq!(error.kind(), io::ErrorKind::InvalidData);
                assert!(error.to_string().contains("original governed owner"));
            }
        })
        .await
        .unwrap();
    }

    struct CancellationProbeFactory {
        cancelled: Arc<AtomicBool>,
    }

    impl QuerySessionFactory for CancellationProbeFactory {
        fn open_statement_session(
            &self,
            _: novarocks_query_application::session::StatementSessionRequest,
        ) -> Result<
            Arc<dyn QuerySession>,
            novarocks_query_application::session_value::StatementSessionError,
        > {
            unreachable!("this fixture exercises the connection session entry")
        }

        fn open_session(
            &self,
            _request: QuerySessionOpenRequest,
        ) -> Result<Arc<dyn QuerySession>, QueryServiceError> {
            Err(QueryServiceError::new(
                QueryServiceErrorKind::Internal,
                "test session factory must not open a session",
            ))
        }

        fn cancel_all(&self, _reason: QueryCancellationReason) {
            self.cancelled.store(true, Ordering::SeqCst);
        }
    }

    fn rejecting_shim() -> QueryApplicationMysqlShim {
        QueryApplicationMysqlShim::new(
            "root".to_string(),
            ClientConnectionToken::new(1, 1).expect("valid connection token"),
            Arc::new(CancellationProbeFactory {
                cancelled: Arc::new(AtomicBool::new(false)),
            }),
            Arc::new(OnceLock::new()),
            ClientDisconnectWatcher::inactive(),
            "test".to_string(),
        )
    }

    #[cfg(feature = "mem-1-m07-exact-mysql-write")]
    #[test]
    fn armed_fixture_target_reaches_query_dispatch_instead_of_shortcut() {
        type Shim = QueryApplicationMysqlShim;
        for target in [1, 2] {
            let (hub, mut controller) =
                crate::mysql_write_gate::late_binding::MysqlWriteGateHub::new(
                    novarocks_types::FrontendProcessId::new_v7(),
                    [1; 16],
                    std::time::Instant::now() + Duration::from_secs(3),
                )
                .unwrap();
            let shim = rejecting_shim().with_fixture_hub(Some(hub));
            assert!(<Shim as AsyncMysqlShim<Vec<u8>>>::permits_query_shortcuts(
                &shim
            ));
            controller
                .arm(controller.snapshot().frontend, [1; 16], target, [2; 32], 1)
                .unwrap();
            assert_eq!(
                <Shim as AsyncMysqlShim<Vec<u8>>>::permits_query_shortcuts(&shim),
                target != 1
            );
            let control = shim.with_connection_class(MysqlConnectionClass::Control);
            assert!(!<Shim as AsyncMysqlShim<Vec<u8>>>::permits_query_shortcuts(
                &control
            ));
            controller.stop();
        }
    }

    #[cfg(feature = "mem-1-m07-exact-mysql-write")]
    #[tokio::test]
    async fn fixture_original_registered_intermediary_retains_actual_io_after_socket_exit() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let connections = MysqlClientConnectionRegistry::new();
        let registration = connections.register().unwrap();
        let actual_token = registration.token();
        let observation = Arc::new(crate::listener::MysqlFixtureSessionJoins::default());
        let permit = observation
            .reserve_watcher(registration.retain_owner())
            .unwrap();
        let server = async {
            let (stream, peer) = listener.accept().await.unwrap();
            serve_registered_mysql_connection(
                "root".into(),
                "test".into(),
                Arc::new(RawResultFactory),
                registration,
                stream,
                peer,
                None,
                Some(permit),
                Some(observation.clone()),
                #[cfg(feature = "mem-1-m07-closing-pressure")]
                None,
            )
            .await;
            observation.abort_remaining_watchers();
            while observation.next_watcher().await.is_some() {}
            connections.wait_drained().await;
        };
        let client = async {
            let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
            async fn read_packet(stream: &mut TcpStream) -> Vec<u8> {
                let mut header = [0; 4];
                stream.read_exact(&mut header).await.unwrap();
                header[3] = 0;
                let len = u32::from_le_bytes(header) as usize;
                assert!(len < 4096);
                let mut body = vec![0; len];
                stream.read_exact(&mut body).await.unwrap();
                body
            }
            assert_eq!(read_packet(&mut stream).await[0], 10);
            let flags = CapabilityFlags::CLIENT_PROTOCOL_41
                | CapabilityFlags::CLIENT_SECURE_CONNECTION
                | CapabilityFlags::CLIENT_PLUGIN_AUTH;
            let mut auth = Vec::new();
            auth.extend_from_slice(&flags.bits().to_le_bytes());
            auth.extend_from_slice(&(64_u32 * 1024 * 1024).to_le_bytes());
            auth.push(33);
            auth.extend_from_slice(&[0; 23]);
            auth.extend_from_slice(b"root\0");
            auth.push(0);
            auth.extend_from_slice(b"mysql_native_password\0");
            let mut header = (auth.len() as u32).to_le_bytes();
            header[3] = 1;
            stream.write_all(&header).await.unwrap();
            stream.write_all(&auth).await.unwrap();
            assert_eq!(read_packet(&mut stream).await[0], 0);
            let query = b"\x03SELECT raw_result";
            let mut header = (query.len() as u32).to_le_bytes();
            header[3] = 0;
            stream.write_all(&header).await.unwrap();
            stream.write_all(query).await.unwrap();
            assert_eq!(stream.read(&mut [0; 1]).await.unwrap(), 0);
        };
        tokio::time::timeout(Duration::from_secs(2), async {
            tokio::join!(server, client);
        })
        .await
        .unwrap();
        let actual = observation.take_protocol_failure_after_join().unwrap();
        assert_eq!(actual.connection, actual_token);
        assert_eq!(actual.class, MysqlConnectionClass::Ordinary);
        assert_eq!(actual.cause.kind(), io::ErrorKind::InvalidData);
        assert!(actual.cause.to_string().contains("original governed owner"));
        assert_eq!(observation.snapshot().protocol_io_failures, 1);
        assert_eq!(observation.watcher_snapshot().joined, 1);
        assert!(observation.watchers_empty());
    }

    async fn authenticate(
        shim: &QueryApplicationMysqlShim,
        auth_plugin: &str,
        user: &[u8],
        auth: &[u8],
    ) -> bool {
        AsyncMysqlShim::<tokio::io::Sink>::authenticate(
            shim,
            auth_plugin,
            user,
            b"0123456789abcdefghij",
            auth,
        )
        .await
    }

    #[test]
    fn reserved_control_connections_disable_protocol_query_shortcuts() {
        let ordinary = rejecting_shim();
        assert!(AsyncMysqlShim::<tokio::io::Sink>::permits_query_shortcuts(
            &ordinary
        ));
        let control = ordinary.with_connection_class(MysqlConnectionClass::Control);
        assert!(!AsyncMysqlShim::<tokio::io::Sink>::permits_query_shortcuts(
            &control
        ));
    }

    #[tokio::test]
    async fn adapter_rejects_unauthorized_handshakes_before_session_open() {
        let shim = rejecting_shim();

        assert!(!authenticate(&shim, "mysql_native_password", b"other", b"").await);
        assert!(!authenticate(&shim, "mysql_native_password", b"ROOT", b"").await);
        assert!(!authenticate(&shim, "mysql_native_password", b"root", b"secret").await);
        assert!(!authenticate(&shim, "caching_sha2_password", b"root", b"").await);
    }

    #[tokio::test]
    async fn immediate_protocol_shutdown_cancels_the_ready_session_factory() {
        let cancelled = Arc::new(AtomicBool::new(false));
        let factory: Arc<dyn QuerySessionFactory> = Arc::new(CancellationProbeFactory {
            cancelled: Arc::clone(&cancelled),
        });
        let settings = crate::ResolvedMysqlListenerSettings::new(
            SocketAddr::from(([127, 0, 0, 1], 0)),
            "root",
        );

        serve_query_application_mysql_until_shutdown(
            settings,
            "test".to_string(),
            factory,
            Arc::new(MysqlClientConnectionRegistry::new()),
            async {},
            |_| {},
        )
        .await
        .expect("ready protocol server should shut down cleanly");

        assert!(cancelled.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn immediate_protocol_shutdown_notifies_registered_connections() {
        let cancelled = Arc::new(AtomicBool::new(false));
        let factory: Arc<dyn QuerySessionFactory> = Arc::new(CancellationProbeFactory {
            cancelled: Arc::clone(&cancelled),
        });
        let connections = Arc::new(MysqlClientConnectionRegistry::new());
        let mut registration = connections
            .register()
            .expect("register protocol connection");
        let settings = crate::ResolvedMysqlListenerSettings::new(
            SocketAddr::from(([127, 0, 0, 1], 0)),
            "root",
        );

        let server = serve_query_application_mysql_until_shutdown(
            settings,
            "test".to_string(),
            factory,
            Arc::clone(&connections),
            async {},
            |_| {},
        );
        let (result, reason) = tokio::join!(server, async move {
            let reason = registration
                .termination_receiver()
                .await
                .expect("shutdown signal");
            // The protocol task actually exits before the registry is drained.
            drop(registration);
            reason
        });
        result.expect("ready protocol server should shut down cleanly");
        assert!(cancelled.load(Ordering::SeqCst));
        assert_eq!(reason, ClientConnectionTerminationReason::ServerShutdown);
    }
}

#[cfg(all(test, feature = "mem-1-m07-exact-mysql-write"))]
#[path = "query_application_shim/exact_eof_tests.rs"]
mod exact_eof_tests;

#[cfg(all(test, feature = "mem-1-m07-closing-pressure"))]
#[path = "query_application_shim/closing_pressure_tests.rs"]
mod closing_pressure_tests;

#[cfg(all(test, feature = "mem-1-m07-exact-mysql-write"))]
mod exact_successful_bind_marker_tests {
    use super::*;
    use crate::mysql_write_gate::late_binding::{MysqlWriteGateController, MysqlWriteGateHub};
    use novarocks_query_application::session_control::{SessionToken, StatementToken};
    use novarocks_types::FrontendProcessId;
    use std::time::Instant;

    // Component-only original hub binding; no service, native scene or stdout mutation.
    fn owner(
        connection: ClientConnectionToken,
        statement: StatementToken,
        digest: [u8; 32],
    ) -> (
        Arc<MysqlWriteGateHub>,
        MysqlWriteGateController,
        crate::mysql_write_gate::late_binding::MysqlWriteRelayHook,
    ) {
        let frontend = FrontendProcessId::new_v7();
        let (hub, mut controller) =
            MysqlWriteGateHub::new(frontend, [1; 16], Instant::now() + Duration::from_secs(5))
                .unwrap();
        controller
            .arm(frontend, [1; 16], connection.connection_id(), digest, 1)
            .unwrap();
        let hook = hub
            .bind_statement(connection, statement.session(), statement, digest)
            .unwrap()
            .unwrap();
        (hub, controller, hook)
    }
    struct Sink {
        bytes: [u8; 384],
        length: usize,
        writes: usize,
        flushes: usize,
        quantum: usize,
    }
    impl Sink {
        fn new(quantum: usize) -> Self {
            Self {
                bytes: [0; 384],
                length: 0,
                writes: 0,
                flushes: 0,
                quantum,
            }
        }
    }
    impl io::Write for Sink {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.writes += 1;
            let n = bytes.len().min(self.quantum);
            assert!(self.length + n <= self.bytes.len());
            self.bytes[self.length..self.length + n].copy_from_slice(&bytes[..n]);
            self.length += n;
            Ok(n)
        }
        fn flush(&mut self) -> io::Result<()> {
            self.flushes += 1;
            Ok(())
        }
    }
    fn raw() -> (ClientConnectionToken, StatementToken) {
        (
            ClientConnectionToken::new(71, 19).unwrap(),
            StatementToken::new(
                SessionToken::new(
                    novarocks_query_application::session_control::ClientSessionKey::new(
                        (71_u64 << 32) | 23_u64,
                    )
                    .expect("nonzero fixture session"),
                ),
                29,
            ),
        )
    }
    #[test]
    fn successful_original_bind_marker_keeps_distinct_raw_identity_domains_and_one_line() {
        let (connection, statement) = raw();
        let mut digest = [0; 32];
        for (i, byte) in digest.iter_mut().enumerate() {
            *byte = i as u8;
        }
        let (_hub, _controller, hook) = owner(connection, statement, digest);
        let mut sink = Sink::new(3);
        write_exact_mysql_bound_marker_for_hook(
            &mut sink,
            Some(&hook),
            connection,
            statement,
            digest,
        )
        .unwrap();
        let expected = b"NOVAROCKS_EXACT_MYSQL_TARGET_BOUND connection_id=71 connection_generation=19 session_connection_id=71 session_key=23 statement_generation=29 sql_sha256=000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f\n";
        assert_eq!(&sink.bytes[..sink.length], expected);
        assert!(sink.writes > 1);
        assert_eq!(sink.flushes, 1);
        assert_eq!(
            sink.bytes[..sink.length]
                .iter()
                .filter(|byte| **byte == b'\n')
                .count(),
            1
        );
        assert!(sink.bytes[..sink.length].is_ascii());
        assert!(sink.bytes[sink.length..].iter().all(|byte| *byte == 0));
    }
    #[test]
    fn maximum_raw_numeric_identity_and_digest_fit_the_fixed_ascii_cap() {
        let connection = ClientConnectionToken::new(u32::MAX, u64::MAX).unwrap();
        let statement = StatementToken::new(
            SessionToken::new(
                novarocks_query_application::session_control::ClientSessionKey::new(
                    ((u32::MAX as u64) << 32) | (u64::MAX as u64),
                )
                .expect("nonzero fixture session"),
            ),
            u64::MAX,
        );
        let (_hub, _controller, hook) = owner(connection, statement, [255; 32]);
        let mut sink = Sink::new(384);
        write_exact_mysql_bound_marker_for_hook(
            &mut sink,
            Some(&hook),
            connection,
            statement,
            [255; 32],
        )
        .unwrap();
        let expected = b"NOVAROCKS_EXACT_MYSQL_TARGET_BOUND connection_id=4294967295 connection_generation=18446744073709551615 session_connection_id=4294967295 session_key=18446744073709551615 statement_generation=18446744073709551615 sql_sha256=ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff\n";
        assert_eq!(&sink.bytes[..sink.length], expected);
        assert!(sink.length <= 384);
        assert_eq!(sink.writes, 1);
        assert_eq!(sink.flushes, 1);
    }
    #[test]
    fn no_hook_and_actual_non_target_bind_do_not_touch_output_or_flush() {
        let (connection, statement) = raw();
        let frontend = FrontendProcessId::new_v7();
        let (hub, mut controller) =
            MysqlWriteGateHub::new(frontend, [1; 16], Instant::now() + Duration::from_secs(5))
                .unwrap();
        controller.arm(frontend, [1; 16], 72, [7; 32], 1).unwrap();
        let hook = hub
            .bind_statement(connection, statement.session(), statement, [7; 32])
            .unwrap();
        assert!(hook.is_none());
        let mut sink = Sink::new(384);
        write_exact_mysql_bound_marker_for_hook(
            &mut sink,
            hook.as_ref(),
            connection,
            statement,
            [7; 32],
        )
        .unwrap();
        write_exact_mysql_bound_marker_for_hook(&mut sink, None, connection, statement, [7; 32])
            .unwrap();
        assert_eq!((sink.length, sink.writes, sink.flushes), (0, 0, 0));
    }
    #[test]
    fn duplicate_original_bind_refuses_before_a_second_marker() {
        let (connection, statement) = raw();
        let (hub, _controller, hook) = owner(connection, statement, [7; 32]);
        let mut sink = Sink::new(384);
        write_exact_mysql_bound_marker_for_hook(
            &mut sink,
            Some(&hook),
            connection,
            statement,
            [7; 32],
        )
        .unwrap();
        let first = sink.length;
        assert!(
            hub.bind_statement(connection, statement.session(), statement, [7; 32])
                .is_err()
        );
        assert_eq!(sink.length, first);
    }

    #[derive(Debug)]
    struct OriginalIo(Arc<()>);
    impl std::fmt::Display for OriginalIo {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("component original marker IO")
        }
    }
    impl std::error::Error for OriginalIo {}
    struct FaultSink {
        inner: Sink,
        identity: Arc<()>,
        fail_flush: bool,
    }
    impl io::Write for FaultSink {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if !self.fail_flush && self.inner.length >= 3 {
                return Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    OriginalIo(Arc::clone(&self.identity)),
                ));
            }
            std::io::Write::write(&mut self.inner, bytes)
        }
        fn flush(&mut self) -> io::Result<()> {
            self.inner.flushes += 1;
            Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                OriginalIo(Arc::clone(&self.identity)),
            ))
        }
    }
    #[test]
    fn partial_write_and_flush_keep_the_original_error_object_for_the_owner_path() {
        for fail_flush in [false, true] {
            let (connection, statement) = raw();
            let (hub, _controller, hook) = owner(connection, statement, [7; 32]);
            let identity = Arc::new(());
            let mut sink = FaultSink {
                inner: Sink::new(if fail_flush { 384 } else { 3 }),
                identity: Arc::clone(&identity),
                fail_flush,
            };
            let error = write_exact_mysql_bound_marker_for_hook(
                &mut sink,
                Some(&hook),
                connection,
                statement,
                [7; 32],
            )
            .unwrap_err();
            let actual = error
                .get_ref()
                .unwrap()
                .downcast_ref::<OriginalIo>()
                .unwrap();
            assert!(Arc::ptr_eq(&actual.0, &identity));
            assert_eq!(
                error.kind(),
                if fail_flush {
                    io::ErrorKind::PermissionDenied
                } else {
                    io::ErrorKind::BrokenPipe
                }
            );
            assert_eq!(sink.inner.flushes, usize::from(fail_flush));
            if !fail_flush {
                assert_eq!(&sink.inner.bytes[..sink.inner.length], b"NOV");
            }
            // Same original error move used by the existing serve_registered caller;
            // this is a ledger component, not an actual intermediary/join receipt.
            hub.fail_selected(connection, crate::mysql_write_gate::GateFailure::Transition);
            let ledger = crate::listener::MysqlFixtureSessionJoins::default();
            ledger.observe_protocol_failure(connection, MysqlConnectionClass::Ordinary, error);
            let retained = ledger.take_protocol_failure_after_join().unwrap();
            assert!(Arc::ptr_eq(
                &retained
                    .cause
                    .get_ref()
                    .unwrap()
                    .downcast_ref::<OriginalIo>()
                    .unwrap()
                    .0,
                &identity
            ));
            assert_eq!(ledger.snapshot().protocol_io_failures, 1);
            assert_eq!(ledger.snapshot().prescribed_protocol_eofs, 0);
        }
    }
    #[test]
    fn zero_write_is_refused_without_flush_or_claiming_a_complete_marker() {
        let (connection, statement) = raw();
        let (_hub, _controller, hook) = owner(connection, statement, [7; 32]);
        let mut sink = Sink::new(0);
        let error = write_exact_mysql_bound_marker_for_hook(
            &mut sink,
            Some(&hook),
            connection,
            statement,
            [7; 32],
        )
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::WriteZero);
        assert_eq!((sink.length, sink.writes, sink.flushes), (0, 1, 0));
    }
}
