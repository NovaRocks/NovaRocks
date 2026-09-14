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

use crate::{ClientDisconnectWatcher, MysqlClientConnectionRegistry, spawn_disconnect_watcher};

async fn write_negotiated_statement<'writer, W: AsyncWrite + Unpin>(
    statement: StatementResult,
    results: QueryResultWriter<'writer, W>,
) -> io::Result<crate::MysqlStatementWriteOutcome<'writer, W>> {
    match statement {
        StatementResult::Query(result) => crate::write_query_result_one(result, results)
            .await
            .map(crate::MysqlStatementWriteOutcome::Continue),
        StatementResult::GovernedQuery(result) => {
            crate::write_governed_query_result_one(result, results).await
        }
        StatementResult::StreamingQuery(result) => {
            crate::write_streaming_query_result_one(result, results).await
        }
        StatementResult::GovernedCompletion(result) => {
            crate::write_governed_terminal_ok_one(result.into_protocol(), results).await
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
    let (bind_addr, session_user) = settings.into_parts();
    crate::serve_tcp_until_drain_then_shutdown(
        bind_addr,
        drain,
        finalize,
        move |stream, peer_addr| {
            serve_query_application_mysql_connection(
                session_user.clone(),
                server_version.clone(),
                Arc::clone(&session_factory),
                Arc::clone(&connections),
                stream,
                peer_addr,
            )
        },
        on_ready,
        cleanup_timeout,
    )
    .await
}

pub async fn serve_query_application_mysql_connection(
    user: String,
    server_version: String,
    session_factory: Arc<dyn QuerySessionFactory>,
    connections: Arc<MysqlClientConnectionRegistry>,
    stream: TcpStream,
    peer_addr: SocketAddr,
) {
    let mut registration = match connections.register() {
        Ok(registration) => registration,
        Err(error) => {
            warn!(
                "reject standalone mysql connection because the connection registry is exhausted: peer={}, error={:?}",
                peer_addr, error
            );
            return;
        }
    };
    let connection = registration.token();
    let session: Arc<OnceLock<Arc<dyn QuerySession>>> = Arc::new(OnceLock::new());
    let session_for_disconnect = Arc::clone(&session);
    let disconnect_watcher = spawn_disconnect_watcher(&stream, move || {
        if let Some(session) = session_for_disconnect.get() {
            session.cancel_current(QueryCancellationReason::ClientDisconnected);
        }
    });
    let shim = QueryApplicationMysqlShim::new(
        user,
        connection,
        session_factory,
        Arc::clone(&session),
        disconnect_watcher,
        server_version,
    );
    let (reader, writer) = stream.into_split();
    let result = {
        let intermediary = AsyncMysqlIntermediary::run_with_options(
            shim,
            reader,
            writer,
            &crate::MYSQL_INTERMEDIARY_OPTIONS,
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
        warn!(
            "standalone mysql connection failed: peer={}, connection_id={}, err={}",
            peer_addr,
            connection.connection_id(),
            err
        );
    }
}

fn query_cancellation_reason_for_connection_termination(
    reason: &ClientConnectionTerminationReason,
) -> QueryCancellationReason {
    match reason {
        ClientConnectionTerminationReason::ExplicitKillConnection {
            requester_connection_id,
        } => QueryCancellationReason::ExplicitKillConnection {
            requester_connection_id: *requester_connection_id,
        },
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
                crate::write_governed_init_ok(result.into_protocol(), writer).await
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
        let session = match self.session() {
            Ok(session) => session,
            Err(error) => {
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
                    return results
                        .error(crate::mysql_error_kind(&error), error.message().as_bytes())
                        .await;
                }
            }
        } else {
            Vec::new()
        };
        if statements.len() > 1 {
            let mut results = results;
            for statement_sql in statements {
                let statement = match session.execute_statement(statement_sql).await {
                    Ok(statement) => statement,
                    Err(error) => {
                        return results
                            .error(crate::mysql_error_kind(&error), error.message().as_bytes())
                            .await;
                    }
                };
                let (statement, terminal) = statement.into_parts();
                let outcome = write_negotiated_statement(statement, results).await;
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
                return results
                    .error(crate::mysql_error_kind(&error), error.message().as_bytes())
                    .await;
            }
        };
        let outcome = match statement {
            StatementResult::Query(result) => crate::write_query_result(result, results).await,
            StatementResult::GovernedQuery(result) => {
                crate::write_governed_query_result(result, results).await
            }
            StatementResult::StreamingQuery(result) => {
                crate::write_streaming_query_result(result, results).await
            }
            StatementResult::GovernedCompletion(result) => {
                crate::write_governed_terminal_ok(result.into_protocol(), results).await
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

    struct CancellationProbeFactory {
        cancelled: Arc<AtomicBool>,
    }

    impl QuerySessionFactory for CancellationProbeFactory {
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

        serve_query_application_mysql_until_shutdown(
            settings,
            "test".to_string(),
            factory,
            Arc::clone(&connections),
            async {},
            |_| {},
        )
        .await
        .expect("ready protocol server should shut down cleanly");

        assert!(cancelled.load(Ordering::SeqCst));
        assert_eq!(
            registration
                .termination_receiver()
                .try_recv()
                .expect("shutdown must reach the registered connection"),
            ClientConnectionTerminationReason::ServerShutdown
        );
    }
}
