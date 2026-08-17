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

mod encoding;
pub mod session;

use std::future::Future;
use std::io;
use std::net::{Ipv4Addr, SocketAddr};
#[cfg(unix)]
use std::os::fd::{AsRawFd, FromRawFd};
#[cfg(test)]
use std::sync::atomic::AtomicBool;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use mysql_common::scramble::scramble_native;
use opensrv_mysql::{
    AsyncMysqlIntermediary, AsyncMysqlShim, ErrorKind, InitWriter, OkResponse, ParamParser,
    QueryResultWriter, StatementMetaWriter,
};
use tokio::io::AsyncWrite;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinSet;
use tracing::{info, warn};

use crate::version;

use self::encoding::write_query_result;
use self::session::{
    QueryServiceError, QueryServiceErrorKind, QuerySession, QuerySessionFactory,
    QuerySessionOpenRequest,
};
use crate::query_execution::StatementResult;
use crate::query_execution::cancellation::QueryCancellationReason;
use novarocks_catalog::memory::DEFAULT_DATABASE;

const DEFAULT_MYSQL_PORT: u16 = 9030;
const ROOT_USER: &str = "root";
const SESSION_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);
static NEXT_CONNECTION_ID: AtomicU32 = AtomicU32::new(1);

struct ClientDisconnectWatcher {
    join_handle: Option<tokio::task::JoinHandle<()>>,
}

impl Drop for ClientDisconnectWatcher {
    fn drop(&mut self) {
        if let Some(handle) = self.join_handle.take() {
            handle.abort();
        }
    }
}

/// Fully resolved MySQL listener settings.
///
/// Frontend composition resolves these settings before opening the protocol
/// listener.  The protocol server receives an already-ready
/// [`QuerySessionFactory`]; it neither reads configuration nor opens a Core
/// application host.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedMysqlListenerSettings {
    bind_addr: SocketAddr,
    user: String,
}

impl ResolvedMysqlListenerSettings {
    pub fn new(bind_addr: SocketAddr, user: impl Into<String>) -> Self {
        Self {
            bind_addr,
            user: user.into(),
        }
    }

    pub fn bind_addr(&self) -> SocketAddr {
        self.bind_addr
    }

    pub fn user(&self) -> &str {
        &self.user
    }
}

/// Resolves the protocol listener settings from an already-loaded config.
///
/// This is the production composition boundary: Frontend owns configuration
/// and application startup, then passes the resulting settings and a ready
/// [`QuerySessionFactory`] to [`run_mysql_server_until_shutdown`].
pub fn resolve_mysql_listener_settings(
    configured_port: Option<u16>,
    configured_user: Option<&str>,
    port_override: Option<u16>,
) -> Result<ResolvedMysqlListenerSettings, String> {
    let mysql_port = port_override
        .or(configured_port)
        .unwrap_or(DEFAULT_MYSQL_PORT);
    let user = configured_user.unwrap_or(ROOT_USER);
    if user != ROOT_USER {
        return Err(format!(
            "standalone server only supports user `{ROOT_USER}`, got `{user}`"
        ));
    }
    Ok(ResolvedMysqlListenerSettings::new(
        SocketAddr::from((Ipv4Addr::LOCALHOST, mysql_port)),
        user,
    ))
}

/// Runs the MySQL protocol listener with a ready frontend-owned session
/// factory.
///
/// The listener preserves the public ready marker and the shutdown drain
/// contract.  On shutdown it first asks the session factory to cancel all
/// sessions, stops accepting new connections, then waits for active protocol
/// tasks to drain (or aborts them after the bounded drain timeout).
pub async fn run_mysql_server_until_shutdown<F>(
    settings: ResolvedMysqlListenerSettings,
    session_factory: Arc<dyn QuerySessionFactory>,
    shutdown: F,
) -> Result<(), String>
where
    F: Future<Output = ()> + Send,
{
    let ready_user = settings.user.clone();
    let session_user = settings.user;
    let shutdown_factory = Arc::clone(&session_factory);
    serve_until_shutdown(
        settings.bind_addr,
        async move {
            shutdown.await;
            shutdown_factory.cancel_all(QueryCancellationReason::ServerShutdown);
        },
        move |stream, peer_addr| {
            serve_frontend_mysql_connection(
                session_user.clone(),
                Arc::clone(&session_factory),
                stream,
                peer_addr,
            )
        },
        move |bound_addr| emit_standalone_ready(bound_addr, &ready_user),
    )
    .await
}

fn emit_standalone_ready(bind_addr: SocketAddr, user: &str) {
    info!(
        "standalone mysql server listening on {} (user={}, db={})",
        bind_addr, user, DEFAULT_DATABASE
    );
    // Emit a parser-friendly readiness marker on stdout. Orchestration
    // scripts must wait for this exact line before connecting; probing the
    // mysql port alone cannot distinguish a freshly-bound server from a
    // pre-existing process that already owned the port. The keyword
    // `NOVAROCKS_READY` is the wait-for-ready contract — do not change it
    // without updating callers (CLAUDE.md, sql-tests harness, etc.).
    println!(
        "NOVAROCKS_READY mysql_port={} pid={}",
        bind_addr.port(),
        std::process::id()
    );
}

async fn serve_until_shutdown<F, H, HFut, R>(
    bind_addr: SocketAddr,
    shutdown: F,
    session_handler: H,
    on_ready: R,
) -> Result<(), String>
where
    F: Future<Output = ()> + Send,
    H: FnMut(TcpStream, SocketAddr) -> HFut,
    HFut: Future<Output = ()> + Send + 'static,
    R: FnOnce(SocketAddr),
{
    serve_until_shutdown_with_drain_timeout(
        bind_addr,
        shutdown,
        session_handler,
        on_ready,
        SESSION_DRAIN_TIMEOUT,
    )
    .await
}

async fn serve_until_shutdown_with_drain_timeout<F, H, HFut, R>(
    bind_addr: SocketAddr,
    shutdown: F,
    mut session_handler: H,
    on_ready: R,
    drain_timeout: Duration,
) -> Result<(), String>
where
    F: Future<Output = ()> + Send,
    H: FnMut(TcpStream, SocketAddr) -> HFut,
    HFut: Future<Output = ()> + Send + 'static,
    R: FnOnce(SocketAddr),
{
    let listener = TcpListener::bind(bind_addr)
        .await
        .map_err(|e| format!("bind standalone mysql server on {bind_addr} failed: {e}"))?;
    let bound_addr = listener
        .local_addr()
        .map_err(|e| format!("read standalone mysql server address failed: {e}"))?;
    on_ready(bound_addr);

    let mut sessions = JoinSet::new();
    tokio::pin!(shutdown);
    let serve_result = loop {
        tokio::select! {
            biased;
            _ = &mut shutdown => break Ok(()),
            completed = sessions.join_next(), if !sessions.is_empty() => {
                if let Some(result) = completed {
                    log_session_join_error(result);
                }
            }
            accepted = listener.accept() => {
                match accepted {
                    Ok((stream, peer_addr)) => {
                        sessions.spawn(session_handler(stream, peer_addr));
                    }
                    Err(err) => {
                        break Err(format!(
                            "accept standalone mysql connection failed: {err}"
                        ));
                    }
                }
            }
        }
    };

    drop(listener);
    drain_session_tasks(&mut sessions, drain_timeout).await;
    serve_result
}

fn log_session_join_error(result: Result<(), tokio::task::JoinError>) {
    if let Err(err) = result
        && !err.is_cancelled()
    {
        warn!("standalone mysql connection task failed: {err}");
    }
}

async fn drain_session_tasks(sessions: &mut JoinSet<()>, drain_timeout: Duration) {
    let drain = async {
        while let Some(result) = sessions.join_next().await {
            log_session_join_error(result);
        }
    };
    if tokio::time::timeout(drain_timeout, drain).await.is_ok() {
        return;
    }

    sessions.abort_all();
    while let Some(result) = sessions.join_next().await {
        log_session_join_error(result);
    }
}

async fn serve_frontend_mysql_connection(
    user: String,
    session_factory: Arc<dyn QuerySessionFactory>,
    stream: TcpStream,
    peer_addr: SocketAddr,
) {
    let connection_id = NEXT_CONNECTION_ID.fetch_add(1, Ordering::Relaxed);
    let session = Arc::new(OnceLock::new());
    let disconnect_watcher = spawn_frontend_disconnect_watcher(&stream, Arc::clone(&session));
    let shim = FrontendMysqlShim::new(
        user,
        connection_id,
        session_factory,
        session,
        disconnect_watcher,
    );
    let (reader, writer) = stream.into_split();
    let result = AsyncMysqlIntermediary::run_on(shim, reader, writer).await;
    if let Err(err) = result {
        warn!(
            "standalone mysql connection failed: peer={}, connection_id={}, err={}",
            peer_addr, connection_id, err
        );
    }
}

struct FrontendMysqlShim {
    user: String,
    connection_id: u32,
    session_factory: Arc<dyn QuerySessionFactory>,
    session: Arc<OnceLock<Arc<dyn QuerySession>>>,
    _disconnect_watcher: ClientDisconnectWatcher,
}

impl FrontendMysqlShim {
    fn new(
        user: String,
        connection_id: u32,
        session_factory: Arc<dyn QuerySessionFactory>,
        session: Arc<OnceLock<Arc<dyn QuerySession>>>,
        disconnect_watcher: ClientDisconnectWatcher,
    ) -> Self {
        Self {
            user,
            connection_id,
            session_factory,
            session,
            _disconnect_watcher: disconnect_watcher,
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

impl Drop for FrontendMysqlShim {
    fn drop(&mut self) {
        if let Some(session) = self.session.get() {
            session.close();
        }
    }
}

#[async_trait]
impl<W: AsyncWrite + Send + Unpin> AsyncMysqlShim<W> for FrontendMysqlShim {
    type Error = io::Error;

    fn version(&self) -> String {
        format!("{}-standalone-mysql", version::short_version())
    }

    fn connect_id(&self) -> u32 {
        self.connection_id
    }

    async fn authenticate(
        &self,
        auth_plugin: &str,
        username: &[u8],
        salt: &[u8],
        auth_data: &[u8],
    ) -> bool {
        if auth_plugin != "mysql_native_password" || username != self.user.as_bytes() {
            return false;
        }
        let authenticated = if auth_data.is_empty() {
            true
        } else {
            scramble_native(salt, b"")
                .map(|expected| auth_data == expected.as_slice())
                .unwrap_or(false)
        };
        if !authenticated {
            return false;
        }
        let session = match self
            .session_factory
            .open_session(QuerySessionOpenRequest::new(
                self.connection_id,
                self.user.clone(),
            )) {
            Ok(session) => session,
            Err(error) => {
                warn!(
                    "failed to open frontend query session for connection_id={}: {}",
                    self.connection_id, error
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
                    .error(
                        map_query_service_error(error.kind()),
                        error.message().as_bytes(),
                    )
                    .await;
            }
        };
        match session.init_database(schema).await {
            Ok(()) => writer.ok().await,
            Err(error) => {
                writer
                    .error(
                        map_query_service_error(error.kind()),
                        error.message().as_bytes(),
                    )
                    .await
            }
        }
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
                    .error(
                        map_query_service_error(error.kind()),
                        error.message().as_bytes(),
                    )
                    .await;
            }
        };
        match session.execute_batch(query).await {
            Ok(StatementResult::Query(result)) => write_query_result(result, results).await,
            Ok(StatementResult::Ok) => results.completed(OkResponse::default()).await,
            Err(error) => {
                results
                    .error(
                        map_query_service_error(error.kind()),
                        error.message().as_bytes(),
                    )
                    .await
            }
        }
    }
}

fn map_query_service_error(kind: QueryServiceErrorKind) -> ErrorKind {
    match kind {
        QueryServiceErrorKind::Parse => ErrorKind::ER_PARSE_ERROR,
        QueryServiceErrorKind::BadDatabase => ErrorKind::ER_BAD_DB_ERROR,
        QueryServiceErrorKind::Unsupported => ErrorKind::ER_NOT_SUPPORTED_YET,
        QueryServiceErrorKind::PermissionDenied => ErrorKind::ER_SPECIFIC_ACCESS_DENIED_ERROR,
        QueryServiceErrorKind::NoSuchSession => ErrorKind::ER_NO_SUCH_THREAD,
        QueryServiceErrorKind::Interrupted => ErrorKind::ER_QUERY_INTERRUPTED,
        QueryServiceErrorKind::Timeout => ErrorKind::ER_UNKNOWN_ERROR,
        QueryServiceErrorKind::InvalidValue => ErrorKind::ER_WRONG_VALUE,
        QueryServiceErrorKind::Unavailable => ErrorKind::ER_UNKNOWN_ERROR,
        QueryServiceErrorKind::Internal => ErrorKind::ER_UNKNOWN_ERROR,
    }
}

#[cfg(test)]
#[test]
fn query_service_error_mapping_keeps_wire_concerns_in_core() {
    assert_eq!(
        map_query_service_error(QueryServiceErrorKind::BadDatabase),
        ErrorKind::ER_BAD_DB_ERROR
    );
    assert_eq!(
        map_query_service_error(QueryServiceErrorKind::Interrupted),
        ErrorKind::ER_QUERY_INTERRUPTED
    );
    assert_eq!(
        map_query_service_error(QueryServiceErrorKind::Unavailable),
        ErrorKind::ER_UNKNOWN_ERROR
    );
}

#[cfg(unix)]
fn spawn_frontend_disconnect_watcher(
    stream: &tokio::net::TcpStream,
    session: Arc<OnceLock<Arc<dyn QuerySession>>>,
) -> ClientDisconnectWatcher {
    let fd = unsafe { libc::dup(stream.as_raw_fd()) };
    if fd < 0 {
        return ClientDisconnectWatcher { join_handle: None };
    }
    let std_stream = unsafe { std::net::TcpStream::from_raw_fd(fd) };
    if let Err(error) = std_stream.set_nonblocking(true) {
        warn!("failed to configure frontend disconnect monitor: {}", error);
        return ClientDisconnectWatcher { join_handle: None };
    }
    let watcher_stream = match tokio::net::TcpStream::from_std(std_stream) {
        Ok(stream) => stream,
        Err(error) => {
            warn!("failed to create frontend disconnect monitor: {}", error);
            return ClientDisconnectWatcher { join_handle: None };
        }
    };
    let join_handle = tokio::spawn(async move {
        let mut buf = [0u8; 1];
        loop {
            match watcher_stream.peek(&mut buf).await {
                Ok(0) => {
                    if let Some(session) = session.get() {
                        session.cancel_current(QueryCancellationReason::ClientDisconnected);
                    }
                    break;
                }
                Ok(_) => tokio::time::sleep(Duration::from_millis(10)).await,
                Err(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
                    ) => {}
                Err(_) => {
                    if let Some(session) = session.get() {
                        session.cancel_current(QueryCancellationReason::ClientDisconnected);
                    }
                    break;
                }
            }
        }
    });
    ClientDisconnectWatcher {
        join_handle: Some(join_handle),
    }
}

#[cfg(not(unix))]
fn spawn_frontend_disconnect_watcher(
    _stream: &tokio::net::TcpStream,
    _session: Arc<OnceLock<Arc<dyn QuerySession>>>,
) -> ClientDisconnectWatcher {
    ClientDisconnectWatcher { join_handle: None }
}

#[cfg(test)]
mod protocol_api_tests {
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

    #[tokio::test]
    async fn ready_session_factory_api_cancels_sessions_before_listener_drain() {
        let cancelled = Arc::new(AtomicBool::new(false));
        let factory: Arc<dyn QuerySessionFactory> = Arc::new(CancellationProbeFactory {
            cancelled: Arc::clone(&cancelled),
        });
        let settings = ResolvedMysqlListenerSettings {
            bind_addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
            user: ROOT_USER.to_string(),
        };

        run_mysql_server_until_shutdown(settings, factory, async {})
            .await
            .expect("ready protocol server should shut down cleanly");

        assert!(cancelled.load(Ordering::SeqCst));
    }

    /// A shim whose session factory must never be reached. Every assertion below
    /// is a rejection, and rejection happens strictly before a session is opened.
    fn rejecting_shim() -> FrontendMysqlShim {
        FrontendMysqlShim::new(
            ROOT_USER.to_string(),
            1,
            Arc::new(CancellationProbeFactory {
                cancelled: Arc::new(AtomicBool::new(false)),
            }),
            Arc::new(OnceLock::new()),
            ClientDisconnectWatcher { join_handle: None },
        )
    }

    async fn authenticate(
        shim: &FrontendMysqlShim,
        auth_plugin: &str,
        username: &[u8],
        auth_data: &[u8],
    ) -> bool {
        AsyncMysqlShim::<tokio::io::Sink>::authenticate(
            shim,
            auth_plugin,
            username,
            b"0123456789abcdefghij",
            auth_data,
        )
        .await
    }

    #[tokio::test]
    async fn authenticate_rejects_other_users() {
        let shim = rejecting_shim();

        assert!(
            !authenticate(&shim, "mysql_native_password", b"other", b"").await,
            "only the configured user may authenticate"
        );
        assert!(
            !authenticate(&shim, "mysql_native_password", b"", b"").await,
            "an empty user name must not authenticate"
        );
        assert!(
            !authenticate(&shim, "mysql_native_password", b"ROOT", b"").await,
            "the user name comparison is exact, not case-insensitive"
        );
    }

    #[tokio::test]
    async fn authenticate_rejects_non_empty_credentials() {
        let shim = rejecting_shim();

        assert!(
            !authenticate(
                &shim,
                "mysql_native_password",
                ROOT_USER.as_bytes(),
                b"secret"
            )
            .await,
            "a non-empty scramble must not authenticate against the empty password"
        );
    }

    #[tokio::test]
    async fn authenticate_rejects_other_auth_plugins() {
        let shim = rejecting_shim();

        assert!(
            !authenticate(&shim, "caching_sha2_password", ROOT_USER.as_bytes(), b"").await,
            "only mysql_native_password is supported"
        );
        assert!(
            !authenticate(&shim, "", ROOT_USER.as_bytes(), b"").await,
            "an absent auth plugin must not authenticate"
        );
    }
}

#[cfg(test)]
mod tests {
    use std::future::pending;
    use std::sync::Mutex;

    use tokio::net::TcpStream;
    use tokio::sync::oneshot;

    use super::*;

    mod shutdown_lifecycle {
        use super::*;

        const TEST_TIMEOUT: Duration = Duration::from_secs(1);

        #[derive(Clone)]
        struct DropProbe(Arc<AtomicBool>);

        impl Drop for DropProbe {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }

        async fn wait_until_connect_refused(addr: SocketAddr) {
            tokio::time::timeout(TEST_TIMEOUT, async {
                loop {
                    match TcpStream::connect(addr).await {
                        Ok(stream) => drop(stream),
                        Err(_) => break,
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("listener should stop accepting within the test timeout");
        }

        #[tokio::test]
        async fn shutdown_before_first_connection_stops_accepting() {
            let (ready_tx, ready_rx) = oneshot::channel();
            let (shutdown_tx, shutdown_rx) = oneshot::channel();
            let server = tokio::spawn(serve_until_shutdown(
                SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
                async move {
                    let _ = shutdown_rx.await;
                },
                |_stream, _peer_addr| async move {
                    panic!("no connection should be accepted before shutdown")
                },
                move |addr| {
                    let _ = ready_tx.send(addr);
                },
            ));
            let addr = tokio::time::timeout(TEST_TIMEOUT, ready_rx)
                .await
                .expect("server should bind within the test timeout")
                .expect("ready sender should stay alive");

            shutdown_tx.send(()).expect("send shutdown");
            tokio::time::timeout(TEST_TIMEOUT, server)
                .await
                .expect("server should stop within the test timeout")
                .expect("server task should not panic")
                .expect("server shutdown should succeed");

            assert!(TcpStream::connect(addr).await.is_err());
        }

        #[tokio::test]
        async fn shutdown_stops_new_accepts_and_waits_for_active_session() {
            let (ready_tx, ready_rx) = oneshot::channel();
            let (shutdown_tx, shutdown_rx) = oneshot::channel();
            let (started_tx, started_rx) = oneshot::channel();
            let started_tx = Arc::new(Mutex::new(Some(started_tx)));
            let (release_tx, release_rx) = oneshot::channel();
            let release_rx = Arc::new(Mutex::new(Some(release_rx)));
            let server = tokio::spawn(serve_until_shutdown(
                SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
                async move {
                    let _ = shutdown_rx.await;
                },
                move |_stream, _peer_addr| {
                    let started_tx = Arc::clone(&started_tx);
                    let release_rx = Arc::clone(&release_rx);
                    async move {
                        if let Some(started_tx) = started_tx.lock().expect("started lock").take() {
                            let _ = started_tx.send(());
                        }
                        let release_rx = release_rx.lock().expect("release lock").take();
                        if let Some(release_rx) = release_rx {
                            let _ = release_rx.await;
                        }
                    }
                },
                move |addr| {
                    let _ = ready_tx.send(addr);
                },
            ));
            let addr = tokio::time::timeout(TEST_TIMEOUT, ready_rx)
                .await
                .expect("server should bind within the test timeout")
                .expect("ready sender should stay alive");
            let _client = TcpStream::connect(addr)
                .await
                .expect("connect active session");
            tokio::time::timeout(TEST_TIMEOUT, started_rx)
                .await
                .expect("session should start within the test timeout")
                .expect("session start sender should stay alive");

            shutdown_tx.send(()).expect("send shutdown");
            wait_until_connect_refused(addr).await;
            assert!(
                !server.is_finished(),
                "server must wait for the accepted session to finish"
            );

            release_tx.send(()).expect("release active session");
            tokio::time::timeout(TEST_TIMEOUT, server)
                .await
                .expect("server should stop after session release")
                .expect("server task should not panic")
                .expect("server shutdown should succeed");
        }

        #[tokio::test]
        async fn drain_timeout_aborts_stuck_session() {
            assert_eq!(SESSION_DRAIN_TIMEOUT, Duration::from_secs(5));

            let (ready_tx, ready_rx) = oneshot::channel();
            let (shutdown_tx, shutdown_rx) = oneshot::channel();
            let (started_tx, started_rx) = oneshot::channel();
            let started_tx = Arc::new(Mutex::new(Some(started_tx)));
            let session_dropped = Arc::new(AtomicBool::new(false));
            let session_dropped_in_task = Arc::clone(&session_dropped);
            let server = tokio::spawn(serve_until_shutdown_with_drain_timeout(
                SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
                async move {
                    let _ = shutdown_rx.await;
                },
                move |_stream, _peer_addr| {
                    let started_tx = Arc::clone(&started_tx);
                    let session_dropped = Arc::clone(&session_dropped_in_task);
                    async move {
                        let _probe = DropProbe(session_dropped);
                        if let Some(started_tx) = started_tx.lock().expect("started lock").take() {
                            let _ = started_tx.send(());
                        }
                        pending::<()>().await;
                    }
                },
                move |addr| {
                    let _ = ready_tx.send(addr);
                },
                Duration::from_millis(20),
            ));
            let addr = tokio::time::timeout(TEST_TIMEOUT, ready_rx)
                .await
                .expect("server should bind within the test timeout")
                .expect("ready sender should stay alive");
            let _client = TcpStream::connect(addr)
                .await
                .expect("connect stuck session");
            tokio::time::timeout(TEST_TIMEOUT, started_rx)
                .await
                .expect("session should start within the test timeout")
                .expect("session start sender should stay alive");

            shutdown_tx.send(()).expect("send shutdown");
            tokio::time::timeout(TEST_TIMEOUT, server)
                .await
                .expect("server should abort the stuck session")
                .expect("server task should not panic")
                .expect("server shutdown should succeed");

            assert!(session_dropped.load(Ordering::SeqCst));
            assert!(TcpStream::connect(addr).await.is_err());
        }

        #[tokio::test]
        async fn bind_failure_returns_without_ready_marker() {
            let occupied = TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
                .await
                .expect("reserve test address");
            let addr = occupied.local_addr().expect("reserved address");
            let ready_emitted = Arc::new(AtomicBool::new(false));
            let ready_emitted_in_callback = Arc::clone(&ready_emitted);

            let err = serve_until_shutdown(
                addr,
                pending::<()>(),
                |_stream, _peer_addr| async move {},
                move |_addr| ready_emitted_in_callback.store(true, Ordering::SeqCst),
            )
            .await
            .expect_err("occupied address should fail to bind");

            assert!(err.contains("bind standalone mysql server"), "{err}");
            assert!(!ready_emitted.load(Ordering::SeqCst));
        }
    }
}
