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

//! Attempt-scoped sharing of one fallible acquisition (L2, L16), and the
//! session establishment entry every session user goes through.
//!
//! Upstream's `OnceCell::get_or_try_init` shares only success: after a failed
//! initialization each waiter re-initializes in turn, so one batch of calls
//! pays one handshake timeout per waiter. An [`AttemptCell`] shares the whole
//! outcome of one attempt with every participant instead, never caches a
//! failure and never retries.

use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};

use futures::future::{BoxFuture, FutureExt, Shared, WeakShared};

use super::auth::RestAuthOwner;
use super::binding::SessionBinding;
use super::evidence::BindingFailure;

/// Identity of one attempt of one [`AttemptCell`]; distinct for every attempt
/// the cell starts.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct AttemptId(u64);

/// A failed acquisition: the attempt that failed and its failed step. Every
/// participant of that attempt observes the same value.
#[derive(Clone, Eq, PartialEq)]
pub(crate) struct AttemptFailure {
    pub(crate) attempt: AttemptId,
    pub(crate) step: BindingFailure,
    pub(crate) original: Option<novarocks_spi::connector::ConnectorError>,
}

/// A failed step retains an already-selected original refusal separately from
/// its transport/session diagnosis. Neither field grants another Root control.
#[derive(Clone, Eq, PartialEq)]
pub(crate) struct BindingStepError {
    pub(crate) step: BindingFailure,
    pub(crate) original: Option<novarocks_spi::connector::ConnectorError>,
}
impl fmt::Debug for BindingStepError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BindingStepError")
            .field("step", &self.step)
            .field(
                "original_fact",
                &self
                    .original
                    .as_ref()
                    .map(novarocks_failure_contract::FailureProjection::failure_fact),
            )
            .finish_non_exhaustive()
    }
}
impl fmt::Debug for AttemptFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AttemptFailure")
            .field("attempt", &self.attempt)
            .field("step", &self.step)
            .field(
                "original_fact",
                &self
                    .original
                    .as_ref()
                    .map(novarocks_failure_contract::FailureProjection::failure_fact),
            )
            .finish_non_exhaustive()
    }
}
impl From<BindingFailure> for BindingStepError {
    fn from(step: BindingFailure) -> Self {
        Self {
            step,
            original: None,
        }
    }
}
impl From<AttemptFailure> for BindingStepError {
    fn from(failure: AttemptFailure) -> Self {
        Self {
            step: failure.step,
            original: failure.original,
        }
    }
}
impl BindingStepError {
    pub(crate) fn from_original(
        step: BindingFailure,
        original: novarocks_spi::connector::ConnectorError,
    ) -> Self {
        Self {
            step,
            original: Some(original),
        }
    }
}
impl AttemptFailure {
    pub(crate) fn from_cause(attempt: AttemptId, cause: BindingStepError) -> Self {
        Self {
            attempt,
            step: cause.step,
            original: cause.original,
        }
    }
    pub(crate) fn into_connector(
        self,
        fallback: impl FnOnce(BindingFailure) -> novarocks_spi::connector::ConnectorError,
    ) -> novarocks_spi::connector::ConnectorError {
        self.original.unwrap_or_else(|| fallback(self.step))
    }
}

type AttemptFuture<T, E> = BoxFuture<'static, Result<T, E>>;

enum AttemptState<T, E> {
    Idle,
    /// Only a weak reference is kept: when every participant gives up, the
    /// attempt is dropped with them.
    Running {
        id: AttemptId,
        attempt: WeakShared<AttemptFuture<T, E>>,
    },
    Ready(T),
}

/// The shared result of one attempt at a time; success is kept, failure is
/// handed to that attempt's participants and the cell returns to idle.
pub(crate) struct AttemptCell<T, E> {
    state: Mutex<CellState<T, E>>,
}

struct CellState<T, E> {
    state: AttemptState<T, E>,
    next_id: u64,
}

impl<T, E> fmt::Debug for AttemptCell<T, E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let state = match state.state {
            AttemptState::Idle => "idle",
            AttemptState::Running { .. } => "running",
            AttemptState::Ready(_) => "ready",
        };
        formatter
            .debug_struct("AttemptCell")
            .field("state", &state)
            .finish()
    }
}

impl<T, E> Default for AttemptCell<T, E> {
    fn default() -> Self {
        Self {
            state: Mutex::new(CellState {
                state: AttemptState::Idle,
                next_id: 0,
            }),
        }
    }
}

impl<T, E> AttemptCell<T, E>
where
    T: Clone + Send + Sync + 'static,
    E: Clone + Send + Sync + 'static,
{
    /// The kept success, if an attempt already succeeded.
    pub(crate) fn ready(&self) -> Option<T> {
        match &self
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .state
        {
            AttemptState::Ready(value) => Some(value.clone()),
            _ => None,
        }
    }

    /// Returns the kept success, joins the running attempt, or starts a new
    /// attempt with `start` and a fresh [`AttemptId`].
    ///
    /// All participants drive the attempt together: cancelling the one that
    /// started it does not cancel it while others still wait.
    pub(crate) async fn get_or_attempt<F>(&self, start: F) -> Result<T, E>
    where
        F: FnOnce(AttemptId) -> AttemptFuture<T, E>,
    {
        let (id, attempt) = {
            let mut cell = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            match cell.join_running() {
                Joined::Ready(value) => return Ok(value),
                Joined::Running(id, attempt) => (id, attempt),
                Joined::None => {
                    let id = AttemptId(cell.next_id);
                    cell.next_id += 1;
                    let attempt = start(id).shared();
                    let weak = attempt
                        .downgrade()
                        .expect("a freshly created attempt has not completed");
                    cell.state = AttemptState::Running { id, attempt: weak };
                    (id, attempt)
                }
            }
        };
        let result = attempt.await;
        let mut cell = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if matches!(cell.state, AttemptState::Running { id: running, .. } if running == id) {
            cell.state = match &result {
                Ok(value) => AttemptState::Ready(value.clone()),
                // Failure is never cached: the next caller starts a new attempt.
                Err(_) => AttemptState::Idle,
            };
        }
        result
    }
}

enum Joined<T, E> {
    Ready(T),
    Running(AttemptId, Shared<AttemptFuture<T, E>>),
    None,
}

impl<T, E> CellState<T, E>
where
    T: Clone + Send + Sync + 'static,
    E: Clone + Send + Sync + 'static,
{
    fn join_running(&mut self) -> Joined<T, E> {
        let (id, attempt) = match &self.state {
            AttemptState::Ready(value) => return Joined::Ready(value.clone()),
            AttemptState::Idle => return Joined::None,
            AttemptState::Running { id, attempt } => match attempt.upgrade() {
                Some(attempt) => (*id, attempt),
                // Every participant gave up; the attempt is gone.
                None => return Joined::None,
            },
        };
        match attempt.peek() {
            None => Joined::Running(id, attempt),
            // Completed, but its participants have not settled the state yet.
            Some(Ok(value)) => {
                let value = value.clone();
                self.state = AttemptState::Ready(value.clone());
                Joined::Ready(value)
            }
            // A failure already ended that attempt: arriving now starts the
            // next one rather than inheriting the failure.
            Some(Err(_)) => Joined::None,
        }
    }
}

/// The session establishment gate the REST owner holds (L2).
pub(crate) type SessionGate = AttemptCell<Arc<SessionBinding>, AttemptFailure>;

/// The establishment entry every NovaRocks request and every read through
/// upstream passes first: returns the session binding, joining or starting
/// one establishment attempt.
// Design: ADR-0175 (docs/adr/ADR-0175-iceberg-rest-session-authority-and-mutation-evidence.md)
pub(crate) async fn ensure_session(
    owner: &Arc<RestAuthOwner>,
    gate: &SessionGate,
    properties: &Arc<std::collections::HashMap<String, String>>,
) -> Result<Arc<SessionBinding>, AttemptFailure> {
    ensure_session_in(
        owner,
        gate,
        properties,
        &super::http::RequestContext::for_read(),
    )
    .await
}
pub(crate) async fn ensure_session_in(
    owner: &Arc<RestAuthOwner>,
    gate: &SessionGate,
    properties: &Arc<std::collections::HashMap<String, String>>,
    context: &super::http::RequestContext,
) -> Result<Arc<SessionBinding>, AttemptFailure> {
    gate.get_or_attempt(|attempt| {
        let context = context.clone();
        let owner = Arc::clone(owner);
        let properties = Arc::clone(properties);
        async move {
            let _initializer = owner.activity().map(|owner| {
                owner.enter(novarocks_spi::connector::CatalogActivityPhase::SessionInitializer)
            });
            tokio::time::timeout(
                crate::catalog_runtime::REST_READ_TIMEOUT,
                super::configuration::establish_in(&owner, &properties, &context),
            )
            .await
            .unwrap_or(Err(BindingFailure::ConfigFetch.into()))
            .map(Arc::new)
            .map_err(|cause| AttemptFailure::from_cause(attempt, cause))
        }
        .boxed()
    })
    .await
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use super::*;

    fn counting_start(
        starts: &Arc<AtomicUsize>,
        delay: Duration,
        outcome: Result<u32, &'static str>,
    ) -> impl FnOnce(AttemptId) -> AttemptFuture<u32, (AttemptId, &'static str)> {
        let starts = Arc::clone(starts);
        move |attempt| {
            starts.fetch_add(1, Ordering::SeqCst);
            async move {
                tokio::time::sleep(delay).await;
                outcome.map_err(|error| (attempt, error))
            }
            .boxed()
        }
    }

    #[tokio::test]
    async fn concurrent_callers_share_one_failed_attempt_and_the_next_call_starts_another() {
        let cell = Arc::new(AttemptCell::<u32, (AttemptId, &'static str)>::default());
        let starts = Arc::new(AtomicUsize::new(0));
        let mut tasks = Vec::new();
        for _ in 0..32 {
            let cell = Arc::clone(&cell);
            let starts = Arc::clone(&starts);
            tasks.push(tokio::spawn(async move {
                cell.get_or_attempt(counting_start(
                    &starts,
                    Duration::from_millis(100),
                    Err("boom"),
                ))
                .await
            }));
        }
        let mut failures = Vec::new();
        for task in tasks {
            failures.push(task.await.unwrap().unwrap_err());
        }
        assert_eq!(starts.load(Ordering::SeqCst), 1);
        assert!(failures.iter().all(|failure| *failure == failures[0]));
        assert!(cell.ready().is_none());

        let second = cell
            .get_or_attempt(counting_start(&starts, Duration::ZERO, Ok(7)))
            .await;
        assert_eq!(second, Ok(7));
        assert_eq!(starts.load(Ordering::SeqCst), 2);
        // Success is kept; no further attempt starts.
        let third = cell
            .get_or_attempt(counting_start(&starts, Duration::ZERO, Ok(8)))
            .await;
        assert_eq!(third, Ok(7));
        assert_eq!(starts.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn cancelling_the_initiator_does_not_cancel_the_attempt() {
        let cell = Arc::new(AttemptCell::<u32, (AttemptId, &'static str)>::default());
        let starts = Arc::new(AtomicUsize::new(0));
        let initiator = {
            let cell = Arc::clone(&cell);
            let starts = Arc::clone(&starts);
            tokio::spawn(async move {
                cell.get_or_attempt(counting_start(&starts, Duration::from_millis(200), Ok(1)))
                    .await
            })
        };
        tokio::time::sleep(Duration::from_millis(20)).await;
        let joiner = {
            let cell = Arc::clone(&cell);
            let starts = Arc::clone(&starts);
            tokio::spawn(async move {
                cell.get_or_attempt(counting_start(&starts, Duration::ZERO, Ok(2)))
                    .await
            })
        };
        tokio::time::sleep(Duration::from_millis(20)).await;
        initiator.abort();
        assert!(initiator.await.unwrap_err().is_cancelled());
        assert_eq!(joiner.await.unwrap(), Ok(1));
        assert_eq!(starts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn an_attempt_every_participant_abandoned_is_dropped() {
        struct DropFlag(Arc<AtomicUsize>);
        impl Drop for DropFlag {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let cell = Arc::new(AttemptCell::<u32, (AttemptId, &'static str)>::default());
        let dropped = Arc::new(AtomicUsize::new(0));
        let mut tasks = Vec::new();
        for _ in 0..2 {
            let cell = Arc::clone(&cell);
            let dropped = Arc::clone(&dropped);
            tasks.push(tokio::spawn(async move {
                cell.get_or_attempt(move |_| {
                    let flag = DropFlag(dropped);
                    async move {
                        let _flag = flag;
                        futures::future::pending::<()>().await;
                        Ok(1)
                    }
                    .boxed()
                })
                .await
            }));
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        for task in &tasks {
            task.abort();
        }
        for task in tasks {
            assert!(task.await.unwrap_err().is_cancelled());
        }
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        let starts = Arc::new(AtomicUsize::new(0));
        let next = cell
            .get_or_attempt(counting_start(&starts, Duration::ZERO, Ok(3)))
            .await;
        assert_eq!(next, Ok(3));
        assert_eq!(starts.load(Ordering::SeqCst), 1);
    }
}

#[cfg(test)]
mod establishment_tests {
    use std::time::{Duration, Instant};

    use reqwest::header::HeaderMap;

    use super::*;
    use crate::iceberg::{NamespaceIdent, TableIdent};
    use crate::rest_protocol::binding::protocol_headers;
    use crate::rest_protocol::http::RestHttpClient;
    use crate::rest_protocol::test_server::{
        FrontendFixture, Reply, Script, TestServer, oauth2_identity, request_lines,
        wait_until_requests,
    };

    const TABLE_HEAD: &str = "HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}";

    fn client() -> RestHttpClient {
        RestHttpClient::with_timeouts(Duration::from_secs(2), Duration::from_secs(5)).unwrap()
    }

    fn short_read_client(read_timeout: Duration) -> RestHttpClient {
        RestHttpClient::with_timeouts(read_timeout / 2, read_timeout).unwrap()
    }

    #[tokio::test]
    async fn establishment_sends_one_config_and_one_token_request_and_never_the_probe() {
        let server = TestServer::start(
            Script::new()
                .on("POST", "/v1/oauth/tokens", Reply::token("exchanged-token"))
                .on("GET", "/v1/config", Reply::config(&[], &[], None)),
        );
        let fixture = FrontendFixture::new(
            &[("uri", &server.url())],
            Some(oauth2_identity("client", "client-secret")),
            client(),
        )
        .await;
        let results = futures::future::join_all((0..8).map(|_| fixture.ensure_session())).await;
        let first = results[0].clone().expect("established");
        assert!(
            results
                .iter()
                .all(|result| Arc::ptr_eq(result.as_ref().unwrap(), &first))
        );
        let again = fixture.ensure_session().await.unwrap();
        assert!(Arc::ptr_eq(&again, &first));
        assert_eq!(
            request_lines(&server.requests()),
            vec!["POST /v1/oauth/tokens", "GET /v1/config"]
        );
        // Upstream's handshake carries the role token and the protocol
        // headers NovaRocks mirrors (L30).
        let config = &server.requests_to("GET", "/v1/config")[0];
        assert_eq!(
            config.header("authorization"),
            Some("Bearer exchanged-token")
        );
        for (name, value) in protocol_headers().iter() {
            assert_eq!(
                config.header(name.as_str()),
                Some(value.to_str().unwrap()),
                "{name}"
            );
        }
        assert!(!first.table_head_advertised());
    }

    #[tokio::test]
    async fn concurrent_cold_callers_share_one_failed_attempt() {
        const READ_TIMEOUT: Duration = Duration::from_millis(500);
        let server = TestServer::start(Script::new().on("GET", "/v1/config", Reply::NeverAnswer));
        let fixture = Arc::new(
            FrontendFixture::new(
                &[("uri", &server.url())],
                None,
                short_read_client(READ_TIMEOUT),
            )
            .await,
        );
        let started = Instant::now();
        let mut tasks = Vec::new();
        for _ in 0..32 {
            let fixture = Arc::clone(&fixture);
            tasks.push(tokio::spawn(async move { fixture.ensure_session().await }));
        }
        let mut failures = Vec::new();
        for task in tasks {
            failures.push(task.await.unwrap().unwrap_err());
        }
        let elapsed = started.elapsed();
        assert_eq!(server.count("GET", "/v1/config"), 1);
        assert!(failures.iter().all(|failure| *failure == failures[0]));
        assert_eq!(failures[0].step, BindingFailure::ConfigFetch);
        assert!(elapsed >= READ_TIMEOUT, "{elapsed:?}");
        assert!(elapsed < READ_TIMEOUT * 3, "{elapsed:?}");

        // Arriving after the failure starts the next attempt.
        let second = fixture.ensure_session().await.unwrap_err();
        assert_ne!(second.attempt, failures[0].attempt);
        assert_eq!(second.step, BindingFailure::ConfigFetch);
        assert_eq!(server.count("GET", "/v1/config"), 2);
    }

    #[tokio::test]
    async fn cancelling_the_initiator_leaves_the_attempt_to_the_other_participants() {
        let server = TestServer::start(Script::new().on(
            "GET",
            "/v1/config",
            Reply::config(&[], &[], None).with_delay(Duration::from_millis(300)),
        ));
        let fixture =
            Arc::new(FrontendFixture::new(&[("uri", &server.url())], None, client()).await);
        let initiator = {
            let fixture = Arc::clone(&fixture);
            tokio::spawn(async move { fixture.ensure_session().await })
        };
        wait_until_requests(&server, Duration::from_secs(5), |requests| {
            !requests.is_empty()
        })
        .await;
        let mut joiners = Vec::new();
        for _ in 0..3 {
            let fixture = Arc::clone(&fixture);
            joiners.push(tokio::spawn(async move { fixture.ensure_session().await }));
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
        initiator.abort();
        assert!(initiator.await.unwrap_err().is_cancelled());
        let mut bindings = Vec::new();
        for joiner in joiners {
            bindings.push(joiner.await.unwrap().expect("the attempt completed"));
        }
        assert!(
            bindings
                .iter()
                .all(|binding| Arc::ptr_eq(binding, &bindings[0]))
        );
        assert_eq!(server.count("GET", "/v1/config"), 1);
    }

    #[tokio::test]
    async fn an_attempt_every_participant_abandoned_is_dropped() {
        let server = TestServer::start(Script::new().on_sequence(
            "GET",
            "/v1/config",
            vec![Reply::NeverAnswer, Reply::config(&[], &[], None)],
        ));
        let fixture =
            Arc::new(FrontendFixture::new(&[("uri", &server.url())], None, client()).await);
        let mut participants = Vec::new();
        for _ in 0..2 {
            let fixture = Arc::clone(&fixture);
            participants.push(tokio::spawn(async move { fixture.ensure_session().await }));
        }
        wait_until_requests(&server, Duration::from_secs(5), |requests| {
            !requests.is_empty()
        })
        .await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        for participant in &participants {
            participant.abort();
        }
        for participant in participants {
            assert!(participant.await.unwrap_err().is_cancelled());
        }
        // The next call starts a fresh attempt instead of waiting on the
        // abandoned one, whose request never got an answer.
        let binding = tokio::time::timeout(Duration::from_secs(3), fixture.ensure_session())
            .await
            .expect("a fresh attempt")
            .expect("established");
        assert_eq!(binding.uri(), server.url());
        assert_eq!(server.count("GET", "/v1/config"), 2);
    }

    #[tokio::test]
    async fn each_failed_step_belongs_to_its_own_attempt() {
        let server = TestServer::start(
            Script::new()
                .on_sequence(
                    "POST",
                    "/v1/oauth/tokens",
                    vec![Reply::status(401), Reply::token("tok")],
                )
                .on_sequence(
                    "GET",
                    "/v1/config",
                    vec![
                        Reply::config(&[], &[("credential", "server:secret")], None),
                        Reply::config(&[], &[], None),
                    ],
                ),
        );
        let fixture = FrontendFixture::new(
            &[("uri", &server.url())],
            Some(oauth2_identity("client", "client-secret")),
            client(),
        )
        .await;
        let token_rejected = fixture.ensure_session().await.unwrap_err();
        assert_eq!(token_rejected.step, BindingFailure::TokenRejected);
        assert_eq!(server.count("GET", "/v1/config"), 0);

        let invalid_config = fixture.ensure_session().await.unwrap_err();
        assert_eq!(invalid_config.step, BindingFailure::InvalidServerConfig);
        assert_ne!(invalid_config.attempt, token_rejected.attempt);

        let binding = fixture.ensure_session().await.expect("established");
        assert_eq!(binding.uri(), server.url());
        // The token is exchanged once successfully and then kept.
        assert_eq!(server.count("POST", "/v1/oauth/tokens"), 2);
        assert_eq!(server.count("GET", "/v1/config"), 2);
    }

    async fn probe_outcome(
        overrides: &[(&str, &str)],
        endpoints: Option<&[&str]>,
    ) -> (Arc<SessionBinding>, Vec<String>) {
        let server = TestServer::start(Script::new().on(
            "GET",
            "/v1/config",
            Reply::config(&[], overrides, endpoints),
        ));
        let fixture = FrontendFixture::new(&[("uri", &server.url())], None, client()).await;
        let binding = fixture.ensure_session().await.expect("established");
        (binding, request_lines(&server.requests()))
    }

    #[tokio::test]
    async fn the_probe_observes_head_when_the_server_advertises_it() {
        let (binding, requests) = probe_outcome(
            &[],
            Some(&[
                TABLE_HEAD,
                "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}",
            ]),
        )
        .await;
        assert!(binding.table_head_advertised());
        assert_eq!(requests, vec!["GET /v1/config"]);
    }

    #[tokio::test]
    async fn the_probe_observes_get_when_head_is_not_advertised() {
        let (binding, requests) = probe_outcome(&[], None).await;
        assert!(!binding.table_head_advertised());
        assert_eq!(requests, vec!["GET /v1/config"]);
    }

    #[tokio::test]
    async fn invalid_effective_route_refuses_establishment() {
        let server = TestServer::start(Script::new().on(
            "GET",
            "/v1/config",
            Reply::config(&[], &[("uri", "not-a-url")], Some(&[TABLE_HEAD])),
        ));
        let fixture = FrontendFixture::new(&[("uri", &server.url())], None, client()).await;
        assert_eq!(
            fixture.ensure_session().await.unwrap_err().step,
            BindingFailure::InvalidServerConfig
        );
        assert_eq!(server.count("GET", "/v1/config"), 1);
    }

    #[tokio::test]
    async fn requests_after_establishment_are_authenticated_and_not_vetoed() {
        let server = TestServer::start(
            Script::new()
                .on(
                    "GET",
                    "/v1/config",
                    Reply::config(&[], &[], Some(&[TABLE_HEAD])),
                )
                .on(
                    "HEAD",
                    "/v1/namespaces/db/tables/orders",
                    Reply::status(204),
                ),
        );
        let fixture = FrontendFixture::new(
            &[("uri", &server.url())],
            Some(crate::rest_protocol::test_server::bearer_identity(
                "role-token",
            )),
            client(),
        )
        .await;
        fixture.ensure_session().await.unwrap();
        let table = TableIdent::new(NamespaceIdent::new("db".to_string()), "orders".to_string());
        assert!(fixture.endpoints.table_exists(&table).await.unwrap());
        let head = &server.requests_to("HEAD", "/v1/namespaces/db/tables/orders")[0];
        assert_eq!(head.header("authorization"), Some("Bearer role-token"));
        let mut headers = HeaderMap::new();
        fixture.owner.authorize(&mut headers).await.unwrap();
        assert_eq!(headers["authorization"], "Bearer role-token");
    }
}

#[cfg(test)]
mod typed_cause_tests {
    use super::*;
    #[test]
    fn private_attempt_debug_keeps_steps_but_never_formats_original_details() {
        let original = novarocks_spi::connector::ConnectorError::compile_interruption(
            novarocks_type_contract::CompileControlError::from_domain(
                novarocks_type_contract::owned_resources::layout::LayoutResourceError::SourceModel,
            ),
        )
        .map_message(|_| "private-secret-url-header-body".to_string());
        let step =
            BindingStepError::from_original(BindingFailure::TokenUnavailable, original.clone());
        let attempt = AttemptFailure::from_cause(AttemptId(13), step.clone());
        for debug in [format!("{step:?}"), format!("{attempt:?}")] {
            assert!(debug.contains("TokenUnavailable"));
            assert!(!debug.contains("private-secret"));
        }
        assert_eq!(
            attempt.into_connector(|_| panic!("the original cause exists")),
            original
        );
    }
}
