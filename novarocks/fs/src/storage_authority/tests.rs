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

//! Local state-machine coverage for the consumer-side storage authority.
//!
//! Every test here drives acquisition through an injected source and a queued
//! executor, so nothing depends on wall-clock sleeps or on a live catalog. The
//! deadlines used are either already elapsed or far in the future, which keeps
//! these cases off the load-sensitive list.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};

use novarocks_spi::connector::{CatalogVersion, ConnectorInstanceId};

use super::*;

/// Runs queued refresh jobs only when the test asks, so every publication
/// point is an explicit step rather than a race.
#[derive(Default)]
struct QueuedExecutor {
    jobs: Mutex<VecDeque<Box<dyn FnOnce() + Send + 'static>>>,
    accepted: AtomicUsize,
}

impl QueuedExecutor {
    fn pending(&self) -> usize {
        self.jobs.lock().expect("queued executor lock").len()
    }

    fn accepted(&self) -> usize {
        self.accepted.load(Ordering::Relaxed)
    }

    /// Runs one queued job to completion on this thread.
    fn run_one(&self) -> bool {
        let job = self.jobs.lock().expect("queued executor lock").pop_front();
        match job {
            Some(job) => {
                job();
                true
            }
            None => false,
        }
    }
}

impl RefreshExecutor for QueuedExecutor {
    fn execute(&self, job: Box<dyn FnOnce() + Send + 'static>) {
        self.accepted.fetch_add(1, Ordering::Relaxed);
        self.jobs
            .lock()
            .expect("queued executor lock")
            .push_back(job);
    }
}

/// Hands out programmed outcomes in order, repeating the last one.
struct ScriptedSource {
    outcomes: Mutex<VecDeque<Result<AuthorityMaterial, AcquisitionFailure>>>,
    last: Mutex<Result<AuthorityMaterial, AcquisitionFailure>>,
    calls: AtomicUsize,
}

impl ScriptedSource {
    fn new(outcomes: Vec<Result<AuthorityMaterial, AcquisitionFailure>>) -> Self {
        let last = outcomes
            .last()
            .cloned()
            .unwrap_or(Err(AcquisitionFailure::NoRenewalCapability));
        Self {
            outcomes: Mutex::new(outcomes.into()),
            last: Mutex::new(last),
            calls: AtomicUsize::new(0),
        }
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::Relaxed)
    }
}

impl AuthorityMaterialSource for ScriptedSource {
    fn acquire(&self, _deadline: Instant) -> Result<AuthorityMaterial, AcquisitionFailure> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        let next = self
            .outcomes
            .lock()
            .expect("scripted source lock")
            .pop_front();
        match next {
            Some(outcome) => outcome,
            None => self.last.lock().expect("scripted source lock").clone(),
        }
    }
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("current-thread runtime")
}

fn catalog(name: &str) -> CatalogHandle {
    CatalogHandle::new(
        ConnectorInstanceId::parse(name).expect("instance id"),
        CatalogVersion::from_bytes([7; 32]),
    )
}

fn prefix(value: &str) -> StorageCredentialScopePrefix {
    StorageCredentialScopePrefix::try_from_normalized(value).expect("scope prefix")
}

fn principal(generation: &str) -> StaticCredentialReference {
    StaticCredentialReference::try_new("vending", generation).expect("credential reference")
}

fn routing(url: &str, headers: &[(&str, &str)]) -> CredentialRoutingContext {
    CredentialRoutingContext::try_new(
        url,
        headers
            .iter()
            .map(|(name, value)| (Arc::from(*name), Arc::from(*value))),
    )
    .expect("routing context")
}

fn endpoint_capability(url: &str) -> AuthorityCapabilityPath {
    AuthorityCapabilityPath::CredentialsEndpoint {
        principal: principal("g1"),
        routing: routing(url, &[]),
    }
}

fn endpoint_capability_as(generation: &str, url: &str) -> AuthorityCapabilityPath {
    AuthorityCapabilityPath::CredentialsEndpoint {
        principal: principal(generation),
        routing: routing(url, &[]),
    }
}

fn identity(url: &str) -> StorageAuthorityId {
    StorageAuthorityId::new(
        catalog("lake"),
        prefix("s3://warehouse/sales/orders/"),
        endpoint_capability(url),
    )
}

fn material(not_after: Instant) -> AuthorityMaterial {
    AuthorityMaterial::new(
        SecretValue::new("ak"),
        SecretValue::new("sk"),
        Some(SecretValue::new("token")),
        not_after,
    )
}

/// A material value distinguishable from `material` by its access key id.
fn renewed_material(not_after: Instant) -> AuthorityMaterial {
    AuthorityMaterial::new(
        SecretValue::new("renewed-ak"),
        SecretValue::new("sk"),
        Some(SecretValue::new("token")),
        not_after,
    )
}

struct Fixture {
    authority: Arc<StorageAuthority>,
    executor: Arc<QueuedExecutor>,
    source: Arc<ScriptedSource>,
}

fn fixture_with(
    id: StorageAuthorityId,
    outcomes: Vec<Result<AuthorityMaterial, AcquisitionFailure>>,
    policy: RefreshPolicy,
) -> Fixture {
    let executor = Arc::new(QueuedExecutor::default());
    let source = Arc::new(ScriptedSource::new(outcomes));
    let authority = Arc::new(StorageAuthority::new(
        id,
        Arc::clone(&source) as Arc<dyn AuthorityMaterialSource>,
        Arc::clone(&executor) as Arc<dyn RefreshExecutor>,
        policy,
    ));
    Fixture {
        authority,
        executor,
        source,
    }
}

fn policy() -> RefreshPolicy {
    RefreshPolicy {
        prefetch_divisor: 5,
        prefetch_window_min: Duration::from_secs(5),
        prefetch_window_max: Duration::from_secs(300),
        validity_margin_divisor: 20,
        validity_margin_min: Duration::from_secs(1),
        validity_margin_max: Duration::from_secs(30),
        min_backoff: Duration::from_millis(200),
        max_backoff: Duration::from_secs(5),
        blocked_acquisition_budget: Duration::from_secs(30),
    }
}

// ---------------------------------------------------------------------------
// CAD-1 D4: the three states
// ---------------------------------------------------------------------------

#[test]
fn usable_material_outside_the_prefetch_window_never_reaches_the_executor() {
    let now = Instant::now();
    let fixture = fixture_with(identity("https://catalog/credentials"), vec![], policy());
    fixture
        .authority
        .install_material(material(now + Duration::from_secs(3600)));

    let obtained = runtime().block_on(
        fixture
            .authority
            .material_for_request(now, now + Duration::from_secs(10)),
    );

    assert_eq!(
        obtained
            .expect("cached material")
            .access_key_id()
            .expose_secret(),
        "ak"
    );
    assert_eq!(
        fixture.executor.accepted(),
        0,
        "a cache hit must not start any acquisition"
    );
    assert_eq!(fixture.authority.metrics().cache_hits, 1);
}

#[test]
fn a_coordinator_and_an_execution_node_never_share_one_authority() {
    // CAD-1 D0 with D9. One process can run both roles, and they renew through
    // different things: the coordinator holds the provider capability already,
    // an execution node authenticates an announced path as itself. If those
    // keyed the same, one role's material would sign the other's requests.
    let owner = CatalogHandle::new(
        ConnectorInstanceId::parse("lake").unwrap(),
        CatalogVersion::from_bytes([0x11; 32]),
    );
    let scope =
        StorageCredentialScopePrefix::try_from_normalized("s3://warehouse/orders/").unwrap();
    let coordinator = StorageAuthorityId::new(
        owner.clone(),
        scope.clone(),
        AuthorityCapabilityPath::InProcessProvider(
            novarocks_spi::connector::VendedS3InProcessAcquisitionIdentity::try_new(
                novarocks_spi::connector::CredentialAcquisitionOwnerId::new(),
                Some(
                    novarocks_spi::connector::CredentialRenewalPath::CredentialsEndpoint(routing(
                        "https://catalog/credentials",
                        &[],
                    )),
                ),
                vec![scope.clone()],
            )
            .unwrap(),
        ),
    );
    let executor = StorageAuthorityId::new(
        owner,
        scope,
        AuthorityCapabilityPath::CredentialsEndpoint {
            principal: StaticCredentialReference::try_new("executor", "v1").unwrap(),
            routing: routing("https://catalog/credentials", &[]),
        },
    );

    assert_ne!(coordinator, executor);
    assert!(coordinator.capability().can_renew());
    assert!(
        coordinator.capability().principal().is_none(),
        "an in-process capability authenticates nothing of its own"
    );
}

#[test]
fn retried_requests_share_one_acquisition_effort_and_are_told_why_it_failed() {
    // CAD-1 D5 with D12, found by the 1FE+3BE vended scenario. The object store
    // retries a request whose credential load failed, and each retry used to
    // restart the acquisition with its own budget. The composed effort then
    // outlasted the query: the read died of an idle exchange, and the operator
    // was told about the exchange rather than about the catalog.
    let now = Instant::now();
    let fixture = fixture_with(
        identity("https://catalog/credentials"),
        vec![
            Err(AcquisitionFailure::CatalogUnreachable(
                "connect refused".into(),
            )),
            Ok(renewed_material(now + Duration::from_secs(3600))),
        ],
        RefreshPolicy {
            // Small on purpose: every wait below is a real one, because the
            // manual executor cannot run the job while a waiter blocks.
            blocked_acquisition_budget: Duration::from_secs(1),
            ..policy()
        },
    );

    // The first blocked caller opens the effort window and its acquisition
    // fails.
    let handle = runtime();
    let first = handle.block_on(
        fixture
            .authority
            .material_for_request(now, now + Duration::from_secs(1)),
    );
    assert!(fixture.executor.run_one());
    assert!(first.is_err());

    // A retry arriving after the window closed is refused with the catalog's
    // own reason rather than paying for a second window.
    let spent = now + Duration::from_secs(2);
    let refused = handle
        .block_on(
            fixture
                .authority
                .material_for_request(spent, spent + Duration::from_secs(60)),
        )
        .expect_err("the effort's window is spent");
    assert_eq!(refused.kind(), FileErrorKind::Transient);
    assert!(
        format!("{refused}").contains("could not reach its catalog"),
        "the operator must be told why, got {refused}"
    );
    assert_eq!(
        fixture.executor.accepted(),
        1,
        "a retry must not open a second acquisition window"
    );

    // Once the hold-off passes, a genuinely new operation may try again.
    let recovered = now + Duration::from_secs(10);
    let obtained = handle.block_on(
        fixture
            .authority
            .material_for_request(recovered, recovered + Duration::from_secs(60)),
    );
    assert!(fixture.executor.run_one());
    assert_eq!(fixture.executor.accepted(), 2);
    drop(obtained);
}

#[test]
fn a_credential_shorter_than_the_window_ceiling_is_not_refreshed_on_every_request() {
    // Found by the 1FE+3BE vended scenario, not by a unit test: with an
    // absolute 300s window and a catalog vending 60s credentials, an authority
    // sat permanently inside its own prefetch window, so every request that
    // found no refresh in flight started one. Catalog traffic then scaled with
    // request count instead of with expiry (CAD-1 acceptance 16).
    let now = Instant::now();
    let fixture = fixture_with(
        identity("https://catalog/credentials"),
        vec![Ok(renewed_material(now + Duration::from_secs(3600)))],
        policy(),
    );
    fixture
        .authority
        .install_material(material(now + Duration::from_secs(60)));

    let handle = runtime();
    for _ in 0..8 {
        handle
            .block_on(
                fixture
                    .authority
                    .material_for_request(now, now + Duration::from_secs(10)),
            )
            .expect("fresh material serves every request");
    }

    assert_eq!(
        fixture.executor.accepted(),
        0,
        "a credential with its whole life ahead of it must not be refreshed at all"
    );
    assert_eq!(fixture.authority.metrics().cache_hits, 8);

    // 50s in, 10s left against a ~12s window: now, and only now, one prefetch.
    let inside_window = now + Duration::from_secs(50);
    handle
        .block_on(
            fixture
                .authority
                .material_for_request(inside_window, inside_window + Duration::from_secs(10)),
        )
        .expect("still usable inside the window");
    assert_eq!(fixture.executor.accepted(), 1);
}

#[test]
fn prefetch_returns_current_material_and_a_transient_failure_fails_nothing() {
    let now = Instant::now();
    let fixture = fixture_with(
        identity("https://catalog/credentials"),
        vec![Err(AcquisitionFailure::Transient(
            "connection reset".into(),
        ))],
        policy(),
    );
    // A 120s credential, read 100s into its life: 20s left against a 24s
    // window, so it is inside the window and still comfortably usable. The
    // window is a fraction of the credential's own lifetime, so "inside it" is
    // a statement about elapsed time, not one about `not_after` alone.
    fixture
        .authority
        .install_material(material(now + Duration::from_secs(120)));
    let inside_window = now + Duration::from_secs(100);

    let obtained = runtime().block_on(
        fixture
            .authority
            .material_for_request(inside_window, inside_window + Duration::from_secs(10)),
    );

    assert!(obtained.is_ok(), "state one must not fail the operation");
    assert_eq!(fixture.executor.accepted(), 1, "prefetch must be started");
    assert_eq!(fixture.authority.metrics().prefetch_started, 1);

    // The refresh now fails transiently. The still-valid material survives.
    assert!(fixture.executor.run_one());
    let after = runtime().block_on(
        fixture
            .authority
            .material_for_request(now, now + Duration::from_secs(10)),
    );
    assert_eq!(
        after
            .expect("material survives a transient prefetch failure")
            .access_key_id()
            .expose_secret(),
        "ak"
    );
    assert_eq!(fixture.authority.metrics().refreshes_failed, 1);
}

#[test]
fn a_confirmed_denial_is_rejected_as_permission_not_retried_as_jitter() {
    let now = Instant::now();
    let fixture = fixture_with(
        identity("https://catalog/credentials"),
        vec![Err(AcquisitionFailure::Denied("token revoked".into()))],
        policy(),
    );

    // No material at all: state two, which has to wait for one acquisition.
    let handle = runtime();
    let first = handle.block_on(async {
        // Register the in-flight request without consuming its outcome.
        let waiter = fixture
            .authority
            .material_for_request(now, now.checked_sub(Duration::from_secs(1)).unwrap_or(now));
        waiter.await
    });
    assert_eq!(
        first.expect_err("no material yet").kind(),
        FileErrorKind::DeadlineExceeded
    );

    assert!(fixture.executor.run_one());

    let second = handle.block_on(
        fixture
            .authority
            .material_for_request(now, now + Duration::from_secs(10)),
    );
    let error = second.expect_err("a denied authority must not hand out material");
    assert_eq!(
        error.kind(),
        FileErrorKind::Permission,
        "a confirmed denial must not be reported as a transient failure"
    );
    assert_eq!(
        fixture.source.calls(),
        1,
        "a denial must not be retried as ordinary jitter"
    );
}

#[test]
fn catalog_unreachable_is_distinguishable_from_denial() {
    // CAD-1 D12 / acceptance 18 is an end-to-end property, but the
    // classification it rests on is decided here.
    let denied = AcquisitionFailure::Denied("revoked".into());
    let unreachable = AcquisitionFailure::CatalogUnreachable("connect timed out".into());
    let id = identity("https://catalog/credentials");

    let denied_error = denied.into_file_error(&id);
    let unreachable_error = unreachable.into_file_error(&id);

    assert_eq!(denied_error.kind(), FileErrorKind::Permission);
    assert_eq!(unreachable_error.kind(), FileErrorKind::Transient);
    assert!(
        unreachable_error
            .to_string()
            .contains("could not reach its catalog"),
        "operators have only the error text to work from: {unreachable_error}"
    );
}

// ---------------------------------------------------------------------------
// CAD-1 D6 / acceptance 5: the shared request belongs to the authority
// ---------------------------------------------------------------------------

#[test]
fn one_waiter_giving_up_does_not_end_the_shared_request() {
    let now = Instant::now();
    let renewed = renewed_material(now + Duration::from_secs(3600));
    let fixture = fixture_with(
        identity("https://catalog/credentials"),
        vec![Ok(renewed)],
        policy(),
    );
    let handle = runtime();

    // Waiter one has no budget left and gives up immediately. It must not take
    // the in-flight request down with it.
    let abandoned = handle.block_on(
        fixture
            .authority
            .material_for_request(now, now.checked_sub(Duration::from_secs(1)).unwrap_or(now)),
    );
    assert_eq!(
        abandoned.expect_err("waiter one gave up").kind(),
        FileErrorKind::DeadlineExceeded
    );
    assert_eq!(
        fixture.executor.pending(),
        1,
        "the request must still be queued after the first waiter gave up"
    );

    // The request completes, and waiter two still gets its result.
    assert!(fixture.executor.run_one());
    let second = handle.block_on(
        fixture
            .authority
            .material_for_request(now, now + Duration::from_secs(10)),
    );
    assert_eq!(
        second.expect("waiter two").access_key_id().expose_secret(),
        "renewed-ak"
    );
    assert_eq!(
        fixture.source.calls(),
        1,
        "the two waiters must share a single acquisition"
    );
}

/// Panics on its first acquisition and succeeds on every later one.
struct PanicsOnceSource {
    calls: AtomicUsize,
    then: AuthorityMaterial,
}

impl AuthorityMaterialSource for PanicsOnceSource {
    fn acquire(&self, _deadline: Instant) -> Result<AuthorityMaterial, AcquisitionFailure> {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            panic!("injected material source panic");
        }
        Ok(self.then.clone())
    }
}

/// Accepts every job and drops it without running it, like an executor
/// whose runtime is shutting down.
#[derive(Default)]
struct DroppingExecutor {
    dropped: AtomicUsize,
}

impl RefreshExecutor for DroppingExecutor {
    fn execute(&self, job: Box<dyn FnOnce() + Send + 'static>) {
        self.dropped.fetch_add(1, Ordering::SeqCst);
        drop(job);
    }
}

#[test]
fn a_panicking_source_settles_its_refresh_and_a_later_one_can_start() {
    let now = Instant::now();
    let executor = Arc::new(QueuedExecutor::default());
    let source = Arc::new(PanicsOnceSource {
        calls: AtomicUsize::new(0),
        then: renewed_material(now + Duration::from_secs(3600)),
    });
    let authority = StorageAuthority::new(
        identity("https://catalog/credentials"),
        Arc::clone(&source) as Arc<dyn AuthorityMaterialSource>,
        Arc::clone(&executor) as Arc<dyn RefreshExecutor>,
        policy(),
    );
    let handle = runtime();
    let already_elapsed = now.checked_sub(Duration::from_secs(1)).unwrap_or(now);

    // The first request queues a refresh and gives up at once.
    assert!(
        handle
            .block_on(authority.material_for_request(now, already_elapsed))
            .is_err()
    );
    assert!(executor.run_one(), "the panicking refresh runs");
    assert!(
        authority.shared.lock_state().inflight.is_none(),
        "a panicked refresh does not stay in flight"
    );
    let error = handle
        .block_on(authority.material_for_request(now, now + Duration::from_secs(10)))
        .expect_err("the panic is reported, inside its backoff");
    assert_eq!(error.kind(), FileErrorKind::Transient);
    assert!(error.to_string().contains("panicked"), "{error}");

    // Past the backoff a new refresh starts and succeeds.
    let later = now + Duration::from_secs(1);
    assert!(
        handle
            .block_on(authority.material_for_request(later, already_elapsed))
            .is_err()
    );
    assert!(executor.run_one(), "a second refresh was started");
    let material = handle
        .block_on(authority.material_for_request(later, later + Duration::from_secs(10)))
        .expect("renewed material");
    assert_eq!(material.access_key_id().expose_secret(), "renewed-ak");
    assert_eq!(source.calls.load(Ordering::SeqCst), 2);
}

#[test]
fn a_refresh_its_executor_drops_unrun_settles_as_a_failure() {
    let now = Instant::now();
    let executor = Arc::new(DroppingExecutor::default());
    let source = Arc::new(ScriptedSource::new(vec![Ok(renewed_material(
        now + Duration::from_secs(3600),
    ))]));
    let authority = StorageAuthority::new(
        identity("https://catalog/credentials"),
        Arc::clone(&source) as Arc<dyn AuthorityMaterialSource>,
        Arc::clone(&executor) as Arc<dyn RefreshExecutor>,
        policy(),
    );
    let handle = runtime();

    // The waiter is told at once rather than holding its whole deadline.
    let error = handle
        .block_on(async {
            tokio::time::timeout(
                Duration::from_secs(1),
                authority.material_for_request(now, now + Duration::from_secs(10)),
            )
            .await
        })
        .expect("an unrun refresh still reaches its waiter")
        .expect_err("no material was acquired");
    assert_eq!(error.kind(), FileErrorKind::Transient);
    assert!(authority.shared.lock_state().inflight.is_none());

    // Past the backoff the authority starts a fresh refresh instead of
    // waiting on the one that never ran.
    let later = now + Duration::from_secs(1);
    let _ = handle.block_on(authority.material_for_request(later, later + Duration::from_secs(10)));
    assert_eq!(executor.dropped.load(Ordering::SeqCst), 2);
    assert_eq!(source.calls(), 0);
}

// ---------------------------------------------------------------------------
// CAD-1 D8 / acceptance 12: a late result cannot reopen a closed authority
// ---------------------------------------------------------------------------

#[test]
fn a_late_success_does_not_restore_a_revoked_authority() {
    let now = Instant::now();
    let fixture = fixture_with(
        identity("https://catalog/credentials"),
        vec![Ok(renewed_material(now + Duration::from_secs(3600)))],
        policy(),
    );
    let handle = runtime();

    // Start a refresh, then observe a revocation while it is still in flight.
    let started = handle.block_on(
        fixture
            .authority
            .material_for_request(now, now.checked_sub(Duration::from_secs(1)).unwrap_or(now)),
    );
    assert_eq!(
        started.expect_err("nothing usable yet").kind(),
        FileErrorKind::DeadlineExceeded
    );
    fixture.authority.close(AcquisitionFailure::Denied(
        "revoked while refreshing".into(),
    ));

    // The earlier refresh now succeeds. It must be discarded.
    assert!(fixture.executor.run_one());

    let after = handle.block_on(
        fixture
            .authority
            .material_for_request(now, now + Duration::from_secs(10)),
    );
    assert_eq!(
        after.expect_err("a closed authority stays closed").kind(),
        FileErrorKind::Permission
    );
    assert_eq!(
        fixture.authority.metrics().refreshes_applied,
        0,
        "a result from a replaced generation must never be applied"
    );
}

#[test]
fn a_closed_authority_does_not_return_a_held_success_to_waiter() {
    let now = Instant::now();
    let fixture = fixture_with(
        identity("https://catalog/credentials"),
        vec![Ok(renewed_material(now + Duration::from_secs(3600)))],
        policy(),
    );
    let handle = runtime();
    handle.block_on(async {
        let authority = Arc::clone(&fixture.authority);
        let waiter = tokio::spawn(async move {
            authority
                .material_for_request(now, now + Duration::from_secs(10))
                .await
        });
        tokio::task::yield_now().await;
        assert_eq!(fixture.executor.pending(), 1, "request must be waiting");

        fixture.authority.close(AcquisitionFailure::Denied(
            "revoked while refreshing".into(),
        ));
        assert!(fixture.executor.run_one(), "release the held success");

        let error = waiter
            .await
            .expect("waiter task")
            .expect_err("a discarded success must not reach its waiter");
        assert_eq!(error.kind(), FileErrorKind::Permission);
        assert_eq!(fixture.authority.metrics().refreshes_applied, 0);
        assert_eq!(fixture.authority.metrics().late_results_discarded, 1);
    });
}

#[cfg(novarocks_dev_controls)]
#[test]
fn debug_close_requires_exact_authority_identity() {
    let fixture = fixture_with(identity("https://catalog/credentials"), vec![], policy());
    let wrong = super::debug_close::control_key(&identity("https://catalog/other-credentials"));
    assert!(!super::debug_close::close_if_key(
        &fixture.authority.shared,
        &wrong
    ));
    assert!(fixture.authority.shared.lock_state().closed.is_none());
    let exact = super::debug_close::control_key(fixture.authority.id());
    assert!(super::debug_close::close_if_key(
        &fixture.authority.shared,
        &exact
    ));
    assert!(!super::debug_close::close_if_key(
        &fixture.authority.shared,
        &exact
    ));
}

#[test]
fn installing_material_into_a_closed_authority_is_ignored() {
    let now = Instant::now();
    let fixture = fixture_with(identity("https://catalog/credentials"), vec![], policy());
    fixture
        .authority
        .close(AcquisitionFailure::Denied("revoked".into()));

    fixture
        .authority
        .install_material(material(now + Duration::from_secs(3600)));

    let obtained = runtime().block_on(
        fixture
            .authority
            .material_for_request(now, now + Duration::from_secs(10)),
    );
    assert_eq!(
        obtained.expect_err("closed").kind(),
        FileErrorKind::Permission
    );
}

// ---------------------------------------------------------------------------
// CAD-1 D7 / acceptance 4: identical material is not an equivalence proof
// ---------------------------------------------------------------------------

#[test]
fn identical_material_with_different_capability_paths_is_not_one_authority() {
    let now = Instant::now();
    let shared = material(now + Duration::from_secs(120));

    let table_a = identity("https://catalog/v1/ns/a/credentials");
    let table_b = identity("https://catalog/v1/ns/b/credentials");
    assert_ne!(
        table_a, table_b,
        "two tables whose refresh endpoints differ are two authorities even when \
         the server happens to vend identical material"
    );

    let fixture_a = fixture_with(
        table_a,
        vec![Ok(renewed_material(now + Duration::from_secs(3600)))],
        policy(),
    );
    let fixture_b = fixture_with(
        table_b,
        vec![Ok(renewed_material(now + Duration::from_secs(3600)))],
        policy(),
    );
    fixture_a.authority.install_material(shared.clone());
    fixture_b.authority.install_material(shared);

    // Read late enough in the shared credential's life that both authorities
    // are inside their prefetch window.
    let inside_window = now + Duration::from_secs(100);
    let handle = runtime();
    let _ = handle.block_on(
        fixture_a
            .authority
            .material_for_request(inside_window, inside_window + Duration::from_secs(10)),
    );
    let _ = handle.block_on(
        fixture_b
            .authority
            .material_for_request(inside_window, inside_window + Duration::from_secs(10)),
    );

    assert_eq!(fixture_a.executor.accepted(), 1);
    assert_eq!(
        fixture_b.executor.accepted(),
        1,
        "the second authority must run its own refresh rather than inherit the first one's"
    );
}

#[test]
fn the_catalog_generation_dimension_lives_in_the_identity() {
    // CAD-1 D7 requires the refresh generation to match before an in-flight
    // result may be shared, and D10 places the generation inside CatalogHandle.
    // Nothing else may compare generations separately.
    let scope = prefix("s3://warehouse/sales/orders/");
    let capability = endpoint_capability("https://catalog/credentials");
    let first = StorageAuthorityId::new(
        CatalogHandle::new(
            ConnectorInstanceId::parse("lake").expect("instance id"),
            CatalogVersion::from_bytes([1; 32]),
        ),
        scope.clone(),
        capability.clone(),
    );
    let second = StorageAuthorityId::new(
        CatalogHandle::new(
            ConnectorInstanceId::parse("lake").expect("instance id"),
            CatalogVersion::from_bytes([2; 32]),
        ),
        scope,
        capability,
    );

    assert_ne!(
        first, second,
        "a catalog generation change must produce a different authority identity"
    );
}

#[test]
fn a_different_refreshing_principal_is_a_different_authority() {
    // CAD-1 D1 gives the coordinator and the executor different principals, so
    // the same table reached under each of them is two authorities.
    let scope = prefix("s3://warehouse/sales/orders/");
    let first = StorageAuthorityId::new(
        catalog("lake"),
        scope.clone(),
        endpoint_capability_as("coordinator-g1", "https://catalog/credentials"),
    );
    let second = StorageAuthorityId::new(
        catalog("lake"),
        scope,
        endpoint_capability_as("executor-g1", "https://catalog/credentials"),
    );

    assert_ne!(first, second);
    assert_ne!(
        first.principal(),
        second.principal(),
        "the refreshing identity is the dimension that separates them"
    );
}

#[test]
fn a_seeded_authority_has_no_refreshing_principal() {
    // CAD-1 M1 and CAD-1 D11 both land here: material arrives from elsewhere
    // and there is no path to authenticate a refresh along.
    let id = StorageAuthorityId::new(
        catalog("lake"),
        prefix("s3://warehouse/sales/orders/"),
        AuthorityCapabilityPath::SeededWithoutRenewal { grant: None },
    );

    assert!(id.principal().is_none());
    assert!(!id.capability().can_renew());
}

// ---------------------------------------------------------------------------
// CAD-1 D11: seeded material with no renewal capability
// ---------------------------------------------------------------------------

#[test]
fn a_seeded_authority_without_renewal_serves_material_then_refuses() {
    let now = Instant::now();
    let id = StorageAuthorityId::new(
        catalog("lake"),
        prefix("s3://warehouse/sales/orders/"),
        AuthorityCapabilityPath::SeededWithoutRenewal { grant: None },
    );
    let fixture = fixture_with(id, vec![], policy());
    fixture
        .authority
        .install_material(material(now + Duration::from_secs(3600)));

    let served = runtime().block_on(
        fixture
            .authority
            .material_for_request(now, now + Duration::from_secs(10)),
    );
    assert!(served.is_ok(), "seeded material is served like any other");
    assert_eq!(
        fixture.executor.accepted(),
        0,
        "an authority without a renewal path must never start an acquisition"
    );

    // Past expiry there is nothing it can do, and it says so precisely.
    let expired = runtime().block_on(fixture.authority.material_for_request(
        now + Duration::from_secs(4000),
        now + Duration::from_secs(4010),
    ));
    let error = expired.expect_err("expired seeded material");
    assert_eq!(error.kind(), FileErrorKind::Permission);
    assert!(error.to_string().contains("holds no renewal capability"));
}

// ---------------------------------------------------------------------------
// CAD-1 D10 / acceptance 16: the registry is process-lived, never per query
// ---------------------------------------------------------------------------

fn registry() -> (StorageAuthorityRegistry, Arc<QueuedExecutor>) {
    let executor = Arc::new(QueuedExecutor::default());
    let registry = StorageAuthorityRegistry::new(
        StorageAuthorityRegistryOptions::default(),
        Arc::clone(&executor) as Arc<dyn RefreshExecutor>,
        policy(),
    )
    .expect("registry options");
    (registry, executor)
}

fn empty_source() -> (Arc<dyn AuthorityMaterialSource>, Option<AuthorityMaterial>) {
    (Arc::new(ScriptedSource::new(vec![])), None)
}

#[test]
fn a_second_query_over_the_same_scope_reuses_the_first_query_s_authority() {
    let now = Instant::now();
    let (registry, _executor) = registry();
    let id = identity("https://catalog/credentials");

    let built = AtomicUsize::new(0);
    let first = registry.authority(&id, now, || {
        built.fetch_add(1, Ordering::Relaxed);
        empty_source()
    });
    first.install_material(material(now + Duration::from_secs(3600)));

    // A different query, same scope. Nothing about the key mentions a query.
    let second = registry.authority(&id, now + Duration::from_secs(1), || {
        built.fetch_add(1, Ordering::Relaxed);
        empty_source()
    });

    assert_eq!(
        built.load(Ordering::Relaxed),
        1,
        "the second query must not build a second authority"
    );
    assert!(
        Arc::ptr_eq(&first, &second),
        "both queries must reach the same authority, material included"
    );
    let metrics = registry.metrics();
    assert_eq!((metrics.hits, metrics.misses, metrics.resident), (1, 1, 1));
}

#[test]
fn a_different_scope_is_a_different_authority() {
    let now = Instant::now();
    let (registry, _executor) = registry();
    let orders = identity("https://catalog/orders/credentials");
    let customers = StorageAuthorityId::new(
        catalog("lake"),
        prefix("s3://warehouse/sales/customers/"),
        endpoint_capability("https://catalog/customers/credentials"),
    );

    let a = registry.authority(&orders, now, empty_source);
    let b = registry.authority(&customers, now, empty_source);

    assert!(!Arc::ptr_eq(&a, &b));
    assert_eq!(registry.metrics().resident, 2);
}

#[test]
fn a_different_route_over_the_same_scope_is_a_different_authority() {
    // IRU-13a L15: the routing context decides which service and catalog an
    // acquisition reaches, so two routes are two capabilities even when the
    // scope, catalog and principal agree.
    let now = Instant::now();
    let (registry, _executor) = registry();
    let url = "https://catalog/v1/p1/namespaces/sales/tables/orders";
    let load_table = |routing: CredentialRoutingContext| {
        StorageAuthorityId::new(
            catalog("lake"),
            prefix("s3://warehouse/sales/orders/"),
            AuthorityCapabilityPath::LoadTableDelegation {
                principal: principal("g1"),
                routing,
                namespace: Arc::from("sales"),
                table: Arc::from("orders"),
                table_uuid: Arc::from("8f1d0c6e-0000-4000-8000-000000000001"),
            },
        )
    };
    let endpoint = |routing: CredentialRoutingContext| {
        StorageAuthorityId::new(
            catalog("lake"),
            prefix("s3://warehouse/sales/orders/"),
            AuthorityCapabilityPath::CredentialsEndpoint {
                principal: principal("g1"),
                routing,
            },
        )
    };
    let base = routing(url, &[("x-project-id", "p1"), ("polaris-realm", "r1")]);
    for (make, label) in [
        (
            &load_table as &dyn Fn(CredentialRoutingContext) -> StorageAuthorityId,
            "load-table",
        ),
        (&endpoint, "credentials-endpoint"),
    ] {
        let id = make(base.clone());
        // The same route assembled in another header order is the same key.
        assert_eq!(
            id,
            make(routing(
                url,
                &[("polaris-realm", "r1"), ("x-project-id", "p1")]
            )),
            "{label}: an equal route must key the same authority"
        );
        for other in [
            routing(
                "https://catalog/v1/p2/namespaces/sales/tables/orders",
                &[("x-project-id", "p1"), ("polaris-realm", "r1")],
            ),
            routing(url, &[("x-project-id", "p2"), ("polaris-realm", "r1")]),
            routing(
                url,
                &[
                    ("x-project-id", "p1"),
                    ("polaris-realm", "r1"),
                    ("x-region", "eu"),
                ],
            ),
            routing(url, &[]),
        ] {
            let other = make(other);
            assert_ne!(id, other, "{label}: a different route must not share a key");
            let first = registry.authority(&id, now, empty_source);
            let second = registry.authority(&other, now, empty_source);
            assert!(
                !Arc::ptr_eq(&first, &second),
                "{label}: a different route must not share an authority"
            );
        }
    }

    // The key renders route header names only, never their values.
    let rendered = format!(
        "{:?}",
        load_table(routing(url, &[("x-project-id", "value-canary")]))
    );
    assert!(rendered.contains("x-project-id"), "{rendered}");
    assert!(!rendered.contains("value-canary"), "{rendered}");
}

#[test]
fn capacity_eviction_drops_the_least_recently_used_authority() {
    let now = Instant::now();
    let executor = Arc::new(QueuedExecutor::default());
    let registry = StorageAuthorityRegistry::new(
        StorageAuthorityRegistryOptions {
            capacity: 2,
            idle_ttl: Duration::from_secs(3600),
        },
        executor as Arc<dyn RefreshExecutor>,
        policy(),
    )
    .expect("registry options");

    let first = identity("https://catalog/a/credentials");
    let second = identity("https://catalog/b/credentials");
    let third = identity("https://catalog/c/credentials");

    let _ = registry.authority(&first, now, empty_source);
    let _ = registry.authority(&second, now + Duration::from_secs(1), empty_source);
    // Touch the first one so the second becomes least recently used.
    let _ = registry.authority(&first, now + Duration::from_secs(2), empty_source);
    let _ = registry.authority(&third, now + Duration::from_secs(3), empty_source);

    assert!(registry.is_resident(&first));
    assert!(registry.is_resident(&third));
    assert!(!registry.is_resident(&second));
    assert_eq!(registry.metrics().capacity_evictions, 1);
}

#[test]
fn an_idle_authority_expires_but_a_reader_still_holding_it_is_unaffected() {
    let now = Instant::now();
    let executor = Arc::new(QueuedExecutor::default());
    let registry = StorageAuthorityRegistry::new(
        StorageAuthorityRegistryOptions {
            capacity: 8,
            idle_ttl: Duration::from_secs(60),
        },
        executor as Arc<dyn RefreshExecutor>,
        policy(),
    )
    .expect("registry options");
    let id = identity("https://catalog/credentials");

    let held = registry.authority(&id, now, empty_source);
    held.install_material(material(now + Duration::from_secs(3600)));

    // Long enough later that the registry lets it go.
    let other = StorageAuthorityId::new(
        catalog("lake"),
        prefix("s3://warehouse/sales/customers/"),
        endpoint_capability("https://catalog/customers/credentials"),
    );
    let later = now + Duration::from_secs(120);
    let _ = registry.authority(&other, later, empty_source);

    assert!(!registry.is_resident(&id));
    assert_eq!(registry.metrics().idle_expirations, 1);

    // Eviction is not revocation: whoever still holds the Arc keeps working.
    let served =
        runtime().block_on(held.material_for_request(later, later + Duration::from_secs(10)));
    assert_eq!(
        served
            .expect("a held authority keeps serving after eviction")
            .access_key_id()
            .expose_secret(),
        "ak"
    );
}

#[test]
fn registry_options_are_validated() {
    let executor = Arc::new(QueuedExecutor::default());
    let rejected = StorageAuthorityRegistry::new(
        StorageAuthorityRegistryOptions {
            capacity: 0,
            idle_ttl: Duration::from_secs(3600),
        },
        Arc::clone(&executor) as Arc<dyn RefreshExecutor>,
        policy(),
    );
    assert!(rejected.is_err());

    let rejected_ttl = StorageAuthorityRegistry::new(
        StorageAuthorityRegistryOptions {
            capacity: 8,
            idle_ttl: Duration::from_secs(1),
        },
        executor as Arc<dyn RefreshExecutor>,
        policy(),
    );
    assert!(rejected_ttl.is_err());
}

#[test]
fn a_short_lived_credential_is_usable_the_instant_it_arrives() {
    // The system scenario vends an 8-second credential on purpose. A fixed
    // 30-second margin judged it unusable on arrival, and because a seeded
    // authority cannot acquire, every write failed with a permission error.
    // The margin has to be a fraction of what is left, never a fixed span.
    let now = Instant::now();
    let fixture = fixture_with(
        StorageAuthorityId::new(
            catalog("lake"),
            prefix("s3://warehouse/sales/orders/"),
            AuthorityCapabilityPath::SeededWithoutRenewal { grant: None },
        ),
        vec![],
        policy(),
    );
    fixture
        .authority
        .install_material(material(now + Duration::from_secs(8)));

    let served = runtime().block_on(
        fixture
            .authority
            .material_for_request(now, now + Duration::from_secs(1)),
    );
    assert_eq!(
        served
            .expect("an 8-second credential must be usable when it arrives")
            .access_key_id()
            .expose_secret(),
        "ak"
    );
}

#[test]
fn the_validity_margin_is_a_clamped_fraction_not_a_fixed_span() {
    let policy = policy();
    // Short lifetimes get the floor, not a span longer than themselves.
    assert_eq!(
        policy.validity_margin_for(Duration::from_secs(8)),
        Duration::from_secs(1)
    );
    // Long lifetimes get the ceiling.
    assert_eq!(
        policy.validity_margin_for(Duration::from_secs(3600)),
        Duration::from_secs(30)
    );
    // In between it is the fraction itself.
    assert_eq!(
        policy.validity_margin_for(Duration::from_secs(200)),
        Duration::from_secs(10)
    );
}

#[cfg(novarocks_dev_controls)]
#[test]
fn session_authority_close_is_exact_once_and_observer_is_joined() {
    use novarocks_failpoint::liveness::{PreparedRoleSession, RunnerSession};
    use novarocks_failpoint::scope::ProcessBinding;
    use novarocks_failpoint::session::{DomainGrant, SessionDeclaration, SessionRole};
    let directory = tempfile::Builder::new()
        .prefix("g2-fs-")
        .tempdir_in("/tmp")
        .unwrap();
    let declaration = SessionDeclaration {
        schema_version: 1,
        session_id: "authority-test".into(),
        launch_id: "authority-launch".into(),
        role: SessionRole::Be,
        lifetime_millis: 10_000,
        liveness_socket: directory.path().join("live.sock"),
        handshake_secret: "01234567890123456789012345678901".into(),
        domain_grants: ["observe", "close"]
            .into_iter()
            .map(|action| DomainGrant {
                domain: "storage-authority".into(),
                grant_id: action.into(),
                parameters: authority_control_parameters_for_test(action, directory.path()),
            })
            .collect(),
    };
    let path = directory.path().join("declaration.json");
    declaration.write_private(&path).unwrap();
    let mut runner = RunnerSession::start(declaration).unwrap();
    let process = "018f0000-0000-7000-8000-000000000001".parse().unwrap();
    let mut role = PreparedRoleSession::read_and_claim(&path, SessionRole::Be)
        .unwrap()
        .bind(ProcessBinding::Backend(process))
        .unwrap();
    let controls = super::StorageAuthorityCloseControls::from_grants(
        role.scope().grants_for("storage-authority"),
    )
    .unwrap();
    let fixture = fixture_with(identity("https://catalog/credentials"), vec![], policy());
    controls.register(&fixture.authority).unwrap();
    let trigger = directory
        .path()
        .join("be-0.storage-authority-close.trigger");
    std::fs::write(
        &trigger,
        super::debug_close::control_key(&identity("https://catalog/other-credentials")),
    )
    .unwrap();
    std::thread::sleep(Duration::from_millis(60));
    assert!(fixture.authority.shared.lock_state().closed.is_none());
    assert!(
        !role
            .scope()
            .observations()
            .iter()
            .any(|event| event.grant_id == "close"),
        "wrong identity consumes no close permission"
    );
    std::fs::write(
        &trigger,
        super::debug_close::control_key(fixture.authority.id()),
    )
    .unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    while fixture.authority.shared.lock_state().closed.is_none() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(fixture.authority.shared.lock_state().closed.is_some());
    runner.end_and_join().unwrap();
    controls.close_and_join().unwrap();
    role.close_and_join().unwrap();
    assert_eq!(controls.worker_count_for_test(), 0);
    assert_eq!(
        role.scope()
            .observations()
            .iter()
            .filter(|event| event.event == "hit")
            .count(),
        2
    );
    assert!(
        !directory
            .path()
            .join("be-0.storage-authority-0.identity")
            .exists(),
        "join completed identity cleanup"
    );
}

#[cfg(novarocks_dev_controls)]
fn authority_control_parameters_for_test(
    action: &str,
    root: &std::path::Path,
) -> serde_json::Value {
    serde_json::json!({
        "action": action,
        "root": root,
        "backend_index": 0,
        "target": {
            "catalog": "lake",
            "scope": "s3://warehouse/sales/orders/",
            "principal_name": "vending",
            "principal_generation": "g1",
            "capability": "credentials-endpoint",
            "endpoint": "https://catalog/credentials"
        }
    })
}

#[cfg(novarocks_dev_controls)]
fn authority_close_session_for_cleanup_test() -> (
    tempfile::TempDir,
    novarocks_failpoint::liveness::RunnerSession,
    novarocks_failpoint::liveness::RoleSessionOwner,
    super::StorageAuthorityCloseControls,
) {
    authority_control_session_for_test(&["observe", "close"])
}

#[cfg(novarocks_dev_controls)]
fn authority_control_session_for_test(
    actions: &[&str],
) -> (
    tempfile::TempDir,
    novarocks_failpoint::liveness::RunnerSession,
    novarocks_failpoint::liveness::RoleSessionOwner,
    super::StorageAuthorityCloseControls,
) {
    use novarocks_failpoint::liveness::{PreparedRoleSession, RunnerSession};
    use novarocks_failpoint::scope::ProcessBinding;
    use novarocks_failpoint::session::{DomainGrant, SessionDeclaration, SessionRole};
    let directory = tempfile::Builder::new()
        .prefix("g2-close-errors-")
        .tempdir_in("/tmp")
        .unwrap();
    let declaration = SessionDeclaration {
        schema_version: 1,
        session_id: "close-cleanup".into(),
        launch_id: "close-cleanup-launch".into(),
        role: SessionRole::Be,
        lifetime_millis: 10_000,
        liveness_socket: directory.path().join("live.sock"),
        handshake_secret: "01234567890123456789012345678901".into(),
        domain_grants: actions
            .iter()
            .map(|action| DomainGrant {
                domain: "storage-authority".into(),
                grant_id: (*action).into(),
                parameters: authority_control_parameters_for_test(action, directory.path()),
            })
            .collect(),
    };
    let path = directory.path().join("declaration.json");
    declaration.write_private(&path).unwrap();
    let runner = RunnerSession::start(declaration).unwrap();
    let process = "018f0000-0000-7000-8000-000000000001".parse().unwrap();
    let role = PreparedRoleSession::read_and_claim(&path, SessionRole::Be)
        .unwrap()
        .bind(ProcessBinding::Backend(process))
        .unwrap();
    let controls = super::StorageAuthorityCloseControls::from_grants(
        role.scope().grants_for("storage-authority"),
    )
    .unwrap();
    (directory, runner, role, controls)
}

#[cfg(novarocks_dev_controls)]
fn wait_for_authority_close_identity(directory: &std::path::Path) -> std::path::PathBuf {
    let path = directory.join("be-0.storage-authority-0.identity");
    let deadline = Instant::now() + Duration::from_secs(2);
    while !path.exists() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(2));
    }
    assert!(
        path.is_file(),
        "actual observer did not register its identity"
    );
    path
}

#[cfg(novarocks_dev_controls)]
#[test]
fn natural_scope_end_with_poisoned_targets_still_cleans_identity_and_repeats_the_error() {
    let (directory, mut runner, mut role, controls) = authority_close_session_for_cleanup_test();
    let fixture = fixture_with(identity("https://catalog/credentials"), vec![], policy());
    controls.register(&fixture.authority).unwrap();
    let identity_path = wait_for_authority_close_identity(directory.path());
    let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        controls.poison_authorities_while_for_test(|| {
            runner.end_and_join().unwrap();
            let deadline = Instant::now() + Duration::from_secs(2);
            while role.scope().is_active() && Instant::now() < deadline {
                std::thread::yield_now();
            }
            assert!(
                !role.scope().is_active(),
                "runner end must reach the original scope"
            );
        });
    }));
    assert!(poisoned.is_err());
    let first = controls
        .close_and_join()
        .expect_err("poison remains a failed attempt");
    assert!(first.contains("natural target cleanup lock poisoned"));
    assert!(
        !identity_path.exists(),
        "poison cannot skip owned identity cleanup"
    );
    assert_eq!(
        controls.worker_count_for_test(),
        0,
        "real observer was joined"
    );
    assert_eq!(
        Arc::strong_count(&fixture.authority.shared),
        1,
        "cleanup released poisoned target retention"
    );
    assert_eq!(
        controls.close_and_join().unwrap_err(),
        first,
        "a repeated join retains the failure"
    );
    assert!(
        !role
            .scope()
            .observations()
            .iter()
            .any(|event| event.grant_id == "close"),
        "cleanup never injected authority closure"
    );
    role.close_and_join().unwrap();
}

#[cfg(novarocks_dev_controls)]
#[test]
fn observer_primary_and_identity_cleanup_failures_are_retained_after_the_filesystem_is_repaired() {
    let (directory, mut runner, mut role, controls) = authority_close_session_for_cleanup_test();
    let fixture = fixture_with(identity("https://catalog/credentials"), vec![], policy());
    controls.register(&fixture.authority).unwrap();
    let identity_path = wait_for_authority_close_identity(directory.path());
    // This test owns the registered file. Replacing it with a directory
    // deliberately makes the observer's real remove_file cleanup fail.
    std::fs::remove_file(&identity_path).unwrap();
    std::fs::create_dir(&identity_path).unwrap();
    std::fs::write(
        directory
            .path()
            .join("be-0.storage-authority-close.trigger"),
        "x".repeat(66),
    )
    .unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    while !controls.workers_returned_for_test() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(2));
    }
    assert!(
        controls.workers_returned_for_test(),
        "actual invalid-transport observer must return before session end"
    );
    runner.end_and_join().unwrap();
    let first = controls.close_and_join().unwrap_err();
    assert!(
        first.contains("bounded digest file"),
        "primary invalid transport error: {first}"
    );
    assert!(
        first.contains("remove authority identity"),
        "cleanup error must survive the primary error: {first}"
    );
    assert_eq!(controls.worker_count_for_test(), 0);
    std::fs::remove_dir(&identity_path).unwrap();
    assert_eq!(
        controls.close_and_join().unwrap_err(),
        first,
        "repair does not erase raw failure"
    );
    assert!(fixture.authority.shared.lock_state().closed.is_none());
    role.close_and_join().unwrap();
}

#[cfg(novarocks_dev_controls)]
#[test]
fn poisoned_join_bookkeeping_still_joins_the_real_observer_and_preserves_history() {
    let (directory, mut runner, mut role, controls) = authority_close_session_for_cleanup_test();
    let fixture = fixture_with(identity("https://catalog/credentials"), vec![], policy());
    controls.register(&fixture.authority).unwrap();
    let identity_path = wait_for_authority_close_identity(directory.path());
    let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        controls.poison_join_history_for_test()
    }));
    assert!(poisoned.is_err());
    let first = controls.close_and_join().unwrap_err();
    assert!(first.contains("preexisting authority close failure"));
    assert!(first.contains("worker lock poisoned"));
    assert!(first.contains("failure history lock poisoned"));
    assert!(!identity_path.exists());
    assert_eq!(controls.worker_count_for_test(), 0);
    assert_eq!(Arc::strong_count(&fixture.authority.shared), 1);
    assert_eq!(controls.close_and_join().unwrap_err(), first);
    runner.end_and_join().unwrap();
    role.close_and_join().unwrap();
}

#[cfg(novarocks_dev_controls)]
#[test]
fn authority_identity_observation_cannot_close_and_ignores_unrelated_targets() {
    let (directory, mut runner, mut role, controls) =
        authority_control_session_for_test(&["observe"]);
    let unrelated = fixture_with(identity("https://catalog/other"), vec![], policy());
    controls.register(&unrelated.authority).unwrap();
    assert_eq!(Arc::strong_count(&unrelated.authority.shared), 1);
    let fixture = fixture_with(identity("https://catalog/credentials"), vec![], policy());
    controls.register(&fixture.authority).unwrap();
    let identity_path = wait_for_authority_close_identity(directory.path());
    let record = std::fs::read_to_string(&identity_path).unwrap();
    assert!(record.contains(&super::debug_close::control_key(fixture.authority.id())));
    assert!(!record.contains("https://catalog/credentials"));
    let trigger = directory
        .path()
        .join("be-0.storage-authority-close.trigger");
    std::fs::write(
        &trigger,
        super::debug_close::control_key(fixture.authority.id()),
    )
    .unwrap();
    std::thread::sleep(Duration::from_millis(60));
    assert!(fixture.authority.shared.lock_state().closed.is_none());
    // Even malformed close transport is outside the observation-only owner.
    std::fs::write(&trigger, "x".repeat(66)).unwrap();
    std::thread::sleep(Duration::from_millis(60));
    runner.end_and_join().unwrap();
    controls.close_and_join().unwrap();
    assert!(fixture.authority.shared.lock_state().closed.is_none());
    assert!(unrelated.authority.shared.lock_state().closed.is_none());
    let observations = role.scope().observations();
    assert!(observations.iter().all(|event| event.grant_id == "observe"));
    assert_eq!(
        observations
            .iter()
            .filter(|event| event.event == "hit")
            .count(),
        1
    );
    assert!(!identity_path.exists());
    assert_eq!(controls.worker_count_for_test(), 0);
    assert_eq!(Arc::strong_count(&fixture.authority.shared), 1);
    controls.register(&fixture.authority).unwrap();
    assert_eq!(Arc::strong_count(&fixture.authority.shared), 1);
    controls.close_and_join().unwrap();
    role.close_and_join().unwrap();
}

#[cfg(novarocks_dev_controls)]
#[test]
fn authority_close_permission_does_not_grant_identity_observation() {
    let (directory, mut runner, mut role, controls) =
        authority_control_session_for_test(&["close"]);
    let fixture = fixture_with(identity("https://catalog/credentials"), vec![], policy());
    controls.register(&fixture.authority).unwrap();
    let trigger = directory
        .path()
        .join("be-0.storage-authority-close.trigger");
    std::fs::write(
        &trigger,
        super::debug_close::control_key(fixture.authority.id()),
    )
    .unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    while fixture.authority.shared.lock_state().closed.is_none() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(2));
    }
    assert!(fixture.authority.shared.lock_state().closed.is_some());
    runner.end_and_join().unwrap();
    controls.close_and_join().unwrap();
    assert!(
        !directory
            .path()
            .join("be-0.storage-authority-0.identity")
            .exists(),
        "close cannot disclose identity without an independent observation grant"
    );
    let observations = role.scope().observations();
    assert!(observations.iter().all(|event| event.grant_id == "close"));
    assert_eq!(
        observations
            .iter()
            .filter(|event| event.event == "hit")
            .count(),
        1
    );
    assert_eq!(controls.worker_count_for_test(), 0);
    assert_eq!(Arc::strong_count(&fixture.authority.shared), 1);
    role.close_and_join().unwrap();
}

#[cfg(novarocks_dev_controls)]
#[test]
fn authority_target_intent_pins_full_generation_before_any_action_claim() {
    let (directory, mut runner, mut role, controls) =
        authority_control_session_for_test(&["close"]);
    let first = fixture_with(identity("https://catalog/credentials"), vec![], policy());
    let replacement_id = StorageAuthorityId::new(
        CatalogHandle::new(
            ConnectorInstanceId::parse("lake").unwrap(),
            CatalogVersion::from_bytes([8; 32]),
        ),
        prefix("s3://warehouse/sales/orders/"),
        endpoint_capability("https://catalog/credentials"),
    );
    let replacement = fixture_with(replacement_id, vec![], policy());
    controls.register(&first.authority).unwrap();
    controls.register(&first.authority).unwrap();
    assert!(
        controls
            .register(&replacement.authority)
            .unwrap_err()
            .contains("already pinned")
    );
    assert_eq!(Arc::strong_count(&replacement.authority.shared), 1);
    assert!(role.scope().observations().is_empty());
    let trigger = directory
        .path()
        .join("be-0.storage-authority-close.trigger");
    std::fs::write(
        &trigger,
        super::debug_close::control_key(replacement.authority.id()),
    )
    .unwrap();
    std::thread::sleep(Duration::from_millis(60));
    assert!(role.scope().observations().is_empty());
    assert!(first.authority.shared.lock_state().closed.is_none());
    assert!(replacement.authority.shared.lock_state().closed.is_none());
    std::fs::write(
        &trigger,
        super::debug_close::control_key(first.authority.id()),
    )
    .unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    while first.authority.shared.lock_state().closed.is_none() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(2));
    }
    assert!(first.authority.shared.lock_state().closed.is_some());
    assert!(replacement.authority.shared.lock_state().closed.is_none());
    runner.end_and_join().unwrap();
    controls.close_and_join().unwrap();
    role.close_and_join().unwrap();
}

#[cfg(novarocks_dev_controls)]
#[test]
fn authority_controls_reject_missing_or_conflicting_predeclared_intent_before_activation() {
    use novarocks_failpoint::liveness::{PreparedRoleSession, RunnerSession};
    use novarocks_failpoint::scope::ProcessBinding;
    use novarocks_failpoint::session::{DomainGrant, SessionDeclaration, SessionRole};
    for case in ["missing", "capability", "endpoint", "conflict"] {
        let directory = tempfile::Builder::new()
            .prefix("g2-authority-invalid-")
            .tempdir_in("/tmp")
            .unwrap();
        let mut parameters = authority_control_parameters_for_test("observe", directory.path());
        let mut grants = Vec::new();
        let expected = match case {
            "missing" => {
                parameters.as_object_mut().unwrap().remove("target");
                "requires action, root, backend_index and target"
            }
            "capability" => {
                parameters["target"]["capability"] = "arbitrary-provider".into();
                "capability is unsupported"
            }
            "endpoint" => {
                parameters["target"]["endpoint"] = "https://secret@catalog/credentials".into();
                "endpoint is invalid"
            }
            "conflict" => {
                let mut close = authority_control_parameters_for_test("close", directory.path());
                close["target"]["endpoint"] = "https://catalog/other".into();
                grants.push(DomainGrant {
                    domain: "storage-authority".into(),
                    grant_id: "close".into(),
                    parameters: close,
                });
                "transport targets conflict"
            }
            _ => unreachable!(),
        };
        grants.push(DomainGrant {
            domain: "storage-authority".into(),
            grant_id: "observe".into(),
            parameters,
        });
        let declaration = SessionDeclaration {
            schema_version: 1,
            session_id: "authority-invalid".into(),
            launch_id: "authority-invalid-launch".into(),
            role: SessionRole::Be,
            lifetime_millis: 10_000,
            liveness_socket: directory.path().join("live.sock"),
            handshake_secret: "01234567890123456789012345678901".into(),
            domain_grants: grants,
        };
        let path = directory.path().join("declaration.json");
        declaration.write_private(&path).unwrap();
        let mut runner = RunnerSession::start(declaration).unwrap();
        let process = "018f0000-0000-7000-8000-000000000001".parse().unwrap();
        let mut role = PreparedRoleSession::read_and_claim(&path, SessionRole::Be)
            .unwrap()
            .bind(ProcessBinding::Backend(process))
            .unwrap();
        let error = super::StorageAuthorityCloseControls::from_grants(
            role.scope().grants_for("storage-authority"),
        )
        .unwrap_err();
        assert!(error.contains(expected), "{case}: {error}");
        assert!(!error.contains("secret"));
        assert!(role.scope().observations().is_empty());
        assert!(
            !directory
                .path()
                .join("be-0.storage-authority-0.identity")
                .exists()
        );
        runner.end_and_join().unwrap();
        role.close_and_join().unwrap();
    }
}

#[test]
fn acquisition_reasons_survive_borrowed_and_owned_file_conversion() {
    use novarocks_failure_contract::{
        FailureClass, FailureEvidence, FailureProjection, SharedFailureCause,
    };
    use novarocks_spi::connector::ConnectorError;
    use std::error::Error;

    fn originating_cause(error: &ConnectorError) -> &SharedFailureCause {
        error
            .source()
            .unwrap()
            .downcast_ref::<SharedFailureCause>()
            .unwrap()
            .downcast_ref::<SharedFailureCause>()
            .unwrap()
    }

    let id = identity("http://catalog.invalid/v1/credentials");
    for (failure, kind, code) in [
        (
            AcquisitionFailure::Transient("private-provider-detail".into()),
            FileErrorKind::Transient,
            1,
        ),
        (
            AcquisitionFailure::Denied("private-provider-detail".into()),
            FileErrorKind::Permission,
            2,
        ),
        (
            AcquisitionFailure::CatalogUnreachable("private-provider-detail".into()),
            FileErrorKind::Transient,
            3,
        ),
        (
            AcquisitionFailure::NoRenewalCapability,
            FileErrorKind::Permission,
            4,
        ),
    ] {
        let error = failure.into_file_error(&id);
        assert_eq!(error.kind(), kind);
        let origin = error.authority_cause().unwrap().clone();
        let borrowed = ConnectorError::from(&error);
        let owned = ConnectorError::from(error);
        for mapped in [&borrowed, &owned] {
            let fact = mapped.failure_fact();
            assert_eq!(fact.class(), FailureClass::Dependency);
            assert_eq!(fact.evidence(), FailureEvidence::Originating);
            assert_eq!((fact.reason().domain(), fact.reason().code()), (23, code));
            assert_eq!(fact.resource(), None);
            assert_eq!(fact.representation(), None);
            assert_eq!(originating_cause(mapped), &origin);
        }
    }
}

#[test]
fn ordinary_file_failures_do_not_acquire_an_authority_reason() {
    use novarocks_failure_contract::FailureProjection;
    use novarocks_spi::connector::ConnectorError;

    for (kind, code) in [
        (FileErrorKind::Permission, 3),
        (FileErrorKind::Transient, 8),
        (FileErrorKind::DeadlineExceeded, 6),
        (FileErrorKind::Cancelled, 5),
    ] {
        let error = FileError::new(kind, "ordinary file operation");
        assert!(error.authority_cause().is_none());
        let fact = ConnectorError::from(error).failure_fact();
        assert_eq!((fact.reason().domain(), fact.reason().code()), (4, code));
    }
}

#[test]
fn registry_hit_preserves_material_acquired_after_original_seed() {
    let now = Instant::now();
    let (registry, executor) = registry();
    let id = identity("https://catalog/credentials");
    let source = Arc::new(ScriptedSource::new(vec![Ok(renewed_material(
        now + Duration::from_secs(3600),
    ))]));
    let authority = registry.authority(&id, now, || {
        (
            Arc::clone(&source) as Arc<dyn AuthorityMaterialSource>,
            Some(material(now + Duration::from_secs(60))),
        )
    });
    let handle = runtime();
    let inside_window = now + Duration::from_secs(50);
    let original = handle
        .block_on(
            authority.material_for_request(inside_window, inside_window + Duration::from_secs(10)),
        )
        .expect("the seed was installed before registry publication");
    assert_eq!(original.access_key_id().expose_secret(), "ak");
    assert_eq!(executor.pending(), 1);
    assert!(
        executor.run_one(),
        "run the original authority's actual renewal"
    );
    assert_eq!(source.calls(), 1);
    let resident = registry.authority(&id, inside_window, || {
        panic!("a resident authority must not recreate its source or reinstall an old seed")
    });
    assert!(Arc::ptr_eq(&authority, &resident));
    let renewed = handle
        .block_on(
            resident.material_for_request(inside_window, inside_window + Duration::from_secs(10)),
        )
        .unwrap();
    assert_eq!(renewed.access_key_id().expose_secret(), "renewed-ak");
    assert_eq!(executor.pending(), 0);
    assert_eq!(resident.metrics().refreshes_applied, 1);
}

#[test]
fn seed_grant_copies_reuse_but_new_authorization_does_not_reuse_expired_authority() {
    let now = Instant::now();
    let (registry, executor) = registry();
    let original_grant = novarocks_spi::connector::VendedS3SeedGrantId::new();
    let key = |grant| {
        StorageAuthorityId::new(
            catalog("lake"),
            prefix("s3://warehouse/sales/orders/"),
            AuthorityCapabilityPath::SeededWithoutRenewal { grant: Some(grant) },
        )
    };
    let original = registry.authority(&key(original_grant), now, || {
        let (source, _) = empty_source();
        (source, Some(material(now + Duration::from_secs(60))))
    });
    let copied_grant = original_grant;
    let later = now + Duration::from_secs(90);
    let same = registry.authority(&key(copied_grant), later, || {
        panic!("copying the same original seed identity cannot install another seed")
    });
    assert!(Arc::ptr_eq(&same, &original));
    let expired = runtime()
        .block_on(same.material_for_request(later, later + Duration::from_secs(1)))
        .expect_err("the original unrenewable grant is expired");
    assert_eq!(expired.kind(), FileErrorKind::Permission);
    let new_grant = novarocks_spi::connector::VendedS3SeedGrantId::new();
    assert_ne!(new_grant, original_grant);
    let next = registry.authority(&key(new_grant), later, || {
        let (source, _) = empty_source();
        (
            source,
            Some(renewed_material(later + Duration::from_secs(3600))),
        )
    });
    assert!(!Arc::ptr_eq(&original, &next));
    let usable = runtime()
        .block_on(next.material_for_request(later, later + Duration::from_secs(1)))
        .expect("the new grant has its own initial material");
    assert_eq!(usable.access_key_id().expose_secret(), "renewed-ak");
    assert_eq!(
        executor.accepted(),
        0,
        "neither seed-only authority may acquire"
    );
}

#[test]
fn original_acquisition_owner_path_and_full_scope_are_all_registry_key_facts() {
    use novarocks_spi::connector::{
        CredentialAcquisitionOwnerId, CredentialRenewalPath, VendedS3InProcessAcquisitionIdentity,
    };
    let owner = CredentialAcquisitionOwnerId::new();
    let other_owner = CredentialAcquisitionOwnerId::new();
    let a = prefix("s3://warehouse/a/");
    let b = prefix("s3://warehouse/b/");
    let original_path = CredentialRenewalPath::CredentialsEndpoint(routing(
        "https://catalog/credentials",
        &[("x-tenant", "a")],
    ));
    let other_path = CredentialRenewalPath::CredentialsEndpoint(routing(
        "https://catalog/credentials",
        &[("x-tenant", "b")],
    ));
    let key = |owner, path, prefixes| {
        StorageAuthorityId::new(
            catalog("lake"),
            a.clone(),
            AuthorityCapabilityPath::InProcessProvider(
                VendedS3InProcessAcquisitionIdentity::try_new(owner, Some(path), prefixes).unwrap(),
            ),
        )
    };
    let original_key = key(owner, original_path.clone(), vec![a.clone(), b.clone()]);
    let (registry, _) = registry();
    let now = Instant::now();
    let original = registry.authority(&original_key, now, empty_source);
    let same = registry.authority(
        &key(owner, original_path.clone(), vec![b.clone(), a.clone()]),
        now,
        || panic!("the same owner/path/scope reuses its original resident source"),
    );
    assert!(Arc::ptr_eq(&original, &same));
    for distinct in [
        key(
            other_owner,
            original_path.clone(),
            vec![a.clone(), b.clone()],
        ),
        key(owner, other_path, vec![a.clone(), b]),
        key(owner, original_path, vec![a.clone()]),
    ] {
        let independent = registry.authority(&distinct, now, empty_source);
        assert!(!Arc::ptr_eq(&original, &independent));
        #[cfg(novarocks_dev_controls)]
        assert_ne!(
            super::debug_close::control_key(&original_key),
            super::debug_close::control_key(&distinct)
        );
    }
    assert_eq!(registry.metrics().resident, 4);
}
