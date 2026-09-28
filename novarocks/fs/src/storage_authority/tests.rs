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

fn endpoint_capability(url: &str) -> AuthorityCapabilityPath {
    AuthorityCapabilityPath::CredentialsEndpoint {
        principal: principal("g1"),
        endpoint: Arc::from(url),
    }
}

fn endpoint_capability_as(generation: &str, url: &str) -> AuthorityCapabilityPath {
    AuthorityCapabilityPath::CredentialsEndpoint {
        principal: principal(generation),
        endpoint: Arc::from(url),
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
        AuthorityCapabilityPath::InProcessProvider,
    );
    let executor = StorageAuthorityId::new(
        owner,
        scope,
        AuthorityCapabilityPath::CredentialsEndpoint {
            principal: StaticCredentialReference::try_new("executor", "v1").unwrap(),
            endpoint: Arc::from("https://catalog/credentials"),
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
fn debug_close_is_disabled_without_runner_trigger() {
    use std::path::PathBuf;
    let configured = super::debug_close::configured_scope;
    assert!(configured(None, Some(PathBuf::from("/tmp")), Some("0")).is_none());
    assert!(configured(Some("1"), None, Some("0")).is_none());
    assert!(configured(Some("1"), Some(PathBuf::from("/tmp")), None).is_none());
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
        AuthorityCapabilityPath::SeededWithoutRenewal,
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
        AuthorityCapabilityPath::SeededWithoutRenewal,
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

fn empty_source() -> Arc<dyn AuthorityMaterialSource> {
    Arc::new(ScriptedSource::new(vec![])) as Arc<dyn AuthorityMaterialSource>
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
            AuthorityCapabilityPath::SeededWithoutRenewal,
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
