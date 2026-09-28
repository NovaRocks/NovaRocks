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

//! Consumer-side storage authority: the owner of a capability to obtain new
//! credential material, and of the material currently in hand.
//!
//! The three concepts CAD-1 separates are kept apart here deliberately:
//!
//! * The **consumer's authorization** is not owned by this module. A storage
//!   request is always bound by it, but nothing here decides it.
//! * The **capability to obtain new material** is [`StorageAuthorityId`] plus
//!   the injected [`AuthorityMaterialSource`]. It is query-independent and
//!   outlives any single query attempt.
//! * The **current material** is [`AuthorityMaterial`]. It lives until its
//!   `not_after`, and the storage client's identity never contains it.
//!
//! This module is connector-neutral on purpose: both the Iceberg and Paimon
//! providers reach object storage through the same `FsAccessHandle` and
//! operator pool, so the authority that authorizes those requests cannot live
//! in a connector crate, in the frontend application, or in the worker.

use std::fmt::{Debug, Formatter};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use novarocks_secret::SecretValue;
use novarocks_spi::connector::{
    CatalogHandle, StaticCredentialReference, StorageCredentialScopePrefix,
};
use tokio::sync::watch;

use crate::{FileError, FileErrorKind, FileResult};

#[cfg(debug_assertions)]
mod debug_close;
mod executor;
mod registry;

#[cfg(test)]
mod tests;

pub use executor::TokioRefreshExecutor;
pub use registry::{
    DEFAULT_STORAGE_AUTHORITY_CAPACITY, DEFAULT_STORAGE_AUTHORITY_IDLE_TTL,
    StorageAuthorityRegistry, StorageAuthorityRegistryMetrics, StorageAuthorityRegistryOptions,
};

/// The renewal path one authority is bound to.
///
/// Selection happens once, when the authority is admitted, and a failure on
/// the selected path never causes another to be probed. The variants are the
/// closed set CAD-1 D2 admits.
///
/// The refreshing identity lives inside the renewing variants rather than
/// beside them: it carries no meaning without a path to refresh along, and a
/// type that demanded one could not express a seeded authority at all.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum AuthorityCapabilityPath {
    /// Renew through the credentials endpoint the server advertised. The URL is
    /// never constructed by the client from a canonical spec path.
    CredentialsEndpoint {
        principal: StaticCredentialReference,
        endpoint: Arc<str>,
    },
    /// Renew through load-table delegation. The table identity is part of the
    /// capability: this path has to know which table to load, which is why it
    /// also belongs in the process-lived cache key (CAD-1 D10).
    LoadTableDelegation {
        principal: StaticCredentialReference,
        namespace: Arc<str>,
        table: Arc<str>,
        table_uuid: Arc<str>,
    },
    /// The consumer's own process already holds the provider capability, so it
    /// renews through that rather than by authenticating a path itself.
    ///
    /// Only the coordinator reaches this: it planned the query, so the catalog
    /// client that observed the response lives here. An execution node never
    /// has it and always names a principal and a path instead, which is what
    /// keeps the two roles' authorities apart even in a single process
    /// (CAD-1 D0 with D9).
    InProcessProvider,
    /// Material was obtained once and this catalog offers no renewal path at
    /// all. A first-class shape, not a degraded fallback (CAD-1 D11).
    SeededWithoutRenewal,
}

impl AuthorityCapabilityPath {
    /// Whether this path can produce new material at all.
    pub const fn can_renew(&self) -> bool {
        !matches!(self, Self::SeededWithoutRenewal)
    }

    /// The identity a refresh along this path authenticates as, when there is
    /// one. CAD-1 D1 gives the coordinator and the executor different
    /// principals, so this is the dimension that keeps their authorities apart.
    pub const fn principal(&self) -> Option<&StaticCredentialReference> {
        match self {
            Self::CredentialsEndpoint { principal, .. }
            | Self::LoadTableDelegation { principal, .. } => Some(principal),
            Self::InProcessProvider | Self::SeededWithoutRenewal => None,
        }
    }
}

/// The query-independent identity of one storage authority.
///
/// It is both the provider's cache key and the credential dimension of the
/// object-store operator pool key. Nothing in it is derived from a query
/// attempt: an identity that carried `QueryExecutionId` would rebuild every
/// operator on every query and defeat CAD-1 D0.
///
/// The five dimensions CAD-1 D7 requires before an in-flight refresh may be
/// shared map onto this type as follows:
///
/// * refreshing identity -> inside `capability`, where it only exists when
///   there is something to refresh along
/// * target -> `scope`
/// * capability path -> `capability`
/// * catalog generation -> carried inside `catalog`, whose `CatalogHandle` is
///   `(catalog_name, version)`; implementations must not add a second
///   generation comparison anywhere else
/// * authorization constraint -> implied by the three above, and enforced at
///   acquisition time by the source's own scope validation
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct StorageAuthorityId {
    catalog: CatalogHandle,
    scope: StorageCredentialScopePrefix,
    capability: AuthorityCapabilityPath,
}

impl StorageAuthorityId {
    pub fn new(
        catalog: CatalogHandle,
        scope: StorageCredentialScopePrefix,
        capability: AuthorityCapabilityPath,
    ) -> Self {
        Self {
            catalog,
            scope,
            capability,
        }
    }

    pub const fn catalog(&self) -> &CatalogHandle {
        &self.catalog
    }

    pub const fn scope(&self) -> &StorageCredentialScopePrefix {
        &self.scope
    }

    pub const fn principal(&self) -> Option<&StaticCredentialReference> {
        self.capability.principal()
    }

    pub const fn capability(&self) -> &AuthorityCapabilityPath {
        &self.capability
    }
}

/// Immutable secret material with the instant it stops being usable.
///
/// `not_after` is a monotonic deadline already adjusted for the wall-clock
/// expiry the provider reported; validity is judged against it plus a safety
/// margin, never by comparing a bare local wall clock against `not_after`.
#[derive(Clone)]
pub struct AuthorityMaterial {
    access_key_id: SecretValue,
    secret_access_key: SecretValue,
    session_token: Option<SecretValue>,
    not_after: Instant,
    /// When this material came into the process's hands.
    ///
    /// Kept so the authority can size its prefetch against *this* credential's
    /// own lifetime. Without it the only available window is an absolute
    /// constant, and a constant longer than a credential's whole lifetime puts
    /// the authority permanently inside its own prefetch window.
    obtained_at: Instant,
}

impl AuthorityMaterial {
    pub fn new(
        access_key_id: SecretValue,
        secret_access_key: SecretValue,
        session_token: Option<SecretValue>,
        not_after: Instant,
    ) -> Self {
        Self {
            access_key_id,
            secret_access_key,
            session_token,
            not_after,
            obtained_at: Instant::now(),
        }
    }

    /// How long this credential was good for when it arrived.
    fn lifetime(&self) -> Duration {
        self.not_after
            .checked_duration_since(self.obtained_at)
            .unwrap_or_default()
    }

    pub const fn access_key_id(&self) -> &SecretValue {
        &self.access_key_id
    }

    pub const fn secret_access_key(&self) -> &SecretValue {
        &self.secret_access_key
    }

    pub const fn session_token(&self) -> Option<&SecretValue> {
        self.session_token.as_ref()
    }

    pub const fn not_after(&self) -> Instant {
        self.not_after
    }

    /// Usable for a request issued now, reserving a margin for clock skew and
    /// for the time between this check and the request reaching storage.
    ///
    /// The margin is a fraction of what is left, clamped — never a fixed span.
    /// A fixed margin is a naive reading of the requirement: it silently makes
    /// every credential shorter than the margin unusable the instant it is
    /// installed, which is not a conservative failure but a total one. Short
    /// vended lifetimes are real, and the engine already sizes its rotation
    /// margins this way.
    fn is_usable(&self, now: Instant, policy: &RefreshPolicy) -> bool {
        self.not_after
            .checked_duration_since(now)
            .is_some_and(|remaining| remaining > policy.validity_margin_for(remaining))
    }
}

impl Debug for AuthorityMaterial {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AuthorityMaterial")
            .field("access_key_id", &"<redacted>")
            .field("secret_access_key", &"<redacted>")
            .field(
                "session_token",
                &self.session_token.as_ref().map(|_| "<redacted>"),
            )
            .field("not_after", &self.not_after)
            .finish()
    }
}

/// Why one acquisition attempt did not produce material.
///
/// The three states CAD-1 D4 distinguishes are decided by this classification,
/// so it must never be collapsed: a confirmed denial retried as if it were
/// jitter is exactly the failure the design forbids.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AcquisitionFailure {
    /// Network jitter, timeout, or a retryable server answer. May back off and
    /// be attempted again.
    Transient(String),
    /// Confirmed revocation, scope mismatch, or permission denial. Must not be
    /// retried as ordinary jitter.
    Denied(String),
    /// The catalog itself could not be reached from this node. Kept separate
    /// from both of the above because executor-to-catalog reachability is a
    /// deployment requirement CAD-1 D12 introduces, and the error text is the
    /// only signal an operator gets.
    CatalogUnreachable(String),
    /// This authority's capability path cannot produce new material at all.
    NoRenewalCapability,
}

impl AcquisitionFailure {
    const fn is_retryable(&self) -> bool {
        matches!(self, Self::Transient(_) | Self::CatalogUnreachable(_))
    }

    /// Whether observing this failure closes the authority: a confirmed denial
    /// is terminal for the capability, and a late success from a refresh that
    /// started earlier must not resurrect it (CAD-1 D8).
    const fn closes_authority(&self) -> bool {
        matches!(self, Self::Denied(_))
    }

    fn into_file_error(self, id: &StorageAuthorityId) -> FileError {
        let scope = id.scope().as_str();
        match self {
            Self::Transient(detail) => FileError::new(
                FileErrorKind::Transient,
                format!("storage authority refresh for {scope} failed transiently: {detail}"),
            ),
            Self::Denied(detail) => FileError::new(
                FileErrorKind::Permission,
                format!("storage authority for {scope} was denied: {detail}"),
            ),
            Self::CatalogUnreachable(detail) => FileError::new(
                FileErrorKind::Transient,
                format!("storage authority for {scope} could not reach its catalog: {detail}"),
            ),
            Self::NoRenewalCapability => FileError::new(
                FileErrorKind::Permission,
                format!(
                    "storage authority for {scope} holds no renewal capability and its material expired"
                ),
            ),
        }
    }
}

/// One acquisition of new material.
///
/// The implementation owns the whole remote interaction and must bound it
/// inside `deadline`: connection, read, and every internal retry (CAD-1 D5,
/// carried into ADR-0151 from ADR-0149 ruling 4). Returning after `deadline` is a
/// violation, not a slow path.
pub trait AuthorityMaterialSource: Send + Sync {
    fn acquire(&self, deadline: Instant) -> Result<AuthorityMaterial, AcquisitionFailure>;
}

/// Where a refresh runs.
///
/// A refresh must never occupy the thread that asked for material. Filesystem
/// reads are driven synchronously from scan threads, and credentials in a
/// cluster commonly expire together, so a refresh that borrows its caller's
/// thread can park the whole scan pool at once (CAD-1 D3).
///
/// Every implementation must hand the job to another thread. The job is handed
/// over after the authority's state lock is released, and an implementation
/// may drop a job it cannot run: the job settles its refresh either way.
pub trait RefreshExecutor: Send + Sync {
    fn execute(&self, job: Box<dyn FnOnce() + Send + 'static>);
}

/// Prefetch, validity margin, and backoff.
///
/// These are the provider's own knobs. They are deliberately not the operation
/// deadline and not the source's request budget: CAD-1 D5 keeps those separate
/// even when they share a configuration source.
#[derive(Clone, Copy, Debug)]
pub struct RefreshPolicy {
    /// The prefetch window is `lifetime / prefetch_divisor`, clamped into
    /// `[prefetch_window_min, prefetch_window_max]`.
    ///
    /// A fraction rather than an absolute span, for the same reason the
    /// validity margin is one: an absolute window wider than a credential's
    /// whole lifetime leaves the authority permanently inside it, so every
    /// request that finds no refresh in flight starts one. That is not a
    /// conservative early refresh, it is a refresh loop for as long as the
    /// query runs, and it scales catalog traffic with request count rather
    /// than with expiry (CAD-1 D3, acceptance 16).
    pub prefetch_divisor: u32,
    pub prefetch_window_min: Duration,
    pub prefetch_window_max: Duration,
    /// The safety margin is `remaining / validity_margin_divisor`, clamped into
    /// `[validity_margin_min, validity_margin_max]`. Expressed as a fraction on
    /// purpose: a fixed span longer than a credential's whole lifetime would
    /// reject that credential outright.
    pub validity_margin_divisor: u32,
    pub validity_margin_min: Duration,
    pub validity_margin_max: Duration,
    pub min_backoff: Duration,
    pub max_backoff: Duration,
    /// The longest a consumer may spend without usable material before an
    /// acquisition failure becomes the operation's answer.
    ///
    /// One storage request's deadline is deliberately not this bound. The
    /// object store retries a request whose credential load failed, and each
    /// retry restarts the acquisition, so a per-request bound multiplies by the
    /// retry count. It can then outlast the query it was serving: the query
    /// dies of an idle exchange instead, and the operator is told about the
    /// exchange rather than about the credentials -- the exact confusion D12
    /// exists to prevent.
    ///
    /// So the window belongs to the *effort*, not to any one request inside it.
    /// Every blocked caller shares the one the first of them opened, and the
    /// ceiling is chosen to sit well inside the engine's shortest liveness
    /// bound (the exchange's idle timeout), so a read fails with the credential
    /// reason while the query is still alive to carry it (CAD-1 D5 with D12).
    pub blocked_acquisition_budget: Duration,
}

impl RefreshPolicy {
    /// The margin to reserve when a request has `remaining` left to use.
    ///
    /// Always strictly less than `remaining` once `remaining` exceeds the
    /// floor, so freshly installed material is never judged unusable on
    /// arrival.
    pub fn validity_margin_for(&self, remaining: Duration) -> Duration {
        (remaining / self.validity_margin_divisor.max(1))
            .clamp(self.validity_margin_min, self.validity_margin_max)
    }

    /// How early to refresh material that was good for `lifetime` when it
    /// arrived.
    ///
    /// Clamped below by the floor so a very short credential still gets one
    /// prefetch attempt before it expires, and above so a very long one does
    /// not sit in its window for hours.
    pub fn prefetch_window_for(&self, lifetime: Duration) -> Duration {
        (lifetime / self.prefetch_divisor.max(1))
            .clamp(self.prefetch_window_min, self.prefetch_window_max)
    }
}

impl Default for RefreshPolicy {
    fn default() -> Self {
        Self {
            // Mirrors the soft rotation margin the coordinator already uses:
            // lifetime/5, clamped to [5s, 5min].
            prefetch_divisor: 5,
            prefetch_window_min: Duration::from_secs(5),
            prefetch_window_max: Duration::from_secs(300),
            // Mirrors the hard rotation margin the coordinator already uses:
            // remaining/20, clamped to [1s, 30s].
            validity_margin_divisor: 20,
            validity_margin_min: Duration::from_secs(1),
            validity_margin_max: Duration::from_secs(30),
            min_backoff: Duration::from_millis(200),
            max_backoff: Duration::from_secs(5),
            blocked_acquisition_budget: Duration::from_secs(30),
        }
    }
}

type AcquisitionOutcome = Arc<Result<AuthorityMaterial, AcquisitionFailure>>;

/// One in-flight refresh, owned by the authority rather than by any waiter.
///
/// Waiters hold only a receiver. Dropping every receiver does not cancel the
/// job: the acquisition is already bounded by its own deadline, and cancelling
/// it on the first waiter's timeout would leave the next waiter to start a
/// second identical request (CAD-1 D6).
struct InflightRefresh {
    generation: u64,
    receiver: watch::Receiver<Option<AcquisitionOutcome>>,
}

struct AuthorityState {
    generation: u64,
    closed: Option<AcquisitionFailure>,
    material: Option<AuthorityMaterial>,
    inflight: Option<Arc<InflightRefresh>>,
    backoff_until: Option<Instant>,
    backoff: Duration,
    /// When the current acquisition effort must give up.
    ///
    /// Opened by the first caller that found nothing usable and shared by every
    /// caller after it, so a retried storage request pays into one window
    /// rather than opening its own.
    blocked_until: Option<Instant>,
    /// Why the last acquisition failed, so a caller that arrives after the
    /// window closed is told the credential reason rather than a bare timeout.
    last_failure: Option<AcquisitionFailure>,
}

/// Observable counters for one authority. ADR-0151 keeps ADR-0149 ruling 5: the
/// renewal loop must be visible in production, with the consumer-side provider
/// as the subject.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct StorageAuthorityMetrics {
    pub cache_hits: u64,
    pub prefetch_started: u64,
    pub blocking_waits: u64,
    pub refreshes_applied: u64,
    pub refreshes_failed: u64,
    pub late_results_discarded: u64,
}

#[derive(Default)]
struct AuthorityCounters {
    cache_hits: AtomicU64,
    prefetch_started: AtomicU64,
    blocking_waits: AtomicU64,
    refreshes_applied: AtomicU64,
    refreshes_failed: AtomicU64,
    late_results_discarded: AtomicU64,
}

/// Everything a refresh job needs after its caller is gone.
///
/// The job holds this directly rather than reaching back through the public
/// handle, which is what lets a detached prefetch publish its own outcome and
/// lets an abandoned request still converge (CAD-1 D6).
struct AuthorityShared {
    id: StorageAuthorityId,
    source: Arc<dyn AuthorityMaterialSource>,
    policy: RefreshPolicy,
    state: Mutex<AuthorityState>,
    counters: AuthorityCounters,
}

/// The consumer-side owner of one capability and its current material.
pub struct StorageAuthority {
    shared: Arc<AuthorityShared>,
    executor: Arc<dyn RefreshExecutor>,
}

impl Debug for StorageAuthority {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StorageAuthority")
            .field("id", &self.shared.id)
            .finish_non_exhaustive()
    }
}

impl StorageAuthority {
    pub fn new(
        id: StorageAuthorityId,
        source: Arc<dyn AuthorityMaterialSource>,
        executor: Arc<dyn RefreshExecutor>,
        policy: RefreshPolicy,
    ) -> Self {
        let authority = Self {
            shared: Arc::new(AuthorityShared {
                id,
                source,
                policy,
                state: Mutex::new(AuthorityState {
                    generation: 1,
                    closed: None,
                    material: None,
                    inflight: None,
                    backoff_until: None,
                    backoff: policy.min_backoff,
                    blocked_until: None,
                    last_failure: None,
                }),
                counters: AuthorityCounters::default(),
            }),
            executor,
        };
        #[cfg(debug_assertions)]
        debug_close::start_if_runner_enabled(&authority.shared);
        authority
    }

    pub fn id(&self) -> &StorageAuthorityId {
        &self.shared.id
    }

    /// Install material this authority did not acquire itself.
    ///
    /// CAD-1 M1 keeps the existing supply path while the acquisition capability
    /// is not yet in place, and CAD-1 D11 keeps a seeded authority that never
    /// renews as a first-class shape.
    pub fn install_material(&self, material: AuthorityMaterial) {
        let mut state = self.shared.lock_state();
        if state.closed.is_some() {
            return;
        }
        state.material = Some(material);
    }

    /// Record an observed revocation or capability closure.
    ///
    /// After this, a refresh that started earlier and succeeds later must not
    /// put material back into a usable state (CAD-1 D8). This is about what has
    /// already been observed; it promises no cross-node instant revocation.
    pub fn close(&self, reason: AcquisitionFailure) {
        self.shared.close(reason);
    }

    pub fn metrics(&self) -> StorageAuthorityMetrics {
        self.shared.metrics()
    }

    /// Material for one request that is about to be signed.
    ///
    /// This is called at every authentication boundary, so the cache-hit path
    /// must not reach the network: it takes the lock, clones, and returns.
    ///
    /// The three states of CAD-1 D4 are decided here:
    ///
    /// 1. material still usable, inside the prefetch window -> start a refresh
    ///    and return the usable material; a transient failure keeps it and
    ///    backs off, failing nothing
    /// 2. no material satisfying this request's validity requirement -> wait,
    ///    bounded by `deadline`, for one refresh, and fail only this operation
    ///    if none arrives
    /// 3. confirmed revocation, scope mismatch, or permission denial -> reject
    ///    with that meaning, never retried as ordinary jitter
    pub async fn material_for_request(
        &self,
        now: Instant,
        deadline: Instant,
    ) -> FileResult<AuthorityMaterial> {
        let (step, refresh) = self.decide(now, deadline);
        // Handed over only once the state lock is released: a job the executor
        // drops without running settles itself, and settling takes that lock.
        if let Some(job) = refresh {
            self.executor.execute(job);
        }
        match step {
            RequestStep::Done(result) => result,
            RequestStep::Wait { inflight, deadline } => {
                self.shared.await_refresh(inflight, now, deadline).await
            }
        }
    }

    /// Decides one request under the state lock, returning what the request
    /// does next and the refresh job it started, if any.
    fn decide(&self, now: Instant, deadline: Instant) -> (RequestStep, Option<RefreshJob>) {
        let mut state = self.shared.lock_state();

        if let Some(reason) = state.closed.clone() {
            return (
                RequestStep::Done(Err(reason.into_file_error(&self.shared.id))),
                None,
            );
        }

        let usable = state
            .material
            .as_ref()
            .filter(|material| material.is_usable(now, &self.shared.policy))
            .cloned();

        match usable {
            Some(material) => {
                self.shared
                    .counters
                    .cache_hits
                    .fetch_add(1, Ordering::Relaxed);
                let refresh = if self.shared.should_prefetch(&state, &material, now) {
                    self.shared
                        .counters
                        .prefetch_started
                        .fetch_add(1, Ordering::Relaxed);
                    self.start_refresh(&mut state, deadline)
                } else {
                    None
                };
                (RequestStep::Done(Ok(material)), refresh)
            }
            // State two. The caller has nothing usable, so it has to wait -- but
            // for the effort's remaining window, not for its own deadline, so a
            // retried request cannot restart the clock.
            None => {
                // A backoff the last failure set applies here too, not only to
                // prefetch. Without that, a caller holding nothing restarts an
                // acquisition the moment the previous one failed, and the
                // storage layer's own retries turn one failing acquisition into
                // several.
                if state.backoff_until.is_some_and(|until| now < until) {
                    return (
                        RequestStep::Done(Err(self.shared.blocked_failure(&state))),
                        None,
                    );
                }
                // A caller that brought no time of its own still starts the
                // acquisition -- the material it cannot wait for is the material
                // the next request needs -- but it must not be the one that
                // sizes the window, or the effort would open already spent.
                let window = *state.blocked_until.get_or_insert_with(|| {
                    let ceiling = now + self.shared.policy.blocked_acquisition_budget;
                    if deadline > now {
                        deadline.min(ceiling)
                    } else {
                        ceiling
                    }
                });
                if now >= window {
                    // This effort has spent an operation's worth of time and
                    // produced nothing. Report why, and hold off long enough
                    // that the storage layer's remaining retries do not each
                    // pay for the same window again.
                    state.blocked_until = None;
                    state.backoff_until = Some(now + self.shared.policy.max_backoff);
                    return (
                        RequestStep::Done(Err(self.shared.blocked_failure(&state))),
                        None,
                    );
                }
                self.shared
                    .counters
                    .blocking_waits
                    .fetch_add(1, Ordering::Relaxed);
                // The job gets the effort's window; this waiter gets the
                // smaller of that and its own deadline, so one caller giving up
                // never ends the shared request.
                let refresh = self.start_refresh(&mut state, window);
                let step =
                    match state.inflight.clone() {
                        Some(inflight) => RequestStep::Wait {
                            inflight,
                            deadline: deadline.min(window),
                        },
                        None => RequestStep::Done(Err(AcquisitionFailure::NoRenewalCapability
                            .into_file_error(&self.shared.id))),
                    };
                (step, refresh)
            }
        }
    }

    /// Registers a refresh unless one is already in flight, and returns the job
    /// that performs it.
    ///
    /// The caller hands the job to the executor after releasing the state lock,
    /// so the caller's thread is never the thread that talks to the catalog
    /// (CAD-1 D3). The job owns the shared state, so it applies its own outcome
    /// and converges even when every waiter has already given up (CAD-1 D6).
    /// Its completion settles the refresh however the job ends -- returned,
    /// panicked, or dropped by the executor without running -- so a refresh
    /// never stays in flight and never blocks the next one.
    fn start_refresh(&self, state: &mut AuthorityState, deadline: Instant) -> Option<RefreshJob> {
        if state.inflight.is_some() || !self.shared.id.capability.can_renew() {
            return None;
        }

        let generation = state.generation;
        let (sender, receiver) = watch::channel(None);
        state.inflight = Some(Arc::new(InflightRefresh {
            generation,
            receiver,
        }));

        let completion = RefreshCompletion {
            shared: Arc::clone(&self.shared),
            generation,
            sender: Some(sender),
        };
        Some(Box::new(move || {
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                completion.shared.source.acquire(deadline)
            }))
            .unwrap_or_else(|_| {
                Err(AcquisitionFailure::Transient(
                    "storage authority material source panicked".to_string(),
                ))
            });
            completion.settle(outcome);
        }))
    }
}

/// What one request does after its decision.
enum RequestStep {
    Done(FileResult<AuthorityMaterial>),
    Wait {
        inflight: Arc<InflightRefresh>,
        deadline: Instant,
    },
}

type RefreshJob = Box<dyn FnOnce() + Send + 'static>;

/// Settles one started refresh exactly once.
///
/// The authority applies the outcome before publishing it, so a waking waiter
/// never observes an outcome the authority has not accounted for. A job that
/// ends without an outcome -- its executor dropped it unrun, or it unwound --
/// settles as a transient failure, which clears the refresh in flight and
/// backs off like any other failed attempt.
struct RefreshCompletion {
    shared: Arc<AuthorityShared>,
    generation: u64,
    sender: Option<watch::Sender<Option<AcquisitionOutcome>>>,
}

impl RefreshCompletion {
    fn settle(mut self, outcome: Result<AuthorityMaterial, AcquisitionFailure>) {
        self.publish(Arc::new(outcome));
    }

    fn publish(&mut self, outcome: AcquisitionOutcome) {
        let Some(sender) = self.sender.take() else {
            return;
        };
        self.shared.apply_outcome(self.generation, &outcome);
        let _ = sender.send(Some(outcome));
    }
}

impl Drop for RefreshCompletion {
    fn drop(&mut self) {
        self.publish(Arc::new(Err(AcquisitionFailure::Transient(
            "storage authority refresh ended before it produced an outcome".to_string(),
        ))));
    }
}

impl AuthorityShared {
    /// What to tell a caller that holds nothing and may not acquire now.
    ///
    /// The last acquisition's own reason when there is one: an operator needs
    /// "could not reach its catalog" or "was denied", not a bare statement that
    /// no material was available (CAD-1 D12).
    fn blocked_failure(&self, state: &AuthorityState) -> FileError {
        state.last_failure.clone().map_or_else(
            || {
                FileError::new(
                    FileErrorKind::Transient,
                    format!(
                        "storage authority for {} holds no usable material yet",
                        self.id.scope().as_str()
                    ),
                )
            },
            |failure| failure.into_file_error(&self.id),
        )
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, AuthorityState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn close(&self, reason: AcquisitionFailure) {
        let mut state = self.lock_state();
        if state.closed.is_none() {
            state.closed = Some(reason);
        }
        state.generation = state.generation.wrapping_add(1);
        state.material = None;
        state.inflight = None;
    }

    fn metrics(&self) -> StorageAuthorityMetrics {
        StorageAuthorityMetrics {
            cache_hits: self.counters.cache_hits.load(Ordering::Relaxed),
            prefetch_started: self.counters.prefetch_started.load(Ordering::Relaxed),
            blocking_waits: self.counters.blocking_waits.load(Ordering::Relaxed),
            refreshes_applied: self.counters.refreshes_applied.load(Ordering::Relaxed),
            refreshes_failed: self.counters.refreshes_failed.load(Ordering::Relaxed),
            late_results_discarded: self.counters.late_results_discarded.load(Ordering::Relaxed),
        }
    }

    fn should_prefetch(
        &self,
        state: &AuthorityState,
        material: &AuthorityMaterial,
        now: Instant,
    ) -> bool {
        if !self.id.capability.can_renew() || state.inflight.is_some() {
            return false;
        }
        if state.backoff_until.is_some_and(|until| now < until) {
            return false;
        }
        let window = self.policy.prefetch_window_for(material.lifetime());
        material
            .not_after
            .checked_duration_since(now)
            .is_some_and(|remaining| remaining <= window)
    }

    async fn await_refresh(
        &self,
        inflight: Arc<InflightRefresh>,
        now: Instant,
        deadline: Instant,
    ) -> FileResult<AuthorityMaterial> {
        let Some(budget) = deadline.checked_duration_since(now) else {
            return Err(FileError::new(
                FileErrorKind::DeadlineExceeded,
                format!(
                    "storage authority for {} had no time left to obtain material",
                    self.id.scope().as_str()
                ),
            ));
        };

        let mut receiver = inflight.receiver.clone();
        let outcome = match tokio::time::timeout(budget, async {
            loop {
                if let Some(outcome) = receiver.borrow_and_update().clone() {
                    return Some(outcome);
                }
                if receiver.changed().await.is_err() {
                    return receiver.borrow().clone();
                }
            }
        })
        .await
        {
            Ok(Some(outcome)) => outcome,
            Ok(None) => {
                return Err(FileError::new(
                    FileErrorKind::Internal,
                    format!(
                        "storage authority refresh for {} ended without an outcome",
                        self.id.scope().as_str()
                    ),
                ));
            }
            Err(_) => {
                // Only this waiter gave up. The request keeps running, applies
                // its own outcome, and stays available to the next waiter.
                return Err(FileError::new(
                    FileErrorKind::DeadlineExceeded,
                    format!(
                        "storage authority for {} did not obtain material before this operation's deadline",
                        self.id.scope().as_str()
                    ),
                ));
            }
        };

        // The refresh job publishes its raw result even when a later close
        // caused apply_outcome to discard it. A waiter must obey the same
        // generation fence as the authority's stored material.
        let state = self.lock_state();
        if let Some(reason) = state.closed.clone() {
            return Err(reason.into_file_error(&self.id));
        }
        if state.generation != inflight.generation {
            return Err(FileError::new(
                FileErrorKind::Permission,
                format!(
                    "storage authority for {} changed while obtaining material",
                    self.id.scope().as_str()
                ),
            ));
        }
        match outcome.as_ref() {
            Ok(material) => Ok(material.clone()),
            Err(failure) => Err(failure.clone().into_file_error(&self.id)),
        }
    }

    /// Publish a refresh outcome into the generation that started it.
    ///
    /// A result that arrives after its generation was replaced or closed is
    /// discarded. Without this an already-observed revocation could be undone
    /// by an older refresh that happened to succeed afterwards (CAD-1 D8).
    fn apply_outcome(&self, generation: u64, outcome: &AcquisitionOutcome) {
        let mut state = self.lock_state();
        if state.generation != generation || state.closed.is_some() {
            self.counters
                .late_results_discarded
                .fetch_add(1, Ordering::Relaxed);
            if state
                .inflight
                .as_ref()
                .is_some_and(|inflight| inflight.generation == generation)
            {
                state.inflight = None;
            }
            return;
        }

        state.inflight = None;
        match outcome.as_ref() {
            Ok(material) => {
                self.counters
                    .refreshes_applied
                    .fetch_add(1, Ordering::Relaxed);
                state.material = Some(material.clone());
                state.backoff_until = None;
                state.backoff = self.policy.min_backoff;
                state.blocked_until = None;
                state.last_failure = None;
            }
            Err(failure) => {
                self.counters
                    .refreshes_failed
                    .fetch_add(1, Ordering::Relaxed);
                state.last_failure = Some(failure.clone());
                if failure.closes_authority() {
                    state.closed = Some(failure.clone());
                    state.generation = state.generation.wrapping_add(1);
                    state.material = None;
                } else if failure.is_retryable() {
                    // State one of CAD-1 D4: a transient failure keeps whatever
                    // material is still valid and only slows the next attempt.
                    state.backoff_until = Some(Instant::now() + state.backoff);
                    state.backoff = (state.backoff * 2).min(self.policy.max_backoff);
                }
            }
        }
    }
}
