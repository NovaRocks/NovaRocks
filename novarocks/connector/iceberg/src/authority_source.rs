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

//! The acquisition side of a consumer-held storage authority.
//!
//! One adapter, from the Iceberg-side refresh capability to the neutral
//! `AuthorityMaterialSource` the filesystem layer depends on. The interesting
//! part is not the call, it is the classification: CAD-1 D4 distinguishes three
//! states, and D12 adds a fourth thing an operator must be able to tell apart,
//! so an adapter that flattened everything into "it failed" would quietly
//! delete the design.

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use novarocks_fs::{AcquisitionFailure, AuthorityMaterial, AuthorityMaterialSource};
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorVendedS3CredentialLeaseRefresher,
    StorageCredentialScopePrefix, VendedS3CredentialRefreshCallPolicy,
    VendedS3CredentialRefreshDispatch, VendedS3CredentialRefreshDispatchGuard,
};

/// The consumer-side authority needs no attempt fence.
///
/// The coordinator's guard existed to stop a refresh belonging to a terminated
/// attempt from dispatching. A consumer-held authority has no attempt to
/// terminate, and the equivalent protection is its own generation fence: a
/// result that arrives after its generation was replaced or closed is discarded
/// on publication rather than prevented on dispatch (CAD-1 D8).
struct AlwaysDispatch;

impl VendedS3CredentialRefreshDispatchGuard for AlwaysDispatch {
    fn provider_dispatch(&self) -> VendedS3CredentialRefreshDispatch {
        VendedS3CredentialRefreshDispatch::Permitted
    }
}

/// How much of the caller's remaining budget one acquisition may consume.
///
/// CAD-1 D5 keeps three bounds apart even when they share a source: the
/// caller's operation deadline, the request budget for one remote interaction,
/// and concurrency admission. This is the second one, derived from the first
/// rather than equal to it, so an acquisition cannot spend a caller's entire
/// deadline and leave nothing for the read it was supposed to enable.
const ACQUISITION_BUDGET_NUMERATOR: u32 = 2;
const ACQUISITION_BUDGET_DENOMINATOR: u32 = 3;
const MIN_ACQUISITION_BUDGET: Duration = Duration::from_secs(1);
const MAX_ACQUISITION_BUDGET: Duration = Duration::from_secs(30);
const ACQUISITION_MAX_ATTEMPTS: u8 = 3;
const ACQUISITION_RETRY_BACKOFF: Duration = Duration::from_millis(200);

/// Adapts one Iceberg refresh capability into the neutral acquisition contract.
pub struct IcebergAuthorityMaterialSource {
    refresher: Arc<dyn ConnectorVendedS3CredentialLeaseRefresher>,
    scope: StorageCredentialScopePrefix,
}

impl std::fmt::Debug for IcebergAuthorityMaterialSource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("IcebergAuthorityMaterialSource")
            .field("scope", &self.scope.as_str())
            .finish_non_exhaustive()
    }
}

impl IcebergAuthorityMaterialSource {
    pub fn new(
        refresher: Arc<dyn ConnectorVendedS3CredentialLeaseRefresher>,
        scope: StorageCredentialScopePrefix,
    ) -> Self {
        Self { refresher, scope }
    }

    fn budget_for(deadline: Instant) -> Duration {
        deadline
            .checked_duration_since(Instant::now())
            .unwrap_or_default()
            .mul_f64(
                f64::from(ACQUISITION_BUDGET_NUMERATOR) / f64::from(ACQUISITION_BUDGET_DENOMINATOR),
            )
            .clamp(MIN_ACQUISITION_BUDGET, MAX_ACQUISITION_BUDGET)
    }
}

/// Classify one acquisition failure into the states CAD-1 acts on.
///
/// The three D4 states plus D12's reachability are decided here and nowhere
/// else, so this mapping is the whole contract:
///
/// * `PermissionDenied` is a confirmed answer from the authorizing side. It
///   closes the capability; retrying it as jitter is precisely what D4's third
///   state forbids.
/// * `Unavailable` that the provider marked retryable-before-progress is the
///   catalog not answering. That is D12's case: an operator told "denied" here
///   would go read policy documents while the actual fact is that this node
///   cannot reach the catalog at all.
/// * `Unavailable` without that marker, and `DeadlineExceeded`, are ordinary
///   jitter: the request may be repeated under backoff.
/// * `Unsupported` means this capability cannot produce material at all, which
///   is the seeded-without-renewal shape rather than a failure to report.
/// * Everything else is a contract violation by the provider and is reported as
///   denial rather than retried, because repeating a malformed exchange cannot
///   make it well formed.
fn classify(error: &ConnectorError) -> AcquisitionFailure {
    let detail = error.to_string();
    match error.kind() {
        ConnectorErrorKind::PermissionDenied => AcquisitionFailure::Denied(detail),
        ConnectorErrorKind::Unavailable if error.retryable_before_progress() => {
            AcquisitionFailure::CatalogUnreachable(detail)
        }
        ConnectorErrorKind::Unavailable | ConnectorErrorKind::DeadlineExceeded => {
            AcquisitionFailure::Transient(detail)
        }
        ConnectorErrorKind::Unsupported => AcquisitionFailure::NoRenewalCapability,
        _ => AcquisitionFailure::Denied(detail),
    }
}

/// Convert the provider's wall-clock expiry into a monotonic deadline.
///
/// Wall clock is what the server states and monotonic is what usability is
/// judged against, so the conversion happens once, here, rather than being
/// re-derived at each check where a clock adjustment could move it.
fn monotonic_not_after(not_after_unix_ms: u64, now: Instant) -> Instant {
    let wall_now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64;
    now + Duration::from_millis(not_after_unix_ms.saturating_sub(wall_now_ms))
}

impl AuthorityMaterialSource for IcebergAuthorityMaterialSource {
    fn acquire(&self, deadline: Instant) -> Result<AuthorityMaterial, AcquisitionFailure> {
        let policy = VendedS3CredentialRefreshCallPolicy::try_new(
            Self::budget_for(deadline),
            std::num::NonZeroU8::new(ACQUISITION_MAX_ATTEMPTS).expect("non-zero attempts"),
            ACQUISITION_RETRY_BACKOFF,
            Arc::new(AlwaysDispatch),
        )
        .map_err(|error| AcquisitionFailure::Denied(error.to_string()))?;

        let refreshed = self
            .refresher
            .refresh_vended_s3_credentials(policy)
            .map_err(|error| classify(&error))?;

        self.project_material(refreshed)
    }

    fn acquire_async<'a>(
        &'a self,
        deadline: Instant,
        request: novarocks_spi::connector::ConnectorRequestContext,
    ) -> futures::future::BoxFuture<'a, Result<AuthorityMaterial, AcquisitionFailure>> {
        Box::pin(async move {
            let policy = VendedS3CredentialRefreshCallPolicy::try_new(
                Self::budget_for(deadline),
                std::num::NonZeroU8::new(ACQUISITION_MAX_ATTEMPTS).expect("non-zero attempts"),
                ACQUISITION_RETRY_BACKOFF,
                Arc::new(AlwaysDispatch),
            )
            .map_err(|error| AcquisitionFailure::Denied(error.to_string()))?;

            let refreshed = self
                .refresher
                .refresh_vended_s3_credentials_async(policy, request)
                .await
                .map_err(|error| classify(&error))?;
            self.project_material(refreshed)
        })
    }
}

impl IcebergAuthorityMaterialSource {
    fn project_material(
        &self,
        refreshed: novarocks_spi::connector::VendedS3CredentialLeaseRefresh,
    ) -> Result<AuthorityMaterial, AcquisitionFailure> {
        // The refresh may legitimately cover several prefixes. This authority
        // owns exactly one, and taking "some entry" instead of "this scope's
        // entry" would hand a reader material for a neighbouring prefix that
        // happens to sort first.
        let entry = refreshed
            .into_entries()
            .into_iter()
            .find(|entry| entry.prefix() == &self.scope)
            .ok_or_else(|| {
                AcquisitionFailure::Denied(format!(
                    "refresh returned no material covering {}",
                    self.scope.as_str()
                ))
            })?;

        let (_, not_after_unix_ms, access_key_id, secret_access_key, session_token) =
            entry.into_parts();
        Ok(AuthorityMaterial::new(
            access_key_id,
            secret_access_key,
            Some(session_token),
            monotonic_not_after(not_after_unix_ms, Instant::now()),
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    struct ScriptedRefresher(Mutex<Option<ConnectorError>>);

    impl ConnectorVendedS3CredentialLeaseRefresher for ScriptedRefresher {
        fn refresh_vended_s3_credentials(
            &self,
            _policy: VendedS3CredentialRefreshCallPolicy,
        ) -> Result<novarocks_spi::connector::VendedS3CredentialLeaseRefresh, ConnectorError>
        {
            Err(self
                .0
                .lock()
                .unwrap()
                .take()
                .expect("scripted refresher called more than scripted"))
        }
    }

    fn failure_for(error: ConnectorError) -> AcquisitionFailure {
        let source = IcebergAuthorityMaterialSource::new(
            Arc::new(ScriptedRefresher(Mutex::new(Some(error)))),
            StorageCredentialScopePrefix::try_from_normalized("s3://warehouse/orders/").unwrap(),
        );
        source
            .acquire(Instant::now() + Duration::from_secs(10))
            .expect_err("scripted failure")
    }

    #[test]
    fn an_unreachable_catalog_is_not_reported_as_a_denial() {
        // CAD-1 D12 / acceptance 18. Collapsing these sends an operator to read
        // policy documents when the fact is that this node cannot reach the
        // catalog at all.
        let unreachable = failure_for(
            ConnectorError::new(ConnectorErrorKind::Unavailable, "connect timed out")
                .with_retryable_before_progress(),
        );
        assert!(matches!(
            unreachable,
            AcquisitionFailure::CatalogUnreachable(_)
        ));

        let denied = failure_for(ConnectorError::new(
            ConnectorErrorKind::PermissionDenied,
            "token revoked",
        ));
        assert!(matches!(denied, AcquisitionFailure::Denied(_)));
        assert_ne!(
            std::mem::discriminant(&unreachable),
            std::mem::discriminant(&denied)
        );
    }

    #[test]
    fn jitter_stays_retryable_and_a_denial_does_not() {
        let transient = failure_for(ConnectorError::new(
            ConnectorErrorKind::Unavailable,
            "server said try later",
        ));
        assert!(matches!(transient, AcquisitionFailure::Transient(_)));

        let deadline = failure_for(ConnectorError::new(
            ConnectorErrorKind::DeadlineExceeded,
            "budget exhausted",
        ));
        assert!(matches!(deadline, AcquisitionFailure::Transient(_)));
    }

    #[test]
    fn a_malformed_exchange_is_a_denial_rather_than_a_retry() {
        // Repeating a malformed exchange cannot make it well formed, and
        // retrying it would burn the caller's deadline to reach the same place.
        let malformed = failure_for(ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            "response was not an access delegation",
        ));
        assert!(matches!(malformed, AcquisitionFailure::Denied(_)));
    }

    #[test]
    fn a_capability_that_cannot_renew_says_so_rather_than_failing_vaguely() {
        let none = failure_for(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "this catalog advertises no credentials endpoint",
        ));
        assert!(matches!(none, AcquisitionFailure::NoRenewalCapability));
    }

    #[test]
    fn the_acquisition_budget_never_consumes_the_whole_operation_deadline() {
        // CAD-1 D5: the request budget is derived from the operation deadline,
        // not equal to it. An acquisition that spent the caller's whole
        // deadline would leave nothing for the read it exists to enable.
        let deadline = Instant::now() + Duration::from_secs(9);
        let budget = IcebergAuthorityMaterialSource::budget_for(deadline);
        assert!(budget < Duration::from_secs(9));
        assert!(budget >= MIN_ACQUISITION_BUDGET);

        // Bounded at both ends, so neither a huge nor an already-elapsed
        // deadline produces a nonsensical request budget.
        assert_eq!(
            IcebergAuthorityMaterialSource::budget_for(Instant::now() + Duration::from_secs(3600)),
            MAX_ACQUISITION_BUDGET
        );
        assert_eq!(
            IcebergAuthorityMaterialSource::budget_for(Instant::now()),
            MIN_ACQUISITION_BUDGET
        );
    }
}
