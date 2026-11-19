// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! One immutable operation, fresh preparation per definite catalog conflict.

pub(crate) mod observation;
pub(crate) mod prepared;

use std::collections::HashMap;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorMutationFailure, ConnectorMutationFailureKind,
};

#[cfg(test)]
use super::dependency::ValidationInputs;
use super::dependency::{self, Verdict};
#[cfg(test)]
use super::model::ObjectIdentity;
use super::model::{
    ArtifactReferences, ArtifactWriter, CleanupScope, FrozenRequest, IcebergCleanupReport,
    OperationIntent, PublicationOutcome, PublicationReport, RequestShape,
};
use super::operation::{IcebergCommitAttempt, IcebergCommitOperation};
use super::staging::{PreparedChange, Preparer, StagingBase, StagingEngine};
use crate::catalog::error::{CatalogCommitEvidence, CatalogOutcome, preparation_failure_kind};
use crate::catalog::transaction::CommitProof;
use crate::iceberg::spec::TableProperties;
use crate::iceberg::{Error, ErrorKind, Result};

pub(crate) type Report = PublicationReport<CommitProof, CatalogCommitEvidence>;

#[async_trait]
pub(crate) trait Publisher: Send + Sync {
    /// Read the authoritative target; no cached source snapshot is a retry base.
    async fn load_target(&self, attempt: &IcebergCommitAttempt) -> Result<StagingBase>;
    async fn load_target_with_snapshots(
        &self,
        attempt: &IcebergCommitAttempt,
        mode: crate::catalog::SnapshotLoading,
    ) -> Result<StagingBase> {
        if mode != crate::catalog::SnapshotLoading::All {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Publisher has no refs-scoped metadata load",
            ));
        }
        self.load_target(attempt).await
    }
    /// Expiry receives the exact full observation. It may never reload a base.
    fn pinned_expiry_base(&self) -> Option<StagingBase> {
        None
    }
    /// Encode and bound the complete recovery ledger before publication becomes
    /// possible. Failure here is definitely undispatched.
    fn preflight_recovery(
        &self,
        request: &FrozenRequest,
        operation: &IcebergCommitOperation,
    ) -> Result<()>;
    /// Borrow the original request slot. A scoped producer may move it only
    /// into its stable original Source while the same CPU receipt is awaited.
    /// Other existing families keep their original pure preflight author until
    /// their P06 interface retirement; no network or refused CPU fallback.
    fn preflight_recovery_in<'a>(
        &'a self,
        request: &'a mut Option<crate::commit::model::FrozenRequest>,
        operation: &'a IcebergCommitOperation,
    ) -> crate::commit::attempt::RecoveryPreflightFuture<'a> {
        crate::commit::attempt::RecoveryPreflightFuture::ready(
            request
                .as_ref()
                .ok_or_else(|| {
                    Error::new(ErrorKind::DataInvalid, "Original frozen request is absent")
                })
                .and_then(|request| self.preflight_recovery(request, operation)),
        )
    }
    /// Consume one complete request and issue at most one external mutation.
    async fn dispatch_once(&self, request: FrozenRequest) -> CatalogOutcome<CommitProof>;
}

/// The retained OCC entrance has one direct catalog dispatch per attempt.
/// It shares staging and retry with owner publication without changing topology.
/// Pure preflight stays inline. Only a real asynchronous scoped phase owns
/// an independently quoted Box pointee; forwarding creates no hidden heap work.
pub(crate) enum RecoveryPreflightFuture<'a> {
    Ready(Option<Result<()>>),
    Pending(std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>>),
}
impl<'a> RecoveryPreflightFuture<'a> {
    pub(crate) fn ready(result: Result<()>) -> Self {
        Self::Ready(Some(result))
    }
    pub(crate) fn pending(
        original: std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>>,
    ) -> Self {
        Self::Pending(original)
    }
    pub(crate) fn bind_preparation<R: Clone + Send + Sync + 'static>(
        self,
        journal: &'a observation::PublicationJournal<R>,
        operation: &'a IcebergCommitOperation,
    ) -> Self {
        match self {
            Self::Ready(mut result) => Self::ready(
                result
                    .take()
                    .expect("one original preflight observation")
                    .and_then(|()| journal.bind_preparation(operation.request_context().clone())),
            ),
            pending => Self::pending(Box::pin(bind_preparation_original(
                pending, journal, operation,
            ))),
        }
    }
    pub(crate) fn binding_future_bytes<R: Clone + Send + Sync + 'static>() -> usize {
        fn bytes<A, B, C, F: std::future::Future>(_: fn(A, B, C) -> F) -> usize {
            std::mem::size_of::<F>()
        }
        bytes(bind_preparation_original::<R>)
    }
}
impl std::future::Future for RecoveryPreflightFuture<'_> {
    type Output = Result<()>;
    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        match self.get_mut() {
            Self::Ready(result) => {
                std::task::Poll::Ready(result.take().expect("one original preflight observation"))
            }
            Self::Pending(original) => original.as_mut().poll(cx),
        }
    }
}
async fn bind_preparation_original<R: Clone + Send + Sync + 'static>(
    original: RecoveryPreflightFuture<'_>,
    journal: &observation::PublicationJournal<R>,
    operation: &IcebergCommitOperation,
) -> Result<()> {
    original.await?;
    journal.bind_preparation(operation.request_context().clone())
}

pub(crate) type RecoveryPreflight =
    std::sync::Arc<dyn Fn(&FrozenRequest, &IcebergCommitOperation) -> Result<()> + Send + Sync>;

/// Dispatch through the same owner while preserving one observed expiry base.
#[cfg(test)]
pub(crate) struct PinnedExpiryPublisher<'a> {
    pub inner: std::sync::Arc<dyn Publisher + 'a>,
    pub base: StagingBase,
}
#[cfg(test)]
#[async_trait]
impl Publisher for PinnedExpiryPublisher<'_> {
    async fn load_target(&self, _attempt: &IcebergCommitAttempt) -> Result<StagingBase> {
        Err(Error::new(
            ErrorKind::DataInvalid,
            "Pinned expiry cannot reload its base",
        ))
    }
    fn pinned_expiry_base(&self) -> Option<StagingBase> {
        Some(self.base.clone())
    }
    fn preflight_recovery(
        &self,
        request: &FrozenRequest,
        operation: &IcebergCommitOperation,
    ) -> Result<()> {
        self.inner.preflight_recovery(request, operation)
    }
    fn preflight_recovery_in<'a>(
        &'a self,
        request: &'a mut Option<crate::commit::model::FrozenRequest>,
        operation: &'a IcebergCommitOperation,
    ) -> crate::commit::attempt::RecoveryPreflightFuture<'a> {
        self.inner.preflight_recovery_in(request, operation)
    }
    async fn dispatch_once(&self, request: FrozenRequest) -> CatalogOutcome<CommitProof> {
        self.inner.dispatch_once(request).await
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct RetryPolicy {
    retries: u32,
    min_wait: Duration,
    max_wait: Duration,
    total_timeout: Duration,
}

pub(crate) fn parse_retry_count_property_value(value: &str) -> Result<u32> {
    value.parse().map_err(|_| {
        Error::new(
            ErrorKind::DataInvalid,
            format!(
                "Invalid value for {}: {value}",
                TableProperties::PROPERTY_COMMIT_NUM_RETRIES
            ),
        )
    })
}

impl RetryPolicy {
    /// Do not parse unrelated table properties: e.g. file-format options do not own retry.
    pub(crate) fn from_properties(properties: &HashMap<String, String>) -> Result<Self> {
        fn parse<T: std::str::FromStr>(
            p: &HashMap<String, String>,
            key: &str,
            default: T,
        ) -> Result<T> {
            p.get(key).map_or(Ok(default), |value| {
                value.parse().map_err(|_| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid value for {key}: {value}"),
                    )
                })
            })
        }
        let retries = properties
            .get(TableProperties::PROPERTY_COMMIT_NUM_RETRIES)
            .map(|value| parse_retry_count_property_value(value))
            .transpose()?
            .unwrap_or(TableProperties::PROPERTY_COMMIT_NUM_RETRIES_DEFAULT as u32);
        let min_wait = Duration::from_millis(parse(
            properties,
            TableProperties::PROPERTY_COMMIT_MIN_RETRY_WAIT_MS,
            TableProperties::PROPERTY_COMMIT_MIN_RETRY_WAIT_MS_DEFAULT,
        )?);
        let max_wait = Duration::from_millis(parse(
            properties,
            TableProperties::PROPERTY_COMMIT_MAX_RETRY_WAIT_MS,
            TableProperties::PROPERTY_COMMIT_MAX_RETRY_WAIT_MS_DEFAULT,
        )?);
        let total_timeout = Duration::from_millis(parse(
            properties,
            TableProperties::PROPERTY_COMMIT_TOTAL_RETRY_TIME_MS,
            TableProperties::PROPERTY_COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT,
        )?);
        if min_wait > max_wait || total_timeout.is_zero() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Invalid Iceberg commit retry time bounds",
            ));
        }
        Ok(Self {
            retries,
            min_wait,
            max_wait,
            total_timeout,
        })
    }

    pub(crate) const fn retries(self) -> u32 {
        self.retries
    }
    pub(crate) const fn total_timeout(self) -> Duration {
        self.total_timeout
    }
    pub(crate) fn backoff(self, retry: u32) -> Duration {
        let ceiling = self.backoff_ceiling(retry);
        // The property lower/upper bounds remain hard bounds. Jitter does not
        // grant extra retry time or affect an already-dispatched request.
        let millis = rand::Rng::gen_range(
            &mut rand::thread_rng(),
            self.min_wait.as_millis()..=ceiling.as_millis(),
        );
        Duration::from_millis(
            u64::try_from(millis).expect("retry bounds originated as u64 milliseconds"),
        )
    }
    fn backoff_ceiling(self, retry: u32) -> Duration {
        self.min_wait
            .saturating_mul(1u32.checked_shl(retry).unwrap_or(u32::MAX))
            .min(self.max_wait)
    }
}

/// Neither cancellation nor elapsed retry time interrupts an already-issued dispatch.
/// Once publication is possible, its typed outcome alone decides cleanup and retry.
// Design: ADR-0234 (docs/adr/ADR-0234-operation-owned-artifacts-and-bounded-publication-facts.md)
#[derive(Clone, Copy)]
enum CommittedCleanupPhase {
    Inline,
    Retained,
}
pub(crate) async fn run(
    operation: &IcebergCommitOperation,
    intent: &OperationIntent,
    prefix: &PreparedChange,
    preparers: &[&dyn Preparer],
    publisher: &dyn Publisher,
    policy: RetryPolicy,
) -> Report {
    run_in_phase(
        operation,
        intent,
        prefix,
        preparers,
        publisher,
        policy,
        CommittedCleanupPhase::Inline,
    )
    .await
}
/// Same algorithm and one dispatch, returning only after the true publication
/// frontier. Its checked journal retains the exact published object set; the
/// caller's same original operation must later drive cleanup to actual exit.
pub(crate) async fn run_to_committed_frontier(
    operation: &IcebergCommitOperation,
    intent: &OperationIntent,
    prefix: &PreparedChange,
    preparers: &[&dyn Preparer],
    publisher: &dyn Publisher,
    policy: RetryPolicy,
) -> Report {
    run_in_phase(
        operation,
        intent,
        prefix,
        preparers,
        publisher,
        policy,
        CommittedCleanupPhase::Retained,
    )
    .await
}
async fn run_in_phase(
    operation: &IcebergCommitOperation,
    intent: &OperationIntent,
    prefix: &PreparedChange,
    preparers: &[&dyn Preparer],
    publisher: &dyn Publisher,
    policy: RetryPolicy,
    cleanup_phase: CommittedCleanupPhase,
) -> Report {
    if let Err(error) = operation.enter_commit_loop() {
        // Re-entry is an implementation error, not another cleanup opportunity.
        return Report {
            publication: PublicationOutcome::KnownUncommitted(before_dispatch_failure(&error)),
            cleanup: IcebergCleanupReport::NotAttempted,
        };
    }
    if operation.token() != intent.token() {
        return rejected(
            operation,
            ConnectorMutationFailure::new(
                ConnectorMutationFailureKind::InvalidRequest,
                "Commit operation and intent identities differ",
            ),
        )
        .await;
    }
    let started = Instant::now();
    let deadline = started
        .checked_add(policy.total_timeout)
        .map_or(operation.request_context().deadline(), |deadline| {
            deadline.min(operation.request_context().deadline())
        });
    if let Err(error) = operation.bind_preparation_deadline(deadline) {
        return rejected(operation, before_dispatch_failure(&error)).await;
    }
    let mut attempt_number = 0;
    let mut captured_merge_policy = None;
    loop {
        // This segment covers only preparation. Returning from the helper
        // releases its observation guard before dispatch or rejected cleanup.
        let prepared = operation
            .runtime()
            .observe_preparation_async(
                async {
                    check_window(operation, started, policy)
                        .map_err(|error| before_dispatch_failure(&error))?;
                    let attempt = operation
                        .begin_attempt()
                        .map_err(|error| before_dispatch_failure(&error))?;
                    let loaded = if intent.shape() == RequestShape::DocumentExpiry {
                        publisher.pinned_expiry_base().ok_or_else(|| {
                            Error::new(
                                ErrorKind::DataInvalid,
                                "Document expiry requires its exact full-observation base",
                            )
                        })
                    } else {
                        publisher
                            .load_target_with_snapshots(&attempt, intent.snapshot_loading())
                            .await
                    };
                    let base = loaded.map_err(|error| before_dispatch_failure(&error))?;
                    check_window(operation, started, policy)
                        .map_err(|error| before_dispatch_failure(&error))?;
                    let (request, retained) = prepare(
                        &attempt,
                        intent,
                        base,
                        prefix,
                        preparers,
                        &mut captured_merge_policy,
                    )
                    .await?;
                    check_window(operation, started, policy)
                        .map_err(|error| before_dispatch_failure(&error))?;
                    let mut original_request = Some(request);
                    publisher
                        .preflight_recovery_in(&mut original_request, operation)
                        .await
                        .map_err(|error| before_dispatch_failure(&error))?;
                    let request = original_request.ok_or_else(|| {
                        before_dispatch_failure(&Error::new(
                            ErrorKind::DataInvalid,
                            "Preflight did not return its original frozen request",
                        ))
                    })?;
                    check_window(operation, started, policy)
                        .map_err(|error| before_dispatch_failure(&error))?;
                    Ok::<_, ConnectorMutationFailure>((attempt, request, retained))
                },
                |failure| failure.clone(),
            )
            .await;
        let (attempt, request, retained) = match prepared {
            Ok(prepared) => prepared,
            Err(failure) => return rejected(operation, failure).await,
        };
        match publisher.dispatch_once(request).await {
            CatalogOutcome::KnownCommitted { receipt, .. } => {
                return Report {
                    publication: PublicationOutcome::Committed(receipt),
                    cleanup: match cleanup_phase {
                        CommittedCleanupPhase::Inline => {
                            operation.cleanup_after_commit(&retained).await
                        }
                        CommittedCleanupPhase::Retained => IcebergCleanupReport::NotAttempted,
                    },
                };
            }
            CatalogOutcome::CommitUnknown { failure, evidence } => {
                // Preserve every owned object, including prior attempts with partial cleanup.
                return Report {
                    publication: PublicationOutcome::Unknown {
                        failure,
                        evidence,
                        operation: operation.token(),
                    },
                    cleanup: IcebergCleanupReport::NotAttempted,
                };
            }
            CatalogOutcome::KnownUncommitted { failure }
                if failure.kind() == ConnectorMutationFailureKind::Conflict
                    && intent.permits_conflict_retry()
                    && attempt_number < policy.retries =>
            {
                if let Err(error) =
                    operation.retire_attempt_bound_artifacts(attempt.attempt_token())
                {
                    return rejected(operation, before_dispatch_failure(&error)).await;
                }
                let wait = policy.backoff(attempt_number);
                let left = policy.total_timeout.saturating_sub(started.elapsed());
                if wait >= left {
                    return rejected(operation, failure).await;
                }
                // Retired objects remain owned until a terminal result. Backoff starts no deletion.
                if let Err(error) = operation
                    .runtime()
                    .observe_preparation_async(operation.pause(wait), before_dispatch_failure)
                    .await
                {
                    return rejected(operation, before_dispatch_failure(&error)).await;
                }
                attempt_number += 1;
            }
            CatalogOutcome::KnownUncommitted { failure }
                if failure.kind() == ConnectorMutationFailureKind::Conflict
                    && intent.shape() != RequestShape::DocumentExpiry
                    && intent.shape() != RequestShape::Create
                    && intent
                        .dependencies()
                        .contains(&super::model::Dependency::RefUnchanged) =>
            {
                // One diagnostic observation cannot authorize another prepare or dispatch.
                // Its failure/cancellation never changes the already-known disposition.
                let failure =
                    diagnose_known_ref_conflict(intent, &attempt, publisher, failure).await;
                return rejected(operation, failure).await;
            }
            CatalogOutcome::KnownUncommitted { failure } => {
                return rejected(operation, failure).await;
            }
            CatalogOutcome::Unsupported(error) => {
                return rejected(
                    operation,
                    ConnectorMutationFailure::new(
                        ConnectorMutationFailureKind::Unsupported,
                        error.message(),
                    ),
                )
                .await;
            }
        }
    }
}

fn check_window(
    operation: &IcebergCommitOperation,
    start: Instant,
    policy: RetryPolicy,
) -> Result<()> {
    operation.check_active()?;
    if start.elapsed() >= policy.total_timeout {
        return Err(Error::new(
            ErrorKind::Unexpected,
            "Iceberg commit retry deadline exceeded",
        )
        .with_source(ConnectorError::new(
            ConnectorErrorKind::DeadlineExceeded,
            "Iceberg commit retry deadline exceeded",
        )));
    }
    Ok(())
}

/// Best-effort dependency detail after a definite rejection. Always retain
/// the original failure unless the same table's fresh ref actually proves it.
async fn diagnose_known_ref_conflict(
    intent: &OperationIntent,
    attempt: &IcebergCommitAttempt,
    publisher: &dyn Publisher,
    original: ConnectorMutationFailure,
) -> ConnectorMutationFailure {
    if attempt.check_active().is_err() {
        return original;
    }
    let Ok(StagingBase::Existing { metadata, .. }) = publisher
        .load_target_with_snapshots(attempt, crate::catalog::SnapshotLoading::Refs)
        .await
    else {
        return original;
    };
    if attempt.check_active().is_err() || intent.target().uuid != Some(metadata.uuid()) {
        return original;
    }
    let parent = metadata
        .snapshot_for_ref(intent.target_ref())
        .map(|snapshot| snapshot.snapshot_id());
    let verdict = dependency::diagnose_ref_unchanged(intent, &metadata, parent, attempt).await;
    if attempt.check_active().is_err() {
        return original;
    }
    match verdict {
        Ok(
            verdict @ Verdict::Broken {
                dependency: super::model::Dependency::RefUnchanged,
                ..
            },
        ) => ConnectorMutationFailure::new(
            original.kind(),
            format!(
                "Iceberg commit dependency failed: {verdict:?}; original rejection: {}",
                original.message(),
            ),
        ),
        _ => original,
    }
}

async fn prepare(
    attempt: &IcebergCommitAttempt,
    intent: &OperationIntent,
    base: StagingBase,
    prefix: &PreparedChange,
    preparers: &[&dyn Preparer],
    captured_merge_policy: &mut Option<super::preparation::CapturedMergePolicy>,
) -> std::result::Result<(FrozenRequest, ArtifactReferences), ConnectorMutationFailure> {
    let metadata = match &base {
        StagingBase::Existing { metadata, .. } => metadata,
        StagingBase::Create { staged, .. } => staged.initial_metadata(),
    };
    attempt
        .check_stage_snapshot_ids(metadata)
        .map_err(|error| before_dispatch_failure(&error))?;
    if let StagingBase::Existing { metadata, .. } = &base {
        if intent.target().uuid != Some(metadata.uuid()) {
            return Err(ConnectorMutationFailure::new(
                ConnectorMutationFailureKind::Conflict,
                "Iceberg commit target table UUID was replaced",
            ));
        }
        let parent = metadata
            .snapshot_for_ref(intent.target_ref())
            .map(|s| s.snapshot_id());
        match dependency::validate(intent, metadata, parent, attempt)
            .await
            .map_err(|e| before_dispatch_failure(&e))?
        {
            Verdict::Holds => {}
            verdict => {
                return Err(ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::Conflict,
                    format!("Iceberg commit dependency failed: {verdict:?}"),
                ));
            }
        }
    }
    let mut engine = StagingEngine::begin_attempt(
        base,
        intent,
        attempt,
        attempt.io_window_width(),
        captured_merge_policy,
    )
    .map_err(|e| before_dispatch_failure(&e))?;
    engine
        .stage_change(prefix.clone())
        .map_err(|e| before_dispatch_failure(&e))?;
    for preparer in preparers {
        engine
            .stage(*preparer)
            .await
            .map_err(|e| before_dispatch_failure(&e))?;
    }
    let request = engine
        .freeze()
        .map_err(|error| before_dispatch_failure(&error))?;
    let retained = request.artifacts().references().clone();
    Ok((request, retained))
}

pub(crate) async fn rejected(
    operation: &IcebergCommitOperation,
    failure: ConnectorMutationFailure,
) -> Report {
    Report {
        publication: PublicationOutcome::KnownUncommitted(failure),
        cleanup: operation.cleanup(CleanupScope::EntireOperation).await,
    }
}

/// The error's typed source preserves cancellation/deadline through the format adapter.
/// Preparation and load failures are proven undispatched regardless of their wording.
pub(crate) fn before_dispatch_failure(error: &Error) -> ConnectorMutationFailure {
    use std::error::Error as _;
    let mut source = error.source();
    while let Some(cause) = source {
        if let Some(error) = cause.downcast_ref::<ConnectorError>() {
            return ConnectorMutationFailure::from_connector(error.clone());
        }
        source = cause.source();
    }
    ConnectorMutationFailure::new(preparation_failure_kind(error), error.to_string())
}

#[cfg(test)]
mod tests;
